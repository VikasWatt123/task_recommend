"""
Permit Files Router - MongoDB Based
"""
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from datetime import datetime
import uuid
import os
import io
import re
import hashlib
import logging

from pypdf import PdfReader
from app.services.file_deduplication_service import FileDeduplicationService
from app.services.stage_tracking_service import get_stage_tracking_service
from app.models.stage_flow import FileStage

# ZIP extraction utilities (copied from zip_assign.py)
def _normalize_extracted_text(text: str) -> str:
    if not text:
        return ""
    # Remove common invisible separators that break regex matching
    text = text.replace("\u200b", " ")  # zero width space
    text = text.replace("\u200c", " ")
    text = text.replace("\u200d", " ")
    text = text.replace("\ufeff", " ")
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def _extract_zip_candidates(text: str) -> List[str]:
    """Return candidate 5-digit ZIPs found in text (best-effort)."""
    if not text:
        return []

    candidates: List[str] = []

    # Pattern: "LA 71303" / "LA-71303" / "LA:71303" / "LA, 71303" and ZIP+4.
    for m in re.finditer(r"\b[A-Z]{2}\s*[-,:]?\s*(\d{5})(?:-\d{4})?\b", text, flags=re.IGNORECASE):
        candidates.append(m.group(1))

    # Pattern: standalone ZIP or ZIP+4
    for m in re.finditer(r"\b(\d{5})(?:-\d{4})?\b", text):
        candidates.append(m.group(1))

    # Pattern: spaced digits e.g. "7 1 3 0 3" or "7-1-3-0-3"
    for m in re.finditer(r"(?<!\d)(\d(?:[\s\-]{1,3}\d){4})(?!\d)", text):
        compact = re.sub(r"[\s\-]+", "", m.group(1))
        if len(compact) == 5 and compact.isdigit():
            candidates.append(compact)

    # De-dupe while preserving order
    seen = set()
    ordered: List[str] = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            ordered.append(c)
    return ordered

def _extract_zip_from_pdf_first_page(pdf_bytes: bytes) -> Optional[str]:
    """Extract first 5-digit ZIP code from PDF first 3 pages."""
    if not pdf_bytes:
        return None
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        if not reader.pages:
            return None
        
        # Extract text from first 3 pages (or fewer if PDF has less pages)
        all_text = ""
        max_pages = min(3, len(reader.pages))
        
        for i in range(max_pages):
            page = reader.pages[i]
            # Try default extraction; if empty, try layout mode
            page_text = (page.extract_text() or "").strip()
            if not page_text:
                try:
                    page_text = (page.extract_text(extraction_mode="layout") or "").strip()
                except TypeError:
                    # Older pypdf versions may not support extraction_mode
                    pass
            all_text += page_text + " "
            
            # If we found a ZIP in current page, no need to check more pages
            if page_text:
                normalized = _normalize_extracted_text(page_text)
                candidates = _extract_zip_candidates(normalized)
                if candidates:
                    zip_code = candidates[0]
                    logger.info(f"[UPLOAD] Extracted ZIP from page {i+1}: {zip_code}")
                    return zip_code
        
        # If no ZIP found in individual pages, try searching all pages combined
        normalized = _normalize_extracted_text(all_text)
        logger.info(f"[UPLOAD] Combined text from {max_pages} pages length: {len(normalized)}")
        
        candidates = _extract_zip_candidates(normalized)
        logger.info(f"[UPLOAD] ZIP candidates found: {candidates}")
        if not candidates:
            logger.warning("[UPLOAD] No ZIP candidates found in extracted text")
            return None

        zip_code = candidates[0]
        logger.info(f"[UPLOAD] Extracted ZIP from combined pages: {zip_code}")
        return zip_code
        
    except Exception as e:
        logger.error(f"[UPLOAD] PDF parsing failed: {e}")
        return None

# State to ZIP range mapping (copied from zip_assign.py)
US_STATE_ZIP_RANGES: Dict[str, Dict[str, str]] = {
    "massachusetts": {"code": "MA", "zip_min": "01001", "zip_max": "05544"},
    "rhode island": {"code": "RI", "zip_min": "02801", "zip_max": "02940"},
    "florida": {"code": "FL", "zip_min": "32003", "zip_max": "34997"},
    "georgia": {"code": "GA", "zip_min": "30002", "zip_max": "39901"},
    "oregon": {"code": "OR", "zip_min": "97001", "zip_max": "97920"},
    "washington": {"code": "WA", "zip_min": "98001", "zip_max": "99403"},
    "arizona": {"code": "AZ", "zip_min": "85001", "zip_max": "86556"},
    "connecticut": {"code": "CT", "zip_min": "06001", "zip_max": "06928"},
    "utah": {"code": "UT", "zip_min": "84001", "zip_max": "84791"},
    "louisiana": {"code": "LA", "zip_min": "70001", "zip_max": "71497"},
    "illinois": {"code": "IL", "zip_min": "60001", "zip_max": "62999"},
    "texas": {"code": "TX", "zip_min": "73301", "zip_max": "88595"},
    "california": {"code": "CA", "zip_min": "90001", "zip_max": "96162"},
    "pennsylvania": {"code": "PA", "zip_min": "15001", "zip_max": "19640"},
    "maryland": {"code": "MD", "zip_min": "20601", "zip_max": "21930"},
}

# Team lead to state mapping (copied from zip_assign.py)
TEAM_LEAD_STATE_MAP: Dict[str, List[str]] = {
    "MA": ["Rahul (0081)", "Tanweer Alam (0067)"],
    "RI": ["Rahul (0081)"],
    "FL": ["Gaurav Mavi (0146)"],
    "GA": ["Gaurav Mavi (0146)"],
    "OR": ["Gaurav Mavi (0146)"],
    "WA": ["Gaurav Mavi (0146)"],
    "COMMERCIAL": ["Harish (0644)"],
    "AZ": ["Prashant Sharma (0079)", "Shivam Kumar (0083)", "Rohan Kashid (0902)"],
    "CT": ["Prashant Sharma (0079)"],
    "UT": ["Prashant Sharma (0079)"],
    "LA": ["Prashant Sharma (0079)"],
    "IL": ["Prashant Sharma (0079)"],
    "TX": ["Saurav Yadav (0119)"],
    "CA": ["Shivam Kumar (0083)", "Rohan Kashid (0902)", "Sunder Raj D (0462)"],
    "PA": ["Shivam Kumar (0083)", "Rohan Kashid (0902)", "Sunder Raj D (0462)", "Tanweer Alam (0067)"],
    "MD": ["Tanweer Alam (0067)"],
}

def _validate_zip_and_get_state(zip_code: str) -> Optional[str]:
    """Validate ZIP and return state code if valid."""
    zip_int = int(zip_code)
    for state_name, info in US_STATE_ZIP_RANGES.items():
        zip_min = int(info["zip_min"])
        zip_max = int(info["zip_max"])
        if zip_min <= zip_int <= zip_max:
            state_code = info["code"]
            logger.info(f"[UPLOAD] ZIP {zip_code} -> state {state_code} ({state_name})")
            return state_code
    logger.warning(f"[UPLOAD] ZIP {zip_code} does not fall in any state range")
    return None

def _choose_team_lead_for_state(state_code: str) -> Optional[str]:
    """Choose team lead for a state."""
    candidates = TEAM_LEAD_STATE_MAP.get(state_code) or []
    if not candidates:
        logger.warning(f"[UPLOAD] No team lead found for state: {state_code}")
        return None
    # For now, pick first candidate (deterministic by load can be added later)
    chosen = candidates[0]
    logger.info(f"[UPLOAD] Chosen team lead for {state_code}: {chosen}")
    return chosen

def _extract_team_lead_code(team_lead: str) -> Optional[str]:
    """Extract team lead code from name string."""
    if not team_lead:
        return None
    match = re.search(r"\(([^)]+)\)", team_lead)
    return match.group(1).strip() if match else None

def _extract_zip_from_address(address: str) -> Optional[str]:
    """Extract 5-digit ZIP code from address string."""
    if not address:
        return None
    
    # Normalize address
    address = address.strip()
    
    # Pattern 1: 5-digit ZIP at end (most common)
    match = re.search(r'\b(\d{5})(?:-\d{4})?\s*$', address)
    if match:
        return match.group(1)
    
    # Pattern 2: ZIP after state abbreviation
    match = re.search(r'\b[A-Z]{2}\s*[,]?\s*(\d{5})(?:-\d{4})?\b', address, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    
    # Pattern 3: Any 5-digit number in address (fallback)
    match = re.search(r'\b(\d{5})(?:-\d{4})?\b', address)
    if match:
        return match.group(1)
    
    return None

from app.db.mongodb import get_db
from app.core.settings import settings
from app.services.stage_tracking_service import get_stage_tracking_service
from app.services.recommendation_engine import get_recommendation_engine
from logging import getLogger

logger = getLogger(__name__)
logger.setLevel(logging.INFO)
from app.api.v1.routers.tasks import TaskCreate, TaskAssign, create_task

router = APIRouter(prefix="/permit-files", tags=["permit_files"])

# Create uploads directory if it doesn't exist
UPLOAD_DIR = settings.uploads_dir
os.makedirs(UPLOAD_DIR, exist_ok=True)

def generate_file_id():
    """Generate unique file ID"""
    return f"PF-{datetime.now().strftime('%Y%m%d')}-{str(uuid.uuid4())[:8].upper()}"

def _get_completed_stages(tracking):
    """Get list of completed stages for a file"""
    if not tracking:
        return []

    stage_history = None
    if isinstance(tracking, dict):
        stage_history = tracking.get('stage_history')
    else:
        stage_history = getattr(tracking, 'stage_history', None)

    if not stage_history:
        return []

    completed = []
    for history_entry in stage_history:
        if isinstance(history_entry, dict):
            stage = history_entry.get("stage")
            status = history_entry.get("stage_status")
            if status == "COMPLETED":
                completed.append(stage)
    return completed

def _calculate_stage_duration(history_entry):
    """Calculate duration of a stage in minutes"""
    if not isinstance(history_entry, dict):
        return None
    
    started_at = history_entry.get("started_stage_at")
    completed_at = history_entry.get("completed_stage_at")
    
    if started_at and completed_at:
        try:
            if isinstance(started_at, str):
                started_at = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
            if isinstance(completed_at, str):
                completed_at = datetime.fromisoformat(completed_at.replace('Z', '+00:00'))
            
            duration = completed_at - started_at
            return int(duration.total_seconds() / 60)
        except:
            pass
    return None

def _calculate_total_time(tracking):
    """Calculate total time spent across all completed stages"""
    if not tracking:
        return 0

    stage_history = None
    if isinstance(tracking, dict):
        stage_history = tracking.get('stage_history')
    else:
        stage_history = getattr(tracking, 'stage_history', None)

    if not stage_history:
        return 0

    total_minutes = 0
    for history_entry in stage_history:
        duration = _calculate_stage_duration(history_entry)
        if duration:
            total_minutes += duration
    return total_minutes


TEAM_LEAD_STATE_MAP: Dict[str, List[str]] = {
    "MA": ["Rahul (0081)", "Tanweer Alam (0067)"],
    "RI": ["Rahul (0081)"],
    "FL": ["Gaurav Mavi (0146)"],
    "GA": ["Gaurav Mavi (0146)"],
    "OR": ["Gaurav Mavi (0146)"],
    "WA": ["Gaurav Mavi (0146)"],
    "COMMERCIAL": ["Harish (0644)"],
    "AZ": ["Prashant Sharma (0079)", "Shivam Kumar (0083)", "Rohan Kashid (0902)"],
    "CT": ["Prashant Sharma (0079)"],
    "UT": ["Prashant Sharma (0079)"],
    "LA": ["Prashant Sharma (0079)"],
    "IL": ["Prashant Sharma (0079)"],
    "TX": ["Saurav Yadav (0119)"],
    "CA": ["Shivam Kumar (0083)", "Rohan Kashid (0902)", "Sunder Raj D (0462)"],
    "PA": ["Shivam Kumar (0083)", "Rohan Kashid (0902)", "Sunder Raj D (0462)", "Tanweer Alam (0067)"],
    "MD": ["Tanweer Alam (0067)"],
}


US_STATE_NAME_TO_CODE: Dict[str, List[str]] = {
    "massachusetts": ["MA", "01001", "05544"],  
    "rhode island": ["RI", "02801", "02940"],
    "florida": ["FL", "32003", "34997"],
    "georgia": ["GA", "30002", "39901"],
    "oregon": ["OR", "97001", "97920"],
    "washington": ["WA", "98001", "99403"],
    "arizona": ["AZ", "85001", "86556"],
    "connecticut": ["CT", "06001", "06928"],
    "utah": ["UT", "84001", "84791"],
    "louisiana": ["LA", "70001", "71497"],
    "illinois": ["IL", "60001", "62999"],
    "texas": ["TX", "73301", "88595"],
    "california": ["CA", "90001", "96162"],
    "pennsylvania": ["PA", "15001", "19640"],
    "maryland": ["MD", "20601", "21930"],
}


def _extract_team_lead_code(team_lead: str) -> Optional[str]:
    if not team_lead:
        return None
    match = re.search(r"\(([^)]+)\)", team_lead)
    return match.group(1).strip() if match else None


def _extract_state_from_pdf_first_page(pdf_bytes: bytes) -> Optional[str]:
    """Extract US state code from the first 3 pages of a PDF using ZIP code fallback."""

    if not pdf_bytes:
        return None

    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        if not reader.pages:
            return None
        
        # Extract text from first 3 pages (or fewer if PDF has less pages)
        all_text = ""
        max_pages = min(3, len(reader.pages))
        
        for i in range(max_pages):
            page = reader.pages[i]
            page_text = (page.extract_text() or "").strip()
            all_text += page_text + " "
            
            # If we found state info in current page, no need to check more pages
            if page_text:
                # Check for commercial keyword
                if re.search(r"\bcommercial\b", page_text, flags=re.IGNORECASE):
                    logger.info(f"[STATE EXTRACTION] Detected COMMERCIAL keyword on page {i+1}")
                    return "COMMERCIAL"
                
                # Try to extract ZIP codes
                zip_matches = re.findall(r"\b(\d{5})\b", page_text)
                if zip_matches:
                    for zip_code in zip_matches:
                        zip_int = int(zip_code)
                        logger.info(f"[STATE EXTRACTION] Found ZIP on page {i+1}: {zip_code}")
                        for state_name, info in US_STATE_NAME_TO_CODE.items():
                            state_code, zip_min, zip_max = info
                            if zip_min <= zip_int <= zip_max:
                                logger.info(f"[STATE EXTRACTION] ZIP {zip_code} -> state {state_name} ({state_code})")
                                return state_code
                
                # Check for state abbreviations
                supported_codes = [
                    "MA", "RI", "FL", "GA", "OR", "WA",
                    "AZ", "CT", "UT", "LA", "IL", "TX", "CA", "PA", "MD",
                ]
                for code in supported_codes:
                    if re.search(rf"\b{re.escape(code)}\b", page_text, flags=re.IGNORECASE):
                        logger.info(f"[STATE EXTRACTION] Detected state code on page {i+1}: {code}")
                        return code
                
                # Check for full state names
                text_lower = page_text.lower()
                for state_name, info in US_STATE_NAME_TO_CODE.items():
                    if state_name in text_lower:
                        state_code = info[0]
                        logger.info(f"[STATE EXTRACTION] Detected state name on page {i+1}: {state_name} -> {state_code}")
                        return state_code
        
        # If nothing found in individual pages, check all pages combined
        logger.info(f"[STATE EXTRACTION] Checking combined text from {max_pages} pages")
        text = all_text.strip()
        
    except Exception:
        return None

    if not text:
        return None

    logger.info(f"[STATE EXTRACTION] Combined text preview: {text[:300]}...")

    # 1) Commercial keyword first
    if re.search(r"\bcommercial\b", text, flags=re.IGNORECASE):
        logger.info("[STATE EXTRACTION] Detected COMMERCIAL keyword in combined text")
        return "COMMERCIAL"

    # 2) Try to extract ZIP codes (5-digit) and map to state (highest priority)
    zip_matches = re.findall(r"\b(\d{5})\b", text)
    if zip_matches:
        for zip_code in zip_matches:
            zip_int = int(zip_code)
            logger.info(f"[STATE EXTRACTION] Found ZIP in combined text: {zip_code}")
            for state_name, info in US_STATE_NAME_TO_CODE.items():
                state_code, zip_min, zip_max = info
                if zip_min <= zip_int <= zip_max:
                    logger.info(f"[STATE EXTRACTION] ZIP {zip_code} -> state {state_name} ({state_code})")
                    return state_code

    # 3) Prefer explicit state abbreviations (e.g., "LA")
    supported_codes = [
        "MA", "RI", "FL", "GA", "OR", "WA",
        "AZ", "CT", "UT", "LA", "IL", "TX", "CA", "PA", "MD",
    ]
    for code in supported_codes:
        if re.search(rf"\b{re.escape(code)}\b", text, flags=re.IGNORECASE):
            logger.info(f"[STATE EXTRACTION] Detected state code in combined text: {code}")
            return code

    # 4) Fallback to full state names
    text_lower = text.lower()
    for state_name, info in US_STATE_NAME_TO_CODE.items():
        if state_name in text_lower:
            state_code = info[0]
            logger.info(f"[STATE EXTRACTION] Detected state name in combined text: {state_name} -> {state_code}")
            return state_code

    logger.warning(f"[STATE EXTRACTION] No state detected in first {max_pages} pages")
    return None


def _choose_team_lead_for_state(state_code: str) -> Optional[str]:
    candidates = TEAM_LEAD_STATE_MAP.get(state_code) or []
    if not candidates:
        logger.warning(f"[TEAM LEAD SELECTION] No candidates found for state: {state_code}")
        return None

    logger.info(f"[TEAM LEAD SELECTION] State: {state_code}, Candidates: {candidates}")

    engine = get_recommendation_engine()
    scored: List[Dict[str, Any]] = []

    for lead in candidates:
        employees = engine.load_employees(lead)
        if not employees:
            continue

        team_active = sum([(e.get("active_task_count", 0) or 0) for e in employees])
        team_size = len(employees)
        scored.append({
            "lead": lead,
            "team_active": team_active,
            "team_size": team_size,
        })

    if not scored:
        return None

    # Deterministic: lowest active load, then smaller team size, then lexicographic lead key.
    scored.sort(key=lambda x: (x["team_active"], x["team_size"], x["lead"].lower()))
    chosen = scored[0]["lead"]
    logger.info(f"[TEAM LEAD SELECTION] Chosen lead: {chosen} (load: {scored[0]['team_active']}, size: {scored[0]['team_size']})")
    return chosen

@router.get("/")
async def get_permit_files(
    limit: int = 100,
    offset: int = 0,
    client: Optional[str] = None,
    status: Optional[str] = None
):
    """Get all permit files from MongoDB with current stage status"""
    db = get_db()
    
    # Build filter from query params
    query_filter = {}
    if client:
        query_filter["project_details.client_name"] = client
    if status:
        query_filter["status"] = status
    
    # Lean projection: only fetch fields needed by frontend
    files = list(db.permit_files.find(
        query_filter,
        {
            "_id": 0,
            "file_id": 1,
            "file_info.original_filename": 1,
            "file_info.stored_filename": 1,
            "file_info.file_size": 1,
            "file_info.uploaded_at": 1,
            "file_info.file_path": 1,
            "project_details.client_name": 1,
            "project_details.project_name": 1,
            "status": 1,
            "workflow_step": 1,
            "metadata.uploaded_by": 1,
            "metadata.created_at": 1,
            "metadata.updated_at": 1,
            "assigned_to_lead": 1
        }
    ).sort("metadata.created_at", -1).skip(offset).limit(limit))
    stage_service = get_stage_tracking_service()
    
    # Bulk fetch stage tracking for all file_ids to avoid N+1 queries
    file_ids = [f.get("file_id") for f in files if f.get("file_id")]
    tracking_map = {}
    if file_ids:
        try:
            # Fetch all tracking records in one query
            all_tracking = list(db.file_tracking.find(
                {"file_id": {"$in": file_ids}}, 
                {"_id": 0, "file_id": 1, "current_stage": 1, "current_assignment": 1}
            ))
            tracking_map = {t["file_id"]: t for t in all_tracking}
            logger.info(f"Bulk fetched tracking for {len(tracking_map)} files")
        except Exception as e:
            logger.warning(f"Failed to bulk fetch stage tracking: {e}")
    
    # Transform files to include file_name from original_filename
    transformed_files = []
    for file in files:
        transformed_file = file.copy()
        file_id = file.get("file_id")
        
        # Set file_name from original_filename for frontend compatibility
        if file.get("file_info", {}).get("original_filename"):
            transformed_file["file_name"] = file["file_info"]["original_filename"]
        elif file.get("file_info", {}).get("stored_filename"):
            stored = file["file_info"]["stored_filename"]
            if file_id and isinstance(stored, str) and stored.startswith(f"{file_id}_"):
                stored = stored[len(f"{file_id}_"):]
            transformed_file["file_name"] = stored
        elif file.get("file_name"):
            transformed_file["file_name"] = file["file_name"]
        else:
            transformed_file["file_name"] = f"File-{file.get('file_id', 'Unknown')}"
        
        # Set client_name from project_details for frontend compatibility
        if file.get("project_details", {}).get("client_name"):
            transformed_file["client"] = file["project_details"]["client_name"]
        
        # Set file_size from file_info if not already set
        if not transformed_file.get("file_size") and file.get("file_info", {}).get("file_size"):
            transformed_file["file_size"] = file["file_info"]["file_size"]
            
        # Ensure workflow_step is included
        if "workflow_step" in file:
            transformed_file["workflow_step"] = file["workflow_step"]
        
        # Include assignment information
        if "assignment" in file:
            transformed_file["assignment"] = file["assignment"]
        
        # Get current stage status from bulk-fetched tracking map
        if file_id and file_id in tracking_map:
            try:
                tracking = tracking_map[file_id]
                current_stage = tracking.get("current_stage") or "PRELIMS"
                status_map = {
                    "PRELIMS": "IN_PRELIMS",
                    "PRODUCTION": "IN_PRODUCTION", 
                    "COMPLETED": "COMPLETED",
                    "QC": "IN_QC",
                    "DELIVERED": "DELIVERED"
                }
                transformed_file["status"] = status_map.get(current_stage, "PENDING")
                transformed_file["current_step"] = current_stage
                
                # Add current assignment info if available
                current_assignment = tracking.get("current_assignment")
                if current_assignment:
                    started_at = current_assignment.get("started_at")
                    # Handle both datetime objects and strings
                    if started_at and hasattr(started_at, 'isoformat'):
                        started_at = started_at.isoformat()
                    
                    transformed_file["current_assignment"] = {
                        "employee_code": current_assignment.get("employee_code"),
                        "employee_name": current_assignment.get("employee_name"),
                        "started_at": started_at
                    }
            except Exception as e:
                logger.warning(f"Failed to process tracking for {file_id}: {e}")
        elif file_id:
            # Fallback: try individual fetch (should be rare)
            try:
                stage_service.auto_progress_from_tasks(file_id)
                tracking = stage_service.get_file_tracking(file_id)
                if tracking:
                    current_stage = tracking.get("current_stage") or "PRELIMS"
                    status_map = {
                        "PRELIMS": "IN_PRELIMS",
                        "PRODUCTION": "IN_PRODUCTION", 
                        "COMPLETED": "COMPLETED",
                        "QC": "IN_QC",
                        "DELIVERED": "DELIVERED"
                    }
                    transformed_file["status"] = status_map.get(current_stage, "PENDING")
                    transformed_file["current_step"] = current_stage
            except Exception as e:
                logger.warning(f"Failed to get stage tracking for {file_id}: {e}")
            
        transformed_files.append(transformed_file)
    
    logger.info(f"Returned {len(transformed_files)} permit files with bulk tracking")
    return transformed_files

@router.get("/unassigned")
async def get_unassigned_permit_files():
    """Get permit files that haven't been assigned to specific employees yet"""
    db = get_db()
    
    # First, get all files that don't have tasks assigned
    unassigned_files = list(db.permit_files.find({
        "$or": [
            {"tasks_created": {"$size": 0}},
            {"tasks_created": {"$exists": False}}
        ]
    }, {"_id": 0}))
    
    # Get stage tracking for all files
    file_ids = [f.get("file_id") for f in unassigned_files if f.get("file_id")]
    tracking_map = {}
    delivered_files = set()
    
    if file_ids:
        try:
            all_tracking = list(db.file_tracking.find(
                {"file_id": {"$in": file_ids}}, 
                {"_id": 0, "file_id": 1, "current_stage": 1, "current_status": 1}
            ))
            for t in all_tracking:
                tracking_map[t["file_id"]] = t
                # Mark delivered files to exclude them
                if t.get("current_stage") == "DELIVERED" or t.get("current_status") == "DELIVERED":
                    delivered_files.add(t["file_id"])
        except Exception as e:
            logger.warning(f"Failed to fetch stage tracking for unassigned files: {e}")
    
    # Filter files based on their actual stage and status
    filtered_files = []
    for file in unassigned_files:
        file_id = file.get("file_id")
        
        # Skip delivered files
        if file_id in delivered_files:
            continue
            
        # Get actual stage from tracking
        tracking = tracking_map.get(file_id, {})
        actual_stage = tracking.get("current_stage", "PRELIMS")
        
        # Only include files that are in appropriate stages for assignment
        # Files in DELIVERED stage should not be assignable
        if actual_stage in ["PRELIMS", "PRODUCTION", "QC"]:
            # Also check if the permit file status matches
            file_status = file.get("status", "IN_PRELIMS")
            
            # Include if status is appropriate for the stage
            if (actual_stage == "PRELIMS" and file_status in ["IN_PRELIMS", "ACCEPTED"]) or \
               (actual_stage == "PRODUCTION" and file_status in ["IN_PRODUCTION"]) or \
               (actual_stage == "QC" and file_status in ["IN_QC"]):
                filtered_files.append(file)
    
    # Transform files to match the same format as the main permit files endpoint
    transformed_files = []
    for file in filtered_files:
        transformed_file = file.copy()
        
        # Set file_name from original_filename (same as main endpoint)
        if file.get("file_info", {}).get("original_filename"):
            transformed_file["file_name"] = file["file_info"]["original_filename"]
        elif not transformed_file.get("file_name"):
            transformed_file["file_name"] = f"File-{file.get('file_id', 'Unknown')}"
        
        # Set client_name from project_details for frontend compatibility
        if file.get("project_details", {}).get("client_name"):
            transformed_file["client"] = file["project_details"]["client_name"]
        
        # Set file_size from file_info if not already set
        if not transformed_file.get("file_size") and file.get("file_info", {}).get("file_size"):
            transformed_file["file_size"] = file["file_info"]["file_size"]
            
        # Ensure workflow_step is included
        if "workflow_step" in file:
            transformed_file["workflow_step"] = file["workflow_step"]
        
        # Set current_stage from actual tracking, fallback to workflow_step
        file_id = file.get("file_id")
        if file_id and file_id in tracking_map:
            tracking = tracking_map[file_id]
            transformed_file["current_stage"] = tracking.get("current_stage", "PRELIMS")
        else:
            transformed_file["current_stage"] = file.get("workflow_step", "PRELIMS")
        
        # Set created_at from metadata (same format as main endpoint)
        if file.get("metadata", {}).get("created_at"):
            if isinstance(file["metadata"]["created_at"], datetime):
                transformed_file["created_at"] = file["metadata"]["created_at"].isoformat()
            else:
                transformed_file["created_at"] = file["metadata"]["created_at"]
        else:
            transformed_file["created_at"] = datetime.utcnow().isoformat()
        
        # Include assignment information
        if "assignment" in file:
            transformed_file["assignment"] = file["assignment"]
            
        # Include uploaded_by for compatibility
        if file.get("metadata", {}).get("uploaded_by"):
            transformed_file["uploaded_by"] = file["metadata"]["uploaded_by"]
        
        transformed_files.append(transformed_file)
    
    return transformed_files

@router.get("/{file_id}")
async def get_permit_file(file_id: str):
    """Get a specific permit file with current stage status"""
    db = get_db()
    file = db.permit_files.find_one({"file_id": file_id}, {"_id": 0})
    
    if not file:
        raise HTTPException(status_code=404, detail="Permit file not found")
    
    # Transform file to include file_name from original_filename
    transformed_file = file.copy()
    if file.get("file_info", {}).get("original_filename"):
        transformed_file["file_name"] = file["file_info"]["original_filename"]
    elif not transformed_file.get("file_name"):
        transformed_file["file_name"] = f"File-{file.get('file_id', 'Unknown')}"
    
    # Set client_name from project_details for frontend compatibility
    if file.get("project_details", {}).get("client_name"):
        transformed_file["client"] = file["project_details"]["client_name"]
    
    # Ensure workflow_step is included
    if "workflow_step" in file:
        transformed_file["workflow_step"] = file["workflow_step"]
    
    # Get current stage status from stage tracking
    stage_service = get_stage_tracking_service()
    try:
        # Best-effort: reconcile tracking from task completion state so UI doesn't lag
        try:
            stage_service.auto_progress_from_tasks(file_id)
        except Exception:
            pass
        tracking = stage_service.get_file_tracking(file_id)
        if tracking:
            # tracking is a dict, not an object
            current_stage = tracking.get("current_stage") or "PRELIMS"
            status_map = {
                "PRELIMS": "IN_PRELIMS",
                "PRODUCTION": "IN_PRODUCTION", 
                "COMPLETED": "COMPLETED",
                "QC": "IN_QC",
                "DELIVERED": "DELIVERED"
            }
            transformed_file["status"] = status_map.get(current_stage, "PENDING")
            transformed_file["current_step"] = current_stage
            
            # Add current assignment info if available
            current_assignment = tracking.get("current_assignment")
            if current_assignment:
                started_at = current_assignment.get("started_at")
                # Handle both datetime objects and strings
                if started_at and hasattr(started_at, 'isoformat'):
                    started_at = started_at.isoformat()
                
                transformed_file["current_assignment"] = {
                    "employee_code": current_assignment.get("employee_code"),
                    "employee_name": current_assignment.get("employee_name"),
                    "started_at": started_at
                }
    except Exception as e:
        logger.warning(f"Failed to get stage tracking for {file_id}: {e}")
        # Keep original status if tracking fails
    
    return transformed_file

@router.post("/upload")
async def upload_permit_file(
    pdf: UploadFile = File(...),
    client_name: str = Form(...),
    project_name: str = Form(None),
    assigned_to_lead: str = Form(...),
    workflow_step: str = Form("PRELIMS")  # Add workflow step selection
):
    """Upload a new permit file"""
    db = get_db()
    
    # Validate workflow step
    valid_steps = ["PRELIMS", "PRODUCTION", "QC"]
    if workflow_step not in valid_steps:
        raise HTTPException(status_code=400, detail=f"Invalid workflow step. Must be one of: {valid_steps}")
    
    # Read file content for hashing
    content = await pdf.read()
    
    # Extract ZIP code from PDF (auto-detection)
    zip_code = _extract_zip_from_pdf_first_page(content)
    state_code = None
    team_lead_from_zip = None
    
    if zip_code:
        state_code = _validate_zip_and_get_state(zip_code)
        if state_code:
            team_lead_from_zip = _choose_team_lead_for_state(state_code)
            logger.info(f"[UPLOAD] Auto-detected: ZIP {zip_code} -> State {state_code} -> Team Lead {team_lead_from_zip}")
    
    # Generate content hash for deduplication
    from app.services.file_deduplication_service import FileDeduplicationService
    file_hash = FileDeduplicationService.generate_content_hash(content)
    
    # Check if file already exists
    existing_file_id = FileDeduplicationService.find_existing_file(
        file_hash, len(content), pdf.filename
    )
    
    if existing_file_id:
        # File already exists, check stage progression rules
        existing_file = db.permit_files.find_one({"file_id": existing_file_id})
        if existing_file:
            # Get stage tracking information
            stage_service = get_stage_tracking_service()
            tracking = stage_service.get_file_tracking(existing_file_id)
            
            # Get current stage and status
            current_stage = "UNKNOWN"
            current_status = "UNKNOWN"
            
            if tracking:
                if isinstance(tracking, dict):
                    raw_stage = tracking.get('current_stage')
                    current_stage = str(raw_stage) if raw_stage is not None else "UNKNOWN"
                    raw_status = tracking.get('current_status')
                    current_status = str(raw_status) if raw_status is not None else "UNKNOWN"
                else:
                    current_stage = tracking.current_stage.value if hasattr(tracking.current_stage, 'value') else str(tracking.current_stage)
                    current_status = tracking.current_status
            
            # Check if trying to re-upload in the same stage
            if workflow_step.upper() == current_stage.upper():
                return {
                    "success": False,
                    "message": f"File '{pdf.filename}' has already been processed in {workflow_step} stage",
                    "duplicate": True,
                    "stage_conflict": True,
                    "existing_file": {
                        "file_id": existing_file_id,
                        "original_filename": existing_file.get("file_info", {}).get("original_filename", pdf.filename),
                        "current_stage": current_stage,
                        "current_status": current_status,
                        "completed_stages": _get_completed_stages(tracking),
                        "message": f"This file has already passed the {workflow_step} stage"
                    },
                    "suggestion": f"File is ready for next stage. Current stage: {current_stage}"
                }
            
            # Check stage progression order
            stage_order = ["PRELIMS", "PRODUCTION", "QC"]
            current_stage_index = stage_order.index(current_stage.upper()) if current_stage.upper() in stage_order else -1
            requested_stage_index = stage_order.index(workflow_step.upper()) if workflow_step.upper() in stage_order else -1
            
            # Prevent skipping stages or going backwards
            if requested_stage_index <= current_stage_index:
                next_stage = stage_order[current_stage_index + 1] if current_stage_index + 1 < len(stage_order) else "COMPLETED"
                return {
                    "success": False,
                    "message": f"Invalid stage progression for file '{pdf.filename}'",
                    "duplicate": True,
                    "stage_conflict": True,
                    "existing_file": {
                        "file_id": existing_file_id,
                        "current_stage": current_stage,
                        "current_status": current_status,
                        "completed_stages": _get_completed_stages(tracking)
                    },
                    "suggestion": f"File should progress to: {next_stage}. Cannot re-upload to: {workflow_step}"
                }
            
            # Valid stage progression - show lifecycle info
            tasks_count = db.tasks.count_documents({"source.permit_file_id": existing_file_id})
            
            # Prepare detailed stage history with time tracking
            stage_history = []
            if tracking:
                stage_history_data = None
                if isinstance(tracking, dict):
                    stage_history_data = tracking.get('stage_history')
                else:
                    stage_history_data = getattr(tracking, 'stage_history', None)
                if stage_history_data:
                    for history_entry in stage_history_data:
                        if isinstance(history_entry, dict):
                            stage_history.append({
                                "stage": history_entry.get("stage"),
                                "status": history_entry.get("stage_status"),
                                "employee": history_entry.get("employee_code"),
                                "employee_name": history_entry.get("employee_name"),
                                "started_at": history_entry.get("started_stage_at"),
                                "completed_at": history_entry.get("completed_stage_at"),
                                "duration_minutes": _calculate_stage_duration(history_entry)
                            })
            
            logger.info(f"Stage progression: {pdf.filename} from {current_stage} to {workflow_step}")
            
            return {
                "success": True,
                "stage_progression": True,
                "message": f"File '{pdf.filename}' ready for {workflow_step} stage (continuing from {current_stage})",
                "file_id": existing_file_id,
                "existing_file": {
                    "file_id": existing_file_id,
                    "original_filename": existing_file.get("file_info", {}).get("original_filename", pdf.filename),
                    "uploaded_at": existing_file.get("file_info", {}).get("uploaded_at"),
                    "current_stage": current_stage,
                    "current_status": current_status,
                    "total_tasks": tasks_count,
                    "stage_history": stage_history,
                    "project_details": existing_file.get("project_details", {}),
                    "completed_stages": _get_completed_stages(tracking)
                },
                "next_stage_info": {
                    "from_stage": current_stage,
                    "to_stage": workflow_step,
                    "ready_for_progression": True,
                    "total_time_so_far": _calculate_total_time(tracking)
                }
            }
    
    # Generate file ID only if file doesn't exist
    file_id = generate_file_id()
    
    # Save file to disk
    file_path = os.path.join(UPLOAD_DIR, f"{file_id}_{pdf.filename}")
    with open(file_path, "wb") as f:
        f.write(content)
    
    # Set initial status based on workflow step
    initial_status = {
        "PRELIMS": "IN_PRELIMS",
        "PRODUCTION": "IN_PRODUCTION", 
        "QC": "IN_QC"
    }.get(workflow_step, "PENDING_REVIEW")
    
    # Create permit file document
    permit_file = {
        "file_id": file_id,
        "file_hash": file_hash,  # Add file hash for deduplication
        "file_info": {
            "original_filename": pdf.filename,
            "stored_filename": f"{file_id}_{pdf.filename}",
            "file_path": file_path,
            "file_size": len(content),
            "mime_type": pdf.content_type,
            "uploaded_at": datetime.utcnow()
        },
        "project_details": {
            "client_name": client_name,
            "project_name": project_name or "Unnamed Project",
            "zip_code": zip_code,  # Add auto-detected ZIP
            "state": state_code,   # Add resolved state
            "team_lead": team_lead_from_zip  # Add auto-detected team lead
        },
        "detected_zip": zip_code,  # For compatibility with zip_assign.py
        "detected_state": state_code,
        "locked_team_lead": team_lead_from_zip,
        "assigned_to_lead": assigned_to_lead,
        "workflow_step": workflow_step,  # Store the workflow step
        "status": initial_status,  # Set status based on workflow step
        "assignment": {
            "assigned_to": assigned_to_lead,
            "assigned_at": datetime.utcnow(),
            "assigned_for_stage": workflow_step,
            "assigned_by": assigned_to_lead  # Uploader is also the assigner for now
        },
        "acceptance": {
            "accepted_by": None,
            "accepted_at": None,
            "rejection_reason": None
        },
        "tasks_created": [],
        "metadata": {
            "uploaded_by": assigned_to_lead,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
    }
    
    # Insert into MongoDB
    db.permit_files.insert_one(permit_file)
    
    # Write to file_lifecycle fact table and emit comprehensive lifecycle events
    try:
        from app.services.clickhouse_service import clickhouse_service
        from app.services.clickhouse_lifecycle_service import clickhouse_lifecycle_service
        from datetime import timedelta
        uploaded_at = datetime.utcnow()
        sla_deadline = uploaded_at + timedelta(days=7)
        
        # Keep existing fact table insert for backward compatibility
        clickhouse_service.client.execute(
            'INSERT INTO file_lifecycle (file_id, uploaded_at, delivered_at, sla_deadline, current_status, current_stage) VALUES',
            [(file_id, uploaded_at, None, sla_deadline, initial_status, workflow_step)]
        )
        
        # Emit comprehensive lifecycle event for file upload
        clickhouse_lifecycle_service.emit_file_lifecycle_event(
            file_id=file_id,
            event_type='FILE_UPLOADED',
            stage=workflow_step,
            employee_code=assigned_to_lead,
            employee_name=assigned_to_lead,  # Could be enhanced to fetch actual name
            event_data={
                'original_filename': pdf.filename,
                'file_size': len(content),
                'client_name': client_name,
                'project_name': project_name,
                'uploaded_at': uploaded_at.isoformat(),
                'mime_type': pdf.content_type
            }
        )
        
        logger.info(f"✅ Inserted file_lifecycle record and emitted lifecycle event for {file_id} with stage {workflow_step}")
    except Exception as e:
        logger.error(f"Failed to insert file_lifecycle or emit lifecycle event: {e}")
    
    # Emit file creation event to ClickHouse for real-time analytics (backward compatibility)
    try:
        from app.services.clickhouse_service import clickhouse_service
        await clickhouse_service.emit_file_created_event(
            file_id=file_id,
            file_name=pdf.filename,
            uploaded_by=assigned_to_lead
        )
    except Exception as e:
        logger.warning(f"Failed to emit file_created event: {e}")
    
    # Initialize stage tracking for the file
    try:
        logger.info(f"🔍 Starting stage tracking initialization for {file_id}")
        stage_service = get_stage_tracking_service()
        logger.info(f"✅ Got stage tracking service")
        
        # Map workflow_step to FileStage
        stage_mapping = {
            "PRELIMS": FileStage.PRELIMS,
            "PRODUCTION": FileStage.PRODUCTION,
            "QC": FileStage.QC
        }
        initial_stage = stage_mapping.get(workflow_step, FileStage.PRELIMS)
        logger.info(f"📍 Mapped workflow_step {workflow_step} to FileStage {initial_stage}")
        
        tracking = stage_service.initialize_file_tracking(file_id, initial_stage)
        logger.info(f"✅ Successfully initialized stage tracking for {file_id} at stage {initial_stage}")
    except Exception as e:
        # Log error but don't fail the upload
        import traceback
        logger.error(f"❌ Failed to initialize stage tracking for {file_id}: {str(e)}")
        logger.error(f"❌ Error type: {type(e).__name__}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
    
    return {
        "file_id": file_id,
        "workflow_step": workflow_step,
        "status": initial_status,
        "message": f"Permit file uploaded successfully for {workflow_step} workflow"
    }


@router.post("/smart-upload-assign")
async def smart_upload_and_assign(
    pdf: UploadFile = File(...),
    task_description: str = Form(...),
    assigned_by: Optional[str] = Form(None),
):
    """Single smart entry point for Smart Recommender (system-driven team lead selection)."""
    try:
        db = get_db()

        pdf_bytes = await pdf.read()
        if not pdf_bytes:
            raise HTTPException(status_code=400, detail="Empty PDF")

        file_hash = hashlib.sha256(pdf_bytes).hexdigest()
        assigned_by_final = (assigned_by or "").strip() or "SYSTEM"

        stage_service = get_stage_tracking_service()

        # Enhanced file detection: check by hash first, then by filename
        from app.services.file_deduplication_service import FileDeduplicationService
        existing_file_id = FileDeduplicationService.find_existing_file(
            file_hash, len(pdf_bytes), pdf.filename
        )
        
        existing = None
        if existing_file_id:
            existing = db.permit_files.find_one({"file_id": existing_file_id}, {"_id": 0})
        
        if existing:
            file_id = existing.get("file_id")
            if not file_id:
                raise HTTPException(status_code=500, detail="Existing file record missing file_id")
            
            # Check if content is different (same filename, updated content)
            if existing.get("file_hash") != file_hash:
                # Track as new version
                upload_info = {
                    'uploaded_at': datetime.utcnow(),
                    'uploaded_by': assigned_by_final,
                    'file_size': len(pdf_bytes),
                    'change_reason': 'File updated via smart upload'
                }
                FileDeduplicationService.track_file_version(file_id, file_hash, upload_info)
                
                # Update the file hash
                db.permit_files.update_one(
                    {'file_id': file_id},
                    {'$set': {'file_hash': file_hash}}
                )
                logger.info(f"Tracked new version for file {file_id} (same filename, different content)")

            locked_lead = existing.get("locked_team_lead") or existing.get("assigned_to_lead")

            # Employee selection: use current stage assignment if exists, else select under locked lead
            tracking = stage_service.get_file_tracking(file_id)
            employee_code = None
            employee_name = None

            if isinstance(tracking, dict):
                current_assignment = tracking.get("current_assignment") or {}
                if isinstance(current_assignment, dict):
                    employee_code = current_assignment.get("employee_code")
                    employee_name = current_assignment.get("employee_name")

            if locked_lead and not employee_code:
                engine = get_recommendation_engine()
                
                # Get current file stage for context
                current_file_stage = None
                try:
                    tracking = stage_service.get_file_tracking(file_id)
                    if tracking:
                        current_file_stage = tracking.current_stage.value
                except Exception as e:
                    logger.warning(f"Failed to get file stage for {file_id}: {e}")
                
                recs = engine.get_recommendations(
                    task_description=task_description,
                    team_lead_code=locked_lead,
                    top_k=1,
                    min_score=0.3,
                    file_id=file_id,
                    current_file_stage=current_file_stage
                )
                if recs:
                    employee_code = recs[0].employee_code
                    employee_name = recs[0].employee_name
                    try:
                        stage_service.assign_employee_to_stage(
                            file_id=file_id,
                            employee_code=employee_code,
                            employee_name=employee_name,
                            notes="Auto-assigned (resume)",
                        )
                    except Exception as e:
                        logger.warning(f"Failed to assign employee to stage: {e}")
            
            return {
                "success": True,
                "resumed": True,
                "message": f"File '{existing.get('file_info', {}).get('original_filename', pdf.filename)}' already exists and has been resumed",
                "file_id": file_id,
                "file_tracking": file_tracking.dict() if hasattr(file_tracking, 'dict') else file_tracking,
                "tasks": tasks,
                "permit_file": permit_file,
                "stage_summary": {
                    "current_stage": file_tracking.current_stage.value if hasattr(file_tracking.current_stage, 'value') else str(file_tracking.current_stage),
                    "current_status": file_tracking.current_status,
                    "total_stages": len(file_tracking.stage_history) if hasattr(file_tracking, 'stage_history') else 0,
                    "completed_stages": len([h for h in file_tracking.stage_history if hasattr(h, 'stage_status') and h.stage_status == 'COMPLETED']) if hasattr(file_tracking, 'stage_history') else 0
                },
                "lifecycle_info": {
                    "file_name": existing.get('file_info', {}).get('original_filename', pdf.filename),
                    "uploaded_at": existing.get('file_info', {}).get('uploaded_at'),
                    "current_stage": file_tracking.current_stage.value if hasattr(file_tracking.current_stage, 'value') else str(file_tracking.current_stage),
                    "current_status": file_tracking.current_status,
                    "assigned_employee": employee_name,
                    "total_tasks": len(tasks) if tasks else 0,
                    "project": existing.get('project_details', {}),
                    "next_actions": [
                        f"Current stage: {file_tracking.current_stage.value if hasattr(file_tracking.current_stage, 'value') else str(file_tracking.current_stage)}",
                        f"Status: {file_tracking.current_status}",
                        f"Tasks: {len(tasks) if tasks else 0} task(s) created"
                    ]
                }
            }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in smart upload and assign: {str(e)}")


@router.post("/sequential-workflow")
async def sequential_workflow_upload(
    pdf: UploadFile = File(...),
    task_description: str = Form(...),
    assigned_by: Optional[str] = Form(None),
):
    """
    Sequential workflow upload - handles the exact workflow:
    1. First upload: PRELIMS stage
    2. Same file upload: PRODUCTION stage  
    3. Same file upload: Move from COMPLETED to QC stage
    4. QC completion: Move to DELIVERED
    """
    try:
        db = get_db()

        pdf_bytes = await pdf.read()
        if not pdf_bytes:
            raise HTTPException(status_code=400, detail="Empty PDF")

        file_hash = hashlib.sha256(pdf_bytes).hexdigest()
        assigned_by_final = (assigned_by or "").strip() or "SYSTEM"

        # Enhanced file detection: check by hash first, then by filename
        from app.services.file_deduplication_service import FileDeduplicationService
        existing_file_id = FileDeduplicationService.find_existing_file(
            file_hash, len(pdf_bytes), pdf.filename
        )
        
        existing = None
        if existing_file_id:
            existing = db.permit_files.find_one({"file_id": existing_file_id}, {"_id": 0})
        
        if not existing:
            # FIRST UPLOAD - Create new file and start PRELIMS
            file_id = generate_file_id()
            
            # Save file
            filename = f"{file_id}.pdf"
            file_path = os.path.join(UPLOAD_DIR, filename)
            with open(file_path, "wb") as f:
                f.write(pdf_bytes)

            # Create permit file record
            permit_file = {
                "file_id": file_id,
                "file_hash": file_hash,
                "file_info": {
                    "original_filename": pdf.filename,
                    "file_path": file_path,
                    "file_size": len(pdf_bytes),
                    "mime_type": pdf.content_type,
                    "uploaded_at": datetime.utcnow()
                },
                "status": "IN_PRELIMS",
                "workflow_step": "PRELIMS",
                "locked_team_lead": assigned_by_final,
                "tasks_created": [],
                "metadata": {
                    "uploaded_by": assigned_by_final,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }
            }
            
            db.permit_files.insert_one(permit_file)
            
            # Initialize stage tracking
            stage_service = get_stage_tracking_service()
            stage_service.initialize_file_tracking(file_id, FileStage.PRELIMS)
            
            # Create PRELIMS task
            task_data = TaskCreate(
                title=f"PRELIMS: {pdf.filename}",
                description=task_description,
                permit_file_id=file_id,
                stage="PRELIMS",
                priority="MEDIUM"
            )
            
            task = await create_task(task_data, assigned_by_final)
            
            return {
                "success": True,
                "action": "PRELIMS_STARTED",
                "file_id": file_id,
                "message": f"File uploaded and PRELIMS task created",
                "stage": "PRELIMS",
                "task_id": task.get("task_id") if task else None
            }
        
        else:
            # EXISTING FILE - Handle sequential progression
            file_id = existing.get("file_id")
            stage_service = get_stage_tracking_service()
            tracking = stage_service.get_file_tracking(file_id)
            
            if not tracking:
                raise HTTPException(status_code=500, detail="File tracking not found")
            
            current_stage = tracking.current_stage.value
            
            if current_stage == "PRELIMS":
                # Move to PRODUCTION
                if tracking.current_status != "COMPLETED":
                    raise HTTPException(
                        status_code=400, 
                        detail="PRELIMS stage must be completed before moving to PRODUCTION"
                    )
                
                # Transition to PRODUCTION
                stage_service.transition_to_next_stage(file_id, assigned_by_final, FileStage.PRODUCTION)
                
                # Create PRODUCTION task
                task_data = TaskCreate(
                    title=f"PRODUCTION: {existing.get('file_info', {}).get('original_filename', pdf.filename)}",
                    description=task_description,
                    permit_file_id=file_id,
                    stage="PRODUCTION",
                    priority="HIGH"
                )
                
                task = await create_task(task_data, assigned_by_final)
                
                return {
                    "success": True,
                    "action": "PRODUCTION_STARTED",
                    "file_id": file_id,
                    "message": f"File moved to PRODUCTION and task created",
                    "stage": "PRODUCTION",
                    "task_id": task.get("task_id") if task else None
                }
                
            elif current_stage == "PRODUCTION":
                # Move to COMPLETED (automatic when PRODUCTION tasks complete)
                if tracking.current_status != "COMPLETED":
                    raise HTTPException(
                        status_code=400,
                        detail="PRODUCTION stage must be completed before moving to COMPLETED"
                    )
                
                return {
                    "success": True,
                    "action": "ALREADY_COMPLETED",
                    "file_id": file_id,
                    "message": f"File is already in COMPLETED stage. Ready for QC.",
                    "stage": "COMPLETED"
                }
                
            elif current_stage == "COMPLETED":
                # Move to QC stage
                stage_service.transition_to_next_stage(file_id, assigned_by_final, FileStage.QC)
                
                # Create QC task
                task_data = TaskCreate(
                    title=f"QC: {existing.get('file_info', {}).get('original_filename', pdf.filename)}",
                    description=task_description,
                    permit_file_id=file_id,
                    stage="QC",
                    priority="HIGH"
                )
                
                task = await create_task(task_data, assigned_by_final)
                
                return {
                    "success": True,
                    "action": "QC_STARTED",
                    "file_id": file_id,
                    "message": f"File moved to QC stage and task created",
                    "stage": "QC",
                    "task_id": task.get("task_id") if task else None
                }
                
            elif current_stage == "QC":
                # Move to DELIVERED (automatic when QC tasks complete)
                if tracking.current_status != "COMPLETED":
                    raise HTTPException(
                        status_code=400,
                        detail="QC stage must be completed before moving to DELIVERED"
                    )
                
                return {
                    "success": True,
                    "action": "ALREADY_DELIVERED",
                    "file_id": file_id,
                    "message": f"File is already in DELIVERED stage",
                    "stage": "DELIVERED"
                }
                
            elif current_stage == "DELIVERED":
                return {
                    "success": True,
                    "action": "WORKFLOW_COMPLETE",
                    "file_id": file_id,
                    "message": f"File workflow is complete (DELIVERED)",
                    "stage": "DELIVERED"
                }
                
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown stage: {current_stage}"
                )
        
    except Exception as e:
        logger.error(f"Error in sequential workflow: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error in sequential workflow: {str(e)}")


@router.post("/cleanup-duplicates")
async def cleanup_duplicate_files():
    """Clean up all duplicate files in the system"""
    try:
        from app.services.file_deduplication_service import FileDeduplicationService
        
        # Get statistics before cleanup
        before_stats = FileDeduplicationService.get_file_statistics()
        
        # Perform cleanup
        cleaned_groups = FileDeduplicationService.cleanup_all_duplicates()
        
        # Get statistics after cleanup
        after_stats = FileDeduplicationService.get_file_statistics()
        
        return {
            "success": True,
            "message": f"Cleaned up {cleaned_groups} duplicate file groups",
            "cleaned_groups": cleaned_groups,
            "statistics_before": before_stats,
            "statistics_after": after_stats,
            "files_removed": before_stats['total_duplicates']
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error cleaning up duplicates: {str(e)}")

@router.get("/deduplication-stats")
async def get_deduplication_statistics():
    """Get statistics about file deduplication"""
    try:
        from app.services.file_deduplication_service import FileDeduplicationService
        
        stats = FileDeduplicationService.get_file_statistics()
        
        return {
            "success": True,
            "statistics": stats
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting deduplication statistics: {str(e)}")

class FileAcceptance(BaseModel):
    accepted_by: str
    rejection_reason: Optional[str] = None

@router.post("/{file_id}/accept")
async def accept_permit_file(file_id: str, acceptance: FileAcceptance):
    """Accept a permit file (required before creating tasks)"""
    db = get_db()
    
    # Check if file exists
    file = db.permit_files.find_one({"file_id": file_id})
    if not file:
        raise HTTPException(status_code=404, detail="Permit file not found")
    
    # Update status to ACCEPTED
    db.permit_files.update_one(
        {"file_id": file_id},
        {
            "$set": {
                "status": "ACCEPTED",
                "acceptance.accepted_by": acceptance.accepted_by,
                "acceptance.accepted_at": datetime.utcnow(),
                "metadata.updated_at": datetime.utcnow()
            }
        }
    )
    
    return {
        "file_id": file_id,
        "status": "ACCEPTED",
        "can_assign_tasks": True,
        "message": "Permit file accepted. You can now create tasks."
    }

@router.post("/{file_id}/reject")
async def reject_permit_file(file_id: str, acceptance: FileAcceptance):
    """Reject a permit file"""
    db = get_db()
    
    # Check if file exists
    file = db.permit_files.find_one({"file_id": file_id})
    if not file:
        raise HTTPException(status_code=404, detail="Permit file not found")
    
    # Update status to REJECTED
    db.permit_files.update_one(
        {"file_id": file_id},
        {
            "$set": {
                "status": "REJECTED",
                "acceptance.accepted_by": acceptance.accepted_by,
                "acceptance.accepted_at": datetime.utcnow(),
                "acceptance.rejection_reason": acceptance.rejection_reason,
                "metadata.updated_at": datetime.utcnow()
            }
        }
    )
    
    return {
        "file_id": file_id,
        "status": "REJECTED",
        "message": "Permit file rejected"
    }


@router.get("/{file_id}/completion-report")
async def get_permit_file_completion_report(file_id: str):
    """Get detailed completion report for a permit file including stage timeline and task summary"""
    db = get_db()

    # Check if file exists
    file_doc = db.permit_files.find_one({"file_id": file_id}, {"_id": 0})
    if not file_doc:
        raise HTTPException(status_code=404, detail="Permit file not found")

    # Stage tracking
    stage_service = get_stage_tracking_service()
    tracking = stage_service.get_file_tracking(file_id)

    current_stage = (tracking or {}).get("current_stage") or file_doc.get("workflow_step") or "PRELIMS"
    total_duration_minutes = (tracking or {}).get("total_duration_minutes")

    stage_timeline = []
    total_penalties = 0.0
    total_breaches = 0

    for stage_entry in (tracking or {}).get("stage_history", []) or []:
        assigned_to = stage_entry.get("assigned_to") or {}
        sla_status = (assigned_to.get("sla_status") or {}) if isinstance(assigned_to, dict) else {}
        penalty_points = float(assigned_to.get("penalty_points") or 0.0) if isinstance(assigned_to, dict) else 0.0
        total_penalties += penalty_points

        sla_state = (sla_status.get("status") or "") if isinstance(sla_status, dict) else ""
        if sla_state in ["over_max", "escalation_needed"] or stage_entry.get("sla_breached"):
            total_breaches += 1

        stage_timeline.append({
            "stage": stage_entry.get("stage"),
            "status": stage_entry.get("status"),
            "employee_code": assigned_to.get("employee_code") if isinstance(assigned_to, dict) else None,
            "employee_name": assigned_to.get("employee_name") if isinstance(assigned_to, dict) else None,
            "assigned_at": assigned_to.get("assigned_at") if isinstance(assigned_to, dict) else None,
            "started_at": assigned_to.get("started_at") if isinstance(assigned_to, dict) else None,
            "completed_at": stage_entry.get("completed_stage_at"),
            "duration_minutes": stage_entry.get("total_duration_minutes") or assigned_to.get("duration_minutes") if isinstance(assigned_to, dict) else None,
            "penalty_points": penalty_points,
            "sla_status": sla_status,
        })

    if total_duration_minutes is None:
        # Fallback: sum completed stage durations
        total_duration_minutes = sum([(s.get("duration_minutes") or 0) for s in stage_timeline])

    # Tasks summary
    tasks = list(db.tasks.find({"source.permit_file_id": file_id}, {"_id": 0}))
    total_tasks = len(tasks)
    completed_tasks = len([t for t in tasks if (t.get("status") or "").upper() == "COMPLETED"])
    active_tasks = len([t for t in tasks if (t.get("status") or "").upper() in ["OPEN", "ASSIGNED", "IN_PROGRESS"]])

    by_stage: Dict[str, Any] = {}
    for t in tasks:
        stage = (t.get("stage") or "UNKNOWN")
        stage_key = stage if isinstance(stage, str) else str(stage)
        if stage_key not in by_stage:
            by_stage[stage_key] = {"total": 0, "completed": 0, "employees": {}}

        by_stage[stage_key]["total"] += 1
        if (t.get("status") or "").upper() == "COMPLETED":
            by_stage[stage_key]["completed"] += 1

        emp_code = t.get("assigned_to") or ""
        if emp_code:
            if emp_code not in by_stage[stage_key]["employees"]:
                emp_doc = db.employee.find_one({"employee_code": emp_code}, {"_id": 0, "employee_name": 1})
                by_stage[stage_key]["employees"][emp_code] = {
                    "employee_name": (emp_doc or {}).get("employee_name") or f"Employee {emp_code}",
                    "tasks": []
                }
            by_stage[stage_key]["employees"][emp_code]["tasks"].append({
                "task_id": t.get("task_id"),
                "title": t.get("title"),
                "status": t.get("status"),
            })

    return {
        "file_id": file_id,
        "current_stage": current_stage,
        "total_duration_minutes": int(total_duration_minutes or 0),
        "stage_timeline": stage_timeline,
        "task_summary": {
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "active_tasks": active_tasks,
            "by_stage": by_stage,
        },
        "sla_summary": {
            "total_breaches": total_breaches,
            "total_penalties": round(float(total_penalties), 2),
        },
    }

@router.post("/cleanup-duplicates")
async def cleanup_duplicate_files():
    """Clean up all duplicate files in the system"""
    try:
        from app.services.file_deduplication_service import FileDeduplicationService
        
        # Get statistics before cleanup
        before_stats = FileDeduplicationService.get_file_statistics()
        
        # Perform cleanup
        cleaned_groups = FileDeduplicationService.cleanup_all_duplicates()
        
        # Get statistics after cleanup
        after_stats = FileDeduplicationService.get_file_statistics()
        
        return {
            "success": True,
            "message": f"Cleaned up {cleaned_groups} duplicate file groups",
            "cleaned_groups": cleaned_groups,
            "statistics_before": before_stats,
            "statistics_after": after_stats,
            "files_removed": before_stats['total_duplicates']
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error cleaning up duplicates: {str(e)}")

@router.get("/deduplication-stats")
async def get_deduplication_statistics():
    """Get statistics about file deduplication"""
    try:
        from app.services.file_deduplication_service import FileDeduplicationService
        
        stats = FileDeduplicationService.get_file_statistics()
        
        return {
            "success": True,
            "statistics": stats
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting deduplication statistics: {str(e)}")
