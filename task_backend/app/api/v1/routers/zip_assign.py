"""
ZIP-based Team Lead Assignment Router
Extract ZIP from PDF, map to state, find team lead, assign to any employee under that lead.
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

from app.db.mongodb import get_db
from app.core.settings import settings
from app.models.stage_flow import FileStage
from app.services.stage_tracking_service import get_stage_tracking_service
from app.services.recommendation_engine import get_recommendation_engine
from app.api.v1.routers.tasks import TaskAssign, TaskCreate, assign_task, create_task

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

router = APIRouter(prefix="/permit-files", tags=["zip_assign"])

# Create uploads directory if it doesn't exist
UPLOAD_DIR = settings.uploads_dir
os.makedirs(UPLOAD_DIR, exist_ok=True)

def generate_file_id():
    """Generate unique file ID"""
    return f"PF-{datetime.now().strftime('%Y%m%d')}-{str(uuid.uuid4())[:8].upper()}"

# Team lead to state mapping (from existing permit_files.py)
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

# State to ZIP range mapping
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

class ZipAssignResponse(BaseModel):
    zip: str
    state: Optional[str]
    assigned_employee_id: Optional[str]
    assignment_status: str  # "Assigned | Unassigned | Invalid ZIP"
    reason: str
    team_lead: Optional[str] = None
    task_id: Optional[str] = None
    file_id: Optional[str] = None
    team_lead_code: Optional[str] = None
    team_lead_name: Optional[str] = None
    employee_name: Optional[str] = None

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


def _extract_team_lead_code(team_lead: str) -> Optional[str]:
    if not team_lead:
        return None
    match = re.search(r"\(([^)]+)\)", team_lead)
    return match.group(1).strip() if match else None


def _ocr_first_page_text(pdf_bytes: bytes) -> Optional[str]:
    """Best-effort OCR for first PDF page.

    This only runs if dependencies are available. If not, returns None.
    """
    try:
        import fitz  # type: ignore
    except Exception:
        fitz = None

    try:
        import pytesseract  # type: ignore
        from PIL import Image  # type: ignore
    except Exception:
        pytesseract = None
        Image = None

    if fitz is None or pytesseract is None or Image is None:
        return None

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        if doc.page_count < 1:
            return None
        page = doc.load_page(0)
        # Render at higher resolution for better OCR
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        text = pytesseract.image_to_string(img) or ""
        return text.strip() or None
    except Exception as e:
        logger.warning(f"[ZIP ASSIGN] OCR attempt failed: {e}")
        return None

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
                    logger.info(f"[ZIP ASSIGN] Extracted ZIP from page {i+1}: {zip_code}")
                    return zip_code
        
        # If no ZIP found in individual pages, try searching all pages combined
        normalized = _normalize_extracted_text(all_text)
        logger.info(f"[ZIP ASSIGN] Combined text from {max_pages} pages length: {len(normalized)}")
        logger.info(f"[ZIP ASSIGN] Combined text preview: {normalized[:350]}...")
        
        candidates = _extract_zip_candidates(normalized)
        logger.info(f"[ZIP ASSIGN] ZIP candidates found: {candidates}")
        if not candidates:
            logger.warning("[ZIP ASSIGN] No ZIP candidates found in extracted text")
            return None

        zip_code = candidates[0]
        logger.info(f"[ZIP ASSIGN] Extracted ZIP from combined pages: {zip_code}")
        return zip_code
        
    except Exception as e:
        logger.error(f"[ZIP ASSIGN] PDF parsing failed: {e}")
        return None

def _validate_zip_and_get_state(zip_code: str) -> Optional[str]:
    """Validate ZIP and return state code if valid."""
    zip_int = int(zip_code)
    for state_name, info in US_STATE_ZIP_RANGES.items():
        zip_min = int(info["zip_min"])
        zip_max = int(info["zip_max"])
        if zip_min <= zip_int <= zip_max:
            state_code = info["code"]
            logger.info(f"[ZIP ASSIGN] ZIP {zip_code} -> state {state_name} ({state_code})")
            return state_code
    logger.warning(f"[ZIP ASSIGN] ZIP {zip_code} does not fall in any state range")
    return None

def _choose_team_lead_for_state(state_code: str) -> Optional[str]:
    """Choose team lead for a state."""
    candidates = TEAM_LEAD_STATE_MAP.get(state_code) or []
    if not candidates:
        logger.warning(f"[ZIP ASSIGN] No team lead found for state: {state_code}")
        return None
    # For now, pick first candidate (deterministic by load can be added later)
    chosen = candidates[0]
    logger.info(f"[ZIP ASSIGN] Chosen team lead for {state_code}: {chosen}")
    return chosen

def _pick_any_employee_under_lead(team_lead: str) -> Optional[Dict[str, Any]]:
    """Pick least-loaded employee under a team lead."""
    engine = get_recommendation_engine()
    employees = engine.load_employees(team_lead)
    if not employees:
        return None
    # Sort by active task count, then by experience
    employees.sort(key=lambda e: (e.get("active_task_count", 0), e.get("experience_years", 0)))
    chosen = employees[0]
    logger.info(f"[ZIP ASSIGN] Picked employee {chosen['employee_name']} ({chosen['employee_code']}) under lead {team_lead}")
    return chosen

@router.post("/zip-assign")
async def zip_assign_permit_file(
    pdf: UploadFile = File(...),
    task_description: str = Form(...),
    assigned_by: Optional[str] = Form(None)
) -> ZipAssignResponse:
    """Upload PDF, extract ZIP, map to state, find team lead, assign to any employee under that lead."""
    try:
        db = get_db()
        assigned_by_final = assigned_by or "1030"

        logger.info(f"[ZIP ASSIGN] Received upload: {pdf.filename}, description: {task_description[:50]}...")

        # 1) Extract ZIP from PDF
        pdf_bytes = await pdf.read()
        zip_code = _extract_zip_from_pdf_first_page(pdf_bytes)
        if not zip_code:
            logger.warning("[ZIP ASSIGN] No ZIP found in PDF")
            return ZipAssignResponse(
                zip="",
                state=None,
                assigned_employee_id=None,
                assignment_status="Unassigned",
                reason="No ZIP code found in PDF"
            )

        # 2) Validate ZIP and get state
        state_code = _validate_zip_and_get_state(zip_code)
        if not state_code:
            logger.warning(f"[ZIP ASSIGN] Invalid ZIP: {zip_code}")
            return ZipAssignResponse(
                zip=zip_code,
                state=None,
                assigned_employee_id=None,
                assignment_status="Invalid ZIP",
                reason=f"ZIP {zip_code} does not fall in any US state range"
            )

        # 3) Choose team lead for state
        chosen_lead = _choose_team_lead_for_state(state_code)
        if not chosen_lead:
            logger.error(f"[ZIP ASSIGN] No team lead for state: {state_code}")
            return ZipAssignResponse(
                zip=zip_code,
                state=state_code,
                assigned_employee_id=None,
                assignment_status="Unassigned",
                reason=f"No team lead configured for state {state_code}"
            )

        # 4) Pick any employee under that lead
        chosen_employee = _pick_any_employee_under_lead(chosen_lead)
        if not chosen_employee:
            logger.error(f"[ZIP ASSIGN] No employees under lead: {chosen_lead}")
            return ZipAssignResponse(
                zip=zip_code,
                state=state_code,
                assigned_employee_id=None,
                assignment_status="Unassigned",
                reason=f"No employees found under team lead {chosen_lead}"
            )

        # 5) Generate file hash and check for duplicates
        file_hash = hashlib.sha256(pdf_bytes).hexdigest()
        
        # Check if file already exists (deduplication)
        from app.services.file_deduplication_service import get_file_deduplication_service
        dedup_service = get_file_deduplication_service()
        
        existing_file_id = dedup_service.find_existing_file(file_hash, len(pdf_bytes), pdf.filename)
        
        if existing_file_id:
            # File already exists - use existing file_id
            file_id = existing_file_id
            logger.info(f"[ZIP ASSIGN] Found existing file {file_id} for duplicate upload")
            
            # Update existing permit file with new detection info if needed
            db.permit_files.update_one(
                {"file_id": file_id},
                {"$set": {
                    "detected_zip": zip_code,
                    "detected_state": state_code,
                    "locked_team_lead": chosen_lead,
                    "metadata.updated_at": datetime.utcnow()
                }}
            )
        else:
            # New file - generate new file_id
            file_id = generate_file_id()
            logger.info(f"[ZIP ASSIGN] Creating new file {file_id}")
        
        file_path = os.path.join(UPLOAD_DIR, f"{file_id}_{pdf.filename}")
        
        # Only save file if it's new (avoid duplicate storage)
        if not existing_file_id:
            with open(file_path, "wb") as f:
                f.write(pdf_bytes)

        # Store or update permit file record
        if existing_file_id:
            # Update existing permit file
            db.permit_files.update_one(
                {"file_id": file_id},
                {"$set": {
                    "detected_zip": zip_code,
                    "detected_state": state_code,
                    "locked_team_lead": chosen_lead,
                    "metadata.updated_at": datetime.utcnow()
                }}
            )
            logger.info(f"[ZIP ASSIGN] Updated existing permit file {file_id}")
        else:
            # Create new permit file record
            permit_file = {
                "file_id": file_id,
                "file_hash": file_hash,
                "detected_zip": zip_code,
                "detected_state": state_code,
                "locked_team_lead": chosen_lead,
                "file_info": {
                    "original_filename": pdf.filename,
                    "stored_filename": f"{file_id}_{pdf.filename}",
                    "file_size": len(pdf_bytes),
                    "content_type": pdf.content_type,
                    "uploaded_at": datetime.utcnow(),
                },
                "project_details": {
                    "zip_code": zip_code,
                    "state": state_code,
                    "team_lead": chosen_lead,
                },
                "workflow_step": "PRELIMS",
                "status": "IN_PRELIMS",
                "assignment": {
                    "assigned_to": chosen_lead,
                    "assigned_at": datetime.utcnow(),
                    "assigned_for_stage": "PRELIMS",
                    "assigned_by": assigned_by_final,
                },
                "acceptance": {
                    "accepted_by": None,
                    "accepted_at": None,
                    "status": "PENDING",
                },
                "tasks_created": [],
                "metadata": {
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }
            }
            db.permit_files.insert_one(permit_file)
            logger.info(f"[ZIP ASSIGN] Created new permit file {file_id}")

        # Initialize stage tracking (only for new files)
        if not existing_file_id:
            try:
                stage_service = get_stage_tracking_service()
                stage_service.initialize_file_tracking(file_id=file_id, initial_stage=FileStage.PRELIMS)
                stage_service.assign_employee_to_stage(
                    file_id=file_id,
                    employee_code=chosen_employee["employee_code"],
                    employee_name=chosen_employee["employee_name"],
                    notes=f"Auto-assigned via ZIP {zip_code} -> state {state_code} -> lead {chosen_lead}",
                )
            except Exception as e:
                logger.warning(f"[ZIP ASSIGN] Stage tracking failed: {e}")
        else:
            logger.info(f"[ZIP ASSIGN] Skipping stage tracking for existing file {file_id}")

        # Create task (tasks.py expects TaskCreate with permit_file_id)
        created = await create_task(
            TaskCreate(
                title=task_description.strip(),
                description=task_description.strip(),
                skills_required=[],
                permit_file_id=file_id,
                assigned_by=assigned_by_final,
                due_date=None,
                estimated_hours=None,
                created_from="zip_assign",
                assignment_source="smart"  # Mark as smart recommendation
            )
        )
        task_id = (created or {}).get("task_id")
        if not task_id:
            raise HTTPException(status_code=500, detail="Failed to create task")

        # Assign task to employee (updates task + profile_building + sends websocket notification)
        await assign_task(task_id, TaskAssign(employee_code=chosen_employee["employee_code"], assigned_by=assigned_by_final))

        # Persist audit linkage on permit file
        try:
            db.permit_files.update_one(
                {"file_id": file_id},
                {"$addToSet": {"tasks_created": task_id}, "$set": {"metadata.updated_at": datetime.utcnow()}},
            )
        except Exception:
            pass

        logger.info(f"[ZIP ASSIGN] Task {task_id} created and assigned to {chosen_employee['employee_name']} ({chosen_employee['employee_code']})")

        return ZipAssignResponse(
            zip=zip_code,
            state=state_code,
            assigned_employee_id=chosen_employee["employee_code"],
            assignment_status="Assigned",
            reason=f"ZIP {zip_code} -> state {state_code} -> lead {chosen_lead} -> employee {chosen_employee['employee_name']}",
            team_lead=chosen_lead,
            task_id=task_id,
            file_id=file_id,
            team_lead_code=_extract_team_lead_code(chosen_lead),
            team_lead_name=chosen_lead,
            employee_name=chosen_employee.get("employee_name"),
        )
    except Exception as e:
        logger.error(f"[ZIP ASSIGN] Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"ZIP assignment failed: {str(e)}")
