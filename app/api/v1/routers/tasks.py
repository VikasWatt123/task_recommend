"""
Task Management Router - MongoDB Based with Embeddings
"""
from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import uuid
import logging
import sys
import os

# Add temporal_workflows to path
sys.path.append('/home/user/smart_task_assignee/task_recommend/temporal_workflows')
from app.db.mongodb import get_db
from app.db.mysql import mysql_service

def _code_variants(employee_code: str) -> list:
    """Return list of possible employee code formats (with/without leading zeros)."""
    return list({employee_code, employee_code.lstrip('0') or employee_code, employee_code.zfill(4)})

from app.utils.validation import (
    BusinessRuleValidator,
    AddressValidator,
    FileIdValidator,
    TaskDescriptionValidator,
    validate_and_extract_address_info
)
from app.services.vertex_ai_embeddings import get_embedding_service
from app.services.recommendation_engine import get_recommendation_engine, EmployeeRecommendation
from app.services.stage_assignment_service import StageAssignmentService
from app.services.stage_tracking_service import get_stage_tracking_service, _parse_file_tracking_safely
from app.models.stage_flow import FileStage
import numpy as np
import time

from app.services.cache_service import cached

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

router = APIRouter(prefix="/tasks", tags=["tasks"])

def generate_task_id():
    """Generate unique task ID"""
    return f"TASK-{datetime.now().strftime('%Y%m%d')}-{str(uuid.uuid4())[:8].upper()}"

class TaskCreate(BaseModel):
    title: str
    description: str
    skills_required: Optional[List[str]] = []
    file_id: Optional[str] = None  # Standardized field name (was permit_file_id)
    assigned_by: str
    due_date: Optional[str] = None
    estimated_hours: Optional[float] = None
    created_from: Optional[str] = "manual"
    assignment_source: Optional[str] = "manual"  # "smart" or "manual"

class TaskCreateMySQL(BaseModel):
    """Task creation model with MySQL backward compatibility"""
    title: str
    description: str
    skills_required: Optional[List[str]] = []
    file_id: Optional[str] = None  # MongoDB field
    id: Optional[str] = None  # MySQL field for file_id
    assigned_by: Optional[str] = None
    creatorparentid: Optional[str] = None  # MySQL field for assigned_by
    due_date: Optional[str] = None
    estimated_hours: Optional[float] = None
    created_from: Optional[str] = "manual"
    assignment_source: Optional[str] = "manual"  # "smart" or "manual"
    stage: Optional[str] = None  # MySQL field (will be ignored, auto-detected)

class TaskAssign(BaseModel):
    employee_code: Optional[str] = None      # MongoDB field
    kekaemployeenumber: Optional[str] = None  # MySQL field for employee_code
    assigned_by: Optional[str] = None         # MongoDB field
    creatorparentid: Optional[str] = None     # MySQL field for assigned_by

# Task Recommendation Models
class TaskRecommendationRequest(BaseModel):
    # Original MongoDB fields
    task_description: str
    top_k: Optional[int] = 10
    min_similarity: Optional[float] = 0.5
    permit_file_id: Optional[str] = None
    file_id: Optional[str] = None
    address: Optional[str] = None  # NEW: Address input for ZIP extraction
    priority: Optional[str] = None
    required_skills: Optional[List[str]] = None
    filter_by_availability: Optional[bool] = True
    team_lead_code: Optional[str] = None
    
    # MySQL backward compatibility fields
    id: Optional[str] = None  # MySQL permits.id → maps to permit_file_id
    creatorparentid: Optional[str] = None  # MySQL permits.creatorparentid → maps to assigned_by

class RecommendationResponse(BaseModel):
    recommendations: List[EmployeeRecommendation]
    total_found: int
    query_info: Dict[str, Any]

def resolve_mysql_to_mongodb_fields_for_task_assign(assignment: TaskAssign) -> TaskAssign:
    """
    Backward compatibility function to map MySQL field names to MongoDB field names for task assignment
    MySQL: kekaemployeenumber, creatorparentid
    MongoDB: employee_code, assigned_by
    """
    # Create a copy of the assignment to modify
    assignment_dict = assignment.dict()
    
    # Map MySQL fields to MongoDB fields
    if assignment_dict.get('kekaemployeenumber') and not assignment_dict.get('employee_code'):
        assignment_dict['employee_code'] = assignment_dict['kekaemployeenumber']
        logger.info(f"Mapped MySQL 'kekaemployeenumber' to MongoDB 'employee_code': {assignment_dict['kekaemployeenumber']}")
    
    if assignment_dict.get('creatorparentid') and not assignment_dict.get('assigned_by'):
        assignment_dict['assigned_by'] = assignment_dict['creatorparentid']
        logger.info(f"Mapped MySQL 'creatorparentid' to MongoDB 'assigned_by': {assignment_dict['creatorparentid']}")
    
    # Remove MySQL-only fields that shouldn't be in the final TaskAssign
    assignment_dict.pop('kekaemployeenumber', None)
    assignment_dict.pop('creatorparentid', None)
    
    # Validate that required fields are present
    if not assignment_dict.get('employee_code'):
        raise HTTPException(
            status_code=400,
            detail="employee_code is required. Provide either 'employee_code' (MongoDB) or 'kekaemployeenumber' (MySQL)"
        )
    
    if not assignment_dict.get('assigned_by'):
        raise HTTPException(
            status_code=400,
            detail="assigned_by is required. Provide either 'assigned_by' (MongoDB) or 'creatorparentid' (MySQL)"
        )
    
    return TaskAssign(**assignment_dict)

def resolve_mysql_to_mongodb_fields_for_task_create(task_data: TaskCreateMySQL) -> TaskCreate:
    """
    Backward compatibility function to map MySQL field names to MongoDB field names for task creation
    MySQL: id, creatorparentid
    MongoDB: file_id, assigned_by
    """
    # Create a copy of the request to modify
    task_dict = task_data.dict()
    
    # Map MySQL fields to MongoDB fields
    if task_dict.get('id') and not task_dict.get('file_id'):
        task_dict['file_id'] = task_dict['id']
        logger.info(f"Mapped MySQL 'id' to MongoDB 'file_id': {task_dict['id']}")
    
    if task_dict.get('creatorparentid') and not task_dict.get('assigned_by'):
        task_dict['assigned_by'] = task_dict['creatorparentid']
        logger.info(f"Mapped MySQL 'creatorparentid' to MongoDB 'assigned_by': {task_dict['creatorparentid']}")
    
    # Remove MySQL-only fields that shouldn't be in TaskCreate
    task_dict.pop('id', None)
    task_dict.pop('creatorparentid', None)
    task_dict.pop('stage', None)  # Stage is auto-detected
    
    # Validate that required fields are present
    if not task_dict.get('assigned_by'):
        raise HTTPException(
            status_code=400,
            detail="assigned_by is required. Provide either 'assigned_by' (MongoDB) or 'creatorparentid' (MySQL)"
        )
    
    return TaskCreate(**task_dict)

def resolve_mysql_to_mongodb_fields(request: TaskRecommendationRequest) -> TaskRecommendationRequest:
    """
    Backward compatibility function to map MySQL field names to MongoDB field names
    MySQL: id, address, creatorparentid
    MongoDB: permit_file_id, address, assigned_by
    """
    # Create a copy of the request to modify
    request_dict = request.dict()
    
    # Map MySQL fields to MongoDB fields
    if request_dict.get('id') and not request_dict.get('permit_file_id'):
        request_dict['permit_file_id'] = request_dict['id']
        logger.info(f"Mapped MySQL 'id' to MongoDB 'permit_file_id': {request_dict['id']}")
    
    # address field is same in both systems, no mapping needed
    
    if request_dict.get('creatorparentid'):
        # Store creatorparentid for later use in task creation
        request_dict['mysql_creatorparentid'] = request_dict['creatorparentid']
        logger.info(f"Mapped MySQL 'creatorparentid' for later use: {request_dict['creatorparentid']}")
    
    return TaskRecommendationRequest(**request_dict)

async def fetch_permit_from_mysql(id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch permit file details from MySQL database
    """
    try:
        from app.db.mysql import MySQLService
        mysql_service = MySQLService()
        
        with mysql_service.get_connection() as conn:
            with conn.cursor() as cursor:
                # Query the permit_files table (correct table name from your database)
                cursor.execute("""
                    SELECT id, name, address , postalcode, 
                    FROM permits
                    WHERE id = %s
                """, (id,))
                
                result = cursor.fetchone()
                if result:
                    logger.info(f"Successfully fetched permit {id} from MySQL")
                    return result
                else:
                    logger.warning(f"Permit {id} not found in MySQL permit_files table")
                    return None
                    
    except Exception as e:
        logger.warning(f"MySQL not available for permit fetch: {e}")
        logger.info(f"Continuing without MySQL permit data for ID: {id}")
        return None

@router.get("/assignment-sources")
async def get_assignment_sources():
    """Get tasks with their assignment sources for transparency"""
    db = get_db()
    
    # Get recent tasks with assignment source
    tasks = list(db.tasks.find(
        {"source.assignment_source": {"$exists": True}},
        {
            "task_id": 1,
            "title": 1,
            "assigned_to": 1,
            "assigned_to_name": 1,
            "assigned_by": 1,
            "status": 1,
            "stage": 1,
            "source.assignment_source": 1,
            "source.created_from": 1,
            "created_at": 1,
            "assigned_at": 1
        }
    ).sort("created_at", -1).limit(50))
    
    # Convert ObjectId to string
    for task in tasks:
        task["_id"] = str(task["_id"])
        if "created_at" in task and task["created_at"]:
            task["created_at"] = task["created_at"].isoformat() + 'Z'
        if "assigned_at" in task and task["assigned_at"]:
            task["assigned_at"] = task["assigned_at"].isoformat() + 'Z'
    
    return {
        "success": True,
        "tasks": tasks,
        "summary": {
            "total": len(tasks),
            "smart_assigned": len([t for t in tasks if t.get("source", {}).get("assignment_source") == "smart"]),
            "manual_assigned": len([t for t in tasks if t.get("source", {}).get("assignment_source") == "manual"])
        }
    }


@router.post("/create")
async def create_task(task_data: TaskCreateMySQL):
    """Create a new task with embedding generation and stage detection (MySQL backward compatible)"""
    logger.info(f"[TASK-CREATE-START] Creating task: '{task_data.title[:50]}...', file_id={task_data.id}")
    
    # Apply MySQL to MongoDB field mapping
    resolved_task_data = resolve_mysql_to_mongodb_fields_for_task_create(task_data)
    
    db = get_db()
    
    # Validate file_id against MySQL permits table if provided
    mysql_permit_data = None
    if resolved_task_data.file_id:
        logger.info(f"[DEBUG] Validating file_id {resolved_task_data.file_id} against MySQL permits table")
        mysql_permit_data = mysql_service.get_permit_by_id(resolved_task_data.file_id)
        if not mysql_permit_data:
            logger.warning(f"[DEBUG] File ID {resolved_task_data.file_id} not found in MySQL permits table, proceeding anyway")
        else:
            logger.info(f"[DEBUG] Validated file_id {resolved_task_data.file_id} exists in MySQL permits table")
    
    # Generate task ID
    task_id = generate_task_id()
    
    # Detect stage from task description with context
    task_text = f"{resolved_task_data.title}. {resolved_task_data.description}"
    logger.info(f"[DEBUG] Creating task with description: {task_text}")
    
    # Determine tracking mode based on file_id presence
    tracking_mode = "FILE_BASED" if resolved_task_data.file_id else "STANDALONE"
    logger.info(f"[TASK-CREATE-MODE] Tracking mode: {tracking_mode}")
    
    # Use context-aware stage detection if file_id is provided
    if resolved_task_data.file_id:
        detected_stage = StageAssignmentService.detect_stage_from_description_with_context(
            task_text, resolved_task_data.file_id
        )
        logger.info(f"[DEBUG] Stage detection with context - File: {resolved_task_data.file_id}, Result: {detected_stage}")
    else:
        detected_stage = StageAssignmentService.detect_stage_from_description(task_text)
        logger.info(f"[TASK-CREATE-STAGE] Detected stage: {detected_stage}")
    
    # Validate stage transition if file_id is provided
    validation_error = None
    validation_warning = None
    
    # Handle case where stage detection returns None (file is COMPLETED)
    if resolved_task_data.file_id and not detected_stage:
        # Check if file is in COMPLETED stage
        file_tracking = db.file_tracking.find_one({'file_id': resolved_task_data.file_id})
        if file_tracking and file_tracking.get('current_stage') == 'COMPLETED':
            raise HTTPException(
                status_code=400,
                detail=f"File {resolved_task_data.file_id} is in COMPLETED stage. Manager must move file to QC stage before assigning tasks. Use /api/v1/stage-tracking/move-to-qc/{resolved_task_data.file_id}"
            )
    
    if resolved_task_data.file_id and detected_stage:
        is_valid, error_msg = StageAssignmentService.check_stage_transition_validity(
            resolved_task_data.file_id, detected_stage
        )
        if not is_valid:
            validation_error = error_msg
            logger.warning(f"Stage validation failed for file {resolved_task_data.file_id}: {error_msg}")
            
            # PRELIMS duplicate is a hard block — file already passed that stage
            if detected_stage == FileStage.PRELIMS:
                raise HTTPException(
                    status_code=400,
                    detail=f"File {resolved_task_data.file_id} has already passed PRELIMS stage. Cannot create another PRELIMS task for this file."
                )
            # Other stage order violations are warnings (allow creation but warn)
            elif detected_stage == FileStage.PRODUCTION:
                validation_warning = "This file has not completed its PRELIMS stage. Complete the PRELIMS stage before moving to PRODUCTION."
            elif detected_stage == FileStage.QC:
                validation_warning = "This file has not completed its PRODUCTION stage. Complete the PRODUCTION stage before moving to QUALITY."
            elif detected_stage == FileStage.DELIVERED:
                validation_warning = "This file has not completed its QUALITY stage. Complete the QUALITY stage before marking as DELIVERED."
    
    # Generate embedding for task description
    embedding_service = get_embedding_service()
    task_embedding = embedding_service.generate_embedding(task_text)
    
    # Determine SLA eligibility (only for file-based tracking)
    sla_applicable = bool(resolved_task_data.file_id and resolved_task_data.file_id.strip())
    
    # Create task document
    task = {
        "task_id": task_id,
        "title": resolved_task_data.title,
        "description": resolved_task_data.description,
        "skills_required": resolved_task_data.skills_required,
        "source": {
            "permit_file_id": resolved_task_data.file_id,  # Keep for backward compatibility
            "created_from": resolved_task_data.created_from or "manual",
            "assignment_source": resolved_task_data.assignment_source or "manual"  # Track if smart or manual
        },
        "assigned_by": resolved_task_data.assigned_by,
        "assigned_to": None,
        "status": "OPEN",
        "due_date": resolved_task_data.due_date,
        "estimated_hours": resolved_task_data.estimated_hours,
        "stage": detected_stage.value if detected_stage else None,
        "sla_applicable": sla_applicable,
        "file_id": resolved_task_data.file_id,  # Standardized field
        "tracking_mode": tracking_mode,  # FILE_BASED or STANDALONE
        "stage_validation": {
            "detected_stage": detected_stage.value if detected_stage else None,
            "validation_error": validation_error,
            "validated_at": datetime.now(timezone.utc) if validation_error else None
        },
        "embeddings": {
            "description_embedding": task_embedding,
            "embedded_text": task_text,
            "model": "text-embedding-004",
            "dimension": len(task_embedding),
            "created_at": datetime.now(timezone.utc)
        },
        "metadata": {
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
    }
    
    # Insert into MongoDB
    db.tasks.insert_one(task)
    logger.info(f"[TASK-CREATE-SUCCESS] Task created: {task_id}, mode={tracking_mode}, stage={detected_stage}")
    
    # Initialize file_tracking if this is the first task for this file
    # This ensures check_stage_transition_validity has data for subsequent tasks
    if resolved_task_data.file_id and detected_stage:
        try:
            from app.services.stage_tracking_service import get_stage_tracking_service
            stage_service = get_stage_tracking_service()
            existing_tracking = stage_service.get_file_tracking(resolved_task_data.file_id)
            if not existing_tracking:
                stage_service.initialize_file_tracking(resolved_task_data.file_id, detected_stage)
                logger.info(f"[STAGE-TRACKING] Initialized file_tracking for {resolved_task_data.file_id} at {detected_stage}")
        except Exception as ft_err:
            logger.warning(f"[STAGE-TRACKING-WARN] Could not initialize file_tracking: {ft_err}")
    
    # Ensure permit_files record exists in MongoDB when file_id is provided
    # This makes the file visible on the Permit Files page with correct name from MySQL
    if resolved_task_data.file_id:
        try:
            existing_pf = db.permit_files.find_one({"file_id": resolved_task_data.file_id})
            if not existing_pf:
                # Build permit_files doc from MySQL data if available, else minimal stub
                permit_name = resolved_task_data.file_id
                permit_address = ""
                if mysql_permit_data:
                    permit_name = str(mysql_permit_data.get("fullname") or mysql_permit_data.get("name") or mysql_permit_data.get("file_name") or resolved_task_data.file_id)
                    permit_address = str(mysql_permit_data.get("address") or "")
                
                pf_doc = {
                    "file_id": resolved_task_data.file_id,
                    "mysql_id": str(resolved_task_data.file_id),
                    "file_name": permit_name,
                    "file_info": {
                        "original_filename": permit_name,
                        "uploaded_at": datetime.now(timezone.utc).isoformat()
                    },
                    "project_details": {
                        "client_name": permit_name,
                        "project_name": detected_stage.value if detected_stage else "PRELIMS",
                    },
                    "address": permit_address,
                    "status": "IN_PRELIMS",
                    "workflow_step": detected_stage.value if detected_stage else "PRELIMS",
                    "assigned_to_lead": None,
                    "current_stage": detected_stage.value if detected_stage else "PRELIMS",
                    "acceptance": {
                        "accepted_by": None,
                        "accepted_at": None
                    },
                    "metadata": {
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                        "uploaded_by": resolved_task_data.assigned_by or "system",
                        "source": "mysql_task_create",
                    },
                    "updated_at": datetime.now(timezone.utc),
                }
                db.permit_files.insert_one(pf_doc)
                logger.info(f"[PERMIT-FILE-CREATED] Auto-created permit_files record for file_id={resolved_task_data.file_id} name='{permit_name}'")
            else:
                logger.info(f"[PERMIT-FILE-EXISTS] permit_files record already exists for file_id={resolved_task_data.file_id}")
        except Exception as pf_err:
            logger.warning(f"[PERMIT-FILE-WARN] Could not ensure permit_files record: {pf_err}")
    
    response = {
        "task_id": task_id,
        "status": "OPEN",
        "message": "Task created successfully with embedding",
        "tracking_mode": tracking_mode,
        "detected_stage": detected_stage.value if detected_stage else None,
        "validation_warning": validation_warning,
        "mysql_fields_mapped": {
            "id": task_data.id,
            "creatorparentid": task_data.creatorparentid,
            "mapped_to": {
                "file_id": resolved_task_data.file_id,
                "assigned_by": resolved_task_data.assigned_by
            }
        }
    }
    
    return response

@router.post("/recommend", response_model=RecommendationResponse)
async def get_task_recommendations(request: TaskRecommendationRequest) -> RecommendationResponse:
    """
    Get AI-powered employee recommendations for a task using Vertex AI Gemini embeddings
    
    Enhanced with MySQL backward compatibility:
    - Accepts MySQL field names: id, address, creatorparentid
    - Auto-fetches permit details from MySQL permits table
    - Maps to MongoDB field names for processing
    
    Performance improvements:
    - Parallel data fetching (embedding generation + employee data)
    - Smart caching (5-minute TTL for employee data)
    - Vectorized similarity computation
    - Batch operations
    
    This endpoint:
    1. Maps MySQL fields to MongoDB fields (backward compatibility)
    2. Fetches permit details from MySQL if MySQL ID provided
    3. Generates embeddings for the task description using Gemini (parallel)
    4. Loads employee data with caching (parallel)
    5. Computes vectorized similarities
    6. Returns top matching employees with similarity scores
    """
    start_time = time.time()
    logger.info(f"[RECOMMEND-START] Task recommendation request received: task_description='{request.task_description[:50]}...', address='{request.address}', file_id={request.file_id or request.permit_file_id}")
    
    try:
        # Step 0: Validate request inputs
        logger.info("[RECOMMEND-VALIDATION] Validating inputs: task_description, address, file_id")
        validation_result = BusinessRuleValidator.validate_task_assignment_request(
            task_description=request.task_description,
            address=request.address,
            file_id=request.file_id or request.permit_file_id,
            team_lead_code=request.team_lead_code
        )
        
        # Log validation warnings
        if validation_result.warnings:
            for warning in validation_result.warnings:
                logger.warning(f"[VALIDATION] {warning}")
        
        # Return error if validation failed
        if not validation_result.is_valid:
            logger.error(f"[VALIDATION] Request validation failed: {validation_result.error_message}")
            raise HTTPException(
                status_code=400,
                detail=f"Validation error: {validation_result.error_message}"
            )
        
        # Step 1: Apply MySQL to MongoDB field mapping
        resolved_request = resolve_mysql_to_mongodb_fields(request)
        
        # Step 2: Query MySQL permits table based on input
        permit_data = None
        mysql_file_id = None
        
        # Case 1: File ID provided (unique, no address check needed)
        if resolved_request.permit_file_id:
            logger.info(f"File ID provided: {resolved_request.permit_file_id}, querying MySQL permits table")
            permit_data = mysql_service.get_permit_by_id(resolved_request.permit_file_id)
            if permit_data:
                mysql_file_id = str(permit_data.get('id'))
                logger.info(f"Found permit in MySQL: id={mysql_file_id}, address={permit_data.get('address')}")
            else:
                logger.warning(f"File ID {resolved_request.permit_file_id} not found in MySQL permits table")
        
        # Case 2: Address provided (query MySQL to get file_id)
        elif resolved_request.address:
            logger.info(f"Address provided: {resolved_request.address}, querying MySQL permits table")
            permit_data = mysql_service.get_permit_by_address(resolved_request.address)
            if permit_data:
                mysql_file_id = str(permit_data.get('id'))
                logger.info(f"Found permit by address in MySQL: id={mysql_file_id}")
                # Update resolved_request with the found file_id
                request_dict = resolved_request.dict()
                request_dict['permit_file_id'] = mysql_file_id
                request_dict['file_id'] = mysql_file_id
                resolved_request = TaskRecommendationRequest(**request_dict)
            else:
                logger.info(f"No permit found for address '{resolved_request.address}' in MySQL, proceeding as standalone task")
        
        # If we found permit data in MySQL, enhance the request
        if permit_data:
            # Use address from MySQL if not provided in request
            if not resolved_request.address and permit_data.get('address'):
                request_dict = resolved_request.dict()
                request_dict['address'] = permit_data['address']
                resolved_request = TaskRecommendationRequest(**request_dict)
                logger.info(f"Used address from MySQL: {permit_data['address']}")
        
        engine = get_recommendation_engine()
        
        effective_permit_file_id = resolved_request.permit_file_id or resolved_request.file_id

        # Prepare additional context
        additional_context = {}
        if effective_permit_file_id:
            additional_context['file_id'] = effective_permit_file_id
        if resolved_request.priority:
            additional_context['priority'] = resolved_request.priority
        if resolved_request.required_skills:
            additional_context['required_skills'] = resolved_request.required_skills
        
        # Add MySQL permit data to context if available
        if permit_data:
            additional_context['mysql_permit_data'] = permit_data
        
        # Determine current file stage if permit_file_id is provided
        current_file_stage = None
        resolved_team_lead_code = resolved_request.team_lead_code
        resolved_team_lead_name = None
        resolved_zip = None
        location_source = None
        
        # Handle address input - use real ZIP range mapping from zip_assign.py for manual assignment
        logger.info(f"[DEBUG] Address from request: {resolved_request.address}")
        logger.info(f"[DEBUG] Effective permit file ID: {effective_permit_file_id}")
        
        # Process address for team lead selection if address is provided (regardless of file_id)
        if resolved_request.address:
            logger.info(f"[RECOMMEND-ADDRESS] Processing address for team lead selection: {resolved_request.address}")
            # Validate and extract postal code from address
            address_info = validate_and_extract_address_info(resolved_request.address)
            logger.info(f"[RECOMMEND-ADDRESS-VALIDATION] Address validation result: valid={address_info['is_valid']}, zip={address_info.get('zip_code')}")
            
            if address_info["is_valid"] and address_info["zip_code"]:
                postal_code = address_info["zip_code"]
                logger.info(f"[RECOMMEND-ZIP] Extracted ZIP: {postal_code} from address")
                
                # Log any warnings
                for warning in address_info.get("warnings", []):
                    logger.warning(f"[ADDRESS] {warning}")
                
                # Use real ZIP range and team lead mapping from zip_assign.py
                try:
                    from app.api.v1.routers.zip_assign import US_STATE_ZIP_RANGES, TEAM_LEAD_STATE_MAP, _extract_team_lead_code
                    
                    # Find which state this postal code belongs to
                    found_state = None
                    for state_name, state_info in US_STATE_ZIP_RANGES.items():
                        zip_min = int(state_info["zip_min"])
                        zip_max = int(state_info["zip_max"])
                        if zip_min <= int(postal_code) <= zip_max:
                            found_state = state_info["code"]
                            logger.info(f"[RECOMMEND-STATE] ZIP {postal_code} → State: {state_name} ({found_state})")
                            break
                    
                    if found_state and found_state in TEAM_LEAD_STATE_MAP:
                        # Get team leads for this state
                        team_leads = TEAM_LEAD_STATE_MAP[found_state]
                        if team_leads:
                            # Use the first team lead (you could implement round-robin or load balancing)
                            selected_team_lead = team_leads[0]
                            resolved_team_lead_code = _extract_team_lead_code(selected_team_lead)
                            resolved_team_lead_name = selected_team_lead
                            location_source = "address_zip_range_mapping"
                            logger.info(f"[RECOMMEND-TEAMLEAD] State {found_state} → Team Lead: {selected_team_lead} (code: {resolved_team_lead_code})")
                        else:
                            logger.warning(f"No team leads found for state: {found_state}")
                            # Use default team lead
                            resolved_team_lead_code = "0083"  # Shivam Kumar from your mapping
                            resolved_team_lead_name = "Shivam Kumar (0083)"
                            location_source = "default_team_lead"
                    else:
                        logger.warning(f"No state mapping found for postal code: {postal_code}")
                        # Use default team lead if no mapping found
                        resolved_team_lead_code = "0083"  # Shivam Kumar from your mapping
                        resolved_team_lead_name = "Shivam Kumar (0083)"
                        location_source = "default_team_lead"
                        
                except ImportError as e:
                    logger.error(f"Could not import ZIP mapping: {e}")
                    # Fallback to default team lead
                    resolved_team_lead_code = "0083"
                    resolved_team_lead_name = "Shivam Kumar (0083)"
                    location_source = "fallback_team_lead"
            else:
                logger.warning(f"No postal code found in address: {resolved_request.address}")
                # Use default team lead if no postal code found
                resolved_team_lead_code = "0083"
                resolved_team_lead_name = "Shivam Kumar (0083)"
                location_source = "default_team_lead"
        
        # Handle permit_file_id (existing logic)
        elif effective_permit_file_id:
            try:
                from app.services.stage_tracking_service import get_stage_tracking_service
                stage_service = get_stage_tracking_service()
                tracking = stage_service.get_file_tracking(effective_permit_file_id)
                if tracking:
                    current_file_stage = tracking.current_stage.value
            except Exception as e:
                logger.warning(f"Failed to get file stage for {effective_permit_file_id}: {e}")
            
            # Get resolved team lead from file if not provided
            if not resolved_team_lead_code:
                resolved_team_lead_code = engine._get_team_lead_from_file(effective_permit_file_id)
                if resolved_team_lead_code:
                    resolved_team_lead_name = engine._extract_team_lead_code(resolved_team_lead_code)
                    location_source = "permit_file"
                    logger.info(f"Resolved team lead from file {effective_permit_file_id}: {resolved_team_lead_code} ({resolved_team_lead_name})")
        
        logger.info(f"[RECOMMEND-ENGINE] Calling recommendation engine with team_lead={resolved_team_lead_code}, top_k={request.top_k}")
        recommendations = engine.get_recommendations(
            task_description=resolved_request.task_description,
            top_k=resolved_request.top_k,
            min_score=resolved_request.min_similarity,
            team_lead_code=resolved_team_lead_code,
            file_id=effective_permit_file_id,
            current_file_stage=current_file_stage
        )
        
        processing_time = round((time.time() - start_time) * 1000, 2)  # in milliseconds
        elapsed = time.time() - start_time
        logger.info(f"[RECOMMEND-SUCCESS] Returning {len(recommendations)} recommendations in {elapsed:.2f}s")
        
        # Import the function for response formatting
        from app.api.v1.routers.permit_files import _extract_team_lead_code
        
        return RecommendationResponse(
            recommendations=recommendations,
            total_found=len(recommendations),
            query_info={
                "task_description": resolved_request.task_description,
                "top_k": resolved_request.top_k,
                "min_similarity": resolved_request.min_similarity,
                "filter_by_availability": resolved_request.filter_by_availability,
                "team_lead_code": _extract_team_lead_code(resolved_team_lead_code) if resolved_team_lead_code else None,
                "team_lead_name": resolved_team_lead_name,
                "location_source": location_source,
                "resolved_zip": resolved_zip,
                "location_filter_applied": bool(resolved_team_lead_code),
                "embedding_model": "text-embedding-004 (Vertex AI Gemini)",
                "processing_time_ms": processing_time,
                "optimization": "parallel_execution + caching + vectorized_computation",
                "validation_warnings": validation_result.warnings if validation_result.warnings else [],
                "mysql_integration": {
                    "enabled": True,
                    "mysql_id_used": bool(request.id),
                    "mysql_permit_fetched": permit_data is not None,
                    "mysql_creatorparentid": request.creatorparentid
                }
            }
        )
    
    except Exception as e:
        logger.error(f"[RECOMMEND-ERROR] Failed to get recommendations: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{task_id}/assign")
async def assign_task(task_id: str, assignment: TaskAssign):
    """Assign task to employee and update profile_building (MySQL backward compatible)"""
    # Apply MySQL to MongoDB field mapping
    resolved_assignment = resolve_mysql_to_mongodb_fields_for_task_assign(assignment)
    
    db = get_db()
    
    try:
        # Get the task to check its file_id before assigning
        task_to_assign = db.tasks.find_one({"task_id": task_id}, {"file_id": 1, "title": 1, "stage": 1})
        if not task_to_assign:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        
        file_id = task_to_assign.get("file_id")
        duplicate_warning = None
        
        # Check for duplicate: same file_id already has an active task assigned
        if file_id:
            existing_active = list(db.tasks.find(
                {
                    "file_id": file_id,
                    "task_id": {"$ne": task_id},
                    "status": {"$in": ["ASSIGNED", "IN_PROGRESS"]},
                    "assigned_to": {"$ne": None}
                },
                {"task_id": 1, "title": 1, "assigned_to": 1, "assigned_to_name": 1, "stage": 1, "status": 1}
            ))
            if existing_active:
                for dup in existing_active:
                    logger.warning(
                        f"[DUPLICATE-ASSIGN-WARNING] File {file_id} already has active task "
                        f"{dup.get('task_id')} (stage={dup.get('stage')}) assigned to "
                        f"{dup.get('assigned_to_name','?')} ({dup.get('assigned_to')})"
                    )
                dup_info = [
                    f"task_id={d.get('task_id')} stage={d.get('stage')} assigned_to={d.get('assigned_to_name','?')} ({d.get('assigned_to')}) status={d.get('status')}"
                    for d in existing_active
                ]
                duplicate_warning = f"File {file_id} already has {len(existing_active)} active task(s): {'; '.join(dup_info)}"
        
        # Fetch employee name for assigned_to_name and ClickHouse
        employee_doc = db.employee.find_one(
            {"$or": [
                {"employee_code": resolved_assignment.employee_code},
                {"kekaemployeenumber": resolved_assignment.employee_code}
            ]},
            {"_id": 0, "employee_name": 1, "reporting_manager": 1, "team_lead": 1}
        )
        employee_name = employee_doc.get("employee_name", "Unknown") if employee_doc else "Unknown"
        
        # Determine team lead from employee document
        team_lead = None
        if employee_doc:
            team_lead = employee_doc.get("team_lead") or employee_doc.get("reporting_manager")
        
        # Update task with assignment details
        update_data = {
            "assigned_to": resolved_assignment.employee_code,
            "assigned_to_name": employee_name,
            "employee_code": resolved_assignment.employee_code,  # Keep for backward compatibility
            "assigned_by": resolved_assignment.assigned_by,
            "assigned_at": datetime.now(timezone.utc),
            "status": "ASSIGNED",
            "metadata.updated_at": datetime.now(timezone.utc)
        }
        
        result = db.tasks.update_one(
            {"task_id": task_id},
            {"$set": update_data}
        )
        
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
            
        # Update permit_files with acceptance and lead data
        if file_id:
            db.permit_files.update_one(
                {"file_id": file_id},
                {
                    "$set": {
                        "acceptance.accepted_by": employee_name,
                        "acceptance.accepted_at": datetime.now(timezone.utc),
                        "assigned_to_lead": team_lead
                    }
                }
            )
        
        # Get task details for profile_building
        task = db.tasks.find_one({"task_id": task_id})
        if not task:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        
        # Add to profile_building collection for employee dashboard
        profile_entry = {
            "employee_code": resolved_assignment.employee_code,
            "task_id": task_id,
            "title": task.get("title", ""),
            "description": task.get("description", ""),
            "assigned_by": resolved_assignment.assigned_by,
            "assigned_at": datetime.now(timezone.utc),
            "status": "ASSIGNED",
            "due_date": task.get("due_date"),
            "estimated_hours": task.get("estimated_hours"),
            "file_id": task.get("file_id"),
            "stage": task.get("stage"),
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        
        db.profile_building.insert_one(profile_entry)
        
        # Register employee with stage tracking so task completion triggers stage progression
        file_id = task.get("file_id")
        task_stage = task.get("stage")
        if file_id and task_stage:
            try:
                from app.services.stage_tracking_service import get_stage_tracking_service
                from app.models.stage_flow import FileStage
                stage_service = get_stage_tracking_service()
                
                # Initialize file tracking if it doesn't exist
                existing_tracking = stage_service.get_file_tracking(file_id)
                if not existing_tracking:
                    try:
                        stage_val = FileStage(task_stage)
                    except Exception:
                        stage_val = FileStage.PRELIMS
                    stage_service.initialize_file_tracking(file_id, stage_val)
                    logger.info(f"[STAGE-TRACKING] Initialized tracking for file {file_id} at stage {stage_val}")
                
                # Ensure tracking status is IN_PROGRESS so assign_employee_to_stage accepts it
                # (initialize_file_tracking sets PENDING; we move it to IN_PROGRESS on first assignment)
                from app.models.file_stage_tracking import FILE_TRACKING_COLLECTION
                db.file_tracking.update_one(
                    {"file_id": file_id, "current_status": {"$in": ["PENDING", "NOT_STARTED"]}},
                    {"$set": {"current_status": "IN_PROGRESS"}}
                )
                
                # Register employee as current assignee for this stage
                stage_service.assign_employee_to_stage(
                    file_id,
                    resolved_assignment.employee_code,
                    employee_name,
                    notes=f"Assigned via task {task_id}"
                )
                logger.info(f"[STAGE-TRACKING] Registered {resolved_assignment.employee_code} for file {file_id} stage {task_stage}")
            except Exception as st_err:
                logger.warning(f"[STAGE-TRACKING-WARN] Could not register employee with stage tracking: {st_err}")
        
        # Emit ClickHouse event if enabled
        try:
            from app.services.clickhouse_service import clickhouse_service
            if clickhouse_service.client:
                # Call async method synchronously using the event loop or create a background task
                loop = asyncio.get_event_loop()
                loop.create_task(clickhouse_service.emit_task_assigned_event(
                    task_id=task_id,
                    employee_code=resolved_assignment.employee_code,
                    employee_name=employee_name,
                    assigned_by=resolved_assignment.assigned_by,
                    file_id_param=task.get("file_id"),
                    tracking_mode=task.get("tracking_mode")
                ))
                logger.info(f"[CLICKHOUSE-EVENT-TASK] Emitting task_assigned event for {task_id}")
        except Exception as e:
            logger.warning(f"Failed to emit ClickHouse event: {e}")
        
        logger.info(f"🎯 TASK ASSIGNED: Task '{task_id}' assigned to employee {resolved_assignment.employee_code} ({employee_name}) by {resolved_assignment.assigned_by}")
        print(f"✅ TASK ASSIGNED: {task_id} → Employee {resolved_assignment.employee_code} ({employee_name})")
        
        return {
            "message": "Task assigned successfully",
            "task_id": task_id,
            "employee_code": resolved_assignment.employee_code,
            "assigned_by": resolved_assignment.assigned_by,
            "assigned_at": datetime.now(timezone.utc).isoformat(),
            "duplicate_warning": duplicate_warning  # non-null if same file had active tasks
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[TASK-ASSIGN-ERROR] Failed to assign task {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to assign task: {str(e)}")


@router.post("/{task_id}/start")
async def start_task(task_id: str, employee_code: str = Query(...)):
    """Start work on a task"""
    logger.info(f"[TASK-START] Employee {employee_code} starting task {task_id}")
    
    try:
        db = get_db()
        
        # Verify task exists and is assigned to this employee
        task = db.tasks.find_one({"task_id": task_id})
        
        if not task:
            logger.warning(f"[TASK-START-WARNING] Task {task_id} not found")
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        
        # Normalize: build a list of possible codes (with and without leading zeros)
        code_variants = list({employee_code, employee_code.lstrip('0') or employee_code, employee_code.zfill(4)})

        if task.get("assigned_to") not in code_variants:
            logger.warning(f"[TASK-START-WARNING] Task {task_id} not assigned to {employee_code}")
            raise HTTPException(status_code=403, detail=f"Task not assigned to employee {employee_code}")
        
        # Update task status to IN_PROGRESS
        update_data = {
            "status": "IN_PROGRESS",
            "started_at": datetime.now(timezone.utc),
            "metadata.updated_at": datetime.now(timezone.utc)
        }
        
        result = db.tasks.update_one(
            {"task_id": task_id},
            {"$set": update_data}
        )
        
        if result.modified_count == 0:
            logger.warning(f"[TASK-START-WARNING] Task {task_id} was not updated")
        
        # Emit ClickHouse event if enabled
        try:
            from app.services.clickhouse_service import clickhouse_service
            if clickhouse_service.client:
                # Call async method using the event loop
                loop = asyncio.get_event_loop()
                loop.create_task(clickhouse_service.emit_stage_started_event(
                    task_id=task_id,
                    employee_code=employee_code,
                    employee_name=employee_code,  # Can't easily get name here without extra DB call, but that's okay
                    stage=task.get("stage", "UNKNOWN"),
                    file_id_param=task.get("file_id"),
                    tracking_mode=task.get("tracking_mode")
                ))
                logger.info(f"[CLICKHOUSE-EVENT-TASK] Emitting task_started event for {task_id}")
        except Exception as e:
            logger.warning(f"Failed to emit ClickHouse event: {e}")
        
        logger.info(f"[TASK-START-SUCCESS] Task {task_id} started by {employee_code}")
        
        return {
            "message": "Task started successfully",
            "task_id": task_id,
            "employee_code": employee_code,
            "status": "IN_PROGRESS",
            "started_at": datetime.now(timezone.utc).isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[TASK-START-ERROR] Failed to start task {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to start task: {str(e)}")


@router.get("/employee/{employee_code}/stats")
async def get_employee_task_stats(employee_code: str):
    """Get task statistics for an employee"""
    logger.info(f"[TASK-STATS-START] Getting task stats for employee: {employee_code}")
    
    try:
        db = get_db()
        
        # Get employee info
        employee = db.employee.find_one({
            "$or": [
                {"employee_code": employee_code},
                {"kekaemployeenumber": employee_code}
            ]
        }, {"_id": 0, "employee_name": 1, "employee_code": 1, "kekaemployeenumber": 1})
        
        if not employee:
            logger.warning(f"[TASK-STATS-WARNING] Employee {employee_code} not found")
            raise HTTPException(status_code=404, detail=f"Employee {employee_code} not found")
        
        # Get task statistics (match with/without leading zeros)
        codes = {"$in": _code_variants(employee_code)}
        total_tasks = db.tasks.count_documents({"assigned_to": codes})
        completed_tasks = db.tasks.count_documents({"assigned_to": codes, "status": "COMPLETED"})
        pending_tasks = db.tasks.count_documents({"assigned_to": codes, "status": {"$ne": "COMPLETED"}})
        
        # Get recent tasks
        recent_tasks = list(db.tasks.find(
            {"assigned_to": codes},
            {"_id": 0, "task_id": 1, "title": 1, "status": 1, "assigned_at": 1, "due_date": 1}
        ).sort("assigned_at", -1).limit(10))
        
        stats = {
            "employee_code": employee_code,
            "employee_name": employee.get("employee_name", f"Employee {employee_code}"),
            "task_statistics": {
                "total_tasks": total_tasks,
                "completed_tasks": completed_tasks,
                "pending_tasks": pending_tasks,
                "completion_rate": round((completed_tasks / total_tasks * 100) if total_tasks > 0 else 0, 2)
            },
            "recent_tasks": recent_tasks,
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"[TASK-STATS-SUCCESS] Retrieved stats for {employee_code}: {total_tasks} total, {completed_tasks} completed")
        return stats
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[TASK-STATS-ERROR] Failed to get stats for {employee_code}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get employee task statistics")


@router.get("/employee/{employee_code}/completed")
async def get_employee_completed_tasks(employee_code: str):
    """Get completed tasks for an employee"""
    logger.info(f"[TASK-COMPLETED-START] Getting completed tasks for employee: {employee_code}")
    
    try:
        db = get_db()
        
        # Get employee info
        employee = db.employee.find_one({
            "$or": [
                {"employee_code": employee_code},
                {"kekaemployeenumber": employee_code}
            ]
        }, {"_id": 0, "employee_name": 1, "employee_code": 1, "kekaemployeenumber": 1})
        
        if not employee:
            logger.warning(f"[TASK-COMPLETED-WARNING] Employee {employee_code} not found")
            raise HTTPException(status_code=404, detail=f"Employee {employee_code} not found")
        
        # Get completed tasks (match with/without leading zeros)
        codes = {"$in": _code_variants(employee_code)}
        completed_tasks = list(db.tasks.find(
            {
                "assigned_to": codes,
                "status": "COMPLETED"
            },
            {
                "_id": 0,
                "task_id": 1,
                "title": 1,
                "description": 1,
                "status": 1,
                "assigned_at": 1,
                "completed_at": 1,
                "due_date": 1,
                "estimated_hours": 1,
                "file_id": 1,
                "stage": 1
            }
        ).sort("completed_at", -1))
        
        # Format datetime fields
        for task in completed_tasks:
            if "assigned_at" in task and task["assigned_at"]:
                task["assigned_at"] = task["assigned_at"].isoformat() + 'Z'
            if "completed_at" in task and task["completed_at"]:
                task["completed_at"] = task["completed_at"].isoformat() + 'Z'
            if "due_date" in task and task["due_date"]:
                task["due_date"] = task["due_date"].isoformat() + 'Z'
        
        result = {
            "employee_code": employee_code,
            "employee_name": employee.get("employee_name", f"Employee {employee_code}"),
            "completed_tasks": completed_tasks,
            "total_completed": len(completed_tasks),
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"[TASK-COMPLETED-SUCCESS] Retrieved {len(completed_tasks)} completed tasks for {employee_code}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[TASK-COMPLETED-ERROR] Failed to get completed tasks for {employee_code}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch completed tasks")


@router.get("/employee/{employee_code}/assigned")
async def get_employee_assigned_tasks(employee_code: str):
    """Get assigned (non-completed) tasks for an employee"""
    logger.info(f"[TASK-ASSIGNED-START] Getting assigned tasks for employee: {employee_code}")
    
    try:
        db = get_db()
        
        # Get employee info
        employee = db.employee.find_one({
            "$or": [
                {"employee_code": employee_code},
                {"kekaemployeenumber": employee_code}
            ]
        }, {"_id": 0, "employee_name": 1, "employee_code": 1, "kekaemployeenumber": 1})
        
        if not employee:
            logger.warning(f"[TASK-ASSIGNED-WARNING] Employee {employee_code} not found")
            raise HTTPException(status_code=404, detail=f"Employee {employee_code} not found")
        
        # Get assigned tasks (non-completed, match with/without leading zeros)
        codes = {"$in": _code_variants(employee_code)}
        assigned_tasks = list(db.tasks.find(
            {
                "assigned_to": codes,
                "status": {"$ne": "COMPLETED"}
            },
            {
                "_id": 0,
                "task_id": 1,
                "title": 1,
                "description": 1,
                "status": 1,
                "assigned_at": 1,
                "due_date": 1,
                "estimated_hours": 1,
                "file_id": 1,
                "stage": 1,
                "priority": 1
            }
        ).sort("assigned_at", -1))
        
        # Format datetime fields
        for task in assigned_tasks:
            if "assigned_at" in task and task["assigned_at"]:
                task["assigned_at"] = task["assigned_at"].isoformat() + 'Z'
            if "due_date" in task and task["due_date"]:
                task["due_date"] = task["due_date"].isoformat() + 'Z'
        
        result = {
            "employee_code": employee_code,
            "employee_name": employee.get("employee_name", f"Employee {employee_code}"),
            "assigned_tasks": assigned_tasks,
            "total_assigned": len(assigned_tasks),
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"[TASK-ASSIGNED-SUCCESS] Retrieved {len(assigned_tasks)} assigned tasks for {employee_code}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[TASK-ASSIGNED-ERROR] Failed to get assigned tasks for {employee_code}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch assigned tasks")


@router.get("/team-lead-stats")
async def get_team_lead_task_stats():
    """Get task statistics for all team leads"""
    logger.info(f"[TEAM-LEAD-STATS-START] Getting team lead task statistics")
    
    try:
        db = get_db()
        
        import re
        
        # Get all employees with reporting_manager
        employees = list(db.employee.find(
            {"reporting_manager": {"$exists": True, "$ne": None, "$ne": ""}},
            {"_id": 0, "employee_code": 1, "reporting_manager": 1}
        ))
        
        # Group employees by team lead
        team_groups = {}  # team_lead_code -> {name, codes[]}
        for emp in employees:
            manager = emp.get("reporting_manager", "")
            if not manager:
                continue
            match = re.match(r"^(.+?)\s*\((\w+)\)\s*$", str(manager))
            if match:
                tl_name = match.group(1).strip()
                tl_code = match.group(2).strip()
            else:
                tl_code = str(manager).strip()
                tl_name = tl_code
            
            if tl_code not in team_groups:
                team_groups[tl_code] = {"name": tl_name, "codes": []}
            emp_code = emp.get("employee_code")
            if emp_code:
                team_groups[tl_code]["codes"].append(emp_code)
        
        # Build stats for each team lead
        team_lead_stats = []
        for tl_code, info in team_groups.items():
            member_codes = info["codes"]
            total = db.tasks.count_documents({"assigned_to": {"$in": member_codes}})
            completed = db.tasks.count_documents({"assigned_to": {"$in": member_codes}, "status": "COMPLETED"})
            pending = total - completed
            rate = round((completed / total) * 100, 2) if total > 0 else 0.0
            
            team_lead_stats.append({
                "team_lead_code": tl_code,
                "team_lead_name": info["name"],
                "total_employees": len(member_codes),
                "task_statistics": {
                    "total_tasks": total,
                    "completed_tasks": completed,
                    "pending_tasks": pending,
                    "completion_rate": rate
                }
            })
        
        # Sort by total tasks descending
        team_lead_stats.sort(key=lambda x: x["task_statistics"]["total_tasks"], reverse=True)
        
        result = {
            "team_lead_stats": team_lead_stats,
            "total_team_leads": len(team_lead_stats),
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"[TEAM-LEAD-STATS-SUCCESS] Returning real stats for {len(team_lead_stats)} team leads")
        return result
        
    except Exception as e:
        logger.error(f"[TEAM-LEAD-STATS-ERROR] Failed to get team lead stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch team lead task statistics")


@router.get("/permit-file-tracking")
async def get_permit_file_tracking():
    """Get permit file tracking information"""
    logger.info(f"[PERMIT-TRACKING-START] Getting permit file tracking data")
    
    try:
        db = get_db()
        
        # Get permit files with their current status
        permit_files = list(db.permit_files.find(
            {},
            {
                "_id": 0,
                "file_id": 1,
                "permit_id": 1,
                "address": 1,
                "current_stage": 1,
                "status": 1,
                "created_at": 1,
                "updated_at": 1,
                "tasks_created": 1,
                "metadata": 1
            }
        ).sort("created_at", -1).limit(50))
        
        # Format datetime fields
        for permit in permit_files:
            if "created_at" in permit and permit["created_at"]:
                permit["created_at"] = permit["created_at"].isoformat() + 'Z'
            if "updated_at" in permit and permit["updated_at"]:
                permit["updated_at"] = permit["updated_at"].isoformat() + 'Z'
        
        # Get stage distribution
        stage_distribution = {}
        for permit in permit_files:
            stage = permit.get("current_stage", "UNKNOWN")
            stage_distribution[stage] = stage_distribution.get(stage, 0) + 1
        
        # Get recent tasks
        recent_tasks = list(db.tasks.find(
            {"file_id": {"$ne": None}},
            {
                "_id": 0,
                "task_id": 1,
                "file_id": 1,
                "title": 1,
                "status": 1,
                "assigned_to": 1,
                "assigned_at": 1
            }
        ).sort("assigned_at", -1).limit(20))
        
        # Format task datetime fields
        for task in recent_tasks:
            if "assigned_at" in task and task["assigned_at"]:
                task["assigned_at"] = task["assigned_at"].isoformat() + 'Z'
        
        result = {
            "permit_files": permit_files,
            "stage_distribution": stage_distribution,
            "recent_tasks": recent_tasks,
            "summary": {
                "total_permit_files": len(permit_files),
                "active_files": len([p for p in permit_files if p.get("status") != "COMPLETED"]),
                "completed_files": len([p for p in permit_files if p.get("status") == "COMPLETED"]),
                "total_tasks": len(recent_tasks)
            },
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"[PERMIT-TRACKING-SUCCESS] Retrieved tracking data for {len(permit_files)} permit files")
        return result
        
    except Exception as e:
        logger.error(f"[PERMIT-TRACKING-ERROR] Failed to get permit file tracking: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch permit file tracking")


@router.get("/debug-employees")
async def debug_employees():
    """Debug endpoint to check employee data"""
    try:
        db = get_db()
        
        # Get first few employees
        employees = list(db.employee.find({}).limit(3))
        
        # Count total employees
        total_count = db.employee.count_documents({})
        
        return {
            "total_employees": total_count,
            "sample_employees": employees,
            "sample_fields": list(employees[0].keys()) if employees else []
        }
        
    except Exception as e:
        return {"error": str(e)}
