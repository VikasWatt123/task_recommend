"""
Employee Assignment Endpoint
Backend-only endpoint for team-lead scoped employee fetching
Used by Temporal workflows and assignment logic
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from datetime import datetime
import logging

from app.db.mongodb import get_db
from app.services.stage_assignment_service import StageAssignmentService
from app.models.stage_flow import FileStage

router = APIRouter(tags=["employee-assignment"])
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logger.info("✅ Employee assignment router loaded")


@router.get("/assignment/available", response_model=Dict[str, List[Dict[str, Any]]])
async def get_employees_for_assignment(
    stage: str = Query(..., description="Stage for assignment (PRELIMS, PRODUCTION, QUALITY)"),
    team_lead_id: Optional[str] = Query(None, description="Team Lead ID to filter employees (optional)"),
    max_tasks: Optional[int] = Query(5, description="Maximum current tasks threshold"),
    shift: Optional[str] = Query(None, description="Filter by shift"),
    availability_mode: str = Query("active", description="Availability filter: active, all")
):
    """
    Get employees available for assignment, optionally scoped to a specific team lead.
    
    This endpoint is optimized for Temporal workflows and backend automation.
    Returns minimal employee data required for assignment decisions.
    """
    
    start_time = datetime.utcnow()
    
    try:
        # Convert stage string to FileStage enum
        try:
            stage_enum = FileStage(stage.upper())
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid stage: {stage}")
        
        # Use new intelligent assignment service
        employees = StageAssignmentService.get_employees_by_experience(
            stage_enum, team_lead_id, prioritize_new_joinees=(stage_enum == FileStage.PRELIMS)
        )
        
        # Filter by current tasks threshold
        eligible_employees = []
        for emp in employees:
            current_task_count = emp.get("current_tasks", 0) or 0
            if current_task_count < max_tasks:
                eligible_employees.append({
                    "employee_code": emp.get("employee_code"),
                    "employee_name": emp.get("employee_name"),
                    "current_tasks": current_task_count,
                    "experience_years": emp.get("experience_years", 0)
                })
        
        # Log performance metrics
        processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        logger.info(
            f"Assignment query: team_lead={team_lead_id}, stage={stage}, "
            f"found={len(eligible_employees)}, time={processing_time:.2f}ms"
        )
        
        return {"employees": eligible_employees}
        
    except Exception as e:
        logger.error(f"Error fetching employees for assignment: {str(e)}")
        # Return empty list on error to prevent workflow failures
        return {"employees": []}


@router.post("/assignment/intelligent", response_model=Dict[str, Any])
async def intelligent_assignment(
    file_id: str = Query(..., description="File ID for stage validation"),
    task_description: str = Query(..., description="Task description to detect stage"),
    team_lead_id: Optional[str] = Query(None, description="Team Lead ID to filter employees (optional)"),
    stage: Optional[str] = Query(None, description="Explicit stage (overrides detection)"),
    employee_code: Optional[str] = Query(None, description="Employee code for forced assignment")
):
    """
    Intelligent assignment with keyword detection and stage validation
    """
    try:
        # Detect stage from description if not explicitly provided
        if stage:
            try:
                target_stage = FileStage(stage.upper())
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid stage: {stage}")
        else:
            # Use context-aware stage detection if file_id is provided
            if file_id:
                target_stage = StageAssignmentService.detect_stage_from_description_with_context(
                    task_description, file_id
                )
            else:
                target_stage = StageAssignmentService.detect_stage_from_description(task_description)
                
            if not target_stage:
                if file_id:
                    # Check if file is in COMPLETED stage
                    from app.db.mongodb import get_db
                    db = get_db()
                    file_tracking = db.file_tracking.find_one({'file_id': file_id})
                    if file_tracking and file_tracking.get('current_stage') == 'COMPLETED':
                        raise HTTPException(
                            status_code=400, 
                            detail=f"File {file_id} is in COMPLETED stage. Manager must move file to QC stage before assigning tasks. Use /api/v1/stage-tracking/move-to-qc/{file_id}"
                        )
                
                raise HTTPException(
                    status_code=400, 
                    detail="Could not determine stage from task description. Please specify stage explicitly."
                )
        
        # Validate stage transition if file_id is provided
        validation_error = None
        if file_id:
            is_valid, error_msg = StageAssignmentService.check_stage_transition_validity(file_id, target_stage)
            if not is_valid:
                validation_error = error_msg
                # Return validation error without assigning
                return {
                    "success": False,
                    "stage": target_stage.value,
                    "validation_error": validation_error,
                    "message": error_msg
                }
        
        # Get best employee for the stage (or use forced employee)
        if employee_code:
            # Use forced employee
            db = get_db()
            employee = db.employee.find_one({"employee_code": employee_code})
            if not employee:
                raise HTTPException(status_code=404, detail=f"Employee {employee_code} not found")
            
            best_employee = {
                "employee_code": employee["employee_code"],
                "employee_name": employee["employee_name"],
                "current_tasks": employee.get("current_tasks", 0),
                "experience_years": employee.get("employment", {}).get("experience_years", 0),
                "skills": employee.get("skills", {}),
                "forced_assignment": True
            }
        else:
            # Use intelligent selection
            best_employee = StageAssignmentService.get_best_employee_for_stage(
                target_stage, file_id, task_description, team_lead_id
            )
        
        # Create and assign the task
        from datetime import datetime
        import uuid
        
        task_id = f"TASK-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"
        
        db = get_db()
        
        # Create task document
        task_doc = {
            "task_id": task_id,
            "title": f"{target_stage.value} - {file_id}",
            "description": task_description,
            "status": "ASSIGNED",
            "assigned_to": best_employee["employee_code"],
            "assigned_to_name": best_employee["employee_name"],
            "assigned_by": "user",
            "assigned_at": datetime.utcnow(),
            "stage": target_stage.value,
            "source": {
                "permit_file_id": file_id
            },
            "metadata": {
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
        }
        
        # Insert task
        db.tasks.insert_one(task_doc)
        
        # Create stage history entry
        stage_history_doc = {
            "file_id": file_id,
            "stage": target_stage.value,
            "status": "IN_PROGRESS",
            "assigned_to": {
                "employee_code": best_employee["employee_code"],
                "employee_name": best_employee["employee_name"],
                "assigned_at": datetime.utcnow(),
                "started_at": datetime.utcnow()
            },
            "entered_stage_at": datetime.utcnow(),
            "completed_stage_at": None,
            "total_duration_minutes": 0
        }
        
        db.stage_history.insert_one(stage_history_doc)
        
        # Create or update file_tracking entry for stage tracking service
        file_tracking_doc = {
            "file_id": file_id,
            "current_stage": target_stage.value,
            "current_status": f"IN_{target_stage.value}",
            "stage_history": [stage_history_doc],
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        # Update or insert file_tracking
        existing_tracking = db.file_tracking.find_one({"file_id": file_id})
        if existing_tracking:
            # Add to existing stage history
            db.file_tracking.update_one(
                {"file_id": file_id},
                {
                    "$push": {"stage_history": stage_history_doc},
                    "$set": {
                        "current_stage": target_stage.value,
                        "current_status": f"IN_{target_stage.value}",
                        "updated_at": datetime.utcnow()
                    }
                }
            )
        else:
            # Create new tracking entry
            db.file_tracking.insert_one(file_tracking_doc)
        
        # Update file status
        db.permit_files.update_one(
            {"file_id": file_id},
            {
                "$set": {
                    "status": f"IN_{target_stage.value}",
                    "metadata.updated_at": datetime.utcnow(),
                    "current_assignment": {
                        "employee_code": best_employee["employee_code"],
                        "employee_name": best_employee["employee_name"],
                        "stage": target_stage.value
                    }
                }
            }
        )
        
        # Broadcast task assignment via WebSocket and SSE
        try:
            from app.api.v1.routers.websocket_events import websocket_manager
            
            # For WebSocket clients (two-way)
            await websocket_manager.broadcast_task_update({
                "task_id": task_id,
                "assigned_to": best_employee["employee_code"],
                "assigned_to_name": best_employee["employee_name"],
                "stage": target_stage.value,
                "description": task_description,
                "file_id": file_id,
                "action": "assigned"
            })
            
            # For SSE clients (one-way)
            await websocket_manager.broadcast_one_way_update("task_assigned", {
                "task_id": task_id,
                "assigned_to": best_employee["employee_code"],
                "assigned_to_name": best_employee["employee_name"],
                "stage": target_stage.value,
                "description": task_description,
                "file_id": file_id,
                "timestamp": datetime.utcnow().isoformat()
            })
            
        except Exception as e:
            logger.warning(f"Failed to broadcast task assignment: {e}")
        
        return {
            "success": True,
            "stage": target_stage.value,
            "employee": best_employee,
            "task_id": task_id,
            "detected_from_keywords": stage is None,
            "validation_passed": True,
            "forced_assignment": employee_code is not None,
            "message": f"Task assigned to {best_employee['employee_name']} for {target_stage.value} stage"
        }
        
    except Exception as e:
        logger.error(f"Error in intelligent assignment: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
