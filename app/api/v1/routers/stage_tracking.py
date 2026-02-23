"""
Stage Tracking Router
Tracks file progression through different stages with SLA monitoring
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging

print("🔥 Loading stage_tracking router...")

router = APIRouter(prefix="/stage-tracking", tags=["stage_tracking"])
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from app.models.stage_flow import FileStage, get_stage_config
from app.models.file_stage_tracking import (
    StageTransitionRequest, StageAssignmentRequest, 
    StageCompletionRequest, FileTracking
)
from app.utils.api_response import APIResponse
from app.services.stage_tracking_service import get_stage_tracking_service, _parse_file_tracking_safely, convert_objectid_to_str
from app.db.mongodb import get_db


# Request/Response models
class InitializeTrackingRequest(BaseModel):
    permit_file_id: str
    initial_stage: FileStage = FileStage.PRELIMS


class FileTrackingResponse(BaseModel):
    permit_file_id: str
    current_stage: FileStage
    current_status: str
    current_assignment: Optional[Dict[str, Any]]
    stage_history: List[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime
    total_penalty_points: float
    escalations_triggered: int


class PipelineViewResponse(BaseModel):
    stage: FileStage
    files: List[Dict[str, Any]]


class EmployeePerformanceResponse(BaseModel):
    employee_code: str
    active_assignments: int
    completed_stages: int
    total_penalty_points: float
    average_stage_duration_minutes: float
    active_work: List[Dict[str, Any]]
    completed_work: List[Dict[str, Any]]


class SLAReportResponse(BaseModel):
    total_stages: int
    completed_stages: int
    within_ideal: int
    over_ideal: int
    over_max: int
    escalations: int
    by_stage: Dict[str, Dict[str, int]]


# Endpoints
@router.post("/initialize")
async def initialize_tracking(request: InitializeTrackingRequest):
    """Initialize tracking for a new file"""
    try:
        service = get_stage_tracking_service()
        tracking = service.initialize_file_tracking(request.permit_file_id, request.initial_stage)
        
        return {
            "success": True,
            "message": f"Tracking initialized for file {request.permit_file_id}",
            "tracking": tracking.dict()
        }
    except Exception as e:
        logger.error(f"Failed to initialize tracking for {request.permit_file_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to initialize tracking: {str(e)}")


@router.get("/file/{permit_file_id}")
async def get_file_tracking(permit_file_id: str):
    """Get tracking information for a specific file"""
    try:
        service = get_stage_tracking_service()
        tracking = service.get_file_tracking(permit_file_id)
        
        if not tracking:
            return APIResponse.error(
                message=f"No tracking found for file {permit_file_id}",
                error_code="TRACKING_NOT_FOUND"
            )

        # StageTrackingService.get_file_tracking returns a raw dict
        if isinstance(tracking, dict):
            # Parse safely but also work with raw dict for enhancement
            parsed_tracking = _parse_file_tracking_safely(tracking)
            if not parsed_tracking:
                return APIResponse.error(
                    message=f"Unable to parse tracking data for file {permit_file_id}",
                    error_code="PARSING_ERROR"
                )
            
            # Use raw dict for enhancement to have full control
            tracking_data = convert_objectid_to_str(tracking)
            
            # Add summary information
            summary = {
                "file_id": permit_file_id,
                "current_stage": tracking_data.get("current_stage"),
                "current_status": tracking_data.get("current_status"),
                "total_stages": len(tracking_data.get("stage_history", [])),
                "completed_stages": len([s for s in tracking_data.get("stage_history", []) if s.get("status") == "COMPLETED"]),
                "current_assignment": None,
                "progress_percentage": 0,
                "last_updated": tracking_data.get("updated_at")
            }
            
            # Handle current assignment
            if tracking_data.get("current_assignment"):
                current_assignment = tracking_data["current_assignment"]
                summary["current_assignment"] = {
                    "employee_code": current_assignment.get("employee_code"),
                    "employee_name": current_assignment.get("employee_name"),
                    "assigned_at": current_assignment.get("assigned_at"),
                    "started_at": current_assignment.get("started_at"),
                    "status": "IN_PROGRESS"
                }
            
            # Calculate progress percentage
            if summary["total_stages"] > 0:
                summary["progress_percentage"] = round((summary["completed_stages"] / summary["total_stages"]) * 100)
            
            # Enhanced stage history with better formatting (backward compatible)
            enhanced_stage_history = []
            for stage in tracking_data.get("stage_history", []):
                stage_data = {
                    "stage": stage.get("stage"),
                    "status": stage.get("status"),
                    "entered_at": stage.get("entered_stage_at"),
                    "completed_at": stage.get("completed_stage_at"),
                    "duration_minutes": stage.get("total_duration_minutes"),
                    "sla_status": "OK" if not stage.get("sla_breached") else "BREACHED",
                    "assignment": None
                }
                
                # Handle assignment in stage history - get data from stage_history collection for accuracy
                if stage.get("assigned_to"):
                    # First try to get fresh data from stage_history collection
                    stage_name = stage.get("stage")
                    fresh_stage = db.stage_history.find_one({"file_id": permit_file_id, "stage": stage_name})
                    
                    if fresh_stage and fresh_stage.get("assigned_to"):
                        assigned = fresh_stage["assigned_to"]
                    else:
                        assigned = stage["assigned_to"]  # Fallback to original data
                    
                    # NEW: Enhanced assignment format
                    stage_data["assignment"] = {
                        "employee_code": assigned.get("employee_code"),
                        "employee_name": assigned.get("employee_name"),
                        "assigned_at": assigned.get("assigned_at"),
                        "started_at": assigned.get("started_at"),
                        "completed_at": assigned.get("completed_at"),
                        "duration_minutes": assigned.get("duration_minutes"),
                        "notes": assigned.get("notes")
                    }
                    
                    # BACKWARD COMPATIBILITY: Keep old assigned_to field
                    stage_data["assigned_to"] = {
                        "employee_code": assigned.get("employee_code"),
                        "employee_name": assigned.get("employee_name"),
                        "assigned_at": assigned.get("assigned_at"),
                        "started_at": assigned.get("started_at"),
                        "completed_at": assigned.get("completed_at"),
                        "duration_minutes": assigned.get("duration_minutes"),
                        "notes": assigned.get("notes")
                    }
                
                enhanced_stage_history.append(stage_data)
            
            # Replace stage history with enhanced version
            tracking_data["stage_history"] = enhanced_stage_history
            
            # Add summary to response
            tracking_data["summary"] = summary
            
            return APIResponse.success(
                data=tracking_data,
                message=f"Enhanced tracking retrieved for file {permit_file_id} - Current stage: {summary['current_stage']}, Progress: {summary['progress_percentage']}%"
            )
    except Exception as e:
        logger.error(f"Failed to get tracking for {permit_file_id}: {str(e)}")
        return APIResponse.error(
            message=f"Failed to get tracking: {str(e)}",
            error_code="TRACKING_RETRIEVAL_ERROR"
        )


@router.post("/assign")
async def assign_employee_to_stage(request: StageAssignmentRequest):
    """Assign an employee to work on the current stage of a file"""
    try:
        service = get_stage_tracking_service()
        
        # Get employee name
        db = get_db()
        employee_doc = db.employee.find_one({"employee_code": request.employee_code})
        if not employee_doc:
            raise HTTPException(status_code=404, detail=f"Employee {request.employee_code} not found")
        
        employee_name = employee_doc.get("employee_name", "Unknown")
        
        # Assign employee
        tracking = service.assign_employee_to_stage(
            request.permit_file_id, 
            request.employee_code, 
            employee_name, 
            request.notes
        )
        
        return {
            "success": True,
            "message": f"Assigned {employee_name} to file {request.permit_file_id}",
            "tracking": tracking.dict()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to assign employee to stage: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to assign employee: {str(e)}")


@router.post("/start-work")
async def start_stage_work(permit_file_id: str, employee_code: str):
    """Mark that work has started on the current stage"""
    try:
        service = get_stage_tracking_service()
        tracking = service.start_stage_work(permit_file_id, employee_code)
        
        return {
            "success": True,
            "message": f"Work started on file {permit_file_id}",
            "tracking": tracking.dict()
        }
    except Exception as e:
        logger.error(f"Failed to start work: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to start work: {str(e)}")


@router.post("/complete-stage")
async def complete_stage(request: StageCompletionRequest):
    """Complete the current stage and optionally transition to next"""
    try:
        service = get_stage_tracking_service()
        tracking = service.complete_and_transition(
            request.permit_file_id,
            request.employee_code,
            request.completion_notes,
            request.next_stage_employee_code
        )
        
        return {
            "success": True,
            "message": f"Stage completed for file {request.permit_file_id}",
            "tracking": tracking.dict()
        }
    except Exception as e:
        logger.error(f"Failed to complete stage: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to complete stage: {str(e)}")


@router.post("/transition")
async def transition_stage(request: StageTransitionRequest):
    """Transition file to next stage (requires current stage completion)"""
    try:
        service = get_stage_tracking_service()
        
        if request.force_transition:
            # Admin override
            tracking = service.force_transition(
                request.permit_file_id,
                request.target_stage,
                request.employee_code,
                request.notes
            )
        else:
            # Normal transition
            tracking = service.transition_to_next_stage(
                request.permit_file_id,
                request.employee_code,
                request.target_stage
            )
        
        return {
            "success": True,
            "message": f"Transitioned file {request.permit_file_id} to stage {request.target_stage}",
            "tracking": tracking.dict()
        }
    except Exception as e:
        logger.error(f"Failed to transition stage: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to transition stage: {str(e)}")


@router.get("/pipeline")
async def get_pipeline_view(stage: Optional[FileStage] = Query(None)):
    """Get pipeline view of files at each stage"""
    try:
        service = get_stage_tracking_service()
        pipeline = service.get_stage_pipeline_view(stage)
        
        return {
            "success": True,
            "pipeline": pipeline
        }
    except Exception as e:
        logger.error(f"Failed to get pipeline view: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get pipeline view: {str(e)}")


@router.get("/employee/{employee_code}/performance")
async def get_employee_performance(employee_code: str, days: int = Query(30, ge=1, le=365)):
    """Get performance metrics for an employee"""
    try:
        service = get_stage_tracking_service()
        performance = service.get_employee_performance(employee_code, days)
        
        return {
            "success": True,
            "performance": performance
        }
    except Exception as e:
        logger.error(f"Failed to get employee performance: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get performance: {str(e)}")


@router.get("/sla-report")
async def get_sla_report(days: int = Query(7, ge=1, le=90)):
    """Get SLA compliance report"""
    try:
        service = get_stage_tracking_service()
        report = service.get_sla_report(days)
        
        return {
            "success": True,
            "report": report
        }
    except Exception as e:
        logger.error(f"Failed to get SLA report: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get SLA report: {str(e)}")


@router.get("/sla-breaches")
async def get_sla_breaches():
    """Get files that have breached SLA and need escalation"""
    try:
        service = get_stage_tracking_service()
        breaches = service.check_sla_breaches()
        
        return {
            "success": True,
            "breaches": breaches,
            "count": len(breaches)
        }
    except Exception as e:
        logger.error(f"Failed to check SLA breaches: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to check SLA breaches: {str(e)}")


@router.post("/send-escalations")
async def send_escalation_notifications():
    """Check for SLA breaches and send escalation notifications"""
    try:
        service = get_stage_tracking_service()
        breaches = service.check_sla_breaches()
        
        sent_count = 0
        for breach in breaches:
            # TODO: Implement actual notification sending
            # This would integrate with your notification system
            logger.warning(f"Escalation needed for file {breach['file_id']} - {breach['employee_name']} ({breach['duration_minutes']} minutes)")
            sent_count += 1
        
        return {
            "success": True,
            "message": f"Processed {len(breaches)} SLA breaches, sent {sent_count} notifications",
            "breaches_processed": breaches
        }
    except Exception as e:
        logger.error(f"Failed to send escalations: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to send escalations: {str(e)}")


@router.post("/complete-and-progress/{permit_file_id}")
async def complete_stage_and_progress(permit_file_id: str, employee_code: str):
    """Complete current stage and move file to next stage in sequence"""
    try:
        service = get_stage_tracking_service()
        
        # Get employee name
        db = get_db()
        employee_doc = db.employee.find_one({"employee_code": employee_code})
        if not employee_doc:
            raise HTTPException(status_code=404, detail=f"Employee {employee_code} not found")
        
        employee_name = employee_doc.get("employee_name", "Unknown")
        
        # Complete stage and progress
        result = service.complete_stage_and_progress(permit_file_id, employee_code, employee_name)
        
        return {
            "success": True,
            "message": result["message"],
            "progression": {
                "permit_file_id": result["file_id"],
                "previous_stage": result["previous_stage"],
                "next_stage": result["next_stage"],
                "completed_by": result["completed_by"],
                "completed_by_name": result["completed_by_name"]
            }
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to complete stage and progress: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to complete stage and progress: {str(e)}")


@router.post("/move-to-qc/{permit_file_id}")
async def move_file_to_qc(permit_file_id: str, employee_code: str):
    """Move file from COMPLETED to QC stage (manager action)"""
    try:
        service = get_stage_tracking_service()
        
        # Get employee name
        db = get_db()
        employee_doc = db.employee.find_one({"employee_code": employee_code})
        if not employee_doc:
            raise HTTPException(status_code=404, detail=f"Employee {employee_code} not found")
        
        employee_name = employee_doc.get("employee_name", "Unknown")
        
        # Get current tracking
        tracking = service.get_file_tracking(permit_file_id)
        if not tracking:
            raise HTTPException(status_code=404, detail=f"No tracking found for file {permit_file_id}")
        
        # get_file_tracking may return a dict or a FileTracking object
        current_stage_raw = tracking.get("current_stage") if isinstance(tracking, dict) else getattr(tracking.current_stage, "value", str(tracking.current_stage))
        
        # Verify file is in COMPLETED stage
        if current_stage_raw != "COMPLETED":
            raise HTTPException(status_code=400, detail=f"File must be in COMPLETED stage to move to QC. Current stage: {current_stage_raw}")
        
        # Transition to QC
        from app.models.stage_flow import FileStage
        result = service.transition_to_next_stage(permit_file_id, employee_code, FileStage.QC)
        
        # Emit QC stage started event
        try:
            from app.services.clickhouse_service import clickhouse_service
            clickhouse_service.emit_stage_started_event(
                task_id=f"FILE-{permit_file_id}",
                employee_code="",
                employee_name="",
                stage="QC",
                file_id=permit_file_id
            )
        except Exception as e:
            logger.warning(f"Failed to emit QC stage started event: {e}")
        
        return {
            "success": True,
            "message": f"File {permit_file_id} moved to QC stage",
            "transition": {
                "permit_file_id": permit_file_id,
                "from_stage": "COMPLETED",
                "to_stage": "QC",
                "moved_by": employee_code,
                "moved_by_name": employee_name,
                "moved_at": datetime.utcnow().isoformat()
            }
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to move file to QC: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to move file to QC: {str(e)}")


@router.get("/ready-for-stage/{stage}")
async def get_files_ready_for_stage(stage: str):
    """Get files that are ready to be assigned to a specific stage"""
    try:
        from app.models.stage_flow import FileStage
        
        # Validate stage
        try:
            target_stage = FileStage(stage.upper())
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid stage: {stage}")
        
        service = get_stage_tracking_service()
        files = service.get_files_ready_for_stage(target_stage)
        
        return {
            "success": True,
            "stage": stage,
            "files": files,
            "total": len(files)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get files ready for stage {stage}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get files ready for stage: {str(e)}")


@router.get("/file/{permit_file_id}/stage-history")
async def get_file_stage_history(permit_file_id: str):
    """Get complete stage history for a specific file"""
    try:
        service = get_stage_tracking_service()
        tracking = service.get_file_tracking(permit_file_id)
        
        if not tracking:
            raise HTTPException(status_code=404, detail=f"No tracking found for file {permit_file_id}")
        
        # Get permit file info
        db = get_db()
        permit_file = db.permit_files.find_one(
            {"file_id": permit_file_id},
            {"_id": 0, "file_info": 1, "project_details": 1}
        )
        
        # Batch fetch employee details to avoid N+1 queries
        stage_history_data = tracking.get("stage_history", [])
        employee_codes_in_history = [
            stage_entry.get("assigned_to", {}).get("employee_code")
            for stage_entry in stage_history_data
            if stage_entry.get("assigned_to") and stage_entry.get("assigned_to").get("employee_code")
        ]
        
        employee_map = {}
        if employee_codes_in_history:
            employees = list(db.employee.find(
                {"employee_code": {"$in": employee_codes_in_history}},
                {"_id": 0, "employee_code": 1, "employee_name": 1, "current_role": 1}
            ))
            employee_map = {emp["employee_code"]: emp for emp in employees}
        
        # Format stage history with employee details
        stage_history = []
        for stage_entry in stage_history_data:
            # Get employee details from batch-fetched map
            employee_doc = None
            assigned_to = stage_entry.get("assigned_to")
            if assigned_to and assigned_to.get("employee_code"):
                employee_doc = employee_map.get(assigned_to.get("employee_code"))
            
            stage_history.append({
                "stage": stage_entry.get("stage"),
                "status": stage_entry.get("status"),
                "assigned_to": {
                    "employee_code": assigned_to.get("employee_code") if assigned_to else None,
                    "employee_name": assigned_to.get("employee_name") if assigned_to else None,
                    "current_role": employee_doc.get("current_role", "Unknown") if employee_doc else None
                } if assigned_to else None,
                "created_at": stage_entry.get("entered_stage_at"),  # When stage was created
                "assigned_at": assigned_to.get("assigned_at") if assigned_to else None,  # When employee was assigned
                "started_at": assigned_to.get("started_at") if assigned_to else None,
                "completed_at": stage_entry.get("completed_stage_at"),
                "duration_minutes": stage_entry.get("total_duration_minutes"),
            })
        
        return {
            "success": True,
            "permit_file_id": permit_file_id,
            "original_filename": (
                permit_file.get("file_info", {}).get("original_filename") or 
                permit_file.get("file_name", "Unknown File")
            ) if permit_file else "Unknown File",
            "current_stage": tracking.get("current_stage"),
            "current_status": tracking.get("current_status"),
            "stage_history": stage_history,
            "total_stages": 4,  # Fixed total stages in the workflow
            "created_at": tracking.get("created_at"),
            "updated_at": tracking.get("updated_at")
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get stage history for {permit_file_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get stage history: {str(e)}")


@router.get("/dashboard")
async def get_dashboard_data():
    """Get comprehensive dashboard data"""
    try:
        import asyncio
        from concurrent.futures import ThreadPoolExecutor
        
        service = get_stage_tracking_service()
        db = get_db()
        
        # Define async wrappers for synchronous operations
        def get_pipeline():
            return service.get_stage_pipeline_view()
        
        def get_breaches():
            return service.check_sla_breaches()
        
        def get_recent_files():
            from datetime import datetime, timedelta, timezone
            yesterday = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=1)
            recent = list(db[FILE_TRACKING_COLLECTION].find({
                "updated_at": {"$gte": yesterday}
            }).sort("updated_at", -1).limit(10))
            return convert_objectid_to_str(recent)
        
        def get_delivered():
            from datetime import datetime, timezone
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
            delivered = list(db.permit_files.find({
                "current_stage": "DELIVERED",
                "updated_at": {"$gte": today}
            }))
            return convert_objectid_to_str(delivered)
        
        # Execute all queries in parallel using ThreadPoolExecutor
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=4) as executor:
            pipeline_future = loop.run_in_executor(executor, get_pipeline)
            breaches_future = loop.run_in_executor(executor, get_breaches)
            recent_future = loop.run_in_executor(executor, get_recent_files)
            delivered_future = loop.run_in_executor(executor, get_delivered)
            
            # Wait for all queries to complete
            pipeline, breaches, recent_files, delivered_today = await asyncio.gather(
                pipeline_future,
                breaches_future,
                recent_future,
                delivered_future
            )
        
        # Calculate total penalties (actual breaches with duration > max_minutes)
        total_penalties = 0
        try:
            for breach in breaches:
                from app.models.stage_flow import get_stage_config
                stage_config = get_stage_config(breach.get("current_stage"))
                if stage_config and breach.get("duration_minutes", 0) > stage_config.max_minutes:
                    total_penalties += 1
        except Exception as e:
            logger.warning(f"Error calculating penalties: {e}")
            total_penalties = 0
        
        return {
            "success": True,
            "data": {
                "pipeline": pipeline,
                "sla_breaches": breaches,
                "recent_activity": recent_files,
                "delivered_today": delivered_today,
                "total_penalties": total_penalties,
                "summary": {
                    "total_files": sum(len(files) for files in pipeline.values()),
                    "active_files": sum(len(files) for stage, files in pipeline.items()
                                        if stage not in ["COMPLETED", "DELIVERED"]),
                    "breaches_count": len(breaches),
                    "delivered_today_count": len(delivered_today),
                    "escalations_today": len([b for b in breaches if b.get("duration_minutes", 0) > 60])
                }
            }
        }
    except Exception as real_db_error:
        logger.error(f"Failed to get real dashboard data: {str(real_db_error)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch dashboard data: {str(real_db_error)}")


@router.get("/stages")
async def get_stage_definitions():
    """Get stage definitions and SLA rules"""
    try:
        from app.models.stage_flow import STAGE_CONFIGS
        
        stages = []
        for stage, config in STAGE_CONFIGS.items():
            stages.append({
                "stage": stage,
                "name": config.name,
                "display_name": config.display_name,
                "description": config.description,
                "ideal_minutes": config.ideal_minutes,
                "max_minutes": config.max_minutes,
                "escalation_minutes": config.escalation_minutes,
                "requires_previous_stage": config.requires_previous_stage,
                "allowed_previous_stages": config.allowed_previous_stages
            })
        
        return {
            "success": True,
            "stages": stages
        }
    except Exception as e:
        logger.error(f"Failed to get stage definitions: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get stage definitions: {str(e)}")


# ===== Enhanced File Lifecycle API Endpoints =====

@router.get("/file/{permit_file_id}/lifecycle")
async def get_file_lifecycle_timeline(permit_file_id: str):
    """Get complete lifecycle timeline for a specific file"""
    try:
        from app.services.clickhouse_lifecycle_service import clickhouse_lifecycle_service
        
        events = clickhouse_lifecycle_service.get_file_lifecycle_timeline(permit_file_id)
        
        # Format events for frontend
        formatted_events = []
        for event in events:
            formatted_events.append({
                "event_id": event[0],
                "event_type": event[1],
                "stage": event[2],
                "employee_code": event[3],
                "employee_name": event[4],
                "event_time": event[5].isoformat() if event[5] else None,
                "event_data": event[6],
                "previous_stage": event[7],
                "next_stage": event[8],
                "duration_minutes": event[9]
            })
        
        return APIResponse.success(
            data={
                "permit_file_id": permit_file_id,
                "lifecycle_events": formatted_events,
                "total_events": len(formatted_events),
                "current_stage": formatted_events[-1]["stage"] if formatted_events else None
            },
            message="Lifecycle timeline retrieved successfully"
        )
    except Exception as e:
        logger.error(f"Failed to get lifecycle timeline for {permit_file_id}: {str(e)}")
        return APIResponse.error(
            message=f"Failed to get lifecycle timeline: {str(e)}",
            error_code="LIFECYCLE_TIMELINE_ERROR"
        )

@router.get("/lifecycle/analytics")
async def get_lifecycle_analytics():
    """Get comprehensive lifecycle analytics and insights"""
    try:
        from app.services.clickhouse_lifecycle_service import clickhouse_lifecycle_service
        
        analytics = clickhouse_lifecycle_service.get_lifecycle_analytics()
        
        return {
            "success": True,
            "analytics": analytics,
            "generated_at": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get lifecycle analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get lifecycle analytics: {str(e)}")

@router.post("/manual-sync")
async def manual_sync():
    """Manually trigger MongoDB to ClickHouse sync"""
    logger.info("[MANUAL-SYNC-START] Triggering manual sync from MongoDB to ClickHouse")
    
    try:
        from app.services.sync_service import sync_service
        
        # Trigger the sync
        await sync_service.sync_data()
        
        logger.info("[MANUAL-SYNC-SUCCESS] Manual sync completed successfully")
        
        return {
            "message": "Manual sync completed successfully",
            "status": "success",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"[MANUAL-SYNC-ERROR] Manual sync failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Manual sync failed: {str(e)}"
        )


@router.get("/lifecycle/events")
async def get_lifecycle_events(
    permit_file_id: Optional[str] = Query(None, description="Filter by specific file ID"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    stage: Optional[str] = Query(None, description="Filter by stage"),
    employee_code: Optional[str] = Query(None, description="Filter by employee code"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of events to return"),
    offset: int = Query(0, ge=0, description="Number of events to skip")
):
    """Get lifecycle events with optional filtering"""
    try:
        from app.services.clickhouse_service import clickhouse_service
        
        # Build simple query without complex parameters
        query_conditions = []
        
        if permit_file_id:
            query_conditions.append(f"file_id = '{permit_file_id}'")
        
        if event_type:
            query_conditions.append(f"event_type = '{event_type}'")
        
        if stage:
            query_conditions.append(f"stage = '{stage}'")
        
        if employee_code:
            query_conditions.append(f"employee_code = '{employee_code}'")
        
        where_clause = f"WHERE {' AND '.join(query_conditions)}" if query_conditions else ""
        
        query = f"""
        SELECT 
            event_id,
            file_id,
            event_type,
            stage,
            employee_code,
            employee_name,
            event_time,
            event_data,
            previous_stage,
            next_stage,
            duration_minutes
        FROM task_analytics.file_lifecycle_events 
        {where_clause}
        ORDER BY event_time DESC
        LIMIT {limit} OFFSET {offset}
        """
        
        events = clickhouse_service.client.execute(query)
        
        # Format events
        formatted_events = []
        for event in events:
            formatted_events.append({
                "event_id": event[0],
                "permit_file_id": event[1],
                "event_type": event[2],
                "stage": event[3],
                "employee_code": event[4],
                "employee_name": event[5],
                "event_time": event[6].isoformat() if event[6] else None,
                "event_data": event[7],
                "previous_stage": event[8],
                "next_stage": event[9],
                "duration_minutes": event[10]
            })
        
        return APIResponse.success(
            data={
                "lifecycle_events": formatted_events,
                "total": len(formatted_events),
                "limit": limit,
                "offset": offset
            },
            message=f"Retrieved {len(formatted_events)} lifecycle events"
        )
    except Exception as e:
        logger.error(f"Failed to get lifecycle events: {str(e)}")
        return APIResponse.error(
            message=f"Failed to get lifecycle events: {str(e)}",
            error_code="LIFECYCLE_EVENTS_ERROR"
        )

@router.post("/lifecycle/setup-tables")
async def setup_lifecycle_tables():
    """Setup ClickHouse lifecycle tables (admin endpoint)"""
    try:
        from app.setup_clickhouse_lifecycle import setup_lifecycle_tables, create_initial_indexes
        
        # Setup tables
        tables_success = setup_lifecycle_tables()
        
        if tables_success:
            # Create indexes
            create_initial_indexes()
            
            return {
                "success": True,
                "message": "ClickHouse lifecycle tables setup successfully",
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to setup lifecycle tables")
    except Exception as e:
        logger.error(f"Failed to setup lifecycle tables: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to setup lifecycle tables: {str(e)}")


@router.post("/manual-sync")
async def manual_sync_mongo_to_clickhouse():
    """
    Manual sync endpoint to force sync latest MongoDB data to ClickHouse
    Called when refresh button is clicked on stage tracking page
    """
    try:
        import asyncio
        from datetime import datetime, timedelta
        
        logger.info("🔄 Starting manual sync from MongoDB to ClickHouse")
        
        # Initialize sync service
        from app.services.sync_service import SyncService
        sync_service = SyncService()
        
        # Force sync of recent data (last 1 hour to ensure latest events)
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        
        # Sync tasks from MongoDB to ClickHouse
        from app.services.clickhouse_service import clickhouse_service
        await clickhouse_service.sync_tasks_from_mongodb(since=one_hour_ago)
        
        # Sync file stage tracking data
        db = get_db()
        service = get_stage_tracking_service()
        
        # Get all recently updated files (last 1 hour)
        recent_files = list(db[FILE_TRACKING_COLLECTION].find({
            "updated_at": {"$gte": one_hour_ago}
        }))
        
        synced_files = 0
        for file_doc in recent_files:
            try:
                file_id = file_doc.get("file_id")
                current_stage = file_doc.get("current_stage")
                
                if file_id and current_stage:
                    # Update ClickHouse with latest stage
                    clickhouse_service.update_file_stage(file_id, current_stage)
                    synced_files += 1
                    
            except Exception as e:
                logger.warning(f"Failed to sync file {file_doc.get('file_id')}: {e}")
                continue
        
        # Sync SLA breaches for recent data
        breaches = service.check_sla_breaches()
        breached_count = len(breaches) if breaches else 0
        
        # Get updated pipeline data after sync
        updated_pipeline = service.get_stage_pipeline_view()
        
        sync_time = datetime.utcnow().isoformat()
        
        logger.info(f"✅ Manual sync completed: {synced_files} files synced, {breached_count} breaches detected")
        
        return {
            "success": True,
            "message": "Manual sync completed successfully",
            "sync_time": sync_time,
            "synced_files": synced_files,
            "breached_files": breached_count,
            "pipeline_summary": {
                stage: len(files) for stage, files in updated_pipeline.items()
            },
            "details": {
                "sync_period": f"Last 1 hour (since {one_hour_ago.isoformat()})",
                "data_sources": ["MongoDB tasks", "File stage tracking", "SLA breaches"],
                "target": "ClickHouse analytics tables"
            }
        }
        
    except Exception as e:
        logger.error(f"Manual sync failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Manual sync failed: {str(e)}")


# Import the collection name for the dashboard endpoint
from app.models.file_stage_tracking import FILE_TRACKING_COLLECTION
