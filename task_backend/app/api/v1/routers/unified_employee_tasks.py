"""
Unified employee tasks endpoint for consistency
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

from app.db.mongodb import get_db
from app.utils.api_response import APIResponse

router = APIRouter(prefix="/tasks", tags=["unified_employee_tasks"])
logger = logging.getLogger(__name__)

@router.get("/employee/{employee_code}/unified")
async def get_unified_employee_tasks(
    employee_code: str,
    include_completed: bool = Query(False, description="Include completed tasks"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Items per page")
):
    """
    Unified endpoint to get all tasks for an employee with consistent format
    Replaces both /employee-tasks/{code} and /tasks/employee/{code}/assigned
    """
    try:
        db = get_db()
        
        # Get employee details
        employee = db.employee.find_one(
            {"employee_code": employee_code},
            {
                "employee_code": 1,
                "employee_name": 1,
                "employment.current_role": 1,
                "employment.shift": 1,
                "employment.status_1": 1,
                "technical_skills.skills": 1,
                "_id": 0
            }
        )
        
        if not employee:
            return APIResponse.error(
                message=f"Employee {employee_code} not found",
                error_code="EMPLOYEE_NOT_FOUND"
            )
        
        # Get assigned tasks (active)
        assigned_tasks = list(db.tasks.find(
            {
                "assigned_to": employee_code,
                "status": {"$in": ["ASSIGNED", "IN_PROGRESS"]}
            },
            {"_id": 0}
        ).sort("assigned_at", -1))
        
        # Get completed tasks if requested
        completed_tasks = []
        if include_completed:
            # From tasks collection
            completed_from_tasks = list(db.tasks.find(
                {
                    "assigned_to": employee_code,
                    "status": "COMPLETED"
                },
                {"_id": 0}
            ).sort("completed_at", -1))
            
            # From profile_building collection
            completed_from_profile = list(db.profile_building.find(
                {"employee_code": employee_code, "status": "COMPLETED"},
                {"_id": 0}
            ).sort("completion_time", -1))
            
            completed_tasks = completed_from_tasks + completed_from_profile
        
        # Enrich all tasks with client information
        file_ids = []
        for task in assigned_tasks + completed_tasks:
            permit_file_id = task.get("permit_file_id") or task.get("source", {}).get("permit_file_id")
            if permit_file_id:
                file_ids.append(permit_file_id)
        
        # Batch fetch permit files for enrichment
        permit_files_map = {}
        if file_ids:
            permit_files = list(db.permit_files.find(
                {"file_id": {"$in": file_ids}},
                {"_id": 0, "project_details": 1, "file_info": 1, "file_name": 1}
            ))
            permit_files_map = {pf["file_id"]: pf for pf in permit_files}
        
        def enrich_task_with_client_info(task):
            enriched_task = task.copy()
            permit_file_id = task.get("permit_file_id") or task.get("source", {}).get("permit_file_id")
            
            if permit_file_id and permit_file_id in permit_files_map:
                permit_file = permit_files_map[permit_file_id]
                enriched_task["client_info"] = {
                    "client_name": permit_file.get("project_details", {}).get("client_name"),
                    "project_name": permit_file.get("project_details", {}).get("project_name"),
                    "original_filename": permit_file.get("file_info", {}).get("original_filename")
                }
            
            # Format dates
            for date_field in ["assigned_at", "completed_at", "completion_time"]:
                if task.get(date_field) and isinstance(task[date_field], datetime):
                    enriched_task[date_field] = task[date_field].isoformat() + 'Z'
            
            return enriched_task
        
        # Enrich tasks
        enriched_assigned = [enrich_task_with_client_info(task) for task in assigned_tasks]
        enriched_completed = [enrich_task_with_client_info(task) for task in completed_tasks]
        
        # Apply pagination
        all_tasks = enriched_assigned + enriched_completed
        total_tasks = len(all_tasks)
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_tasks = all_tasks[start_idx:end_idx]
        
        # Calculate statistics
        stats = {
            "total_assigned": len(assigned_tasks),
            "total_completed": len(completed_tasks),
            "active_tasks": len([t for t in assigned_tasks if t.get("status") == "IN_PROGRESS"]),
            "pending_tasks": len([t for t in assigned_tasks if t.get("status") == "ASSIGNED"])
        }
        
        return APIResponse.paginated(
            data=paginated_tasks,
            total=total_tasks,
            page=page,
            limit=limit,
            message=f"Retrieved {len(paginated_tasks)} tasks for {employee_code}"
        )
        
    except Exception as e:
        logger.error(f"Failed to get unified tasks for {employee_code}: {str(e)}")
        return APIResponse.error(
            message=f"Failed to retrieve tasks: {str(e)}",
            error_code="TASK_RETRIEVAL_ERROR"
        )

@router.get("/employee/{employee_code}/summary")
async def get_employee_task_summary(employee_code: str):
    """
    Get summary statistics for employee tasks
    """
    try:
        db = get_db()
        
        # Get basic employee info
        employee = db.employee.find_one(
            {"employee_code": employee_code},
            {"employee_code": 1, "employee_name": 1, "_id": 0}
        )
        
        if not employee:
            return APIResponse.error(
                message=f"Employee {employee_code} not found",
                error_code="EMPLOYEE_NOT_FOUND"
            )
        
        # Get task counts
        assigned_count = db.tasks.count_documents({
            "assigned_to": employee_code,
            "status": {"$in": ["ASSIGNED", "IN_PROGRESS"]}
        })
        
        completed_count = db.tasks.count_documents({
            "assigned_to": employee_code,
            "status": "COMPLETED"
        })
        
        profile_completed_count = db.profile_building.count_documents({
            "employee_code": employee_code,
            "status": "COMPLETED"
        })
        
        # Get recent activity
        recent_tasks = list(db.tasks.find(
            {"assigned_to": employee_code},
            {"task_id": 1, "title": 1, "status": 1, "assigned_at": 1, "_id": 0}
        ).sort("assigned_at", -1).limit(5))
        
        # Format dates
        for task in recent_tasks:
            if task.get("assigned_at") and isinstance(task["assigned_at"], datetime):
                task["assigned_at"] = task["assigned_at"].isoformat() + 'Z'
        
        summary = {
            "employee": employee,
            "task_counts": {
                "assigned": assigned_count,
                "completed_from_tasks": completed_count,
                "completed_from_profile": profile_completed_count,
                "total_completed": completed_count + profile_completed_count
            },
            "recent_tasks": recent_tasks
        }
        
        return APIResponse.success(
            data=summary,
            message=f"Task summary retrieved for {employee_code}"
        )
        
    except Exception as e:
        logger.error(f"Failed to get task summary for {employee_code}: {str(e)}")
        return APIResponse.error(
            message=f"Failed to retrieve task summary: {str(e)}",
            error_code="SUMMARY_RETRIEVAL_ERROR"
        )
