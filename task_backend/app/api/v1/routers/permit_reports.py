"""
Permit File Reports Router
Provides detailed completion reports for permit files
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from app.db.mongodb import get_db
from app.models.stage_flow import FileStage

router = APIRouter(prefix="/permit-files", tags=["permit-reports"])
logger = logging.getLogger(__name__)

@router.get("/{file_id}/completion-report")
async def get_file_completion_report(file_id: str):
    """
    Get detailed completion report for a permit file including:
    - Who completed each stage
    - Time taken for each stage
    - Stage history timeline
    """
    db = get_db()
    
    # Get file tracking info
    file_tracking = db.permit_files.find_one({"file_id": file_id})
    if not file_tracking:
        raise HTTPException(status_code=404, detail="File not found")
    
    # Get stage history
    stage_history = list(db.stage_history.find(
        {"file_id": file_id},
        {"_id": 0}
    ).sort("started_at", 1))
    
    # Get tasks for this file
    tasks = list(db.tasks.find(
        {"source.permit_file_id": file_id},
        {"_id": 0, "embeddings": 0}
    ).sort("assigned_at", 1))
    
    # Build completion report
    report = {
        "file_id": file_id,
        "file_name": file_tracking.get("file_name", file_tracking.get("original_filename", "Unknown")),
        "current_stage": file_tracking.get("status", "PENDING"),
        "created_at": file_tracking.get("metadata", {}).get("created_at") or file_tracking.get("file_info", {}).get("uploaded_at"),
        "updated_at": file_tracking.get("metadata", {}).get("updated_at"),
        "stages_completed": {},
        "total_duration_minutes": 0,
        "stage_timeline": [],
        "task_summary": {
            "by_stage": {},
            "total_tasks": 0,
            "completed_tasks": 0,
            "active_tasks": 0
        }
    }
    
    # Process stage history
    for stage_entry in stage_history:
        stage = stage_entry.get("stage")
        started_at = stage_entry.get("started_at") or stage_entry.get("entered_stage_at")
        completed_at = stage_entry.get("completed_at")
        
        # Get employee info from nested structure
        assigned_to = stage_entry.get("assigned_to") or {}
        employee_code = assigned_to.get("employee_code")
        employee_name = assigned_to.get("employee_name")
        
        if started_at:
            duration_minutes = 0
            if completed_at:
                duration = completed_at - started_at
                duration_minutes = int(duration.total_seconds() / 60)
                report["total_duration_minutes"] += duration_minutes
            
            report["stages_completed"][stage] = {
                "employee_code": employee_code,
                "employee_name": employee_name,
                "started_at": started_at.isoformat() + 'Z' if started_at else None,
                "completed_at": completed_at.isoformat() + 'Z' if completed_at else None,
                "duration_minutes": duration_minutes,
                "status": "COMPLETED" if completed_at else "IN_PROGRESS"
            }
            
            report["stage_timeline"].append({
                "stage": stage,
                "employee_name": employee_name,
                "started_at": started_at.isoformat() + 'Z' if started_at else None,
                "completed_at": completed_at.isoformat() + 'Z' if completed_at else None,
                "duration_minutes": duration_minutes
            })
    
    # Process tasks by stage
    for task in tasks:
        task_stage = task.get("stage", "UNASSIGNED")
        if task_stage not in report["task_summary"]["by_stage"]:
            report["task_summary"]["by_stage"][task_stage] = {
                "total": 0,
                "completed": 0,
                "employees": {}
            }
        
        report["task_summary"]["by_stage"][task_stage]["total"] += 1
        if task.get("status") == "COMPLETED":
            report["task_summary"]["by_stage"][task_stage]["completed"] += 1
        
        assigned_to = task.get("assigned_to")
        if assigned_to:
            if assigned_to not in report["task_summary"]["by_stage"][task_stage]["employees"]:
                report["task_summary"]["by_stage"][task_stage]["employees"][assigned_to] = {
                    "employee_name": task.get("assigned_to_name", "Unknown"),
                    "tasks": []
                }
            report["task_summary"]["by_stage"][task_stage]["employees"][assigned_to]["tasks"].append({
                "task_id": task.get("task_id"),
                "title": task.get("title"),
                "assigned_at": task.get("assigned_at"),
                "completed_at": task.get("completed_at"),
                "status": task.get("status")
            })
    
    # Initialize task counters
    active_tasks = 0
    completed_tasks = 0
    
    # Define SLA thresholds (in minutes)
    SLA_THRESHOLDS = {
        "PRELIMS": {"ideal": 60, "max": 120},
        "PRODUCTION": {"ideal": 180, "max": 360},
        "QC": {"ideal": 30, "max": 60}
    }
    
    # Initialize SLA summary before the loop
    report["sla_summary"] = {
        "total_breaches": 0,
        "total_penalties": 0,
        "breach_details": []
    }
    
    # Check stage history for SLA breaches
    for stage_entry in stage_history:
        stage = stage_entry.get("stage")
        started_at = stage_entry.get("started_at") or stage_entry.get("entered_stage_at")
        completed_at = stage_entry.get("completed_at")
        
        if started_at and stage in SLA_THRESHOLDS:
            duration = 0
            if completed_at:
                duration = int((completed_at - started_at).total_seconds() / 60)
            else:
                # For incomplete stages, calculate duration from start to now
                duration = int((datetime.utcnow() - started_at).total_seconds() / 60)
            
            thresholds = SLA_THRESHOLDS[stage]
            if duration > thresholds["max"]:
                report["sla_summary"]["total_breaches"] += 1
                # Calculate penalty: 10 points per hour over max
                hours_over = (duration - thresholds["max"]) / 60
                penalty_points = max(1, int(hours_over)) * 10  # At least 1 hour over = 10 points
                report["sla_summary"]["total_penalties"] += penalty_points
                
                # Add breach details
                report["sla_summary"]["breach_details"].append({
                    "stage": stage,
                    "duration": duration,
                    "threshold": thresholds["max"],
                    "over_by": duration - thresholds["max"],
                    "penalty": penalty_points
                })
    
    # Count active and completed tasks
    for task in tasks:
        if task.get("status") == "COMPLETED":
            completed_tasks += 1
        elif task.get("status") in ["ASSIGNED", "IN_PROGRESS"]:
            active_tasks += 1
    
    report["task_summary"]["total_tasks"] = len(tasks)
    report["task_summary"]["completed_tasks"] = completed_tasks
    report["task_summary"]["active_tasks"] = active_tasks
    
    return report

@router.get("/{file_id}/stage-summary")
async def get_file_stage_summary(file_id: str):
    """
    Get summary of stages and who worked on them
    """
    db = get_db()
    
    # Get stage history
    stage_history = list(db.stage_history.find(
        {"file_id": file_id},
        {"_id": 0, "file_id": 0}
    ).sort("started_at", 1))
    
    summary = {
        "file_id": file_id,
        "stages": []
    }
    
    for entry in stage_history:
        stage = entry.get("stage")
        if stage:
            duration_minutes = 0
            if entry.get("started_at") and entry.get("completed_at"):
                duration = entry["completed_at"] - entry["started_at"]
                duration_minutes = int(duration.total_seconds() / 60)
            
            summary["stages"].append({
                "stage": stage,
                "employee_code": entry.get("employee_code"),
                "employee_name": entry.get("employee_name"),
                "started_at": entry.get("started_at").isoformat() + 'Z' if entry.get("started_at") else None,
                "completed_at": entry.get("completed_at").isoformat() + 'Z' if entry.get("completed_at") else None,
                "duration_minutes": duration_minutes,
                "notes": entry.get("notes", "")
            })
    
    return summary
