"""
Notification router for handling real-time notifications
"""
from fastapi import APIRouter, HTTPException
from typing import Dict, Any
from app.services.websocket_manager import websocket_manager

router = APIRouter(prefix="/notifications", tags=["notifications"])

@router.post("/task-assigned")
async def notify_task_assigned(notification: Dict[str, Any]):
    """Send notification when task is assigned"""
    try:
        await websocket_manager.notify_task_assigned(
            file_id=notification["file_id"],
            employee_name=notification["employee_name"],
            employee_code=notification["employee_code"],
            task_id=notification["task_id"],
            stage=notification["stage"]
        )
        return {"success": True, "message": "Notification sent"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/stage-completed")
async def notify_stage_completed(notification: Dict[str, Any]):
    """Send notification when stage is completed"""
    try:
        await websocket_manager.notify_stage_completed(
            file_id=notification["file_id"],
            employee_name=notification["employee_name"],
            employee_code=notification["employee_code"],
            stage=notification["stage"],
            quality_score=notification.get("quality_score", 0.0)
        )
        return {"success": True, "message": "Notification sent"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sla-breached")
async def notify_sla_breached(notification: Dict[str, Any]):
    """Send notification when SLA is breached"""
    try:
        await websocket_manager.notify_sla_breached(
            file_id=notification["file_id"],
            stage=notification["stage"],
            employee_code=notification["employee_code"],
            employee_name=notification.get("employee_name")
        )
        return {"success": True, "message": "Notification sent"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
