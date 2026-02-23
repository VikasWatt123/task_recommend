"""
Webhook endpoints for SQL application integration
Handles real-time data synchronization from SQL application to MongoDB
"""
from fastapi import APIRouter, HTTPException, Depends, status
from pydantic import BaseModel
from typing import Dict, Any, Optional
from datetime import datetime
import logging

from app.services.sql_sync_service import sync_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/webhook", tags=["webhooks"])

# Pydantic models for webhook data
class SQLEmployeeData(BaseModel):
    """Model for SQL employee data from webhooks"""
    # Basic fields (adjust based on your SQL schema)
    kekaemployeecode: Optional[str] = None
    employee_code: Optional[str] = None
    employee_name: Optional[str] = None
    date_of_birth: Optional[str] = None
    joining_date: Optional[str] = None
    experience_years: Optional[float] = None
    current_role: Optional[str] = None
    contact_email: Optional[str] = None
    shift: Optional[str] = None
    status_1: Optional[str] = None
    status_2: Optional[str] = None
    status_3: Optional[str] = None
    reporting_manager: Optional[str] = None
    
    class Config:
        extra = "allow"  # Allow additional fields from SQL

class SQLPermitFileData(BaseModel):
    """Model for SQL permit file data from webhooks"""
    permit_id: Optional[str] = None
    permit_name: Optional[str] = None
    file_path: Optional[str] = None
    status: Optional[str] = None
    assigned_to: Optional[str] = None
    created_by: Optional[str] = None
    upload_date: Optional[str] = None
    
    class Config:
        extra = "allow"

@router.post("/employee/new")
async def employee_created_webhook(employee_data: SQLEmployeeData):
    """
    Webhook endpoint for new employee creation in SQL
    Called by SQL application when a new employee is added
    """
    try:
        logger.info(f"Received webhook for new employee: {employee_data}")
        
        # Convert to dict for processing
        employee_dict = employee_data.dict(exclude_unset=True)
        
        # Sync to MongoDB
        result = await sync_service.sync_new_employee(employee_dict)
        
        return {
            "status": "success",
            "message": "Employee synced successfully",
            "kekaemployeecode": result.get("kekaemployeecode"),
            "employee_name": result.get("employee_name"),
            "onboarding_status": result.get("onboarding", {}).get("status"),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error processing employee creation webhook: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to sync employee: {str(e)}"
        )

@router.post("/employee/update")
async def employee_updated_webhook(employee_data: SQLEmployeeData):
    """
    Webhook endpoint for employee updates in SQL
    Called by SQL application when employee data is updated
    """
    try:
        logger.info(f"Received webhook for employee update: {employee_data}")
        
        # Convert to dict for processing
        employee_dict = employee_data.dict(exclude_unset=True)
        
        # Sync to MongoDB
        result = await sync_service.sync_employee_update(employee_dict)
        
        return {
            "status": "success",
            "message": "Employee updated successfully",
            "kekaemployeecode": result.get("kekaemployeecode"),
            "employee_name": result.get("employee_name"),
            "last_synced": result.get("sync_info", {}).get("last_synced"),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error processing employee update webhook: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update employee: {str(e)}"
        )

@router.post("/employee/delete")
async def employee_deleted_webhook(employee_data: SQLEmployeeData):
    """
    Webhook endpoint for employee deletion in SQL
    Called by SQL application when an employee is deleted
    """
    try:
        logger.info(f"Received webhook for employee deletion: {employee_data}")
        
        # Find the employee code
        employee_code = employee_data.kekaemployeecode or employee_data.employee_code
        if not employee_code:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Employee code is required for deletion"
            )
        
        from app.db.mongodb import get_db
        db = get_db()
        
        # Soft delete by marking as inactive
        result = db.employee.update_one(
            {"kekaemployeecode": employee_code},
            {
                "$set": {
                    "status_1": "DELETED",
                    "sync_info": {
                        "sql_source": True,
                        "last_synced": datetime.utcnow(),
                        "deleted_at": datetime.utcnow(),
                        "sync_version": 1
                    }
                }
            }
        )
        
        if result.matched_count == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Employee not found in MongoDB"
            )
        
        return {
            "status": "success",
            "message": "Employee marked as deleted",
            "kekaemployeecode": employee_code,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error processing employee deletion webhook: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete employee: {str(e)}"
        )

@router.post("/permit-file/new")
async def permit_file_created_webhook(permit_data: SQLPermitFileData):
    """
    Webhook endpoint for new permit file creation in SQL
    Called by SQL application when a new permit file is added
    """
    try:
        logger.info(f"Received webhook for new permit file: {permit_data}")
        
        # TODO: Implement permit file sync logic
        # - Store permit file in MongoDB
        # - Trigger analytics processing
        # - Update ClickHouse if needed
        
        return {
            "status": "success",
            "message": "Permit file received successfully",
            "permit_id": permit_data.permit_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error processing permit file webhook: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process permit file: {str(e)}"
        )

@router.post("/permit-file/update")
async def permit_file_updated_webhook(permit_data: SQLPermitFileData):
    """
    Webhook endpoint for permit file updates in SQL
    Called by SQL application when a permit file is updated
    """
    try:
        logger.info(f"Received webhook for permit file update: {permit_data}")
        
        # TODO: Implement permit file update logic
        
        return {
            "status": "success",
            "message": "Permit file updated successfully",
            "permit_id": permit_data.permit_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error processing permit file update webhook: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update permit file: {str(e)}"
        )

@router.get("/status")
async def webhook_status():
    """
    Check webhook and sync service status
    """
    try:
        # Test MySQL connection
        mysql_status = sync_service.mysql_service.test_mysql_connection()
        
        return {
            "status": "active",
            "mysql_connection": "connected" if mysql_status else "disconnected",
            "employee_table": sync_service.employee_table_name,
            "permit_files_table": sync_service.permit_files_table_name,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error checking webhook status: {e}")
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
