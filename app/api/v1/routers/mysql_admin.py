"""
MySQL Integration Admin Router
Provides administrative endpoints for managing SQL-MongoDB integration
"""
from fastapi import APIRouter, HTTPException, Depends, status, Query
from typing import Dict, Any, Optional
import logging
from datetime import datetime

from app.services.sql_sync_service import sync_service
from app.services.backup_sync_service import backup_sync_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/mysql", tags=["mysql-admin"])

@router.get("/status")
async def get_mysql_integration_status():
    """Get current MySQL integration status"""
    try:
        # Test basic connections
        mysql_status = sync_service.mysql_service.test_mysql_connection()
        ssh_status = sync_service.mysql_service.test_ssh_connection()
        
        # Get sync service status
        sync_status = backup_sync_service.get_sync_status()
        
        return {
            "status": "active" if mysql_status else "inactive",
            "mysql_connection": "connected" if mysql_status else "disconnected",
            "ssh_connection": "connected" if ssh_status else "disconnected",
            "sync_service": sync_status,
            "employee_table": sync_service.employee_table_name,
            "permit_files_table": sync_service.permit_files_table_name,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting MySQL status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get status: {str(e)}"
        )

@router.post("/sync/full")
async def trigger_full_sync():
    """Trigger a full synchronization from SQL to MongoDB"""
    try:
        logger.info("Manual full sync triggered")
        result = await backup_sync_service.perform_backup_sync()
        return result
        
    except Exception as e:
        logger.error(f"Manual full sync failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Full sync failed: {str(e)}"
        )

@router.post("/sync/employee/{kekaemployeecode}")
async def sync_specific_employee(kekaemployeecode: str):
    """Sync a specific employee by code"""
    try:
        result = await backup_sync_service.sync_specific_employee(kekaemployeecode)
        return result
        
    except Exception as e:
        logger.error(f"Employee sync failed for {kekaemployeecode}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Employee sync failed: {str(e)}"
        )

@router.get("/employees/sql")
async def get_sql_employees():
    """Get all employees from SQL database"""
    try:
        employees = sync_service.mysql_service.get_all_employees(sync_service.employee_table_name)
        return {
            "status": "success",
            "count": len(employees),
            "employees": employees,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting SQL employees: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get SQL employees: {str(e)}"
        )

@router.get("/employees/compare")
async def compare_sql_mongo_employees():
    """Compare employee data between SQL and MongoDB"""
    try:
        consistency_check = await backup_sync_service.perform_consistency_check()
        return consistency_check
        
    except Exception as e:
        logger.error(f"Error comparing employees: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Comparison failed: {str(e)}"
        )

@router.get("/tables")
async def get_database_tables():
    """Get list of tables and their structures"""
    try:
        employee_tables = sync_service.mysql_service.get_employee_tables()
        
        tables_info = {}
        for table in employee_tables:
            structure = sync_service.mysql_service.get_table_structure(table)
            tables_info[table] = structure
        
        return {
            "status": "success",
            "employee_tables": employee_tables,
            "table_structures": tables_info,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting table info: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get table info: {str(e)}"
        )

@router.post("/test-connection")
async def test_mysql_connection():
    """Test MySQL and SSH connections"""
    try:
        ssh_result = sync_service.mysql_service.test_ssh_connection()
        mysql_result = sync_service.mysql_service.test_mysql_connection()
        
        return {
            "ssh_connection": "success" if ssh_result else "failed",
            "mysql_connection": "success" if mysql_result else "failed",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return {
            "ssh_connection": "failed",
            "mysql_connection": "failed",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
