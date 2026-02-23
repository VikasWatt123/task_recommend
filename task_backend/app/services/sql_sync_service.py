"""
SQL to MongoDB Data Synchronization Service
Handles syncing employee data from SQL (source of truth) to MongoDB (extended data)
"""
import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from app.db.mongodb import get_db
from app.db.mysql import mysql_service
from app.services.notification_service import get_notification_service

logger = logging.getLogger(__name__)

class SQLToMongoSyncService:
    """Service for syncing SQL data to MongoDB"""
    
    def __init__(self):
        self.mysql_service = mysql_service
        self.employee_table_name = None
        self.permit_files_table_name = None
    
    async def initialize(self):
        """Initialize the sync service by discovering table structures"""
        try:
            # Discover employee table
            employee_tables = self.mysql_service.get_employee_tables()
            if employee_tables:
                self.employee_table_name = employee_tables[0]
                logger.info(f"Using employee table: {self.employee_table_name}")
                
                # Get table structure (no logging of column details)
            else:
                raise ValueError("No employee tables found in SQL database")
            
            # Discover permit files table
            try:
                with self.mysql_service.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            SELECT table_name 
                            FROM information_schema.tables 
                            WHERE table_schema = %s 
                            AND (table_name LIKE '%%permit%%' OR table_name LIKE '%%file%%' OR table_name = 'permits')
                        """, (self.mysql_service.mysql_database,))
                        result = cursor.fetchall()
                        if result:
                            self.permit_files_table_name = result[0].get('table_name') or result[0].get('TABLE_NAME')
                            logger.info(f"Using permit files table: {self.permit_files_table_name}")
            except Exception as e:
                logger.warning(f"Could not find permit files table: {e}")
                
        except Exception as e:
            logger.error(f"Failed to initialize sync service: {e}")
            raise
    
    def map_sql_to_mongo_employee(self, sql_employee: Dict[str, Any]) -> Dict[str, Any]:
        """Map SQL employee data to MongoDB format"""
        # Try to find the employee code field
        code_field = None
        for field in ['kekaemployeecode', 'employee_code', 'emp_code', 'code']:
            if field in sql_employee:
                code_field = field
                break
        
        if not code_field:
            return None
        
        # Map common fields (adjust field names as needed)
        mapped_employee = {
            "kekaemployeecode": sql_employee.get(code_field),
            "employee_name": sql_employee.get('employee_name') or sql_employee.get('name') or sql_employee.get('emp_name'),
            "date_of_birth": sql_employee.get('date_of_birth') or sql_employee.get('dob'),
            "joining_date": sql_employee.get('joining_date') or sql_employee.get('join_date') or sql_employee.get('hire_date'),
            "experience_years": float(sql_employee.get('experience_years', 0)),
            "current_role": sql_employee.get('current_role') or sql_employee.get('role') or sql_employee.get('position'),
            "contact_email": sql_employee.get('contact_email') or sql_employee.get('email'),
            "shift": sql_employee.get('shift'),
            "status_1": sql_employee.get('status_1') or sql_employee.get('status') or sql_employee.get('employment_status'),
            "status_2": sql_employee.get('status_2'),
            "status_3": sql_employee.get('status_3'),
            # Map reporting manager
            "reporting_manager": sql_employee.get('reporting_manager') or sql_employee.get('manager_code') or sql_employee.get('reporting_manager_kekaemployeecode'),
            # Sync metadata
            "sync_info": {
                "sql_source": True,
                "last_synced": datetime.utcnow(),
                "sync_version": 1
            }
        }
        
        # Remove None values
        return {k: v for k, v in mapped_employee.items() if v is not None}
    
    async def sync_new_employee(self, sql_employee: Dict[str, Any]) -> Dict[str, Any]:
        """Sync a new employee from SQL to MongoDB"""
        try:
            db = get_db()
            mapped_employee = self.map_sql_to_mongo_employee(sql_employee)
            if not mapped_employee:
                return None
            kekaemployeecode = mapped_employee["kekaemployeecode"]
            
            # Check if employee already exists
            existing_employee = db.employee.find_one({"kekaemployeecode": kekaemployeecode})
            
            if existing_employee:
                # Update existing employee with SQL master data
                update_data = {k: v for k, v in mapped_employee.items() if k != "kekaemployeecode"}
                
                # Preserve existing skills, tasks, analytics, and other MongoDB-specific data
                preserved_fields = [
                    "skills", "technical_skills", "raw_technical_skills", "raw_strength_expertise",
                    "embedding", "metadata", "list_of_task_assigned", "special_task",
                    "onboarding", "tasks_assigned", "performance_metrics"
                ]
                
                for field in preserved_fields:
                    if field in existing_employee:
                        update_data[field] = existing_employee[field]
                
                # Update sync info
                update_data["sync_info"] = {
                    "sql_source": True,
                    "last_synced": datetime.utcnow(),
                    "sync_version": existing_employee.get("sync_info", {}).get("sync_version", 0) + 1
                }
                
                db.employee.update_one(
                    {"kekaemployeecode": kekaemployeecode},
                    {"$set": update_data}
                )
                
                logger.info(f"Updated existing employee {kekaemployeecode} from SQL")
                
                # Check if skills need to be collected
                if not existing_employee.get("skills") and not existing_employee.get("technical_skills"):
                    await self._trigger_skills_collection(kekaemployeecode, mapped_employee.get("employee_name"))
                
            else:
                # Create new employee record
                mapped_employee["onboarding"] = {
                    "status": "skills_pending",
                    "invited_at": datetime.utcnow(),
                    "completed_at": None
                }
                
                db.employee.insert_one(mapped_employee)
                logger.info(f"Created new employee {kekaemployeecode} from SQL")
                
                # Trigger skills collection for new employee
                await self._trigger_skills_collection(kekaemployeecode, mapped_employee.get("employee_name"))
            
            return mapped_employee
            
        except Exception as e:
            return None
    
    async def sync_employee_update(self, sql_employee: Dict[str, Any]) -> Dict[str, Any]:
        """Sync employee updates from SQL to MongoDB"""
        try:
            db = get_db()
            mapped_employee = self.map_sql_to_mongo_employee(sql_employee)
            if not mapped_employee:
                return None
            kekaemployeecode = mapped_employee["kekaemployeecode"]
            
            # Get existing employee
            existing_employee = db.employee.find_one({"kekaemployeecode": kekaemployeecode})
            
            if not existing_employee:
                # Employee doesn't exist, create them
                return await self.sync_new_employee(sql_employee)
            
            # Update only SQL master data fields, preserve MongoDB-specific data
            update_data = {k: v for k, v in mapped_employee.items() if k not in ["kekaemployeecode", "skills", "technical_skills", "embedding"]}
            
            # Preserve existing MongoDB-specific data
            preserved_fields = [
                "skills", "technical_skills", "raw_technical_skills", "raw_strength_expertise",
                "embedding", "metadata", "list_of_task_assigned", "special_task",
                "onboarding", "tasks_assigned", "performance_metrics"
            ]
            
            for field in preserved_fields:
                if field in existing_employee:
                    update_data[field] = existing_employee[field]
            
            # Update sync info
            update_data["sync_info"] = {
                "sql_source": True,
                "last_synced": datetime.utcnow(),
                "sync_version": existing_employee.get("sync_info", {}).get("sync_version", 0) + 1
            }
            
            db.employee.update_one(
                {"kekaemployeecode": kekaemployeecode},
                {"$set": update_data}
            )
            
            logger.info(f"Updated employee {kekaemployeecode} from SQL")
            return mapped_employee
            
        except Exception as e:
            logger.error(f"Error syncing employee update {sql_employee}: {e}")
            raise
    
    async def sync_all_employees(self) -> Dict[str, Any]:
        """Perform full sync of all employees from SQL to MongoDB"""
        try:
            logger.info("Starting full employee sync from SQL to MongoDB")
            
            sql_employees = self.mysql_service.get_all_employees(self.employee_table_name)
            sync_results = {
                "total_sql_employees": len(sql_employees),
                "synced": 0,
                "updated": 0,
                "created": 0,
                "errors": []
            }
            
            db = get_db()
            
            for sql_emp in sql_employees:
                try:
                    mapped_employee = self.map_sql_to_mongo_employee(sql_emp)
                    if not mapped_employee:
                        continue  # Skip records that don't have employee code fields
                    kekaemployeecode = mapped_employee["kekaemployeecode"]
                    
                    existing_employee = db.employee.find_one({"kekaemployeecode": kekaemployeecode})
                    
                    if existing_employee:
                        await self.sync_employee_update(sql_emp)
                        sync_results["updated"] += 1
                    else:
                        await self.sync_new_employee(sql_emp)
                        sync_results["created"] += 1
                    
                    sync_results["synced"] += 1
                    
                except Exception as e:
                    sync_results["errors"].append(f"Sync error: {e}")
            logger.info(f"Full sync completed: {sync_results['synced']} synced, {sync_results['created']} created, {sync_results['updated']} updated")
            return sync_results
            
        except Exception as e:
            logger.error(f"Full sync failed: {e}")
            raise
    
    async def _trigger_skills_collection(self, kekaemployeecode: str, employee_name: str):
        """Trigger skills collection for a new employee"""
        try:
            # This would integrate with your notification system
            logger.info(f"Triggering skills collection for {kekaemployeecode} - {employee_name}")
            
            # TODO: Implement actual notification logic
            # - Generate skills collection link
            # - Send email/notification to employee
            # - Track invitation status
            
        except Exception as e:
            logger.error(f"Error triggering skills collection for {kekaemployeecode}: {e}")
    
    async def sync_permit_files(self) -> Dict[str, Any]:
        """Sync permit files from SQL to MongoDB"""
        try:
            if not self.permit_files_table_name:
                logger.warning("No permit files table configured")
                return {"status": "skipped", "reason": "no_table"}
            
            logger.info("Starting permit files sync from SQL to MongoDB")
            
            sql_permit_files = self.mysql_service.get_permit_files(self.permit_files_table_name)
            
            # TODO: Implement permit files sync logic
            # - Map SQL permit files to MongoDB format
            # - Store in MongoDB permit_files collection
            # - Trigger analytics processing
            
            return {
                "status": "completed",
                "total_files": len(sql_permit_files)
            }
            
        except Exception as e:
            logger.error(f"Permit files sync failed: {e}")
            raise

# Global sync service instance
sync_service = SQLToMongoSyncService()
