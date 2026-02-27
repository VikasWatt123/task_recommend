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
        # Use kekaemployeenumber (not kekaemployeecode)
        kekaemployeenumber = sql_employee.get('kekaemployeenumber')
        if not kekaemployeenumber:
            return None
        
        # Map only the 3 fields that exist in up_users
        mapped_employee = {
            "kekaemployeecode": kekaemployeenumber,  # Map kekaemployeenumber to kekaemployeecode
            "employee_name": sql_employee.get('fullname'),  # Use fullname field
            "contact_email": sql_employee.get('email'),  # Use email field
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
        """Update existing employee with SQL data (only email and fullname)"""
        try:
            db = get_db()
            mapped_employee = self.map_sql_to_mongo_employee(sql_employee)
            if not mapped_employee:
                return None
            
            kekaemployeecode = mapped_employee["kekaemployeecode"]
            
            # Get existing employee from MongoDB
            existing_employee = db.employee.find_one({"kekaemployeecode": kekaemployeecode})
            
            if not existing_employee:
                logger.warning(f"Employee {kekaemployeecode} not found in MongoDB for update")
                return None
            
            # Only update the 3 fields from SQL, preserve everything else
            update_data = {
                "employee_name": mapped_employee.get("employee_name"),
                "contact_email": mapped_employee.get("contact_email"),
                "sync_info": {
                    "sql_source": True,
                    "last_synced": datetime.utcnow(),
                    "sync_version": existing_employee.get("sync_info", {}).get("sync_version", 0) + 1
                }
            }
            
            # Perform update
            db.employee.update_one(
                {"kekaemployeecode": kekaemployeecode},
                {"$set": update_data}
            )
            
            logger.info(f"Updated employee {kekaemployeecode} with SQL data (email, fullname)")
            return mapped_employee
            
        except Exception as e:
            logger.error(f"Error syncing employee update {sql_employee}: {e}")
            raise
    
    async def sync_all_employees(self) -> Dict[str, Any]:
        """Sync existing MongoDB employees with SQL data (no new employees created)"""
        try:
            logger.info("Starting employee sync from SQL to MongoDB (existing employees only)")
            
            db = get_db()
            
            # Step 1: Get all existing employees from MongoDB
            mongo_employees = list(db.employee.find({}, {"kekaemployeecode": 1}))
            mongo_employee_codes = {emp["kekaemployeecode"] for emp in mongo_employees}
            
            sync_results = {
                "total_mongo_employees": len(mongo_employees),
                "matched_in_sql": 0,
                "updated": 0,
                "not_found_in_sql": [],
                "errors": []
            }
            
            if not mongo_employee_codes:
                logger.info("No employees found in MongoDB to sync")
                return sync_results
            
            # Step 2: Fetch only matching employees from SQL
            # Build WHERE clause for kekaemployeenumber
            employee_codes_list = list(mongo_employee_codes)
            placeholders = ', '.join(['%s'] * len(employee_codes_list))
            
            sql_employees = []
            with self.mysql_service.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = f"SELECT kekaemployeenumber, fullname, email FROM {self.employee_table_name} WHERE kekaemployeenumber IN ({placeholders})"
                    cursor.execute(query, employee_codes_list)
                    sql_employees = cursor.fetchall()
            
            # Create a mapping of kekaemployeenumber to employee data
            sql_employee_map = {emp['kekaemployeenumber']: emp for emp in sql_employees}
            
            # Step 3: Update only existing MongoDB employees
            for kekaemployeecode in mongo_employee_codes:
                try:
                    sql_emp = sql_employee_map.get(kekaemployeecode)
                    
                    if sql_emp:
                        # Employee found in SQL, update MongoDB
                        await self.sync_employee_update(sql_emp)
                        sync_results["updated"] += 1
                        sync_results["matched_in_sql"] += 1
                    else:
                        # Employee not found in SQL
                        sync_results["not_found_in_sql"].append(kekaemployeecode)
                        logger.warning(f"Employee {kekaemployeecode} not found in SQL table")
                    
                except Exception as e:
                    sync_results["errors"].append(f"Sync error for {kekaemployeecode}: {e}")
                    logger.error(f"Error syncing employee {kekaemployeecode}: {e}")
            
            logger.info(f"Sync completed: {sync_results['updated']} updated, {sync_results['matched_in_sql']} matched in SQL, {len(sync_results['not_found_in_sql'])} not found")
            return sync_results
            
        except Exception as e:
            logger.error(f"Employee sync failed: {e}")
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
