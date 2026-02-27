"""
Backup sync service for periodic synchronization
Runs scheduled syncs to ensure data consistency between SQL and MongoDB
"""
import asyncio
import logging
from typing import Dict, Any
from datetime import datetime, timedelta
from app.services.sql_sync_service import sync_service
from app.db.mongodb import get_db

logger = logging.getLogger(__name__)

class BackupSyncService:
    """Service for periodic backup synchronization"""
    
    def __init__(self):
        self.sync_service = sync_service
        self.last_sync_time: Optional[datetime] = None
        self.sync_interval_minutes = 10080  # Sync every week (7 days = 10080 minutes)
        self.is_running = False
        self.sync_task: Optional[asyncio.Task] = None
    
    async def start_periodic_sync(self):
        """Start the periodic sync process"""
        if self.is_running:
            logger.warning("Periodic sync is already running")
            return
        
        self.is_running = True
        logger.info("Starting periodic backup sync service")
        
        try:
            # Initialize sync service
            await self.sync_service.initialize()
            
            while self.is_running:
                try:
                    await self.perform_backup_sync()
                    await asyncio.sleep(self.sync_interval_minutes * 60)
                except Exception as e:
                    logger.error(f"Error in periodic sync: {e}")
                    await asyncio.sleep(60)  # Wait 1 minute before retrying
        
        except Exception as e:
            logger.error(f"Fatal error in periodic sync service: {e}")
        finally:
            self.is_running = False
            logger.info("Periodic backup sync service stopped")
    
    def stop_periodic_sync(self):
        """Stop the periodic sync process"""
        self.is_running = False
        logger.info("Stopping periodic backup sync service")
    
    async def perform_backup_sync(self) -> Dict[str, Any]:
        """Perform a backup sync operation"""
        try:
            logger.info("Starting backup sync operation")
            start_time = datetime.utcnow()
            
            # Sync employees
            employee_sync_result = await self.sync_service.sync_all_employees()
            
            # Sync permit files
            permit_sync_result = await self.sync_service.sync_permit_files()
            
            # Check for data consistency
            consistency_check = await self.perform_consistency_check()
            
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            sync_result = {
                "status": "completed",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "employee_sync": employee_sync_result,
                "permit_sync": permit_sync_result,
                "consistency_check": consistency_check
            }
            
            self.last_sync_time = end_time
            logger.info(f"Backup sync completed in {duration:.2f} seconds")
            
            return sync_result
            
        except Exception as e:
            logger.error(f"Backup sync failed: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def perform_consistency_check(self) -> Dict[str, Any]:
        """Check data consistency between SQL and MongoDB"""
        try:
            db = get_db()
            
            # Get counts
            sql_employees = self.sync_service.mysql_service.get_all_employees(self.sync_service.employee_table_name)
            mongo_employees = list(db.employee.find({}, {"kekaemployeecode": 1, "_id": 0}))
            
            sql_codes = set()
            for emp in sql_employees:
                mapped = self.sync_service.map_sql_to_mongo_employee(emp)
                if mapped:
                    sql_codes.add(mapped.get("kekaemployeecode"))
            
            mongo_codes = set(emp.get("kekaemployeecode") for emp in mongo_employees if emp.get("kekaemployeecode"))
            
            # Find discrepancies
            missing_in_mongo = sql_codes - mongo_codes
            extra_in_mongo = mongo_codes - sql_codes
            
            consistency_result = {
                "sql_employee_count": len(sql_codes),
                "mongo_employee_count": len(mongo_codes),
                "missing_in_mongo_count": len(missing_in_mongo),
                "extra_in_mongo_count": len(extra_in_mongo),
                "missing_in_mongo": list(missing_in_mongo),
                "extra_in_mongo": list(extra_in_mongo),
                "is_consistent": len(missing_in_mongo) == 0 and len(extra_in_mongo) == 0
            }
            
            if not consistency_result["is_consistent"]:
                logger.warning(f"Data inconsistency detected: {consistency_result}")
            else:
                logger.info("Data consistency check passed")
            
            return consistency_result
            
        except Exception as e:
            logger.error(f"Consistency check failed: {e}")
            return {
                "status": "failed",
                "error": str(e)
            }
    
    async def sync_specific_employee(self, kekaemployeecode: str) -> Dict[str, Any]:
        """Sync a specific employee by code"""
        try:
            logger.info(f"Syncing specific employee: {kekaemployeecode}")
            
            # Get employee from SQL
            sql_employee = self.sync_service.mysql_service.get_employee_by_code(kekaemployeecode)
            
            if not sql_employee:
                return {
                    "status": "failed",
                    "error": f"Employee {kekaemployeecode} not found in SQL database"
                }
            
            # Sync to MongoDB
            result = await self.sync_service.sync_employee_update(sql_employee)
            
            return {
                "status": "success",
                "message": f"Employee {kekaemployeecode} synced successfully",
                "employee": result,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to sync employee {kekaemployeecode}: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def get_sync_status(self) -> Dict[str, Any]:
        """Get current sync service status"""
        return {
            "is_running": self.is_running,
            "last_sync_time": self.last_sync_time.isoformat() if self.last_sync_time else None,
            "sync_interval_minutes": self.sync_interval_minutes,
            "next_sync_time": (self.last_sync_time + timedelta(minutes=self.sync_interval_minutes)).isoformat() if self.last_sync_time else None,
            "employee_table": self.sync_service.employee_table_name,
            "permit_files_table": self.sync_service.permit_files_table_name
        }

# Global backup sync service instance
backup_sync_service = BackupSyncService()
