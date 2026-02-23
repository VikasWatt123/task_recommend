"""
Sync Service - MongoDB to ClickHouse
Automated data synchronization for high-performance analytics
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from app.services.clickhouse_service import clickhouse_service
from app.db.mongodb import get_db

logger = logging.getLogger(__name__)

class SyncService:
    """Service for syncing MongoDB data to ClickHouse"""
    
    def __init__(self):
        self.last_sync_time = None
        self.sync_interval = 300  # 5 minutes
        self.batch_size = 1000
    
    async def start_sync_worker(self):
        """Background worker to continuously sync data"""
        logger.info("🔄 Starting MongoDB to ClickHouse sync worker (5-minute interval)")
        
        while True:
            try:
                await self.sync_recent_data()
                await asyncio.sleep(self.sync_interval)
            except Exception as e:
                logger.error(f"Sync worker error: {e}")
                await asyncio.sleep(60)  # Wait before retrying
    
    async def sync_recent_data(self):
        """Sync recent data from MongoDB to ClickHouse"""
        try:
            # For initial sync, get all data
            if self.last_sync_time is None:
                since = None  # Sync all data for initial run
                logger.info("Performing initial full sync of all MongoDB data")
            else:
                # For subsequent syncs, get data since last sync
                since = self.last_sync_time
                logger.info(f"Performing incremental sync since {self.last_sync_time}")
            
            await clickhouse_service.sync_tasks_from_mongodb(since=since)
            
            # Sync employee performance daily
            now = datetime.utcnow()
            if now.hour == 0 or self.last_sync_time is None:  # Once daily or first run
                await clickhouse_service.sync_employee_performance(days=30)
            
            self.last_sync_time = datetime.utcnow()
            logger.info(f"Sync completed at {self.last_sync_time}")
            
        except Exception as e:
            logger.error(f"Failed to sync recent data: {e}")
    
    async def sync_task_completion(self, task_id: str, employee_code: str):
        """Sync specific task completion to ClickHouse"""
        try:
            # Update real-time metrics
            await clickhouse_service.update_real_time_metric(
                "tasks_completed_today",
                1.0,
                {"employee_code": employee_code, "task_id": task_id}
            )
            
            # Update employee efficiency
            await clickhouse_service.update_real_time_metric(
                "employee_efficiency",
                95.0,  # Example efficiency score
                {"employee_code": employee_code}
            )
            
            logger.info(f"Synced task completion: {task_id}")
            
        except Exception as e:
            logger.error(f"Failed to sync task completion: {e}")
    
    async def sync_sla_breach(self, task_data: dict):
        """Sync SLA breach to ClickHouse"""
        try:
            await clickhouse_service.update_real_time_metric(
                "sla_breaches_today",
                1.0,
                {
                    "stage": task_data.get("stage"),
                    "employee_code": task_data.get("assigned_to"),
                    "hours_overdue": task_data.get("hours_overdue", 0)
                }
            )
            
            logger.warning(f"SLA breach synced: {task_data.get('task_id')}")
            
        except Exception as e:
            logger.error(f"Failed to sync SLA breach: {e}")
    
    async def get_sync_status(self):
        """Get current sync status"""
        return {
            "last_sync": self.last_sync_time.isoformat() if self.last_sync_time else None,
            "sync_interval": self.sync_interval,
            "next_sync_in": self.sync_interval if not self.last_sync_time else 0
        }

# Global sync service instance
sync_service = SyncService()
