"""
ClickHouse Backfill Script - Add tracking_mode Column
Adds tracking_mode column to task_events table and backfills existing data
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def backfill_tracking_mode():
    """Backfill tracking_mode for existing ClickHouse records"""
    
    try:
        from app.services.clickhouse_service import clickhouse_service, CLICKHOUSE_ENABLED
        
        if not CLICKHOUSE_ENABLED:
            logger.error("ClickHouse is disabled. Please enable it before running backfill.")
            return False
        
        logger.info("="*80)
        logger.info("ClickHouse Backfill Script - Adding tracking_mode Column")
        logger.info("="*80)
        
        # Step 1: Check if column already exists
        logger.info("\n[Step 1] Checking if tracking_mode column exists...")
        try:
            result = clickhouse_service.client.execute("""
                SELECT name 
                FROM system.columns 
                WHERE table = 'task_events' 
                AND database = 'task_analytics' 
                AND name = 'tracking_mode'
            """)
            
            if result:
                logger.info("✅ tracking_mode column already exists")
            else:
                logger.info("❌ tracking_mode column does not exist, will add it")
                
                # Add column
                logger.info("\n[Step 2] Adding tracking_mode column...")
                clickhouse_service.client.execute("""
                    ALTER TABLE task_events 
                    ADD COLUMN IF NOT EXISTS tracking_mode String DEFAULT 'FILE_BASED'
                """)
                logger.info("✅ Added tracking_mode column")
        except Exception as e:
            logger.error(f"Failed to check/add column: {e}")
            return False
        
        # Step 3: Count existing records
        logger.info("\n[Step 3] Counting existing records...")
        try:
            result = clickhouse_service.client.execute("SELECT COUNT(*) as count FROM task_events")
            total_records = result[0][0] if result else 0
            logger.info(f"Found {total_records} total records in task_events")
            
            if total_records == 0:
                logger.info("No records to backfill")
                return True
        except Exception as e:
            logger.error(f"Failed to count records: {e}")
            return False
        
        # Step 4: Update tracking_mode based on file_id
        logger.info("\n[Step 4] Updating tracking_mode for existing records...")
        logger.info("This may take a few minutes for large datasets...")
        
        try:
            # Update records with empty file_id to STANDALONE
            clickhouse_service.client.execute("""
                ALTER TABLE task_events 
                UPDATE tracking_mode = 'STANDALONE'
                WHERE file_id = '' OR file_id IS NULL
            """)
            logger.info("✅ Updated STANDALONE tasks (empty file_id)")
            
            # Update records with file_id to FILE_BASED
            clickhouse_service.client.execute("""
                ALTER TABLE task_events 
                UPDATE tracking_mode = 'FILE_BASED'
                WHERE file_id != '' AND file_id IS NOT NULL
            """)
            logger.info("✅ Updated FILE_BASED tasks (with file_id)")
            
        except Exception as e:
            logger.error(f"Failed to update tracking_mode: {e}")
            return False
        
        # Step 5: Optimize table to apply mutations
        logger.info("\n[Step 5] Optimizing table to apply mutations...")
        try:
            clickhouse_service.client.execute("OPTIMIZE TABLE task_events FINAL")
            logger.info("✅ Table optimized")
        except Exception as e:
            logger.warning(f"Failed to optimize table (non-critical): {e}")
        
        # Step 6: Verify results
        logger.info("\n[Step 6] Verifying backfill results...")
        try:
            result = clickhouse_service.client.execute("""
                SELECT 
                    tracking_mode,
                    COUNT(*) as count
                FROM task_events
                GROUP BY tracking_mode
                ORDER BY tracking_mode
            """)
            
            logger.info("Tracking mode distribution:")
            for row in result:
                logger.info(f"  - {row[0]}: {row[1]} records")
            
        except Exception as e:
            logger.error(f"Failed to verify results: {e}")
            return False
        
        logger.info("\n" + "="*80)
        logger.info("🎉 Backfill completed successfully!")
        logger.info("="*80)
        return True
        
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = backfill_tracking_mode()
    sys.exit(0 if success else 1)
