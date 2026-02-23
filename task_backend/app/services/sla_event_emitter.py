"""
Async SLA event emitter for WebSocket notifications
Runs on main event loop only
"""
import asyncio
import logging
from typing import List, Dict, Any
from app.services.clickhouse_service import clickhouse_service
from app.services.websocket_manager import websocket_manager

logger = logging.getLogger(__name__)

class SLAEventEmitter:
    """Handles async emission of SLA events to WebSocket"""
    
    def __init__(self):
        self.running = False
        self.task = None
    
    async def start(self):
        """Start the emitter loop"""
        if self.running:
            return
        
        self.running = True
        self.task = asyncio.create_task(self._emitter_loop())
        logger.info("SLA event emitter started")
    
    async def stop(self):
        """Stop the emitter loop"""
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info("SLA event emitter stopped")
    
    async def _emitter_loop(self):
        """Main emitter loop - runs on async event loop only"""
        while self.running:
            try:
                # Temporarily disabled to prevent notification spam
                # TODO: Fix the emitted flag update in ClickHouse before re-enabling
                logger.info("SLA event emitter loop disabled to prevent notification spam")
                await asyncio.sleep(60)  # Sleep for 1 minute instead of processing
                continue
                
                # Fetch unemitted SLA events
                unemitted = clickhouse_service.client.execute("""
                    SELECT event_id, file_id, event_type, event_time
                    FROM file_events
                    WHERE event_type = 'SLA_BREACH' AND emitted = 0
                    ORDER BY event_time
                    LIMIT 100
                """)
                
                if unemitted:
                    logger.info(f"Processing {len(unemitted)} unemitted SLA events")
                
                for event_id, file_id, event_type, event_time in unemitted:
                    try:
                        # Emit WebSocket notification
                        await websocket_manager.notify_sla_breached(
                            file_id=file_id,
                            stage="BREACHED",
                            employee_code="system"
                        )
                        
                        # Mark as emitted
                        clickhouse_service.client.execute(
                            f"ALTER TABLE file_events UPDATE emitted = 1 WHERE event_id = '{event_id}'"
                        )
                        
                        logger.info(f"Emitted SLA breach for {file_id}")
                        
                    except Exception as e:
                        logger.error(f"Failed to emit SLA breach for {file_id}: {e}")
                        # Continue with next event
                
                # Wait before next poll
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Error in SLA emitter loop: {e}")
                await asyncio.sleep(30)  # Wait longer on error

# Global emitter instance
_sla_emitter = SLAEventEmitter()

def get_sla_emitter() -> SLAEventEmitter:
    """Get the global SLA emitter instance"""
    return _sla_emitter
