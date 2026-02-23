"""
Enhanced ClickHouse service for comprehensive file lifecycle tracking
"""
import json
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional
from app.services.clickhouse_service import CLICKHOUSE_ENABLED, clickhouse_service
import logging

logger = logging.getLogger(__name__)

class ClickHouseLifecycleService:
    """Enhanced ClickHouse service for complete file lifecycle tracking"""
    
    def emit_file_lifecycle_event(self, file_id: str, event_type: str, 
                                 stage: str, employee_code: str = None,
                                 employee_name: str = None, 
                                 event_data: Dict = None):
        """Emit a file lifecycle event to ClickHouse"""
        
        if not CLICKHOUSE_ENABLED:
            logger.info(f"ClickHouse disabled - skipping lifecycle event for {file_id}")
            return
        
        try:
            # Get previous state for context
            previous_state = self._get_current_state(file_id)
            
            event = {
                'event_id': str(uuid.uuid4()),
                'file_id': file_id,
                'event_type': event_type,
                'stage': stage,
                'employee_code': employee_code or '',
                'employee_name': employee_name or '',
                'event_time': datetime.utcnow(),
                'event_data': json.dumps(event_data or {}),
                'previous_stage': previous_state.get('current_stage') if previous_state else None,
                'next_stage': stage if event_type in ['STAGE_STARTED', 'STAGE_TRANSITION'] else None,
                'duration_minutes': self._calculate_duration(file_id, event_type, previous_state) or 0,
                'created_at': datetime.utcnow()
            }
            
            # Insert into events table
            clickhouse_service.client.execute(
                'INSERT INTO task_analytics.file_lifecycle_events (*) VALUES',
                [event]
            )
            
            # Update current state table
            self._update_current_state(file_id, event_type, event, previous_state)
            
            logger.info(f"✅ Emitted lifecycle event {event_type} for file {file_id}")
            
        except Exception as e:
            logger.error(f"Failed to emit lifecycle event for {file_id}: {e}")

    def _get_current_state(self, file_id: str) -> Optional[Dict]:
        """Get current state of a file"""
        if not CLICKHOUSE_ENABLED:
            return None
        
        try:
            query = "SELECT * FROM task_analytics.file_current_state WHERE file_id = %(file_id)s LIMIT 1"
            result = clickhouse_service.client.execute(query, {'file_id': file_id})
            return result[0] if result else None
        except Exception as e:
            logger.warning(f"Failed to get current state for {file_id}: {e}")
            return None

    def _update_current_state(self, file_id: str, event_type: str, event: Dict, previous_state: Dict):
        """Update the current state table"""
        if not CLICKHOUSE_ENABLED:
            return
        
        try:
            now = datetime.utcnow()
            
            if event_type in ['STAGE_STARTED', 'STAGE_ASSIGNED']:
                # New stage starting
                state_update = {
                    'file_id': file_id,
                    'current_stage': event['stage'],
                    'current_status': f"IN_{event['stage']}",
                    'current_employee_code': event['employee_code'],
                    'current_employee_name': event['employee_name'],
                    'last_updated': now,
                    'total_duration_minutes': previous_state.get('total_duration_minutes', 0) if previous_state else 0
                }
            elif event_type == 'STAGE_COMPLETED':
                # Stage completed
                total_duration = previous_state.get('total_duration_minutes', 0) if previous_state else 0
                # We don't have stage_duration_minutes in previous_state, so we just use what's there
                state_update = {
                    'file_id': file_id,
                    'current_stage': previous_state.get('current_stage') if previous_state else None,
                    'current_status': 'COMPLETED',
                    'last_updated': now,
                    'total_duration_minutes': total_duration
                }
            elif event_type == 'FILE_DELIVERED':
                # Final delivery
                state_update = {
                    'file_id': file_id,
                    'current_stage': 'DELIVERED',
                    'current_status': 'DELIVERED',
                    'last_updated': now
                }
            else:
                # Other events - just update timestamp
                state_update = {
                    'file_id': file_id,
                    'last_updated': now
                }
            
            clickhouse_service.client.execute(
                'INSERT INTO task_analytics.file_current_state (*) VALUES',
                [state_update]
            )
            
        except Exception as e:
            logger.warning(f"Failed to update current state for {file_id}: {e}")

    def _calculate_duration(self, file_id: str, event_type: str, previous_state: Dict) -> int:
        """Calculate duration for lifecycle events"""
        if event_type == 'STAGE_COMPLETED' and previous_state:
            started_at = previous_state.get('stage_started_at')
            if started_at:
                duration = int((datetime.utcnow() - started_at).total_seconds() / 60)
                return max(0, duration)
        return 0

    def get_file_lifecycle_timeline(self, file_id: str) -> List[Dict]:
        """Get complete lifecycle timeline for a file"""
        try:
            query = """
            SELECT 
                event_id,
                event_type,
                stage,
                employee_code,
                employee_name,
                event_time,
                event_data,
                previous_stage,
                next_stage,
                duration_minutes
            FROM task_analytics.file_lifecycle_events 
            WHERE file_id = %(file_id)s
            ORDER BY event_time ASC
            """
            
            return clickhouse_service.client.execute(query, {'file_id': file_id})
        except Exception as e:
            logger.error(f"Failed to get lifecycle timeline for {file_id}: {e}")
            return []

    def get_pipeline_view_realtime(self, stage_filter: str = None):
        """Get current pipeline view from current state table"""
        try:
            where_clause = f"AND current_stage = '{stage_filter}'" if stage_filter else ""
            
            query = f"""
            SELECT 
                current_stage,
                file_id,
                current_employee_code,
                current_employee_name,
                stage_started_at,
                current_status,
                stage_duration_minutes,
                sla_status
            FROM task_analytics.file_current_state
            WHERE current_stage != ''
            {where_clause}
            ORDER BY stage_started_at DESC
            """
            
            return clickhouse_service.client.execute(query)
        except Exception as e:
            logger.error(f"Failed to get pipeline view: {e}")
            return []

    def get_lifecycle_analytics(self) -> Dict:
        """Get comprehensive lifecycle analytics"""
        try:
            # Average time per stage
            stage_duration_query = """
            SELECT 
                stage,
                avg(duration_minutes) as avg_duration,
                count() as event_count
            FROM task_analytics.file_lifecycle_events 
            WHERE event_type = 'STAGE_COMPLETED'
            GROUP BY stage
            """
            
            # Files delivered per day
            daily_deliveries_query = """
            SELECT 
                toDate(event_time) as delivery_date,
                count() as files_delivered
            FROM task_analytics.file_lifecycle_events 
            WHERE event_type = 'FILE_DELIVERED'
            GROUP BY delivery_date
            ORDER BY delivery_date DESC
            LIMIT 30
            """
            
            # Current pipeline distribution
            pipeline_distribution_query = """
            SELECT 
                current_stage,
                count() as file_count,
                avg(stage_duration_minutes) as avg_stage_time
            FROM task_analytics.file_current_state
            GROUP BY current_stage
            """
            
            stage_durations = clickhouse_service.client.execute(stage_duration_query)
            daily_deliveries = clickhouse_service.client.execute(daily_deliveries_query)
            pipeline_dist = clickhouse_service.client.execute(pipeline_distribution_query)
            
            return {
                'stage_performance': [
                    {
                        'stage': row[0],
                        'avg_duration_minutes': row[1],
                        'completed_files': row[2]
                    } for row in stage_durations
                ],
                'daily_deliveries': [
                    {
                        'date': str(row[0]),
                        'files_delivered': row[1]
                    } for row in daily_deliveries
                ],
                'pipeline_distribution': [
                    {
                        'stage': row[0],
                        'file_count': row[1],
                        'avg_stage_time_minutes': row[2]
                    } for row in pipeline_dist
                ]
            }
            
        except Exception as e:
            logger.error(f"Failed to get lifecycle analytics: {e}")
            return {}

    def emit_sla_breach_event(self, file_id: str, stage: str, employee_code: str, 
                             employee_name: str, breach_data: Dict[str, Any]):
        """Emit SLA breach lifecycle event"""
        try:
            self.emit_file_lifecycle_event(
                file_id=file_id,
                event_type='SLA_BREACH',
                stage=stage,
                employee_code=employee_code,
                employee_name=employee_name,
                event_data={
                    'breach_type': breach_data.get('breach_type', 'time_exceeded'),
                    'breach_minutes': breach_data.get('breach_minutes', 0),
                    'sla_threshold': breach_data.get('sla_threshold', 0),
                    'impact_level': breach_data.get('impact_level', 'medium'),
                    'overdue_by': breach_data.get('overdue_by', 0),
                    'stage_duration': breach_data.get('stage_duration', 0),
                    'notification_sent': breach_data.get('notification_sent', False)
                }
            )
            logger.info(f"✅ Emitted SLA breach lifecycle event for {file_id} in {stage}")
        except Exception as e:
            logger.error(f"Failed to emit SLA breach lifecycle event: {e}")

# Global lifecycle service instance
clickhouse_lifecycle_service = ClickHouseLifecycleService()
