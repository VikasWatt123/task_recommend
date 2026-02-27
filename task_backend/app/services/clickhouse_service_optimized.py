"""
Optimized ClickHouse Analytics Service
High-performance analytics with efficient resource usage
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Iterator, Tuple
from clickhouse_driver import Client
from clickhouse_pool import ClickHousePool
import redis
import json
from functools import lru_cache
import threading
from collections import defaultdict

from app.db.mongodb import get_db
from app.constants.sla import STAGE_SLA_THRESHOLDS

logger = logging.getLogger(__name__)

# Toggle to disable ClickHouse
CLICKHOUSE_ENABLED = True

# Performance tuning constants
BATCH_SIZE = 500  # Reduced from 1000
SYNC_INTERVAL = 900  # 15 minutes instead of 5
CACHE_TTL = 300  # 5 minutes
MAX_CONNECTIONS = 10
QUERY_TIMEOUT = 30

class OptimizedClickHouseService:
    """Optimized ClickHouse service with efficient resource usage"""
    
    def __init__(self):
        self.client: Optional[ClickHousePool] = None
        self.redis_client: Optional[redis.Redis] = None
        self._employee_cache = {}
        self._cache_lock = threading.Lock()
        
        if CLICKHOUSE_ENABLED:
            self._initialize_connections()
    
    def _initialize_connections(self):
        """Initialize ClickHouse connection pool and Redis"""
        try:
            # ClickHouse connection pool
            self.client = ClickHousePool(
                host='localhost',
                port=9000,
                database='task_analytics',
                max_connections=MAX_CONNECTIONS,
                settings={'max_execution_time': QUERY_TIMEOUT}
            )
            
            # Test connection
            with self.client.get_client() as client:
                client.execute("SELECT 1")
            logger.info("✅ Connected to ClickHouse with connection pool")
            
            # Redis for caching
            try:
                self.redis_client = redis.Redis(
                    host='localhost', 
                    port=6379, 
                    decode_responses=True,
                    socket_timeout=5,
                    socket_connect_timeout=5
                )
                self.redis_client.ping()
                logger.info("✅ Connected to Redis for caching")
            except Exception as e:
                logger.warning(f"Redis connection failed: {e}. Running without cache.")
                self.redis_client = None
            
            self._ensure_optimized_tables()
            
        except Exception as e:
            logger.warning(f"⚠️ ClickHouse connection failed: {e}")
            self.client = None
    
    def _ensure_optimized_tables(self):
        """Create optimized tables with proper indexing"""
        if not self.client:
            return
            
        with self.client.get_client() as client:
            # Optimized task_events table with proper partitioning
            client.execute("""
                CREATE TABLE IF NOT EXISTS task_events_optimized (
                    task_id String,
                    employee_code String,
                    employee_name String,
                    stage String,
                    status String,
                    assigned_at DateTime64(3),
                    completed_at DateTime64(3),
                    duration_minutes UInt32,
                    file_id String,
                    tracking_mode String,
                    team_lead_id String,
                    skills_required Array(String),
                    priority UInt8,
                    event_type String,
                    task_name String,
                    date Date MATERIALIZED toDate(assigned_at)
                ) ENGINE = ReplacingMergeTree(assigned_at)
                PARTITION BY date
                ORDER BY (file_id, stage, assigned_at, employee_code)
                TTL date + INTERVAL 90 DAY
            """)
            
            # Materialized view for real-time pipeline state
            client.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS pipeline_state_mv
                ENGINE = SummingMergeTree()
                ORDER BY (file_id, stage)
                AS SELECT
                    file_id,
                    stage,
                    employee_code,
                    employee_name,
                    status,
                    any(assigned_at) as last_assigned,
                    dateDiff('minute', any(assigned_at), now64()) as duration_minutes
                FROM task_events_optimized
                WHERE event_type IN ('task_assigned', 'stage_started', 'task_sync')
                GROUP BY file_id, stage, employee_code, employee_name, status
            """)
            
            # Pre-computed SLA breach table
            client.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS sla_breaches_mv
                ENGINE = ReplacingMergeTree()
                ORDER BY (file_id, stage, employee_code)
                AS SELECT
                    file_id,
                    stage,
                    employee_code,
                    employee_name,
                    duration_minutes,
                    max_minutes,
                    (duration_minutes > max_minutes) as is_breach,
                    dateDiff('hour', last_assigned, now64()) as hours_overdue
                FROM (
                    SELECT 
                        file_id,
                        stage,
                        employee_code,
                        employee_name,
                        duration_minutes,
                        multiIf(
                            stage = 'PRELIMS', 30,
                            stage = 'PRODUCTION', 240,
                            stage = 'QC', 120,
                            stage = 'COMPLETED', 5,
                            stage = 'DELIVERED', 5,
                            60
                        ) as max_minutes,
                        last_assigned
                    FROM pipeline_state_mv
                )
                WHERE duration_minutes > max_minutes
            """)
    
    @lru_cache(maxsize=1000)
    def _get_employee_lookup(self, cache_key: str) -> Dict[str, Dict]:
        """Cached employee lookup with TTL"""
        if self.redis_client:
            cached = self.redis_client.get(f"employee_lookup:{cache_key}")
            if cached:
                return json.loads(cached)
        
        # Fresh lookup from MongoDB
        db = get_db()
        employees = list(db.employee.find(
            {}, 
            {"_id": 0, "employee_code": 1, "employee_name": 1, "reporting_manager": 1, "employment": 1}
        ))
        employee_lookup = {e.get("employee_code"): e for e in employees if e.get("employee_code")}
        
        # Cache for 1 hour
        if self.redis_client:
            self.redis_client.setex(
                f"employee_lookup:{cache_key}", 
                3600, 
                json.dumps(employee_lookup, default=str)
            )
        
        return employee_lookup
    
    async def sync_tasks_from_mongodb_optimized(self, since: Optional[datetime] = None):
        """Optimized sync with streaming and batching"""
        if not CLICKHOUSE_ENABLED or not self.client:
            return
        
        try:
            db = get_db()
            
            # Use cached employee lookup
            employee_lookup = self._get_employee_lookup("sync_tasks")
            
            # Stream tasks from MongoDB with cursor
            query = {"assigned_at": {"$gte": since}} if since else {}
            cursor = db.tasks.find(query).sort("assigned_at", 1).batch_size(BATCH_SIZE)
            
            batch_rows = []
            total_processed = 0
            skipped_count = 0
            
            with self.client.get_client() as client:
                for task in cursor:
                    # Process task (same logic as before but optimized)
                    row_data = self._process_task_for_sync(task, employee_lookup)
                    if row_data:
                        batch_rows.append(row_data)
                    else:
                        skipped_count += 1
                    
                    # Insert in batches
                    if len(batch_rows) >= BATCH_SIZE:
                        await self._insert_batch(client, batch_rows)
                        total_processed += len(batch_rows)
                        batch_rows = []
                        logger.info(f"Processed {total_processed} tasks...")
                
                # Insert remaining rows
                if batch_rows:
                    await self._insert_batch(client, batch_rows)
                    total_processed += len(batch_rows)
            
            logger.info(f"✅ Synced {total_processed} tasks to ClickHouse (skipped {skipped_count})")
            
        except Exception as e:
            logger.error(f"Failed to sync tasks to ClickHouse: {e}")
    
    def _process_task_for_sync(self, task: Dict, employee_lookup: Dict) -> Optional[Tuple]:
        """Process single task for sync - extracted for clarity"""
        # Extract and validate file_id
        file_id = self._extract_file_id(task)
        if not file_id:
            return None
        
        # Calculate duration
        start_time = task.get('work_started_at') or task.get('assigned_at')
        duration = 0
        if task.get('completed_at') and start_time:
            duration = self._calculate_duration(task['completed_at'], start_time)
        
        # Get employee info
        employee_code = task.get('assigned_to')
        emp_doc = employee_lookup.get(employee_code) if employee_code else None
        employee_name = task.get('assigned_to_name', '') or (emp_doc.get('employee_name') if emp_doc else '')
        
        # Get manager info
        manager_code = ""
        if emp_doc:
            manager_raw = emp_doc.get('reporting_manager') or emp_doc.get('employment', {}).get('reporting_manager') or ""
            manager_code = self._extract_manager_code(manager_raw, employee_lookup)
        
        # Parse timestamps
        assigned_at_value = self._parse_timestamp(task.get('work_started_at') or task.get('assigned_at') or task.get('created_at'))
        completed_at_value = self._parse_timestamp(task.get('completed_at'))
        
        if not assigned_at_value:
            return None
        
        tracking_mode = task.get('tracking_mode', 'FILE_BASED' if file_id and file_id.strip() and file_id != 'None' else 'STANDALONE')
        
        return (
            task.get('task_id') or '',
            employee_code or '',
            employee_name,
            (task.get('stage') or 'UNASSIGNED'),
            (task.get('status') or 'UNKNOWN'),
            assigned_at_value,
            completed_at_value,
            int(duration),
            file_id or '',
            tracking_mode,
            manager_code,
            task.get('skills_required', []),
            1 if task.get('priority') == 'HIGH' else 0,
            'task_assigned',
            (task.get('title') or '')
        )
    
    async def _insert_batch(self, client: Client, batch_rows: List[Tuple]):
        """Insert batch with error handling"""
        try:
            client.execute(
                'INSERT INTO task_events_optimized (task_id, employee_code, employee_name, stage, status, assigned_at, completed_at, duration_minutes, file_id, tracking_mode, team_lead_id, skills_required, priority, event_type, task_name) VALUES',
                batch_rows
            )
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            # Optionally implement retry logic here
    
    def get_dashboard_analytics_optimized(self, days: int = 7) -> Optional[Dict]:
        """Optimized dashboard analytics with caching"""
        if not CLICKHOUSE_ENABLED or not self.client:
            return None
        
        cache_key = f"dashboard_analytics:{days}"
        
        # Try cache first
        if self.redis_client:
            cached = self.redis_client.get(cache_key)
            if cached:
                logger.info("📊 Serving dashboard from cache")
                return json.loads(cached)
        
        try:
            with self.client.get_client() as client:
                # Use materialized view for faster pipeline state
                pipeline_query = f"""
                    SELECT 
                        stage,
                        file_id,
                        employee_code,
                        employee_name,
                        status,
                        last_assigned,
                        duration_minutes
                    FROM pipeline_state_mv
                    WHERE last_assigned >= now() - INTERVAL {days} DAY
                      AND file_id != ''
                    ORDER BY last_assigned DESC
                """
                
                results = client.execute(pipeline_query)
                
                # Process results efficiently
                pipeline = self._process_pipeline_results(results)
                
                # Get SLA breaches from pre-computed view
                breaches_query = f"""
                    SELECT file_id, stage, employee_code, employee_name, 
                           duration_minutes, max_minutes, hours_overdue
                    FROM sla_breaches_mv
                    WHERE last_assigned >= now() - INTERVAL {days} DAY
                    ORDER BY hours_overdue DESC
                """
                
                breaches = client.execute(breaches_query)
                sla_breaches = self._process_breach_results(breaches)
                
                analytics_data = {
                    'pipeline': pipeline,
                    'sla_breaches': sla_breaches,
                    'recent_activity': [],
                    'delivered_today': pipeline.get('DELIVERED', []),
                    'total_penalties': len(sla_breaches),
                    'summary': self._generate_summary(pipeline, sla_breaches)
                }
                
                # Cache the results
                if self.redis_client:
                    self.redis_client.setex(
                        cache_key, 
                        CACHE_TTL, 
                        json.dumps(analytics_data, default=str)
                    )
                
                logger.info(f"📊 Generated dashboard analytics: {len(results)} files processed")
                return analytics_data
                
        except Exception as e:
            logger.error(f"Failed to get dashboard analytics: {e}")
            return None
    
    def _process_pipeline_results(self, results: List[Tuple]) -> Dict[str, List[Dict]]:
        """Efficiently process pipeline results"""
        pipeline = {
            "PRELIMS": [],
            "PRODUCTION": [],
            "QC": [],
            "COMPLETED": [],
            "DELIVERED": []
        }
        
        for row in results:
            stage, file_id, employee_code, employee_name, status, last_assigned, duration_minutes = row
            
            # Map ASSIGNED to PRELIMS
            if stage == 'ASSIGNED':
                stage = 'PRELIMS'
            
            sla_status = self.calculate_sla_status(stage, duration_minutes)
            
            file_data = {
                'file_id': file_id,
                'current_stage': stage,
                'current_status': status,
                'current_assignment': {
                    'employee_code': employee_code,
                    'employee_name': employee_name
                },
                'employee_name': employee_name,
                'duration_minutes': duration_minutes,
                'sla_status': sla_status,
                'updated_at': str(last_assigned)
            }
            
            if stage in pipeline:
                pipeline[stage].append(file_data)
        
        return pipeline
    
    def _process_breach_results(self, breaches: List[Tuple]) -> List[Dict]:
        """Process SLA breach results"""
        return [
            {
                'file_id': file_id,
                'stage': stage,
                'employee_code': employee_code,
                'employee_name': employee_name,
                'duration_minutes': duration_minutes,
                'max_minutes': max_minutes,
                'hours_overdue': hours_overdue
            }
            for file_id, stage, employee_code, employee_name, duration_minutes, max_minutes, hours_overdue in breaches
        ]
    
    def _generate_summary(self, pipeline: Dict, breaches: List) -> Dict:
        """Generate summary statistics"""
        total_files = sum(len(files) for files in pipeline.values())
        return {
            'total_files': total_files,
            'active_files': len(pipeline.get('PRELIMS', [])) + len(pipeline.get('PRODUCTION', [])) + len(pipeline.get('QC', [])),
            'completed_files': len(pipeline.get('COMPLETED', [])) + len(pipeline.get('DELIVERED', [])),
            'total_breaches': len(breaches)
        }
    
    # Helper methods (extracted for clarity)
    def _extract_file_id(self, task: Dict) -> Optional[str]:
        """Extract file_id from task with fallback logic"""
        if task.get('file_id'):
            return task.get('file_id')
        elif task.get('source', {}).get('permit_file_id'):
            return task.get('source', {}).get('permit_file_id')
        elif task.get('permit_file_id'):
            return task.get('permit_file_id')
        else:
            return task.get('task_id') or str(task.get('_id'))
    
    def _calculate_duration(self, completed_at, start_time) -> int:
        """Calculate duration in minutes safely"""
        try:
            if isinstance(completed_at, str):
                completed_at = datetime.fromisoformat(completed_at.replace('Z', '+00:00'))
            if isinstance(start_time, str):
                start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            
            duration_seconds = (completed_at - start_time).total_seconds()
            return max(0, int(duration_seconds / 60))
        except Exception:
            return 0
    
    def _extract_manager_code(self, manager_raw: str, employee_lookup: Dict) -> str:
        """Extract manager code efficiently"""
        manager_raw = (manager_raw or "").strip()
        if not manager_raw:
            return ""
        
        # Simple regex for manager code extraction
        import re
        match = re.search(r"\(([^)]+)\)", manager_raw)
        if match:
            return match.group(1).strip()
        
        if manager_raw in employee_lookup:
            return manager_raw
        
        return manager_raw
    
    def _parse_timestamp(self, timestamp) -> Optional[datetime]:
        """Parse timestamp safely"""
        if not timestamp:
            return None
        if isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        return timestamp
    
    def calculate_sla_status(self, stage: str, duration_minutes: int) -> str:
        """Calculate SLA status using cached thresholds"""
        thresholds = STAGE_SLA_THRESHOLDS.get(stage, {"ideal": 30, "max": 60})
        
        if duration_minutes <= thresholds["ideal"]:
            return "within_ideal"
        elif duration_minutes <= thresholds["max"]:
            return "over_ideal"
        else:
            return "over_max"

# Global optimized service instance
optimized_clickhouse_service = OptimizedClickHouseService()
