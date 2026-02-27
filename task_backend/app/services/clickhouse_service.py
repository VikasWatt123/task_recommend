"""
ClickHouse Analytics Service
High-performance analytics database for time-series data
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, cast
from clickhouse_driver import Client
from app.db.mongodb import get_db
import pandas as pd
import re
from app.core.settings import settings

# Import SLA constants
from app.constants.sla import STAGE_SLA_THRESHOLDS

logger = logging.getLogger(__name__)

# Toggle to disable ClickHouse (set to True to enable, False to disable)
CLICKHOUSE_ENABLED = True

_MAIN_EVENT_LOOP: Optional[asyncio.AbstractEventLoop] = None

class ClickHouseService:
    """Service for ClickHouse analytics operations"""
    
    def __init__(self):
        # Initialize ClickHouse client only if enabled
        self.client: Optional[Client] = None

        if CLICKHOUSE_ENABLED:
            try:
                self.client = Client(
                    host=settings.clickhouse_host,
                    port=settings.clickhouse_port,
                    database=settings.clickhouse_database,
                    # Add authentication if needed
                    # user='default',
                    # password='',
                    # secure=False
                )
                # Test connection
                self.client.execute("SELECT 1")
                logger.info("✅ Connected to ClickHouse")
                self._ensure_tables()
            except Exception as e:
                logger.warning(f"⚠️ ClickHouse connection failed: {e}. Running without ClickHouse analytics.")
                self.client = None
        else:
            logger.info("ClickHouse disabled via configuration.")
    
    def _ensure_tables(self):
        """Create analytics tables if they don't exist"""
        client = self.client
        if not CLICKHOUSE_ENABLED or client is None:
            logger.info("ClickHouse is disabled - skipping table creation")
            return
        
        try:
            # Task events table (updated for event-driven analytics)
            client.execute("""
                CREATE TABLE IF NOT EXISTS task_events (
                    task_id String,
                    employee_code String,
                    employee_name String,
                    stage String,
                    stage_original String,
                    status String,
                    task_name String,
                    assigned_at DateTime64(3),
                    completed_at Nullable(DateTime64(3)),
                    duration_minutes UInt32,
                    file_id String,
                    tracking_mode String DEFAULT 'FILE_BASED',
                    date Date MATERIALIZED (toDate(assigned_at)),
                    team_lead_id String,
                    skills_required Array(String),
                    priority UInt8,
                    event_type String,
                    created_at DateTime64(3) DEFAULT now64()
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(date)
                ORDER BY (date, task_id, stage, event_type)
            """)
            
            # Employee performance table
            client.execute("""
                CREATE TABLE IF NOT EXISTS employee_performance (
                    employee_code String,
                    employee_name String,
                    date Date,
                    last_activity DateTime64(3),
                    tasks_assigned UInt32,
                    tasks_completed UInt32,
                    avg_completion_time Float32,
                    max_completion_time UInt32,
                    min_completion_time UInt32,
                    sla_breaches UInt32,
                    efficiency_score Float32,
                    stage_performance Map(String, Float32)
                ) ENGINE = ReplacingMergeTree(last_activity)
                ORDER BY (employee_code, date)
            """)
            
            # SLA analytics table
            client.execute("""
                CREATE TABLE IF NOT EXISTS sla_metrics (
                    date Date,
                    stage String,
                    total_tasks UInt32,
                    breached_tasks UInt32,
                    breach_rate Float32,
                    avg_duration Float32,
                    p95_duration UInt32,
                    p99_duration UInt32
                ) ENGINE = SummingMergeTree()
                ORDER BY (date, stage)
            """)
            
            # Real-time metrics table
            client.execute("""
                CREATE TABLE IF NOT EXISTS realtime_metrics (
                    timestamp DateTime64,
                    metric_name String,
                    metric_value Float64,
                    tags Map(String, String)
                ) ENGINE = MergeTree()
                ORDER BY (timestamp, metric_name)
            """)
            
            # File lifecycle events table
            client.execute("""
                CREATE TABLE IF NOT EXISTS file_lifecycle_events (
                    event_id String,
                    file_id String,
                    event_type String,
                    stage String,
                    employee_code String,
                    employee_name String,
                    event_time DateTime64(3),
                    event_data String,
                    previous_stage Nullable(String),
                    next_stage Nullable(String),
                    duration_minutes UInt32,
                    created_at DateTime64(3) DEFAULT now64()
                ) ENGINE = MergeTree()
                ORDER BY (file_id, event_time, event_type)
            """)
            
            # File current state table
            client.execute("""
                CREATE TABLE IF NOT EXISTS file_current_state (
                    file_id String,
                    current_stage String,
                    current_status String,
                    current_employee_code String,
                    current_employee_name String,
                    last_updated DateTime64(3),
                    total_duration_minutes UInt32
                ) ENGINE = ReplacingMergeTree(last_updated)
                ORDER BY file_id
            """)
            
            # File events table
            client.execute("""
                CREATE TABLE IF NOT EXISTS file_events (
                    file_id String,
                    event_type String,
                    event_time DateTime64(3) DEFAULT now64()
                ) ENGINE = MergeTree()
                ORDER BY (file_id, event_time)
            """)
            
            # File lifecycle table (for real-time pipeline tracking)
            client.execute("""
                CREATE TABLE IF NOT EXISTS file_lifecycle (
                    file_id String,
                    current_stage String,
                    current_status String,
                    uploaded_at DateTime64(3),
                    sla_deadline Nullable(DateTime64(3)),
                    last_updated DateTime64(3) DEFAULT now64()
                ) ENGINE = ReplacingMergeTree(last_updated)
                ORDER BY file_id
            """)
            
            # Task file map table (for employee-file associations)
            client.execute("""
                CREATE TABLE IF NOT EXISTS task_file_map (
                    file_id String,
                    task_id String,
                    employee_id String,
                    employee_name String,
                    task_status String,
                    assigned_at DateTime64(3),
                    completed_at Nullable(DateTime64(3))
                ) ENGINE = MergeTree()
                ORDER BY (file_id, assigned_at)
            """)
            
            logger.info("ClickHouse tables ensured")
            
        except Exception as e:
            logger.error(f"Failed to create ClickHouse tables: {e}")

    def calculate_sla_status(self, stage: str, duration_minutes: int) -> str:
        """Calculate SLA status based on stage-specific thresholds"""
        thresholds = STAGE_SLA_THRESHOLDS.get(stage, {'ideal': 30, 'max': 60})
        
        if duration_minutes <= thresholds['ideal']:
            return 'within_ideal'
        elif duration_minutes <= thresholds['max']:
            return 'over_ideal'
        else:
            return 'escalation_needed'

    def set_main_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        global _MAIN_EVENT_LOOP
        _MAIN_EVENT_LOOP = loop
    
    async def sync_tasks_from_mongodb(self, since: Optional[datetime] = None):
        """Sync task data from MongoDB to ClickHouse"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            logger.info("ClickHouse is disabled - skipping MongoDB sync")
            return
        
        try:
            db = get_db()

            employees = list(db.employee.find({}, {"_id": 0, "employee_code": 1, "employee_name": 1, "reporting_manager": 1, "employment": 1}))
            employee_lookup = {e.get("employee_code"): e for e in employees if e.get("employee_code")}

            def _extract_manager_code_and_name(raw: str) -> tuple[str, str]:
                raw = (raw or "").strip()
                if not raw:
                    return "", ""
                match = re.search(r"\(([^)]+)\)", raw)
                if match:
                    code = match.group(1).strip()
                    name = raw.split("(", 1)[0].strip()
                    return code, name
                if raw in employee_lookup:
                    return raw, employee_lookup[raw].get("employee_name", "")
                return raw, ""
            
            # Find tasks assigned in the last sync period
            query = {"assigned_at": {"$gte": since}} if since else {}
            assigned_tasks = list(db.tasks.find(query).sort("assigned_at", 1))
            logger.info(f"Syncing {len(assigned_tasks)} assigned tasks from MongoDB to ClickHouse since {since or 'beginning'}")
            skipped_missing_file_id = 0
            skipped_missing_file_id_samples: List[str] = []
            batch_rows = []
            for task in assigned_tasks:
                # Calculate duration based on work_started_at if available, otherwise assigned_at
                start_time = task.get('work_started_at') or task.get('assigned_at')
                duration = 0
                if task.get('completed_at') and start_time:
                    if isinstance(task["completed_at"], str):
                        completed_at = datetime.fromisoformat(task["completed_at"].replace('Z', '+00:00'))
                    else:
                        completed_at = task["completed_at"]
                    
                    if isinstance(start_time, str):
                        start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    else:
                        start_time = start_time
                    
                    # Calculate duration and ensure it's non-negative (UInt32 requirement)
                    duration_seconds = (completed_at - start_time).total_seconds()
                    duration = max(0, int(duration_seconds / 60))  # Clamp to 0 if negative
                    
                    # Log warning if negative duration detected (data quality issue)
                    if duration_seconds < 0:
                        logger.warning(
                            f"Negative duration detected for task {task.get('task_id')}: "
                            f"completed_at={completed_at}, start_time={start_time}. "
                            f"Setting duration to 0."
                        )
                
                employee_code = task.get('assigned_to')
                emp_doc = employee_lookup.get(employee_code) if employee_code else None
                employee_name = task.get('assigned_to_name', '') or (emp_doc.get('employee_name') if emp_doc else '') or ''

                manager_raw = ""
                if emp_doc:
                    manager_raw = emp_doc.get('reporting_manager') or emp_doc.get('employment', {}).get('reporting_manager') or ""
                manager_code, _manager_name = _extract_manager_code_and_name(manager_raw)

                # Use work_started_at or assigned_at for ClickHouse timestamp (prioritize work_started_at)
                assigned_at_value = task.get('work_started_at') or task.get('assigned_at') or task.get('created_at')
                completed_at_value = task.get('completed_at')
                if isinstance(assigned_at_value, str):
                    assigned_at_value = datetime.fromisoformat(assigned_at_value.replace('Z', '+00:00'))
                if isinstance(completed_at_value, str):
                    completed_at_value = datetime.fromisoformat(completed_at_value.replace('Z', '+00:00'))

                # Skip rows without any timestamp
                if not assigned_at_value:
                    continue

                # Extract file_id with better fallback logic
                file_id = None
                if task.get('file_id'):
                    file_id = task.get('file_id')
                elif task.get('source', {}).get('permit_file_id'):
                    file_id = task.get('source', {}).get('permit_file_id')
                elif task.get('permit_file_id'):
                    file_id = task.get('permit_file_id')
                else:
                    # Fallback for manual tasks: use task_id as file_id so they appear in analytics
                    file_id = task.get('task_id') or str(task.get('_id'))
                
                # Extract tracking_mode from task
                tracking_mode = task.get('tracking_mode', 'FILE_BASED' if file_id and file_id.strip() and file_id != 'None' else 'STANDALONE')
                
                # Skip tasks without valid file_id for analytics (only if FILE_BASED)
                if tracking_mode == 'FILE_BASED' and (not file_id or file_id.strip() == '' or file_id == 'None'):
                    skipped_missing_file_id += 1
                    if len(skipped_missing_file_id_samples) < 5:
                        skipped_missing_file_id_samples.append(task.get('task_id') or str(task.get('_id') or 'unknown'))
                    continue

                batch_rows.append((
                    task.get('task_id') or '',
                    employee_code or '',
                    employee_name,
                    (task.get('stage') or 'UNASSIGNED'),  # stage
                    (task.get('status') or 'UNKNOWN'),
                    assigned_at_value,
                    completed_at_value,
                    int(duration),
                    file_id or '',  # Empty string for standalone tasks
                    tracking_mode,  # Add tracking_mode
                    manager_code,
                    task.get('skills_required', []),
                    1 if task.get('priority') == 'HIGH' else 0,
                    'task_assigned',  # Event type for pipeline view inclusion
                    (task.get('title') or '')  # task_name at end
                ))
            
            # Batch insert into ClickHouse
            # Avoid inserting into any materialized `date` column; ClickHouse will compute it.
            self.client.execute(
                'INSERT INTO task_events (task_id, employee_code, employee_name, stage, status, assigned_at, completed_at, duration_minutes, file_id, tracking_mode, team_lead_id, skills_required, priority, event_type, task_name) VALUES',
                batch_rows
            )
            
            logger.info(f"Synced {len(batch_rows)} tasks to ClickHouse (including task_assigned events)")
            if skipped_missing_file_id:
                logger.warning(
                    "Skipped %s tasks during ClickHouse sync due to missing file_id (sample task_ids=%s)",
                    skipped_missing_file_id,
                    skipped_missing_file_id_samples,
                )
            
        except Exception as e:
            logger.error(f"Failed to sync tasks to ClickHouse: {e}")
    
    async def sync_employee_performance(self, days: int = 30):
        """Calculate and sync employee performance metrics"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            logger.info("ClickHouse is disabled - skipping employee performance sync")
            return
        
        try:
            # Get performance data from ClickHouse
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)

            try:
                self.client.execute(
                    f"ALTER TABLE employee_performance DELETE WHERE date >= '{start_date}' AND date < '{end_date}'"
                )
            except Exception as e:
                logger.warning(f"Failed to clear employee_performance for refresh window: {e}")

            self.client.execute(f"""
                INSERT INTO employee_performance
                WITH per_stage AS (
                    SELECT
                        employee_code,
                        any(employee_name) AS employee_name,
                        date,
                        stage,
                        max(assigned_at) as assigned_at,
                        count() AS tasks_assigned_stage,
                        countIf(status = 'COMPLETED') AS tasks_completed_stage,
                        sumIf(duration_minutes, status = 'COMPLETED' AND duration_minutes > 0) AS completed_duration_sum,
                        countIf(status = 'COMPLETED' AND duration_minutes > 0) AS completed_duration_cnt,
                        maxIf(duration_minutes, status = 'COMPLETED' AND duration_minutes > 0) AS max_completion_time_stage,
                        minIf(duration_minutes, status = 'COMPLETED' AND duration_minutes > 0) AS min_completion_time_stage,
                        countIf(status = 'COMPLETED' AND duration_minutes > 60) AS sla_breaches_stage
                    FROM task_events
                    WHERE date >= '{start_date}' AND date < '{end_date}'
                      AND employee_code != ''
                      AND event_type = 'task_assigned'
                    GROUP BY employee_code, date, stage
                )
                SELECT
                    employee_code,
                    any(employee_name) AS employee_name,
                    date,
                    max(assigned_at) AS last_activity,
                    sum(tasks_assigned_stage) AS tasks_assigned,
                    sum(tasks_completed_stage) AS tasks_completed,
                    if(sum(completed_duration_cnt) = 0, 0, sum(completed_duration_sum) / sum(completed_duration_cnt)) AS avg_completion_time,
                    max(max_completion_time_stage) AS max_completion_time,
                    minIf(min_completion_time_stage, min_completion_time_stage > 0) AS min_completion_time,
                    sum(sla_breaches_stage) AS sla_breaches,
                    if(sum(tasks_completed_stage) > 0, 100 - (sum(sla_breaches_stage) / sum(tasks_completed_stage) * 100), 0) AS efficiency_score,
                    mapFromArrays(
                        groupArray(stage),
                        groupArray(toFloat32(if(completed_duration_cnt = 0, 0, completed_duration_sum / completed_duration_cnt)))
                    ) AS stage_performance
                FROM per_stage
                GROUP BY employee_code, date
            """)
            
            logger.info(f"Synced employee performance for {days} days")
            
        except Exception as e:
            logger.error(f"Failed to sync employee performance: {e}")
    
    def get_task_analytics(self, days: int = 30, stage: Optional[str] = None):
        """Get task analytics with 100x speed"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return []
        
        try:
            where_clause = ""
            if stage:
                where_clause += f" AND stage = '{stage}'"
            
            return self.client.execute(f"""
                SELECT 
                    stage,
                    count() as total_tasks,
                    countIf(status = 'COMPLETED') as completed_tasks,
                    avg(duration_minutes) as avg_duration,
                    median(duration_minutes) as median_duration,
                    quantile(0.95)(duration_minutes) as p95_duration,
                    quantile(0.99)(duration_minutes) as p99_duration,
                    max(duration_minutes) as max_duration,
                    countIf(duration_minutes > 60) as total_breaches,
                    round(total_breaches / count() * 100, 2) as breach_rate
                FROM task_events
                WHERE assigned_at >= now() - INTERVAL {days} DAY {where_clause}
                GROUP BY stage
                ORDER BY stage
            """)
            
        except Exception as e:
            logger.error(f"Failed to get task analytics: {e}")
            return []
    
    def get_pipeline_view(self, stage: Optional[str] = None):
        """Get pipeline view with 100x speed using ClickHouse - optimized"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return []
        
        try:
            stage_filter = ""
            if stage:
                stage_filter = f" AND stage_latest = '{stage}'"
            
            return self.client.execute(f"""
                SELECT
                    CASE
                        WHEN status_latest = 'COMPLETED' THEN 'COMPLETED'
                        WHEN status_latest = 'DELIVERED' THEN 'DELIVERED'
                        ELSE stage_latest
                    END as stage_latest,
                    file_id,
                    employee_code_latest,
                    employee_name_latest,
                    status_latest,
                    assigned_at_latest,
                    completed_at_latest,
                    duration_minutes_latest,
                    team_lead_id_latest,
                    priority_latest,
                    status_latest as current_status,
                    CASE
                        WHEN stage_latest = 'PRELIMS' AND duration_minutes_latest <= 20 THEN 'within_ideal'
                        WHEN stage_latest = 'PRELIMS' AND duration_minutes_latest <= 30 THEN 'over_ideal'
                        WHEN stage_latest = 'PRODUCTION' AND duration_minutes_latest <= 210 THEN 'within_ideal'
                        WHEN stage_latest = 'PRODUCTION' AND duration_minutes_latest <= 240 THEN 'over_ideal'
                        WHEN stage_latest = 'QC' AND duration_minutes_latest <= 90 THEN 'within_ideal'
                        WHEN stage_latest = 'QC' AND duration_minutes_latest <= 120 THEN 'over_ideal'
                        WHEN stage_latest IN ('COMPLETED', 'DELIVERED') AND duration_minutes_latest <= 5 THEN 'within_ideal'
                        ELSE 'escalation_needed'
                    END as sla_status
                FROM (
                    SELECT
                        file_id,
                        argMax(stage, assigned_at) as stage_latest,
                        argMax(employee_code, assigned_at) as employee_code_latest,
                        argMax(employee_name, assigned_at) as employee_name_latest,
                        argMax(status, assigned_at) as status_latest,
                        max(assigned_at) as assigned_at_latest,
                        argMax(completed_at, assigned_at) as completed_at_latest,
                        argMax(duration_minutes, assigned_at) as duration_minutes_latest,
                        argMax(team_lead_id, assigned_at) as team_lead_id_latest,
                        argMax(priority, assigned_at) as priority_latest
                    FROM task_events
                    WHERE assigned_at >= now() - INTERVAL 7 DAY
                      AND file_id != ''
                      AND event_type IN ('stage_started', 'stage_completed', 'task_assigned')
                      AND stage IS NOT NULL
                    GROUP BY file_id
                )
                WHERE 1 = 1 {stage_filter}
                ORDER BY stage_latest, assigned_at_latest DESC
            """)
            
        except Exception as e:
            logger.error(f"Failed to get pipeline view from ClickHouse: {e}")
            return []

    def get_reporting_manager_overview(self, days: int = 7, limit_employees: int = 5):
        """Get reporting manager analytics overview from ClickHouse"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return {"managers": [], "employees": [], "days": days, "limit_employees": limit_employees}
        
        try:
            managers = self.client.execute(f"""
                SELECT
                    team_lead_id as reporting_manager_code,
                    count() as total_tasks,
                    countIf(status = 'COMPLETED') as completed_tasks,
                    countIf(status = 'IN_PROGRESS') as in_progress_tasks,
                    countIf(status = 'ASSIGNED') as assigned_tasks,
                    round(completed_tasks / nullIf(total_tasks, 0) * 100, 1) as completion_rate,
                    avgIf(duration_minutes, duration_minutes > 0) as avg_duration_minutes,
                    quantile(0.95)(duration_minutes) as p95_duration_minutes,
                    countIf(duration_minutes > 60) as breaches_count
                FROM task_events
                WHERE assigned_at >= now() - INTERVAL {days} DAY
                  AND team_lead_id != ''
                GROUP BY team_lead_id
                ORDER BY total_tasks DESC
            """)

            employees = self.client.execute(f"""
                SELECT
                    team_lead_id as reporting_manager_code,
                    employee_code,
                    any(employee_name) as employee_name,
                    count() as task_count,
                    countIf(status = 'COMPLETED') as completed_tasks,
                    countIf(status = 'IN_PROGRESS') as in_progress_tasks,
                    avgIf(duration_minutes, duration_minutes > 0) as avg_duration_minutes
                FROM task_events
                WHERE assigned_at >= now() - INTERVAL {days} DAY
                  AND team_lead_id != ''
                  AND employee_code != ''
                GROUP BY team_lead_id, employee_code
                ORDER BY reporting_manager_code, task_count DESC
            """)

            return {
                "managers": managers,
                "employees": employees,
                "days": days,
                "limit_employees": limit_employees,
            }
        except Exception as e:
            logger.error(f"Failed to get reporting manager overview: {e}")
            return {"managers": [], "employees": [], "days": days, "limit_employees": limit_employees}
    
    def get_employee_performance(self, days: int = 30, limit: int = 100):
        """Get top employee performance metrics"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return []
        
        try:
            return self.client.execute(f"""
                SELECT 
                    employee_code,
                    employee_name,
                    max(last_activity) as last_activity,
                    sum(tasks_completed) as total_completed,
                    avg(avg_completion_time) as avg_time,
                    max(max_completion_time) as max_time,
                    sum(sla_breaches) as total_breaches,
                    avg(efficiency_score) as efficiency,
                    groupArray(stage_performance) as stage_performance
                FROM employee_performance
                WHERE date >= today() - {days}
                GROUP BY employee_code, employee_name
                ORDER BY total_completed DESC
                LIMIT {limit}
            """)
            
        except Exception as e:
            logger.error(f"Failed to get employee performance: {e}")
            return []
    
    def get_sla_analytics(self, days: int = 30):
        """Get SLA compliance analytics"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return []
        
        try:
            return self.client.execute(f"""
                SELECT 
                    date,
                    stage,
                    total_tasks,
                    breached_tasks,
                    breach_rate,
                    avg_duration,
                    p95_duration,
                    p99_duration
                FROM sla_metrics
                WHERE date >= today() - {days}
                ORDER BY date DESC, stage
            """)
            
        except Exception as e:
            logger.error(f"Failed to get SLA analytics: {e}")
            return []
    
    def get_real_time_metrics(self, hours: int = 24):
        """Get real-time metrics for dashboard"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return []
        
        try:
            return self.client.execute(f"""
                SELECT 
                    metric_name,
                    argMax(metric_value, timestamp) as latest_value,
                    timestamp
                FROM realtime_metrics
                WHERE timestamp >= now() - INTERVAL {hours} HOUR
                GROUP BY metric_name
                ORDER BY metric_name
            """)
            
        except Exception as e:
            logger.error(f"Failed to get real-time metrics: {e}")
            return []
    
    async def update_real_time_metric(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """Update real-time metric"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return
        
        try:
            self.client.execute(
                'INSERT INTO realtime_metrics VALUES',
                [{
                    'timestamp': datetime.now(),
                    'metric_name': metric_name,
                    'metric_value': value,
                    'tags': tags or {}
                }]
            )
        except Exception as e:
            logger.error(f"Failed to update real-time metric {metric_name}: {e}")
    
    # Event-driven analytics functions
    async def emit_file_created_event(self, file_id: str, file_name: str, uploaded_by: str):
        """Emit file creation event to ClickHouse"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return
        
        try:
            event_data = {
                'task_id': f"FILE-{file_id}",
                'employee_code': '',
                'employee_name': '',
                'stage': 'PRELIMS',
                'status': 'CREATED',
                'assigned_at': datetime.utcnow(),
                'completed_at': None,
                'duration_minutes': 0,
                'file_id': file_id,
                'team_lead_id': uploaded_by,
                'skills_required': [],
                'priority': 0,
                'event_type': 'file_created'
            }
            
            self.client.execute(
                'INSERT INTO task_events (task_id, employee_code, employee_name, stage, status, assigned_at, completed_at, duration_minutes, file_id, team_lead_id, skills_required, priority, event_type) VALUES',
                [event_data]
            )
            
            logger.info(f"Emitted file_created event for {file_id}")
            
        except Exception as e:
            logger.error(f"Failed to emit file_created event: {e}")
    
    async def emit_task_assigned_event(self, task_id: str, employee_code: str, employee_name: str, assigned_by: str, file_id_param: Optional[str] = None, tracking_mode: Optional[str] = None):
        """Emit task assignment event to ClickHouse"""
        if not CLICKHOUSE_ENABLED:
            logger.info(f"ClickHouse disabled - skipping task assignment event for {task_id}")
            return
        
        try:
            from app.db.mongodb import get_db
            db = get_db()
            task = db.tasks.find_one({"task_id": task_id})
            true_stage = task.get("stage", "UNASSIGNED") if task else "UNASSIGNED"
            
            # Determine tracking_mode
            effective_tracking_mode = tracking_mode or (task.get('tracking_mode') if task else None) or ('FILE_BASED' if file_id_param else 'STANDALONE')
            
            event_data = {
                'task_id': task_id,
                'employee_code': employee_code,
                'employee_name': employee_name,
                'stage': 'ASSIGNED',
                'status': 'ASSIGNED',
                'assigned_at': datetime.utcnow(),
                'completed_at': None,
                'duration_minutes': 0,
                'file_id': file_id_param or '',
                'tracking_mode': effective_tracking_mode,
                'team_lead_id': assigned_by,
                'skills_required': [],
                'priority': 0,
                'event_type': 'task_assigned',
                'task_name': task.get('title', '') if task else ''
            }
            
            self.client.execute(
                'INSERT INTO task_events (task_id, employee_code, employee_name, stage, status, assigned_at, completed_at, duration_minutes, file_id, tracking_mode, team_lead_id, skills_required, priority, event_type, task_name) VALUES',
                [event_data]
            )
            
            logger.info(f"Emitted task_assigned event for {task_id} to {employee_code} (tracking_mode={effective_tracking_mode})")
            
        except Exception as e:
            logger.error(f"Failed to emit task_assigned event: {e}")
    
    async def emit_stage_started_event(self, task_id: str, employee_code: str, employee_name: str, stage: str, file_id_param: Optional[str] = None, tracking_mode: Optional[str] = None):
        """Emit stage started event to ClickHouse"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return
        
        try:
            from app.db.mongodb import get_db
            db = get_db()
            task = db.tasks.find_one({"task_id": task_id})
            task_name = task.get('title', '') if task else ''
            
            # Determine tracking_mode
            effective_tracking_mode = tracking_mode or (task.get('tracking_mode') if task else None) or ('FILE_BASED' if file_id_param else 'STANDALONE')
            
            event_data = {
                'task_id': task_id,
                'employee_code': employee_code,
                'employee_name': employee_name,
                'stage': stage,
                'status': 'IN_PROGRESS',
                'assigned_at': datetime.utcnow(),
                'completed_at': None,
                'duration_minutes': 0,
                'file_id': file_id_param or '',
                'tracking_mode': effective_tracking_mode,
                'team_lead_id': '',
                'skills_required': [],
                'priority': 0,
                'event_type': 'stage_started',
                'task_name': task_name
            }
            
            self.client.execute(
                'INSERT INTO task_events (task_id, employee_code, employee_name, stage, status, assigned_at, completed_at, duration_minutes, file_id, tracking_mode, team_lead_id, skills_required, priority, event_type, task_name) VALUES',
                [event_data]
            )
            
            logger.info(f"Emitted stage_started event for {task_id} in {stage} (tracking_mode={effective_tracking_mode})")
            
        except Exception as e:
            logger.error(f"Failed to emit stage_started event: {e}")
    
    async def emit_stage_completed_event(self, task_id: str, employee_code: str, employee_name: str, stage: str, duration_minutes: int, file_id_param: Optional[str] = None, tracking_mode: Optional[str] = None):
        """Emit stage completed event to ClickHouse"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return
        
        try:
            from app.db.mongodb import get_db
            db = get_db()
            task = db.tasks.find_one({"task_id": task_id})
            task_name = task.get('title', '') if task else ''
            
            # Determine tracking_mode
            effective_tracking_mode = tracking_mode or (task.get('tracking_mode') if task else None) or ('FILE_BASED' if file_id_param else 'STANDALONE')
            
            event_data = {
                'task_id': task_id,
                'employee_code': employee_code,
                'employee_name': employee_name,
                'stage': stage,
                'status': 'COMPLETED',
                'assigned_at': datetime.utcnow(),
                'completed_at': datetime.utcnow(),
                'duration_minutes': duration_minutes,
                'file_id': file_id_param or '',
                'tracking_mode': effective_tracking_mode,
                'team_lead_id': '',
                'skills_required': [],
                'priority': 0,
                'event_type': 'stage_completed',
                'task_name': task_name
            }
            
            self.client.execute(
                'INSERT INTO task_events (task_id, employee_code, employee_name, stage, status, assigned_at, completed_at, duration_minutes, file_id, tracking_mode, team_lead_id, skills_required, priority, event_type, task_name) VALUES',
                [event_data]
            )
            
            logger.info(f"Emitted stage_completed event for {task_id} in {stage} (tracking_mode={effective_tracking_mode})")
            
        except Exception as e:
            logger.error(f"Failed to emit stage_completed event: {e}")
    
    async def emit_sla_breach_event(self, file_id: str, employee_code: str, employee_name: str, stage: str, sla_status: str, file_id_param: Optional[str] = None):
        """Emit SLA breach event to ClickHouse"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return
        
        try:
            event_data = {
                'task_id': f"SLA-{file_id}",
                'employee_code': employee_code,
                'employee_name': employee_name,
                'stage': stage,
                'status': 'SLA_BREACH',
                'assigned_at': datetime.utcnow(),
                'completed_at': None,
                'duration_minutes': 0,
                'file_id': file_id_param or '',
                'team_lead_id': '',
                'skills_required': [],
                'priority': 1,  # High priority for SLA breaches
                'event_type': 'sla_breach'
            }
            
            self.client.execute(
                'INSERT INTO task_events (task_id, employee_code, employee_name, stage, status, assigned_at, completed_at, duration_minutes, file_id, team_lead_id, skills_required, priority, event_type) VALUES',
                [event_data]
            )
            
            logger.info(f"Emitted sla_breach event for {file_id} in {stage}")
            
        except Exception as e:
            logger.error(f"Failed to emit sla_breach event: {e}")
    
    def emit_sla_breach_event_sync(self, file_id: str, employee_code: str, employee_name: str, stage: str, sla_status: str, file_id_param: Optional[str] = None):
        """Synchronous SLA breach event recorder - NO async calls"""
        # This method now only records the event, does NOT emit
        # Emission is handled by async SLAEventEmitter
        if not CLICKHOUSE_ENABLED or self.client is None:
            return
        
        try:
            # Insert SLA breach event into file_events (idempotent)
            self.client.execute(
                'INSERT INTO file_events (file_id, event_type) VALUES',
                [(file_id or file_id_param, 'SLA_BREACH')]
            )
            logger.info(f"Recorded SLA breach event for {file_id or file_id_param}")
        except Exception as e:
            logger.error(f"Failed to record SLA breach event: {e}")
    
    def update_file_stage(self, file_id: str, new_stage: str):
        """Update current_stage in file_lifecycle for deterministic stage tracking"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return
        
        try:
            # ClickHouse requires OPTIMIZE TABLE after ALTER TABLE UPDATE
            self.client.execute(
                f"ALTER TABLE file_lifecycle UPDATE current_stage = '{new_stage}', current_status = 'IN_{new_stage}' WHERE file_id = '{file_id}'"
            )
            # Apply mutations immediately
            self.client.execute("OPTIMIZE TABLE file_lifecycle FINAL")
            logger.info(f"Updated file_lifecycle stage for {file_id} to {new_stage}")
        except Exception as e:
            logger.error(f"Failed to update file_lifecycle stage: {e}")
    
    def get_pipeline_view_realtime(self, stage_filter: Optional[str] = None):
        """Get pipeline view using file_lifecycle for real-time stage tracking"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return []
        
        try:
            where_clause = f"AND fl.current_stage = '{stage_filter}'" if stage_filter else ""
            
            query = f"""
                SELECT 
                    fl.current_stage,
                    fl.file_id,
                    tfm.employee_id,
                    tfm.employee_name,
                    tfm.assigned_at,
                    fl.current_status,
                    fl.sla_deadline,
                    CASE 
                        WHEN fl.current_stage = 'PRELIMS' AND dateDiff('minute', tfm.assigned_at, now64()) <= 20 THEN 'within_ideal'
                        WHEN fl.current_stage = 'PRELIMS' AND dateDiff('minute', tfm.assigned_at, now64()) <= 30 THEN 'over_ideal'
                        WHEN fl.current_stage = 'PRODUCTION' AND dateDiff('minute', tfm.assigned_at, now64()) <= 210 THEN 'within_ideal'
                        WHEN fl.current_stage = 'PRODUCTION' AND dateDiff('minute', tfm.assigned_at, now64()) <= 240 THEN 'over_ideal'
                        WHEN fl.current_stage = 'QC' AND dateDiff('minute', tfm.assigned_at, now64()) <= 90 THEN 'within_ideal'
                        WHEN fl.current_stage = 'QC' AND dateDiff('minute', tfm.assigned_at, now64()) <= 120 THEN 'over_ideal'
                        WHEN fl.current_stage IN ('COMPLETED', 'DELIVERED') AND dateDiff('minute', tfm.assigned_at, now64()) <= 5 THEN 'within_ideal'
                        ELSE 'escalation_needed'
                    END as sla_status
                FROM file_lifecycle fl
                LEFT JOIN (
                    SELECT file_id, employee_id, employee_name, assigned_at
                    FROM task_file_map
                    WHERE task_status = 'ASSIGNED'
                    GROUP BY file_id, employee_id, employee_name, assigned_at
                    HAVING assigned_at = max(assigned_at)
                ) tfm ON fl.file_id = tfm.file_id
                WHERE fl.current_stage != ''
                {where_clause}
                ORDER BY fl.uploaded_at DESC
            """
            
            return self.client.execute(query)
        except Exception as e:
            logger.error(f"Failed to get real-time pipeline view: {e}")
            return []
    
    def get_team_lead_stats(self, days: int = 7):
        """Get team lead task statistics from ClickHouse with employee task details (last 7 days only)"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return []
        
        try:
            # Get team lead stats with employee breakdown from last 7 days only
            query = f"""
                SELECT
                    team_lead_id as team_lead_code,
                    employee_code,
                    any(employee_name) as employee_name,
                    count() as total_tasks,
                    countIf(status = 'COMPLETED') as completed_tasks,
                    countIf(status = 'ASSIGNED') as assigned_tasks,
                    countIf(status = 'IN_PROGRESS') as in_progress_tasks,
                    round(completed_tasks / nullIf(total_tasks, 0) * 100, 2) as completion_rate,
                    groupArray((task_id, task_name, status, toString(assigned_at), toString(completed_at))) as tasks
                FROM task_events
                WHERE assigned_at >= now() - INTERVAL {days} DAY
                  AND team_lead_id != ''
                  AND employee_code != ''
                  AND event_type IN ('task_assigned', 'task_sync')
                GROUP BY team_lead_id, employee_code
                ORDER BY team_lead_id, total_tasks DESC
            """
            
            results = self.client.execute(query)
            
            # Fetch team lead names from MongoDB
            from app.db.mongodb import get_db
            db = get_db()
            team_lead_names = {}
            unique_team_leads = set(row[0] for row in results)
            
            # First, build a mapping of manager codes to names from all employees
            manager_code_to_name = {}
            employees = db.employee.find({'reporting_manager': {'$exists': True, '$ne': None}})
            for emp in employees:
                rm = emp.get('reporting_manager', '')
                # Extract code from format like 'Rahul (0081)'
                if '(' in rm and ')' in rm:
                    try:
                        code = rm.split('(')[1].split(')')[0].strip()
                        name = rm.split('(')[0].strip()
                        manager_code_to_name[code] = name
                    except:
                        pass
            
            # Now map team lead codes to names
            for team_lead_code in unique_team_leads:
                if team_lead_code in manager_code_to_name:
                    team_lead_names[team_lead_code] = manager_code_to_name[team_lead_code]
                else:
                    # Fallback: try to find employee by this code
                    employee = db.employee.find_one({'employee_code': team_lead_code})
                    if employee:
                        team_lead_names[team_lead_code] = employee.get('employee_name', team_lead_code)
                    else:
                        team_lead_names[team_lead_code] = team_lead_code
            
            # Group by team lead using plain Dict[str, Any] to allow mutation
            team_stats: Dict[str, Dict[str, Any]] = {}
            for row in results:
                team_lead_code = str(row[0])
                team_lead_name = team_lead_names.get(team_lead_code, team_lead_code)
                employee_code = str(row[1])
                employee_name = str(row[2])
                total_tasks = int(row[3])
                completed_tasks = int(row[4])
                assigned_tasks = int(row[5])
                in_progress_tasks = int(row[6])
                tasks = row[8][:10]  # Limit to 10 most recent tasks per employee

                if team_lead_code not in team_stats:
                    team_stats[team_lead_code] = {
                        'team_lead_code': team_lead_code,
                        'team_lead_name': team_lead_name,
                        'total_employees': 0,
                        'employees': [],
                        'task_statistics': {
                            'total_tasks': 0,
                            'completed_tasks': 0,
                            'assigned_tasks': 0,
                            'in_progress_tasks': 0,
                            'pending_tasks': 0,
                            'completion_rate': 0.0
                        }
                    }

                # Add employee data
                employee_tasks: List[Dict[str, Any]] = []
                for task in tasks:
                    employee_tasks.append({
                        'task_id': task[0],
                        'task_title': task[1],
                        'status': task[2],
                        'assigned_at': task[3],
                        'completed_at': task[4] if task[4] != 'None' else None
                    })

                team_stats[team_lead_code]['employees'].append({  # type: ignore[union-attr]
                    'employee_code': employee_code,
                    'employee_name': employee_name,
                    'employee_role': 'Employee',  # Can be enhanced with MongoDB lookup
                    'task_count': len(employee_tasks),
                    'tasks': employee_tasks
                })

                # Aggregate stats via explicit cast so type checker knows the shape
                ts: Dict[str, Any] = cast(Dict[str, Any], team_stats[team_lead_code]['task_statistics'])
                ts['total_tasks'] = int(ts['total_tasks']) + total_tasks
                ts['completed_tasks'] = int(ts['completed_tasks']) + completed_tasks
                ts['assigned_tasks'] = int(ts['assigned_tasks']) + assigned_tasks
                ts['in_progress_tasks'] = int(ts['in_progress_tasks']) + in_progress_tasks
                team_stats[team_lead_code]['total_employees'] = int(team_stats[team_lead_code]['total_employees']) + 1  # type: ignore[operator]

            # Calculate completion rates and pending tasks
            for team_lead_code, stats in team_stats.items():
                ts_final: Dict[str, Any] = cast(Dict[str, Any], stats['task_statistics'])
                total_f = float(ts_final.get('total_tasks', 0))
                completed_f = float(ts_final.get('completed_tasks', 0))
                ts_final['pending_tasks'] = int(total_f - completed_f)
                if total_f > 0:
                    ts_final['completion_rate'] = round((completed_f / total_f) * 100, 2)

            return list(team_stats.values())
            
        except Exception as e:
            logger.error(f"Failed to get team lead stats from ClickHouse: {e}")
            return []
    
    def get_permit_file_tracking_stats(self, days: int = 7, limit: int = 50):
        """Get permit file tracking statistics from ClickHouse (last 7 days only)"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return []
        
        try:
            query = f"""
                SELECT
                    file_id,
                    argMax(stage, assigned_at) as current_stage,
                    argMax(status, assigned_at) as current_status,
                    count() as total_tasks,
                    countIf(status = 'COMPLETED') as completed_tasks,
                    countIf(status = 'ASSIGNED') as assigned_tasks,
                    countIf(status = 'IN_PROGRESS') as in_progress_tasks,
                    countIf(status IN ('ASSIGNED', 'IN_PROGRESS')) as active_tasks,
                    round(completed_tasks / nullIf(total_tasks, 0) * 100, 2) as completion_rate,
                    max(assigned_at) as last_updated,
                    groupArray((task_id, task_name, status, employee_code, employee_name, toString(assigned_at), toString(completed_at))) as tasks
                FROM task_events
                WHERE assigned_at >= now() - INTERVAL {days} DAY
                  AND file_id != ''
                  AND tracking_mode = 'FILE_BASED'
                  AND event_type IN ('task_assigned', 'task_sync')
                GROUP BY file_id
                ORDER BY last_updated DESC
                LIMIT {limit}
            """
            
            results = self.client.execute(query)
            
            # Fetch client names from MongoDB permit_files collection
            from app.db.mongodb import get_db
            db = get_db()
            file_names = {}
            unique_file_ids = set(row[0] for row in results)
            for file_id in unique_file_ids:
                permit_file = db.permit_files.find_one({'permit_file_id': file_id})
                if permit_file:
                    # Use original_filename field if available, otherwise client_name or permit_file_name
                    client_name = permit_file.get('original_filename') or permit_file.get('client_name') or permit_file.get('permit_file_name') or file_id
                    file_names[file_id] = client_name
                else:
                    file_names[file_id] = file_id
            
            permit_files = []
            for row in results:
                file_id = row[0]
                file_name = file_names.get(file_id, file_id)
                current_stage = row[1]
                current_status = row[2]
                total_tasks = row[3]
                completed_tasks = row[4]
                assigned_tasks = row[5]
                in_progress_tasks = row[6]
                active_tasks = row[7]
                completion_rate = row[8]
                last_updated = row[9]
                tasks_data = row[10][:10]  # Limit to 10 most recent tasks
                
                tasks = []
                for task in tasks_data:
                    tasks.append({
                        'task_id': task[0],
                        'title': task[1],
                        'status': task[2],
                        'assigned_to': task[3],
                        'employee_name': task[4],
                        'employee_role': 'Employee',
                        'assigned_at': task[5],
                        'completed_at': task[6] if task[6] != 'None' else None
                    })
                
                permit_files.append({
                    'file_id': file_id,
                    'file_name': file_name,  # Add client name
                    'current_stage': current_stage,
                    'status': current_status,
                    'total_tasks': total_tasks,
                    'completed_tasks': completed_tasks,
                    'assigned_tasks': assigned_tasks,
                    'in_progress_tasks': in_progress_tasks,
                    'active_tasks': active_tasks,
                    'completion_rate': completion_rate,
                    'updated_at': str(last_updated),
                    'tasks': tasks
                })
            
            return permit_files
            
        except Exception as e:
            logger.error(f"Failed to get permit file tracking from ClickHouse: {e}")
            return []
    
    def get_dashboard_analytics(self, days: int = 7):
        """Get complete dashboard analytics from ClickHouse"""
        if not CLICKHOUSE_ENABLED or self.client is None:
            return None
        
        try:
            # Get pipeline view grouped by stage
            pipeline_query = f"""
                SELECT
                    argMax(stage, assigned_at) as current_stage,
                    file_id,
                    argMax(employee_code, assigned_at) as employee_code,
                    argMax(employee_name, assigned_at) as employee_name,
                    argMax(status, assigned_at) as current_status,
                    max(assigned_at) as last_assigned,
                    dateDiff('minute', max(assigned_at), now64()) as duration_minutes
                FROM task_events
                WHERE assigned_at >= now() - INTERVAL {days} DAY
                  AND file_id != ''
                  AND tracking_mode = 'FILE_BASED'
                  AND event_type IN ('task_assigned', 'stage_started', 'task_sync')
                GROUP BY file_id
                HAVING current_stage NOT IN ('COMPLETED', 'DELIVERED')
                ORDER BY last_assigned DESC
            """
            
            pipeline_results = self.client.execute(pipeline_query)
            
            # Group by stage - include completed files
            pipeline = {
                "PRELIMS": [],
                "PRODUCTION": [],
                "QC": [],
                "COMPLETED": [],
                "DELIVERED": []
            }
            
            # Also get completed files separately
            completed_query = f"""
                SELECT
                    argMax(stage, assigned_at) as current_stage,
                    file_id,
                    argMax(employee_code, assigned_at) as employee_code,
                    argMax(employee_name, assigned_at) as employee_name,
                    argMax(status, assigned_at) as current_status,
                    max(assigned_at) as last_assigned,
                    dateDiff('minute', max(assigned_at), now64()) as duration_minutes
                FROM task_events
                WHERE assigned_at >= now() - INTERVAL {days} DAY
                  AND file_id != ''
                  AND tracking_mode = 'FILE_BASED'
                  AND event_type IN ('task_assigned', 'stage_started', 'task_sync')
                GROUP BY file_id
                HAVING current_stage IN ('COMPLETED', 'DELIVERED')
                ORDER BY last_assigned DESC
            """
            
            completed_results = self.client.execute(completed_query)
            logger.info(f"Found {len(completed_results)} completed files for dashboard")
            
            for row in pipeline_results:
                stage = row[0]
                file_id = row[1]
                employee_code = row[2]
                employee_name = row[3]
                status = row[4]
                last_assigned = row[5]
                duration_minutes = row[6]
                
                # Map ASSIGNED to PRELIMS for pipeline display
                if stage == 'ASSIGNED':
                    stage = 'PRELIMS'
                
                # Calculate SLA status using stage-specific thresholds
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
            
            # Process completed files
            for row in completed_results:
                stage = row[0]
                file_id = row[1]
                employee_code = row[2]
                employee_name = row[3]
                status = row[4]
                last_assigned = row[5]
                duration_minutes = row[6]
                
                # Create file data for completed files
                file_data = {
                    'file_id': file_id,
                    'current_stage': stage,
                    'current_assignment': {
                        'employee_code': employee_code,
                        'employee_name': employee_name
                    },
                    'employee_name': employee_name,
                    'duration_minutes': duration_minutes,
                    'sla_status': 'completed',
                    'updated_at': str(last_assigned)
                }
                
                if stage in pipeline:
                    pipeline[stage].append(file_data)
            
            # Get SLA breaches
            breach_query = f"""
                SELECT
                    file_id,
                    argMax(stage, assigned_at) as current_stage,
                    argMax(employee_code, assigned_at) as employee_code,
                    argMax(employee_name, assigned_at) as employee_name,
                    dateDiff('minute', max(assigned_at), now64()) as duration_minutes
                FROM task_events
                WHERE assigned_at >= now() - INTERVAL {days} DAY
                  AND file_id != ''
                  AND tracking_mode = 'FILE_BASED'
                GROUP BY file_id
                HAVING duration_minutes > 60
                ORDER BY duration_minutes DESC
                LIMIT 100
            """
            
            breach_results = self.client.execute(breach_query)
            
            sla_breaches = []
            for row in breach_results:
                stage = row[1]
                # Map ASSIGNED to PRELIMS for consistency
                if stage == 'ASSIGNED':
                    stage = 'PRELIMS'
                    
                sla_breaches.append({
                    'file_id': row[0],
                    'current_stage': stage,
                    'current_assignment': {
                        'employee_code': row[2],
                        'employee_name': row[3]
                    },
                    'employee_name': row[3],
                    'duration_minutes': row[4],
                    'sla_status': 'escalation_needed' if row[4] > 120 else 'over_max'
                })
            
            # Get recent activity (delivered today)
            delivered_query = f"""
                SELECT
                    file_id,
                    argMax(employee_code, assigned_at) as employee_code,
                    argMax(employee_name, assigned_at) as employee_name,
                    max(completed_at) as delivered_at
                FROM task_events
                WHERE toDate(assigned_at) = today()
                  AND file_id != ''
                  AND status = 'DELIVERED'
                GROUP BY file_id
                ORDER BY delivered_at DESC
                LIMIT 50
            """
            
            delivered_results = self.client.execute(delivered_query)
            
            delivered_today = []
            for row in delivered_results:
                delivered_today.append({
                    'file_id': row[0],
                    'employee_code': row[1],
                    'employee_name': row[2],
                    'delivered_at': str(row[3])
                })
            
            return {
                'pipeline': pipeline,
                'sla_breaches': sla_breaches,
                'delivered_today': delivered_today,
                'summary': {
                    'total_files': sum(len(files) for files in pipeline.values()),
                    'active_files': sum(len(files) for stage, files in pipeline.items() if stage not in ['COMPLETED', 'DELIVERED']),
                    'breaches_count': len(sla_breaches),
                    'delivered_today_count': len(delivered_today),
                    'escalations_today': len([b for b in sla_breaches if b['duration_minutes'] > 120])
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to get dashboard analytics from ClickHouse: {e}")
            return None

# Global ClickHouse service instance
clickhouse_service = ClickHouseService()