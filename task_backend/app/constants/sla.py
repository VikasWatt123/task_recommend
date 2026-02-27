"""
SLA constants for backend - aligned with frontend
"""

# Stage-specific SLA thresholds (in minutes)
STAGE_SLA_THRESHOLDS = {
    'PRELIMS': {'ideal': 20, 'max': 30, 'display': 'Prelims'},
    'PRODUCTION': {'ideal': 210, 'max': 240, 'display': 'Production'},
    'COMPLETED': {'ideal': 0, 'max': 5, 'display': 'Completed'},
    'QC': {'ideal': 90, 'max': 120, 'display': 'Quality Control'},
    'DELIVERED': {'ideal': 0, 'max': 5, 'display': 'Delivered'}
}

# SLA status definitions
SLA_STATUS_DEFINITIONS = {
    'within_ideal': {'description': 'Completed within ideal time', 'color': 'green'},
    'over_ideal': {'description': 'Completed within max time', 'color': 'yellow'},
    'escalation_needed': {'description': 'Exceeded max time', 'color': 'red'}
}

# Default values for task creation
DEFAULT_TASK_VALUES = {
    'estimated_hours': 4.0,
    'due_date_days': 3  # Default due date is 3 days from creation
}

# Cache TTL values (in milliseconds)
CACHE_TTL = {
    'employee': 1800000,      # 30 minutes
    'team_stats': 300000,    # 5 minutes
    'analytics': 1800000     # 30 minutes
}

# Feature flags
FEATURE_FLAGS = {
    'clickhouse_analytics': True,
    'real_time_tracking': True,
    'sla_monitoring': True
}
