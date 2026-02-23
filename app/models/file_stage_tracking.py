"""
File stage tracking models and database schema
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from enum import Enum
from pydantic import BaseModel, Field
from bson import ObjectId

from app.models.stage_flow import FileStage, calculate_sla_status, calculate_penalty


class StageAssignment(BaseModel):
    """Track who worked on which stage"""
    employee_code: str
    employee_name: str
    assigned_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_minutes: Optional[int] = None
    sla_status: Optional[Dict[str, Any]] = None
    penalty_points: float = 0.0
    notes: Optional[str] = None
    
    def model_dump(self, **kwargs) -> Dict[str, Any]:
        """Custom model_dump method to handle ObjectId serialization"""
        from app.services.stage_tracking_service import convert_objectid_to_str
        result = super().model_dump(**kwargs)
        return convert_objectid_to_str(result)
    
    # Keep dict() for backward compatibility
    def dict(self, **kwargs) -> Dict[str, Any]:
        return self.model_dump(**kwargs)


class FileStageHistory(BaseModel):
    """Track stage transitions for a file"""
    file_id: str
    stage: FileStage
    status: str  # IN_PROGRESS, COMPLETED, ESCALATED
    assigned_to: Optional[StageAssignment] = None
    entered_stage_at: Optional[datetime] = None
    completed_stage_at: Optional[datetime] = None
    total_duration_minutes: Optional[int] = None
    sla_breached: bool = False
    escalation_sent: bool = False
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    def model_dump(self, **kwargs) -> Dict[str, Any]:
        """Custom model_dump method to handle ObjectId serialization"""
        from app.services.stage_tracking_service import convert_objectid_to_str
        result = super().model_dump(**kwargs)
        return convert_objectid_to_str(result)
    
    # Keep dict() for backward compatibility
    def dict(self, **kwargs) -> Dict[str, Any]:
        return self.model_dump(**kwargs)


class FileTracking(BaseModel):
    """Main tracking document for a file through all stages"""
    file_id: str
    current_stage: FileStage
    current_status: str = "IN_PROGRESS"  # IN_PROGRESS, COMPLETED, DELIVERED
    stage_history: List[FileStageHistory] = Field(default_factory=list)
    
    # Overall tracking
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    total_duration_minutes: Optional[int] = None
    
    # Performance metrics
    total_penalty_points: float = 0.0
    escalations_triggered: int = 0
    
    # Current assignment
    current_assignment: Optional[StageAssignment] = None
    
    metadata: Dict[str, Any] = Field(default_factory=dict)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    def model_dump(self, **kwargs) -> Dict[str, Any]:
        """Custom model_dump method to handle ObjectId serialization"""
        from app.services.stage_tracking_service import convert_objectid_to_str
        result = super().model_dump(**kwargs)
        return convert_objectid_to_str(result)
    
    # Keep dict() for backward compatibility
    def dict(self, **kwargs) -> Dict[str, Any]:
        return self.model_dump(**kwargs)


class StageTransitionRequest(BaseModel):
    """Request to transition a file to a new stage"""
    permit_file_id: str
    target_stage: FileStage
    employee_code: str
    notes: Optional[str] = None
    force_transition: bool = False  # Admin override


class StageAssignmentRequest(BaseModel):
    """Assign an employee to work on current stage"""
    permit_file_id: str
    employee_code: str
    notes: Optional[str] = None


class StageCompletionRequest(BaseModel):
    """Mark current stage as completed"""
    permit_file_id: str
    employee_code: str
    completion_notes: Optional[str] = None
    next_stage_employee_code: Optional[str] = None  # Auto-assign to next stage


# Database collection names
FILE_TRACKING_COLLECTION = "file_tracking"
STAGE_HISTORY_COLLECTION = "stage_history"


# Database indexes for performance
def get_indexes():
    """Return list of indexes for collections"""
    return {
        FILE_TRACKING_COLLECTION: [
            [("file_id", 1)],  # Unique lookup by file_id
            [("current_stage", 1), ("current_status", 1)],  # Filter by stage/status
            [("current_assignment.employee_code", 1)],  # Employee workload
            [("created_at", -1)],  # Recent files
            [("updated_at", -1)],  # Recently updated
        ],
        STAGE_HISTORY_COLLECTION: [
            [("file_id", 1), ("stage", 1)],  # File stage lookup
            [("stage", 1), ("status", 1)],  # Stage status filtering
            [("assigned_to.employee_code", 1), ("completed_at", -1)],  # Employee performance
            [("entered_stage_at", -1)],  # Time-based queries
        ]
    }


# Utility functions for database operations
def create_file_tracking(file_id: str, initial_stage: FileStage = FileStage.PRELIMS) -> FileTracking:
    """Create initial tracking for a new file"""
    tracking = FileTracking(
        file_id=file_id,
        current_stage=initial_stage,
        current_status="IN_PROGRESS"
    )
    
    # Create initial stage history
    initial_history = FileStageHistory(
        file_id=file_id,
        stage=initial_stage,
        status="IN_PROGRESS",
        entered_stage_at=datetime.utcnow()
    )
    tracking.stage_history.append(initial_history)
    
    return tracking


def assign_employee_to_stage(tracking: FileTracking, employee_code: str, employee_name: str, notes: Optional[str] = None) -> FileTracking:
    """Assign employee to current stage"""
    assignment = StageAssignment(
        employee_code=employee_code,
        employee_name=employee_name,
        assigned_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        notes=notes
    )
    
    tracking.current_assignment = assignment
    
    # Update current stage history
    if tracking.stage_history:
        current_stage_history = tracking.stage_history[-1]
        current_stage_history.assigned_to = assignment
        current_stage_history.status = "IN_PROGRESS"
    
    tracking.updated_at = datetime.utcnow()
    return tracking


def complete_current_stage(tracking: FileTracking, completion_notes: Optional[str] = None) -> FileTracking:
    """Complete the current stage and calculate metrics"""
    if not tracking.current_assignment:
        raise ValueError("No employee assigned to current stage")
    
    now = datetime.utcnow()
    current_stage_history = tracking.stage_history[-1]
    
    # Calculate duration and SLA
    if tracking.current_assignment.started_at:
        duration = now - tracking.current_assignment.started_at
        tracking.current_assignment.duration_minutes = int(duration.total_seconds() / 60)
        
        # Calculate SLA status
        sla_status = calculate_sla_status(
            tracking.current_assignment.started_at,
            now,
            tracking.current_stage
        )
        tracking.current_assignment.sla_status = sla_status
        
        # Calculate penalty
        penalty = calculate_penalty(
            sla_status,
            escalated=current_stage_history.escalation_sent
        )
        tracking.current_assignment.penalty_points = penalty
        tracking.total_penalty_points += penalty
        
        # Check for escalation
        if sla_status["status"] == "escalation_needed" and not current_stage_history.escalation_sent:
            tracking.escalations_triggered += 1
    
    # Update stage history
    current_stage_history.completed_stage_at = now
    current_stage_history.status = "COMPLETED"
    if tracking.current_assignment.duration_minutes is not None:
        current_stage_history.total_duration_minutes = tracking.current_assignment.duration_minutes
    else:
        current_stage_history.total_duration_minutes = 0
    
    tracking.updated_at = datetime.utcnow()
    return tracking


def transition_to_next_stage(tracking: FileTracking, next_stage: Optional[FileStage] = None) -> FileTracking:
    """Transition file to next stage"""
    if next_stage is None:
        from app.models.stage_flow import get_next_stage
        next_stage = get_next_stage(tracking.current_stage)
    
    if not next_stage:
        # File is delivered
        tracking.current_status = "DELIVERED"
        tracking.completed_at = datetime.utcnow()
        total_duration = tracking.completed_at - tracking.started_at
        tracking.total_duration_minutes = int(total_duration.total_seconds() / 60)
        return tracking
    
    # Validate transition
    from app.models.stage_flow import can_transition_to
    if not can_transition_to(tracking.current_stage, next_stage):
        raise ValueError(f"Cannot transition from {tracking.current_stage} to {next_stage}")
    
    # Special rule: Auto-transition from PRODUCTION to COMPLETED
    if tracking.current_stage == FileStage.PRODUCTION and next_stage == FileStage.COMPLETED:
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Auto-transitioning file {tracking.file_id} from PRODUCTION to COMPLETED")
    
    # Update current stage
    tracking.current_stage = next_stage
    tracking.current_assignment = None
    
    # Create new stage history
    new_stage_history = FileStageHistory(
        file_id=tracking.file_id,
        stage=next_stage,
        status="PENDING",
        entered_stage_at=datetime.utcnow()
    )
    tracking.stage_history.append(new_stage_history)
    
    tracking.updated_at = datetime.utcnow()
    return tracking


def get_employee_workload_summary(employee_code: str, tracking_docs: List[FileTracking]) -> Dict:
    """Get workload and performance summary for an employee"""
    active_assignments = []
    completed_stages = []
    total_penalties = 0.0
    
    for tracking in tracking_docs:
        # Active assignments
        if (tracking.current_assignment and 
            tracking.current_assignment.employee_code == employee_code and
            tracking.current_status == "IN_PROGRESS"):
            active_assignments.append({
                "file_id": tracking.file_id,
                "stage": tracking.current_stage,
                "assigned_at": tracking.current_assignment.assigned_at,
                "started_at": tracking.current_assignment.started_at,
                "duration_minutes": tracking.current_assignment.duration_minutes
            })
        
        # Completed stages
        for stage_history in tracking.stage_history:
            if (stage_history.assigned_to and 
                stage_history.assigned_to.employee_code == employee_code and
                stage_history.status == "COMPLETED"):
                completed_stages.append({
                    "file_id": tracking.file_id,
                    "stage": stage_history.stage,
                    "duration_minutes": stage_history.assigned_to.duration_minutes,
                    "penalty_points": stage_history.assigned_to.penalty_points,
                    "completed_at": stage_history.completed_stage_at
                })
                total_penalties += stage_history.assigned_to.penalty_points
    
    return {
        "employee_code": employee_code,
        "active_assignments": len(active_assignments),
        "completed_stages": len(completed_stages),
        "total_penalty_points": round(total_penalties, 2),
        "active_work": active_assignments,
        "completed_work": completed_stages
    }
