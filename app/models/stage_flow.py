"""
Stage flow definitions and SLA rules
"""
from enum import Enum
from typing import Dict, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel, Field


class FileStage(str, Enum):
    PRELIMS = "PRELIMS"
    PRODUCTION = "PRODUCTION"
    COMPLETED = "COMPLETED"
    QC = "QC"
    DELIVERED = "DELIVERED"


class StageConfig(BaseModel):
    name: str
    display_name: str
    description: str
    ideal_minutes: int
    max_minutes: int
    escalation_minutes: int = 60  # Notify manager if exceeds this
    requires_previous_stage: bool = True
    allowed_previous_stages: list[str]


# Stage definitions with time limits
STAGE_CONFIGS: Dict[FileStage, StageConfig] = {
    FileStage.PRELIMS: StageConfig(
        name="PRELIMS",
        display_name="Prelims",
        description="ARORA, SALES PROPOSAL, LAYOUT work",
        ideal_minutes=20,
        max_minutes=30,
        escalation_minutes=60,
        requires_previous_stage=False,
        allowed_previous_stages=[]
    ),
    FileStage.PRODUCTION: StageConfig(
        name="PRODUCTION",
        display_name="Production",
        description="Permit Design (structural and electrical work)",
        ideal_minutes=120 if False else 210,  # 2 hrs if Zippy, else 3.5 hrs
        max_minutes=240,  # 4 hrs max
        escalation_minutes=60,
        requires_previous_stage=True,
        allowed_previous_stages=[FileStage.PRELIMS]
    ),
    FileStage.COMPLETED: StageConfig(
        name="COMPLETED",
        display_name="Completed",
        description="Production work completed",
        ideal_minutes=0,  # Immediate transition
        max_minutes=5,
        escalation_minutes=30,
        requires_previous_stage=True,
        allowed_previous_stages=[FileStage.PRODUCTION]
    ),
    FileStage.QC: StageConfig(
        name="QC",
        display_name="Quality Control",
        description="Quality Assurance work",
        ideal_minutes=90,
        max_minutes=120,
        escalation_minutes=60,
        requires_previous_stage=True,
        allowed_previous_stages=[FileStage.COMPLETED]
    ),
    FileStage.DELIVERED: StageConfig(
        name="DELIVERED",
        display_name="Delivered",
        description="Final delivered state",
        ideal_minutes=0,
        max_minutes=5,
        escalation_minutes=15,
        requires_previous_stage=True,
        allowed_previous_stages=[FileStage.QC]
    )
}


def get_stage_config(stage: FileStage) -> StageConfig:
    """Get configuration for a stage"""
    return STAGE_CONFIGS.get(stage)


def get_next_stage(current_stage: FileStage) -> Optional[FileStage]:
    """Get the next stage in the flow"""
    stage_order = [
        FileStage.PRELIMS,
        FileStage.PRODUCTION,
        FileStage.COMPLETED,
        FileStage.QC,
        FileStage.DELIVERED
    ]
    
    # Special case: if current stage is QC, next should be DELIVERED
    if current_stage == FileStage.QC:
        return FileStage.DELIVERED
    
    try:
        current_index = stage_order.index(current_stage)
        if current_index < len(stage_order) - 1:
            return stage_order[current_index + 1]
    except ValueError:
        pass
    return None


def can_transition_to(from_stage: Optional[FileStage], to_stage: FileStage) -> bool:
    """Check if transition from from_stage to to_stage is allowed"""
    config = get_stage_config(to_stage)
    
    # If this stage doesn't require previous stage, allow from None
    if not config.requires_previous_stage and from_stage is None:
        return True
    
    # Check if from_stage is in allowed previous stages
    if from_stage and from_stage in config.allowed_previous_stages:
        return True
    
    return False


def calculate_sla_status(start_time, end_time, stage: FileStage) -> Dict:
    """Calculate SLA status for a stage execution"""
    config = get_stage_config(stage)
    if not start_time:
        return {"status": "not_started"}
    
    duration = (end_time or datetime.utcnow()) - start_time
    duration_minutes = int(duration.total_seconds() / 60)
    
    status = "within_ideal"
    if duration_minutes > config.ideal_minutes:
        status = "over_ideal"
    if duration_minutes > config.max_minutes:
        status = "over_max"
    if duration_minutes > config.escalation_minutes:
        status = "escalation_needed"
    
    return {
        "status": status,
        "duration_minutes": duration_minutes,
        "ideal_minutes": config.ideal_minutes,
        "max_minutes": config.max_minutes,
        "escalation_minutes": config.escalation_minutes,
        "over_by_minutes": max(0, duration_minutes - config.max_minutes)
    }


# Penalty calculation rules
PENALTY_RULES = {
    "over_ideal_rate": 0.5,  # 0.5 penalty points per minute over ideal
    "over_max_rate": 2.0,    # 2.0 penalty points per minute over max
    "escalation_multiplier": 1.5  # Multiply penalty if escalation was triggered
}


def calculate_penalty(sla_status: Dict, escalated: bool = False) -> float:
    """Calculate penalty points based on SLA status"""
    if sla_status["status"] in ["not_started", "within_ideal"]:
        return 0.0
    
    over_by = sla_status["over_by_minutes"]
    penalty = 0.0
    
    if sla_status["status"] == "over_ideal":
        penalty = over_by * PENALTY_RULES["over_ideal_rate"]
    elif sla_status["status"] in ["over_max", "escalation_needed"]:
        penalty = over_by * PENALTY_RULES["over_max_rate"]
    
    if escalated:
        penalty *= PENALTY_RULES["escalation_multiplier"]
    
    return round(penalty, 2)
