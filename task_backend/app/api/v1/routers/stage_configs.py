"""
Stage configuration endpoints
"""
from fastapi import APIRouter
from app.constants.sla import STAGE_SLA_THRESHOLDS

router = APIRouter()

@router.get("/stage-configs")
async def get_stage_configs():
    """Get stage configurations with SLA thresholds"""
    configs = []
    
    for stage, thresholds in STAGE_SLA_THRESHOLDS.items():
        configs.append({
            "stage": stage,
            "display_name": thresholds["display"],
            "description": f"{thresholds['display']} stage with SLA monitoring",
            "ideal_minutes": thresholds["ideal"],
            "max_minutes": thresholds["max"],
            "escalation_minutes": thresholds["max"],  # Same as max for now
            "requires_previous_stage": stage != "PRELIMS",  # PRELIMS doesn't require previous stage
            "allowed_previous_stages": get_allowed_previous_stages(stage)
        })
    
    return {
        "stage_configs": configs,
        "total_stages": len(configs)
    }

def get_allowed_previous_stages(stage: str) -> list:
    """Get allowed previous stages for a given stage"""
    stage_flow = {
        "PRELIMS": [],
        "PRODUCTION": ["PRELIMS"],
        "COMPLETED": ["PRODUCTION"],
        "QC": ["COMPLETED"],
        "DELIVERED": ["QC"]
    }
    return stage_flow.get(stage, [])
