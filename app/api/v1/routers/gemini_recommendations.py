"""
Gemini-powered Smart Recommendations API
Uses Vertex AI embeddings for intelligent employee-task matching
"""
from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from app.services.recommendation_engine import get_recommendation_engine, EmployeeRecommendation

router = APIRouter(prefix="/task", tags=["task-recommendations"])

class TaskRecommendationRequest(BaseModel):
    task_description: str
    top_k: Optional[int] = 10
    min_similarity: Optional[float] = 0.5
    permit_file_id: Optional[str] = None
    file_id: Optional[str] = None
    address: Optional[str] = None
    priority: Optional[str] = None
    required_skills: Optional[List[str]] = None
    filter_by_availability: Optional[bool] = True
    team_lead_code: Optional[str] = None

class RecommendationResponse(BaseModel):
    recommendations: List[EmployeeRecommendation]
    total_found: int
    query_info: Dict[str, Any]

@router.post("/recommend", response_model=RecommendationResponse)
async def get_task_recommendations(request: TaskRecommendationRequest) -> RecommendationResponse:
    """
    Get AI-powered employee recommendations for a task using Vertex AI Gemini embeddings - OPTIMIZED
    
    Performance improvements:
    - Parallel data fetching (embedding generation + employee data)
    - Smart caching (5-minute TTL for employee data)
    - Vectorized similarity computation
    - Batch operations
    
    This endpoint:
    1. Generates embeddings for the task description using Gemini (parallel)
    2. Loads employee data with caching (parallel)
    3. Computes vectorized similarities
    4. Returns top matching employees with similarity scores
    """
    import time
    start_time = time.time()
    
    try:
        engine = get_recommendation_engine()
        
        effective_permit_file_id = request.permit_file_id or request.file_id

        # Prepare additional context
        additional_context = {}
        if effective_permit_file_id:
            additional_context['file_id'] = effective_permit_file_id
        if request.priority:
            additional_context['priority'] = request.priority
        if request.required_skills:
            additional_context['required_skills'] = request.required_skills
        
        current_file_stage = None
        actual_team_lead_code = request.team_lead_code
        actual_team_lead_name = None
        resolved_zip = None
        location_source = None
        
        # Handle address input (NEW)
        if request.address and not effective_permit_file_id:
            from app.api.v1.routers.permit_files import _extract_zip_from_address, _validate_zip_and_get_state, _choose_team_lead_for_state, _extract_team_lead_code
            resolved_zip = _extract_zip_from_address(request.address)
            if resolved_zip:
                state_code = _validate_zip_and_get_state(resolved_zip)
                if state_code:
                    team_lead = _choose_team_lead_for_state(state_code)
                    actual_team_lead_code = team_lead  # Pass FULL name, not just code
                    actual_team_lead_name = team_lead
                    location_source = "address_input"
        
        # Handle permit_file_id (existing logic)
        elif effective_permit_file_id:
            try:
                from app.services.stage_tracking_service import get_stage_tracking_service

                stage_service = get_stage_tracking_service()
                tracking = stage_service.get_file_tracking(effective_permit_file_id)
                if tracking and hasattr(tracking, "current_stage"):
                    current_file_stage = tracking.current_stage.value
            except Exception as e:
                current_file_stage = None

            # Auto-detect team lead from file if not provided
            if not actual_team_lead_code:
                actual_team_lead_code = engine._get_team_lead_from_file(effective_permit_file_id)
                if actual_team_lead_code:
                    actual_team_lead_name = engine._extract_team_lead_code(actual_team_lead_code)
                    location_source = "permit_file"
        
        # Get optimized recommendations
        recommendations = engine.get_recommendations(
            task_description=request.task_description,
            top_k=request.top_k,
            min_score=request.min_similarity,
            team_lead_code=actual_team_lead_code,
            file_id=effective_permit_file_id,
            current_file_stage=current_file_stage,
        )
        
        processing_time = round((time.time() - start_time) * 1000, 2)  # in milliseconds
        
        # Import the function for response formatting
        from app.api.v1.routers.permit_files import _extract_team_lead_code
        
        return RecommendationResponse(
            recommendations=recommendations,
            total_found=len(recommendations),
            query_info={
                "task_description": request.task_description,
                "top_k": request.top_k,
                "min_similarity": request.min_similarity,
                "filter_by_availability": request.filter_by_availability,
                "team_lead_code": _extract_team_lead_code(actual_team_lead_code) if actual_team_lead_code else None,
                "team_lead_name": actual_team_lead_name,
                "location_source": location_source,
                "resolved_zip": resolved_zip,
                "location_filter_applied": bool(actual_team_lead_code),
                "file_id": effective_permit_file_id,
                "embedding_model": "text-embedding-004 (Vertex AI Gemini)",
                "processing_time_ms": processing_time,
                "optimization": "parallel_execution + caching + vectorized_computation"
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating recommendations: {str(e)}")

@router.post("/embeddings/refresh")
async def refresh_employee_embeddings() -> Dict[str, Any]:
    """
    Refresh all employee embeddings
    Use this when employee data is updated
    """
    try:
        # Note: Embeddings are pre-computed and stored in MongoDB
        # Use scripts/generate_initial_embeddings.py to regenerate
        return {
            "status": "info",
            "message": "Embeddings are pre-computed and stored in MongoDB. Use scripts/generate_initial_embeddings.py to regenerate if needed."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/embeddings/status")
async def get_embeddings_status() -> Dict[str, Any]:
    """
    Get status of employee embeddings
    """
    try:
        engine = get_recommendation_engine()
        employees = engine.load_employees()
        
        return {
            "total_employees": len(employees),
            "embeddings_cached": len(engine._employee_cache),
            "embedding_dimension": 768,
            "model": "text-embedding-004 (Vertex AI Gemini)",
            "cache_status": "loaded" if engine._employee_cache else "empty"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting status: {str(e)}")

@router.post("/test")
async def test_embedding_generation(text: str) -> Dict[str, Any]:
    """
    Test embedding generation for a given text
    """
    try:
        engine = get_recommendation_engine()
        embedding = engine.embedding_service.generate_embedding(text)
        
        return {
            "text": text,
            "embedding_dimension": len(embedding),
            "embedding_sample": embedding[:10],  # First 10 values
            "model": "text-embedding-004 (Vertex AI Gemini)"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error testing embedding: {str(e)}")

@router.get("/info")
async def get_service_info() -> Dict[str, Any]:
    """
    Get information about the Gemini recommendation service
    """
    from app.core.settings import settings
    
    return {
        "service": "Gemini-Powered Smart Recommendations",
        "version": "1.0.0",
        "embedding_model": "text-embedding-004",
        "embedding_dimension": 768,
        "provider": "Google Vertex AI",
        "vertex_ai_enabled": settings.use_vertex_ai,
        "project_id": settings.vertex_ai_project_id or settings.project_id,
        "region": settings.vertex_ai_region or settings.location,
        "features": [
            "Semantic employee-task matching",
            "Skill-based recommendations",
            "Experience consideration",
            "Availability filtering",
            "Batch embedding generation",
            "Real-time similarity scoring"
        ]
    }
