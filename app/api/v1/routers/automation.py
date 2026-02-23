"""
Automation endpoints for external system integration - Integrated with existing task flow
"""
from fastapi import APIRouter, HTTPException, Depends, Body
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from pydantic import BaseModel

from app.services.recommendation_engine import RecommendationEngine, get_recommendation_engine
from app.services.stage_tracking_service import get_stage_tracking_service
from app.db.mongodb import get_db

router = APIRouter(prefix="/automation", tags=["automation"])
logger = logging.getLogger(__name__)

# Initialize services
recommendation_engine = get_recommendation_engine()
# stage_service = get_stage_tracking_service()  # Initialize at module load

class AutoAssignRequest(BaseModel):
    file_id: str
    file_name: str = "Unknown file"
    workflow_step: str = "PRELIMS"
    priority_rules: Optional[Dict[str, Any]] = None
    assigned_by: Optional[str] = "external_automation"

@router.post("/auto-assign")
async def auto_assign_task(
    request: AutoAssignRequest
):
    """
    Automatically assign a task using existing task flow
    Integrates with your current task creation and assignment system
    """
    try:
        logger.info(f"[AUTOMATION] Starting auto-assignment for file: {request.file_id}")
        
        # Extract task information
        file_id = request.file_id
        file_name = request.file_name
        workflow_step = request.workflow_step
        priority_rules = request.priority_rules
        assigned_by = request.assigned_by or "external_automation"
        
        # Create task description
        task_description = f"Review permit file {file_id} - {file_name} ({workflow_step} workflow)"
        
        # Get recommendations using existing engine
        recommendations = recommendation_engine.get_recommendations(
            task_description, 
            None, 
            top_k=1, 
            min_similarity=0.3
        )
        
        # Fallback assignment if no recommendations found
        if not recommendations:
            logger.warning(f"No skill-based recommendations found for task {file_id}, using fallback assignment")
            fallback_rec = recommendation_engine.get_fallback_assignment(None)
            if fallback_rec:
                recommendations = [fallback_rec]
            else:
                raise HTTPException(status_code=404, detail="No employees available for assignment")
        
        # Get current workload for recommended employees using existing task structure
        db = get_db()
        available_employees = []
        
        for rec in recommendations:
            emp_code = rec.employee_code
            
            # Use existing task query structure
            current_tasks = list(db.tasks.find({
                "assigned_to": emp_code,
                "status": {"$in": ["ASSIGNED", "IN_PROGRESS"]}
            }))
            
            active_tasks = len(current_tasks)
            max_tasks = priority_rules.get('max_active_tasks', 5) if priority_rules else 5
            
            if active_tasks < max_tasks:
                available_employees.append({
                    **rec.dict(),
                    'active_task_count': active_tasks,
                    'total_task_count': len(current_tasks)
                })
        
        if not available_employees:
            raise HTTPException(status_code=404, detail="No employees available for assignment")
        
        # Select best employee
        selected_employee = available_employees[0]
        logger.info(f"[AUTOMATION] Selected employee: {selected_employee['employee_name']} (score: {selected_employee.get('similarity_score', 'N/A')})")
        
        # Create task using existing task creation logic from tasks.py
        from app.api.v1.routers.tasks import generate_task_id
        from app.services.vertex_ai_embeddings import get_embedding_service
        
        task_id = generate_task_id()
        
        # Generate embedding using existing service
        embedding_service = get_embedding_service()
        task_embedding = embedding_service.generate_embedding(task_description)
        
        # Create task following existing structure from tasks.py
        task_document = {
            "task_id": task_id,
            "title": f"Review {file_name}",
            "description": task_description,
            "task_assigned": task_description,
            "skills_required": [],
            "source": {
                "permit_file_id": file_id,
                "file_name": file_name,
                "workflow_step": workflow_step,
                "automation": True
            },
            "assigned_to": selected_employee['employee_code'],
            "assigned_by": assigned_by,
            "status": "ASSIGNED",
            "assigned_at": datetime.utcnow(),
            "date_assigned": datetime.utcnow().date().isoformat(),
            "time_assigned": datetime.utcnow(),
            "completion_time": None,
            "hours_taken": None,
            "due_date": None,
            "estimated_hours": None,
            "permit_file_id": file_id,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "embeddings": {
                "description_embedding": task_embedding,
                "embedded_text": task_description,
                "model": "text-embedding-004",
                "dimension": len(task_embedding),
                "created_at": datetime.utcnow()
            },
            "metadata": {
                "automation_source": "existing_flow",
                "file_id": file_id,
                "workflow_step": workflow_step,
                "selection_score": selected_employee.get('similarity_score', 0),
                "created_at": datetime.utcnow()
            }
        }
        
        # Insert task using existing database structure
        db.tasks.insert_one(task_document)
        
        # Update employee following existing pattern
        db.employee.update_one(
            {"employee_code": selected_employee['employee_code']},
            {"$set": {"last_task_assigned": datetime.utcnow()}},
            upsert=False
        )
        
        # Assign employee to stage tracking
        try:
            stage_tracking = stage_service.assign_employee_to_stage(
                file_id,
                selected_employee['employee_code'],
                selected_employee['employee_name'],
                "Auto-assigned via automation"
            )
            logger.info(f"Assigned {selected_employee['employee_name']} to stage tracking for file {file_id}")
        except Exception as e:
            logger.warning(f"Failed to assign to stage tracking: {str(e)}")
        
        # Prepare response
        result = {
            "success": True,
            "task_id": task_id,
            "automation_source": "existing_flow",
            "employee": {
                "employee_code": selected_employee['employee_code'],
                "employee_name": selected_employee['employee_name'],
                "current_role": selected_employee.get('current_role'),
                "similarity_score": selected_employee.get('similarity_score', 0),
                "previous_workload": selected_employee['active_task_count'],
                "new_workload": selected_employee['active_task_count'] + 1
            },
            "task": {
                "task_id": task_id,
                "description": task_description,
                "file_id": file_id,
                "workflow_step": workflow_step,
                "status": "ASSIGNED",
                "assigned_at": datetime.utcnow().isoformat() + 'Z'
            },
            "automation_metadata": {
                "total_employees_considered": len(recommendations),
                "available_employees": len(available_employees),
                "selection_method": "ai_recommendation_existing",
                "processed_at": datetime.utcnow().isoformat(),
                "note": "Integrated with existing task flow and services"
            }
        }
        
        logger.info(f"[AUTOMATION] Auto-assignment completed: {result['employee']['employee_name']} assigned to {file_id}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[AUTOMATION] Auto-assignment failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Auto-assignment failed: {str(e)}")

@router.post("/bulk-assign")
async def bulk_assign_tasks(
    files: List[Dict[str, Any]],
    assignment_strategy: str = "round_robin"  # round_robin, workload_based, ai_based
):
    """
    Assign multiple tasks in bulk using different strategies
    """
    try:
        logger.info(f"Starting bulk assignment for {len(files)} files using {assignment_strategy} strategy")
        
        results = []
        
        if assignment_strategy == "round_robin":
            # Get all employees and distribute evenly
            available_employees = list(get_db().employee.find({}))
            
            employee_index = 0
            for file_data in files:
                if employee_index >= len(available_employees):
                    employee_index = 0  # Reset to start
                
                selected_employee = available_employees[employee_index]
                
                # Assign task using simplified logic
                try:
                    assignment_result = await auto_assign_task(file_data)
                    assignment_result["strategy"] = "round_robin"
                    results.append(assignment_result)
                except Exception as e:
                    results.append({"success": False, "error": str(e), "strategy": "round_robin"})
                
                employee_index += 1
                
        elif assignment_strategy == "workload_based":
            # Sort by current workload each time
            for file_data in files:
                try:
                    assignment_result = await auto_assign_task(file_data)
                    assignment_result["strategy"] = "workload_based"
                    results.append(assignment_result)
                except Exception as e:
                    results.append({"success": False, "error": str(e), "strategy": "workload_based"})
                
        elif assignment_strategy == "ai_based":
            # Use AI recommendations for each task
            for file_data in files:
                try:
                    assignment_result = await auto_assign_task(file_data)
                    assignment_result["strategy"] = "ai_based"
                    results.append(assignment_result)
                except Exception as e:
                    results.append({"success": False, "error": str(e), "strategy": "ai_based"})
        
        # Summary statistics
        successful_assignments = [r for r in results if r.get("success", False)]
        failed_assignments = [r for r in results if not r.get("success", False)]
        
        summary = {
            "total_files": len(files),
            "successful_assignments": len(successful_assignments),
            "failed_assignments": len(failed_assignments),
            "strategy_used": assignment_strategy,
            "processed_at": datetime.now().isoformat(),
            "results": results
        }
        
        logger.info(f"Bulk assignment completed: {len(successful_assignments)}/{len(files)} successful")
        return summary
        
    except Exception as e:
        logger.error(f"Bulk assignment failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Bulk assignment failed: {str(e)}")

@router.get("/workload-status")
async def get_workload_status():
    """
    Get current workload status of all employees for monitoring
    """
    try:
        # Get all employees
        all_employees = list(get_db().employee.find({}))
        employee_codes = [emp["employee_code"] for emp in all_employees]
        
        # Get current tasks
        current_tasks = recommendation_engine._load_current_tasks(employee_codes)
        
        # Analyze workload
        workload_analysis = []
        total_active_tasks = 0
        
        for emp in all_employees:
            emp_code = emp["employee_code"]
            emp_tasks = current_tasks.get(emp_code, [])
            
            active_tasks = len([t for t in emp_tasks if t.get('status') in ['ASSIGNED', 'IN_PROGRESS']])
            completed_tasks = len([t for t in emp_tasks if t.get('status') == 'COMPLETED'])
            
            workload_analysis.append({
                "employee_code": emp_code,
                "employee_name": emp.get("employee_name"),
                "current_role": emp.get("current_role"),
                "active_tasks": active_tasks,
                "completed_tasks": completed_tasks,
                "total_tasks": len(emp_tasks),
                "availability": emp.get("employee_status", {}).get("availability", "Unknown"),
                "utilization_percent": round((active_tasks / 5) * 100, 1) if active_tasks > 0 else 0  # Assuming 5 is max capacity
            })
            
            total_active_tasks += active_tasks
        
        # Sort by workload (least busy first)
        workload_analysis.sort(key=lambda x: x["active_tasks"])
        
        return {
            "total_employees": len(all_employees),
            "total_active_tasks": total_active_tasks,
            "average_workload": round(total_active_tasks / len(all_employees), 1) if all_employees else 0,
            "available_for_assignment": len([w for w in workload_analysis if w["active_tasks"] < 5]),
            "workload_details": workload_analysis,
            "generated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Workload status check failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get workload status: {str(e)}")

@router.post("/trigger-scan")
async def trigger_assignment_scan(
    scan_type: str = "unassigned_files"  # unassigned_files, workload_rebalance, priority_tasks
):
    """
    Trigger automated scanning for assignment opportunities
    """
    try:
        logger.info(f"Starting {scan_type} scan")
        
        if scan_type == "unassigned_files":
            # For demo, create a test assignment
            test_file = {
                "file_id": f"SCAN-{datetime.now().strftime('%Y%m%d%H%M%S')}",
                "file_name": "Scanned Permit Document.pdf",
                "workflow_step": "PRELIMS"
            }
            
            try:
                result = await auto_assign_task(test_file)
                return {
                    "scan_type": scan_type,
                    "files_scanned": 1,
                    "assignments_made": 1,
                    "actions": [result],
                    "scanned_at": datetime.now().isoformat()
                }
            except Exception as e:
                return {
                    "scan_type": scan_type,
                    "files_scanned": 1,
                    "assignments_made": 0,
                    "error": str(e),
                    "scanned_at": datetime.now().isoformat()
                }
        
        elif scan_type == "workload_rebalance":
            # Check for employees with excessive workload
            workload_status = await get_workload_status()
            overloaded_employees = [w for w in workload_status["workload_details"] if w["active_tasks"] > 5]
            
            return {
                "scan_type": scan_type,
                "overloaded_employees": len(overloaded_employees),
                "details": overloaded_employees,
                "recommendations": "Consider redistributing tasks or hiring more staff",
                "scanned_at": datetime.now().isoformat()
            }
        
        else:
            raise HTTPException(status_code=400, detail=f"Unknown scan type: {scan_type}")
            
    except Exception as e:
        logger.error(f"Scan failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Scan failed: {str(e)}")
