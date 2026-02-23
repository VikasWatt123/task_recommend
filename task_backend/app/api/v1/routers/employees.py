"""
Employee Management Router - MongoDB Based
"""
from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import math
import logging
from datetime import datetime
from app.db.mongodb import get_db

def clean_nan_values(obj):
    """Recursively clean NaN values from objects"""
    if isinstance(obj, dict):
        return {k: clean_nan_values(v) for k, v in obj.items() if v is not None}
    elif isinstance(obj, list):
        return [clean_nan_values(item) for item in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    elif isinstance(obj, str) and obj.lower() == "nan":
        return None
    else:
        return obj

def get_employee_field(employee: dict, old_field: str, new_field: str):
    """Helper function to get field value with backward compatibility"""
    # Try new field first (MySQL-aligned), then fall back to old field
    return employee.get(new_field) or employee.get(old_field)

def find_employee_by_code(db, employee_code: str):
    """Helper function to find employee by code with backward compatibility"""
    # Try new field first (kekaemployeenumber), then fall back to old field
    employee = db.employee.find_one({"kekaemployeenumber": employee_code})
    if not employee:
        employee = db.employee.find_one({"employee_code": employee_code})
    return employee

def update_employee_fields(update_data: dict, old_field: str, new_field: str, value):
    """Helper function to update both old and new fields for backward compatibility"""
    if value is not None:
        update_data[new_field] = value
        update_data[old_field] = value

router = APIRouter(prefix="/employees", tags=["employees"])
security = HTTPBearer()

# Pydantic models for profile updates
class ProfileUpdate(BaseModel):
    employee_name: Optional[str] = None
    current_role: Optional[str] = None
    shift: Optional[str] = None
    experience_years: Optional[float] = None
    contact_email: Optional[str] = None
    reporting_manager: Optional[str] = None
    raw_technical_skills: Optional[str] = None

class TechnicalSkillsUpdate(BaseModel):
    structural_design: Optional[list[str]] = []
    electrical_design: Optional[list[str]] = []
    coordination: Optional[list[str]] = []

def get_current_employee(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """Get employee code from JWT token (simplified version)"""
    # In production, decode JWT token properly
    # For now, we'll use the X-Employee-Code header approach
    return credentials.credentials  # This should be the employee code

# Profile Management Endpoints
@router.post("/register")
async def register_new_employee(employee_data: dict, db = Depends(get_db)):
    """Register a new employee and assign them to a team"""
    
    try:
        # Check if employee already exists (check both old and new fields)
        existing_employee = find_employee_by_code(db, employee_data.get("employee_code"))
        if existing_employee:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Employee with this code already exists"
            )
        
        # Validate reporting manager exists
        manager_code = employee_data.get("reporting_manager")
        if manager_code:
            manager = find_employee_by_code(db, manager_code)
            if not manager:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Reporting manager not found"
                )
        
        # Prepare employee document with both old and new fields
        new_employee = {
            # New MySQL-aligned fields
            "kekaemployeenumber": employee_data.get("employee_code"),
            "fullname": employee_data.get("employee_name"),
            "email": employee_data.get("contact_email"),
            # Original fields (for backward compatibility)
            "employee_code": employee_data.get("employee_code"),
            "employee_name": employee_data.get("employee_name"),
            "contact_email": employee_data.get("contact_email"),
            # Other fields
            "current_role": employee_data.get("current_role"),
            "shift": employee_data.get("shift"),
            "experience_years": employee_data.get("experience_years", 0),
            "reporting_manager": employee_data.get("reporting_manager"),
            "raw_technical_skills": employee_data.get("raw_technical_skills", ""),
            "skills": employee_data.get("skills", {}),
            "status_1": employee_data.get("status_1", "Permanent"),
            "profile_updated_at": datetime.utcnow(),
            "team_changed_at": datetime.utcnow(),
            "created_at": datetime.utcnow()
        }
        
        # Insert new employee
        result = db.employee.insert_one(new_employee)
        
        if result.inserted_id:
            return {
                "success": True,
                "message": f"Employee {employee_data.get('employee_name')} registered successfully and added to {manager_code}'s team",
                "employee_code": employee_data.get("employee_code")
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to register employee"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Registration failed: {str(e)}"
        )

@router.get("/profile/me")
async def get_my_profile(employee_code: str = Depends(get_current_employee), db = Depends(get_db)):
    """Get current employee's profile"""
    
    employee = find_employee_by_code(db, employee_code)
    
    if not employee:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Employee not found"
        )
    
    # Remove sensitive fields
    employee.pop("_id", None)
    employee.pop("embedding", None)  # Don't expose embeddings
    
    return {
        "success": True,
        "data": employee
    }

@router.put("/profile/me")
async def update_my_profile(
    profile_data: ProfileUpdate,
    employee_code: str = Depends(get_current_employee),
    db = Depends(get_db)
):
    """Update current employee's profile"""
    
    # Check if employee exists
    employee = find_employee_by_code(db, employee_code)
    if not employee:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Employee not found"
        )
    
    # Prepare update data
    update_data = {}
    
    # Only update fields that are provided (update both old and new fields)
    if profile_data.employee_name is not None:
        update_employee_fields(update_data, "employee_name", "fullname", profile_data.employee_name)
    
    if profile_data.current_role is not None:
        update_data["current_role"] = profile_data.current_role
    
    if profile_data.shift is not None:
        update_data["shift"] = profile_data.shift
    
    if profile_data.experience_years is not None:
        update_data["experience_years"] = profile_data.experience_years
    
    if profile_data.contact_email is not None:
        # Simple email validation
        if profile_data.contact_email and "@" not in profile_data.contact_email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid email format. Please include '@' in email address."
            )
        update_employee_fields(update_data, "contact_email", "email", profile_data.contact_email)
    
    if profile_data.raw_technical_skills is not None:
        update_data["raw_technical_skills"] = profile_data.raw_technical_skills
    
    # Handle reporting manager change
    old_reporting_manager = employee.get("reporting_manager")
    new_reporting_manager = profile_data.reporting_manager
    new_manager = None  # Initialize to avoid undefined reference
    
    if new_reporting_manager is not None and new_reporting_manager != old_reporting_manager:
        # Validate that the new reporting manager exists (check both old and new fields)
        new_manager = db.employee.find_one({
            "$or": [
                {"kekaemployeenumber": new_reporting_manager},
                {"employee_code": new_reporting_manager}
            ],
            "current_role": {"$regex": "Team Lead|Lead|Manager", "$options": "i"}
        })
        
        if not new_manager:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid reporting manager. Please check the employee code."
            )
        
        update_data["reporting_manager"] = new_reporting_manager
        update_data["team_changed_at"] = datetime.utcnow()
    
    if not update_data:
        return {
            "success": True,
            "message": "No changes to update",
            "data": None
        }
    
    # Add timestamp
    update_data["profile_updated_at"] = datetime.utcnow()
    
    # Update the employee record (update both old and new fields for backward compatibility)
    result = db.employee.update_one(
        {"$or": [
            {"kekaemployeenumber": employee_code},
            {"employee_code": employee_code}
        ]},
        {"$set": update_data}
    )
    
    if result.modified_count == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to update profile"
        )
    
    # Get updated employee data
    updated_employee = find_employee_by_code(db, employee_code)
    
    # Remove MongoDB ObjectId and other sensitive fields
    if updated_employee:
        updated_employee.pop("_id", None)
        updated_employee.pop("embedding", None)
    
    response_data = {
        "success": True,
        "message": "Profile updated successfully",
        "data": updated_employee
    }
    
    # Add team change notification if applicable
    if new_reporting_manager and new_reporting_manager != old_reporting_manager and new_manager:
        response_data["team_change"] = {
            "old_manager": old_reporting_manager,
            "new_manager": new_reporting_manager,
            "new_manager_name": get_employee_field(new_manager, "employee_name", "fullname"),
            "message": f"You have been dealt to {get_employee_field(new_manager, 'employee_name', 'fullname')}'s team"
        }
    
    return response_data

@router.put("/profile/me/technical-skills")
async def update_technical_skills(
    skills_data: TechnicalSkillsUpdate,
    employee_code: str = Depends(get_current_employee),
    db = Depends(get_db)
):
    """Update employee's technical skills"""
    
    # Check if employee exists
    employee = find_employee_by_code(db, employee_code)
    if not employee:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Employee not found"
        )
    
    # Prepare skills update
    skills_update = {}
    
    if skills_data.structural_design is not None:
        skills_update["skills.structural_design"] = skills_data.structural_design
    
    if skills_data.electrical_design is not None:
        skills_update["skills.electrical_design"] = skills_data.electrical_design
    
    if skills_data.coordination is not None:
        skills_update["skills.coordination"] = skills_data.coordination
    
    if not skills_update:
        return {
            "success": True,
            "message": "No skill changes to update",
            "data": None
        }
    
    # Add timestamp
    skills_update["skills_updated_at"] = datetime.utcnow()
    
    # Update the employee record (update both old and new fields for backward compatibility)
    result = db.employee.update_one(
        {"$or": [
            {"kekaemployeenumber": employee_code},
            {"employee_code": employee_code}
        ]},
        {"$set": skills_update}
    )
    
    if result.modified_count == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to update skills"
        )
    
    # Get updated employee data
    updated_employee = find_employee_by_code(db, employee_code)
    
    return {
        "success": True,
        "message": "Technical skills updated successfully",
        "data": updated_employee
    }

@router.get("/profile/available-managers")
async def get_available_managers(db = Depends(get_db)):
    """Get list of available team leads/managers for reassignment"""
    
    # Find all employees with team lead roles
    managers = list(db.employee.find(
        {
            "current_role": {"$regex": "Team Lead|Lead|Manager|Assistant Group Manager", "$options": "i"}
        },
        {
            "_id": 0,
            "employee_code": 1,
            "kekaemployeenumber": 1,
            "employee_name": 1,
            "fullname": 1,
            "current_role": 1,
            "reporting_manager": 1
        }
    ).sort("employee_name", 1))
    
    return {
        "success": True,
        "data": managers
    }

@router.get("/with-managers")
async def get_employees_with_managers() -> List[Dict[str, Any]]:
    """Get all employees with full reporting manager information"""
    db = get_db()
    employees = list(db.employee.find({}, {"_id": 0}))
    
    # Transform data to include full manager information
    transformed_employees = []
    for emp in employees:
        transformed_emp = {
            "employee_code": emp.get("employee_code"),
            "employee_name": emp.get("employee_name"),
            "date_of_birth": emp.get("date_of_birth"),
            "joining_date": emp.get("joining_date"),
            "experience_years": emp.get("experience_years", 0),
            "technical_skills": emp.get("skills", {}),
            "raw_technical_skills": emp.get("raw_technical_skills", ""),
            "raw_strength_expertise": emp.get("raw_strength_expertise", ""),
            "current_role": emp.get("current_role"),
            "reporting_manager": emp.get("reporting_manager", ""),  # Direct field
            "contact_email": emp.get("contact_email"),
            "shift": emp.get("shift"),
            "status_1": emp.get("status_1"),
            "status_2": emp.get("status_2"),
            "status_3": emp.get("status_3"),
            "list_of_task_assigned": emp.get("List of task assigned", ""),
            "special_task": emp.get("Special Task", ""),
            "employee_status": {
                "availability": "ACTIVE" if emp.get("status_1") == "Permanent" else "INACTIVE"
            }
        }
        transformed_employees.append(transformed_emp)
    
    # Clean any NaN values
    return clean_nan_values(transformed_employees)

@router.get("/available")
async def get_available_employees(
    stage: Optional[str] = None,
    max_tasks: Optional[int] = None
) -> Dict[str, Any]:
    """
    Get available employees with stage-aware filtering.
    NEVER returns 404 - returns empty list instead.
    
    Args:
        stage: PRELIMS, PRODUCTION, QUALITY (optional)
        max_tasks: Maximum current tasks (optional, defaults vary by stage)
    
    Returns:
        {
            "employees": [...],
            "availability_mode": "strict | relaxed | fallback",
            "total_before_filter": int,
            "total_after_filter": int,
            "filters_applied": {...}
        }
    """
    db = get_db()
    
    # Stage-aware max task limits
    stage_limits = {
        "PRELIMS": 5,
        "PRODUCTION": 3,
        "QUALITY": 4
    }
    
    # Use stage-specific limit or provided max_tasks
    if max_tasks is None and stage:
        max_tasks = stage_limits.get(stage.upper(), 5)
    elif max_tasks is None:
        max_tasks = 5
    
    # Fetch all employees
    all_employees = list(db.employee.find({}, {"_id": 0}))
    total_before = len(all_employees)
    
    # Get task counts for each employee
    task_counts = {}
    for emp in all_employees:
        emp_code = emp.get("employee_code")
        task_count = db.tasks.count_documents({
            "assigned_to": emp_code,
            "status": {"$in": ["assigned", "in_progress"]}
        })
        task_counts[emp_code] = task_count
    
    # Apply filters
    filters_applied = {
        "status_permanent": True,
        "max_tasks": max_tasks,
        "stage": stage
    }
    
    # Strict filtering
    available = []
    for emp in all_employees:
        emp_code = emp.get("employee_code")
        
        # Must be permanent
        if emp.get("status_1") != "Permanent":
            continue
        
        # Must be under task limit
        current_tasks = task_counts.get(emp_code, 0)
        if current_tasks >= max_tasks:
            continue
        
        # Add current task count to employee data
        emp["current_tasks"] = current_tasks
        available.append(emp)
    
    availability_mode = "strict"
    
    # Fallback 1: Relax task limit if no employees found
    if not available:
        availability_mode = "relaxed"
        filters_applied["max_tasks_relaxed"] = max_tasks * 2
        
        for emp in all_employees:
            emp_code = emp.get("employee_code")
            
            if emp.get("status_1") != "Permanent":
                continue
            
            current_tasks = task_counts.get(emp_code, 0)
            if current_tasks >= max_tasks * 2:
                continue
            
            emp["current_tasks"] = current_tasks
            available.append(emp)
    
    # Fallback 2: Return all permanent employees
    if not available:
        availability_mode = "fallback"
        filters_applied["fallback_reason"] = "No employees under relaxed limits"
        
        for emp in all_employees:
            if emp.get("status_1") == "Permanent":
                emp_code = emp.get("employee_code")
                emp["current_tasks"] = task_counts.get(emp_code, 0)
                available.append(emp)
    
    # Transform to expected format
    transformed = []
    for emp in available:
        transformed.append({
            "employee_code": emp.get("employee_code"),
            "employee_name": emp.get("employee_name"),
            "current_role": emp.get("current_role"),
            "shift": emp.get("shift"),
            "experience_years": emp.get("experience_years", 0),
            "current_tasks": emp.get("current_tasks", 0),
            "skills": emp.get("skills", {}),
            "raw_technical_skills": emp.get("raw_technical_skills", ""),
            "status_1": emp.get("status_1"),
            "reporting_manager": emp.get("reporting_manager", "")
        })
    
    return {
        "employees": clean_nan_values(transformed),
        "availability_mode": availability_mode,
        "total_before_filter": total_before,
        "total_after_filter": len(transformed),
        "filters_applied": filters_applied
    }

@router.get("/")
async def get_employees() -> List[Dict[str, Any]]:
    """Get all employees from MongoDB"""
    db = get_db()
    employees = list(db.employee.find({}, {"_id": 0}))  # Include all fields except _id
    
    # Transform data to match expected frontend format
    transformed_employees = []
    for emp in employees:
        transformed_emp = {
            "employee_code": emp.get("employee_code"),
            "employee_name": emp.get("employee_name"),
            "date_of_birth": emp.get("date_of_birth"),
            "joining_date": emp.get("joining_date"),
            "experience_years": emp.get("experience_years", 0),
            "technical_skills": emp.get("skills", {}),
            "raw_technical_skills": emp.get("raw_technical_skills", ""),
            "raw_strength_expertise": emp.get("raw_strength_expertise", ""),
            "current_role": emp.get("current_role"),
            "reporting_manager": emp.get("reporting_manager", ""),  # Direct field
            "contact_email": emp.get("contact_email"),
            "shift": emp.get("shift"),
            "status_1": emp.get("status_1"),
            "status_2": emp.get("status_2"),
            "status_3": emp.get("status_3"),
            "list_of_task_assigned": emp.get("List of task assigned", ""),
            "special_task": emp.get("Special Task", ""),
            "employee_status": {
                "availability": "ACTIVE" if emp.get("status_1") == "Permanent" else "INACTIVE"
            },
            "embedding": emp.get("embedding", []),  # Single embedding
            "metadata": emp.get("metadata", {})
        }
        transformed_employees.append(transformed_emp)
    
    # Clean any NaN values
    return clean_nan_values(transformed_employees)

@router.get("/employees-grouped-by-team-lead")
async def get_employees_grouped_by_team_lead():
    """Get employees grouped by team lead from MongoDB - optimized version"""
    logger = logging.getLogger(__name__)
    logger.info("[EMPLOYEES-GROUPED-MAIN-START] Fetching employees grouped by team lead (main router)")
    
    try:
        db = get_db()
        
        # Optimized query with timeout and field projection
        employees = list(db.employee.find(
            {"status_1": "Permanent"},
            {
                "_id": 0,
                "employee_code": 1,
                "employee_name": 1,
                "reporting_manager": 1,
                "current_role": 1,
                "experience_years": 1,
                "contact_email": 1,
                "status_1": 1,
                "technical_skills": 1
            }
        ).max_time_ms(10000).limit(100))
        
        logger.info(f"[EMPLOYEES-GROUPED-MAIN-SUCCESS] Retrieved {len(employees)} employees")
        
        if not employees:
            return []
        
        # Group by reporting manager with uniform parsing
        teams = {}
        team_lead_cache = {}
        
        for emp in employees:
            manager = emp.get("reporting_manager", "Unassigned")
            
            # Parse reporting_manager uniformly: "Name (Code)" -> extract name and code
            team_lead_code = manager
            team_lead_name = manager
            
            if manager and manager != "Unassigned" and "(" in manager and ")" in manager:
                import re
                match = re.match(r"(.+?)\s*\(([^)]+)\)", manager.strip())
                if match:
                    team_lead_name = match.group(1).strip()
                    team_lead_code = match.group(2).strip()
            
            # Cache team lead info to avoid repeated parsing
            if team_lead_code not in team_lead_cache:
                team_lead_cache[team_lead_code] = {
                    "team_lead_name": team_lead_name,
                    "team_lead_code": team_lead_code
                }
            
            # Initialize team if not exists
            if team_lead_code not in teams:
                lead_info = team_lead_cache[team_lead_code]
                teams[team_lead_code] = {
                    "team_lead_code": lead_info["team_lead_code"],
                    "team_lead_name": lead_info["team_lead_name"],
                    "employees": []
                }
            
            # Add employee to team with consistent fields
            teams[team_lead_code]["employees"].append({
                "employee_code": emp.get("employee_code"),
                "employee_name": emp.get("employee_name"),
                "current_role": emp.get("current_role"),
                "experience_years": emp.get("experience_years", 0),
                "skills": emp.get("technical_skills", {}),  # Use technical_skills instead of skills
                "status": emp.get("status_1", ""),
                "contact_email": emp.get("contact_email")
            })
        
        # Convert to list and sort
        result = list(teams.values())
        result.sort(key=lambda x: x["team_lead_name"])
        
        logger.info(f"[EMPLOYEES-GROUPED-MAIN-SUCCESS] Returning {len(result)} teams")
        
        return clean_nan_values(result)
        
    except Exception as e:
        logger.error(f"[EMPLOYEES-GROUPED-MAIN-ERROR] Failed to fetch employees: {str(e)}")
        # Return empty list instead of crashing
        return []

@router.get("/{employee_code}")
async def get_employee(employee_code: str):
    """Get a specific employee by code"""
    db = get_db()
    employee = db.employee.find_one({"employee_code": employee_code}, {"_id": 0})
    
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")
    
    # Map experience fields to match frontend expectations
    employee["current_experience_years"] = employee.get("experience_years", 0)
    employee["previous_experience_years"] = 0  # Not tracked separately in new structure
    
    # Map technical skills
    employee["technical_skills"] = employee.get("skills", {})
    
    # Map employment status
    employee["employee_status"] = {
        "availability": "ACTIVE" if employee.get("status_1") == "Permanent" else "INACTIVE"
    }
    
    # Clean any NaN values
    return clean_nan_values(employee)

@router.get("/{employee_code}/tasks")
async def get_employee_tasks(employee_code: str):
    """Get tasks for a specific employee from profile_building collection"""
    db = get_db()
    
    # Get tasks from profile_building collection
    tasks = list(db.profile_building.find(
        {"employee_code": employee_code},
        {"_id": 0}
    ).sort("time_assigned", -1))
    
    return {
        "tasks": tasks,
        "total": len(tasks)
    }

class TaskAssignment(BaseModel):
    task_description: str
    assigned_by: str

@router.post("/{employee_code}/assign-task")
async def assign_task_to_employee(employee_code: str, task_data: TaskAssignment):
    """Assign a task to an employee"""
    db = get_db()
    
    # Verify employee exists
    employee = db.employee.find_one({"employee_code": employee_code})
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")
    
    # This endpoint is kept for compatibility but actual task assignment
    # should go through the tasks router which handles embeddings and recommendations
    
    return {
        "message": "Task assigned successfully",
        "employee_code": employee_code,
        "note": "Use /api/v1/tasks/{task_id}/assign for full task assignment workflow"
    }
