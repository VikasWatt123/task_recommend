"""
Updated Recommendation Engine for Employee Task Assignment
- Works with new Excel-based structure
- Uses task-aware embeddings (skills + previous tasks)
- Fast recommendations with pre-computed embeddings
"""

from typing import List, Dict, Any, Optional, Tuple
import numpy as np
from datetime import datetime, timedelta
from pydantic import BaseModel
import re
import logging

from app.db.mongodb import get_db
from app.services.vertex_ai_embeddings import VertexAIEmbeddingService
from app.services.skill_normalizer import SkillNormalizer
from app.services.stage_assignment_service import StageAssignmentService
from app.models.stage_flow import FileStage

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class EmployeeRecommendation(BaseModel):
    """Employee recommendation model"""
    employee_code: str
    employee_name: str
    similarity_score: float
    match_percentage: int
    current_role: str
    shift: str
    experience_years: float
    availability: str
    reasoning: str
    skills_match: Dict[str, List[str]]
    task_relevance: str
    current_tasks: List[Dict[str, Any]] = []
    active_task_count: int = 0
    total_task_count: int = 0

class RecommendationEngine:
    """Enhanced recommendation engine with task awareness"""
    
    def __init__(self):
        self.db = get_db()
        self.embedding_service = VertexAIEmbeddingService()
        self.skill_normalizer = SkillNormalizer()
        self._employee_cache = {}
        self._cache_timestamp = None
        self._cache_ttl = 300  # 5 minutes
    
    def _extract_team_lead_code(self, team_lead: str) -> Optional[str]:
        """Extract team lead code from name string."""
        if not team_lead:
            return None
        match = re.search(r"\(([^)]+)\)", team_lead)
        return match.group(1).strip() if match else None
    
    def _get_team_lead_from_file(self, file_id: str) -> Optional[str]:
        """Get the team lead assigned to a file using ZIP code mapping (same as auto-assign)"""
        try:
            # First check if there are tasks assigned to this file (for existing files)
            tasks = list(self.db.tasks.find(
                {"source.permit_file_id": file_id},
                {"assigned_to": 1, "assigned_to_lead": 1, "_id": 0}
            ).sort([("assigned_at", -1)]).limit(1))
            
            if tasks:
                latest_task = tasks[0]
                team_lead = latest_task.get("assigned_to_lead")
                if team_lead:
                    print(f"[DEBUG] Found team lead from existing tasks: {team_lead}")
                    return team_lead

            # For new files, use ZIP/state mapping like auto-assign does.
            # The generic upload endpoint may not persist ZIP/state fields, so we derive
            # them from the stored PDF file when needed.
            permit_file = self.db.permit_files.find_one(
                {"file_id": file_id},
                {
                    "assigned_to_lead": 1,
                    "locked_team_lead": 1,
                    "detected_zip": 1,
                    "detected_state": 1,
                    "file_info.file_path": 1,
                    "_id": 0,
                },
            )
            
            if not permit_file:
                print(f"[DEBUG] No permit file found for: {file_id}")
                return None

            locked_lead = permit_file.get("locked_team_lead") or permit_file.get("assigned_to_lead")
            if locked_lead and locked_lead != "SYSTEM":
                print(f"[DEBUG] Found locked team lead from permit file: {locked_lead}")
                return locked_lead

            detected_zip = permit_file.get("detected_zip")
            detected_state = permit_file.get("detected_state")

            from app.api.v1.routers.zip_assign import _choose_team_lead_for_state

            if not detected_zip or not detected_state:
                file_path = (permit_file.get("file_info") or {}).get("file_path")
                if not file_path:
                    print(f"[DEBUG] No file path found for permit file: {file_id}")
                    return None

                try:
                    with open(file_path, "rb") as f:
                        pdf_bytes = f.read()
                except Exception as e:
                    print(f"[DEBUG] Failed to read PDF from disk for {file_id}: {e}")
                    return None

                from app.api.v1.routers.zip_assign import (
                    _extract_zip_from_pdf_first_page,
                    _validate_zip_and_get_state,
                )

                detected_zip = _extract_zip_from_pdf_first_page(pdf_bytes)
                if not detected_zip:
                    print(f"[DEBUG] Could not extract ZIP from PDF for file: {file_id}")
                    return None

                detected_state = _validate_zip_and_get_state(detected_zip)
                if not detected_state:
                    print(f"[DEBUG] ZIP {detected_zip} did not map to any configured state")
                    return None

                print(f"[DEBUG] Derived ZIP/state for file {file_id}: {detected_zip} / {detected_state}")

                # Cache on permit file for next time (best-effort)
                try:
                    self.db.permit_files.update_one(
                        {"file_id": file_id},
                        {
                            "$set": {
                                "detected_zip": detected_zip,
                                "detected_state": detected_state,
                                "metadata.updated_at": datetime.utcnow(),
                            }
                        },
                    )
                except Exception:
                    pass

            team_lead = _choose_team_lead_for_state(detected_state)
            if not team_lead:
                print(f"[DEBUG] No team lead found for state: {detected_state}")
                return None

            print(f"[DEBUG] Selected team lead for state {detected_state}: {team_lead}")

            # Cache locked lead (best-effort)
            try:
                self.db.permit_files.update_one(
                    {"file_id": file_id},
                    {
                        "$set": {
                            "locked_team_lead": team_lead,
                            "metadata.updated_at": datetime.utcnow(),
                        }
                    },
                )
            except Exception:
                pass

            return team_lead
            
        except Exception as e:
            print(f"[DEBUG] Error getting team lead from file: {e}")
            return None
    
    # ===================== LOAD EMPLOYEES ====
    
    def load_employees(self, team_lead_code: Optional[str] = None):
        """Load employees with new structure and current task information"""
        cache_key = team_lead_code or "ALL"

        if cache_key in self._employee_cache and self._cache_valid():
            return self._employee_cache[cache_key]
        
        # If no team lead specified, load all employees
        if not team_lead_code:
            employees = list(self.db.employee.find(
                {"status_1": "Permanent"},
                {
                    "_id": 0,
                    "skills": 1,
                    "technical_skills": 1,
                    "raw_technical_skills": 1,
                    "raw_strength_expertise": 1,
                    "kekaemployeenumber": 1,
                    "employee_name": 1,
                    "current_role": 1,
                    "shift": 1,
                    "experience_years": 1,
                    "status_1": 1,
                    "reporting_manager": 1,
                    "List of task assigned": 1,
                    "Special Task": 1,
                    "embedding": 1,
                }
            ))
        else:
            # For team-specific recommendations, use the same logic as get_employees_grouped_by_team_lead
            # Team assignment is based on reporting_manager field
            print(f"[DEBUG] Loading employees for team lead: {team_lead_code}")
            
            # Get all permanent employees
            all_employees = list(self.db.employee.find(
                {"status_1": "Permanent"},
                {
                    "_id": 0,
                    "skills": 1,
                    "technical_skills": 1,
                    "raw_technical_skills": 1,
                    "raw_strength_expertise": 1,
                    "kekaemployeenumber": 1,
                    "employee_name": 1,
                    "current_role": 1,
                    "shift": 1,
                    "experience_years": 1,
                    "status_1": 1,
                    "reporting_manager": 1,
                    "List of task assigned": 1,
                    "Special Task": 1,
                    "embedding": 1,
                }
            ))
            
            print(f"[DEBUG] Found {len(all_employees)} total employees")
            
            # Filter employees by reporting_manager (same logic as get_employees_grouped_by_team_lead)
            employees = []
            for emp in all_employees:
                reporting_manager = emp.get("reporting_manager", "")
                
                # Handle both formats: "0083" and "Shivam Kumar (0083)"
                if reporting_manager == team_lead_code:
                    employees.append(emp)
                    print(f"[DEBUG] Employee {emp.get('employee_name')} reports to {team_lead_code} (direct code match)")
                elif team_lead_code in reporting_manager:
                    # Extract code from "Shivam Kumar (0083)" format
                    import re
                    match = re.search(r"\(([^)]+)\)", reporting_manager)
                    if match:
                        extracted_code = match.group(1).strip()
                        if extracted_code == team_lead_code:
                            employees.append(emp)
                            print(f"[DEBUG] Employee {emp.get('employee_name')} reports to {team_lead_code} (extracted from {reporting_manager})")
            
            print(f"[DEBUG] Found {len(employees)} employees under team lead {team_lead_code}")
        
        # Load current tasks for all employees
        employee_numbers = [emp.get("kekaemployeenumber") for emp in employees]
        current_tasks = self._load_current_tasks(employee_numbers)
        
        # Attach task information to each employee
        for emp in employees:
            emp_number = emp.get("kekaemployeenumber")
            emp["current_tasks"] = current_tasks.get(emp_number, [])
            emp["active_task_count"] = len([t for t in current_tasks.get(emp_number, []) if t.get("status") == "ASSIGNED"])
            emp["total_task_count"] = len(current_tasks.get(emp_number, []))
        
        self._employee_cache[cache_key] = employees
        self._cache_timestamp = datetime.utcnow()

        return employees
    
    def _load_current_tasks(self, employee_codes: List[str]) -> Dict[str, List[Dict]]:
        """Load current tasks for the given employee codes"""
        if not employee_codes:
            return {}
        
        # Convert employee codes to task format (without leading zeros)
        # Tasks store codes like '622' while employees store '0622'
        task_codes = [code.lstrip('0') or '0' for code in employee_codes]
        
        # Build combined code list: both stripped and original formats
        all_codes = list(set(task_codes + employee_codes))
        
        # Get all tasks for these employees (both active and completed)
        # Query both assigned_to and employee_code for backward compatibility
        tasks = list(self.db.tasks.find(
            {
                "$or": [
                    {"assigned_to": {"$in": all_codes}},
                    {"employee_code": {"$in": all_codes}}
                ]
            },
            {
                "_id": 0,
                "task_id": 1,
                "title": 1,
                "description": 1,
                "assigned_to": 1,
                "employee_code": 1,
                "status": 1,
                "assigned_at": 1,
                "due_date": 1,
                "skills_required": 1
            }
        ).sort("assigned_at", -1))  # Most recent first
        
        # Group tasks by employee (convert back to employee code format with leading zeros)
        tasks_by_employee = {}
        for task in tasks:
            # Use assigned_to first, fall back to employee_code
            task_code = task.get("assigned_to") or task.get("employee_code")
            # Find the corresponding employee code with leading zeros
            emp_code = None
            for original_code in employee_codes:
                if (original_code.lstrip('0') or '0') == task_code or original_code == task_code:
                    emp_code = original_code
                    break
            
            if emp_code and emp_code not in tasks_by_employee:
                tasks_by_employee[emp_code] = []
            if emp_code:
                tasks_by_employee[emp_code].append(task)
        
        print(f"[DEBUG] Loaded all tasks for {len(tasks_by_employee)} employees")
        return tasks_by_employee

    def _cache_valid(self) -> bool:
        """Check if cache is still valid"""
        if not self._cache_timestamp:
            return False
        return datetime.utcnow() - self._cache_timestamp < timedelta(seconds=self._cache_ttl)
    
    # ===================== EMBEDDINGS =====================
    
    def embed_text(self, text: str) -> List[float]:
        """Generate embedding for task description"""
        return self.embedding_service.generate_embedding(text)
    
    # ===================== RECOMMENDATIONS =====================
    
    def get_recommendations(
        self,
        task_description: str,
        team_lead_code: Optional[str] = None,
        top_k: int = 10,
        min_score: float = 0.3,
        file_id: Optional[str] = None,
        current_file_stage: Optional[str] = None
    ) -> List[EmployeeRecommendation]:
        """Get task recommendations using hybrid scoring (embedding + keyword matching)"""
        
        print(f"[DEBUG] Getting recommendations for: {task_description}")
        print(f"[DEBUG] Team lead: {team_lead_code}")
        print(f"[DEBUG] File ID: {file_id}, Current Stage: {current_file_stage}")
        
        # Auto-detect team lead from file if not provided
        if not team_lead_code and file_id:
            team_lead_code = self._get_team_lead_from_file(file_id)
            print(f"[DEBUG] Auto-detected team lead from file: {team_lead_code}")
        
        # Load employees
        employees = self.load_employees(team_lead_code)
        print(f"[DEBUG] Loaded {len(employees)} employees")
        
        if not employees:
            print("[DEBUG] No employees found")
            return []
        
        # Use enhanced stage detection with context
        detected_stage = StageAssignmentService.detect_stage_from_description_with_context(
            task_description or "", file_id, current_file_stage
        )

        # Handle case where file is COMPLETED
        if detected_stage is None:
            # Check if this is a QC task for a completed file
            description_lower = task_description.lower()
            qc_keywords = ["qc", "quality check", "quality control", "review", "inspection", 
                          "verify", "audit", "check quality", "quality review", "quality assurance"]
            
            if any(keyword in description_lower for keyword in qc_keywords):
                logger.info(f"File {file_id} is COMPLETED but QC task requested - allowing QC recommendations")
                # Return QC recommendations for completed files
                return self._qc_priority_recommendations(employees, top_k)
            else:
                logger.info(f"File {file_id} is in COMPLETED stage - no recommendations until moved to QC")
                return []

        # PRELIMS keywords should be routed to new joinees / low experience
        if detected_stage == FileStage.PRELIMS:
            return self._prelims_priority_recommendations(employees, top_k)
        
        # QC keywords should be routed to experienced employees
        if detected_stage == FileStage.QC:
            return self._qc_priority_recommendations(employees, top_k)

        # Generate task embedding
        task_embedding = self.embed_text(task_description)
        task_embedding = np.array(task_embedding)
        task_embedding = task_embedding / np.linalg.norm(task_embedding)
        
        # Extract keywords from task description for direct matching
        task_keywords = self._extract_task_keywords(task_description)
        print(f"[DEBUG] Task keywords extracted: {task_keywords}")
        
        # Calculate similarities
        recommendations = []
        
        for emp in employees:
            # Get employee embedding
            emp_embedding = np.array(emp.get("embedding", []))
            
            # Calculate embedding similarity (if available)
            embedding_similarity = 0.0
            if len(emp_embedding) > 0:
                emp_embedding = emp_embedding / np.linalg.norm(emp_embedding)
                embedding_similarity = float(np.dot(task_embedding, emp_embedding))
            
            # Calculate keyword-based skill match score
            keyword_score = self._calculate_keyword_score(emp, task_keywords, task_description)
            print(f"[DEBUG] Employee {emp.get('employee_name')}: keyword_score={keyword_score}, embedding_similarity={embedding_similarity}")
            
            # Calculate weighted hybrid score (40% keyword, 35% embedding, 15% experience, 10% workload)
            # Normalize scores to 0-1 range
            keyword_weight = 0.40
            embedding_weight = 0.35
            experience_weight = 0.15
            workload_weight = 0.10
            
            # Experience score (normalize by 10 years max)
            experience_years = emp.get("experience_years", 0) or 0
            # For PRELIMS, invert experience so less experience gets higher score
            # For QC, keep normal so more experience gets higher score
            if detected_stage == FileStage.PRELIMS:
                experience_score = max(0, 1 - (experience_years / 10.0))  # Inverted for PRELIMS
            else:
                experience_score = min(experience_years / 10.0, 1.0)  # Normal for QC and others
            
            # Workload score (inverse - fewer tasks = higher score)
            active_tasks = emp.get("active_task_count", 0) or 0
            max_tasks = 10  # Consider 10+ tasks as full workload
            workload_score = max(0, 1 - (active_tasks / max_tasks))
            
            # Calculate weighted score
            weighted_score = (
                (keyword_score * keyword_weight) +
                (embedding_similarity * embedding_weight) +
                (experience_score * experience_weight) +
                (workload_score * workload_weight)
            )
            
            # Use weighted score, but ensure minimum threshold
            similarity = max(weighted_score, 0.1)
            print(f"[DEBUG] Employee {emp.get('employee_name')}: Weighted score={weighted_score:.3f} (keyword:{keyword_score:.3f}, embed:{embedding_similarity:.3f}, exp:{experience_score:.3f}, work:{workload_score:.3f}) -> final={similarity:.3f}")
            
            # Skip if below threshold (but be more lenient for team-specific recommendations)
            threshold = min_score if team_lead_code else 0.1  # Lower threshold for team-specific
            if similarity < threshold:
                print(f"[DEBUG] Employee {emp.get('employee_name')}: Score {similarity} below threshold {threshold}")
                continue
            
            # Build reasoning
            reasoning = self.build_reasoning(task_description, emp, similarity)
            
            # Create recommendation
            skills_match = self.extract_skills_match(emp)
            
            rec = EmployeeRecommendation(
                employee_code=emp.get("employee_code") or emp.get("kekaemployeenumber"),
                employee_name=emp.get("employee_name"),
                similarity_score=round(similarity, 3),
                match_percentage=int(similarity * 100),
                current_role=emp.get("current_role", "Not specified"),
                shift=emp.get("shift", "Day"),
                experience_years=emp.get("experience_years", 0),
                availability="ACTIVE" if emp.get("status_1") == "Permanent" else "INACTIVE",
                reasoning=reasoning,
                skills_match=skills_match,
                task_relevance=self.extract_task_relevance(emp, task_description),
                current_tasks=emp.get("current_tasks", []),
                active_task_count=emp.get("active_task_count", 0),
                total_task_count=emp.get("total_task_count", 0)
            )
            
            recommendations.append(rec)
        
        # Sort by similarity and return top_k
        recommendations.sort(key=lambda x: x.similarity_score, reverse=True)

        # If no one met the min_score threshold, still return a best-effort suggestion
        if not recommendations:
            print(f"[DEBUG] No recommendations met threshold, using fallback assignment")
            fallback = self.get_fallback_assignment(team_lead_code=team_lead_code, task_description=task_description)
            return [fallback] if fallback else []
        
        # For team-specific recommendations, if we have some results but fewer than top_k,
        # add fallback recommendations to fill the list
        if team_lead_code and len(recommendations) < top_k:
            print(f"[DEBUG] Have {len(recommendations)} recommendations, adding fallback to reach {top_k}")
            fallback = self.get_fallback_assignment(team_lead_code=team_lead_code, task_description=task_description)
            if fallback and fallback.employee_code not in [r.employee_code for r in recommendations]:
                recommendations.append(fallback)

        return recommendations[:top_k]

    def _prelims_priority_recommendations(
        self,
        employees: List[Dict[str, Any]],
        top_k: int
    ) -> List[EmployeeRecommendation]:
        """For PRELIMS-style work, prioritize low experience (new joinees) and low workload."""

        if not employees:
            return []

        # Prefer lower experience, then lower active workload.
        employees_sorted = sorted(
            employees,
            key=lambda e: (
                e.get("experience_years", 0) or 0,
                e.get("active_task_count", 0),
            ),
        )

        recommendations: List[EmployeeRecommendation] = []
        for emp in employees_sorted[:top_k]:
            skills_match = self.extract_skills_match(emp)
            exp = emp.get("experience_years", 0) or 0
            active = emp.get("active_task_count", 0)
            reasoning = f"PRELIMS priority: lower experience ({exp}y) and low workload ({active} active tasks)"

            recommendations.append(
                EmployeeRecommendation(
                    employee_code=emp.get("employee_code") or emp.get("kekaemployeenumber"),
                    employee_name=emp.get("employee_name"),
                    similarity_score=0.35,
                    match_percentage=35,
                    current_role=emp.get("current_role", "Not specified"),
                    shift=emp.get("shift", "Day"),
                    experience_years=exp,
                    availability="ACTIVE" if emp.get("status_1") == "Permanent" else "INACTIVE",
                    reasoning=reasoning,
                    skills_match=skills_match,
                    task_relevance="Preliminary design work",
                    current_tasks=emp.get("current_tasks", []),
                    active_task_count=emp.get("active_task_count", 0),
                    total_task_count=emp.get("total_task_count", 0)
                )
            )

        return recommendations

    def _qc_priority_recommendations(
        self,
        employees: List[Dict[str, Any]],
        top_k: int
    ) -> List[EmployeeRecommendation]:
        """For QC-style work, prioritize high experience and low workload."""

        if not employees:
            return []

        # Prefer higher experience, then lower active workload.
        employees_sorted = sorted(
            employees,
            key=lambda e: (
                -(e.get("experience_years", 0) or 0),  # Negative for descending order
                e.get("active_task_count", 0),
            ),
        )

        recommendations: List[EmployeeRecommendation] = []
        for emp in employees_sorted[:top_k]:
            skills_match = self.extract_skills_match(emp)
            exp = emp.get("experience_years", 0) or 0
            active = emp.get("active_task_count", 0)
            reasoning = f"QC priority: higher experience ({exp}y) and low workload ({active} active tasks)"

            recommendations.append(
                EmployeeRecommendation(
                    employee_code=emp.get("employee_code") or emp.get("kekaemployeenumber"),
                    employee_name=emp.get("employee_name"),
                    similarity_score=0.35,
                    match_percentage=35,
                    current_role=emp.get("current_role", "Not specified"),
                    shift=emp.get("shift", "Day"),
                    experience_years=exp,
                    availability="ACTIVE" if emp.get("status_1") == "Permanent" else "INACTIVE",
                    reasoning=reasoning,
                    skills_match=skills_match,
                    task_relevance="Quality control and review",
                    current_tasks=emp.get("current_tasks", []),
                    active_task_count=emp.get("active_task_count", 0),
                    total_task_count=emp.get("total_task_count", 0)
                )
            )

        return recommendations

    def get_fallback_assignment(
        self,
        team_lead_code: Optional[str] = None,
        task_description: Optional[str] = None,
        file_id: Optional[str] = None,
        current_file_stage: Optional[str] = None
    ) -> Optional[EmployeeRecommendation]:
        """
        Get fallback assignment for tasks with no skill mapping.
        Assigns to employee with least active tasks (or no tasks).
        """
        try:
            # Load all employees
            employees = self.load_employees(team_lead_code)
            
            if not employees:
                return None
            
            # Use enhanced stage detection with context
            detected_stage = StageAssignmentService.detect_stage_from_description_with_context(
                task_description or "", file_id, current_file_stage
            )

            # Handle case where file is COMPLETED
            if detected_stage is None:
                # Check if this is a QC task for a completed file
                description_lower = task_description.lower()
                qc_keywords = ["qc", "quality check", "quality control", "review", "inspection", 
                              "verify", "audit", "check quality", "quality review", "quality assurance"]
                
                if any(keyword in description_lower for keyword in qc_keywords):
                    logger.info(f"File {file_id} is COMPLETED but QC task requested - allowing QC assignment")
                    # For QC work, prefer high experience
                    employees.sort(key=lambda x: (-(x.get("experience_years", 0) or 0), x.get("active_task_count", 0)))
                    reasoning = "File is COMPLETED - Assigned QC task to most experienced employee."
                else:
                    logger.info(f"File {file_id} is in COMPLETED stage - no recommendations until moved to QC")
                    return None

            # For PRELIMS-like work, prefer low experience (new joinees) first.
            if detected_stage == FileStage.PRELIMS:
                employees.sort(key=lambda x: (x.get("experience_years", 0) or 0, x.get("active_task_count", 0)))
                reasoning = "Fallback assignment: No skill match found. Assigned to least experienced employee for PRELIMS work."
            elif detected_stage == FileStage.QC:
                # For QC work, prefer high experience first when no skills match
                employees.sort(key=lambda x: (-(x.get("experience_years", 0) or 0), x.get("active_task_count", 0)))
                reasoning = "Fallback assignment: No skill match found. Assigned to most experienced employee for QC review."
            else:
                # Otherwise, least busy first.
                employees.sort(key=lambda x: (x.get("active_task_count", 0), x.get("experience_years", 0) or 0))
                reasoning = "Fallback assignment: No skill match found. Assigned to least busy employee."
            
            # Get the least busy employee
            least_busy_emp = employees[0]
            
            # Create fallback recommendation
            rec = EmployeeRecommendation(
                employee_code=least_busy_emp.get("employee_code"),
                employee_name=least_busy_emp.get("employee_name"),
                similarity_score=0.1,  # Low score for fallback
                match_percentage=10,
                current_role=least_busy_emp.get("current_role", "Not specified"),
                shift=least_busy_emp.get("shift", "Day"),
                experience_years=least_busy_emp.get("experience_years", 0),
                availability="ACTIVE" if least_busy_emp.get("status_1") == "Permanent" else "INACTIVE",
                reasoning=reasoning,
                skills_match={},
                task_relevance="General task assignment",
                current_tasks=least_busy_emp.get("current_tasks", []),
                active_task_count=least_busy_emp.get("active_task_count", 0),
                total_task_count=least_busy_emp.get("total_task_count", 0)
            )
            
            return rec
            
        except Exception as e:
            logger.error(f"Error in fallback assignment: {str(e)}")
            return None
    
    def build_reasoning(self, task: str, employee: dict, similarity: float) -> str:
        """Build explanation for recommendation"""
        
        reasons = []
        
        # Skills-based reasoning using normalized skills
        normalized_skills = self.skill_normalizer.normalize_employee_skills(employee)
        
        # Check for structural design skills
        if normalized_skills.get("structural_design"):
            has_structural = True
            structural_list = normalized_skills["structural_design"][:3]
            reasons.append(f"Strong in structural design ({', '.join(structural_list)})")
        
        # Check for electrical design skills
        if normalized_skills.get("electrical_design"):
            electrical_list = normalized_skills["electrical_design"][:3]
            reasons.append(f"Experienced in electrical design ({', '.join(electrical_list)})")
        
        # Check for coordination skills
        if normalized_skills.get("coordination"):
            reasons.append(f"Good coordination skills")
        
        # Experience-based reasoning
        exp = employee.get("experience_years", 0)
        if exp >= 5:
            reasons.append(f"Senior professional with {exp} years experience")
        elif exp >= 2:
            reasons.append(f"Experienced professional with {exp} years")
        
        # Task history reasoning
        task_history = employee.get("List of task assigned", "")
        if task_history and "design" in task_history.lower():
            reasons.append("Previous design experience")
        
        # Raw skills reasoning
        raw_skills = employee.get("raw_technical_skills", "")
        if raw_skills and "structural" in raw_skills.lower():
            reasons.append("Structural engineering background")
        if raw_skills and "design" in raw_skills.lower():
            reasons.append("Design experience")
        
        # If no specific reasons found, provide generic
        if not reasons:
            reasons.append("Qualified professional")
        
        return " | ".join(reasons)
    
    def extract_skills_match(self, employee: dict) -> Dict[str, List[str]]:
        """Extract skills in the expected format for frontend using skill normalizer"""
        
        # Use skill normalizer to properly extract and categorize skills
        normalized_skills = self.skill_normalizer.normalize_employee_skills(employee)
        
        skills_match = {}
        
        # Convert normalized skills to expected format
        for category, skills in normalized_skills.items():
            if skills:  # Only include categories that have skills
                skills_match[category] = skills
        
        # Fallback: Try old structure if no normalized skills found
        if not skills_match:
            old_skills = employee.get("skills", {})
            if old_skills and isinstance(old_skills, dict):
                for key, value in old_skills.items():
                    if isinstance(value, list):
                        skills_match[key] = value
                    else:
                        skills_match[key] = [str(value)]
        
        # Final fallback: Basic raw skills extraction
        if not skills_match:
            raw_skills = employee.get("raw_technical_skills", "")
            if raw_skills:
                raw_lower = raw_skills.lower()
                if "structural" in raw_lower:
                    skills_match["structural_design"] = ["Structural Engineering"]
                if "electrical" in raw_lower:
                    skills_match["electrical_design"] = ["Electrical Engineering"]
                if "coordination" in raw_lower:
                    skills_match["coordination"] = ["Coordination"]
        
        return skills_match
    
    def extract_task_relevance(self, employee: dict, task: str) -> str:
        """Extract relevant task experience from employee history"""
        
        task_lower = task.lower()
        task_history = employee.get("List of task assigned", "").lower()
        special_tasks = employee.get("Special Task", "").lower()
        
        relevant_tasks = []
        
        # Check for relevant keywords
        keywords = {
            "design": ["design", "designing", "designed"],
            "analysis": ["analysis", "analyzing", "analyzed"],
            "solar": ["solar", "pv", "photovoltaic"],
            "structural": ["structural", "structure"],
            "electrical": ["electrical", "electrics"],
            "autocad": ["autocad", "cad"],
            "preparation": ["preparation", "prepared"]
        }
        
        for key, variants in keywords.items():
            if any(v in task_lower for v in variants):
                if any(v in task_history for v in variants):
                    relevant_tasks.append(f"Previous {key} work")
                if any(v in special_tasks for v in variants):
                    relevant_tasks.append(f"Specialized in {key}")
        
        return " | ".join(relevant_tasks[:2]) if relevant_tasks else "New task type"
    
    def _extract_task_keywords(self, task_description: str) -> set:
        """Extract relevant keywords from task description"""
        task_lower = task_description.lower()
        
        keywords = set()
        
        # Structural design keywords
        structural_keywords = ["structural", "structure", "steel", "building", "rafter", 
                              "foundation", "column", "truss", "roof", "concrete", 
                              "beam", "load", "cad", "drawing"]
        
        # Electrical design keywords
        electrical_keywords = ["electrical", "pv", "solar", "photovoltaic", "inverter",
                              "string", "earthing", "cable", "switchgear", "panel"]
        
        # Coordination keywords
        coordination_keywords = ["coordination", "coordinate", "coordinating"]
        
        for kw in structural_keywords:
            if kw in task_lower:
                keywords.add("structural")
                keywords.add(kw)
        
        for kw in electrical_keywords:
            if kw in task_lower:
                keywords.add("electrical")
                keywords.add(kw)
        
        for kw in coordination_keywords:
            if kw in task_lower:
                keywords.add("coordination")
                keywords.add(kw)
        
        # Add generic design keyword
        if "design" in task_lower:
            keywords.add("design")
        
        return keywords
    
    def _calculate_keyword_score(self, employee: dict, task_keywords: set, task_description: str) -> float:
        """Calculate skill match score based on keyword matching"""
        
        if not task_keywords:
            return 0.0
        
        # Prefer normalized skills (from raw_technical_skills/raw_strength_expertise).
        skills = self.skill_normalizer.normalize_employee_skills(employee) or {}
        task_lower = task_description.lower()
        
        score = 0.0
        matches = 0
        
        # Check structural design skills
        if "structural" in task_keywords or "design" in task_keywords:
            structural_skills = skills.get("structural_design", [])
            if structural_skills:
                # Check for specific skill matches
                for skill in structural_skills:
                    skill_lower = skill.lower()
                    if any(kw in skill_lower for kw in task_keywords):
                        matches += 1
                        score += 0.15
                    elif "structural" in task_lower and "structural" in skill_lower:
                        matches += 1
                        score += 0.2
                    elif "design" in task_lower and "design" in skill_lower:
                        matches += 1
                        score += 0.1
        
        # Check electrical design skills
        if "electrical" in task_keywords or "solar" in task_keywords or "pv" in task_keywords:
            electrical_skills = skills.get("electrical_design", [])
            if electrical_skills:
                for skill in electrical_skills:
                    skill_lower = skill.lower()
                    if any(kw in skill_lower for kw in task_keywords):
                        matches += 1
                        score += 0.15
                    elif "electrical" in task_lower and "electrical" in skill_lower:
                        matches += 1
                        score += 0.2
        
        # Check coordination skills
        if "coordination" in task_keywords:
            coordination_skills = skills.get("coordination", [])
            if coordination_skills:
                for skill in coordination_skills:
                    skill_lower = skill.lower()
                    if "coordination" in skill_lower:
                        matches += 1
                        score += 0.15
        
        # Cap the score at 0.9 and ensure minimum if there are matches
        if matches > 0:
            score = min(score, 0.9)
            score = max(score, 0.35)  # Minimum score for any match
        
        return score

# Singleton instance
_engine_instance = None

def get_recommendation_engine() -> RecommendationEngine:
    """Get recommendation engine instance"""
    global _engine_instance
    if _engine_instance is None:
        _engine_instance = RecommendationEngine()
    return _engine_instance
