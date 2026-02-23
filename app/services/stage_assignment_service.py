"""
Stage Assignment Service
Handles keyword detection and stage-based employee assignment with experience priority
"""

from typing import Dict, List, Any, Optional, Tuple
import re
from datetime import datetime
import logging
from app.db.mongodb import get_db
from app.models.stage_flow import FileStage, STAGE_CONFIGS
from app.models.file_stage_tracking import FILE_TRACKING_COLLECTION

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class StageAssignmentService:
    """Service for intelligent stage-based task assignment"""
    
    # Keywords for each stage
    STAGE_KEYWORDS = {
        FileStage.PRELIMS: [
            "arora", "sales proposal", "salesproposal", "sales", "proposal", 
            "cad", "layout", "preliminary", "prelim", "initial", "basic",
            "draft", "sketch", "plan", "design review", "concept"
        ],
        FileStage.PRODUCTION: [
            "structural design", "structural analysis", "structural calculation",
            "load calculation", "structural drawing", "cad design", "autocad",
            "roof design", "building design", "foundation design", "beam design",
            "truss design", "rafter design", "column design",
            "electrical design", "electrical calculation", "solar design",
            "pv design", "photovoltaic design", "inverter design", "string design",
            "electrical drawing", "single line diagram", "three line diagram",
            "conduit sizing", "wire sizing", "voltage drop calculation",
            "structural", "structure", "steel", "building", "rafter", 
            "foundation", "column", "truss", "roof", "concrete", 
            "beam", "load", "cad", "drawing",
            "electrical", "pv", "solar", "photovoltaic", "inverter",
            "string", "earthing", "cable", "switchgear", "panel"
        ],
        FileStage.QC: [
            "quality", "analytics", "analysis", "review", "inspection",
            "testing", "audit", "check", "verification", "validation",
            "quality control", "quality assurance", "qa", "qc",
            # Include all PRODUCTION keywords for QC stage (when files are COMPLETED)
            "structural design", "structural analysis", "structural calculation",
            "load calculation", "structural drawing", "cad design", "autocad",
            "roof design", "building design", "foundation design", "beam design",
            "truss design", "rafter design", "column design",
            "electrical design", "electrical calculation", "solar design",
            "pv design", "photovoltaic design", "inverter design", "string design",
            "electrical drawing", "single line diagram", "three line diagram",
            "conduit sizing", "wire sizing", "voltage drop calculation",
            "structural", "structure", "steel", "building", "rafter", 
            "foundation", "column", "truss", "roof", "concrete", 
            "beam", "load", "cad", "drawing",
            "electrical", "pv", "solar", "photovoltaic", "inverter",
            "string", "earthing", "cable", "switchgear", "panel"
        ]
    }
    
    # Technical skills database for enhanced categorization
    TECHNICAL_SKILLS_DB = {
        'structural_design': [
            'structural design', 'structural analysis', 'structural calculation',
            'load calculation', 'structural drawing', 'cad design', 'autocad',
            'roof design', 'building design', 'foundation design', 'beam design',
            'truss design', 'rafter design', 'column design'
        ],
        'electrical_design': [
            'electrical design', 'electrical calculation', 'solar design',
            'pv design', 'photovoltaic design', 'inverter design', 'string design',
            'electrical drawing', 'single line diagram', 'three line diagram',
            'conduit sizing', 'wire sizing', 'voltage drop calculation'
        ],
        'coordination': [
            'coordination', 'project coordination', 'project management',
            'team management', 'team leadership', 'leadership', 'management'
        ]
    }
    
    @staticmethod
    def detect_stage_from_description(task_description: str) -> Optional[FileStage]:
        """
        Detect the appropriate stage based on task description keywords
        Enhanced with technical skills database for better accuracy
        """
        if not task_description:
            return None
            
        description_lower = task_description.lower()
        
        # Count keyword matches for each stage
        stage_scores = {}
        
        # First, check stage keywords with improved matching logic
        for stage, keywords in StageAssignmentService.STAGE_KEYWORDS.items():
            score = 0
            # Sort keywords by length (longer first) to prioritize specific matches
            sorted_keywords = sorted(keywords, key=len, reverse=True)
            
            for keyword in sorted_keywords:
                # Count occurrences of each keyword
                matches = len(re.findall(rf'\b{re.escape(keyword)}\b', description_lower))
                if matches > 0:
                    # Give higher weight to longer, more specific keywords
                    weight = len(keyword.split())  # Multi-word keywords get higher weight
                    score += matches * weight
            if score > 0:
                stage_scores[stage] = score
        
        # Then, enhance with technical skills database
        technical_scores = StageAssignmentService._get_technical_skill_scores(description_lower)
        
        # Combine scores with weighted logic
        # Technical skills have higher weight for production
        if 'structural_design' in technical_scores or 'electrical_design' in technical_scores:
            production_boost = sum(technical_scores.get(skill, 0) for skill in ['structural_design', 'electrical_design'])
            stage_scores[FileStage.PRODUCTION] = stage_scores.get(FileStage.PRODUCTION, 0) + (production_boost * 2)
        
        # Coordination skills can apply to any stage but give slight boost to PRELIMS
        if 'coordination' in technical_scores:
            coordination_score = technical_scores['coordination']
            stage_scores[FileStage.PRELIMS] = stage_scores.get(FileStage.PRELIMS, 0) + coordination_score
        
        # Special QC logic: Only prioritize QC when it has explicit QC keywords
        qc_score = stage_scores.get(FileStage.QC, 0)
        production_score = stage_scores.get(FileStage.PRODUCTION, 0)
        prelims_score = stage_scores.get(FileStage.PRELIMS, 0)
        
        # Check for explicit QC-specific keywords (not shared with production)
        explicit_qc_keywords = ["quality", "analytics", "analysis", "review", "inspection",
                               "testing", "audit", "check", "verification", "validation",
                               "quality control", "quality assurance", "qa", "qc"]
        
        explicit_qc_matches = sum(1 for keyword in explicit_qc_keywords if keyword in description_lower)
        
        if explicit_qc_matches > 0:
            # Has explicit QC keywords - give QC priority
            stage_scores[FileStage.QC] = qc_score + 5  # Strong boost for explicit QC keywords
        elif qc_score > 0 and production_score > 0:
            # QC score comes from shared production keywords, prefer PRODUCTION
            stage_scores[FileStage.PRODUCTION] = production_score + 3  # Boost PRODUCTION over QC
            stage_scores[FileStage.QC] = qc_score  # Remove any QC advantage
        
        # Return stage with highest score
        if stage_scores:
            best_stage = max(stage_scores, key=stage_scores.get)
            logger.info(f"Detected stage {best_stage} from description with score {stage_scores[best_stage]}")
            logger.debug(f"Stage scores: {stage_scores}")
            logger.debug(f"Technical scores: {technical_scores}")
            return best_stage
            
        return None

    @staticmethod
    def detect_stage_from_description_with_context(task_description: str, file_id: str = None, current_file_stage: str = None) -> Optional[FileStage]:
        """
        Enhanced stage detection that considers file context
        - For COMPLETED files: QC keywords get priority, production keywords indicate QC review
        - For other files: Normal keyword detection
        """
        from app.db.mongodb import get_db
        
        # First, get basic stage detection
        detected_stage = StageAssignmentService.detect_stage_from_description(task_description)
        
        # Check if file is in COMPLETED stage (either from parameter or database)
        file_is_completed = False
        
        if current_file_stage == 'COMPLETED':
            file_is_completed = True
        elif file_id:
            # Check database if stage not provided
            db = get_db()
            file_tracking = db.file_tracking.find_one({'file_id': file_id})
            if file_tracking and file_tracking.get('current_stage') == 'COMPLETED':
                file_is_completed = True
        
        # If file is COMPLETED, do not auto-assign to QC stage
        # Manager must manually move file to QC stage first
        if file_is_completed:
            logger.info(f"File is COMPLETED. Stage detection disabled - manager must move file to QC stage before assigning QC tasks")
            # Return None to indicate no stage should be auto-assigned
            # The task assignment logic will handle this appropriately
            return None
        
        return detected_stage
    
    @staticmethod
    def _get_technical_skill_scores(description_lower: str) -> Dict[str, int]:
        """
        Get technical skill scores from description using TECHNICAL_SKILLS_DB
        """
        skill_scores = {}
        
        for skill_category, skills in StageAssignmentService.TECHNICAL_SKILLS_DB.items():
            score = 0
            for skill in skills:
                # Count occurrences of each skill
                matches = len(re.findall(rf'\b{re.escape(skill)}\b', description_lower))
                score += matches
            if score > 0:
                skill_scores[skill_category] = score
        
        return skill_scores
    
    @staticmethod
    def get_employees_by_experience(
        stage: FileStage, 
        team_lead_id: Optional[str] = None,
        prioritize_new_joinees: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get employees sorted by experience (new joinees first if specified)
        """
        db = get_db()
        
        # Build query
        query = {"status_1": "Permanent"}
        if team_lead_id:
            query["$or"] = [
                {"reporting_manager": team_lead_id},
                {"reporting_manager.employee_code": team_lead_id}
            ]
        
        # Get employees with experience
        employees = list(db.employee.find(query, {
            "_id": 0,
            "employee_code": 1,
            "employee_name": 1,
            "experience_years": 1,
            "current_tasks": 1,
            "skills": 1,
            "reporting_manager": 1
        }))
        
        # Sort by experience
        if prioritize_new_joinees:
            # New joinees first (lower experience), then by current tasks
            employees.sort(key=lambda x: (x.get("experience_years", 0), x.get("current_tasks", 0)))
        else:
            # Higher experience first, then by current tasks
            employees.sort(key=lambda x: (-x.get("experience_years", 0), x.get("current_tasks", 0)))
        
        return employees
    
    @staticmethod
    def check_stage_transition_validity(
        file_id: str, 
        requested_stage: FileStage
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if a file can transition to the requested stage
        Returns (is_valid, error_message)
        """
        db = get_db()
        
        # Get current file stage
        file_tracking = db[FILE_TRACKING_COLLECTION].find_one({"file_id": file_id})
        if not file_tracking:
            # New file - can only start with PRELIMS
            if requested_stage == FileStage.PRELIMS:
                return True, None
            else:
                return False, f"New file must start with PRELIMS stage, not {requested_stage.value}"

        current_stage_raw = file_tracking.get("current_stage")
        try:
            current_stage = FileStage(current_stage_raw) if current_stage_raw else None
        except Exception:
            current_stage = None
        
        # Check stage flow rules
        if requested_stage == FileStage.PRODUCTION:
            # Allow PRODUCTION tasks if file is in PRELIMS (progressing) OR already in PRODUCTION
            if current_stage not in [FileStage.PRELIMS, FileStage.PRODUCTION]:
                return False, f"File must be in PRELIMS or PRODUCTION stage to create PRODUCTION tasks. Current stage: {current_stage.value if current_stage else 'None'}"
                
        elif requested_stage == FileStage.QC:
            if current_stage != FileStage.QC and current_stage != FileStage.COMPLETED:
                return False, f"File must be in QC or COMPLETED stage to create QC tasks. Current stage: {current_stage.value if current_stage else 'None'}"
                
        elif requested_stage == FileStage.DELIVERED:
            if current_stage != FileStage.QC:
                return False, f"File must complete QUALITY stage before being delievered. Current stage: {current_stage.value if current_stage else 'None'}"
        
        # Also check if trying to create tasks for earlier stages
        elif requested_stage == FileStage.PRELIMS:
            if current_stage and current_stage != FileStage.PRELIMS:
                return False, f"File has already progressed past PRELIMS stage (currently in {current_stage.value}). Cannot create PRELIMS tasks."
            # Even if still at PRELIMS stage, block if a PRELIMS task already exists for this file
            existing_prelims = db.tasks.find_one({
                "$or": [{"file_id": file_id}, {"source.permit_file_id": file_id}],
                "stage": "PRELIMS"
            })
            if existing_prelims:
                return False, f"File {file_id} already has a PRELIMS task ({existing_prelims.get('task_id')}). Cannot create duplicate PRELIMS tasks."
        
        return True, None
    
    @staticmethod
    def get_best_employee_for_stage(
        stage: FileStage,
        file_id: Optional[str] = None,
        task_description: Optional[str] = None,
        team_lead_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get the best employee for a given stage considering:
        1. Stage requirements and keyword matching
        2. Experience level (new joinees for PRELIMS)
        3. Current workload
        4. Skills matching (if available)
        """
        db = get_db()
        
        # Determine if we should prioritize new joinees
        prioritize_new_joinees = (stage == FileStage.PRELIMS)
        
        # Get eligible employees
        employees = StageAssignmentService.get_employees_by_experience(
            stage, team_lead_id, prioritize_new_joinees
        )
        
        if not employees:
            raise ValueError(f"No employees available for {stage.value} stage")
        
        # Filter employees with less than 5 current tasks
        eligible_employees = [
            emp for emp in employees 
            if emp.get("current_tasks", 0) < 5
        ]
        
        if not eligible_employees:
            # If no one has <5 tasks, take the one with least tasks
            eligible_employees = [min(employees, key=lambda x: x.get("current_tasks", 0))]
        
        # For PRELIMS, prioritize new joinees even if they don't have the skills
        if stage == FileStage.PRELIMS:
            selected = eligible_employees[0]  # Already sorted by experience (new joinees first)
        else:
            # For other stages, try to match skills if task description is provided
            if task_description:
                selected = StageAssignmentService._find_best_skill_match(
                    eligible_employees, task_description, stage
                )
            else:
                selected = eligible_employees[0]
        
        logger.info(f"Selected employee {selected['employee_code']} for {stage.value} stage")
        
        return {
            "employee_code": selected["employee_code"],
            "employee_name": selected["employee_name"],
            "experience_years": selected.get("experience_years", 0),
            "current_tasks": selected.get("current_tasks", 0),
            "selection_reason": StageAssignmentService._get_selection_reason(selected, stage)
        }
    
    @staticmethod
    def _find_best_skill_match(
        employees: List[Dict[str, Any]], 
        task_description: str, 
        stage: FileStage
    ) -> Dict[str, Any]:
        """Find employee with best skill match for task using enhanced technical skills database"""
        stage_keywords = StageAssignmentService.STAGE_KEYWORDS.get(stage, [])
        
        # Get technical skill scores from task description
        task_desc_lower = task_description.lower() if task_description else ""
        task_technical_scores = StageAssignmentService._get_technical_skill_scores(task_desc_lower)
        
        best_employee = employees[0]
        best_score = 0
        
        for employee in employees:
            employee_skills = employee.get("skills", [])
            if not employee_skills:
                continue
            
            # Calculate skill match score
            score = 0
            employee_skills_lower = [skill.lower() for skill in employee_skills]
            
            # Match stage keywords
            for keyword in stage_keywords:
                if keyword in employee_skills_lower:
                    score += 1
            
            # Enhanced matching with technical skills database
            for skill_category, skills in StageAssignmentService.TECHNICAL_SKILLS_DB.items():
                # Check if employee has skills in this category
                employee_has_category = any(
                    any(emp_skill in skill.lower() for skill in skills)
                    for emp_skill in employee_skills_lower
                )
                
                # Check if task needs this category
                task_needs_category = skill_category in task_technical_scores
                
                if employee_has_category and task_needs_category:
                    score += task_technical_scores[skill_category] * 2  # Weighted score
            
            # Also check for direct skill matches in task description
            if task_description:
                for skill in employee_skills_lower:
                    if skill in task_desc_lower:
                        score += 3  # Extra weight for direct skill matches
            
            if score > best_score:
                best_score = score
                best_employee = employee
        
        return best_employee
    
    @staticmethod
    def _get_selection_reason(employee: Dict[str, Any], stage: FileStage) -> str:
        """Generate reason for employee selection"""
        experience = employee.get("experience_years", 0)
        current_tasks = employee.get("current_tasks", 0)
        
        if stage == FileStage.PRELIMS:
            if experience < 2:
                return f"New joinee ({experience}y exp) with low workload ({current_tasks} tasks)"
            else:
                return f"Available employee ({experience}y exp, {current_tasks} tasks)"
        else:
            if experience >= 5:
                return f"Senior employee ({experience}y exp) with low workload ({current_tasks} tasks)"
            elif experience >= 2:
                return f"Experienced employee ({experience}y exp) with low workload ({current_tasks} tasks)"
            else:
                return f"Available employee ({experience}y exp, {current_tasks} tasks)"
    
    @staticmethod
    def auto_move_to_production(file_id: str) -> bool:
        """
        Automatically move file to PRODUCTION stage after PRELIMS is done
        """
        db = get_db()
        
        # Check if file is in PRELIMS stage
        file_tracking = db.file_stage_tracking.find_one({"file_id": file_id})
        if not file_tracking:
            logger.warning(f"No tracking found for file {file_id}")
            return False
            
        if file_tracking.get("current_stage") != FileStage.PRELIMS:
            logger.info(f"File {file_id} is not in PRELIMS stage, current: {file_tracking.get('current_stage')}")
            return False
        
        # Check if all prelims tasks are completed
        tasks_collection = db.tasks
        active_prelims_tasks = tasks_collection.count_documents({
            "source.permit_file_id": file_id,
            "status": {"$ne": "COMPLETED"},
            "stage": FileStage.PRELIMS
        })
        
        logger.info(f"File {file_id} has {active_prelims_tasks} active PRELIMS tasks")
        
        if active_prelims_tasks == 0:
            # Move to PRODUCTION
            db.file_stage_tracking.update_one(
                {"file_id": file_id},
                {
                    "$set": {
                        "current_stage": FileStage.PRODUCTION,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            # Create stage history entry
            db.stage_history.insert_one({
                "file_id": file_id,
                "stage": FileStage.PRODUCTION,
                "started_at": datetime.utcnow(),
                "completed_at": None,
                "duration_minutes": 0,
                "employee_code": "automation",
                "employee_name": "System Automation",
                "notes": "Auto-moved from PRELIMS to PRODUCTION"
            })
            
            logger.info(f"Auto-moved file {file_id} from PRELIMS to PRODUCTION")
            return True
        
        return False
    
    @staticmethod
    def auto_move_to_qc(file_id: str) -> bool:
        """
        Automatically move file to QC stage after PRODUCTION is completed
        """
        db = get_db()
        
        # Check if file is in COMPLETED stage
        file_tracking = db.file_stage_tracking.find_one({"file_id": file_id})
        if not file_tracking or file_tracking.get("current_stage") != FileStage.COMPLETED:
            return False
        
        # Move to QC
        db.file_stage_tracking.update_one(
            {"file_id": file_id},
            {
                "$set": {
                    "current_stage": FileStage.QC,
                    "updated_at": datetime.utcnow()
                }
            }
        )
        
        # Create stage history entry
        db.stage_history.insert_one({
            "file_id": file_id,
            "stage": FileStage.QC,
            "started_at": datetime.utcnow(),
            "completed_at": None,
            "duration_minutes": 0,
            "employee_code": "automation",
            "employee_name": "System Automation",
            "notes": "Auto-moved from COMPLETED to QC"
        })
        
        logger.info(f"Auto-moved file {file_id} from COMPLETED to QC")
        return True
    
    @staticmethod
    def auto_move_to_delivered(file_id: str) -> bool:
        """
        Automatically move file to DELIVERED stage after QC is done
        """
        db = get_db()
        
        # Check if file is in QC stage
        file_tracking = db.file_stage_tracking.find_one({"file_id": file_id})
        if not file_tracking or file_tracking.get("current_stage") != FileStage.QC:
            return False
        
        # Check if all QC tasks are completed
        tasks_collection = db.tasks
        active_qc_tasks = tasks_collection.count_documents({
            "source.permit_file_id": file_id,
            "status": {"$ne": "COMPLETED"},
            "stage": FileStage.QC
        })
        
        if active_qc_tasks == 0:
            # Move to DELIVERED
            db.file_stage_tracking.update_one(
                {"file_id": file_id},
                {
                    "$set": {
                        "current_stage": FileStage.DELIVERED,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            # Create stage history entry
            db.stage_history.insert_one({
                "file_id": file_id,
                "stage": FileStage.DELIVERED,
                "started_at": datetime.utcnow(),
                "completed_at": datetime.utcnow(),
                "duration_minutes": 0,
                "employee_code": "automation",
                "employee_name": "System Automation",
                "notes": "Auto-moved from QC to DELIVERED"
            })
            
            logger.info(f"Auto-moved file {file_id} from QC to DELIVERED")
            return True
        
        return False
    
    @staticmethod
    def auto_move_to_completed(file_id: str) -> bool:
        """
        Automatically move file to COMPLETED stage after PRODUCTION is done
        """
        db = get_db()
        
        # Check if file is in PRODUCTION stage
        file_tracking = db.file_stage_tracking.find_one({"file_id": file_id})
        if not file_tracking or file_tracking.get("current_stage") != FileStage.PRODUCTION:
            return False
        
        # Check if all production tasks are completed
        tasks_collection = db.tasks
        active_production_tasks = tasks_collection.count_documents({
            "source.permit_file_id": file_id,
            "status": {"$ne": "COMPLETED"},
            "stage": FileStage.PRODUCTION
        })
        
        if active_production_tasks == 0:
            # Move to COMPLETED
            db.file_stage_tracking.update_one(
                {"file_id": file_id},
                {
                    "$set": {
                        "current_stage": FileStage.COMPLETED,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            # Create stage history entry
            db.stage_history.insert_one({
                "file_id": file_id,
                "stage": FileStage.COMPLETED,
                "started_at": datetime.utcnow(),
                "completed_at": datetime.utcnow(),
                "duration_minutes": 0,
                "employee_code": "automation",
                "employee_name": "System Automation",
                "notes": "Auto-moved from PRODUCTION to COMPLETED"
            })
            
            logger.info(f"Auto-moved file {file_id} from PRODUCTION to COMPLETED")
            return True
        
        return False
