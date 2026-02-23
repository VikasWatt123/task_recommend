"""
Stage tracking service for file workflow management
"""
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
from bson import ObjectId
import logging

from app.db.mongodb import get_db
from app.services.notification_service import get_notification_service
from app.services.clickhouse_service import clickhouse_service
from app.services.clickhouse_lifecycle_service import clickhouse_lifecycle_service
from app.models.file_stage_tracking import (
    FileTracking, FileStageHistory, StageAssignment,
    FILE_TRACKING_COLLECTION, STAGE_HISTORY_COLLECTION,
    create_file_tracking, get_employee_workload_summary,
    assign_employee_to_stage, complete_current_stage, transition_to_next_stage
)
from app.models.stage_flow import FileStage, calculate_sla_status, calculate_penalty, get_stage_config
from app.services.cache_service import cached, get_cache

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def convert_objectid_to_str(obj):
    """Convert ObjectId to string recursively"""
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, dict):
        return {key: convert_objectid_to_str(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectid_to_str(item) for item in obj]
    else:
        return obj


def _parse_file_stage_history_safely(stage_doc: Dict) -> Optional[FileStageHistory]:
    """Safely parse FileStageHistory from raw dict, handling legacy data"""
    try:
        # Create a copy to avoid modifying original
        doc_copy = stage_doc.copy()
        
        # Ensure entered_stage_at exists
        if "entered_stage_at" not in doc_copy:
            if "created_at" in doc_copy:
                doc_copy["entered_stage_at"] = doc_copy["created_at"]
            else:
                doc_copy["entered_stage_at"] = datetime.utcnow()
        
        # Fix assigned_to fields
        if "assigned_to" in doc_copy and isinstance(doc_copy["assigned_to"], dict):
            assigned = doc_copy["assigned_to"]
            if "assigned_at" not in assigned:
                assigned["assigned_at"] = assigned.get("started_at") or datetime.utcnow()
        
        # Try to parse with Pydantic
        return FileStageHistory(**doc_copy)
    except Exception as e:
        logger.warning(f"Failed to parse FileStageHistory for {stage_doc.get('file_id', 'unknown')}: {e}")
        return None


def _parse_file_tracking_safely(file_doc: Dict) -> Optional[FileTracking]:
    """Safely parse FileTracking from raw dict, handling legacy data"""
    try:
        # Create a copy to avoid modifying original
        doc_copy = file_doc.copy()
        
        # Handle stage_history - fix missing fields
        if "stage_history" in doc_copy and isinstance(doc_copy["stage_history"], list):
            for i, stage in enumerate(doc_copy["stage_history"]):
                # Ensure entered_stage_at exists
                if "entered_stage_at" not in stage and "created_at" in stage:
                    stage["entered_stage_at"] = stage["created_at"]
                elif "entered_stage_at" not in stage:
                    stage["entered_stage_at"] = datetime.utcnow()
                
                # Fix assigned_to fields
                if "assigned_to" in stage and isinstance(stage["assigned_to"], dict):
                    assigned = stage["assigned_to"]
                    if "assigned_at" not in assigned:
                        assigned["assigned_at"] = assigned.get("started_at") or datetime.utcnow()
        
        # Try to parse with Pydantic
        return FileTracking(**doc_copy)
    except Exception as e:
        # If still fails, try to create a minimal valid FileTracking
        try:
            # Extract essential fields with defaults
            file_id = file_doc.get("file_id", f"unknown-{ObjectId()}")
            current_stage = file_doc.get("current_stage", "PRELIMS")
            
            # Create minimal tracking
            minimal_tracking = FileTracking(
                file_id=file_id,
                current_stage=current_stage,
                current_status=file_doc.get("current_status", "IN_PROGRESS"),
                created_at=file_doc.get("created_at", datetime.utcnow()),
                updated_at=file_doc.get("updated_at", datetime.utcnow())
            )
            logger.info(f"Created minimal tracking for {file_id} from legacy data")
            return minimal_tracking
        except Exception as e2:
            logger.warning(f"Failed to create minimal tracking for {file_doc.get('file_id', 'unknown')}: {e2}")
            return None


class StageTrackingService:
    """Service for managing file stage transitions and tracking"""
    
    def __init__(self):
        self.db = get_db()
        self.notification_service = get_notification_service()
        self._ensure_indexes()
    
    def _ensure_indexes(self):
        """Ensure database indexes exist"""
        from app.models.file_stage_tracking import get_indexes
        indexes = get_indexes()
        
        for collection, index_list in indexes.items():
            for index in index_list:
                self.db[collection].create_index(index)
    
    def _batch_fetch_employees(self, employee_codes: List[str]) -> Dict[str, Dict]:
        """Batch fetch employee data to avoid N+1 queries"""
        if not employee_codes:
            return {}
        
        # Remove duplicates and None values
        unique_codes = list(set(code for code in employee_codes if code))
        
        if not unique_codes:
            return {}
        
        # Single query to fetch all employees
        employees = list(self.db.employee.find(
            {"employee_code": {"$in": unique_codes}},
            {"_id": 0, "employee_code": 1, "employee_name": 1, "current_role": 1}
        ))
        
        # Create lookup map
        return {emp["employee_code"]: emp for emp in employees}
    
    def initialize_file_tracking(self, file_id: str, initial_stage: FileStage = FileStage.PRELIMS) -> FileTracking:
        """Initialize tracking for a new file"""
        # Check if tracking already exists
        existing = self.db[FILE_TRACKING_COLLECTION].find_one({"file_id": file_id})
        if existing:
            logger.warning(f"Tracking already exists for file {file_id}")
            return FileTracking(**existing)
        
        # Create new tracking
        tracking = create_file_tracking(file_id, initial_stage)
        
        # Insert into database
        self.db[FILE_TRACKING_COLLECTION].insert_one(tracking.dict())
        
        # Insert initial stage history
        if tracking.stage_history:
            self.db[STAGE_HISTORY_COLLECTION].insert_one(tracking.stage_history[0].dict())
        
        # Emit file created event to ClickHouse
        try:
            from app.services.clickhouse_lifecycle_service import clickhouse_lifecycle_service
            # Get permit file info for name
            permit_file = self.db.permit_files.find_one({"file_id": file_id})
            file_name = permit_file.get("file_name", file_id) if permit_file else file_id
            
            # Emit synchronously
            clickhouse_lifecycle_service.emit_file_lifecycle_event(
                file_id=file_id,
                event_type="FILE_CREATED",
                stage=initial_stage.value,
                employee_code=None,
                employee_name=None,
                event_data={"file_name": file_name}
            )
            logger.info(f"Emitted file_created event to ClickHouse for {file_id}")
        except Exception as e:
            logger.warning(f"Failed to emit file_created event to ClickHouse: {e}")
        
        logger.info(f"Initialized tracking for file {file_id} at stage {initial_stage}")
        return tracking

    def _get_next_stage(self, current_stage: FileStage) -> Optional[FileStage]:
        """Get the next stage in the workflow"""
        stage_flow = {
            FileStage.PRELIMS: FileStage.PRODUCTION,
            FileStage.PRODUCTION: FileStage.COMPLETED,
            FileStage.COMPLETED: FileStage.QC,  # Will be handled by manager
            FileStage.QC: FileStage.DELIVERED,
            FileStage.DELIVERED: None
        }
        return stage_flow.get(current_stage)

    def auto_progress_from_tasks(self, file_id: str) -> Optional[FileTracking]:
        tracking_doc = self.get_file_tracking(file_id)
        if not tracking_doc:
            return None

        tracking = _parse_file_tracking_safely(tracking_doc) if isinstance(tracking_doc, dict) else tracking_doc
        if not tracking:
            return None

        def _stage_value(stage: FileStage) -> str:
            return stage.value if hasattr(stage, "value") else str(stage)

        def _incomplete_count(stage: FileStage) -> int:
            stage_val = _stage_value(stage)
            return self.db.tasks.count_documents({
                "$or": [{"source.permit_file_id": file_id}, {"file_id": file_id}],
                "stage": stage_val,
                "status": {"$nin": ["COMPLETED", "DONE"]},
            })

        def _total_count(stage: FileStage) -> int:
            stage_val = _stage_value(stage)
            return self.db.tasks.count_documents({
                "$or": [{"source.permit_file_id": file_id}, {"file_id": file_id}],
                "stage": stage_val,
            })

        now = datetime.utcnow()
        current_stage = tracking.current_stage
        if isinstance(current_stage, str):
            try:
                current_stage = FileStage(current_stage)
            except Exception:
                current_stage = FileStage.PRELIMS

        if current_stage in (FileStage.PRELIMS, FileStage.PRODUCTION, FileStage.QC):
            total = _total_count(current_stage)
            remaining = _incomplete_count(current_stage)
            # Auto-progress if there are no incomplete tasks (either no tasks or all tasks completed)
            if remaining == 0 and tracking.stage_history:
                before_stage_value = current_stage.value
                assignment_employee_code = tracking.current_assignment.employee_code if tracking.current_assignment else ""
                assignment_employee_name = tracking.current_assignment.employee_name if tracking.current_assignment else ""

                # Mark current stage completed in tracking doc
                current_hist = tracking.stage_history[-1]
                current_hist.status = "COMPLETED"
                current_hist.completed_stage_at = now
                entered_at = current_hist.entered_stage_at or current_hist.created_at
                if isinstance(entered_at, datetime):
                    current_hist.total_duration_minutes = max(0, int((now - entered_at).total_seconds() / 60))

                # Update assignment completion in both current tracking and stage history
                if tracking.current_assignment:
                    tracking.current_assignment.completed_at = now
                    if tracking.current_assignment.started_at and isinstance(tracking.current_assignment.started_at, datetime):
                        tracking.current_assignment.duration_minutes = max(
                            0,
                            int((now - tracking.current_assignment.started_at).total_seconds() / 60),
                        )
                
                # Also update the assignment in the stage history
                if current_hist.assigned_to:
                    current_hist.assigned_to.completed_at = now
                    if current_hist.assigned_to.started_at and isinstance(current_hist.assigned_to.started_at, datetime):
                        current_hist.assigned_to.duration_minutes = max(
                            0,
                            int((now - current_hist.assigned_to.started_at).total_seconds() / 60),
                        )

                # Stage-specific auto transitions
                if current_stage == FileStage.PRELIMS:
                    next_stage = FileStage.PRODUCTION
                    tracking = transition_to_next_stage(tracking, next_stage)
                    tracking.current_status = "IN_PROGRESS"
                    new_stage_value = FileStage.PRODUCTION.value
                elif current_stage == FileStage.PRODUCTION:
                    # Auto move to COMPLETED state immediately
                    tracking.current_stage = FileStage.COMPLETED
                    tracking.current_assignment = None
                    tracking.current_status = "COMPLETED"
                    new_stage_value = FileStage.COMPLETED.value

                    completed_history = FileStageHistory(
                        file_id=file_id,
                        stage=FileStage.COMPLETED,
                        status="COMPLETED",
                        entered_stage_at=now,
                        completed_stage_at=now,
                        metadata={"auto_progressed": True, "from_stage": "PRODUCTION"},
                    )
                    tracking.stage_history.append(completed_history)
                elif current_stage == FileStage.QC:
                    tracking.current_stage = FileStage.DELIVERED
                    tracking.current_assignment = None
                    tracking.current_status = "DELIVERED"
                    tracking.completed_at = now
                    total_duration = now - tracking.started_at
                    tracking.total_duration_minutes = max(0, int(total_duration.total_seconds() / 60))
                    new_stage_value = FileStage.DELIVERED.value

                    delivered_history = FileStageHistory(
                        file_id=file_id,
                        stage=FileStage.DELIVERED,
                        status="COMPLETED",
                        entered_stage_at=now,
                        completed_stage_at=now,
                        metadata={"auto_progressed": True, "from_stage": "QC"},
                    )
                    tracking.stage_history.append(delivered_history)

                tracking.updated_at = now

                # Keep ClickHouse file_lifecycle in sync for real-time dashboards
                try:
                    clickhouse_service.update_file_stage(file_id, new_stage_value)
                except Exception:
                    pass
                
                # Also update permit_files collection to keep status in sync
                try:
                    # Map FileStage to permit_files status
                    stage_to_status = {
                        "PRELIMS": "IN_PRELIMS",
                        "PRODUCTION": "IN_PRODUCTION", 
                        "COMPLETED": "COMPLETED",
                        "QC": "IN_QC",
                        "DELIVERED": "DELIVERED"
                    }
                    
                    new_status = stage_to_status.get(new_stage_value, "IN_PRELIMS")
                    
                    self.db.permit_files.update_one(
                        {"file_id": file_id},
                        {
                            "$set": {
                                "current_stage": new_stage_value,
                                "status": new_status,
                                "updated_at": now
                            }
                        }
                    )
                    logger.info(f"✅ Updated permit_files for {file_id} to {new_status} (auto-progress)")
                except Exception as e:
                    logger.warning(f"Failed to update permit_files for {file_id} during auto-progress: {e}")

                # Emit stage completion/started events (best-effort)
                try:
                    clickhouse_service.emit_stage_completed_event(
                        task_id=f"FILE-{file_id}",
                        employee_code=assignment_employee_code,
                        employee_name=assignment_employee_name,
                        stage=before_stage_value,
                        duration_minutes=current_hist.total_duration_minutes or 0,
                        file_id_param=file_id,
                    )
                    clickhouse_service.emit_stage_started_event(
                        task_id=f"FILE-{file_id}",
                        employee_code="",
                        employee_name="",
                        stage=new_stage_value,
                        file_id_param=file_id,
                    )
                except Exception:
                    pass

                # Persist tracking + last stage history changes
                self.db[FILE_TRACKING_COLLECTION].update_one(
                    {"file_id": file_id},
                    {"$set": tracking.model_dump()},
                )

                # Upsert current stage history record and insert any new stage history rows
                try:
                    self.db[STAGE_HISTORY_COLLECTION].update_one(
                        {"file_id": file_id, "stage": current_hist.stage},
                        {"$set": current_hist.model_dump()},
                        upsert=True,
                    )
                except Exception:
                    pass

                if tracking.stage_history:
                    new_hist = tracking.stage_history[-1]
                    if new_hist is not current_hist:
                        try:
                            self.db[STAGE_HISTORY_COLLECTION].insert_one(new_hist.model_dump())
                        except Exception:
                            pass

        return tracking
    
    def get_file_tracking(self, file_id: str) -> Optional[dict]:
        """Get tracking information for a file - returns raw dict to handle mixed data formats"""
        doc = self.db[FILE_TRACKING_COLLECTION].find_one({"file_id": file_id})
        return doc
    
    def assign_employee_to_stage(self, file_id: str, employee_code: str, employee_name: str, notes: Optional[str] = None) -> FileTracking:
        """Assign employee to work on current stage"""
        tracking = self.get_file_tracking(file_id)
        if not tracking:
            raise ValueError(f"No tracking found for file {file_id}")

        # get_file_tracking returns a raw dict; normalize to model for internal updates
        if isinstance(tracking, dict):
            tracking = _parse_file_tracking_safely(tracking)
            if not tracking:
                raise ValueError(f"Unable to parse tracking for file {file_id}")
        
        if tracking.current_status != "IN_PROGRESS":
            raise ValueError(f"Cannot assign employee to file {file_id} - status is {tracking.current_status}")
        
        # Update tracking
        tracking = assign_employee_to_stage(tracking, employee_code, employee_name, notes)
        
        # Save to database
        self.db[FILE_TRACKING_COLLECTION].update_one(
            {"file_id": file_id},
            {"$set": tracking.model_dump()}
        )
        
        # Update stage history
        if tracking.stage_history:
            current_stage_history = tracking.stage_history[-1]
            self.db[STAGE_HISTORY_COLLECTION].update_one(
                {"file_id": file_id, "stage": tracking.current_stage, "status": {"$in": ["PENDING", "IN_PROGRESS"]}},
                {"$set": current_stage_history.model_dump()}
            )
        
        # Emit lifecycle event for stage assignment
        try:
            clickhouse_lifecycle_service.emit_file_lifecycle_event(
                file_id=file_id,
                event_type='STAGE_ASSIGNED',
                stage=tracking.current_stage.value,
                employee_code=employee_code,
                employee_name=employee_name,
                event_data={
                    'notes': notes,
                    'assigned_at': tracking.current_assignment.assigned_at.isoformat() if tracking.current_assignment.assigned_at else None
                }
            )
        except Exception as e:
            logger.warning(f"Failed to emit lifecycle event for assignment: {e}")
        
        logger.info(f"Assigned {employee_name} to file {file_id} stage {tracking.current_stage}")
        return tracking
    
    def start_stage_work(self, file_id: str, employee_code: str) -> FileTracking:
        """Mark that employee has started working on the stage"""
        tracking = self.get_file_tracking(file_id)
        if not tracking:
            raise ValueError(f"No tracking found for file {file_id}")

        if isinstance(tracking, dict):
            tracking = _parse_file_tracking_safely(tracking)
            if not tracking:
                raise ValueError(f"Unable to parse tracking for file {file_id}")
        
        if not tracking.current_assignment or tracking.current_assignment.employee_code != employee_code:
            raise ValueError(f"Employee {employee_code} not assigned to file {file_id}")
        
        if tracking.current_assignment.started_at:
            logger.warning(f"Work already started for file {file_id} by {employee_code}")
            return tracking
        
        # Update start time
        tracking.current_assignment.started_at = datetime.utcnow()
        tracking.updated_at = datetime.utcnow()
        
        # Update stage history
        if tracking.stage_history:
            tracking.stage_history[-1].assigned_to = tracking.current_assignment
            tracking.stage_history[-1].status = "IN_PROGRESS"
        
        # Save to database
        self.db[FILE_TRACKING_COLLECTION].update_one(
            {"file_id": file_id},
            {"$set": tracking.model_dump()}
        )
        
        self.db[STAGE_HISTORY_COLLECTION].update_one(
            {"file_id": file_id, "stage": tracking.current_stage},
            {"$set": {"assigned_to": tracking.current_assignment.model_dump(), "status": "IN_PROGRESS"}}
        )
        
        logger.info(f"Started work on file {file_id} stage {tracking.current_stage} by {employee_code}")
        
        # Emit stage started event to ClickHouse for real-time analytics
        try:
            from app.services.clickhouse_service import clickhouse_service
            clickhouse_service.emit_stage_started_event(
                task_id=f"FILE-{file_id}",
                employee_code=employee_code,
                employee_name=tracking.current_assignment.employee_name if tracking.current_assignment else '',
                stage=tracking.current_stage.value,
                file_id=file_id
            )
        except Exception as e:
            logger.warning(f"Failed to emit stage_started event: {e}")
        
        return tracking
    
    def complete_stage(self, file_id: str, employee_code: str, completion_notes: Optional[str] = None) -> FileTracking:
        """Complete current stage and calculate metrics"""
        tracking = self.get_file_tracking(file_id)
        if not tracking:
            raise ValueError(f"No tracking found for file {file_id}")

        if isinstance(tracking, dict):
            tracking = _parse_file_tracking_safely(tracking)
            if not tracking:
                raise ValueError(f"Unable to parse tracking for file {file_id}")
        
        if not tracking.current_assignment or tracking.current_assignment.employee_code != employee_code:
            raise ValueError(f"Employee {employee_code} not assigned to file {file_id}")
        
        # Complete current stage
        tracking = complete_current_stage(tracking, completion_notes)
        
        # Save to database
        self.db[FILE_TRACKING_COLLECTION].update_one(
            {"file_id": file_id},
            {"$set": tracking.model_dump()}
        )
        
        # Update stage history
        if tracking.stage_history:
            current_stage_history = tracking.stage_history[-1]
            self.db[STAGE_HISTORY_COLLECTION].update_one(
                {"file_id": file_id, "stage": tracking.current_stage},
                {"$set": current_stage_history.model_dump()}
            )
        
        logger.info(f"Completed stage {tracking.current_stage} for file {file_id} by {employee_code}")
        
        # Emit stage completion event to ClickHouse lifecycle service
        try:
            # Calculate duration for analytics
            duration_minutes = 0
            if tracking.current_assignment and tracking.current_assignment.started_at:
                if isinstance(tracking.current_assignment.started_at, datetime):
                    duration_minutes = int((datetime.utcnow() - tracking.current_assignment.started_at).total_seconds() / 60)
            
            # Emit comprehensive lifecycle event
            clickhouse_lifecycle_service.emit_file_lifecycle_event(
                file_id=file_id,
                event_type='STAGE_COMPLETED',
                stage=tracking.current_stage.value,
                employee_code=tracking.current_assignment.employee_code if tracking.current_assignment else '',
                employee_name=tracking.current_assignment.employee_name if tracking.current_assignment else '',
                event_data={
                    'completion_notes': completion_notes,
                    'duration_minutes': duration_minutes,
                    'next_stage': self._get_next_stage(tracking.current_stage).value if self._get_next_stage(tracking.current_stage) else None
                }
            )
            
            # Keep existing event for backward compatibility
            clickhouse_service.emit_stage_completed_event(
                task_id=f"FILE-{file_id}",
                employee_code=tracking.current_assignment.employee_code if tracking.current_assignment else '',
                employee_name=tracking.current_assignment.employee_name if tracking.current_assignment else '',
                stage=tracking.current_stage.value,
                duration_minutes=duration_minutes,
                file_id_param=file_id
            )
        except Exception as e:
            logger.warning(f"Failed to emit stage_completed event: {e}")
        
        # Automatic progression: PRODUCTION → COMPLETED
        if tracking.current_stage.value == "PRODUCTION":
            try:
                self._auto_progress_to_completed(file_id, tracking, employee_code)
            except Exception as e:
                logger.warning(f"Failed to auto-progress to COMPLETED: {e}")
        
        # Automatic progression: QC → DELIVERED
        elif tracking.current_stage.value == "QC":
            try:
                self._auto_progress_to_delivered(file_id, tracking, employee_code)
            except Exception as e:
                logger.warning(f"Failed to auto-progress to DELIVERED: {e}")
        
        # Note: COMPLETED → QC requires manager action (no auto-progression)
        # Manager picks file from COMPLETED stage for QC
        
        # Send stage completion notification
        try:
            self.notification_service.send_stage_completion_notification(
                file_id, 
                tracking.current_stage, 
                employee_code
            )
        except Exception as e:
            logger.warning(f"Failed to send stage completion notification: {str(e)}")
        
        return tracking
    
    def transition_to_next_stage(self, file_id: str, employee_code: str, next_stage: Optional[FileStage] = None) -> FileTracking:
        """Transition file to next stage"""
        tracking = self.get_file_tracking(file_id)
        if not tracking:
            raise ValueError(f"No tracking found for file {file_id}")

        if isinstance(tracking, dict):
            tracking = FileTracking(**tracking)
        
        # Validate that current stage is completed
        if tracking.stage_history and tracking.stage_history[-1].status != "COMPLETED":
            raise ValueError(f"Current stage {tracking.current_stage} must be completed before transition")
        
        # Transition
        tracking = transition_to_next_stage(tracking, next_stage)

        # Ensure current_status reflects the new stage
        try:
            if tracking.current_stage == FileStage.COMPLETED:
                tracking.current_status = "COMPLETED"
            elif tracking.current_stage == FileStage.DELIVERED:
                tracking.current_status = "DELIVERED"
            else:
                tracking.current_status = "IN_PROGRESS"
        except Exception:
            tracking.current_status = "IN_PROGRESS"

        tracking.updated_at = datetime.utcnow()
        
        # Save to database
        self.db[FILE_TRACKING_COLLECTION].update_one(
            {"file_id": file_id},
            {"$set": tracking.model_dump()}
        )
        
        # Keep ClickHouse file_lifecycle in sync for real-time dashboards
        try:
            clickhouse_service.update_file_stage(file_id, tracking.current_stage.value)
            logger.info(f"✅ Updated ClickHouse file_lifecycle for {file_id} to {tracking.current_stage.value}")
        except Exception as e:
            logger.warning(f"Failed to update ClickHouse file_lifecycle for {file_id}: {e}")
        
        # Also update permit_files collection to keep status in sync
        try:
            # Map FileStage to permit_files status
            stage_to_status = {
                "PRELIMS": "IN_PRELIMS",
                "PRODUCTION": "IN_PRODUCTION", 
                "COMPLETED": "COMPLETED",
                "QC": "IN_QC",
                "DELIVERED": "DELIVERED"
            }
            
            new_status = stage_to_status.get(tracking.current_stage.value, "IN_PRELIMS")
            
            self.db.permit_files.update_one(
                {"file_id": file_id},
                {
                    "$set": {
                        "current_stage": tracking.current_stage.value,
                        "status": new_status,
                        "updated_at": tracking.updated_at
                    }
                }
            )
            logger.info(f"✅ Updated permit_files for {file_id} to {new_status}")
        except Exception as e:
            logger.warning(f"Failed to update permit_files for {file_id}: {e}")
        
        # Insert new stage history if not delivered
        if tracking.current_status != "DELIVERED" and tracking.stage_history:
            new_stage_history = tracking.stage_history[-1]
            self.db[STAGE_HISTORY_COLLECTION].insert_one(new_stage_history.model_dump())
        
        logger.info(f"Transitioned file {file_id} to stage {tracking.current_stage}")
        return tracking
    
    def complete_and_transition(self, file_id: str, employee_code: str, completion_notes: Optional[str] = None, 
                               next_stage_employee_code: Optional[str] = None) -> FileTracking:
        """Complete current stage and optionally assign employee to next stage"""
        # Complete current stage
        tracking = self.complete_stage(file_id, employee_code, completion_notes)
        
        # Transition to next stage
        tracking = self.transition_to_next_stage(file_id, employee_code)
        
        # Auto-assign to next stage if employee provided
        if (next_stage_employee_code and 
            tracking.current_status == "IN_PROGRESS" and 
            tracking.current_stage != FileStage.DELIVERED):
            
            # Get employee name
            employee_doc = self.db.employee.find_one({"employee_code": next_stage_employee_code})
            employee_name = employee_doc.get("employee_name", "Unknown") if employee_doc else "Unknown"
            
            tracking = self.assign_employee_to_stage(file_id, next_stage_employee_code, employee_name)
        
        return tracking
    
    def force_transition(self, file_id: str, target_stage: FileStage, employee_code: str, notes: Optional[str] = None) -> FileTracking:
        """Force transition to a specific stage (admin override)"""
        tracking = self.get_file_tracking(file_id)
        if not tracking:
            raise ValueError(f"No tracking found for file {file_id}")

        if isinstance(tracking, dict):
            tracking = FileTracking(**tracking)
        
        # Complete current stage if in progress
        if tracking.current_status == "IN_PROGRESS" and tracking.current_assignment:
            tracking = complete_current_stage(tracking, notes)
        
        # Force transition
        tracking.current_stage = target_stage
        tracking.current_assignment = None
        
        # Create new stage history
        new_stage_history = FileStageHistory(
            file_id=tracking.file_id,
            stage=target_stage,
            status="PENDING",
            entered_stage_at=datetime.utcnow(),
            metadata={"forced_transition": True, "forced_by": employee_code, "notes": notes}
        )
        tracking.stage_history.append(new_stage_history)
        tracking.updated_at = datetime.utcnow()
        
        # Save to database
        self.db[FILE_TRACKING_COLLECTION].update_one(
            {"file_id": file_id},
            {"$set": tracking.model_dump()}
        )
        
        self.db[STAGE_HISTORY_COLLECTION].insert_one(new_stage_history.model_dump())
        
        logger.warning(f"Force transitioned file {file_id} to stage {target_stage} by {employee_code}")
        return tracking
    
    def _auto_progress_to_completed(self, file_id: str, tracking: FileTracking, employee_code: str):
        """Automatically progress from PRODUCTION to COMPLETED stage"""
        from app.models.stage_flow import FileStage
        
        logger.info(f"Auto-progressing file {file_id} from PRODUCTION to COMPLETED")
        
        # Update tracking to COMPLETED
        tracking.current_stage = FileStage.COMPLETED
        tracking.current_assignment = None
        tracking.current_status = "COMPLETED"
        tracking.updated_at = datetime.utcnow()
        
        # Create new stage history for COMPLETED
        completed_history = FileStageHistory(
            file_id=file_id,
            stage=FileStage.COMPLETED,
            status="COMPLETED",
            entered_stage_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            metadata={"auto_progressed": True, "from_stage": "PRODUCTION", "progressed_by": employee_code}
        )
        tracking.stage_history.append(completed_history)
        
        # Save to database
        self.db[FILE_TRACKING_COLLECTION].update_one(
            {"file_id": file_id},
            {"$set": tracking.model_dump()}
        )
        
        # Insert completed stage history
        self.db[STAGE_HISTORY_COLLECTION].insert_one(completed_history.model_dump())
        
        # Emit COMPLETED stage event
        try:
            from app.services.clickhouse_service import clickhouse_service
            clickhouse_service.emit_stage_completed_event(
                task_id=f"FILE-{file_id}",
                employee_code="",
                employee_name="",
                stage="COMPLETED",
                duration_minutes=0,  # Immediate progression, no work duration
                file_id_param=file_id
            )
            
            # Also emit stage_started for COMPLETED to track the transition
            clickhouse_service.emit_stage_started_event(
                task_id=f"FILE-{file_id}",
                employee_code="",
                employee_name="",
                stage="COMPLETED",
                file_id_param=file_id
            )
            
        except Exception as e:
            logger.warning(f"Failed to emit COMPLETED stage events: {e}")
        
        logger.info(f"Successfully auto-progressed file {file_id} to COMPLETED stage")
    
    def _auto_progress_to_delivered(self, file_id: str, tracking: FileTracking, employee_code: str):
        """Automatically progress from QC to DELIVERED stage"""
        from app.models.stage_flow import FileStage
        
        logger.info(f"Auto-progressing file {file_id} from QC to DELIVERED")
        
        now = datetime.utcnow()
        
        # Update tracking to DELIVERED
        tracking.current_stage = FileStage.DELIVERED
        tracking.current_assignment = None
        tracking.current_status = "DELIVERED"
        tracking.completed_at = now
        tracking.updated_at = now
        
        # Calculate total duration
        if tracking.started_at:
            total_duration = now - tracking.started_at
            tracking.total_duration_minutes = max(0, int(total_duration.total_seconds() / 60))
        
        # Create new stage history for DELIVERED
        delivered_history = FileStageHistory(
            file_id=file_id,
            stage=FileStage.DELIVERED,
            status="COMPLETED",
            entered_stage_at=now,
            completed_stage_at=now,
            metadata={"auto_progressed": True, "from_stage": "QC", "progressed_by": employee_code}
        )
        tracking.stage_history.append(delivered_history)
        
        # Save to database
        self.db[FILE_TRACKING_COLLECTION].update_one(
            {"file_id": file_id},
            {"$set": tracking.model_dump()}
        )
        
        # Insert delivered stage history
        self.db[STAGE_HISTORY_COLLECTION].insert_one(delivered_history.model_dump())
        
        # Keep ClickHouse in sync
        try:
            from app.services.clickhouse_service import clickhouse_service
            clickhouse_service.update_file_stage(file_id, "DELIVERED")
            logger.info(f"✅ Updated ClickHouse file_lifecycle for {file_id} to DELIVERED")
        except Exception as e:
            logger.warning(f"Failed to update ClickHouse file_lifecycle for {file_id}: {e}")
        
        # Emit DELIVERED stage event
        try:
            from app.services.clickhouse_service import clickhouse_service
            clickhouse_service.emit_stage_completed_event(
                task_id=f"FILE-{file_id}",
                employee_code=employee_code,
                employee_name=tracking.current_assignment.employee_name if tracking.current_assignment else "",
                stage="QC",
                duration_minutes=0,  # Immediate progression, no additional work duration
                file_id_param=file_id
            )
            
            # Also emit stage_started for DELIVERED to track the transition
            clickhouse_service.emit_stage_started_event(
                task_id=f"FILE-{file_id}",
                employee_code="",
                employee_name="",
                stage="DELIVERED",
                file_id_param=file_id
            )
            
        except Exception as e:
            logger.warning(f"Failed to emit DELIVERED stage events: {e}")
        
        logger.info(f"Successfully auto-progressed file {file_id} to DELIVERED stage")
    
    def check_sla_breaches(self) -> List[Dict]:
        """Check for files that have breached SLA and need escalation"""
        breached_files = []
        now = datetime.utcnow()
        seen_ids = set()  # Track seen file/task IDs to avoid duplicates
        
        # 1. Check file_stage_tracking collection (original logic)
        active_files = self.db[FILE_TRACKING_COLLECTION].find({
            "current_status": "IN_PROGRESS",
            "current_assignment.started_at": {"$exists": True}
        })
        
        for file_doc in active_files:
            try:
                tracking = _parse_file_tracking_safely(file_doc)
                if not tracking:
                    continue
                
                if not tracking.current_assignment or not tracking.current_assignment.started_at:
                    continue
                
                # Calculate SLA status
                sla_status = calculate_sla_status(
                    tracking.current_assignment.started_at,
                    now,
                    tracking.current_stage
                )
                
                # Check if escalation needed
                config = get_stage_config(tracking.current_stage)
                duration_minutes = int((now - tracking.current_assignment.started_at).total_seconds() / 60)
                
                if duration_minutes > config.escalation_minutes:
                    current_stage_history = tracking.stage_history[-1] if tracking.stage_history else None
                    if not current_stage_history or not current_stage_history.escalation_sent:
                        seen_ids.add(tracking.file_id)
                        breached_files.append({
                            "file_id": tracking.file_id,
                            "current_stage": tracking.current_stage,
                            "employee_code": tracking.current_assignment.employee_code,
                            "employee_name": tracking.current_assignment.employee_name,
                            "duration_minutes": duration_minutes,
                            "ideal_minutes": config.ideal_minutes,
                            "max_minutes": config.max_minutes,
                            "escalation_threshold": config.escalation_minutes,
                            "sla_status": sla_status,
                            "status": "over_max" if duration_minutes > config.max_minutes else "over_ideal"
                        })
            except Exception as e:
                logger.warning(f"Error processing file tracking {file_doc.get('file_id')}: {e}")
                continue
        
        # 2. Also check tasks collection for assigned tasks (Smart Recommender tasks)
        assigned_tasks = self.db.tasks.find({
            "status": {"$in": ["ASSIGNED", "IN_PROGRESS"]},
            "assigned_to": {"$exists": True, "$ne": None},
            "assigned_at": {"$exists": True}
        })
        
        # Batch fetch employees to avoid N+1 queries
        employee_codes_needed = []
        for task in assigned_tasks:
            employee_code = task.get("assigned_to", "")
            if employee_code and not task.get("assigned_to_name"):
                employee_codes_needed.append(employee_code)
        
        employee_map = self._batch_fetch_employees(employee_codes_needed)
        
        for task in assigned_tasks:
            try:
                task_id = task.get("task_id", str(task.get("_id")))
                if task_id in seen_ids:
                    continue
                
                # Parse assigned_at timestamp
                assigned_at = task.get("assigned_at")
                if isinstance(assigned_at, str):
                    try:
                        assigned_at = datetime.fromisoformat(assigned_at.replace('Z', '+00:00'))
                    except:
                        continue
                elif not isinstance(assigned_at, datetime):
                    continue
                
                # Determine stage from task (default to PRELIMS for Smart Recommender tasks)
                stage_value = task.get("stage", "PRELIMS")
                stage_str = stage_value.upper() if stage_value else "PRELIMS"
                try:
                    stage = FileStage(stage_str)
                except ValueError:
                    stage = FileStage.PRELIMS
                
                config = get_stage_config(stage)
                if not config:
                    continue
                
                # Calculate duration in minutes
                if assigned_at.tzinfo:
                    duration_minutes = int((datetime.now(assigned_at.tzinfo) - assigned_at).total_seconds() / 60)
                else:
                    duration_minutes = int((now - assigned_at).total_seconds() / 60)
                
                # Check if SLA breached (using escalation_minutes as threshold)
                if duration_minutes > config.escalation_minutes:
                    seen_ids.add(task_id)
                    
                    # Get employee name from batch-fetched map
                    employee_code = task.get("assigned_to", "")
                    employee_name = task.get("assigned_to_name", "")
                    if not employee_name and employee_code:
                        emp = employee_map.get(employee_code)
                        employee_name = emp.get("employee_name", f"Employee {employee_code}") if emp else f"Employee {employee_code}"
                    
                    sla_status = "over_max" if duration_minutes > config.max_minutes else "over_ideal"
                    
                    breached_files.append({
                        "file_id": task_id,
                        "current_stage": stage.value,
                        "employee_code": employee_code,
                        "employee_name": employee_name,
                        "duration_minutes": duration_minutes,
                        "ideal_minutes": config.ideal_minutes,
                        "max_minutes": config.max_minutes,
                        "escalation_threshold": config.escalation_minutes,
                        "sla_status": sla_status,
                        "status": sla_status,
                        "source": "tasks"  # Indicate this came from tasks collection
                    })
            except Exception as e:
                logger.warning(f"Error processing task {task.get('task_id')}: {e}")
                continue
        
        # Send notifications for breaches
        if breached_files:
            try:
                self.notification_service.check_and_send_sla_escalations(breached_files)
                
                # Emit SLA breach events to ClickHouse for real-time analytics
                cache = get_cache()
                dedupe_ttl_seconds = 1800  # 30 minutes
                for breached_file in breached_files:
                    try:
                        from app.services.clickhouse_service import clickhouse_service
                        
                        # Handle both dict and object access
                        file_id = breached_file.get('file_id') if isinstance(breached_file, dict) else getattr(breached_file, 'file_id', None)
                        current_assignment = breached_file.get('current_assignment') if isinstance(breached_file, dict) else getattr(breached_file, 'current_assignment', None)
                        current_stage = breached_file.get('current_stage') if isinstance(breached_file, dict) else getattr(breached_file, 'current_stage', None)
                        sla_status = breached_file.get('sla_status') if isinstance(breached_file, dict) else getattr(breached_file, 'sla_status', None)
                        
                        # Extract employee info safely
                        employee_code = ''
                        employee_name = ''
                        if current_assignment:
                            if isinstance(current_assignment, dict):
                                employee_code = current_assignment.get('employee_code', '')
                                employee_name = current_assignment.get('employee_name', '')
                            else:
                                employee_code = getattr(current_assignment, 'employee_code', '')
                                employee_name = getattr(current_assignment, 'employee_name', '')
                        
                        # Extract stage and SLA status safely
                        stage_name = ''
                        if current_stage:
                            if hasattr(current_stage, 'value'):
                                stage_name = current_stage.value
                            elif isinstance(current_stage, str):
                                stage_name = current_stage
                        
                        sla_status_name = ''
                        if sla_status:
                            if hasattr(sla_status, 'value'):
                                sla_status_name = sla_status.value
                            elif isinstance(sla_status, str):
                                sla_status_name = sla_status
                        
                        # Emit SLA breach events to both old and new systems
                        try:
                            # Calculate breach details
                            stage_config = get_stage_config(current_stage) if current_stage else None
                            sla_threshold = stage_config.max_minutes if stage_config else 60
                            
                            # Calculate stage duration
                            stage_duration = 0
                            if current_assignment and hasattr(current_assignment, 'started_at') and current_assignment.started_at:
                                if isinstance(current_assignment.started_at, datetime):
                                    stage_duration = int((datetime.utcnow() - current_assignment.started_at).total_seconds() / 60)
                            
                            overdue_by = stage_duration - sla_threshold if stage_duration > sla_threshold else 0
                            
                            # Determine impact level
                            impact_level = 'high' if overdue_by > 120 else 'medium' if overdue_by > 60 else 'low'
                            
                            breach_data = {
                                'breach_type': 'time_exceeded',
                                'breach_minutes': stage_duration,
                                'sla_threshold': sla_threshold,
                                'impact_level': impact_level,
                                'overdue_by': overdue_by,
                                'stage_duration': stage_duration,
                                'notification_sent': False
                            }
                            
                            # Emit to new lifecycle service
                            clickhouse_lifecycle_service.emit_sla_breach_event(
                                file_id=file_id,
                                stage=stage_name,
                                employee_code=employee_code,
                                employee_name=employee_name,
                                breach_data=breach_data
                            )
                            
                            # Keep old system for backward compatibility
                            clickhouse_service.emit_sla_breach_event_sync(
                                file_id=file_id,
                                employee_code=employee_code,
                                employee_name=employee_name,
                                stage=stage_name,
                                sla_status=sla_status_name,
                                file_id_param=file_id
                            )
                            
                            logger.info(f"✅ Emitted SLA breach events for {file_id} in {stage_name}")
                            
                        except Exception as e:
                            logger.warning(f"Failed to emit SLA breach events for {file_id}: {e}")
                    except Exception as e:
                        file_id_for_log = breached_file.get('file_id') if isinstance(breached_file, dict) else getattr(breached_file, 'file_id', 'unknown')
                        logger.warning(f"Failed to emit sla_breach event for {file_id_for_log}: {e}")
                        
            except Exception as e:
                logger.warning(f"Failed to send escalation notifications: {str(e)}")
        
        return breached_files
    
    def _get_recent_mongodb_assignments(self, minutes: int = 5) -> Dict[str, Dict[str, Any]]:
        """
        Get recent task assignments from MongoDB that may not have been synced to ClickHouse yet.
        This ensures newly assigned tasks appear in stage tracking immediately.
        """
        try:
            from datetime import timedelta
            cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
            
            recent_assignments = {}
            
            # Get recent assigned tasks from tasks collection
            recent_tasks = list(self.db.tasks.find({
                "status": "ASSIGNED",
                "assigned_at": {"$gte": cutoff_time},
                "source.permit_file_id": {"$exists": True, "$ne": None}
            }))
            
            for task in recent_tasks:
                file_id = task.get("source", {}).get("permit_file_id")
                if not file_id:
                    continue
                
                # Get stage from task or use PRELIMS as default
                stage = "PRELIMS"  # Default for newly assigned tasks
                if hasattr(task, 'stage') and task.stage:
                    stage = task.stage.value if hasattr(task.stage, 'value') else str(task.stage)
                elif 'stage' in task:
                    stage = task['stage']
                
                # Calculate duration
                assigned_at = task.get("assigned_at", cutoff_time)
                if isinstance(assigned_at, str):
                    assigned_at = datetime.fromisoformat(assigned_at.replace('Z', '+00:00')).replace(tzinfo=None)
                
                duration_minutes = max(0, int((datetime.utcnow() - assigned_at).total_seconds() / 60))
                
                recent_assignments[file_id] = {
                    "employee_code": task.get("assigned_to"),
                    "employee_name": task.get("assigned_to_name", task.get("assigned_to")),
                    "assigned_at": assigned_at,
                    "started_at": assigned_at,
                    "stage": stage,
                    "status": f"IN_{stage}" if stage != "COMPLETED" else stage,
                    "duration_minutes": duration_minutes
                }
            
            # Also check file_tracking for recent current_assignment updates
            recent_tracking = list(self.db[FILE_TRACKING_COLLECTION].find({
                "current_assignment.assigned_at": {"$gte": cutoff_time}
            }))
            
            for tracking in recent_tracking:
                file_id = tracking.get("file_id")
                if not file_id:
                    continue
                
                current_assignment = tracking.get("current_assignment", {})
                assigned_at = current_assignment.get("assigned_at", cutoff_time)
                if isinstance(assigned_at, str):
                    assigned_at = datetime.fromisoformat(assigned_at.replace('Z', '+00:00')).replace(tzinfo=None)
                
                stage = tracking.get("current_stage", "PRELIMS")
                duration_minutes = max(0, int((datetime.utcnow() - assigned_at).total_seconds() / 60))
                
                # Use tracking data if more recent than task data
                if file_id not in recent_assignments or assigned_at > recent_assignments[file_id]["assigned_at"]:
                    recent_assignments[file_id] = {
                        "employee_code": current_assignment.get("employee_code"),
                        "employee_name": current_assignment.get("employee_name"),
                        "assigned_at": assigned_at,
                        "started_at": current_assignment.get("started_at", assigned_at),
                        "stage": stage,
                        "status": tracking.get("current_status", f"IN_{stage}" if stage != "COMPLETED" else stage),
                        "duration_minutes": duration_minutes
                    }
            
            logger.info(f"Found {len(recent_assignments)} recent assignments from MongoDB (last {minutes} minutes)")
            return recent_assignments
            
        except Exception as e:
            logger.error(f"Failed to get recent MongoDB assignments: {e}")
            return {}
    
    def get_stage_pipeline_view(self, stage: Optional[FileStage] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get pipeline view of files at each stage.
        Uses ClickHouse for real-time lifecycle tracking with MongoDB fallback.
        """
        try:
            # Use real-time pipeline view from file_lifecycle
            stage_filter = stage.value if stage and hasattr(stage, 'value') else stage
            ch_data = clickhouse_service.get_pipeline_view_realtime(stage_filter)
            
            if ch_data:
                logger.info(f"✅ Using real-time pipeline view - returned {len(ch_data)} rows")
                # Format ClickHouse data for frontend
                pipeline = {
                    "PRELIMS": [],
                    "PRODUCTION": [],
                    "COMPLETED": [],
                    "QC": [],
                    "DELIVERED": []
                }

                # Batch fetch tracking + permit file name info for all files
                file_ids = [row[1] for row in ch_data if row and len(row) > 1 and row[1]]
                tracking_map: Dict[str, Dict[str, Any]] = {}
                filename_map: Dict[str, str] = {}
                uploaded_at_map: Dict[str, datetime] = {}
                if file_ids:
                    try:
                        tracking_docs = list(self.db[FILE_TRACKING_COLLECTION].find(
                            {"file_id": {"$in": file_ids}},
                            {
                                "_id": 0,
                                "file_id": 1,
                                "current_stage": 1,
                                "current_status": 1,
                                "current_assignment": 1,
                                "stage_history": 1,
                                "created_at": 1,
                                "updated_at": 1,
                                "total_penalty_points": 1,
                                "escalations_triggered": 1,
                            }
                        ))
                        tracking_map = {d.get("file_id"): d for d in tracking_docs if d.get("file_id")}
                    except Exception as e:
                        logger.warning(f"Failed to batch fetch tracking docs: {e}")

                    try:
                        permit_docs = list(self.db.permit_files.find(
                            {"file_id": {"$in": file_ids}},
                            {
                                "_id": 0,
                                "file_id": 1,
                                "file_info.original_filename": 1,
                                "file_info.uploaded_at": 1,
                                "metadata.created_at": 1,
                                "file_name": 1,
                            }
                        ))
                        for pf in permit_docs:
                            pf_id = pf.get("file_id")
                            if not pf_id:
                                continue
                            filename_map[pf_id] = (
                                pf.get("file_info", {}).get("original_filename") or
                                pf.get("file_name") or
                                pf_id
                            )

                            uploaded_at = pf.get("file_info", {}).get("uploaded_at") or pf.get("metadata", {}).get("created_at")
                            if isinstance(uploaded_at, datetime):
                                uploaded_at_map[pf_id] = uploaded_at
                    except Exception as e:
                        logger.warning(f"Failed to batch fetch permit file names: {e}")

                now = datetime.utcnow()
                
                for row in ch_data:
                    stage_name = row[0]
                    # Filter out internal states - only expose business workflow stages
                    if stage_name and stage_name.upper() in pipeline:
                        # Columns: stage(0), file_id(1), employee_id(2), employee_name(3), 
                        # assigned_at(4), current_status(5), sla_deadline(6), sla_status(7)
                        
                        assigned_at = row[4] if isinstance(row[4], datetime) else None
                        sla_deadline = row[6] if isinstance(row[6], datetime) else None
                        
                        # Add SLA config values for consistent UI
                        try:
                            from app.models.stage_flow import STAGE_CONFIGS
                            stage_enum = FileStage(stage_name)
                            stage_config = STAGE_CONFIGS.get(stage_enum)
                            
                            if not stage_config:
                                logger.warning(f"No stage config found for {stage_name}")
                                continue

                            file_id = row[1]
                            tracking_doc = tracking_map.get(file_id)
                            tracking = _parse_file_tracking_safely(tracking_doc) if tracking_doc else None

                            # Determine time-in-current-stage (entered_stage_at -> now)
                            entered_at: Optional[datetime] = None
                            if tracking and tracking.stage_history:
                                for h in reversed(tracking.stage_history):
                                    h_stage = h.stage.value if hasattr(h.stage, "value") else str(h.stage)
                                    if h_stage and h_stage.upper() == stage_name.upper():
                                        entered_at = h.entered_stage_at or h.created_at
                                        break

                            if not entered_at:
                                # Prefer ClickHouse assignment time, otherwise fall back to permit upload time
                                entered_at = assigned_at or uploaded_at_map.get(file_id) or now

                            if entered_at.tzinfo:
                                entered_at = entered_at.astimezone(timezone.utc).replace(tzinfo=None)

                            duration_minutes = max(0, int((now - entered_at).total_seconds() / 60))

                            # Build current_assignment for frontend
                            employee_code = row[2] or ""
                            employee_name = row[3] or ""
                            started_at: Optional[datetime] = assigned_at
                            sla_status = row[7]

                            if tracking and tracking.current_assignment:
                                employee_code = tracking.current_assignment.employee_code or employee_code
                                employee_name = tracking.current_assignment.employee_name or employee_name
                                assigned_at = tracking.current_assignment.assigned_at or assigned_at
                                started_at = tracking.current_assignment.started_at or started_at
                                if tracking.current_assignment.sla_status and isinstance(tracking.current_assignment.sla_status, dict):
                                    sla_status = tracking.current_assignment.sla_status.get("status") or sla_status

                            # Use the same duration source everywhere: time in current stage
                            created_at = (tracking.created_at if tracking else None) or uploaded_at_map.get(file_id) or now
                            updated_at = (tracking.updated_at if tracking else None) or uploaded_at_map.get(file_id) or now

                            file_data = {
                                "file_id": file_id,
                                "original_filename": filename_map.get(file_id, file_id),
                                "current_status": (tracking.current_status if tracking else None) or row[5],
                                "entered_stage_at": entered_at.isoformat() + 'Z' if isinstance(entered_at, datetime) else None,
                                "current_assignment": {
                                    "employee_code": employee_code,
                                    "employee_name": employee_name,
                                    "assigned_at": assigned_at.isoformat() + 'Z' if isinstance(assigned_at, datetime) else None,
                                    "started_at": started_at.isoformat() + 'Z' if isinstance(started_at, datetime) else None,
                                    "duration_minutes": duration_minutes,
                                    "ideal_minutes": stage_config.ideal_minutes,
                                    "max_minutes": stage_config.max_minutes,
                                    "sla_status": sla_status,
                                },
                                "created_at": created_at,
                                "updated_at": updated_at,
                                "total_penalty_points": tracking.total_penalty_points if tracking else 0,
                                "escalations_triggered": tracking.escalations_triggered if tracking else 0,
                                "sla_deadline": sla_deadline.isoformat() + 'Z' if sla_deadline else None,
                                "source": "clickhouse",
                            }

                            pipeline[stage_name.upper()].append(file_data)
                            
                        except Exception as e:
                            logger.warning(f"Error adding file {row[1]} to pipeline: {e}")
                            continue
                
                logger.info(f"✅ Real-time pipeline view built successfully")
                
                # HYBRID APPROACH: Add recent assignments from MongoDB that might not be in ClickHouse yet
                try:
                    recent_assignments = self._get_recent_mongodb_assignments(minutes=5)
                    for file_id, assignment_data in recent_assignments.items():
                        # Check if file already exists in pipeline data
                        existing_in_pipeline = False
                        for stage_files in pipeline.values():
                            if any(f.get("file_id") == file_id for f in stage_files):
                                existing_in_pipeline = True
                                break
                        
                        if not existing_in_pipeline:
                            # Add to appropriate stage based on assignment
                            stage_name = assignment_data.get("stage", "PRELIMS").upper()
                            if stage_name in pipeline:
                                tracking_doc = tracking_map.get(file_id)
                                tracking = _parse_file_tracking_safely(tracking_doc) if tracking_doc else None
                                
                                file_data = {
                                    "file_id": file_id,
                                    "original_filename": filename_map.get(file_id, file_id),
                                    "current_status": assignment_data.get("status", "IN_PRELIMS"),
                                    "entered_stage_at": assignment_data.get("assigned_at", now).isoformat() + 'Z',
                                    "current_assignment": {
                                        "employee_code": assignment_data.get("employee_code"),
                                        "employee_name": assignment_data.get("employee_name"),
                                        "assigned_at": assignment_data.get("assigned_at", now).isoformat() + 'Z',
                                        "started_at": assignment_data.get("started_at", now).isoformat() + 'Z',
                                        "duration_minutes": assignment_data.get("duration_minutes", 0),
                                        "ideal_minutes": 30,  # PRELIMS default
                                        "max_minutes": 60,    # PRELIMS default
                                        "sla_status": "within_ideal",
                                    },
                                    "created_at": tracking.created_at if tracking else now,
                                    "updated_at": tracking.updated_at if tracking else now,
                                    "total_penalty_points": tracking.total_penalty_points if tracking else 0,
                                    "escalations_triggered": tracking.escalations_triggered if tracking else 0,
                                    "sla_deadline": None,
                                    "source": "mongodb_fresh",
                                }
                                pipeline[stage_name].append(file_data)
                                logger.info(f"Added recent assignment {file_id} to {stage_name} from MongoDB")
                except Exception as e:
                    logger.warning(f"Failed to add recent MongoDB assignments: {e}")
                
                return pipeline
            
            # Empty result
            return {
                "PRELIMS": [],
                "PRODUCTION": [],
                "COMPLETED": [],
                "QC": [],
                "DELIVERED": []
            }
            
        except Exception as e:
            logger.warning(f"Real-time pipeline query failed, falling back to MongoDB: {e}")
            
            # Fallback to MongoDB (original logic)
            logger.info("⚠️ Using MongoDB for pipeline view (slow)")
            pipeline = {
                "PRELIMS": [],
                "PRODUCTION": [],
                "COMPLETED": [],
                "QC": [],
                "DELIVERED": []
            }
            
            seen_ids = set()
            now = datetime.utcnow()
            
            # 1. Get all files from file_stage_tracking collection
            files = list(self.db[FILE_TRACKING_COLLECTION].find({}))
            
            # Batch fetch all permit files to avoid N+1 queries
            file_ids = [f.get('file_id') for f in files if f.get('file_id')]
            permit_files = {}
            if file_ids:
                for pf in self.db.permit_files.find({'file_id': {'$in': file_ids}}):
                    permit_files[pf['file_id']] = pf
            
            # 2. Process each file's current stage
            for file_tracking in files:
                file_id = file_tracking.get('file_id')
                if not file_id or file_id in seen_ids:
                    continue
                
                seen_ids.add(file_id)
                
                tracking = _parse_file_tracking_safely(file_tracking)
                if not tracking:
                    continue

                current_stage = tracking.current_stage.value if hasattr(tracking.current_stage, "value") else str(tracking.current_stage)
                if not current_stage or current_stage.upper() not in pipeline:
                    continue
                
                # Get permit file info
                permit_file = permit_files.get(file_id, {})
                client_name = permit_file.get('client_info', {}).get('client_name', 'Unknown Client')
                project_name = permit_file.get('project_name', '')
                uploaded_at = permit_file.get('metadata', {}).get('created_at', now)
                
                # Determine time-in-current-stage
                entered_at = None
                if tracking.stage_history:
                    for h in reversed(tracking.stage_history):
                        h_stage = h.stage.value if hasattr(h.stage, "value") else str(h.stage)
                        if h_stage and h_stage.upper() == current_stage.upper():
                            entered_at = h.entered_stage_at or h.created_at
                            break
                entered_at = entered_at or uploaded_at or now
                if isinstance(entered_at, str):
                    try:
                        entered_at = datetime.fromisoformat(entered_at.replace('Z', '+00:00'))
                    except Exception:
                        entered_at = now
                if isinstance(entered_at, datetime) and entered_at.tzinfo:
                    entered_at = entered_at.astimezone(timezone.utc).replace(tzinfo=None)

                duration_minutes = max(0, int((now - entered_at).total_seconds() / 60))

                # Build current_assignment
                employee_code = ""
                employee_name = ""
                assigned_at = None
                started_at = None
                if tracking.current_assignment:
                    employee_code = tracking.current_assignment.employee_code
                    employee_name = tracking.current_assignment.employee_name
                    assigned_at = tracking.current_assignment.assigned_at
                    started_at = tracking.current_assignment.started_at

                stage_config = get_stage_config(FileStage(current_stage))
                file_data = {
                    "file_id": file_id,
                    "original_filename": (
                        permit_file.get("file_info", {}).get("original_filename") or
                        permit_file.get("file_name") or
                        file_id
                    ),
                    "current_status": tracking.current_status,
                    "entered_stage_at": entered_at.isoformat() + 'Z' if isinstance(entered_at, datetime) else None,
                    "current_assignment": {
                        "employee_code": employee_code,
                        "employee_name": employee_name,
                        "assigned_at": assigned_at.isoformat() + 'Z' if isinstance(assigned_at, datetime) else None,
                        "started_at": started_at.isoformat() + 'Z' if isinstance(started_at, datetime) else None,
                        "duration_minutes": duration_minutes,
                        "ideal_minutes": stage_config.ideal_minutes if stage_config else 30,
                        "max_minutes": stage_config.max_minutes if stage_config else 60,
                        "sla_status": "within_ideal",
                    },
                    "created_at": tracking.created_at,
                    "updated_at": tracking.updated_at,
                    "total_penalty_points": tracking.total_penalty_points,
                    "escalations_triggered": tracking.escalations_triggered,
                    "source": "mongodb",
                }

                pipeline[current_stage.upper()].append(file_data)
            
            logger.info(f"✅ MongoDB fallback pipeline view built with {sum(len(v) for v in pipeline.values())} files")
            return pipeline
    
    def complete_stage_and_progress(self, file_id: str, employee_code: str, employee_name: str) -> Dict:
        """Complete current stage and move file to next stage in sequence.
        
        Stage auto-progression rules:
          PRELIMS  → explicit transition to PRODUCTION (via this method)
          PRODUCTION → auto-progresses to COMPLETED inside complete_stage()
          COMPLETED → requires manager action to move to QC (no auto)
          QC       → auto-progresses to DELIVERED inside complete_stage()
          DELIVERED → terminal stage
        """
        from app.models.stage_flow import FileStage
        
        # Get current tracking before completing
        tracking = self.get_file_tracking(file_id)
        if not tracking:
            raise ValueError(f"No tracking found for file {file_id}")

        if isinstance(tracking, dict):
            tracking = FileTracking(**tracking)

        previous_stage = tracking.current_stage
        if isinstance(previous_stage, str):
            try:
                previous_stage = FileStage(previous_stage)
            except Exception:
                previous_stage = FileStage.PRELIMS

        # Complete current stage (this auto-progresses PRODUCTION→COMPLETED and QC→DELIVERED)
        self.complete_stage(file_id, employee_code)

        # For PRELIMS only: explicitly transition to PRODUCTION
        # (PRODUCTION→COMPLETED and QC→DELIVERED are handled inside complete_stage)
        if previous_stage == FileStage.PRELIMS:
            try:
                self.transition_to_next_stage(file_id, employee_code, FileStage.PRODUCTION)
                next_stage = FileStage.PRODUCTION
            except Exception as e:
                logger.warning(f"[STAGE-PROGRESS] Could not transition {file_id} to PRODUCTION: {e}")
                next_stage = FileStage.PRODUCTION
        elif previous_stage == FileStage.PRODUCTION:
            next_stage = FileStage.COMPLETED   # already done by complete_stage auto-progress
        elif previous_stage == FileStage.QC:
            next_stage = FileStage.DELIVERED   # already done by complete_stage auto-progress
        else:
            next_stage = None

        # Always sync permit_files status to match the actual file_tracking stage
        try:
            ft_now = self.db[FILE_TRACKING_COLLECTION].find_one({"file_id": file_id}, {"current_stage": 1})
            if ft_now:
                real_stage = ft_now.get("current_stage", "PRELIMS")
                stage_to_status = {
                    "PRELIMS": "IN_PRELIMS",
                    "PRODUCTION": "IN_PRODUCTION",
                    "COMPLETED": "COMPLETED",
                    "QC": "IN_QC",
                    "DELIVERED": "DELIVERED",
                }
                real_status = stage_to_status.get(real_stage, "IN_PRELIMS")
                self.db.permit_files.update_one(
                    {"file_id": file_id},
                    {
                        "$set": {
                            "current_stage": real_stage,
                            "workflow_step": real_stage,
                            "project_details.project_name": real_stage,
                            "status": real_status,
                            "acceptance.accepted_by": None,
                            "acceptance.accepted_at": None,
                            "updated_at": datetime.utcnow()
                        }
                    }
                )
                logger.info(f"[PERMIT-SYNC] Synced permit_files {file_id} → {real_stage} / {real_status}")
        except Exception as ps_err:
            logger.warning(f"[PERMIT-SYNC-WARN] Could not sync permit_files for {file_id}: {ps_err}")

        return {
            "success": True,
            "file_id": file_id,
            "previous_stage": previous_stage.value if hasattr(previous_stage, "value") else str(previous_stage),
            "next_stage": next_stage.value if next_stage and hasattr(next_stage, "value") else str(next_stage) if next_stage else None,
            "completed_by": employee_code,
            "completed_by_name": employee_name,
            "message": f"File {file_id} progressed from {previous_stage} to {next_stage}" if next_stage else f"File {file_id} has completed all stages"
        }
    
    def get_files_ready_for_stage(self, stage: FileStage) -> List[Dict]:
        """Get files that are ready to be assigned to a specific stage"""
        from app.models.stage_flow import FileStage
        
        # Define stage progression sequence
        stage_sequence = [FileStage.PRELIMS, FileStage.PRODUCTION, FileStage.QC, FileStage.COMPLETED]
        
        # Files ready for PRELIMS (newly uploaded or accepted)
        if stage == FileStage.PRELIMS:
            # Get accepted permit files that haven't started PRELIMS
            permit_files = list(self.db.permit_files.find({
                "status": "ACCEPTED",
                "$or": [
                    {"workflow_step": {"$exists": False}},
                    {"workflow_step": "PRELIMS"}
                ]
            }))
            
            ready_files = []
            for permit_file in permit_files:
                # Check if not already in tracking or at PRELIMS stage
                tracking = self.get_file_tracking(permit_file["file_id"])
                current_stage_raw = (tracking or {}).get("current_stage") if isinstance(tracking, dict) else None
                is_prelims = current_stage_raw in (FileStage.PRELIMS, getattr(FileStage.PRELIMS, "value", None), "PRELIMS")
                if not tracking or is_prelims:
                    ready_files.append({
                        "file_id": permit_file["file_id"],
                        "original_filename": (
                            permit_file.get("file_info", {}).get("original_filename") or 
                            permit_file.get("file_name", "Unknown File")
                        ),
                        "client": permit_file.get("project_details", {}).get("client_name", "Unknown"),
                        "project": permit_file.get("project_details", {}).get("project_name", "Unknown"),
                        "ready_for_stage": stage
                    })
            
            return ready_files
        
        # Files ready for other stages (must complete previous stage)
        else:
            stage_index = stage_sequence.index(stage)
            previous_stage = stage_sequence[stage_index - 1]
            
            # Get files that completed previous stage
            completed_files = list(self.db[FILE_TRACKING_COLLECTION].find({
                "current_stage": previous_stage,
                "stage_history.stage": previous_stage,
                "stage_history.completed_at": {"$exists": True}
            }))
            
            ready_files = []
            for file_doc in completed_files:
                tracking = FileTracking(**file_doc)
                
                # Get permit file info
                permit_file = self.db.permit_files.find_one(
                    {"file_id": tracking.file_id},
                    {"_id": 0, "file_info": 1, "project_details": 1}
                )
                
                ready_files.append({
                    "file_id": tracking.file_id,
                    "original_filename": (
                        permit_file.get("file_info", {}).get("original_filename") or 
                        permit_file.get("file_name", "Unknown File")
                    ) if permit_file else "Unknown File",
                    "client": (
                        permit_file.get("project_details", {}).get("client_name", "Unknown")
                    ) if permit_file else "Unknown",
                    "project": (
                        permit_file.get("project_details", {}).get("project_name", "Unknown")
                    ) if permit_file else "Unknown",
                    "ready_for_stage": stage,
                    "completed_previous_stage": previous_stage,
                    "completed_by": tracking.stage_history[-1].assigned_to if tracking.stage_history else None
                })
            
            return ready_files
    
    def get_employee_performance(self, employee_code: str, days: int = 30) -> Dict:
        """Get performance metrics for an employee"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Get all tracking docs for the period
        tracking_docs = []
        files = self.db[FILE_TRACKING_COLLECTION].find({
            "updated_at": {"$gte": cutoff_date}
        })
        
        for file_doc in files:
            try:
                tracking_docs.append(FileTracking(**file_doc))
            except Exception:
                continue
        
        # Get summary from file tracking
        summary = get_employee_workload_summary(employee_code, tracking_docs)
        
        # ALSO check tasks collection for Smart Recommender tasks
        active_tasks = list(self.db.tasks.find({
            "assigned_to": employee_code,
            "status": {"$in": ["ASSIGNED", "IN_PROGRESS"]},
            "assigned_at": {"$gte": cutoff_date}
        }))
        
        completed_tasks = list(self.db.tasks.find({
            "assigned_to": employee_code,
            "status": {"$in": ["COMPLETED", "DONE"]},
            "assigned_at": {"$gte": cutoff_date}
        }))
        
        # Merge task counts into summary
        summary["active_assignments"] = summary.get("active_assignments", 0) + len(active_tasks)
        summary["completed_stages"] = summary.get("completed_stages", 0) + len(completed_tasks)
        
        # Calculate penalties from tasks
        task_penalties = 0
        for task in active_tasks + completed_tasks:
            assigned_at = task.get("assigned_at")
            if isinstance(assigned_at, str):
                try:
                    assigned_at = datetime.fromisoformat(assigned_at.replace('Z', '+00:00'))
                except:
                    continue
            elif not isinstance(assigned_at, datetime):
                continue
            
            # Calculate if SLA was breached (using PRELIMS threshold of 60 minutes)
            if assigned_at.tzinfo:
                duration = int((datetime.now(assigned_at.tzinfo) - assigned_at).total_seconds() / 60)
            else:
                duration = int((datetime.utcnow() - assigned_at).total_seconds() / 60)
            
            if duration > 60:  # PRELIMS escalation threshold
                task_penalties += 1
        
        summary["total_penalty_points"] = summary.get("total_penalty_points", 0) + task_penalties
        
        # Add active and completed work lists for tasks
        if "active_work" not in summary:
            summary["active_work"] = []
        if "completed_work" not in summary:
            summary["completed_work"] = []
        
        for task in active_tasks:
            summary["active_work"].append({
                "file_id": task.get("task_id"),
                "stage": task.get("stage", "PRELIMS"),
                "assigned_at": task.get("assigned_at"),
                "source": "tasks"
            })
        
        for task in completed_tasks:
            summary["completed_work"].append({
                "file_id": task.get("task_id"),
                "stage": task.get("stage", "PRELIMS"),
                "assigned_at": task.get("assigned_at"),
                "completed_at": task.get("completed_at"),
                "duration_minutes": 0,  # Will be calculated if timestamps available
                "source": "tasks"
            })
        
        # Add additional metrics
        completed_stages = summary.get("completed_work", [])
        if completed_stages:
            durations = [s.get("duration_minutes", 0) for s in completed_stages if s.get("duration_minutes")]
            avg_duration = sum(durations) / len(durations) if durations else 0
            summary["average_stage_duration_minutes"] = round(avg_duration, 1)
        else:
            summary["average_stage_duration_minutes"] = 0
        
        return summary
    
    def get_sla_report(self, days: int = 7) -> Dict:
        """Get SLA compliance report"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        now = datetime.utcnow()
        
        report = {
            "total_stages": 0,
            "completed_stages": 0,
            "within_ideal": 0,
            "over_ideal": 0,
            "over_max": 0,
            "escalations": 0,
            "by_stage": {}
        }
        
        # 1. Get stage history for the period (original logic)
        stages = self.db[STAGE_HISTORY_COLLECTION].find({
            "entered_stage_at": {"$gte": cutoff_date}
        })
        
        for stage_doc in stages:
            try:
                stage_history = _parse_file_stage_history_safely(stage_doc)
                if not stage_history:
                    continue
                    
                report["total_stages"] += 1
                
                stage_name = stage_history.stage
                if stage_name not in report["by_stage"]:
                    report["by_stage"][stage_name] = {
                        "total": 0,
                        "completed": 0,
                        "within_ideal": 0,
                        "over_ideal": 0,
                        "over_max": 0,
                        "escalations": 0
                    }
                
                stage_report = report["by_stage"][stage_name]
                stage_report["total"] += 1
                
                if stage_history.status == "COMPLETED":
                    report["completed_stages"] += 1
                    stage_report["completed"] += 1
                    
                    if stage_history.assigned_to and stage_history.assigned_to.sla_status:
                        sla_status = stage_history.assigned_to.sla_status
                        
                        if sla_status["status"] == "within_ideal":
                            report["within_ideal"] += 1
                            stage_report["within_ideal"] += 1
                        elif sla_status["status"] == "over_ideal":
                            report["over_ideal"] += 1
                            stage_report["over_ideal"] += 1
                        elif sla_status["status"] in ["over_max", "escalation_needed"]:
                            report["over_max"] += 1
                            stage_report["over_max"] += 1
                    
                    if stage_history.escalation_sent:
                        report["escalations"] += 1
                        stage_report["escalations"] += 1
            except Exception:
                continue
        
        # 2. Also include tasks from tasks collection
        tasks = self.db.tasks.find({
            "assigned_at": {"$gte": cutoff_date},
            "assigned_to": {"$exists": True, "$ne": None}
        })
        
        for task in tasks:
            try:
                report["total_stages"] += 1
                stage_value = task.get("stage", "PRELIMS")
                stage_name = stage_value.upper() if stage_value else "PRELIMS"
                
                if stage_name not in report["by_stage"]:
                    report["by_stage"][stage_name] = {
                        "total": 0,
                        "completed": 0,
                        "within_ideal": 0,
                        "over_ideal": 0,
                        "over_max": 0,
                        "escalations": 0
                    }
                
                stage_report = report["by_stage"][stage_name]
                stage_report["total"] += 1
                
                status_value = task.get("status", "")
                status = status_value.upper() if status_value else ""
                if status in ["COMPLETED", "DONE"]:
                    report["completed_stages"] += 1
                    stage_report["completed"] += 1
                    
                    # Calculate SLA status based on duration
                    assigned_at = task.get("assigned_at")
                    completed_at = task.get("completed_at")
                    if assigned_at and completed_at:
                        if isinstance(assigned_at, str):
                            assigned_at = datetime.fromisoformat(assigned_at.replace('Z', '+00:00'))
                        if isinstance(completed_at, str):
                            completed_at = datetime.fromisoformat(completed_at.replace('Z', '+00:00'))
                        
                        if isinstance(assigned_at, datetime) and isinstance(completed_at, datetime):
                            duration = int((completed_at - assigned_at).total_seconds() / 60)
                            config = get_stage_config(FileStage.PRELIMS)  # Default to PRELIMS
                            
                            if duration <= config.ideal_minutes:
                                report["within_ideal"] += 1
                                stage_report["within_ideal"] += 1
                            elif duration <= config.max_minutes:
                                report["over_ideal"] += 1
                                stage_report["over_ideal"] += 1
                            else:
                                report["over_max"] += 1
                                stage_report["over_max"] += 1
                else:
                    # Active task - check if breached
                    assigned_at = task.get("assigned_at")
                    if assigned_at:
                        if isinstance(assigned_at, str):
                            try:
                                assigned_at = datetime.fromisoformat(assigned_at.replace('Z', '+00:00'))
                            except:
                                continue
                        
                        if isinstance(assigned_at, datetime):
                            if assigned_at.tzinfo:
                                duration = int((datetime.now(assigned_at.tzinfo) - assigned_at).total_seconds() / 60)
                            else:
                                duration = int((now - assigned_at).total_seconds() / 60)
                            
                            config = get_stage_config(FileStage.PRELIMS)
                            if duration > config.escalation_minutes:
                                report["escalations"] += 1
                                stage_report["escalations"] += 1
            except Exception:
                continue
        
        return report


# Singleton instance
_stage_tracking_service = None

def get_stage_tracking_service() -> StageTrackingService:
    """Get singleton instance of stage tracking service"""
    global _stage_tracking_service
    if _stage_tracking_service is None:
        _stage_tracking_service = StageTrackingService()
    return _stage_tracking_service
