"""
Notification service for SLA breaches and escalations
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
from bson import ObjectId

from app.db.mongodb import get_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class NotificationType(str, Enum):
    SLA_BREACH = "sla_breach"
    STAGE_COMPLETED = "stage_completed"
    FILE_DELIVERED = "file_delivered"
    ASSIGNMENT_CREATED = "assignment_created"
    ESCALATION_REQUIRED = "escalation_required"


class NotificationChannel(str, Enum):
    EMAIL = "email"
    SMS = "sms"
    WEBHOOK = "webhook"
    IN_APP = "in_app"


@dataclass
class NotificationMessage:
    recipient: str
    recipient_type: str  # employee, manager, reporting_manager
    channel: NotificationChannel
    subject: str
    message: str
    data: Dict[str, Any]
    priority: str = "normal"  # low, normal, high, urgent


class NotificationService:
    """Service for sending notifications about SLA breaches and escalations"""
    
    def __init__(self):
        self.db = get_db()
    
    def check_and_send_sla_escalations(self, breaches: List[Dict] = None) -> Dict[str, Any]:
        """Check for SLA breaches and send escalation notifications"""
        try:
            if breaches is None:
                # This method should be called with breaches from stage service
                # to avoid circular dependency
                logger.warning("No breaches provided to notification service")
                return {"success": False, "error": "No breaches provided", "notifications_sent": 0}
            
            sent_notifications = []
            
            for breach in breaches:
                # Get employee details to find manager
                employee = self.db.employee.find_one({"employee_code": breach["employee_code"]})
                if not employee:
                    logger.warning(f"Employee {breach['employee_code']} not found for escalation")
                    continue
                
                # Get manager information
                manager_code = employee.get("reporting_manager", "")
                manager = None
                if manager_code:
                    # Try to find manager by code or name
                    manager = self.db.employee.find_one({"employee_code": manager_code})
                    if not manager:
                        # Try by name
                        manager = self.db.employee.find_one({"employee_name": manager_code})
                
                # Send notifications
                notifications_sent = self._send_sla_breach_notifications(breach, employee, manager)
                sent_notifications.extend(notifications_sent)
                
                # Mark escalation as sent in tracking
                self._mark_escalation_sent(breach["file_id"], breach["current_stage"])
            
            return {
                "success": True,
                "breaches_processed": len(breaches),
                "notifications_sent": len(sent_notifications),
                "details": sent_notifications
            }
            
        except Exception as e:
            logger.error(f"Failed to process SLA escalations: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "notifications_sent": 0
            }
    
    def _send_sla_breach_notifications(self, breach: Dict, employee: Dict, manager: Optional[Dict]) -> List[Dict]:
        """Send notifications for SLA breach"""
        notifications = []
        
        # Prepare message data
        message_data = {
            "file_id": breach["file_id"],
            "stage": breach["current_stage"],
            "employee_name": breach["employee_name"],
            "employee_code": breach["employee_code"],
            "duration_minutes": breach["duration_minutes"],
            "threshold_minutes": breach["escalation_threshold"],
            "over_by_minutes": breach["duration_minutes"] - breach["escalation_threshold"],
            "sla_status": breach["sla_status"]
        }
        
        # 1. Notify manager if available
        if manager:
            manager_notification = NotificationMessage(
                recipient=manager.get("employee_email", ""),
                recipient_type="manager",
                channel=NotificationChannel.EMAIL,
                subject=f"🚨 SLA Breach Alert - {breach['file_id']}",
                message=self._format_sla_breach_message(message_data, manager_type=True),
                data=message_data,
                priority="high"
            )
            
            result = self._send_notification(manager_notification)
            if result["success"]:
                notifications.append({
                    "type": "manager_notification",
                    "recipient": manager.get("employee_name", "Unknown Manager"),
                    "channel": "email",
                    "sent_at": datetime.utcnow().isoformat()
                })
        
        # 2. Notify reporting manager if different from manager
        reporting_manager_code = employee.get("reporting_manager_2", "")
        if reporting_manager_code and reporting_manager_code != manager_code:
            reporting_manager = self.db.employee.find_one({"employee_code": reporting_manager_code})
            if reporting_manager:
                reporting_notification = NotificationMessage(
                    recipient=reporting_manager.get("employee_email", ""),
                    recipient_type="reporting_manager",
                    channel=NotificationChannel.EMAIL,
                    subject=f"🚨 SLA Escalation - {breach['file_id']}",
                    message=self._format_sla_breach_message(message_data, manager_type=False),
                    data=message_data,
                    priority="urgent"
                )
                
                result = self._send_notification(reporting_notification)
                if result["success"]:
                    notifications.append({
                        "type": "reporting_manager_notification",
                        "recipient": reporting_manager.get("employee_name", "Unknown Reporting Manager"),
                        "channel": "email",
                        "sent_at": datetime.utcnow().isoformat()
                    })
        
        # 3. Log in-app notification for the employee
        employee_notification = NotificationMessage(
            recipient=breach["employee_code"],
            recipient_type="employee",
            channel=NotificationChannel.IN_APP,
            subject="SLA Breach Warning",
            message=f"Your work on {breach['file_id']} has exceeded the time limit. Please contact your manager.",
            data=message_data,
            priority="high"
        )
        
        result = self._send_notification(employee_notification)
        if result["success"]:
            notifications.append({
                "type": "employee_notification",
                "recipient": breach["employee_name"],
                "channel": "in_app",
                "sent_at": datetime.utcnow().isoformat()
            })
        
        return notifications
    
    def _format_sla_breach_message(self, data: Dict, manager_type: bool = True) -> str:
        """Format SLA breach notification message"""
        over_by = data["over_by_minutes"]
        hours = over_by // 60
        minutes = over_by % 60
        
        duration_str = f"{data['duration_minutes']} minutes"
        over_str = f"{hours}h {minutes}m" if hours > 0 else f"{minutes} minutes"
        
        if manager_type:
            return f"""
Dear Manager,

This is an automated alert that a task has exceeded the SLA threshold:

File ID: {data['file_id']}
Stage: {data['stage']}
Assigned to: {data['employee_name']} ({data['employee_code']})
Current Duration: {duration_str}
Threshold: {data['threshold_minutes']} minutes
Over by: {over_str}

The task is now {over_str} over the expected time limit. Please take appropriate action to ensure timely completion.

You can view the details in the Stage Tracking Dashboard.

Regards,
Task Assignment System
            """.strip()
        else:
            return f"""
URGENT ESCALATION: Task SLA Breach Requires Immediate Attention

File ID: {data['file_id']}
Stage: {data['stage']}
Employee: {data['employee_name']} ({data['employee_code']})
Duration: {duration_str} (Over by: {over_str})

This task requires immediate escalation and intervention. The assigned employee has significantly exceeded the time threshold.

Please review and take necessary action immediately.

Regards,
Task Assignment System
            """.strip()
    
    def _send_notification(self, notification: NotificationMessage) -> Dict[str, Any]:
        """Send notification through the specified channel"""
        try:
            if notification.channel == NotificationChannel.EMAIL:
                return self._send_email_notification(notification)
            elif notification.channel == NotificationChannel.IN_APP:
                return self._send_in_app_notification(notification)
            elif notification.channel == NotificationChannel.WEBHOOK:
                return self._send_webhook_notification(notification)
            else:
                logger.warning(f"Notification channel {notification.channel} not implemented")
                return {"success": False, "error": "Channel not implemented"}
                
        except Exception as e:
            logger.error(f"Failed to send {notification.channel} notification: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def _send_email_notification(self, notification: NotificationMessage) -> Dict[str, Any]:
        """Send email notification (placeholder implementation)"""
        # TODO: Implement actual email sending (SMTP, SendGrid, etc.)
        logger.info(f"EMAIL to {notification.recipient}: {notification.subject}")
        logger.info(f"Message: {notification.message[:200]}...")
        
        # For now, just log and return success
        return {
            "success": True,
            "channel": "email",
            "recipient": notification.recipient,
            "sent_at": datetime.utcnow().isoformat()
        }
    
    def _send_in_app_notification(self, notification: NotificationMessage) -> Dict[str, Any]:
        """Store in-app notification in database"""
        try:
            notification_doc = {
                "recipient_code": notification.recipient,
                "recipient_type": notification.recipient_type,
                "type": NotificationType.SLA_BREACH,
                "channel": notification.channel,
                "subject": notification.subject,
                "message": notification.message,
                "data": notification.data,
                "priority": notification.priority,
                "read": False,
                "created_at": datetime.utcnow(),
                "expires_at": datetime.utcnow() + timedelta(days=7)
            }
            
            # Insert into notifications collection
            self.db.notifications.insert_one(notification_doc)
            
            return {
                "success": True,
                "channel": "in_app",
                "recipient": notification.recipient,
                "notification_id": str(notification_doc["_id"]),
                "sent_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to store in-app notification: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def _send_webhook_notification(self, notification: NotificationMessage) -> Dict[str, Any]:
        """Send webhook notification (placeholder implementation)"""
        # TODO: Implement webhook calls to external systems
        logger.info(f"WEBHOOK notification: {notification.subject}")
        return {
            "success": True,
            "channel": "webhook",
            "sent_at": datetime.utcnow().isoformat()
        }
    
    def _mark_escalation_sent(self, file_id: str, stage: str) -> bool:
        """Mark escalation as sent in stage history"""
        try:
            self.db.stage_history.update_one(
                {"file_id": file_id, "stage": stage},
                {"$set": {"escalation_sent": True, "escalation_sent_at": datetime.utcnow()}}
            )
            return True
        except Exception as e:
            logger.error(f"Failed to mark escalation sent: {str(e)}")
            return False
    
    def send_stage_completion_notification(self, file_id: str, stage: str, employee_code: str) -> Dict[str, Any]:
        """Send notification when a stage is completed"""
        try:
            # Get employee details
            employee = self.db.employee.find_one({"employee_code": employee_code})
            if not employee:
                return {"success": False, "error": "Employee not found"}
            
            # Get manager for notification
            manager_code = employee.get("reporting_manager", "")
            manager = None
            if manager_code:
                manager = self.db.employee.find_one({"employee_code": manager_code})
            
            message_data = {
                "file_id": file_id,
                "stage": stage,
                "employee_name": employee.get("employee_name", "Unknown"),
                "employee_code": employee_code,
                "completed_at": datetime.utcnow().isoformat()
            }
            
            # Send to manager if available
            if manager:
                notification = NotificationMessage(
                    recipient=manager.get("employee_email", ""),
                    recipient_type="manager",
                    channel=NotificationChannel.EMAIL,
                    subject=f"Stage Completed - {file_id}",
                    message=f"""
Stage {stage} has been completed for file {file_id}.

Completed by: {employee.get('employee_name', 'Unknown')} ({employee_code})
Completed at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}

The file is ready for the next stage in the workflow.

Regards,
Task Assignment System
                    """.strip(),
                    data=message_data,
                    priority="normal"
                )
                
                result = self._send_notification(notification)
                return result
            
            return {"success": True, "message": "No manager to notify"}
            
        except Exception as e:
            logger.error(f"Failed to send stage completion notification: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def get_user_notifications(self, user_code: str, unread_only: bool = False) -> List[Dict]:
        """Get notifications for a user"""
        try:
            query = {
                "$or": [
                    {"recipient_code": user_code},
                    {"recipient_type": "all"}  # System-wide notifications
                ]
            }
            
            if unread_only:
                query["read"] = False
            
            notifications = list(self.db.notifications.find(query).sort("created_at", -1).limit(50))
            
            # Convert ObjectId to string
            for notif in notifications:
                notif["_id"] = str(notif["_id"])
            
            return notifications
            
        except Exception as e:
            logger.error(f"Failed to get user notifications: {str(e)}")
            return []
    
    def mark_notification_read(self, notification_id: str, user_code: str) -> bool:
        """Mark a notification as read"""
        try:
            result = self.db.notifications.update_one(
                {"_id": ObjectId(notification_id), "recipient_code": user_code},
                {"$set": {"read": True, "read_at": datetime.utcnow()}}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to mark notification as read: {str(e)}")
            return False


# Singleton instance
_notification_service = None

def get_notification_service() -> NotificationService:
    """Get singleton instance of notification service"""
    global _notification_service
    if _notification_service is None:
        _notification_service = NotificationService()
    return _notification_service
