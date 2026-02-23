"""
Safe Response Updater - Zero Risk Endpoint Enhancement
This module provides safe ways to update existing endpoints without breaking changes
"""
from typing import Any, Dict, List, Optional
from app.utils.safe_response_wrapper import safe_response

class SafeEndpointUpdater:
    """
    Safely update existing endpoints without breaking existing functionality
    """
    
    @staticmethod
    def update_task_endpoint_response(tasks: List[Dict], 
                                    summary: Optional[Dict] = None,
                                    legacy_format: bool = True) -> Dict[str, Any]:
        """
        Safely update task endpoint response
        Maintains exact same structure as before
        """
        response = {
            "success": True,
            "tasks": tasks
        }
        
        # Add summary if provided (maintains existing pattern)
        if summary:
            response["summary"] = summary
            
        # Add standard fields only if not legacy format
        if not legacy_format:
            response.update({
                "message": f"Retrieved {len(tasks)} tasks",
                "timestamp": datetime.utcnow().isoformat() + 'Z'
            })
            
        return response
    
    @staticmethod
    def update_employee_response(employee_data: Dict[str, Any],
                               message: Optional[str] = None,
                               legacy_format: bool = True) -> Dict[str, Any]:
        """
        Safely update employee endpoint response
        Maintains exact same structure as before
        """
        response = {
            "success": True
        }
        
        # Add all existing employee fields
        response.update(employee_data)
        
        # Add message if provided
        if message:
            response["message"] = message
            
        # Add standard fields only if not legacy format
        if not legacy_format:
            response["timestamp"] = datetime.utcnow().isoformat() + 'Z'
            
        return response
    
    @staticmethod
    def update_file_response(file_data: Dict[str, Any],
                           message: Optional[str] = None,
                           legacy_format: bool = True) -> Dict[str, Any]:
        """
        Safely update file endpoint response
        Maintains exact same structure as before
        """
        response = file_data.copy()
        
        # Add success flag (safe addition)
        response["success"] = True
        
        # Add message if provided
        if message:
            response["message"] = message
            
        # Add standard fields only if not legacy format
        if not legacy_format:
            response["timestamp"] = datetime.utcnow().isoformat() + 'Z'
            
        return response
    
    @staticmethod
    def update_notification_response(message: str = "Notification sent",
                                   legacy_format: bool = True) -> Dict[str, Any]:
        """
        Safely update notification endpoint response
        Maintains exact same structure as before
        """
        response = {
            "success": True,
            "message": message
        }
        
        # Add standard fields only if not legacy format
        if not legacy_format:
            response["timestamp"] = datetime.utcnow().isoformat() + 'Z'
            
        return response

# Global instance
safe_updater = SafeEndpointUpdater()

# Import datetime for timestamp
from datetime import datetime
