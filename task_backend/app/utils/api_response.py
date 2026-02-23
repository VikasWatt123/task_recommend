"""
Standard response format utilities for API consistency
"""
from typing import Any, Dict, Optional, Union
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class APIResponse:
    """Standard API response format for consistency"""
    
    @staticmethod
    def success(
        data: Any = None, 
        message: str = "Operation successful", 
        metadata: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Create a standardized success response"""
        response = {
            "success": True,
            "message": message,
            "data": data,
            "timestamp": datetime.utcnow().isoformat() + 'Z'
        }
        
        if metadata:
            response["metadata"] = metadata
            
        return response
    
    @staticmethod
    def error(
        message: str, 
        error_code: Optional[str] = None,
        details: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Create a standardized error response"""
        response = {
            "success": False,
            "message": message,
            "timestamp": datetime.utcnow().isoformat() + 'Z'
        }
        
        if error_code:
            response["error_code"] = error_code
            
        if details:
            response["details"] = details
            
        return response
    
    @staticmethod
    def paginated(
        data: list, 
        total: int, 
        page: int = 1, 
        limit: int = 10,
        message: str = "Data retrieved successfully"
    ) -> Dict[str, Any]:
        """Create a standardized paginated response"""
        return {
            "success": True,
            "message": message,
            "data": data,
            "pagination": {
                "total": total,
                "page": page,
                "limit": limit,
                "pages": (total + limit - 1) // limit,
                "has_next": page * limit < total,
                "has_prev": page > 1
            },
            "timestamp": datetime.utcnow().isoformat() + 'Z'
        }
    
    @staticmethod
    def created(
        data: Any = None, 
        message: str = "Resource created successfully",
        resource_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a standardized resource creation response"""
        response = APIResponse.success(data, message)
        
        if resource_id:
            response["resource_id"] = resource_id
            
        return response
    
    @staticmethod
    def updated(
        data: Any = None, 
        message: str = "Resource updated successfully",
        changes: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Create a standardized resource update response"""
        response = APIResponse.success(data, message)
        
        if changes:
            response["changes"] = changes
            
        return response
    
    @staticmethod
    def deleted(
        message: str = "Resource deleted successfully",
        resource_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a standardized resource deletion response"""
        response = {
            "success": True,
            "message": message,
            "timestamp": datetime.utcnow().isoformat() + 'Z'
        }
        
        if resource_id:
            response["resource_id"] = resource_id
            
        return response

def format_existing_response(existing_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert existing response to standard format
    This helps migrate old endpoints to new format
    """
    # If already in standard format, return as-is
    if "success" in existing_response:
        return existing_response
    
    # Convert old format to standard
    return APIResponse.success(
        data=existing_response,
        message="Data retrieved successfully"
    )

def wrap_with_metadata(data: Any, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Add metadata to existing response"""
    if isinstance(data, dict) and "success" in data:
        data["metadata"] = metadata
        return data
    else:
        return APIResponse.success(data, metadata=metadata)
