"""
Safe Response Format Wrapper - Zero Risk Implementation
This provides backward compatibility while enabling standard format
"""
from typing import Any, Dict, Optional, Union
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class SafeResponseWrapper:
    """
    Safe response wrapper that maintains backward compatibility
    while providing standard format capabilities
    """
    
    @staticmethod
    def wrap_existing_response(existing_response: Dict[str, Any], 
                             message: str = "Operation successful",
                             add_standard_fields: bool = False) -> Dict[str, Any]:
        """
        Wrap existing response without changing its structure
        ZERO RISK - Maintains all existing fields and structure
        """
        # If already has success field, return as-is (already wrapped)
        if "success" in existing_response:
            return existing_response
            
        # Create wrapped response that includes all original fields
        wrapped_response = existing_response.copy()
        
        # Add standard fields ONLY if requested (backward compatibility)
        if add_standard_fields:
            wrapped_response.update({
                "success": True,
                "message": message,
                "timestamp": datetime.utcnow().isoformat() + 'Z'
            })
        else:
            # For zero risk, just add success without changing structure
            wrapped_response["success"] = True
            
        return wrapped_response
    
    @staticmethod
    def standard_format(data: Any, 
                       message: str = "Operation successful",
                       metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Create standard format response
        Use this for NEW endpoints only
        """
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
    def error_response(message: str, 
                      error_code: Optional[str] = None,
                      details: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Create standardized error response
        Safe to use anywhere
        """
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
    def paginated_response(data: list, 
                           total: int, 
                           page: int = 1, 
                           limit: int = 10,
                           message: str = "Data retrieved successfully") -> Dict[str, Any]:
        """
        Create paginated response
        Use for NEW endpoints only
        """
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

# Global instance for easy access
safe_response = SafeResponseWrapper()

# Convenience functions for backward compatibility
def wrap_response(response: Dict[str, Any], message: str = "Success") -> Dict[str, Any]:
    """Wrap existing response safely"""
    return safe_response.wrap_existing_response(response, message)

def standard_response(data: Any, message: str = "Success") -> Dict[str, Any]:
    """Create standard response for new endpoints"""
    return safe_response.standard_format(data, message)

def error_response(message: str, error_code: str = None) -> Dict[str, Any]:
    """Create error response"""
    return safe_response.error_response(message, error_code)
