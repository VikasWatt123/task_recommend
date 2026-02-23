"""
Validation utilities for task assignment system
Provides validation for addresses, ZIP codes, file IDs, and business rules
"""
import re
import logging
from typing import Optional, Dict, Any, Tuple
from pydantic import BaseModel, validator

logger = logging.getLogger(__name__)


class ValidationResult(BaseModel):
    """Result of a validation check"""
    is_valid: bool
    error_message: Optional[str] = None
    extracted_data: Optional[Dict[str, Any]] = None
    warnings: list[str] = []


class AddressValidator:
    """Validates and extracts information from addresses"""
    
    # Common US address patterns
    ZIP_PATTERN = r'\b(\d{5})(?:-\d{4})?\b'
    STATE_ABBREV_PATTERN = r'\b([A-Z]{2})\b'
    
    @staticmethod
    def validate_address(address: str) -> ValidationResult:
        """
        Validate address and extract ZIP code
        
        Args:
            address: Full address string
            
        Returns:
            ValidationResult with extracted ZIP and state info
        """
        if not address or not address.strip():
            return ValidationResult(
                is_valid=False,
                error_message="Address cannot be empty"
            )
        
        address = address.strip()
        
        # Check minimum length
        if len(address) < 10:
            return ValidationResult(
                is_valid=False,
                error_message="Address is too short. Please provide a complete address including ZIP code."
            )
        
        # Extract ZIP code
        zip_match = re.search(AddressValidator.ZIP_PATTERN, address)
        if not zip_match:
            return ValidationResult(
                is_valid=False,
                error_message="No valid ZIP code found in address. Please include a 5-digit ZIP code."
            )
        
        zip_code = zip_match.group(1)
        
        # Extract state abbreviation (if present)
        state_match = re.search(AddressValidator.STATE_ABBREV_PATTERN, address)
        state_abbrev = state_match.group(1) if state_match else None
        
        # Validate ZIP code format
        if not zip_code.isdigit() or len(zip_code) != 5:
            return ValidationResult(
                is_valid=False,
                error_message=f"Invalid ZIP code format: {zip_code}. Must be 5 digits."
            )
        
        # Check if ZIP is in valid range (00001-99999)
        zip_int = int(zip_code)
        if zip_int < 1 or zip_int > 99999:
            return ValidationResult(
                is_valid=False,
                error_message=f"ZIP code {zip_code} is out of valid range."
            )
        
        warnings = []
        if not state_abbrev:
            warnings.append("State abbreviation not found in address. Will rely on ZIP code mapping.")
        
        return ValidationResult(
            is_valid=True,
            extracted_data={
                "zip_code": zip_code,
                "state_abbrev": state_abbrev,
                "full_address": address
            },
            warnings=warnings
        )
    
    @staticmethod
    def validate_zip_code(zip_code: str) -> ValidationResult:
        """
        Validate a standalone ZIP code
        
        Args:
            zip_code: 5-digit ZIP code string
            
        Returns:
            ValidationResult
        """
        if not zip_code:
            return ValidationResult(
                is_valid=False,
                error_message="ZIP code cannot be empty"
            )
        
        zip_code = zip_code.strip()
        
        if not zip_code.isdigit():
            return ValidationResult(
                is_valid=False,
                error_message=f"ZIP code must contain only digits: {zip_code}"
            )
        
        if len(zip_code) != 5:
            return ValidationResult(
                is_valid=False,
                error_message=f"ZIP code must be exactly 5 digits: {zip_code}"
            )
        
        zip_int = int(zip_code)
        if zip_int < 1 or zip_int > 99999:
            return ValidationResult(
                is_valid=False,
                error_message=f"ZIP code {zip_code} is out of valid range (00001-99999)"
            )
        
        return ValidationResult(
            is_valid=True,
            extracted_data={"zip_code": zip_code}
        )


class FileIdValidator:
    """Validates file IDs"""
    
    # Common file ID patterns
    FILE_ID_PATTERNS = [
        r'^PF-\d{8}-[A-Z0-9]{8}$',  # PF-20240219-ABC12345
        r'^FILE_\d+$',               # FILE_12345
        r'^\d+$',                    # 12345 (MySQL ID)
    ]
    
    @staticmethod
    def validate_file_id(file_id: str) -> ValidationResult:
        """
        Validate file ID format
        
        Args:
            file_id: File ID string
            
        Returns:
            ValidationResult
        """
        if not file_id or not file_id.strip():
            return ValidationResult(
                is_valid=False,
                error_message="File ID cannot be empty"
            )
        
        file_id = file_id.strip()
        
        # Check if matches any known pattern
        matches_pattern = any(
            re.match(pattern, file_id) 
            for pattern in FileIdValidator.FILE_ID_PATTERNS
        )
        
        if not matches_pattern:
            return ValidationResult(
                is_valid=False,
                error_message=f"Invalid file ID format: {file_id}. Expected formats: PF-YYYYMMDD-XXXXXXXX, FILE_XXXXX, or numeric ID.",
                warnings=["File ID format not recognized but will attempt to process"]
            )
        
        return ValidationResult(
            is_valid=True,
            extracted_data={"file_id": file_id}
        )


class TaskDescriptionValidator:
    """Validates task descriptions"""
    
    MIN_LENGTH = 10
    MAX_LENGTH = 5000
    
    @staticmethod
    def validate_description(description: str) -> ValidationResult:
        """
        Validate task description
        
        Args:
            description: Task description string
            
        Returns:
            ValidationResult
        """
        if not description or not description.strip():
            return ValidationResult(
                is_valid=False,
                error_message="Task description cannot be empty"
            )
        
        description = description.strip()
        
        if len(description) < TaskDescriptionValidator.MIN_LENGTH:
            return ValidationResult(
                is_valid=False,
                error_message=f"Task description is too short (minimum {TaskDescriptionValidator.MIN_LENGTH} characters). Please provide more details."
            )
        
        if len(description) > TaskDescriptionValidator.MAX_LENGTH:
            return ValidationResult(
                is_valid=False,
                error_message=f"Task description is too long (maximum {TaskDescriptionValidator.MAX_LENGTH} characters)."
            )
        
        warnings = []
        
        # Check for common issues
        if description.lower() == description:
            warnings.append("Task description is all lowercase. Consider proper capitalization.")
        
        if len(description.split()) < 3:
            warnings.append("Task description is very brief. More details may improve recommendations.")
        
        return ValidationResult(
            is_valid=True,
            extracted_data={"description": description},
            warnings=warnings
        )


class BusinessRuleValidator:
    """Validates business rules for task assignment"""
    
    @staticmethod
    def validate_task_assignment_request(
        task_description: str,
        address: Optional[str] = None,
        file_id: Optional[str] = None,
        team_lead_code: Optional[str] = None
    ) -> ValidationResult:
        """
        Validate complete task assignment request
        
        Args:
            task_description: Task description
            address: Optional address
            file_id: Optional file ID
            team_lead_code: Optional team lead code
            
        Returns:
            ValidationResult with comprehensive validation
        """
        errors = []
        warnings = []
        extracted_data = {}
        
        # Validate task description (required)
        desc_result = TaskDescriptionValidator.validate_description(task_description)
        if not desc_result.is_valid:
            errors.append(desc_result.error_message)
        else:
            extracted_data.update(desc_result.extracted_data or {})
            warnings.extend(desc_result.warnings)
        
        # Validate address (if provided)
        if address:
            addr_result = AddressValidator.validate_address(address)
            if not addr_result.is_valid:
                warnings.append(f"Address validation: {addr_result.error_message}")
            else:
                extracted_data.update(addr_result.extracted_data or {})
                warnings.extend(addr_result.warnings)
        
        # Validate file_id (if provided)
        if file_id:
            file_result = FileIdValidator.validate_file_id(file_id)
            if not file_result.is_valid:
                warnings.append(f"File ID validation: {file_result.error_message}")
            else:
                extracted_data.update(file_result.extracted_data or {})
                warnings.extend(file_result.warnings)
        
        # Business rule: At least one of address or file_id should be provided for better recommendations
        if not address and not file_id and not team_lead_code:
            warnings.append(
                "No address, file ID, or team lead provided. "
                "Recommendations will use default team lead. "
                "Provide an address for location-based team lead selection."
            )
        
        # Business rule: If both address and file_id provided, file_id takes precedence
        if address and file_id:
            warnings.append(
                "Both address and file ID provided. File ID will take precedence for team lead selection."
            )
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            error_message="; ".join(errors) if errors else None,
            extracted_data=extracted_data,
            warnings=warnings
        )
    
    @staticmethod
    def validate_zip_to_state_mapping(
        zip_code: str,
        state_ranges: Dict[str, Dict[str, str]]
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Validate ZIP code and map to state
        
        Args:
            zip_code: 5-digit ZIP code
            state_ranges: Dictionary of state ZIP ranges
            
        Returns:
            Tuple of (state_code, state_name) or (None, None) if not found
        """
        try:
            zip_int = int(zip_code)
            
            for state_name, state_info in state_ranges.items():
                zip_min = int(state_info["zip_min"])
                zip_max = int(state_info["zip_max"])
                
                if zip_min <= zip_int <= zip_max:
                    state_code = state_info["code"]
                    logger.info(f"ZIP {zip_code} mapped to {state_name} ({state_code})")
                    return state_code, state_name
            
            logger.warning(f"ZIP code {zip_code} not found in any state range")
            return None, None
            
        except (ValueError, KeyError) as e:
            logger.error(f"Error mapping ZIP {zip_code} to state: {e}")
            return None, None


def validate_and_extract_address_info(address: str) -> Dict[str, Any]:
    """
    Convenience function to validate address and extract all info
    
    Args:
        address: Full address string
        
    Returns:
        Dictionary with validation results and extracted data
    """
    result = AddressValidator.validate_address(address)
    
    return {
        "is_valid": result.is_valid,
        "error_message": result.error_message,
        "zip_code": result.extracted_data.get("zip_code") if result.extracted_data else None,
        "state_abbrev": result.extracted_data.get("state_abbrev") if result.extracted_data else None,
        "warnings": result.warnings
    }
