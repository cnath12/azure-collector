from typing import Any, Dict, Optional
from datetime import datetime
from pydantic import BaseModel, Field

class AzureApiRequest(BaseModel):
    """Base model for Azure API requests"""
    api_version: str
    method: str
    resource_path: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    subscription_id: Optional[str] = None

class CollectionMessage(BaseModel):
    """Model for messages received from the queue"""
    message_id: str
    api_request: AzureApiRequest
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    correlation_id: Optional[str] = None

class CollectionResult(BaseModel):
    """Model for API call results"""
    message_id: str
    correlation_id: Optional[str]
    status: str
    data: Dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    error: Optional[str] = None

def validate_message(message_data: Dict[str, Any]) -> CollectionMessage:
    """
    Validate and parse a raw message into a CollectionMessage
    
    Args:
        message_data: Raw message data from the queue
        
    Returns:
        Validated CollectionMessage instance
        
    Raises:
        ValidationError: If message data is invalid
    """
    return CollectionMessage.model_validate(message_data)

def validate_result(result_data: Dict[str, Any]) -> CollectionResult:
    """
    Validate and parse API call results
    
    Args:
        result_data: Raw result data from API call
        
    Returns:
        Validated CollectionResult instance
        
    Raises:
        ValidationError: If result data is invalid
    """
    return CollectionResult.model_validate(result_data)