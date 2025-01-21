from enum import Enum
from typing import Dict, Any, Optional, List
from datetime import datetime, UTC
from pydantic import BaseModel, Field

class ServiceType(str, Enum):
    """Azure service types"""
    COMPUTE = "compute"
    NETWORK = "network"
    STORAGE = "storage"
    RESOURCE = "resource"
    SECURITY = "security"
    DATABASE = "database"
    CONTAINER = "container"

class APIVersion(str, Enum):
    """Azure API versions"""
    COMPUTE_2023 = "2023-07-01"
    NETWORK_2023 = "2023-05-01"
    STORAGE_2023 = "2023-01-01"
    RESOURCE_2023 = "2023-07-01"
    SECURITY_2023 = "2023-01-01"
    DATABASE_2023 = "2023-05-01"
    CONTAINER_2023 = "2023-06-01"

class HttpMethod(str, Enum):
    """HTTP methods"""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"

class APIRequest(BaseModel):
    """Azure API request configuration"""
    service: ServiceType
    api_version: APIVersion
    method: HttpMethod
    resource_path: str = Field(..., description="e.g., /subscriptions/{subscriptionId}/resourceGroups")
    parameters: Dict[str, str] = Field(default_factory=dict)
    query_params: Dict[str, str] = Field(default_factory=dict)
    headers: Dict[str, str] = Field(default_factory=dict)
    body: Optional[Dict[str, Any]] = None

class CollectorMessage(BaseModel):
    """Main message structure sent to collector"""
    message_id: str
    correlation_id: Optional[str] = None
    api_requests: List[APIRequest]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: Optional[Dict[str, Any]] = None

class CollectorResponse(BaseModel):
    """Response structure for collector"""
    message_id: str
    correlation_id: Optional[str]
    status: str
    data: List[Dict[str, Any]]
    errors: List[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def dict(self, *args, **kwargs):
        return super().dict(*args, **kwargs)

# Example message templates
def get_vm_info_message(subscription_id: str, resource_group: str, vm_name: str) -> CollectorMessage:
    """Get VM and its network interface information"""
    return CollectorMessage(
        message_id=f"msg-vm-{vm_name}",
        correlation_id=f"corr-{datetime.now(UTC).timestamp()}",
        api_requests=[
            APIRequest(
                service=ServiceType.COMPUTE,
                api_version=APIVersion.COMPUTE_2023,
                method=HttpMethod.GET,
                resource_path="/subscriptions/{subscriptionId}/resourceGroups/{resourceGroup}"
                          "/providers/Microsoft.Compute/virtualMachines/{vmName}",
                parameters={
                    "subscriptionId": subscription_id,
                    "resourceGroup": resource_group,
                    "vmName": vm_name
                }
            ),
            APIRequest(
                service=ServiceType.NETWORK,
                api_version=APIVersion.NETWORK_2023,
                method=HttpMethod.GET,
                resource_path="/subscriptions/{subscriptionId}/resourceGroups/{resourceGroup}"
                          "/providers/Microsoft.Network/networkInterfaces",
                parameters={
                    "subscriptionId": subscription_id,
                    "resourceGroup": resource_group
                },
                query_params={
                    "$filter": f"virtualMachine/id eq '{vm_name}'"
                }
            )
        ]
    )

def get_storage_security_message(subscription_id: str, resource_group: str) -> CollectorMessage:
    """Get storage accounts and their security settings"""
    return CollectorMessage(
        message_id=f"msg-storage-{resource_group}",
        api_requests=[
            APIRequest(
                service=ServiceType.STORAGE,
                api_version=APIVersion.STORAGE_2023,
                method=HttpMethod.GET,
                resource_path="/subscriptions/{subscriptionId}/resourceGroups/{resourceGroup}"
                          "/providers/Microsoft.Storage/storageAccounts",
                parameters={
                    "subscriptionId": subscription_id,
                    "resourceGroup": resource_group
                }
            ),
            APIRequest(
                service=ServiceType.SECURITY,
                api_version=APIVersion.SECURITY_2023,
                method=HttpMethod.GET,
                resource_path="/subscriptions/{subscriptionId}/resourceGroups/{resourceGroup}"
                          "/providers/Microsoft.Security/assessments",
                parameters={
                    "subscriptionId": subscription_id,
                    "resourceGroup": resource_group
                },
                query_params={
                    "$filter": "resourceType eq 'Microsoft.Storage/storageAccounts'"
                }
            )
        ]
    )