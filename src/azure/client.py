import importlib
import json
from typing import Dict, Any, Optional, List
from azure.core.exceptions import AzureError
from datetime import datetime, UTC
import requests
from src.config.logging_config import LoggerMixin
from src.utils.retry import with_retry
from .auth import AzureAuthManager
from src.azure.message_interface import (
    ServiceType,
    APIRequest,
    CollectorMessage,
    CollectorResponse,
    HttpMethod
)

class AzureClientFactory:
    """Factory for creating Azure service clients"""
    
    _CLIENT_MAPPING = {
        ServiceType.COMPUTE: ("azure.mgmt.compute", "ComputeManagementClient"),
        ServiceType.NETWORK: ("azure.mgmt.network", "NetworkManagementClient"),
        ServiceType.STORAGE: ("azure.mgmt.storage", "StorageManagementClient"),
        ServiceType.RESOURCE: ("azure.mgmt.resource", "ResourceManagementClient"),
        ServiceType.SECURITY: ("azure.mgmt.security", "SecurityCenter"),
        ServiceType.DATABASE: ("azure.mgmt.sql", "SqlManagementClient"),
        ServiceType.CONTAINER: ("azure.mgmt.containerservice", "ContainerServiceClient"),
    }

    def __init__(self, credential, subscription_id: str):
        self.credential = credential
        self.subscription_id = subscription_id
        self._clients: Dict[str, Any] = {}

    def get_client(self, service_type: ServiceType) -> Any:
        """Get or create a client for the specified service"""
        if isinstance(service_type, ServiceType):
            service_key = service_type.value
        else:
            service_key = service_type
            
        if service_key not in self._clients:
            if service_key == ServiceType.RESOURCE.value:
                from azure.mgmt.resource import ResourceManagementClient
                client_class = ResourceManagementClient
            elif service_key == ServiceType.COMPUTE.value:
                from azure.mgmt.compute import ComputeManagementClient
                client_class = ComputeManagementClient
            elif service_key == ServiceType.NETWORK.value:
                from azure.mgmt.network import NetworkManagementClient
                client_class = NetworkManagementClient
            else:
                raise ValueError(f"Unsupported service type: {service_type}")
                
            self._clients[service_key] = client_class(
                credential=self.credential,
                subscription_id=self.subscription_id
            )
        return self._clients[service_key]

class AzureClient(LoggerMixin):
    """Client for making Azure API calls"""
    
    BASE_URL = "https://management.azure.com"
    
    def __init__(self) -> None:
        super().__init__()
        self.auth_manager = AzureAuthManager()
        self._client_factory: Optional[AzureClientFactory] = None

    async def _get_headers(self) -> Dict[str, str]:
        """Get authentication headers for API calls"""
        token = await self.auth_manager.get_token("https://management.azure.com/.default")
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    
    def _build_url(self, request: APIRequest) -> str:
        """Build full API URL from request."""
        path = request.resource_path
        # Replace placeholders with actual parameter values
        for key, value in request.parameters.items():
            path = path.replace(f"{{{key}}}", str(value))
        
        # Extract the api-version from query params (assuming it's an Enum object)
        api_version = str(request.query_params.get("api-version", "2021-04-01").value) if hasattr(request.query_params.get("api-version"), 'value') else "2021-04-01"
        
        # Build the full URL
        base_url = self.BASE_URL.rstrip("/")
        query_params = {
            "api-version": api_version,  # Use the extracted API version
            **request.query_params
        }
        
        # Build the query string
        query_string = "&".join(f"{k}={v}" for k, v in query_params.items())
        return f"{base_url}{path}?{query_string}"

    
    async def _get_client_factory(self) -> AzureClientFactory:
        """Lazy initialization of client factory"""
        if self._client_factory is None:
            credentials = await self.auth_manager.get_credentials()
            self._client_factory = AzureClientFactory(
                credential=self.auth_manager.credential,
                subscription_id=credentials['subscription_id']
            )
        return self._client_factory

    def _get_api_method(self, client: Any, request: APIRequest) -> Any:
        """Get the appropriate API method from the client"""
        # Parse the resource path to get service and method
        service, method = self._parse_resource_path(request.resource_path)
        
        # Get the method from the client
        service_client = getattr(client, service, None)
        if not service_client:
            raise ValueError(f"Service {service} not found in client")
            
        method_func = getattr(service_client, method, None)
        if not method_func:
            raise ValueError(f"Method {method} not found in service {service}")
            
        return method_func

    @with_retry(max_attempts=3, exception_types=(AzureError,))
    async def execute_api_calls(self, message: CollectorMessage) -> CollectorResponse:
        """Execute Azure API calls using HTTP requests"""
        results = []
        errors = []
        
        try:
            headers = await self._get_headers()
            any_success = False
            
            for request in message.api_requests:
                try:
                    self.log_info(
                        "Executing API call",
                        service=request.service,
                        method=request.method,
                        path=request.resource_path
                    )
                    
                    url = self._build_url(request)
                    
                    # Make HTTP request
                    response = requests.request(
                        method=request.method,
                        url=url,
                        headers={**headers, **request.headers},
                        json=request.body if request.body else None,
                        timeout=30
                    )
                    
                    # Raise for error status
                    response.raise_for_status()
                    
                    # Process response
                    result = response.json()
                    results.append(result)
                    errors.append(None)
                    any_success = True
                    
                except Exception as e:
                    self.log_error(
                        "API call failed",
                        error=e,
                        service=request.service,
                        method=request.method
                    )
                    results.append({})
                    errors.append(str(e))
            
            return CollectorResponse(
                message_id=message.message_id,
                correlation_id=message.correlation_id,
                status="success" if all(e is None for e in errors) else "partial_failure",
                data=[r for r in results if r],  # Only include successful results
                errors=[e for e in errors if e],    # Only include actual errors
                timestamp=datetime.now(UTC)
            )
            
        except Exception as e:
            self.log_error("Failed to execute API calls", error=e)
            return CollectorResponse(
                message_id=message.message_id,
                correlation_id=message.correlation_id,
                status="error",
                data=[],
                errors=[str(e)],
                timestamp=datetime.now(UTC)
            )
            
    def _parse_resource_path(self, resource_path: str) -> tuple[str, str]:
        """Parse resource path into service and method components"""
        parts = [p.lower() for p in resource_path.split('/') if p]
        
        # Check for resource groups
        if 'resourcegroups' in parts:
            return 'resource_groups', 'list'
            
        # Find Microsoft.* service
        for i, part in enumerate(parts):
            if part.startswith('microsoft.'):
                service_name = part.split('.')[1]
                if i + 1 < len(parts):
                    method_name = parts[i + 1]
                    return service_name, method_name
        
        raise ValueError(f"Invalid resource path: {resource_path}")

    # def _parse_resource_path(self, resource_path: str) -> tuple[str, str]:
    #     """Parse resource path into service and method components"""
    #     parts = [p.lower() for p in resource_path.split('/') if p]
        
    #     # Check for resource groups
    #     if 'resourcegroups' in parts:
    #         return 'resource_groups', 'list'
            
    #     # Find Microsoft.* service
    #     for i, part in enumerate(parts):
    #         if part.startswith('microsoft.'):
    #             service_name = part.split('.')[1]
    #             if i + 1 < len(parts):
    #                 method_name = parts[i + 1]
    #                 return service_name, method_name
        
    #     raise ValueError(f"Invalid resource path: {resource_path}")

    def _process_response(self, response: Any) -> Dict[str, Any]:
        """Process API response into a dictionary"""
        if hasattr(response, 'as_dict'):
            return response.as_dict()
        elif hasattr(response, '__dict__'):
            return response.__dict__
        else:
            return json.loads(json.dumps(response))