from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from typing import Optional, Dict, Any
import json
from datetime import datetime, UTC, timedelta

from src.config.logging_config import LoggerMixin
from src.utils.retry import with_retry
from src.config.settings import get_settings
from src.utils.json_utils import to_json

class AzureAuthManager(LoggerMixin):
    """Manages Azure authentication and credentials"""
    
    def __init__(self) -> None:
        super().__init__()
        self.settings = get_settings()
        self._credential: Optional[DefaultAzureCredential] = None
        self._secret_client: Optional[SecretClient] = None
        self._last_refresh: Optional[datetime] = None
        self._cached_credentials: Optional[Dict[str, Any]] = None

    @property
    def credential(self) -> DefaultAzureCredential:
        """Lazy initialization of Azure credential"""
        if self._credential is None:
            self.log_info("Initializing Azure credential")
            self._credential = DefaultAzureCredential()
        return self._credential

    @property
    def secret_client(self) -> SecretClient:
        """Lazy initialization of Key Vault secret client"""
        if self._secret_client is None:
            self.log_info("Initializing Key Vault secret client")
            self._secret_client = SecretClient(
                vault_url=self.settings.azure_keyvault_url,
                credential=self.credential
            )
        return self._secret_client

    @with_retry(max_attempts=3, exception_types=(Exception,))
    async def get_secret(self, secret_name: str) -> str:
        """
        Retrieve a secret from Azure Key Vault
        
        Args:
            secret_name: Name of the secret to retrieve
            
        Returns:
            Secret value as string
            
        Raises:
            Exception: If secret retrieval fails
        """
        try:
            self.log_info(f"Retrieving secret: {secret_name}")
            secret = self.secret_client.get_secret(secret_name)
            return secret.value
        except Exception as e:
            self.log_error(
                f"Failed to retrieve secret: {secret_name}",
                error=e,
                secret_name=secret_name
            )
            raise

    @with_retry(max_attempts=3, exception_types=(Exception,))
    async def get_credentials(self, force_refresh: bool = False) -> Dict[str, str]:
        """
        Retrieve Azure credentials from Key Vault
        
        Args:
            force_refresh: Force refresh of credentials even if cached
            
        Returns:
            Dictionary containing Azure credentials
            
        Raises:
            Exception: If credential retrieval fails
        """
        try:
            # Check if we need to refresh
            if not force_refresh and self._cached_credentials and self._last_refresh:
                # Check if credentials are still valid (less than 6 hours old)
                if datetime.now(UTC) - self._last_refresh < timedelta(hours=6):
                    return self._cached_credentials

            self.log_info("Retrieving Azure credentials")
            creds_json = await self.get_secret("azure-collector-creds")
            
            # Parse and validate credentials
            credentials = json.loads(creds_json)
            required_fields = ["subscription_id", "tenant_id"]
            
            missing_fields = [field for field in required_fields if field not in credentials]
            if missing_fields:
                raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
            
            # Update cache
            self._cached_credentials = credentials
            self._last_refresh = datetime.now(UTC)
            
            self.log_info("Successfully retrieved credentials")
            return credentials
            
        except Exception as e:
            self.log_error("Failed to retrieve Azure credentials", error=e)
            raise

    async def refresh_credentials(self) -> None:
        """
        Refresh Azure credentials
        
        Raises:
            Exception: If credential refresh fails
        """
        try:
            self.log_info("Refreshing Azure credentials")
            
            # Create new credential instance
            self._credential = DefaultAzureCredential()
            
            # Force credential refresh
            await self.get_credentials(force_refresh=True)
            
            self.log_info(
                "Successfully refreshed Azure credentials",
                refresh_time=datetime.now(UTC).isoformat()
            )
            
        except Exception as e:
            self.log_error("Failed to refresh Azure credentials", error=e)
            raise

    async def validate_credentials(self) -> bool:
        """
        Validate current credentials are working
        
        Returns:
            True if credentials are valid, False otherwise
        """
        try:
            # Try to get credentials
            credentials = await self.get_credentials()
            
            # Try to use credentials
            self.secret_client.get_secret("test-secret-name")
            
            return True
        except Exception as e:
            self.log_error("Credential validation failed", error=e)
            return False
        
    async def get_token(self, scope: str) -> str:
        """Get an authentication token for the given scope"""
        token = await self.credential.get_token(scope)
        return token.token