from pydantic import BaseModel
from typing import Optional
import os
from functools import lru_cache
import pathlib
from dotenv import load_dotenv

# Get the project root directory
ROOT_DIR = pathlib.Path(__file__).parent.parent.parent

class Settings(BaseModel):
    """Configuration settings for the collector"""
    
    # Azure Configuration
    azure_queue_name: str
    azure_queue_connection_string: str
    azure_keyvault_url: str
    
    # Snowflake Configuration 
    snowflake_account: str
    snowflake_user: str
    snowflake_password: str
    snowflake_warehouse: str
    snowflake_database: str
    snowflake_schema: str
    snowflake_region: str = "west-us-2.azure"
    
    # Collector Configuration
    batch_size: int = 100
    batch_timeout: int = 10
    num_threads: int = 25
    max_retries: int = 3
    initial_retry_delay: int = 1
    
    # Logging Configuration
    log_level: str = "INFO"

    class Config:
        env_file = str(ROOT_DIR / ".env")
        env_file_encoding = 'utf-8'

class SettingsSingleton:
    _instance = None
    _settings = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_settings()
        return cls._instance

    def _load_settings(self):
        if self._settings is None:
            # Load .env file
            env_path = ROOT_DIR / ".env"
            load_dotenv(dotenv_path=env_path, override=True)

            # Get environment variables
            self._settings = Settings(
                azure_queue_name=os.environ.get("AZURE_QUEUE_NAME"),
                azure_queue_connection_string=os.environ.get("AZURE_QUEUE_CONNECTION_STRING"),
                azure_keyvault_url=os.environ.get("AZURE_KEYVAULT_URL"),
                snowflake_account=os.environ.get("SNOWFLAKE_ACCOUNT"),
                snowflake_user=os.environ.get("SNOWFLAKE_USER"), 
                snowflake_password=os.environ.get("SNOWFLAKE_PASSWORD"),
                snowflake_warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
                snowflake_database=os.environ.get("SNOWFLAKE_DATABASE"),
                snowflake_schema=os.environ.get("SNOWFLAKE_SCHEMA"),
                batch_size=int(os.environ.get("BATCH_SIZE", "1000")),
                batch_timeout=int(os.environ.get("BATCH_TIMEOUT", "10")),
                num_threads=int(os.environ.get("NUM_THREADS", "25")),
                max_retries=int(os.environ.get("MAX_RETRIES", "3")),
                initial_retry_delay=int(os.environ.get("INITIAL_RETRY_DELAY", "1")),
                log_level=os.environ.get("LOG_LEVEL", "INFO")
            )

    def get_settings(self) -> Settings:
        return self._settings

# Global settings instance
settings_instance = SettingsSingleton()

def get_settings() -> Settings:
    """Get the singleton settings instance"""
    return settings_instance.get_settings()

# Export Settings class and get_settings function
__all__ = ['Settings', 'get_settings']