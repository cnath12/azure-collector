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
    batch_size: int = 32
    batch_timeout: int = 10
    num_threads: int = 25
    max_retries: int = 3
    initial_retry_delay: int = 1
    
    
    # Logging Configuration
    log_level: str = "INFO"

    class Config:
        env_file = str(ROOT_DIR / ".env")
        env_file_encoding = 'utf-8'

@lru_cache()
def get_settings() -> Settings:
    """Creates and returns a cached Settings instance."""
    # Load .env file explicitly
    env_path = ROOT_DIR / ".env"
    load_dotenv(dotenv_path=env_path, override=True)
    
    # Print debug info
    print(f"Loading settings from: {env_path}")
    print(f"Found .env file: {env_path.exists()}")
    
    # Get values and print them for debugging
    azure_queue_name = os.environ.get("AZURE_QUEUE_NAME")
    azure_queue_connection_string = os.environ.get("AZURE_QUEUE_CONNECTION_STRING")
    
    print(f"Raw env values:")
    print(f"Queue Name: {azure_queue_name}")
    print(f"Connection String length: {len(azure_queue_connection_string) if azure_queue_connection_string else 0}")
    
    settings = Settings(
        azure_queue_name=azure_queue_name,
        azure_queue_connection_string=azure_queue_connection_string,
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
    
    print(f"Final settings values:")
    print(f"Queue Name: {settings.azure_queue_name}")
    print(f"Connection String length: {len(settings.azure_queue_connection_string)}")
    
    return settings

# Export Settings class and get_settings function
__all__ = ['Settings', 'get_settings']