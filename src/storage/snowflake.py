import snowflake.connector
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from typing import List, Dict, Any, Optional
import json
from datetime import datetime, UTC

from src.config.logging_config import LoggerMixin
from src.utils.retry import with_retry
from src.config.settings import get_settings

class SnowflakeManager(LoggerMixin):
    """Manages Snowflake operations with explicit initialization sequence"""
    
    def __init__(self) -> None:
        super().__init__()
        self.settings = get_settings()
        self._connection: Optional[SnowflakeConnection] = None
        self._is_initialized: bool = False

    def _create_connection(self) -> SnowflakeConnection:
        """Create initial connection to Snowflake without any context"""
        self.log_info("Creating base Snowflake connection")
        return snowflake.connector.connect(
            account=f"{self.settings.snowflake_account.split('.')[0]}.west-us-2.azure",
            user=self.settings.snowflake_user,
            password=self.settings.snowflake_password
        )

    @property
    def connection(self) -> SnowflakeConnection:
        """Get the Snowflake connection, ensuring it's initialized"""
        if not self._is_initialized:
            raise RuntimeError("Must call initialize() before using connection")
            
        if self._connection is None or self._connection.is_closed():
            self._is_initialized = False
            raise RuntimeError("Connection lost - must reinitialize")
            
        return self._connection

    def _get_cursor(self) -> SnowflakeCursor:
        """Get a Snowflake cursor from initialized connection"""
        return self.connection.cursor()

    def _create_object(self, cursor: SnowflakeCursor, object_type: str, object_name: str) -> None:
        """Create a Snowflake object if it doesn't exist"""
        create_commands = {
            'DATABASE': f"CREATE DATABASE IF NOT EXISTS {object_name}",
            'WAREHOUSE': f"""
                CREATE WAREHOUSE IF NOT EXISTS {object_name}
                WITH WAREHOUSE_SIZE = 'XSMALL'
                AUTO_SUSPEND = 300
                AUTO_RESUME = TRUE
            """,
            'SCHEMA': f"CREATE SCHEMA IF NOT EXISTS {self.settings.snowflake_database}.{object_name}",
            'TABLE': self._get_create_table_sql()
        }
        
        command = create_commands.get(object_type.upper())
        if not command:
            raise ValueError(f"Unsupported object type: {object_type}")
            
        self.log_info(f"Creating {object_type.lower()}: {object_name}")
        cursor.execute(command)

    def _get_create_table_sql(self) -> str:
        """Get SQL for creating the table if it doesn't exist"""
        return """
        CREATE TABLE IF NOT EXISTS azure_config_data (
            message_id VARCHAR NOT NULL,
            correlation_id VARCHAR,
            status VARCHAR NOT NULL,
            data VARCHAR,
            errors VARCHAR,
            timestamp TIMESTAMP_NTZ,
            request_index INTEGER,
            PRIMARY KEY (message_id, request_index)
        )
        """

    def _validate_and_create_objects(self, cursor: SnowflakeCursor) -> None:
        """Create required Snowflake objects in correct order"""
        try:
            # Create database and set context
            self._create_object(cursor, 'DATABASE', self.settings.snowflake_database)
            cursor.execute(f"USE DATABASE {self.settings.snowflake_database}")
            
            # Create warehouse and set context
            self._create_object(cursor, 'WAREHOUSE', self.settings.snowflake_warehouse)
            cursor.execute(f"USE WAREHOUSE {self.settings.snowflake_warehouse}")
            
            # Create schema with fully qualified name and set context
            self._create_object(cursor, 'SCHEMA', self.settings.snowflake_schema)
            cursor.execute(f"USE SCHEMA {self.settings.snowflake_schema}")
            
            # Create table in current schema
            self._create_object(cursor, 'TABLE', 'AZURE_CONFIG_DATA')
            
            self.log_info("Successfully created Snowflake objects")
        except Exception as e:
            self.log_error("Failed to create Snowflake objects", error=e)
            raise

    async def initialize(self) -> None:
        """Initialize Snowflake connection in correct sequence"""
        if self._is_initialized:
            return

        try:
            self.log_info("Starting Snowflake initialization")
            self._connection = self._create_connection()
            
            cursor = self._connection.cursor()
            try:
                self._validate_and_create_objects(cursor)
                self._is_initialized = True
                self.log_info("Snowflake initialization complete")
            finally:
                cursor.close()
                
        except Exception as e:
            self.log_error("Failed to initialize Snowflake", error=e)
            if self._connection:
                self._connection.close()
                self._connection = None
            self._is_initialized = False
            raise

    def _safe_serialize(self, obj: Any) -> Optional[str]:
        """Safely serialize data, handling special cases and Azure response types"""
        if obj is None:
            return None
        
        if isinstance(obj, dict):
            cleaned_dict = {}
            for k, v in obj.items():
                if k.startswith('_'):
                    continue
                if isinstance(v, (dict, list)):
                    cleaned_dict[k] = self._safe_serialize(v)
                elif not callable(v):
                    cleaned_dict[k] = v
            return json.dumps(cleaned_dict)
        elif isinstance(obj, list):
            return json.dumps([self._safe_serialize(item) for item in obj if not callable(item)])
        else:
            return json.dumps(str(obj))

    @with_retry(max_attempts=3)
    async def write_batch(self, results: List[Dict[str, Any]]) -> None:
        """Write a batch of results to Snowflake"""
        if not results:
            return

        # Ensure initialization
        if not self._is_initialized:
            await self.initialize()

        try:
            self.log_info(f"Writing batch of {len(results)} results to Snowflake")
            cursor = self._get_cursor()
            
            try:
                rows = []
                for result in results:
                    try:
                        rows.append((
                            result['message_id'],
                            result['correlation_id'],
                            result['status'],
                            self._safe_serialize(result.get('data')),
                            self._safe_serialize(result.get('errors')),
                            result['timestamp'],
                            result['request_index']
                        ))
                        self.log_info(f"Successfully serialized result {result['message_id']}")
                    except Exception as e:
                        self.log_error(
                            "Failed to serialize result",
                            error=str(e),
                            message_id=result.get('message_id'),
                            data_sample=str(result.get('data'))[:200]
                        )
                        raise

                cursor.executemany(
                    """
                    INSERT INTO azure_config_data (
                        message_id, correlation_id, status, data,
                        errors, timestamp, request_index
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    rows
                )

                self.log_info(f"Successfully wrote {len(results)} results to Snowflake")
            finally:
                cursor.close()

        except Exception as e:
            self.log_error("Failed to write batch to Snowflake", error=e)
            if results:
                self.log_error(
                    "Data that caused error",
                    sample_data=str(results[0])[:200],
                    data_type=str(type(results[0]))
                )
            raise

    async def close(self) -> None:
        """Close the Snowflake connection"""
        try:
            if self._connection and not self._connection.is_closed():
                self._connection.close()
                self._connection = None
            self._is_initialized = False
            self.log_info("Closed Snowflake connections")
        except Exception as e:
            self.log_error("Error closing Snowflake connections", error=e)
            raise