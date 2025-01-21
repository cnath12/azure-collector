import asyncio
from typing import List, Tuple, Optional
from datetime import datetime, UTC
from azure.storage.queue import QueueMessage

from src.config.logging_config import LoggerMixin
from src.storage.snowflake import SnowflakeManager
from src.storage.queue import QueueManager
from src.azure.message_interface import CollectorMessage, CollectorResponse
from src.config.settings import get_settings
from src.utils.json_utils import to_json

class BatchManager(LoggerMixin):
    """Manages batching of results and writing to Snowflake"""
    
    def __init__(self) -> None:
        super().__init__()
        self.settings = get_settings()
        self.snowflake_manager = SnowflakeManager()
        self.queue_manager = QueueManager()
        
        # Batch state
        self.current_batch: List[Tuple[CollectorResponse, QueueMessage]] = []
        self.last_flush_time: datetime = datetime.now(UTC)
        self._batch_lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    async def add_to_batch(
        self,
        results: List[Tuple[CollectorResponse, QueueMessage]]
    ) -> None:
        """Add results to the current batch"""
        if not results:
            return

        async with self._batch_lock:
            self.log_info(
                "Adding results to batch",
                num_results=len(results),
                current_batch_size=len(self.current_batch)
            )
            
            self.current_batch.extend(results)
            
            # Force flush if we've hit batch size
            if len(self.current_batch) >= self.settings.batch_size:
                self.log_info("Batch size limit reached, flushing")
                await self.flush()

    async def _should_flush(self) -> bool:
        """Check if the batch should be flushed"""
        if len(self.current_batch) >= self.settings.batch_size:
            return True

        time_since_flush = datetime.now(UTC) - self.last_flush_time
        return time_since_flush.total_seconds() >= self.settings.batch_timeout

    async def flush(self) -> None:
        """Flush the current batch to Snowflake"""
        if not self.current_batch:
            return

        try:
            batch_size = len(self.current_batch)
            self.log_info(f"Flushing batch of {batch_size} results")
            
            results, queue_messages = zip(*self.current_batch)
            
            # Prepare data for Snowflake
            snowflake_data = []
            for result in results:
                for i, data in enumerate(result.data):
                    snowflake_data.append({
                        "message_id": result.message_id,
                        "correlation_id": result.correlation_id,
                        "status": result.status,
                        "data": data,
                        "errors": result.errors[i] if result.errors and i < len(result.errors) else None,
                        "timestamp": result.timestamp,
                        "request_index": i
                    })
            
            # Write to Snowflake
            self.log_info(f"Writing {len(snowflake_data)} records to Snowflake")
            await self.snowflake_manager.initialize()
            await self.snowflake_manager.write_batch(snowflake_data)
            
            # Delete messages from queue
            for msg in queue_messages:
                await self.queue_manager.delete_message(msg)
            
            self.current_batch = []
            self.last_flush_time = datetime.now(UTC)
            
        except Exception as e:
            self.log_error("Failed to flush batch", error=e)
            raise

    async def start_periodic_flush(self) -> None:
        """Start periodic flush of batch"""
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(self.settings.batch_timeout)
                if not self._stop_event.is_set() and await self._should_flush():
                    await self.flush()
            except Exception as e:
                self.log_error("Error in periodic flush", error=e)

    async def close(self) -> None:
        """Close the batch manager and flush any remaining items"""
        try:
            self._stop_event.set()
            await self.flush()
            await self.snowflake_manager.close()
        except Exception as e:
            self.log_error("Error closing batch manager", error=e)
