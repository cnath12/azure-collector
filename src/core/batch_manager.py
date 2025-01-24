import threading
from typing import List, Tuple, Optional
from datetime import datetime, UTC
from azure.storage.queue import QueueMessage
import time
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
        self._batch_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._flush_thread: Optional[threading.Thread] = None

    def add_to_batch(
        self,
        results: List[Tuple[CollectorResponse, QueueMessage]]
    ) -> None:
        if not results:
            return

        with self._batch_lock:
            self.log_info(
                "Adding results to batch",
                num_results=len(results),
                current_batch_size=len(self.current_batch)
            )
            
            self.current_batch.extend(results)
            
            if len(self.current_batch) >= self.settings.batch_size:
                self.log_info("Batch size limit reached, flushing")
                self.flush()

    def _should_flush(self) -> bool:
        if len(self.current_batch) >= self.settings.batch_size:
            return True

        time_since_flush = datetime.now(UTC) - self.last_flush_time
        return time_since_flush.total_seconds() >= self.settings.batch_timeout

    def _flush_batch(self) -> None:
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
        self.snowflake_manager.initialize()
        self.snowflake_manager.write_batch(snowflake_data)
        
        # Delete messages from queue
        for msg in queue_messages:
            self.queue_manager.delete_message(msg)

    def flush(self) -> None:
        with self._batch_lock:
            if not self.current_batch:
                return
            try:
                self._flush_batch()
            except Exception as e:
                self.log_error("Failed to flush batch", error=e)
                raise
            finally:
                self.current_batch = []
                self.last_flush_time = datetime.now(UTC)

    def start_periodic_flush(self) -> None:
        def periodic_flush():
            while not self._stop_event.is_set():
                try:
                    time.sleep(self.settings.batch_timeout)
                    if not self._stop_event.is_set() and self._should_flush():
                        self.flush()
                except Exception as e:
                    self.log_error("Error in periodic flush", error=e)

        self._flush_thread = threading.Thread(target=periodic_flush)
        self._flush_thread.daemon = True
        self._flush_thread.start()

    def close(self) -> None:
        try:
            self._stop_event.set()
            if self._flush_thread:
                self._flush_thread.join(timeout=30)
            self.flush()
            self.snowflake_manager.close()
        except Exception as e:
            self.log_error("Error closing batch manager", error=e)