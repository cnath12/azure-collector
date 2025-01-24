import threading
import time
from typing import Optional
from datetime import datetime, UTC
from concurrent.futures import ThreadPoolExecutor

from src.config.logging_config import LoggerMixin
from src.storage.queue import QueueManager
from src.core.message_processor import MessageProcessor
from src.core.batch_manager import BatchManager
from src.config.settings import get_settings
from src.utils.validation import CollectionResult
from src.azure.client import AzureClient

class Collector(LoggerMixin):
    """Main collector class orchestrating the collection process"""
    
    def __init__(self) -> None:
        super().__init__()
        self.settings = get_settings()
        self.queue_manager = QueueManager()
        self.message_processor = MessageProcessor()
        self.batch_manager = BatchManager()
        self.azure_client = AzureClient()
        self._stop_event = threading.Event()
        self.thread_pool = ThreadPoolExecutor(max_workers=self.settings.num_threads)

    def start(self) -> None:
        """Start the collector"""
        self.log_info("Starting collector")
        
        try:
            # Start periodic flush in a separate thread
            self.batch_manager.start_periodic_flush()
            
            # Process messages until stopped
            self.process_messages()
                
        except Exception as e:
            self.log_error("Fatal error in collector", error=e)
            raise
        finally:
            self.shutdown()

    def process_messages(self) -> None:
        """Process messages from the queue"""
        while not self._stop_event.is_set():
            try:
                messages = self.queue_manager.receive_messages(max_messages=32)
                
                if not messages:
                    time.sleep(5)
                    continue

                self.log_info(f"Processing batch of {len(messages)} messages")
                
                # Process messages using thread pool
                futures = []
                for message, raw_msg in messages:
                    future = self.thread_pool.submit(
                        self.message_processor.process_message,
                        message,
                        raw_msg
                    )
                    futures.append((future, raw_msg))

                # Collect results
                results = []
                for (future, raw_msg) in futures:
                    try:
                        result = future.result()
                        if result:
                            results.append((result, raw_msg))
                    except Exception as e:
                        self.log_error("Failed to process message", error=e)

                # Add to batch manager
                if results:
                    self.batch_manager.add_to_batch(results)
                    self.batch_manager.flush()
                    
            except Exception as e:
                self.log_error("Error in message processing loop", error=e)
                time.sleep(1)

    def shutdown(self) -> None:
        """Shutdown the collector gracefully"""
        self.log_info("Shutting down collector")
        self._stop_event.set()
        
        # Shutdown thread pool
        self.thread_pool.shutdown(wait=True)
        
        # Final flush
        try:
            self.batch_manager.flush()
        except Exception as e:
            self.log_error("Error during final flush", error=e)
        
        self.log_info("Collector shutdown complete")