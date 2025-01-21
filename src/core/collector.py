import asyncio
from typing import Optional, AsyncGenerator
from datetime import datetime, UTC

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
        self._stop_event = asyncio.Event()
        # self._periodic_flush_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the collector"""
        self.log_info("Starting collector")
        
        try:
            # Process messages until stopped
            await self.process_messages()
                
        except Exception as e:
            self.log_error("Fatal error in collector", error=e)
            raise
        finally:
            await self.shutdown()

    async def process_messages(self) -> None:
        """Process messages from the queue"""
        while not self._stop_event.is_set():
            try:
                messages = await self.queue_manager.receive_messages(max_messages=32)
                
                if not messages:
                    await asyncio.sleep(1)
                    continue

                self.log_info(f"Processing batch of {len(messages)} messages")
                for message, raw_msg in messages:
                    try:
                        # Execute Azure API calls
                        result = await self.azure_client.execute_api_calls(message)
                        
                        # Add to batch manager
                        await self.batch_manager.add_to_batch([(result, raw_msg)])
                        
                        self.log_info(f"Successfully processed message {message.message_id}")
                    except Exception as e:
                        self.log_error(f"Failed to process message {message.message_id}", error=e)
                        
                # Force flush after batch processing
                await self.batch_manager.flush()
                    
            except Exception as e:
                self.log_error("Error in message processing loop", error=e)
                await asyncio.sleep(1)

    async def shutdown(self) -> None:
        """Shutdown the collector gracefully"""
        self.log_info("Shutting down collector")
        self._stop_event.set()
        
        # Final flush
        try:
            await self.batch_manager.flush()
        except Exception as e:
            self.log_error("Error during final flush", error=e)
        
        self.log_info("Collector shutdown complete")