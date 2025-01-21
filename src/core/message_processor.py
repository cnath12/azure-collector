from typing import Optional, Dict, Any, List, Tuple
import asyncio
from datetime import datetime, UTC
from azure.storage.queue import QueueMessage

from src.config.logging_config import LoggerMixin
from src.azure.client import AzureClient
from src.storage.queue import QueueManager
from src.azure.message_interface import (
    CollectorMessage,
    CollectorResponse,
    ServiceType,
    APIRequest
)
from src.config.settings import get_settings

class MessageProcessor(LoggerMixin):
    """Processes individual messages from the queue"""
    
    def __init__(self) -> None:
        super().__init__()
        self.settings = get_settings()
        self.azure_client = AzureClient()
        self.queue_manager = QueueManager()

    async def process_message(
        self,
        message: CollectorMessage,
        queue_message: QueueMessage
    ) -> Optional[CollectorResponse]:
        """
        Process a single message from the queue
        
        Args:
            message: Parsed collection message
            queue_message: Original queue message
            
        Returns:
            Collection result if successful, None if failed
            
        Notes:
            Does not delete message from queue - that's handled by batch manager
            after successful Snowflake write
        """
        try:
            self.log_info("Processing message",
                         message_id=message.message_id,
                         num_requests=len(message.api_requests))

            # Update message visibility while processing
            await self.queue_manager.update_message_visibility(
                queue_message,
                visibility_timeout=300  # 5 minutes
            )

            # Execute Azure API calls
            result = await self.azure_client.execute_api_calls(message)

            if result.status == "success":
                self.log_info("Successfully processed message",
                            message_id=message.message_id,
                            num_results=len(result.results))
            else:
                self.log_error("Failed to process message",
                             errors=result.errors,
                             message_id=message.message_id)

            return result

        except Exception as e:
            self.log_error("Error processing message", e,
                          message_id=message.message_id)
            # Create error result
            return CollectorResponse(
                message_id=message.message_id,
                correlation_id=message.correlation_id,
                status="error",
                results=[],
                errors=[str(e)],
                timestamp=datetime.now(UTC)
            )

    async def process_messages_batch(
        self,
        messages: List[Tuple[CollectorMessage, QueueMessage]]
    ) -> List[Tuple[CollectorResponse, QueueMessage]]:
        """
        Process a batch of messages concurrently
        
        Args:
            messages: List of (parsed_message, queue_message) tuples
            
        Returns:
            List of (result, queue_message) tuples
        """
        if not messages:
            return []

        self.log_info(f"Processing batch of {len(messages)} messages")

        # Process messages concurrently
        tasks = [
            self.process_message(message, queue_message)
            for message, queue_message in messages
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Pair results with their original queue messages
        processed_results = []
        for (_, queue_message), result in zip(messages, results):
            if isinstance(result, Exception):
                self.log_error("Message processing failed with exception",
                             error=str(result))
                continue
            if result:
                processed_results.append((result, queue_message))

        self.log_info(f"Successfully processed {len(processed_results)} messages")
        return processed_results