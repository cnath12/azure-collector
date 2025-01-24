from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, UTC
from azure.storage.queue import QueueMessage

from src.config.logging_config import LoggerMixin  # Added this import
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

    def process_message(
        self,
        message: CollectorMessage,
        queue_message: QueueMessage
    ) -> Optional[CollectorResponse]:
        try:
            self.log_info("Processing message",
                         message_id=message.message_id,
                         num_requests=len(message.api_requests))

            # Update message visibility while processing
            self.queue_manager.update_message_visibility(
                queue_message,
                visibility_timeout=300  # 5 minutes
            )

            # Execute Azure API calls
            result = self.azure_client.execute_api_calls(message)

            if result.status == "success":
                self.log_info("Successfully processed message",
                            message_id=message.message_id,
                            num_results=len(result.data))  # Changed from result.results to result.data
            else:
                self.log_error("Failed to process message",
                             errors=result.errors,
                             message_id=message.message_id)

            return result

        except Exception as e:
            self.log_error("Error processing message", error=e,
                          message_id=message.message_id)
            # Create error result
            return CollectorResponse(
                message_id=message.message_id,
                correlation_id=message.correlation_id,
                status="error",
                data=[],  # Empty list instead of results
                errors=[str(e)],
                timestamp=datetime.now(UTC)
            )

    def process_messages_batch(
        self,
        messages: List[Tuple[CollectorMessage, QueueMessage]]
    ) -> List[Tuple[CollectorResponse, QueueMessage]]:
        if not messages:
            return []

        self.log_info(f"Processing batch of {len(messages)} messages")

        # Process messages concurrently
        results = []
        for message, queue_message in messages:
            try:
                result = self.process_message(message, queue_message)
                if result:
                    results.append((result, queue_message))
            except Exception as e:
                self.log_error("Message processing failed with exception",
                             error=str(e))
                continue

        self.log_info(f"Successfully processed {len(results)} messages")
        return results