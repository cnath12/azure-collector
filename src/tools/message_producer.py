import asyncio
import json
import uuid
from azure.storage.queue import QueueClient
from datetime import datetime
from typing import Dict, Any

from src.config.settings import get_settings
from src.config.logging_config import get_logger

logger = get_logger(__name__)

class MessageProducer:
    """Test message producer for Azure Queue"""
    
    def __init__(self) -> None:
        self.settings = get_settings()
        self.queue_client = QueueClient.from_connection_string(
            self.settings.azure_queue_connection_string,
            self.settings.azure_queue_name
        )

    def _create_test_message(self) -> Dict[str, Any]:
        """Create a test API request message"""
        return {
            "message_id": str(uuid.uuid4()),
            "api_request": {
                "api_version": "2021-04-01",
                "method": "list",
                "resource_path": "resource_groups/list",
                "parameters": {
                    "subscription_id": "test-subscription-id"
                }
            },
            "timestamp": datetime.utcnow().isoformat(),
            "correlation_id": str(uuid.uuid4())
        }

    async def send_test_messages(self, count: int = 1) -> None:
        """
        Send test messages to the queue
        
        Args:
            count: Number of messages to send
        """
        try:
            logger.info(f"Sending {count} test messages to queue")
            
            for i in range(count):
                message = self._create_test_message()
                self.queue_client.send_message(json.dumps(message))
                logger.info(f"Sent message {i+1}/{count}")
                await asyncio.sleep(0.1)  # Small delay between messages
                
            logger.info("Finished sending test messages")
            
        except Exception as e:
            logger.error(f"Error sending test messages: {str(e)}")
            raise

async def main():
    """Main entry point for message producer"""
    producer = MessageProducer()
    await producer.send_test_messages(count=5)  # Send 5 test messages

if __name__ == "__main__":
    asyncio.run(main())