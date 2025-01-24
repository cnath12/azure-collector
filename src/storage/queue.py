from azure.storage.queue import QueueClient, QueueMessage
from typing import List, Optional, Dict, Any, Tuple
import json
from datetime import datetime, UTC
from azure.core.exceptions import ResourceNotFoundError
from src.config.logging_config import LoggerMixin
from src.utils.retry import with_retry
from src.azure.message_interface import CollectorMessage
from src.utils.json_utils import to_json
from src.config.settings import get_settings

class QueueManager(LoggerMixin):
   """Manages Azure Queue operations"""
   _instance = None
   _initialized = False
   
   def __new__(cls):
       if cls._instance is None:
           cls._instance = super().__new__(cls)
       return cls._instance

   def __init__(self) -> None:
       if not self._initialized:
           super().__init__()
           self.settings = get_settings()
           
           # Validate settings
           if not self.settings.azure_queue_connection_string:
               raise ValueError("Azure Queue connection string is not set")
           if len(self.settings.azure_queue_connection_string) < 50:
               raise ValueError("Azure Queue connection string appears invalid")
               
           self._queue_client = None
           self._initialized = True
           self.log_info(
               "Initialized QueueManager",
               queue_name=self.settings.azure_queue_name,
               connection_string_length=len(self.settings.azure_queue_connection_string)
           )

   @property
   def queue_client(self) -> QueueClient:
       """Lazy initialization of Queue client"""
       if self._queue_client is None:
           self.log_info("Initializing Queue client")
           self.log_info(
               "Using connection details",
               queue_name=self.settings.azure_queue_name,
               connection_string_length=len(self.settings.azure_queue_connection_string)
           )
           self._queue_client = QueueClient.from_connection_string(
               conn_str=self.settings.azure_queue_connection_string,
               queue_name=self.settings.azure_queue_name
           )
       return self._queue_client

   def _parse_message(self, message: QueueMessage) -> Optional[CollectorMessage]:
       """
       Parse a queue message into a CollectorMessage
       
       Args:
           message: Raw queue message
           
       Returns:
           Parsed CollectorMessage or None if parsing fails
       """
       try:
           message_data = json.loads(message.content)
           return CollectorMessage.model_validate(message_data)
       except Exception as e:
           self.log_error(
               "Failed to parse message",
               error=e,
               message_content=message.content[:1000]  # Log first 1000 chars
           )
           return None

   @with_retry(max_attempts=3)
   def receive_messages(
       self,
       max_messages: int = 32,
       visibility_timeout: int = 300
   ) -> List[Tuple[CollectorMessage, QueueMessage]]:
       try:
           # Get messages from queue with debug info
           messages = self.queue_client.receive_messages(
               messages_per_page=max_messages,
               visibility_timeout=visibility_timeout
           )
           
           # Debug: Print raw messages
           messages_list = list(messages)
           self.log_info(f"Raw messages received: {len(messages_list)}")
           for msg in messages_list:
               self.log_info(f"Message ID: {msg.id}, Content: {msg.content[:100]}")
           
           result = []
           for msg in messages_list:
               parsed_message = self._parse_message(msg)
               if parsed_message:
                   result.append((parsed_message, msg))
                   self.log_info(f"Successfully parsed message: {parsed_message.message_id}")
               else:
                   self.log_error(f"Failed to parse message: {msg.id}")
                   self.delete_message(msg)

           self.log_info(
               "Received messages",
               total_messages=len(messages_list),
               valid_messages=len(result)
           )
           return result

       except Exception as e:
           self.log_error("Failed to receive messages", error=e)
           raise

   @with_retry(max_attempts=3)
   def delete_message(self, message: QueueMessage) -> None:
       """
       Delete a message from the queue
       
       Args:
           message: Queue message to delete
       """
       try:
           self.log_info("Deleting message", message_id=message.id)
           self.queue_client.delete_message(message)
       except ResourceNotFoundError:
           self.log_info(
               "Message already deleted or expired",
               message_id=message.id
           )
       except Exception as e:
           self.log_error(
               "Failed to delete message",
               error=e,
               message_id=message.id
           )
           raise

   @with_retry(max_attempts=3)
   def update_message_visibility(
       self,
       message: QueueMessage,
       visibility_timeout: int
   ) -> None:
       """
       Update message visibility timeout
       
       Args:
           message: Queue message to update
           visibility_timeout: New visibility timeout in seconds
       """
       try:
           self.log_info(
               "Updating message visibility",
               message_id=message.id,
               visibility_timeout=visibility_timeout
           )
           self.queue_client.update_message(
               message,
               visibility_timeout=visibility_timeout
           )
       except Exception as e:
           self.log_error(
               "Failed to update message visibility",
               error=e,
               message_id=message.id
           )
           raise

   def send_message(
       self,
       message: CollectorMessage,
       visibility_timeout: Optional[int] = None
   ) -> None:
       """
       Send a message to the queue
       
       Args:
           message: Message to send
           visibility_timeout: Optional initial visibility timeout
       """
       try:
           self.log_info(
               "Sending message",
               message_id=message.message_id,
               num_requests=len(message.api_requests)
           )
           
           # Convert message to JSON
           message_json = to_json(message.model_dump())
           
           # Send to queue
           self.queue_client.send_message(
               content=message_json,
               visibility_timeout=visibility_timeout
           )
           
           self.log_info("Successfully sent message", message_id=message.message_id)
           
       except Exception as e:
           self.log_error(
               "Failed to send message",
               error=e,
               message_id=message.message_id
           )
           raise