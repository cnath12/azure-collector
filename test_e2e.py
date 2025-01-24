import time
import logging
from datetime import datetime, UTC
import json
from typing import Optional
import requests

from src.core.collector import Collector
from src.storage.queue import QueueManager
from src.storage.snowflake import SnowflakeManager
from src.config.settings import get_settings
from src.azure.message_interface import (
    ServiceType,
    APIVersion,
    HttpMethod,
    CollectorMessage,
    APIRequest
)

# Quiet Azure logging
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)

class E2ETest:
    def __init__(self):
        self.collector = None

    def __enter__(self):
        """Initialize resources"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup resources"""
        if self.collector:
            self.collector.shutdown()

    def verify_snowflake_connection(self) -> bool:
        """Test Snowflake connection and table creation"""
        snowflake_manager = None
        try:
            print("\nTesting Snowflake Connection...")
            snowflake_manager = SnowflakeManager()
            snowflake_manager.initialize()
            print("✓ Snowflake connection successful")
            return True
        except Exception as e:
            print(f"✗ Snowflake connection failed: {str(e)}")
            return False
        finally:
            if snowflake_manager:
                snowflake_manager.close()

    def send_test_message(self) -> Optional[str]:
        """Send test message to queue"""
        queue_manager = None
        try:
            queue_manager = QueueManager()
            print("\nSending test message to Azure Queue...")
            
            message = CollectorMessage(
                message_id=f"test-{datetime.now(UTC).timestamp()}",
                api_requests=[
                    APIRequest(
                        service=ServiceType.RESOURCE,
                        api_version=APIVersion.RESOURCE_2023,
                        method=HttpMethod.GET,
                        resource_path="/subscriptions/{subscriptionId}/resourceGroups",
                        parameters={"subscriptionId": "99efcb44-1887-4f2d-80ce-056303f329dd"}
                    )
                ]
            )
            queue_manager.send_message(message)
            print(f"✓ Test message sent successfully (ID: {message.message_id})")
            
            time.sleep(2)  # Changed from await asyncio.sleep(2)
            print("Waited for message propagation")
            
            return message.message_id
        except Exception as e:
            print(f"✗ Failed to send test message: {str(e)}")
            return None

    def verify_message_in_queue(self, message_id: str) -> bool:
        """Verify message exists in queue"""
        queue_manager = None
        try:
            queue_manager = QueueManager()
            print("\nVerifying message in Azure Queue...")
            
            properties = queue_manager.queue_client.get_queue_properties()
            print(f"Queue message count: {properties.approximate_message_count}")
            
            messages = queue_manager.receive_messages(max_messages=32)
            if not messages:
                print("No messages in queue")
                return False
                
            for msg, raw_msg in messages:
                if msg.message_id == message_id:
                    queue_manager.update_message_visibility(raw_msg, visibility_timeout=30)
                    print(f"✓ Found target message (ID: {message_id})")
                    return True
            
            return False
        except Exception as e:
            print(f"✗ Failed to verify message: {str(e)}")
            return False

    def cleanup_queue(self):
        """Clean up existing messages"""
        queue_manager = None
        try:
            queue_manager = QueueManager()
            print("\nCleaning up queue...")
            
            while True:
                messages = queue_manager.receive_messages(max_messages=32)
                if not messages:
                    break
                    
                for _, msg in messages:
                    queue_manager.delete_message(msg)
                    
            print("✓ Queue cleaned")
            return True
        except Exception as e:
            print(f"✗ Failed to clean queue: {str(e)}")
            return False


    # def send_multiple_test_messages(self, count: int = 10) -> List[str]:
    #     """Send multiple test messages to queue"""
    #     queue_manager = None
    #     message_ids = []
    #     try:
    #         queue_manager = QueueManager()
    #         print(f"\nSending {count} test messages to Azure Queue...")
            
    #         for i in range(count):
    #             message = CollectorMessage(
    #                 message_id=f"test-{datetime.now(UTC).timestamp()}-{i}",
    #                 api_requests=[
    #                     APIRequest(
    #                         service=ServiceType.RESOURCE,
    #                         api_version=APIVersion.RESOURCE_2023,
    #                         method=HttpMethod.GET,
    #                         resource_path="/subscriptions/{subscriptionId}/resourceGroups",
    #                         parameters={"subscriptionId": "99efcb44-1887-4f2d-80ce-056303f329dd"}
    #                     )
    #                 ]
    #             )
    #             queue_manager.send_message(message)
    #             message_ids.append(message.message_id)
    #             print(f"✓ Test message {i+1}/{count} sent successfully (ID: {message.message_id})")
    #             time.sleep(0.1)  # Small delay between messages
                
    #         print("Waiting for message propagation...")
    #         time.sleep(2)
    #         return message_ids
            
    #     except Exception as e:
    #         print(f"✗ Failed to send test messages: {str(e)}")
    #         return message_ids

    # def verify_multiple_messages_processed(self, message_ids: List[str]) -> bool:
    #     """Verify multiple messages were processed"""
    #     snowflake_manager = None
    #     try:
    #         print("\nVerifying data in Snowflake...")
    #         snowflake_manager = SnowflakeManager()
    #         snowflake_manager.initialize()
            
    #         cursor = snowflake_manager._get_cursor()
    #         try:
    #             message_list = "','".join(message_ids)
    #             cursor.execute(f"""
    #                 SELECT COUNT(*)
    #                 FROM azure_config_data
    #                 WHERE message_id IN ('{message_list}')
    #             """)
    #             count = cursor.fetchone()[0]
                
    #             print(f"✓ Found {count}/{len(message_ids)} messages processed in Snowflake")
    #             return count == len(message_ids)
    #         finally:
    #             cursor.close()
    #     except Exception as e:
    #         print(f"✗ Failed to verify Snowflake data: {str(e)}")
    #         return False
    #     finally:
    #         if snowflake_manager:
    #             snowflake_manager.close()



    def verify_data_in_snowflake(self, message_id: str) -> bool:
        """Verify data in Snowflake"""
        snowflake_manager = None
        try:
            print("\nVerifying data in Snowflake...")
            snowflake_manager = SnowflakeManager()
            snowflake_manager.initialize()
            
            cursor = snowflake_manager._get_cursor()
            try:
                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM azure_config_data
                    WHERE message_id = '{message_id}'
                """)
                count = cursor.fetchone()[0]
                
                if count > 0:
                    print(f"✓ Found {count} records in Snowflake for message {message_id}")
                    return True
                else:
                    print(f"✗ No records found in Snowflake for message {message_id}")
                    return False
            finally:
                cursor.close()
        except Exception as e:
            print(f"✗ Failed to verify Snowflake data: {str(e)}")
            return False
        finally:
            if snowflake_manager:
                snowflake_manager.close()

    def run_collector(self, timeout: int = 30):
        """Run collector for specified time"""
        try:
            print("\nStarting Collector...")
            self.collector = Collector()
            
            def stop_after_delay():
                time.sleep(timeout)
                self.collector.shutdown()
            
            # Start the collector in the main thread
            self.collector.start()
            print("✓ Collector completed successfully")
            return True
                
        except Exception as e:
            print(f"✗ Collector failed: {str(e)}")
            return False

def main():
    print("Starting End-to-End Test\n" + "="*50)
    
    with E2ETest() as test:
        # Run test steps
        test.cleanup_queue()
        
        message_id = test.send_test_message()
        if not message_id:
            print("Failed to send test message. Aborting test.")
            return
        
        time.sleep(5)
        
        if not test.verify_message_in_queue(message_id):
            print("Failed to verify message in queue. Aborting test.")
            return
        
        if not test.run_collector(timeout=30):
            print("Collector failed. Aborting test.")
            return
        
        time.sleep(12)
        
        
        
        if not test.verify_data_in_snowflake(message_id):
            print("Failed to verify data in Snowflake. Test failed.")
            return
        
        
        
        
        #  # Send multiple messages
        # message_ids = test.send_multiple_test_messages(count=10)
        # if not message_ids:
        #     print("Failed to send test messages. Aborting test.")
        #     return
            
        # # Run collector
        # if not test.run_collector(timeout=60):  # Increased timeout for multiple messages
        #     print("Collector failed. Aborting test.")
        #     return
            
        # # Verify all messages were processed
        # if not test.verify_multiple_messages_processed(message_ids):
        #     print("Failed to verify all messages were processed. Test failed.")
        #     return
        
        print("\n" + "="*50)
        print("End-to-End Test Completed Successfully! 🎉")
        print("="*50)

if __name__ == "__main__":
    main()