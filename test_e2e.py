import asyncio
import logging
from datetime import datetime, UTC
import json
from typing import Optional
import aiohttp

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
        self.session = None

    async def __aenter__(self):
        """Initialize resources"""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup resources"""
        if self.collector:
            await self.collector.shutdown()
        if self.session and not self.session.closed:
            await self.session.close()

    async def verify_snowflake_connection(self) -> bool:
        """Test Snowflake connection and table creation"""
        snowflake_manager = None
        try:
            print("\nTesting Snowflake Connection...")
            snowflake_manager = SnowflakeManager()
            await snowflake_manager.initialize()
            print("✓ Snowflake connection successful")
            return True
        except Exception as e:
            print(f"✗ Snowflake connection failed: {str(e)}")
            return False
        finally:
            if snowflake_manager:
                await snowflake_manager.close()

    async def send_test_message(self) -> Optional[str]:
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
            await queue_manager.send_message(message)
            print(f"✓ Test message sent successfully (ID: {message.message_id})")
            
            await asyncio.sleep(2)
            print("Waited for message propagation")
            
            return message.message_id
        except Exception as e:
            print(f"✗ Failed to send test message: {str(e)}")
            return None

    async def verify_message_in_queue(self, message_id: str) -> bool:
        """Verify message exists in queue"""
        queue_manager = None
        try:
            queue_manager = QueueManager()
            print("\nVerifying message in Azure Queue...")
            
            properties = queue_manager.queue_client.get_queue_properties()
            print(f"Queue message count: {properties.approximate_message_count}")
            
            messages = await queue_manager.receive_messages(max_messages=32)
            if not messages:
                print("No messages in queue")
                return False
                
            for msg, raw_msg in messages:
                if msg.message_id == message_id:
                    await queue_manager.update_message_visibility(raw_msg, visibility_timeout=30)
                    print(f"✓ Found target message (ID: {message_id})")
                    return True
            
            return False
        except Exception as e:
            print(f"✗ Failed to verify message: {str(e)}")
            return False

    async def cleanup_queue(self):
        """Clean up existing messages"""
        queue_manager = None
        try:
            queue_manager = QueueManager()
            print("\nCleaning up queue...")
            
            while True:
                messages = await queue_manager.receive_messages(max_messages=32)
                if not messages:
                    break
                    
                for _, msg in messages:
                    await queue_manager.delete_message(msg)
                    
            print("✓ Queue cleaned")
            return True
        except Exception as e:
            print(f"✗ Failed to clean queue: {str(e)}")
            return False

    async def verify_data_in_snowflake(self, message_id: str) -> bool:
        """Verify data in Snowflake"""
        snowflake_manager = None
        try:
            print("\nVerifying data in Snowflake...")
            snowflake_manager = SnowflakeManager()
            await snowflake_manager.initialize()
            
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
                await snowflake_manager.close()

    async def run_collector(self, timeout: int = 30):
        """Run collector for specified time"""
        try:
            print("\nStarting Collector...")
            self.collector = Collector()
            
            async def stop_after_delay():
                await asyncio.sleep(timeout)
                await self.collector.shutdown()
            
            try:
                await asyncio.gather(
                    self.collector.start(),
                    stop_after_delay()
                )
                print("✓ Collector completed successfully")
                return True
            except asyncio.CancelledError:
                print("✓ Collector stopped as planned")
                return True
                
        except Exception as e:
            print(f"✗ Collector failed: {str(e)}")
            return False

async def main():
    print("Starting End-to-End Test\n" + "="*50)
    
    async with E2ETest() as test:
        # Run test steps
        await test.cleanup_queue()
        
        message_id = await test.send_test_message()
        if not message_id:
            print("Failed to send test message. Aborting test.")
            return
        
        await asyncio.sleep(5)
        
        if not await test.verify_message_in_queue(message_id):
            print("Failed to verify message in queue. Aborting test.")
            return
        
        if not await test.run_collector(timeout=30):
            print("Collector failed. Aborting test.")
            return
        
        await asyncio.sleep(12)
        
        if not await test.verify_data_in_snowflake(message_id):
            print("Failed to verify data in Snowflake. Test failed.")
            return
        
        print("\n" + "="*50)
        print("End-to-End Test Completed Successfully! 🎉")
        print("="*50)

if __name__ == "__main__":
    asyncio.run(main())