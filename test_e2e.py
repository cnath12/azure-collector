import asyncio
import logging
from datetime import datetime, UTC
import json
from typing import Optional

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


async def send_test_message() -> Optional[str]:
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
        
        # Add delay to ensure message is available
        await asyncio.sleep(2)
        print("Waited for message propagation")
        
        return message.message_id
    except Exception as e:
        print(f"✗ Failed to send test message: {str(e)}")
        return None

async def verify_message_in_queue(message_id: str) -> bool:
    try:
        queue_manager = QueueManager()
        print("\nVerifying message in Azure Queue...")
        
        # Get queue properties to check message count
        properties = queue_manager.queue_client.get_queue_properties()
        print(f"Queue message count: {properties.approximate_message_count}")
        
        messages = await queue_manager.receive_messages(max_messages=32)
        if not messages:
            print("No messages in queue")
            return False
            
        for msg, raw_msg in messages:
            if msg.message_id == message_id:
                # Update visibility timeout instead of processing
                await queue_manager.update_message_visibility(raw_msg, visibility_timeout=30)
                print(f"✓ Found target message (ID: {message_id})")
                return True
        
        return False
    except Exception as e:
        print(f"✗ Failed to verify message: {str(e)}")
        return False
    
async def verify_queue_exists():
    try:
        queue_manager = QueueManager()
        print("\nVerifying queue existence...")
        print(f"Queue name: {queue_manager.settings.azure_queue_name}")
        
        # Try to get queue properties to verify existence
        try:
            properties = queue_manager.queue_client.get_queue_properties()
            print(f"✓ Queue exists with properties:")
            print(f"  - Approximate message count: {properties.approximate_message_count}")
            print(f"  - Metadata: {properties.metadata}")
            return True
        except Exception as e:
            if "QueueNotFound" in str(e):
                print("✗ Queue does not exist")
                # Create the queue
                print("Creating queue...")
                queue_manager.queue_client.create_queue()
                print("✓ Queue created successfully")
                return True
            else:
                raise
            
    except Exception as e:
        print(f"✗ Failed to verify queue: {str(e)}")
        return False
    
async def cleanup_queue():
    """Clean up any existing messages in queue"""
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
   

async def run_collector(timeout: int = 30):
    """Run the collector for specified time"""
    try:
        print("\nStarting Collector...")
        collector = Collector()
        
        async def stop_after_delay():
            await asyncio.sleep(timeout)
            await collector.shutdown()
        
        tasks = [
            collector.start(),
            stop_after_delay()
        ]
        
        try:
            await asyncio.gather(*tasks)
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
    
    await cleanup_queue()
    
    # Step 1: Verify Azure Queue exists
    if not await verify_queue_exists():
        print("Queue verification failed. Aborting test.")
        return
        
    # Step 2: Send test message
    message_id = await send_test_message()
    if not message_id:
        print("Failed to send test message. Aborting test.")
        return
    
    # Add delay to ensure message propagation
    await asyncio.sleep(5)
    
    # Step 3: Verify message in queue
    if not await verify_message_in_queue(message_id):
        print("Failed to verify message in queue. Aborting test.")
        return
    
    # Step 4: Run collector
    if not await run_collector(timeout=30):
        print("Collector failed. Aborting test.")
        return
    
    print("\n" + "="*50)
    print("End-to-End Test Completed Successfully! 🎉")
    print("="*50)

if __name__ == "__main__":
    asyncio.run(main())
