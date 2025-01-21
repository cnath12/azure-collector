from datetime import datetime, UTC
from src.azure.message_interface import (
    ServiceType,
    APIVersion,
    HttpMethod,
    APIRequest,
    CollectorMessage,
    get_vm_info_message,
    get_storage_security_message
)
from src.config.logging_config import get_logger
from src.utils.json_utils import to_json

logger = get_logger(__name__)

def generate_example_messages():
    """Generate example messages for demonstration"""
    messages = []

    # Example 1: VM Info Message
    vm_message = get_vm_info_message(
        subscription_id="sub-123",
        resource_group="rg-test",
        vm_name="vm-test"
    )
    messages.append(("VM Info Message", vm_message))

    # Example 2: NSG Audit Message
    nsg_message = CollectorMessage(
        message_id="msg-nsg-audit",
        correlation_id="corr-security-audit",
        api_requests=[
            APIRequest(
                service=ServiceType.NETWORK,
                api_version=APIVersion.NETWORK_2023,
                method=HttpMethod.GET,
                resource_path="/subscriptions/{subscriptionId}/resourceGroups/{resourceGroup}"
                          "/providers/Microsoft.Network/networkSecurityGroups",
                parameters={
                    "subscriptionId": "sub-123",
                    "resourceGroup": "rg-test"
                },
                query_params={
                    "$expand": "securityRules"
                }
            )
        ],
        metadata={
            "audit_type": "security",
            "priority": "high"
        }
    )
    messages.append(("NSG Audit Message", nsg_message))

    # Example 3: Storage Security Message
    storage_message = get_storage_security_message(
        subscription_id="sub-123",
        resource_group="rg-test"
    )
    messages.append(("Storage Security Message", storage_message))

    # Example 4: Database Backup Check Message
    db_backup_message = CollectorMessage(
        message_id="msg-db-backup",
        api_requests=[
            APIRequest(
                service=ServiceType.DATABASE,
                api_version=APIVersion.DATABASE_2023,
                method=HttpMethod.GET,
                resource_path="/subscriptions/{subscriptionId}/resourceGroups/{resourceGroup}"
                          "/providers/Microsoft.Sql/servers/{serverName}/databases/{databaseName}/backupLongTermRetentionPolicies",
                parameters={
                    "subscriptionId": "sub-123",
                    "resourceGroup": "rg-test",
                    "serverName": "sql-server-test",
                    "databaseName": "db-test"
                }
            )
        ],
        metadata={
            "check_type": "backup_policy",
            "compliance_required": True
        }
    )
    messages.append(("Database Backup Check Message", db_backup_message))

    return messages

async def run_examples():
    """Run examples and print results"""
    logger.info("Running Azure collector message examples")
    
    messages = generate_example_messages()
    
    for title, message in messages:
        print("\n" + "="*80)
        print(f"\n{title}:")
        print("-"*40)
        
        # Convert message to JSON using our custom encoder
        message_json = to_json(message.model_dump())
        print(message_json)
        
        # Print some key information about the message
        print("\nKey Information:")
        print(f"Message ID: {message.message_id}")
        print(f"Number of API Requests: {len(message.api_requests)}")
        print(f"Services Used: {', '.join(str(req.service) for req in message.api_requests)}")
        if message.metadata:
            print(f"Metadata: {message.metadata}")
        
    print("\n" + "="*80)
    logger.info("Completed running examples")

if __name__ == "__main__":
    import asyncio
    asyncio.run(run_examples())