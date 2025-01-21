import asyncio
import sys
import os
from typing import Optional
import argparse
from dotenv import load_dotenv

from .config.settings import Settings, get_settings
from .config.logging_config import setup_logging, get_logger

logger = get_logger(__name__)

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Azure Configuration Collector')
    parser.add_argument(
        '--env-file',
        type=str,
        default='.env',
        help='Path to .env file'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level'
    )
    return parser.parse_args()

async def main() -> None:
    """Main entry point for the collector"""
    try:
        # Parse command line arguments
        args = parse_args()
        
        # Load environment variables
        if not load_dotenv(args.env_file):
            print(f"Warning: Could not load environment file: {args.env_file}")
            
        # Setup logging
        setup_logging(args.log_level)
        logger.info("Starting Azure Configuration Collector")
        
        # Validate settings
        settings = get_settings()
        logger.info("Configuration loaded successfully")
        logger.info("Settings loaded: %s", settings.model_dump())
        
        # For now, just keep the program running
        while True:
            await asyncio.sleep(1)
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)

def run_collector():
    """Entry point for running the collector"""
    try:
        if sys.platform == 'win32':
            # Set up proper asyncio event loop policy for Windows
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_collector()

# Export the run function for entry point
run = run_collector







# import asyncio
# import sys
# import os
# from typing import Optional
# import argparse
# from dotenv import load_dotenv

# from src.config.settings import Settings, get_settings
# from src.config.logging_config import setup_logging, get_logger
# from src.core.collector import Collector

# logger = get_logger(__name__)

# def parse_args():
#     """Parse command line arguments"""
#     parser = argparse.ArgumentParser(description='Azure Configuration Collector')
#     parser.add_argument(
#         '--env-file',
#         type=str,
#         default='.env',
#         help='Path to .env file'
#     )
#     parser.add_argument(
#         '--log-level',
#         type=str,
#         default='INFO',
#         choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
#         help='Logging level'
#     )
#     return parser.parse_args()

# async def main() -> None:
#     """Main entry point for the collector"""
#     try:
#         # Parse command line arguments
#         args = parse_args()
        
#         # Load environment variables
#         if not load_dotenv(args.env_file):
#             print(f"Warning: Could not load environment file: {args.env_file}")
            
#         # Setup logging
#         setup_logging(args.log_level)
#         logger.info("Starting Azure Configuration Collector")
        
#         # Validate settings
#         settings = get_settings()
#         logger.info("Configuration loaded successfully")
        
#         # Create and start collector
#         collector = Collector()
#         await collector.start()
        
#     except Exception as e:
#         logger.error(f"Fatal error: {str(e)}")
#         sys.exit(1)

# def run() -> None:
#     """Entry point for running the collector"""
#     try:
#         if sys.platform == 'win32':
#             # Set up proper asyncio event loop policy for Windows
#             asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         logger.info("Received keyboard interrupt, shutting down")
#     except Exception as e:
#         logger.error(f"Unexpected error: {str(e)}")
#         sys.exit(1)

# if __name__ == "__main__":
#     run()