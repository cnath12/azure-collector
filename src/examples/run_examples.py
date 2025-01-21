#!/usr/bin/env python3
"""
Script to run message examples.
Usage: python -m src.examples.run_examples
"""

import asyncio
from src.examples.message_examples import run_examples
from src.config.logging_config import setup_logging

async def main():
    setup_logging()
    await run_examples()

if __name__ == "__main__":
    asyncio.run(main())