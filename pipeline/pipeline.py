import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingestion.fetcher import fetch_runner
import asyncio

async def run_pipeline():
    await fetch_runner()
    
if __name__ == "__main__":
    asyncio.run(run_pipeline())
