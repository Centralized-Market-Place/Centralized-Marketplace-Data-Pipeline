# from pipeline.pipeline import run_pipeline
import asyncio
from ingestion.realtime import realtimeRunner
if __name__ == "__main__":
    asyncio.run(realtimeRunner())()
