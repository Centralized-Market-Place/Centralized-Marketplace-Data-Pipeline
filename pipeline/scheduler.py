from apscheduler.schedulers.blocking import BlockingScheduler
from pipeline.pipeline import run_pipeline

scheduler = BlockingScheduler()
scheduler.add_job(run_pipeline, 'interval', minutes=60)

if __name__ == "__main__":
    scheduler.start()
