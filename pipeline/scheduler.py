from apscheduler.schedulers.blocking import BlockingScheduler
from pipeline.run import run_pipeline

scheduler = BlockingScheduler()
scheduler.add_job(run_pipeline, 'interval', minutes=60)

if __name__ == "__main__":
    scheduler.start()
    
