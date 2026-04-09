"""
APScheduler setup — hourly pipeline job.

Runs `run_with_retry()` at the top of every hour (UTC). Uses a blocking
scheduler so the process stays alive in a terminal or as a background service.

Design notes
------------
- max_instances=1 prevents job pile-up if a run takes longer than 60 minutes.
- misfire_grace_time=300 means a missed fire (e.g. machine asleep) is still
  executed if it's within 5 minutes of the scheduled time.
- BlockingScheduler is correct here — BackgroundScheduler would exit
  immediately when the script ends.

At higher data volumes or when orchestrating multiple independent pipelines,
Airflow would be the natural next step. For a single hourly job APScheduler
avoids that operational complexity entirely.

Usage
-----
    # Blocking (foreground):
    python pipeline/scheduler.py

    # Non-blocking (background, Unix):
    nohup python pipeline/scheduler.py &
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from pipeline.runner import run_with_retry

_log_dir = Path(__file__).parent.parent / "logs"
_log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(_log_dir / "scheduler.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

scheduler = BlockingScheduler(timezone="UTC")

scheduler.add_job(
    run_with_retry,
    trigger=CronTrigger(minute=0),
    id="energy_pipeline",
    name="Sweden energy ingestion",
    max_instances=1,
    misfire_grace_time=300,
)

if __name__ == "__main__":
    logger.info("Scheduler starting — hourly job registered")
    print("Scheduler running. Press Ctrl+C to stop.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")
        print("\nScheduler stopped.")
