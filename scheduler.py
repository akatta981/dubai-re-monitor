"""
scheduler.py — Background scheduler for data refresh + anomaly detection.
Uses APScheduler (works on Windows without cron).

Run in a separate terminal: python scheduler.py
Fetches DLD data + runs detection pipeline every 15 minutes.
"""

from __future__ import annotations

import logging
import signal
import sys
import time
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("data/scheduler.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

import config
from alerts import send_alerts
from anomaly_detector import run_detection_pipeline
from data_fetcher import fetch_dld_transactions
from db import init_db


def run_full_cycle() -> None:
    """
    One complete data refresh cycle:
    1. Fetch new DLD transactions
    2. Run anomaly detection pipeline
    3. Send alerts for any new signals
    """
    logger.info("─── Cycle start: %s ───", datetime.utcnow().isoformat())

    try:
        new_rows = fetch_dld_transactions()
        logger.info("DLD: %d new transactions", new_rows)
    except Exception as e:
        logger.error("DLD fetch error (continuing): %s", e)

    try:
        signals = run_detection_pipeline()
        if signals:
            logger.info("Sending alerts for %d signals", len(signals))
            results = send_alerts(signals)
            for ch, ok in results.items():
                logger.info("  Alert %s: %s", ch, "sent" if ok else "FAILED")
    except Exception as e:
        logger.error("Detection pipeline error: %s", e, exc_info=True)

    logger.info("─── Cycle complete ───")


def graceful_shutdown(signum, frame) -> None:
    logger.info("Scheduler shutting down (signal %d)", signum)
    sys.exit(0)


if __name__ == "__main__":
    init_db()

    # Register shutdown handler (Ctrl+C)
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    scheduler = BlockingScheduler(timezone="Asia/Dubai")

    scheduler.add_job(
        run_full_cycle,
        trigger=IntervalTrigger(minutes=config.FETCH_INTERVAL_MINUTES),
        id="full_cycle",
        name="DLD Fetch + Anomaly Detection",
        replace_existing=True,
        next_run_time=datetime.now(),  # Run immediately on start
    )

    print(f"""
╔══════════════════════════════════════════════════╗
║     Dubai RE Monitor — Scheduler Running         ║
║  Refresh interval: {config.FETCH_INTERVAL_MINUTES} minutes                     ║
║  Press Ctrl+C to stop                            ║
╚══════════════════════════════════════════════════╝
    """)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")
