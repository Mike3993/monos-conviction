"""
scheduler.py

Defines and manages the nightly job schedule for the MONOS Conviction Engine.
Triggers the supervisor agent pipeline at configured times and handles
job logging, retries, and failure alerting.
"""

import logging
import os
import sys
import time
import traceback
from datetime import datetime

import schedule
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# Default nightly run time (Eastern-ish; adjust per deployment TZ)
NIGHTLY_TIME = os.getenv("MONOS_NIGHTLY_TIME", "22:00")
MAX_RETRIES = int(os.getenv("MONOS_MAX_RETRIES", "2"))


class Scheduler:
    """
    Nightly job runner that triggers the full conviction engine pipeline.
    Wraps the SupervisorAgent with scheduling, error handling, and observability.
    """

    def __init__(self):
        self.sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # ---------------------------------------------------------------- jobs

    def register_jobs(self):
        """Register the nightly pipeline job with the schedule library."""
        schedule.every().day.at(NIGHTLY_TIME).do(self.run_nightly)
        logger.info("Registered nightly pipeline job at %s", NIGHTLY_TIME)

    def run_nightly(self):
        """
        Execute the full MONOS conviction engine pipeline:
          1. Portfolio ingestion (refresh position data)
          2. Greeks snapshot
          3. Conviction map scoring
        Logs a task_runs record to Supabase for observability.
        """
        run_start = datetime.utcnow().isoformat()
        logger.info("=== MONOS nightly pipeline started at %s ===", run_start)

        steps = [
            ("portfolio_ingestion", self._step_portfolio_ingestion),
            ("greeks_snapshot", self._step_greeks_snapshot),
            ("conviction_scoring", self._step_conviction_scoring),
        ]

        results: dict = {}
        overall_status = "success"

        for step_name, step_fn in steps:
            attempt = 0
            while attempt <= MAX_RETRIES:
                try:
                    logger.info("Running step: %s (attempt %d)", step_name, attempt + 1)
                    result = step_fn()
                    results[step_name] = {"status": "success", "detail": result}
                    logger.info("Step %s completed successfully", step_name)
                    break
                except Exception as exc:
                    attempt += 1
                    logger.error("Step %s failed (attempt %d): %s",
                                 step_name, attempt, exc)
                    if attempt > MAX_RETRIES:
                        results[step_name] = {
                            "status": "failed",
                            "error": str(exc),
                            "traceback": traceback.format_exc(),
                        }
                        overall_status = "partial_failure"

        # Persist task_runs record
        run_end = datetime.utcnow().isoformat()
        task_record = {
            "task_name": "nightly_pipeline",
            "status": overall_status,
            "details": {
                "started_at": run_start,
                "finished_at": run_end,
                "steps": results,
            },
        }
        try:
            self.sb.table("task_runs").insert(task_record).execute()
            logger.info("task_runs record written (status=%s)", overall_status)
        except Exception:
            logger.exception("Failed to write task_runs record")

        logger.info("=== MONOS nightly pipeline finished (%s) ===", overall_status)
        return results

    # -------------------------------------------------------------- steps

    def _step_portfolio_ingestion(self) -> str:
        from services.portfolio_service import ingest_positions
        ingest_positions()
        return "positions ingested"

    def _step_greeks_snapshot(self) -> str:
        from engines.greeks_engine import GreeksEngine
        engine = GreeksEngine(supabase_client=self.sb)
        snapshots = engine.snapshot_and_store()
        return f"{len(snapshots)} greeks snapshots written"

    def _step_conviction_scoring(self) -> str:
        from engines.conviction_map_engine import ConvictionMapEngine
        engine = ConvictionMapEngine(supabase_client=self.sb, regime="risk_on")
        scores = engine.run_from_supabase()
        return f"{len(scores)} positions scored"

    # ----------------------------------------------------------- run loop

    def start(self):
        """Register jobs and enter the blocking schedule loop."""
        self.register_jobs()
        logger.info("Scheduler loop started. Waiting for scheduled jobs...")
        while True:
            schedule.run_pending()
            time.sleep(30)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="MONOS Conviction Engine Scheduler")
    parser.add_argument("--now", action="store_true",
                        help="Run the nightly pipeline immediately instead of waiting")
    args = parser.parse_args()

    sched = Scheduler()
    if args.now:
        sched.run_nightly()
    else:
        sched.start()
