"""
scheduler.py

Defines and manages the nightly job schedule for the MONOS Conviction Engine.
Triggers the supervisor agent pipeline at configured times and handles
job logging, retries, and failure alerting.

Pipeline execution order:
  1. portfolio_service   — ingest positions
  2. market_service      — fetch live prices
  3. greeks_engine       — compute & store greeks
  4. conviction_map      — score convexity
  5. briefing_builder    — assemble nightly report
"""

import logging
import os
import sys
import time
import traceback
from datetime import datetime

import schedule
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.supabase_helpers import get_supabase_client, write_agent_log

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

AGENT_NAME = "scheduler"

# Default nightly run time (Eastern-ish; adjust per deployment TZ)
NIGHTLY_TIME = os.getenv("MONOS_NIGHTLY_TIME", "22:00")
MAX_RETRIES = int(os.getenv("MONOS_MAX_RETRIES", "2"))


class Scheduler:
    """
    Nightly job runner that triggers the full conviction engine pipeline.
    Wraps the SupervisorAgent with scheduling, error handling, and observability.
    """

    def __init__(self):
        self.sb = get_supabase_client()

    # ---------------------------------------------------------------- jobs

    def register_jobs(self):
        """Register the nightly pipeline job with the schedule library."""
        schedule.every().day.at(NIGHTLY_TIME).do(self.run_nightly)
        logger.info("Registered nightly pipeline job at %s", NIGHTLY_TIME)

    def run_nightly(self):
        """
        Execute the full MONOS conviction engine pipeline (5 steps).
        Logs a task_runs record to Supabase for observability.
        """
        run_start = datetime.utcnow().isoformat()
        logger.info("=== MONOS nightly pipeline started at %s ===", run_start)

        steps = [
            ("portfolio_ingestion", self._step_portfolio_ingestion),
            ("market_snapshot",     self._step_market_snapshot),
            ("greeks_snapshot",     self._step_greeks_snapshot),
            ("conviction_scoring",  self._step_conviction_scoring),
            ("briefing_build",      self._step_briefing_build),
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

                    # Per-step agent_log
                    write_agent_log(self.sb, AGENT_NAME,
                                    f"step:{step_name}", "success",
                                    {"attempt": attempt + 1, "detail": result})
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

                        write_agent_log(self.sb, AGENT_NAME,
                                        f"step:{step_name}", "failed",
                                        {"error": str(exc), "attempts": attempt})

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

        # Overall pipeline agent_log
        write_agent_log(self.sb, AGENT_NAME, "run_nightly", overall_status, {
            "started_at": run_start,
            "finished_at": run_end,
            "steps_completed": sum(
                1 for v in results.values() if v.get("status") == "success"
            ),
            "steps_total": len(steps),
        })

        logger.info("=== MONOS nightly pipeline finished (%s) ===", overall_status)
        return results

    # -------------------------------------------------------------- steps

    def _step_portfolio_ingestion(self) -> str:
        from services.portfolio_service import ingest_positions
        ingest_positions()
        return "positions ingested"

    def _step_market_snapshot(self) -> str:
        from services.market_service import MarketService
        svc = MarketService(supabase_client=self.sb)
        snapshots = svc.fetch_and_store()
        return f"{len(snapshots)} market snapshots written"

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

    def _step_briefing_build(self) -> str:
        from services.briefing_builder import BriefingBuilder
        builder = BriefingBuilder(supabase_client=self.sb)
        report = builder.build_and_store(regime="risk_on")
        return f"briefing built for {report['report_date']}"

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
