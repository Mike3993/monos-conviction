"""
portfolio_analyzer.py

Computes portfolio-level risk metrics by joining greeks_snapshots
with position_legs.  Produces net delta, gamma, theta, and vega
exposure scaled by quantity.

Results are persisted to simulation_runs and logged to agent_logs.
"""

import logging
import os
import sys
from datetime import date

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.supabase_helpers import get_supabase_client, write_agent_log

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)

AGENT_NAME = "portfolio_analyzer"


class PortfolioAnalyzer:
    """
    Joins greeks_snapshots with position_legs, scales by quantity,
    and returns net portfolio-level Greeks exposure.
    """

    def __init__(self, supabase_client=None):
        self.sb = supabase_client or get_supabase_client()

    def fetch_legs(self) -> pd.DataFrame:
        resp = self.sb.table("position_legs").select("*").execute()
        logger.info("Fetched %d position_legs rows", len(resp.data))
        return pd.DataFrame(resp.data)

    def fetch_greeks(self) -> pd.DataFrame:
        resp = (self.sb.table("greeks_snapshots")
                .select("*")
                .order("created_at", desc=True)
                .limit(500)
                .execute())
        logger.info("Fetched %d greeks_snapshots rows", len(resp.data))
        return pd.DataFrame(resp.data)

    def compute_portfolio_metrics(self) -> dict | None:
        """
        Join greeks to legs, scale by quantity, return net exposure.
        """
        legs = self.fetch_legs()
        greeks = self.fetch_greeks()

        if legs.empty or greeks.empty:
            logger.warning("No data available for portfolio analysis")
            return None

        df = greeks.merge(
            legs,
            left_on="position_id",
            right_on="id",
            how="inner",
        )
        logger.info("Merged rows (inner join): %d", len(df))

        if df.empty:
            logger.warning("Join produced zero rows")
            return None

        df["quantity"] = df["quantity"].apply(
            lambda q: float(q) if q is not None and q == q else 1.0
        )

        df["delta_exposure"] = df["delta"].astype(float) * df["quantity"]
        df["gamma_exposure"] = df["gamma"].astype(float) * df["quantity"]
        df["theta_exposure"] = df["theta"].astype(float) * df["quantity"]
        df["vega_exposure"] = df["vega"].astype(float) * df["quantity"]

        portfolio = {
            "net_delta": round(df["delta_exposure"].sum(), 4),
            "net_gamma": round(df["gamma_exposure"].sum(), 6),
            "net_theta": round(df["theta_exposure"].sum(), 4),
            "net_vega": round(df["vega_exposure"].sum(), 4),
            "legs_analysed": len(df),
        }
        return portfolio

    def run_and_store(self) -> dict | None:
        """Compute metrics, persist to simulation_runs, log to agent_logs."""
        logger.info("=== Portfolio Analyzer started ===")

        metrics = self.compute_portfolio_metrics()

        if metrics is None:
            write_agent_log(self.sb, AGENT_NAME, "run_and_store", "empty",
                            {"reason": "no data"})
            logger.warning("Portfolio analysis produced no results")
            return None

        # Print report
        print("\nPortfolio Risk Summary")
        print("-" * 30)
        print(f"  Net Delta:  {metrics['net_delta']:>+10.4f}")
        print(f"  Net Gamma:  {metrics['net_gamma']:>10.6f}")
        print(f"  Theta Burn: {metrics['net_theta']:>+10.4f}")
        print(f"  Net Vega:   {metrics['net_vega']:>+10.4f}")
        print(f"  Legs:       {metrics['legs_analysed']}")

        # Persist
        payload = {
            "engine": "portfolio_analyzer",
            "parameters": {"run_date": date.today().isoformat()},
            "result": metrics,
        }
        self.sb.table("simulation_runs").insert(payload).execute()
        logger.info("simulation_runs record written (portfolio_analyzer)")

        write_agent_log(self.sb, AGENT_NAME, "run_and_store", "success", metrics)

        logger.info("=== Portfolio Analyzer complete ===")
        return metrics


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    analyzer = PortfolioAnalyzer()
    analyzer.run_and_store()
