"""
convexity_heatmap_engine.py

Builds a gamma-concentration heatmap by joining the latest greeks_snapshots
with position_legs to recover each leg's strike, then grouping by strike
and summing gamma.

The join key is:  greeks_snapshots.position_id  =  position_legs.id

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

AGENT_NAME = "convexity_heatmap_engine"


class ConvexityHeatmapEngine:
    """
    Produces a strike-level gamma heatmap by joining greeks_snapshots
    back to position_legs (which holds the strike price).
    """

    def __init__(self, supabase_client=None):
        self.sb = supabase_client or get_supabase_client()

    # ------------------------------------------------------------- loaders

    def _fetch_latest_greeks(self) -> pd.DataFrame:
        """Fetch the most recent batch of greeks_snapshots."""
        resp = (self.sb.table("greeks_snapshots")
                .select("position_id, delta, gamma, theta, vega, created_at")
                .order("created_at", desc=True)
                .limit(200)
                .execute())
        logger.info("Fetched %d greeks_snapshot rows", len(resp.data))
        return pd.DataFrame(resp.data)

    def _fetch_legs(self) -> pd.DataFrame:
        """Fetch position_legs with id, ticker, and strike."""
        resp = (self.sb.table("position_legs")
                .select("id, ticker, strike, expiration")
                .execute())
        logger.info("Fetched %d position_legs rows", len(resp.data))
        return pd.DataFrame(resp.data)

    # ------------------------------------------------------------- core

    def build_heatmap(self) -> pd.DataFrame:
        """
        Join greeks_snapshots → position_legs on position_id = id,
        then group by strike and sum gamma.

        Returns a DataFrame with columns: strike, gamma (sorted desc).
        """
        greeks_df = self._fetch_latest_greeks()
        legs_df = self._fetch_legs()

        if greeks_df.empty or legs_df.empty:
            logger.warning("Insufficient data for heatmap (greeks=%d, legs=%d)",
                           len(greeks_df), len(legs_df))
            return pd.DataFrame(columns=["strike", "gamma"])

        # Filter to snapshots that actually link to a leg
        linked = greeks_df[greeks_df["position_id"].notna()]
        logger.info("greeks_snapshots with non-null position_id: %d / %d",
                     len(linked), len(greeks_df))

        if linked.empty:
            logger.warning("No linked greeks_snapshots found — position_id "
                           "linkage may not yet exist. Returning empty heatmap.")
            return pd.DataFrame(columns=["strike", "gamma"])

        # Join: greeks_snapshots.position_id = position_legs.id
        merged = linked.merge(
            legs_df,
            left_on="position_id",
            right_on="id",
            how="inner",
        )
        logger.info("Merged rows (inner join): %d", len(merged))

        if merged.empty:
            logger.warning("Join produced zero rows — IDs may not match")
            return pd.DataFrame(columns=["strike", "gamma"])

        # Aggregate: group by strike, sum gamma
        heatmap = (merged
                   .groupby("strike", as_index=False)["gamma"]
                   .sum()
                   .sort_values("gamma", ascending=False)
                   .reset_index(drop=True))

        heatmap["strike"] = heatmap["strike"].astype(float)
        heatmap["gamma"] = heatmap["gamma"].round(6)

        return heatmap

    # --------------------------------------------------------- persistence

    def run_and_store(self) -> list[dict]:
        """
        Build the heatmap, persist to simulation_runs, and log to agent_logs.
        Returns the heatmap records.
        """
        logger.info("=== Convexity heatmap engine started ===")

        heatmap = self.build_heatmap()
        records = heatmap.to_dict(orient="records")

        if records:
            print("\nTop Gamma Concentrations:")
            print(f"{'strike':>10} | {'gamma':>12}")
            print("-" * 26)
            for row in records:
                print(f"{row['strike']:>10.0f} | {row['gamma']:>12.6f}")

            payload = {
                "engine": "convexity_heatmap",
                "parameters": {
                    "run_date": date.today().isoformat(),
                    "strikes_counted": len(records),
                },
                "result": records,
            }
            logger.info("Writing simulation_runs record...")
            self.sb.table("simulation_runs").insert(payload).execute()
            logger.info("simulation_runs record written")
        else:
            logger.warning("Heatmap is empty — nothing to store")

        write_agent_log(self.sb, AGENT_NAME, "run_and_store",
                        "success" if records else "empty", {
                            "strikes": len(records),
                            "top_strike": records[0] if records else None,
                        })

        logger.info("=== Convexity heatmap engine complete ===")
        return records


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    engine = ConvexityHeatmapEngine()
    results = engine.run_and_store()
