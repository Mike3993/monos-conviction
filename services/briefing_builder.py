"""
briefing_builder.py

Assembles the nightly conviction briefing from scored positions,
regime state, Greeks summary, and portfolio-level risk metrics.
Aggregates data from ladders, positions, greeks_snapshots, and
conviction scores (simulation_runs), then writes a JSON report
to briefing_reports.
"""

import logging
import os
import sys
from datetime import date, datetime

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

AGENT_NAME = "briefing_builder"


class BriefingBuilder:
    """
    Composes the nightly briefing payload from engine outputs.
    Supports multiple output formats (Markdown, JSON, Slack message).
    """

    def __init__(self, supabase_client=None):
        self.sb = supabase_client or get_supabase_client()

    # ---------------------------------------------------------- data loaders

    def _fetch_ladders(self) -> list[dict]:
        resp = self.sb.table("ladders").select("*").execute()
        logger.info("Loaded %d ladders", len(resp.data))
        return resp.data

    def _fetch_positions(self) -> list[dict]:
        resp = self.sb.table("positions").select("*").execute()
        logger.info("Loaded %d positions", len(resp.data))
        return resp.data

    def _fetch_latest_greeks(self) -> list[dict]:
        """Return the most recent greeks_snapshots batch (last 20 rows)."""
        resp = (self.sb.table("greeks_snapshots")
                .select("*")
                .order("created_at", desc=True)
                .limit(20)
                .execute())
        logger.info("Loaded %d greeks_snapshot rows", len(resp.data))
        return resp.data

    def _fetch_latest_conviction(self) -> dict | None:
        """Return the most recent simulation_runs row from conviction_map."""
        resp = (self.sb.table("simulation_runs")
                .select("*")
                .eq("engine", "conviction_map")
                .order("created_at", desc=True)
                .limit(1)
                .execute())
        if resp.data:
            logger.info("Loaded latest conviction simulation run")
            return resp.data[0]
        logger.warning("No conviction simulation_runs found")
        return None

    def _fetch_market_snapshots(self) -> list[dict]:
        resp = (self.sb.table("market_snapshots")
                .select("*")
                .order("created_at", desc=True)
                .limit(10)
                .execute())
        logger.info("Loaded %d market_snapshot rows", len(resp.data))
        return resp.data

    # ---------------------------------------------------------- aggregation

    def _aggregate_greeks(self, greeks: list[dict]) -> dict:
        """Sum greeks across all snapshot rows."""
        totals = {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}
        for g in greeks:
            for k in totals:
                totals[k] += float(g.get(k, 0) or 0)
        return {k: round(v, 4) for k, v in totals.items()}

    # ---------------------------------------------------------- build report

    def build(self, regime: str = "risk_on") -> dict:
        """
        Assemble the full nightly briefing JSON from all data sources.
        """
        logger.info("=== Briefing build started ===")

        ladders = self._fetch_ladders()
        positions = self._fetch_positions()
        greeks = self._fetch_latest_greeks()
        conviction_run = self._fetch_latest_conviction()
        market = self._fetch_market_snapshots()

        greeks_summary = self._aggregate_greeks(greeks)

        conviction_scores = []
        avg_conviction = 0.0
        if conviction_run and conviction_run.get("result"):
            result_data = conviction_run["result"]
            if isinstance(result_data, str):
                import json
                result_data = json.loads(result_data)
            conviction_scores = result_data.get("scores", [])
            avg_conviction = result_data.get("avg_score", 0.0)

        report = {
            "report_date": date.today().isoformat(),
            "generated_at": datetime.utcnow().isoformat(),
            "regime": regime,
            "portfolio_summary": {
                "total_ladders": len(ladders),
                "total_positions": len(positions),
                "unique_tickers": list({p.get("ticker", "") for p in positions}),
            },
            "greeks_summary": greeks_summary,
            "conviction_summary": {
                "avg_score": avg_conviction,
                "top_positions": conviction_scores[:5],
                "total_scored": len(conviction_scores),
            },
            "market_snapshot": [
                {"ticker": m.get("ticker"), "price": m.get("price"),
                 "volume": m.get("volume")}
                for m in market
            ],
            "ladders": [
                {"name": l.get("ladder_name") or l.get("name"),
                 "underlying": l.get("underlying") or l.get("ticker"),
                 "notional": l.get("notional")}
                for l in ladders
            ],
        }

        logger.info("Briefing assembled: %d ladders, %d positions, "
                     "avg conviction=%.2f",
                     len(ladders), len(positions), avg_conviction)
        return report

    # ---------------------------------------------------------- persistence

    def build_and_store(self, regime: str = "risk_on") -> dict:
        """Build the report and persist to briefing_reports table."""
        report = self.build(regime=regime)

        row = {
            "report_date": date.today().isoformat(),
            "content": report,
        }
        logger.info("Writing briefing_reports row...")
        self.sb.table("briefing_reports").insert(row).execute()
        logger.info("briefing_reports row written successfully")

        write_agent_log(self.sb, AGENT_NAME, "build_and_store",
                        "success", {
                            "report_date": date.today().isoformat(),
                            "ladders": report["portfolio_summary"]["total_ladders"],
                            "positions": report["portfolio_summary"]["total_positions"],
                            "avg_conviction": report["conviction_summary"]["avg_score"],
                        })

        logger.info("=== Briefing build complete ===")
        return report

    # ---------------------------------------------------------- rendering

    def render_markdown(self, briefing: dict) -> str:
        """Format briefing as a readable Markdown report."""
        lines = [
            f"# MONOS Conviction Briefing — {briefing['report_date']}",
            f"**Regime:** {briefing['regime']}",
            f"**Generated:** {briefing['generated_at']}",
            "",
            "## Portfolio Summary",
            f"- Ladders: {briefing['portfolio_summary']['total_ladders']}",
            f"- Positions: {briefing['portfolio_summary']['total_positions']}",
            f"- Tickers: {', '.join(briefing['portfolio_summary']['unique_tickers'])}",
            "",
            "## Greeks Summary",
        ]
        gs = briefing["greeks_summary"]
        lines.append(f"- Delta: {gs['delta']}  |  Gamma: {gs['gamma']}  "
                      f"|  Theta: {gs['theta']}  |  Vega: {gs['vega']}")
        lines += [
            "",
            "## Conviction Summary",
            f"- Average Score: {briefing['conviction_summary']['avg_score']}",
            f"- Positions Scored: {briefing['conviction_summary']['total_scored']}",
        ]
        for i, pos in enumerate(briefing["conviction_summary"]["top_positions"], 1):
            lines.append(f"  {i}. {pos.get('ticker')} {pos.get('leg_type')} "
                         f"K={pos.get('strike')} -> {pos.get('conviction_score')}")

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    builder = BriefingBuilder()
    report = builder.build_and_store()
    md = builder.render_markdown(report)
    print(md)
