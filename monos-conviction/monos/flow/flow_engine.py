"""
flow_engine.py

Skeleton module for options flow intelligence.

Phase-1: generates placeholder flow data.
Phase-2: integrate Polygon / Unusual Whales feeds.

Writes to flow.snapshots.
"""

import logging
import random
from datetime import datetime

from monos.storage.flow_repository import write_snapshots
from monos.storage.supabase_client import write_agent_log

logger = logging.getLogger(__name__)

AGENT = "flow_engine"


class FlowEngine:
    """
    Options flow aggregator.

    Phase-1 produces synthetic data to validate the pipeline.
    Phase-2 will integrate:
        - Polygon options trades API
        - Unusual Whales flow feed
    """

    def _generate_placeholder(self, ticker: str) -> dict:
        """Generate placeholder flow snapshot for one ticker."""
        call_vol = random.randint(5000, 80000)
        put_vol  = random.randint(3000, 60000)
        ratio = round(call_vol / max(put_vol, 1), 2)

        # Determine signal
        if ratio > 1.5:
            signal = "BULLISH_FLOW"
        elif ratio < 0.7:
            signal = "BEARISH_FLOW"
        else:
            signal = "NEUTRAL"

        return {
            "ticker": ticker,
            "call_volume": call_vol,
            "put_volume": put_vol,
            "call_put_ratio": ratio,
            "largest_trade": {
                "type": random.choice(["CALL", "PUT"]),
                "premium": random.randint(50000, 2000000),
                "strike": round(random.uniform(80, 800), 0),
                "expiry": "2026-06-19",
                "side": random.choice(["BUY", "SELL"]),
            },
            "flow_signal": signal,
        }

    def run(self, tickers: list[str]) -> list[dict]:
        """
        Generate flow snapshots for all tickers and persist.
        """
        logger.info("=== Flow engine started (%d tickers) [PLACEHOLDER] ===",
                     len(tickers))

        snapshots = []
        for ticker in tickers:
            try:
                snap = self._generate_placeholder(ticker)
                snapshots.append(snap)
                logger.info("%s: C/P=%.2f signal=%s",
                            ticker, snap["call_put_ratio"], snap["flow_signal"])
            except Exception:
                logger.exception("Failed to generate flow for %s", ticker)

        write_snapshots(snapshots)

        write_agent_log(AGENT, "run", "success", {
            "tickers": len(snapshots),
            "signals": {s["flow_signal"]: sum(1 for x in snapshots
                       if x["flow_signal"] == s["flow_signal"])
                       for s in snapshots},
        })

        logger.info("=== Flow engine complete [PLACEHOLDER] ===")
        return snapshots
