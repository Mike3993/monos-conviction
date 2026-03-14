"""
conviction_map_engine.py

Builds and scores the conviction map across all active positions and ladders.
Combines regime context, Greeks exposure, and macro signals to produce
a normalized conviction score per position or thesis.

Stores scored results in simulation_runs for historical tracking.
"""

import json
import logging
import math
import os
import sys
from datetime import datetime, date

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from engines.greeks_engine import GreeksEngine, _get_spot, _years_to_expiry, DEFAULT_IV
from utils.supabase_helpers import get_supabase_client, write_agent_log

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)

AGENT_NAME = "conviction_map_engine"

# ---------------------------------------------------------------------------
# Regime multipliers  (maps regime_engine labels -> conviction weight)
# ---------------------------------------------------------------------------

REGIME_WEIGHTS: dict[str, float] = {
    "risk_on":          1.20,
    "reflation":        1.15,
    "vol_compression":  1.10,
    "risk_off":         0.75,
    "stagflation":      0.70,
    "deflation_scare":  0.65,
    "vol_expansion":    0.80,
}


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

def _moneyness(spot: float, strike: float) -> float:
    """Return log-moneyness (positive = ITM for calls)."""
    if strike <= 0 or spot <= 0:
        return 0.0
    return math.log(spot / strike)


def _time_value_score(expiration_str: str) -> float:
    """
    Score from 0-1 reflecting how much time value remains.
    Longer-dated legs score higher.
    """
    T = _years_to_expiry(expiration_str)
    return 1 - math.exp(-1.5 * T)


def _gamma_convexity_score(gamma: float, vega: float) -> float:
    """
    Higher gamma and vega relative to cost indicate better convexity.
    Returns a 0-1 score.
    """
    raw = abs(gamma) * 1000 + abs(vega) / 100
    return min(raw / 5.0, 1.0)


class ConvictionMapEngine:
    """
    Produces conviction scores for each position in the portfolio.
    Scores are used by the supervisor agent to prioritize the nightly briefing.
    """

    def __init__(self, supabase_client=None, regime: str = "risk_on"):
        if supabase_client is None:
            self.sb = get_supabase_client()
        else:
            self.sb = supabase_client

        self.regime = regime
        self.greeks_engine = GreeksEngine(supabase_client=self.sb)

    # ------------------------------------------------------------------ core

    def score_position(self, leg: dict) -> dict:
        """
        Score a single position leg on a 0-100 conviction / convexity scale.

        Components
        ----------
        1. Moneyness factor   (0-1) : ITM or near-the-money = higher
        2. Time value factor   (0-1) : more DTE = higher
        3. Gamma/vega convexity(0-1) : higher gamma+vega = higher
        4. Regime multiplier   (0.6-1.2)
        """
        ticker = leg["ticker"]
        spot = _get_spot(ticker)
        strike = float(leg["strike"])
        expiration = leg["expiration"]

        # 1. Moneyness
        m = _moneyness(spot, strike)
        moneyness_score = max(0.0, min(1.0, 0.5 + m * 5))

        # 2. Time value
        time_score = _time_value_score(expiration)

        # 3. Greeks-based convexity
        greeks = self.greeks_engine.compute_greeks(leg)
        convexity_score = _gamma_convexity_score(greeks["gamma"], greeks["vega"])

        # 4. Regime
        regime_mult = REGIME_WEIGHTS.get(self.regime, 1.0)

        # Weighted combination -> 0-100
        raw = (0.25 * moneyness_score
               + 0.30 * time_score
               + 0.30 * convexity_score
               + 0.15 * (regime_mult - 0.5))

        score = round(min(max(raw * 100, 0), 100), 2)

        result = {
            "ticker": ticker,
            "leg_type": leg.get("leg_type"),
            "strike": strike,
            "expiration": expiration,
            "conviction_score": score,
            "moneyness": round(moneyness_score, 4),
            "time_value": round(time_score, 4),
            "convexity": round(convexity_score, 4),
            "regime": self.regime,
            "regime_mult": regime_mult,
            "delta": float(greeks["delta"]),
            "gamma": float(greeks["gamma"]),
        }
        logger.info("Conviction score for %s %s K=%.0f: %.2f",
                     ticker, leg.get("leg_type"), strike, score)
        return result

    def score_positions(self, legs: list[dict]) -> list[dict]:
        """Score every leg and return sorted by conviction descending."""
        scored = [self.score_position(leg) for leg in legs]
        scored.sort(key=lambda x: x["conviction_score"], reverse=True)
        return scored

    # ------------------------------------------------------ Supabase-backed

    def run_from_supabase(self) -> list[dict]:
        """
        Pull position_legs from Supabase, score them, store in simulation_runs,
        and return results.
        """
        logger.info("=== Conviction map scoring started ===")

        resp = self.sb.table("position_legs").select("*").execute()
        legs = resp.data
        logger.info("Scoring %d legs against regime='%s'", len(legs), self.regime)

        enriched_legs = []
        for leg in legs:
            enriched_legs.append({
                "ticker": leg.get("ticker", ""),
                "leg_type": leg.get("leg_type", "LONG_CALL"),
                "strike": leg.get("strike", 100),
                "expiration": leg.get("expiration", "2026-12-18"),
            })

        scores = self.score_positions(enriched_legs)

        # Persist to simulation_runs
        if scores:
            sim_row = {
                "engine": "conviction_map",
                "parameters": {
                    "regime": self.regime,
                    "legs_scored": len(scores),
                    "run_date": date.today().isoformat(),
                },
                "result": {
                    "scores": scores,
                    "top_conviction": scores[0] if scores else None,
                    "avg_score": round(
                        sum(s["conviction_score"] for s in scores) / len(scores), 2
                    ),
                },
            }
            logger.info("Writing simulation_runs record...")
            self.sb.table("simulation_runs").insert(sim_row).execute()
            logger.info("simulation_runs record written")

        # Agent log
        write_agent_log(self.sb, AGENT_NAME, "run_from_supabase",
                        "success", {
                            "legs_scored": len(scores),
                            "regime": self.regime,
                            "top_score": scores[0]["conviction_score"] if scores else 0,
                        })

        logger.info("=== Conviction map scoring complete ===")
        return scores


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    engine = ConvictionMapEngine(regime="risk_on")
    scores = engine.run_from_supabase()
    print("\n=== Conviction Map ===")
    for s in scores:
        print(f"  {s['ticker']} {s['leg_type']} K={s['strike']:.0f} "
              f"exp={s['expiration']}  -> conviction={s['conviction_score']:.2f}")
