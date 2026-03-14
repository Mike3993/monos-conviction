"""
greeks_engine.py

Calculates and monitors options Greeks (delta, gamma, vega, theta, rho)
for all positions in the conviction portfolio. Surfaces convexity exposure
and flags Greeks thresholds for review.

Reads spot prices from market_snapshots when available, falling back to
hardcoded overrides. Writes results to greeks_snapshots and logs to agent_logs.
"""

import logging
import math
import os
import sys
from datetime import datetime, date

from dotenv import load_dotenv
from scipy.stats import norm

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.supabase_helpers import get_supabase_client, write_agent_log

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)

AGENT_NAME = "greeks_engine"

# ---------------------------------------------------------------------------
# Black-Scholes helpers
# ---------------------------------------------------------------------------

RISK_FREE_RATE = 0.045  # ~current Fed funds proxy
DEFAULT_IV = 0.20       # fallback implied vol

# Static fallbacks when no market_snapshots row exists
SPOT_OVERRIDES: dict[str, float] = {
    "SPY": 564.0,
    "SLV": 30.0,
}


def _years_to_expiry(expiration_str: str) -> float:
    """Return time to expiry in years from today."""
    exp = datetime.strptime(expiration_str, "%Y-%m-%d").date()
    delta = (exp - date.today()).days
    return max(delta / 365.0, 1 / 365.0)  # floor at 1 day


def _d1(S: float, K: float, T: float, r: float, sigma: float) -> float:
    return (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))


def _d2(S: float, K: float, T: float, r: float, sigma: float) -> float:
    return _d1(S, K, T, r, sigma) - sigma * math.sqrt(T)


def bs_greeks(S: float, K: float, T: float, r: float, sigma: float,
              option_type: str = "call") -> dict:
    """
    Return Black-Scholes Greeks for a European option.

    Parameters
    ----------
    S : spot price
    K : strike price
    T : time to expiry (years)
    r : risk-free rate
    sigma : implied volatility
    option_type : 'call' or 'put'
    """
    d1 = _d1(S, K, T, r, sigma)
    d2 = _d2(S, K, T, r, sigma)

    if option_type == "call":
        delta = norm.cdf(d1)
        rho = K * T * math.exp(-r * T) * norm.cdf(d2)
    else:
        delta = norm.cdf(d1) - 1
        rho = -K * T * math.exp(-r * T) * norm.cdf(-d2)

    gamma = norm.pdf(d1) / (S * sigma * math.sqrt(T))
    vega = S * norm.pdf(d1) * math.sqrt(T)            # per 1 vol point
    theta = (-(S * norm.pdf(d1) * sigma) / (2 * math.sqrt(T))
             - r * K * math.exp(-r * T) * norm.cdf(d2 if option_type == "call" else -d2))

    return {
        "delta": round(delta, 6),
        "gamma": round(gamma, 6),
        "vega":  round(vega, 6),
        "theta": round(theta, 6),
        "rho":   round(rho, 6),
        "iv":    round(sigma, 6),
    }


def _get_spot(ticker: str) -> float:
    """Return a spot price from static overrides (used when no DB available)."""
    return SPOT_OVERRIDES.get(ticker, 100.0)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class GreeksEngine:
    """
    Computes Greeks for individual positions and aggregates them
    across ladders and the full portfolio.
    """

    def __init__(self, supabase_client=None):
        if supabase_client is None:
            self.sb = get_supabase_client()
        else:
            self.sb = supabase_client
        self._spot_cache: dict[str, float] = {}

    # ---- spot from market_snapshots ----------------------------------------

    def _load_spot_prices(self) -> None:
        """Load latest spot prices from market_snapshots into cache."""
        try:
            resp = (self.sb.table("market_snapshots")
                    .select("ticker, price")
                    .order("created_at", desc=True)
                    .execute())
            seen: set[str] = set()
            for row in resp.data:
                t = row["ticker"]
                if t not in seen:
                    self._spot_cache[t] = float(row["price"])
                    seen.add(t)
            logger.info("Loaded spot prices from market_snapshots: %s", self._spot_cache)
        except Exception:
            logger.warning("Could not load market_snapshots; using static overrides")

    def _get_spot_price(self, ticker: str) -> float:
        """Return spot from market_snapshots cache, then static fallback."""
        if ticker in self._spot_cache:
            return self._spot_cache[ticker]
        return SPOT_OVERRIDES.get(ticker, 100.0)

    # ---- per-leg calculation -----------------------------------------------

    def compute_greeks(self, leg: dict) -> dict:
        """
        Compute Greeks for a single position leg.

        Parameters
        ----------
        leg : dict with keys ticker, leg_type, strike, expiration
              (optionally iv, spot)

        Returns
        -------
        dict with delta, gamma, vega, theta, rho, iv
        """
        ticker = leg["ticker"]
        S = leg.get("spot", self._get_spot_price(ticker))
        K = float(leg["strike"])
        T = _years_to_expiry(leg["expiration"])
        sigma = leg.get("iv", DEFAULT_IV)
        r = RISK_FREE_RATE

        leg_type = leg.get("leg_type", "LONG_CALL").upper()
        option_type = "put" if "PUT" in leg_type else "call"

        greeks = bs_greeks(S, K, T, r, sigma, option_type)

        # Flip sign for short legs
        if "SHORT" in leg_type:
            for key in ("delta", "gamma", "vega", "theta", "rho"):
                greeks[key] = -greeks[key]

        logger.info("Greeks for %s %s K=%.0f: delta=%.4f gamma=%.6f vega=%.4f theta=%.4f",
                     ticker, leg_type, K, greeks["delta"], greeks["gamma"],
                     greeks["vega"], greeks["theta"])
        return greeks

    # ---- portfolio aggregation ---------------------------------------------

    def aggregate_portfolio_greeks(self, legs: list[dict]) -> dict:
        """Sum Greeks across all position legs."""
        totals: dict[str, float] = {
            "delta": 0.0, "gamma": 0.0, "vega": 0.0,
            "theta": 0.0, "rho": 0.0,
        }
        for leg in legs:
            g = self.compute_greeks(leg)
            for key in totals:
                totals[key] += g[key]

        totals = {k: round(v, 6) for k, v in totals.items()}
        logger.info("Aggregate portfolio Greeks: %s", totals)
        return totals

    # ---- Supabase persistence ----------------------------------------------

    def snapshot_and_store(self) -> list[dict]:
        """
        Fetch all position_legs from Supabase, compute Greeks for each,
        and write a greeks_snapshots row per leg.

        Returns the list of snapshot rows written.
        """
        logger.info("=== Greeks snapshot started ===")

        # Try to load live spot prices first
        self._load_spot_prices()

        logger.info("Fetching position legs from Supabase...")
        resp = self.sb.table("position_legs").select("*").execute()
        legs = resp.data
        logger.info("Found %d position legs", len(legs))

        snapshots: list[dict] = []
        for leg in legs:
            greeks = self.compute_greeks({
                "ticker": leg.get("ticker", ""),
                "leg_type": leg.get("leg_type", "LONG_CALL"),
                "strike": leg.get("strike", 100),
                "expiration": leg.get("expiration", "2026-12-18"),
                "iv": float(leg["iv"]) if leg.get("iv") else DEFAULT_IV,
            })

            row = {
                "position_id": leg.get("id"),  # link to position_legs.id
                "delta": float(greeks["delta"]),
                "gamma": float(greeks["gamma"]),
                "theta": float(greeks["theta"]),
                "vega":  float(greeks["vega"]),
                "iv":    float(greeks["iv"]),
            }
            snapshots.append(row)

        if snapshots:
            logger.info("Writing %d greeks_snapshots rows...", len(snapshots))
            self.sb.table("greeks_snapshots").insert(snapshots).execute()
            logger.info("greeks_snapshots written successfully")
        else:
            logger.warning("No legs found; nothing to snapshot")

        # Write agent_log
        write_agent_log(self.sb, AGENT_NAME, "snapshot_and_store",
                        "success", {
                            "legs_processed": len(legs),
                            "snapshots_written": len(snapshots),
                            "spot_sources": dict(self._spot_cache),
                        })

        logger.info("=== Greeks snapshot complete ===")
        return snapshots


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    engine = GreeksEngine()
    results = engine.snapshot_and_store()
    for r in results:
        print(r)
