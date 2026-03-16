"""
dealer_positioning_engine.py

Computes dealer positioning metrics from options chain data.

GEX per strike:
    GEX(K) = (call_gamma × call_OI) − (put_gamma × put_OI)

Determines:
    gamma_flip  — strike where cumulative GEX crosses zero
    call_wall   — strike with highest call GEX
    put_wall    — strike with highest put GEX (most negative)

Regime:
    spot < gamma_flip  →  NEGATIVE  (dealer short gamma, volatile)
    spot >= gamma_flip →  POSITIVE  (dealer long gamma, mean-reverting)

Phase-1 uses synthetic/simulated OI data.
Phase-2 will integrate Polygon or CBOE feeds.
"""

import logging
import math
import random
from dataclasses import dataclass

import yfinance as yf

from monos.storage.supabase_client import write_agent_log
from monos.storage.dealer_repository import write_positioning

logger = logging.getLogger(__name__)

AGENT = "dealer_positioning_engine"


@dataclass
class DealerPositioning:
    ticker: str
    spot: float
    gamma_flip: float
    call_wall: float
    put_wall: float
    gamma_regime: str
    gex_by_strike: dict[float, float]


class DealerPositioningEngine:
    """
    Computes dealer gamma exposure and positioning for a list of tickers.
    """

    def _get_spot(self, ticker: str) -> float:
        """Fetch current spot price via yfinance."""
        try:
            t = yf.Ticker(ticker)
            price = float(t.fast_info.get("lastPrice", 0)
                          or t.fast_info.get("previousClose", 0))
            return price if price > 0 else 100.0
        except Exception:
            logger.warning("Failed to fetch spot for %s, using placeholder", ticker)
            return 100.0

    def _generate_synthetic_chain(self, spot: float) -> list[dict]:
        """
        Generate synthetic option chain OI data around spot.
        Phase-2 replaces this with live chain from Polygon.
        """
        strikes = []
        base = round(spot * 0.85, -1)
        top = round(spot * 1.15, -1)
        step = max(round((top - base) / 20, -1), 1)

        k = base
        while k <= top:
            # Synthetic gamma peaks near ATM
            dist = abs(k - spot) / spot
            gamma_base = max(0.001, 0.05 * math.exp(-8 * dist * dist))

            call_oi = random.randint(500, 15000)
            put_oi  = random.randint(500, 15000)
            call_gamma = gamma_base * (1 + random.uniform(-0.2, 0.2))
            put_gamma  = gamma_base * (1 + random.uniform(-0.2, 0.2))

            strikes.append({
                "strike": k,
                "call_gamma": call_gamma,
                "call_oi": call_oi,
                "put_gamma": put_gamma,
                "put_oi": put_oi,
            })
            k += step

        return strikes

    def compute(self, ticker: str) -> DealerPositioning:
        """
        Compute dealer positioning for a single ticker.
        """
        spot = self._get_spot(ticker)
        chain = self._generate_synthetic_chain(spot)

        # GEX per strike
        gex_by_strike: dict[float, float] = {}
        for row in chain:
            k = row["strike"]
            gex = (row["call_gamma"] * row["call_oi"]
                   - row["put_gamma"] * row["put_oi"])
            gex_by_strike[k] = round(gex, 4)

        # Gamma flip: strike nearest zero-crossing of cumulative GEX
        sorted_strikes = sorted(gex_by_strike.keys())
        cumulative = 0.0
        gamma_flip = spot  # default
        for k in sorted_strikes:
            prev_cum = cumulative
            cumulative += gex_by_strike[k]
            if prev_cum <= 0 < cumulative:
                gamma_flip = k
                break

        # Call wall: strike with max positive GEX
        call_wall = max(gex_by_strike, key=lambda k: gex_by_strike[k])

        # Put wall: strike with most negative GEX
        put_wall = min(gex_by_strike, key=lambda k: gex_by_strike[k])

        # Regime
        gamma_regime = "POSITIVE" if spot >= gamma_flip else "NEGATIVE"

        return DealerPositioning(
            ticker=ticker,
            spot=spot,
            gamma_flip=gamma_flip,
            call_wall=call_wall,
            put_wall=put_wall,
            gamma_regime=gamma_regime,
            gex_by_strike=gex_by_strike,
        )

    def run(self, tickers: list[str]) -> list[DealerPositioning]:
        """
        Compute dealer positioning for all tickers and persist.
        """
        logger.info("=== Dealer positioning engine started (%d tickers) ===",
                     len(tickers))

        results = []
        rows_to_write = []

        for ticker in tickers:
            try:
                dp = self.compute(ticker)
                results.append(dp)
                rows_to_write.append({
                    "ticker": dp.ticker,
                    "gamma_flip": dp.gamma_flip,
                    "call_wall": dp.call_wall,
                    "put_wall": dp.put_wall,
                    "gamma_regime": dp.gamma_regime,
                })
                logger.info("%s: spot=%.2f flip=%.0f regime=%s call_wall=%.0f put_wall=%.0f",
                            dp.ticker, dp.spot, dp.gamma_flip,
                            dp.gamma_regime, dp.call_wall, dp.put_wall)
            except Exception:
                logger.exception("Failed to compute dealer positioning for %s", ticker)

        write_positioning(rows_to_write)

        write_agent_log(AGENT, "run", "success", {
            "tickers": len(results),
            "regimes": {r.ticker: r.gamma_regime for r in results},
        })

        logger.info("=== Dealer positioning engine complete ===")
        return results


# ── CLI ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
    engine = DealerPositioningEngine()
    for dp in engine.run(["SPY", "QQQ", "GLD"]):
        print(f"  {dp.ticker}: regime={dp.gamma_regime} flip={dp.gamma_flip}")
