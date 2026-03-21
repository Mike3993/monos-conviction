"""
vol_surface_engine.py

Computes volatility surface metrics for option structure evaluation.

Metrics
-------
1. IV Rank
    (current_IV − 52wk_low) / (52wk_high − 52wk_low)
    Contextualises current IV within its annual range (0–100).

2. IV Percentile
    Percentage of trading days in the past year where IV was lower
    than today's IV.

3. Skew Slope
    (25Δ put IV − 25Δ call IV) / ATM IV
    Measures the cost of downside protection relative to upside.
    Higher = steeper put skew = more fear premium.

4. Smile Curvature
    (put_wing_IV + call_wing_IV) / 2 − ATM_IV
    Measures convexity of the smile. Higher = fatter tails priced in.

5. Term Structure
    IV30 / IV90
    > 1 = backwardation (near-term fear), < 1 = contango (normal).

6. Realized vs Implied Gap
    (IV30 − HV20) / HV20
    Positive = options are expensive relative to realised vol.

Data Sources (Phase-1)
----------------------
- yfinance: historical prices → HV20 / HV60 / HV252
- yfinance: options chain → implied vols per strike/expiry
- Synthetic fallbacks when options chain unavailable

Phase-2 will integrate Polygon / CBOE data feeds.
"""

import logging
import math
from dataclasses import dataclass, asdict
from datetime import datetime

import numpy as np
import yfinance as yf

from monos.storage.vol_repository import write_surface
from monos.storage.supabase_client import write_agent_log

logger = logging.getLogger(__name__)

AGENT = "vol_surface_engine"


@dataclass
class VolSurface:
    """Volatility surface snapshot for a single ticker."""
    ticker: str
    iv_rank: float            # 0–100
    iv_percentile: float      # 0–100
    skew_slope: float         # typically -0.3 to +0.3
    smile_curvature: float    # typically 0 to 0.15
    term_structure: float     # ratio: IV30 / IV90
    iv_rv_gap: float          # (IV - HV) / HV


# ── Historical volatility helpers ─────────────────────────────────

def _compute_hv(prices: np.ndarray, window: int) -> float:
    """
    Annualised historical volatility over a rolling window.
    Returns 0 if insufficient data.
    """
    if len(prices) < window + 1:
        return 0.0
    log_returns = np.diff(np.log(prices[-window - 1:]))
    return float(np.std(log_returns, ddof=1) * math.sqrt(252))


def _compute_iv_series(hist_prices: np.ndarray,
                       window: int = 20,
                       lookback: int = 252) -> np.ndarray:
    """
    Approximate a rolling IV proxy series from historical volatility.
    Phase-1 approximation — Phase-2 replaces with actual IV history.

    Returns an array of annualised HV values, one per day, for the
    trailing `lookback` period using a `window`-day rolling window.
    """
    if len(hist_prices) < window + lookback:
        lookback = max(0, len(hist_prices) - window)
    if lookback == 0:
        return np.array([])

    series = []
    for i in range(lookback):
        end = len(hist_prices) - lookback + i + 1
        start = end - window - 1
        if start < 0:
            continue
        chunk = hist_prices[start:end]
        lr = np.diff(np.log(chunk))
        series.append(float(np.std(lr, ddof=1) * math.sqrt(252)))
    return np.array(series)


# ── Options chain extraction ──────────────────────────────────────

def _extract_chain_ivs(ticker_obj: yf.Ticker) -> dict:
    """
    Pull IV data from the options chain.

    Returns dict with keys:
        atm_iv, put_25d_iv, call_25d_iv, near_atm_iv, far_atm_iv
    Falls back to synthetic values if chain unavailable.
    """
    result = {
        "atm_iv": None,
        "put_25d_iv": None,
        "call_25d_iv": None,
        "near_atm_iv": None,   # shortest expiry ATM
        "far_atm_iv": None,    # ~90 DTE ATM
    }

    try:
        spot = float(ticker_obj.fast_info.get("lastPrice", 0)
                     or ticker_obj.fast_info.get("previousClose", 0))
        if spot <= 0:
            return result

        expirations = ticker_obj.options
        if not expirations or len(expirations) == 0:
            return result

        # ── Near-term chain (first expiry) ────────────────────────
        near_exp = expirations[0]
        chain = ticker_obj.option_chain(near_exp)

        calls = chain.calls
        puts = chain.puts

        if calls.empty or puts.empty:
            return result

        # ATM: closest strike to spot
        calls = calls.copy()
        calls["dist"] = (calls["strike"] - spot).abs()
        atm_call = calls.loc[calls["dist"].idxmin()]
        atm_iv = float(atm_call.get("impliedVolatility", 0))
        result["atm_iv"] = atm_iv
        result["near_atm_iv"] = atm_iv

        # 25-delta approximation: ~5-8% OTM
        otm_call_target = spot * 1.06
        otm_put_target = spot * 0.94

        calls["dist_otm"] = (calls["strike"] - otm_call_target).abs()
        call_25d = calls.loc[calls["dist_otm"].idxmin()]
        result["call_25d_iv"] = float(call_25d.get("impliedVolatility", 0))

        puts = puts.copy()
        puts["dist_otm"] = (puts["strike"] - otm_put_target).abs()
        put_25d = puts.loc[puts["dist_otm"].idxmin()]
        result["put_25d_iv"] = float(put_25d.get("impliedVolatility", 0))

        # ── Far-term chain (~90 DTE) ──────────────────────────────
        if len(expirations) >= 3:
            far_exp = expirations[min(3, len(expirations) - 1)]
            far_chain = ticker_obj.option_chain(far_exp)
            far_calls = far_chain.calls
            if not far_calls.empty:
                far_calls = far_calls.copy()
                far_calls["dist"] = (far_calls["strike"] - spot).abs()
                far_atm = far_calls.loc[far_calls["dist"].idxmin()]
                result["far_atm_iv"] = float(far_atm.get("impliedVolatility", 0))

    except Exception:
        logger.warning("Options chain extraction failed for %s", ticker_obj.ticker)

    return result


# ── Core engine ───────────────────────────────────────────────────

class VolSurfaceEngine:
    """
    Computes volatility surface metrics for each ticker in the universe.
    """

    def compute(self, ticker: str) -> VolSurface:
        """
        Compute all 6 vol surface metrics for a single ticker.
        """
        t = yf.Ticker(ticker)

        # ── Historical data ───────────────────────────────────────
        hist = t.history(period="1y")
        if hist.empty or len(hist) < 30:
            logger.warning("%s: insufficient history (%d rows)", ticker, len(hist))
            return VolSurface(ticker=ticker, iv_rank=50, iv_percentile=50,
                              skew_slope=0, smile_curvature=0,
                              term_structure=1.0, iv_rv_gap=0)

        prices = hist["Close"].values

        # ── Historical volatility ─────────────────────────────────
        hv20 = _compute_hv(prices, 20)
        hv60 = _compute_hv(prices, 60)

        # ── Options chain IVs ─────────────────────────────────────
        chain_ivs = _extract_chain_ivs(t)

        atm_iv = chain_ivs["atm_iv"] or hv20 * 1.1   # fallback: HV + premium
        put_25d_iv = chain_ivs["put_25d_iv"] or atm_iv * 1.08
        call_25d_iv = chain_ivs["call_25d_iv"] or atm_iv * 0.95
        near_iv = chain_ivs["near_atm_iv"] or atm_iv
        far_iv = chain_ivs["far_atm_iv"] or atm_iv * 0.95

        # ── 1. IV Rank ────────────────────────────────────────────
        iv_series = _compute_iv_series(prices, window=20, lookback=252)
        if len(iv_series) > 10:
            iv_low = float(np.min(iv_series))
            iv_high = float(np.max(iv_series))
            iv_range = iv_high - iv_low
            iv_rank = ((atm_iv - iv_low) / iv_range * 100) if iv_range > 0 else 50.0
            iv_rank = max(0, min(100, iv_rank))
        else:
            iv_rank = 50.0

        # ── 2. IV Percentile ──────────────────────────────────────
        if len(iv_series) > 10:
            iv_percentile = float(np.sum(iv_series < atm_iv) / len(iv_series) * 100)
        else:
            iv_percentile = 50.0

        # ── 3. Skew Slope ────────────────────────────────────────
        #   (25Δ put IV − 25Δ call IV) / ATM IV
        if atm_iv > 0:
            skew_slope = (put_25d_iv - call_25d_iv) / atm_iv
        else:
            skew_slope = 0.0

        # ── 4. Smile Curvature ────────────────────────────────────
        #   (put_wing_IV + call_wing_IV) / 2 − ATM_IV
        smile_curvature = (put_25d_iv + call_25d_iv) / 2 - atm_iv

        # ── 5. Term Structure ─────────────────────────────────────
        #   IV30 / IV90  (near / far)
        if far_iv > 0:
            term_structure = near_iv / far_iv
        else:
            term_structure = 1.0

        # ── 6. Realized vs Implied Gap ────────────────────────────
        #   (IV30 − HV20) / HV20
        if hv20 > 0:
            iv_rv_gap = (atm_iv - hv20) / hv20
        else:
            iv_rv_gap = 0.0

        surface = VolSurface(
            ticker=ticker,
            iv_rank=round(iv_rank, 2),
            iv_percentile=round(iv_percentile, 2),
            skew_slope=round(skew_slope, 4),
            smile_curvature=round(smile_curvature, 4),
            term_structure=round(term_structure, 4),
            iv_rv_gap=round(iv_rv_gap, 4),
        )

        logger.info("%s: IV_rank=%.1f IV_pctl=%.1f skew=%.4f curve=%.4f "
                     "term=%.4f gap=%.4f",
                     ticker, surface.iv_rank, surface.iv_percentile,
                     surface.skew_slope, surface.smile_curvature,
                     surface.term_structure, surface.iv_rv_gap)

        return surface

    def run(self, tickers: list[str]) -> list[VolSurface]:
        """
        Compute vol surface for all tickers and persist to Supabase.
        """
        logger.info("=== Vol surface engine started (%d tickers) ===",
                     len(tickers))

        results: list[VolSurface] = []

        for ticker in tickers:
            try:
                surface = self.compute(ticker)
                results.append(surface)
            except Exception:
                logger.exception("Failed to compute vol surface for %s", ticker)

        # Persist
        rows = [{
            "ticker": s.ticker,
            "iv_rank": s.iv_rank,
            "iv_percentile": s.iv_percentile,
            "skew_slope": s.skew_slope,
            "smile_curvature": s.smile_curvature,
            "term_structure": s.term_structure,
            "iv_rv_gap": s.iv_rv_gap,
        } for s in results]

        write_surface(rows)

        write_agent_log(AGENT, "run", "success", {
            "tickers": len(results),
            "avg_iv_rank": round(sum(s.iv_rank for s in results) / max(len(results), 1), 2),
            "avg_skew": round(sum(s.skew_slope for s in results) / max(len(results), 1), 4),
        })

        # Print report
        print()
        print("Vol Surface Report")
        print("=" * 90)
        print(f"  {'Ticker':<8} {'IVRank':>7} {'IVPctl':>7} {'Skew':>8} "
              f"{'Curve':>8} {'Term':>7} {'IV-RV':>8}")
        print(f"  {'------':<8} {'------':>7} {'------':>7} {'----':>8} "
              f"{'-----':>8} {'----':>7} {'-----':>8}")
        for s in sorted(results, key=lambda x: x.iv_rank, reverse=True):
            print(f"  {s.ticker:<8} {s.iv_rank:>7.1f} {s.iv_percentile:>7.1f} "
                  f"{s.skew_slope:>+8.4f} {s.smile_curvature:>+8.4f} "
                  f"{s.term_structure:>7.4f} {s.iv_rv_gap:>+8.4f}")
        print()

        logger.info("=== Vol surface engine complete ===")
        return results


# ── CLI ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(levelname)s %(message)s")

    tickers = sys.argv[1:] if len(sys.argv) > 1 else ["SPY", "QQQ", "GLD", "SLV"]
    engine = VolSurfaceEngine()
    engine.run(tickers)
