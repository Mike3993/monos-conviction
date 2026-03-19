"""
monos_engine.mean_reversion.reversion_engine

Mean Reversion Engine — captures short-term reversal trades by
detecting RSI overextension in neutral market regimes.

Only activates when MSA == MSA_NEUTRAL to avoid fighting structural
trends.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import yfinance as yf

from monos_engine.momentum.momentum import _compute_rsi
from monos_engine.msa.msa_engine import get_msa_state_on_date


# ── Thresholds ───────────────────────────────────────────────────────

RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30
RSI_PERIOD = 14


# ── Core engine ──────────────────────────────────────────────────────

def compute_reversion_signal(
    ticker: str,
    closes: np.ndarray,
    as_of_date: str | None = None,
) -> dict[str, Any]:
    """Evaluate a mean-reversion signal for *ticker*.

    Parameters
    ----------
    ticker : str
        Equity / ETF symbol.
    closes : np.ndarray
        Close prices up to (and including) the evaluation bar.
    as_of_date : str | None
        ISO date string for MSA lookup.  If ``None``, MSA gate is
        skipped (useful for unit testing).

    Returns
    -------
    dict
        ticker, signal, rsi, msa_state, confidence, active.
    """
    rsi = _compute_rsi(closes, period=RSI_PERIOD)

    # MSA gate — only activate in neutral regimes
    if as_of_date is not None:
        msa_state = get_msa_state_on_date(ticker, as_of_date)
    else:
        msa_state = "MSA_NEUTRAL"

    active = msa_state == "MSA_NEUTRAL"

    # Determine signal direction from RSI extremes
    signal = "NEUTRAL"
    confidence = 0.0

    if rsi > RSI_OVERBOUGHT:
        signal = "SHORT"
        # Confidence scales with how far RSI exceeds the threshold
        # RSI 70 → 50, RSI 85 → 100, capped at 100
        confidence = round(min(50.0 + (rsi - RSI_OVERBOUGHT) * (50.0 / 15.0), 100.0), 2)
    elif rsi < RSI_OVERSOLD:
        signal = "LONG"
        # RSI 30 → 50, RSI 15 → 100, capped at 100
        confidence = round(min(50.0 + (RSI_OVERSOLD - rsi) * (50.0 / 15.0), 100.0), 2)

    # Suppress signal if MSA is not neutral
    if not active:
        signal = "NEUTRAL"
        confidence = 0.0

    return {
        "ticker": ticker,
        "signal": signal,
        "rsi": round(rsi, 2),
        "msa_state": msa_state,
        "confidence": confidence,
        "active": active,
    }


# ── Live runner ──────────────────────────────────────────────────────

def run_reversion(ticker: str = "SPY", lookback: int = 60) -> dict[str, Any]:
    """Compute a live mean-reversion signal for *ticker*.

    Downloads recent price history via yfinance, computes RSI, checks
    the MSA gate, and returns the signal dict.

    Parameters
    ----------
    ticker : str
        Equity / ETF symbol.
    lookback : int
        Trading days of history to fetch (must be > RSI_PERIOD).

    Returns
    -------
    dict
        Same as :func:`compute_reversion_signal`.
    """
    period = f"{lookback + 30}d"
    hist = yf.Ticker(ticker).history(period=period)

    if hist.empty or len(hist) < RSI_PERIOD + 1:
        return {
            "ticker": ticker,
            "signal": "NEUTRAL",
            "rsi": 50.0,
            "msa_state": "MSA_NEUTRAL",
            "confidence": 0.0,
            "active": False,
        }

    closes = hist["Close"].values
    as_of_date = str(hist.index[-1].date())

    return compute_reversion_signal(ticker, closes, as_of_date)


# ── Example runner ───────────────────────────────────────────────────

def run_example(ticker: str = "SPY") -> dict[str, Any]:
    """Run mean-reversion engine and print result."""
    result = run_reversion(ticker)

    print(f"\n{'='*50}")
    print(f"  MONOS MEAN REVERSION — {ticker}")
    print(f"{'='*50}")
    print(f"  RSI              : {result['rsi']}")
    print(f"  MSA State        : {result['msa_state']}")
    print(f"  Active           : {result['active']}")
    print(f"  Signal           : {result['signal']}")
    print(f"  Confidence       : {result['confidence']}")
    print(f"{'='*50}\n")

    return result


if __name__ == "__main__":
    import sys

    tickers = sys.argv[1:] or ["SPY"]
    for t in tickers:
        run_example(t)
