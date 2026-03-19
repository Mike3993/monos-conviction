"""
monos_engine.msa.msa_engine

MSA (Market Structure Analysis) Engine — determines market regime
from price structure (40-week / 200-day SMA trend + momentum overlay).

Provides:
    get_msa_state()          — regime at a bar index from a close array.
    get_msa_state_on_date()  — regime at a calendar date via yfinance lookup.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf


def get_msa_state(closes: np.ndarray, index: int) -> str:
    """Determine MSA regime at a given bar index.

    Uses 200-bar SMA as structural trend proxy and 50-bar SMA
    as momentum overlay.

    Parameters
    ----------
    closes : np.ndarray
        Full close price array.
    index : int
        Bar index to evaluate.

    Returns
    -------
    str
        MSA_BULLISH, MSA_BEARISH, or MSA_NEUTRAL.
    """
    if index < 200:
        # Not enough history for 200-bar SMA — use 50 only
        if index < 50:
            return "MSA_NEUTRAL"
        sma50 = float(np.mean(closes[index - 49 : index + 1]))
        price = closes[index]
        if price > sma50 * 1.02:
            return "MSA_BULLISH"
        if price < sma50 * 0.98:
            return "MSA_BEARISH"
        return "MSA_NEUTRAL"

    price = closes[index]
    sma200 = float(np.mean(closes[index - 199 : index + 1]))
    sma50 = float(np.mean(closes[index - 49 : index + 1]))

    above_200 = price > sma200
    above_50 = price > sma50
    sma50_above_200 = sma50 > sma200

    # Strong bull: price above both, 50 above 200
    if above_200 and above_50 and sma50_above_200:
        return "MSA_BULLISH"

    # Strong bear: price below both, 50 below 200
    if not above_200 and not above_50 and not sma50_above_200:
        return "MSA_BEARISH"

    # Mixed signals
    return "MSA_NEUTRAL"


# ── Date-based MSA lookup ────────────────────────────────────────────

def get_msa_state_on_date(ticker: str, as_of_date: str) -> str:
    """Determine MSA regime for *ticker* as of a specific calendar date.

    Downloads enough price history to compute a 50-day SMA and a 20-day
    slope, then classifies the regime using price vs MA and slope sign.

    Parameters
    ----------
    ticker : str
        Equity / ETF symbol (e.g. ``"SPY"``).
    as_of_date : str
        ISO date string (``"YYYY-MM-DD"``).  Only data up to and
        including this date is used.

    Returns
    -------
    str
        ``MSA_BULLISH``, ``MSA_BEARISH``, or ``MSA_NEUTRAL``.
    """
    cutoff = pd.Timestamp(as_of_date)
    # Fetch ~100 trading days before as_of_date to have enough for SMA-50
    start = cutoff - pd.Timedelta(days=150)
    hist = yf.Ticker(ticker).history(start=str(start.date()), end=str((cutoff + pd.Timedelta(days=1)).date()))

    if hist.empty:
        return "MSA_NEUTRAL"

    # Normalise tz for comparison — yfinance returns tz-aware index
    if hist.index.tz is not None:
        cutoff = cutoff.tz_localize(hist.index.tz)

    # Slice up to as_of_date (inclusive)
    hist = hist[hist.index <= cutoff]

    if len(hist) < 20:
        return "MSA_NEUTRAL"

    closes = hist["Close"].values
    price = closes[-1]

    # 50-day SMA (or as many bars as available up to 50)
    sma_window = min(50, len(closes))
    sma50 = float(np.mean(closes[-sma_window:]))

    # 20-day slope: linear regression slope over last 20 bars
    slope_window = min(20, len(closes))
    recent = closes[-slope_window:]
    x = np.arange(slope_window, dtype=float)
    slope = float(np.polyfit(x, recent, 1)[0])

    # Classification
    if price > sma50 and slope > 0:
        return "MSA_BULLISH"
    if price < sma50 and slope < 0:
        return "MSA_BEARISH"
    return "MSA_NEUTRAL"
