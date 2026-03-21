"""
monos_engine.momentum.momentum

Momentum signal engine for the MONOS Conviction Engine.
Computes RSI, short/medium-term returns, trend score, and signal direction
from yfinance historical price data.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import yfinance as yf

from monos_engine.db.writes import insert_momentum_signal


# ── RSI ──────────────────────────────────────────────────────────────

def _compute_rsi(closes: np.ndarray, period: int = 14) -> float:
    """Compute Relative Strength Index over the last *period* bars.

    Uses the Wilder smoothing (exponential moving average) method.
    Returns a value between 0 and 100.
    """
    if len(closes) < period + 1:
        return 50.0  # neutral fallback

    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss < 1e-12:
        return 100.0

    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 2)


# ── Returns ──────────────────────────────────────────────────────────

def _compute_return(closes: np.ndarray, days: int) -> float:
    """Percentage return over the last *days* trading sessions."""
    if len(closes) <= days:
        return 0.0
    return round(((closes[-1] / closes[-(days + 1)]) - 1.0) * 100.0, 4)


# ── Trend score ──────────────────────────────────────────────────────

def _compute_trend_score(rsi: float, ret_5d: float, ret_20d: float) -> float:
    """Combine RSI and returns into a 0–100 trend score.

    Weights:
        RSI          — 50%  (already 0–100)
        5-day return — 30%  (clamped to ±10%, rescaled to 0–100)
        20-day return— 20%  (clamped to ±20%, rescaled to 0–100)
    """
    rsi_component = rsi

    ret5_clamped = max(-10.0, min(10.0, ret_5d))
    ret5_scaled = (ret5_clamped + 10.0) / 20.0 * 100.0

    ret20_clamped = max(-20.0, min(20.0, ret_20d))
    ret20_scaled = (ret20_clamped + 20.0) / 40.0 * 100.0

    score = 0.50 * rsi_component + 0.30 * ret5_scaled + 0.20 * ret20_scaled
    return round(max(0.0, min(100.0, score)), 2)


# ── Signal direction ─────────────────────────────────────────────────

def _classify_direction(trend_score: float) -> str:
    if trend_score > 60:
        return "LONG"
    if trend_score < 40:
        return "SHORT"
    return "NEUTRAL"


def _classify_regime(rsi: float, ret_20d: float) -> str:
    """Classify momentum regime from RSI and medium-term return."""
    if rsi > 65 and ret_20d > 3.0:
        return "STRONG_BULLISH"
    if rsi > 55 and ret_20d > 0.0:
        return "BULLISH"
    if rsi < 35 and ret_20d < -3.0:
        return "STRONG_BEARISH"
    if rsi < 45 and ret_20d < 0.0:
        return "BEARISH"
    return "NEUTRAL"


# ── Core computation ─────────────────────────────────────────────────

def compute_momentum(ticker: str) -> dict[str, Any]:
    """Compute a momentum signal for *ticker*.

    Fetches 60 trading days of history via yfinance, then computes
    RSI-14, 5-day return, 20-day return, trend score, velocity,
    regime, and signal direction.

    Returns
    -------
    dict
        ticker, trend_score, velocity, rsi, regime, signal_direction,
        return_5d, return_20d.
    """
    hist = yf.Ticker(ticker).history(period="3mo")
    if hist.empty or len(hist) < 21:
        raise ValueError(f"Insufficient price history for {ticker} ({len(hist)} bars)")

    closes = hist["Close"].values

    rsi = _compute_rsi(closes, period=14)
    ret_5d = _compute_return(closes, 5)
    ret_20d = _compute_return(closes, 20)
    trend_score = _compute_trend_score(rsi, ret_5d, ret_20d)
    direction = _classify_direction(trend_score)
    regime = _classify_regime(rsi, ret_20d)

    # Velocity: rate of trend score change approximated by
    # short-term return momentum (5d return / 20d return ratio)
    if abs(ret_20d) > 0.01:
        velocity = round(ret_5d / ret_20d, 4)
    else:
        velocity = 0.0

    return {
        "ticker": ticker,
        "trend_score": trend_score,
        "velocity": velocity,
        "rsi": rsi,
        "regime": regime,
        "signal_direction": direction,
        "return_5d": ret_5d,
        "return_20d": ret_20d,
    }


# ── Example runner ───────────────────────────────────────────────────

def run_example(ticker: str) -> dict[str, Any]:
    """Compute momentum signal, persist to Supabase, return both.

    Returns
    -------
    dict
        computed: momentum result, stored: inserted Supabase row.
    """
    result = compute_momentum(ticker)

    stored = insert_momentum_signal({
        "ticker": result["ticker"],
        "trend_score": result["trend_score"],
        "velocity": result["velocity"],
        "rsi": result["rsi"],
        "regime": result["regime"],
        "signal_direction": result["signal_direction"],
        "metadata": {
            "source": "momentum_engine",
            "return_5d": result["return_5d"],
            "return_20d": result["return_20d"],
        },
    })

    return {
        "computed": result,
        "stored": stored,
    }


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json
    import sys

    tickers = sys.argv[1:] or ["SPY"]
    for t in tickers:
        res = run_example(t)
        print(json.dumps({"computed": res["computed"], "stored": res["stored"]}, indent=2, default=str))
