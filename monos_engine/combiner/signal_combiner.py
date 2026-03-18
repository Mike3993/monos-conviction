"""
monos_engine.combiner.signal_combiner

Combines gamma exposure and momentum signals into a unified
decision signal with a confidence score.
"""

from __future__ import annotations

from typing import Any

from monos_engine.gamma.gex import compute_gex, generate_synthetic_chain
from monos_engine.momentum.momentum import compute_momentum

import yfinance as yf


# ── Signal combination ───────────────────────────────────────────────

def combine_signals(gamma: dict[str, Any], momentum: dict[str, Any]) -> dict[str, Any]:
    """Combine gamma and momentum signals into a unified decision.

    Parameters
    ----------
    gamma : dict
        Must contain at least ``dealer_positioning`` and ``ticker``.
    momentum : dict
        Must contain at least ``signal_direction``, ``trend_score``,
        ``rsi``, and ``ticker``.

    Returns
    -------
    dict
        ticker, combined_signal, confidence, gamma, momentum.
    """
    positioning = gamma.get("dealer_positioning", "UNKNOWN")
    direction = momentum.get("signal_direction", "NEUTRAL")
    trend_score = momentum.get("trend_score", 50.0)

    combined_signal = _classify_direction(positioning, direction)
    confidence = _compute_confidence(gamma, momentum)

    ticker = gamma.get("ticker") or momentum.get("ticker") or "UNKNOWN"

    return {
        "ticker": ticker,
        "combined_signal": combined_signal,
        "confidence": confidence,
        "gamma": gamma,
        "momentum": momentum,
    }


def _classify_direction(positioning: str, direction: str) -> str:
    """Classify into LONG / SHORT / NEUTRAL based on gamma-momentum alignment."""
    if positioning == "POSITIVE" and direction in ("LONG", "NEUTRAL"):
        return "LONG"
    if positioning == "NEGATIVE" and direction in ("SHORT", "NEUTRAL"):
        return "SHORT"
    if direction == "LONG" and positioning != "NEGATIVE":
        return "LONG"
    if direction == "SHORT" and positioning != "POSITIVE":
        return "SHORT"
    return "NEUTRAL"


def _compute_confidence(
    gamma: dict[str, Any],
    momentum: dict[str, Any],
) -> float:
    """Continuous confidence score (10–90) from trend strength and gamma magnitude.

    Formula:
        confidence = 0.6 × trend_score + 0.4 × gamma_score

    Where:
        trend_score   — momentum trend_score (already 0–100)
        gamma_score   — |total_gamma| / GAMMA_SCALE, clamped to 0–100

    Output bands (typical):
        Weak     → 20–40
        Moderate → 40–70
        Strong   → 70–90
    """
    # Trend component: use trend_score directly (0–100)
    trend_score = momentum.get("trend_score", 50.0)

    # Gamma component: normalise |total_gamma| to 0–100
    # Scale factor chosen so typical synthetic GEX (~50–500) spreads across range
    GAMMA_SCALE = 500.0
    total_gamma = abs(gamma.get("total_gamma", 0.0))
    gamma_score = min(total_gamma / GAMMA_SCALE, 1.0) * 100.0

    confidence = 0.6 * trend_score + 0.4 * gamma_score
    return round(max(10.0, min(90.0, confidence)), 2)


# ── Example runner ───────────────────────────────────────────────────

def run_example(ticker: str, seed: int = 42) -> dict[str, Any]:
    """Run gamma + momentum engines, combine, and return unified signal.

    Parameters
    ----------
    ticker : str
        Equity or ETF symbol (e.g. "SPY").
    seed : int
        Random seed for synthetic gamma chain (default 42).

    Returns
    -------
    dict
        Combined signal with gamma and momentum sub-results.
    """
    # Gamma
    info = yf.Ticker(ticker).info
    spot = info.get("lastPrice") or info.get("regularMarketPrice") or info.get("previousClose")
    if not spot:
        raise ValueError(f"Could not fetch spot price for {ticker}")

    chain = generate_synthetic_chain(spot, seed=seed)
    gamma_result = compute_gex(
        spot=spot,
        strikes=chain["strikes"],
        call_gamma=chain["call_gamma"],
        put_gamma=chain["put_gamma"],
        call_oi=chain["call_oi"],
        put_oi=chain["put_oi"],
    )
    gamma_result["ticker"] = ticker
    gamma_result["spot"] = spot

    # Momentum
    momentum_result = compute_momentum(ticker)

    # Combine
    return combine_signals(gamma_result, momentum_result)


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json
    import sys

    tickers = sys.argv[1:] or ["SPY"]
    for t in tickers:
        res = run_example(t)
        # Strip gex_by_strike for cleaner output
        if "gex_by_strike" in res.get("gamma", {}):
            res["gamma"] = {k: v for k, v in res["gamma"].items() if k != "gex_by_strike"}
        print(json.dumps(res, indent=2, default=str))
