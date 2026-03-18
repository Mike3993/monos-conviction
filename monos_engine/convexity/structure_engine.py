"""
monos_engine.convexity.structure_engine

Translates combined signals into options structures based on
direction and confidence.
"""

from __future__ import annotations

from typing import Any

from monos_engine.combiner.signal_combiner import combine_signals
from monos_engine.gamma.gex import compute_gex, generate_synthetic_chain
from monos_engine.momentum.momentum import compute_momentum

import yfinance as yf


def select_structure(
    ticker: str,
    combined_signal: str,
    confidence: float,
) -> dict[str, Any]:
    """Select an options structure from signal direction and confidence.

    Parameters
    ----------
    ticker : str
        Underlying symbol.
    combined_signal : str
        LONG, SHORT, or NEUTRAL.
    confidence : float
        Confidence score (0–100).

    Returns
    -------
    dict
        ticker, structure, confidence.
    """
    if combined_signal == "LONG":
        structure = "CALL_SPREAD" if confidence >= 60 else "LONG_CALL"
    elif combined_signal == "SHORT":
        structure = "PUT_SPREAD" if confidence >= 60 else "LONG_PUT"
    else:
        structure = "NO_TRADE"

    return {
        "ticker": ticker,
        "structure": structure,
        "confidence": confidence,
    }


def build_structure(data: dict[str, Any]) -> dict[str, Any]:
    """Build a structure from a signal dict.

    Accepts a dict with ticker, combined_signal, confidence.
    Convenience wrapper around select_structure.
    """
    return select_structure(
        ticker=data["ticker"],
        combined_signal=data["combined_signal"],
        confidence=data["confidence"],
    )


def run_example(ticker: str, seed: int = 42) -> dict[str, Any]:
    """Compute signals and select structure for *ticker*.

    Returns
    -------
    dict
        ticker, structure, confidence, combined_signal, gamma, momentum.
    """
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

    momentum_result = compute_momentum(ticker)

    combined = combine_signals(gamma_result, momentum_result)

    result = select_structure(
        ticker=ticker,
        combined_signal=combined["combined_signal"],
        confidence=combined["confidence"],
    )
    result["combined_signal"] = combined["combined_signal"]

    return result


if __name__ == "__main__":
    import json
    import sys

    tickers = sys.argv[1:] or ["SPY"]
    for t in tickers:
        res = run_example(t)
        print(json.dumps(res, indent=2, default=str))
