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


def classify_mode(ticker: str) -> str:
    """Classify asset into TACTICAL, HYBRID, or CONVEX.

    TACTICAL — index ETFs, default.  Use spreads for defined risk.
    HYBRID   — sector ETFs with moderate vol.  Spreads or naked
               depending on confidence.
    CONVEX   — commodities / miners with outsized move potential.
               Always naked options for full convexity.
    """
    t = ticker.upper()
    if t in ("SPY", "QQQ"):
        return "TACTICAL"
    if t in ("SMH", "SOXX", "XLK"):
        return "HYBRID"
    if t in ("SLV", "GLD", "GDX", "SILJ", "COPX"):
        return "CONVEX"
    return "TACTICAL"


def get_mode_config(mode: str) -> dict[str, Any]:
    """Return trading parameters for the given asset mode.

    Parameters
    ----------
    mode : str
        TACTICAL, HYBRID, CONVEX, or MEAN_REVERSION.

    Returns
    -------
    dict
        hold_days, use_spreads, allow_naked, position_scale,
        stop_loss, take_profit, min_confidence.
    """
    if mode == "TACTICAL":
        return {
            "hold_days": 2,
            "use_spreads": True,
            "allow_naked": False,
            "position_scale": 1.0,
            "stop_loss": 1.0,
            "take_profit": 1.5,
            "min_confidence": 60,
        }
    elif mode == "HYBRID":
        return {
            "hold_days": 10,
            "use_spreads": True,
            "allow_naked": True,
            "position_scale": 1.2,
            "stop_loss": 1.5,
            "take_profit": 2.0,
            "min_confidence": 55,
        }
    elif mode == "CONVEX":
        return {
            "hold_days": 10,
            "use_spreads": False,
            "allow_naked": True,
            "position_scale": 1.5,
            "stop_loss": 2.5,
            "take_profit": 5.0,
            "min_confidence": 50,
        }
    elif mode == "MEAN_REVERSION":
        return {
            "hold_days": 2,
            "use_spreads": False,
            "allow_naked": True,
            "position_scale": 0.7,
            "stop_loss": 1.0,
            "take_profit": 2.0,
            "min_confidence": 45,
        }
    # Fallback to TACTICAL
    return get_mode_config("TACTICAL")


def get_asset_hold_override(ticker: str, mode: str) -> int | None:
    """Return a per-ticker hold-day override, or ``None`` to use the mode default.

    This lets individual assets deviate from their mode's default hold
    period based on observed backtest behaviour.
    """
    t = ticker.upper()
    if t in ("SPY", "QQQ"):
        return 2
    if t in ("SMH", "SOXX", "XLK"):
        return 10
    if t in ("SLV", "GLD", "GDX", "SILJ", "COPX"):
        return 10
    return None


# Keep backward-compatible alias so existing imports don't break
def get_asset_mode(ticker: str) -> str:
    """Legacy alias — maps TACTICAL→CONTROLLED, HYBRID/CONVEX→EXPLOSIVE."""
    mode = classify_mode(ticker)
    if mode in ("HYBRID", "CONVEX"):
        return "EXPLOSIVE"
    return "CONTROLLED"


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
        ticker, structure, confidence, mode.
    """
    mode = classify_mode(ticker)
    cfg = get_mode_config(mode)
    min_conf = cfg["min_confidence"]

    if combined_signal == "LONG":
        if not cfg["allow_naked"]:
            # TACTICAL — always spreads above min confidence
            structure = "CALL_SPREAD" if confidence >= min_conf else "LONG_CALL"
        elif not cfg["use_spreads"]:
            # CONVEX — always naked for full convexity
            structure = "LONG_CALL"
        else:
            # HYBRID — spread at high confidence, naked below
            structure = "LONG_CALL" if confidence < min_conf else "CALL_SPREAD"
    elif combined_signal == "SHORT":
        if not cfg["allow_naked"]:
            structure = "PUT_SPREAD" if confidence >= min_conf else "LONG_PUT"
        elif not cfg["use_spreads"]:
            structure = "LONG_PUT"
        else:
            structure = "LONG_PUT" if confidence < min_conf else "PUT_SPREAD"
    else:
        structure = "NO_TRADE"

    return {
        "ticker": ticker,
        "structure": structure,
        "confidence": confidence,
        "mode": mode,
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
