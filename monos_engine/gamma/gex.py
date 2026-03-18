"""
monos_engine.gamma.gex

Deterministic Gamma Exposure (GEX) engine for the MONOS Conviction Engine.
Computes per-strike GEX, aggregate gamma, gamma flip level, and dealer positioning.
"""

from __future__ import annotations

import math
import random
from typing import Any

import yfinance as yf

from monos_engine.db.writes import insert_gamma_exposure


# ── Core GEX computation ────────────────────────────────────────────

def compute_gex(
    spot: float,
    strikes: list[float],
    call_gamma: list[float],
    put_gamma: list[float],
    call_oi: list[float],
    put_oi: list[float],
) -> dict[str, Any]:
    """Compute gamma exposure across a strike ladder.

    Parameters
    ----------
    spot : float
        Current underlying price.
    strikes : list[float]
        Strike prices (must be same length as gamma/OI lists).
    call_gamma : list[float]
        Call gamma per strike.
    put_gamma : list[float]
        Put gamma per strike.
    call_oi : list[float]
        Call open interest per strike.
    put_oi : list[float]
        Put open interest per strike.

    Returns
    -------
    dict
        total_gamma, call_gamma, put_gamma, gamma_flip_level,
        dealer_positioning, gex_by_strike.
    """
    n = len(strikes)
    if not (n == len(call_gamma) == len(put_gamma) == len(call_oi) == len(put_oi)):
        raise ValueError("All input lists must be the same length")
    if n == 0:
        raise ValueError("Strike list must not be empty")

    # Per-strike GEX
    gex_by_strike: dict[float, float] = {}
    total_call = 0.0
    total_put = 0.0

    for i in range(n):
        cg = call_gamma[i] * call_oi[i]
        pg = put_gamma[i] * put_oi[i]
        gex_by_strike[strikes[i]] = cg - pg
        total_call += cg
        total_put += pg

    total = total_call - total_put

    # Gamma flip: strike where cumulative GEX crosses zero
    gamma_flip = _find_gamma_flip(strikes, gex_by_strike, spot)

    # Dealer positioning
    positioning = "POSITIVE" if total > 0 else "NEGATIVE"

    return {
        "total_gamma": round(total, 4),
        "call_gamma": round(total_call, 4),
        "put_gamma": round(total_put, 4),
        "gamma_flip_level": round(gamma_flip, 2),
        "dealer_positioning": positioning,
        "gex_by_strike": {k: round(v, 4) for k, v in gex_by_strike.items()},
    }


def _find_gamma_flip(
    strikes: list[float],
    gex_by_strike: dict[float, float],
    spot: float,
) -> float:
    """Find the strike where cumulative GEX crosses zero.

    Walks strikes low-to-high, accumulating GEX. Returns the strike
    at which the running sum changes sign. Falls back to spot if no
    crossing is found.
    """
    sorted_strikes = sorted(strikes)
    cumulative = 0.0
    prev_cum = 0.0

    for k in sorted_strikes:
        prev_cum = cumulative
        cumulative += gex_by_strike[k]

        if prev_cum != 0.0 and _sign(prev_cum) != _sign(cumulative):
            # Linear interpolation between previous and current strike
            idx = sorted_strikes.index(k)
            if idx > 0:
                k_prev = sorted_strikes[idx - 1]
                denom = cumulative - prev_cum
                if abs(denom) > 1e-12:
                    frac = -prev_cum / denom
                    return k_prev + frac * (k - k_prev)
            return float(k)

    return spot


def _sign(x: float) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


# ── Synthetic chain generator ────────────────────────────────────────

def generate_synthetic_chain(
    spot: float,
    num_strikes: int = 30,
    strike_step: float | None = None,
    seed: int | None = None,
) -> dict[str, list[float]]:
    """Generate a synthetic options chain around *spot*.

    Parameters
    ----------
    spot : float
        Current underlying price.
    num_strikes : int
        Number of strikes to generate (default 30).
    strike_step : float or None
        Distance between strikes. Defaults to ~0.5% of spot.
    seed : int or None
        Random seed for deterministic output.

    Returns
    -------
    dict with keys: strikes, call_gamma, put_gamma, call_oi, put_oi
    """
    if seed is not None:
        random.seed(seed)

    if strike_step is None:
        strike_step = round(spot * 0.005, 2) or 1.0

    half = num_strikes // 2
    strikes = [round(spot + (i - half) * strike_step, 2) for i in range(num_strikes)]

    sigma = spot * 0.03  # Gaussian width for gamma distribution

    call_gamma_list: list[float] = []
    put_gamma_list: list[float] = []
    call_oi_list: list[float] = []
    put_oi_list: list[float] = []

    for k in strikes:
        # Gamma peaks at ATM, decays away from spot
        distance = (k - spot) / sigma if sigma > 0 else 0.0
        base_gamma = 0.05 * math.exp(-0.5 * distance * distance)

        call_gamma_list.append(round(base_gamma * random.uniform(0.8, 1.2), 6))
        put_gamma_list.append(round(base_gamma * random.uniform(0.8, 1.2), 6))
        call_oi_list.append(round(random.uniform(500, 15000)))
        put_oi_list.append(round(random.uniform(500, 15000)))

    return {
        "strikes": strikes,
        "call_gamma": call_gamma_list,
        "put_gamma": put_gamma_list,
        "call_oi": call_oi_list,
        "put_oi": put_oi_list,
    }


# ── Example runner ───────────────────────────────────────────────────

def run_example(ticker: str, seed: int = 42) -> dict[str, Any]:
    """Fetch spot price, generate synthetic chain, and compute GEX.

    Parameters
    ----------
    ticker : str
        Equity or ETF symbol (e.g. "SPY").
    seed : int
        Random seed for reproducibility (default 42).

    Returns
    -------
    dict
        Full GEX result including ticker and spot.
    """
    info = yf.Ticker(ticker).info
    spot = info.get("lastPrice") or info.get("regularMarketPrice") or info.get("previousClose")
    if not spot:
        raise ValueError(f"Could not fetch spot price for {ticker}")

    chain = generate_synthetic_chain(spot, seed=seed)

    result = compute_gex(
        spot=spot,
        strikes=chain["strikes"],
        call_gamma=chain["call_gamma"],
        put_gamma=chain["put_gamma"],
        call_oi=chain["call_oi"],
        put_oi=chain["put_oi"],
    )

    result["ticker"] = ticker
    result["spot"] = spot

    stored = insert_gamma_exposure({
        "ticker": ticker,
        "total_gamma": result["total_gamma"],
        "call_gamma": result["call_gamma"],
        "put_gamma": result["put_gamma"],
        "gamma_flip_level": result["gamma_flip_level"],
        "dealer_positioning": result["dealer_positioning"],
        "metadata": {
            "source": "synthetic_engine",
            "stage": "gex_test",
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
        # Drop gex_by_strike for cleaner CLI output
        computed = {k: v for k, v in res["computed"].items() if k != "gex_by_strike"}
        print(json.dumps({"computed": computed, "stored": res["stored"]}, indent=2, default=str))
