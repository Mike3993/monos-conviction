"""
strike_selector.py

Selects strikes for each tier of an option structure.

Tier allocation:
    ITM_ANCHOR   20%   — delta 0.65-0.80
    ATM_CORE     35%   — delta 0.45-0.55
    OTM_CONVEX   30%   — delta 0.20-0.35
    DEEP_OTM     15%   — delta 0.05-0.15
"""

import logging
import math

logger = logging.getLogger(__name__)

TIER_TARGETS = {
    "ITM_ANCHOR":  {"delta_lo": 0.65, "delta_hi": 0.80, "alloc": 0.20},
    "ATM_CORE":    {"delta_lo": 0.45, "delta_hi": 0.55, "alloc": 0.35},
    "OTM_CONVEX":  {"delta_lo": 0.20, "delta_hi": 0.35, "alloc": 0.30},
    "DEEP_OTM":    {"delta_lo": 0.05, "delta_hi": 0.15, "alloc": 0.15},
}


def _approx_strike_from_delta(spot: float, target_delta: float,
                               iv: float, dte_years: float,
                               is_call: bool = True) -> float:
    """
    Approximate the strike for a given target delta using inverted
    Black-Scholes delta relationship.

    delta ≈ N(d1)  →  d1 ≈ N_inv(delta)
    K = S × exp(-d1 × σ√T + 0.5 × σ²T)
    """
    from scipy.stats import norm

    if dte_years <= 0 or iv <= 0:
        return spot

    d1 = norm.ppf(target_delta) if is_call else norm.ppf(1 - target_delta)
    sqrt_t = math.sqrt(dte_years)
    k = spot * math.exp(-d1 * iv * sqrt_t + 0.5 * iv * iv * dte_years)
    return round(k, 2)


def select_strikes(spot: float, iv: float = 0.25,
                   dte_days: int = 60,
                   is_call: bool = True) -> dict[str, dict]:
    """
    Return strike selections for each tier.

    Returns
    -------
    dict: tier_name → {strike, delta_target, allocation}
    """
    dte_years = dte_days / 365.0
    result = {}

    for tier, cfg in TIER_TARGETS.items():
        mid_delta = (cfg["delta_lo"] + cfg["delta_hi"]) / 2
        strike = _approx_strike_from_delta(spot, mid_delta, iv, dte_years, is_call)
        result[tier] = {
            "strike": strike,
            "delta_target": round(mid_delta, 3),
            "allocation": cfg["alloc"],
        }
        logger.debug("%s: strike=%.2f delta=%.3f alloc=%.0f%%",
                     tier, strike, mid_delta, cfg["alloc"] * 100)

    return result
