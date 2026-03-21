"""
trigger_engine.py

MONOS Trigger Engine — scores convexity trigger readiness from
gamma exposure, momentum signals, and volatility state.

Trigger score (0–100) determines state:
    >80  → TRIGGER_ACTIVE
    60–80 → ARMED
    <60  → DORMANT
"""

from __future__ import annotations

from typing import Any


# ── Scoring weights ──────────────────────────────────────────────────

W_GAMMA    = 0.40   # gamma alignment weight
W_MOMENTUM = 0.35   # momentum strength weight
W_VOL      = 0.25   # volatility compression weight


# ── Volatility state scoring ─────────────────────────────────────────

VOL_SCORES: dict[str, float] = {
    "COMPRESSED": 90.0,   # coiled spring — high trigger potential
    "NORMAL":     50.0,
    "EXPANDED":   20.0,   # already moved — low trigger potential
}


# ── Core computation ─────────────────────────────────────────────────

def compute_trigger(
    gamma: dict[str, Any],
    momentum: dict[str, Any],
    vol_state: str = "NORMAL",
) -> dict[str, Any]:
    """Compute a trigger score from gamma, momentum, and vol inputs.

    Parameters
    ----------
    gamma : dict
        Output from gex.compute_gex().  Must contain:
        total_gamma, gamma_flip_level, dealer_positioning.
    momentum : dict
        Output from momentum.compute_momentum().  Must contain:
        trend_score, signal_direction, rsi.
    vol_state : str
        One of COMPRESSED, NORMAL, EXPANDED.

    Returns
    -------
    dict
        ticker, trigger_score, state, gamma_flip_distance,
        vol_state, breakdown.
    """
    ticker = momentum.get("ticker", gamma.get("ticker", "UNKNOWN"))
    spot = gamma.get("spot", 0.0)

    # ── 1. Gamma sub-score (0–100) ───────────────────────────────
    total_gamma = abs(gamma.get("total_gamma", 0.0))
    positioning = gamma.get("dealer_positioning", "NEUTRAL")

    # Normalise gamma magnitude: 500 is considered max-scale
    GAMMA_SCALE = 500.0
    gamma_magnitude = min(total_gamma / GAMMA_SCALE, 1.0) * 100.0

    # Alignment bonus: NEGATIVE dealer positioning = dealers short gamma
    # which means explosive move potential → bonus
    alignment_bonus = 0.0
    direction = momentum.get("signal_direction", "NEUTRAL")
    if positioning == "NEGATIVE":
        alignment_bonus = 15.0  # dealers short gamma — high trigger value
    if positioning == "NEGATIVE" and direction in ("LONG", "SHORT"):
        alignment_bonus = 25.0  # gamma + momentum aligned

    gamma_score = min(gamma_magnitude + alignment_bonus, 100.0)

    # ── 2. Momentum sub-score (0–100) ────────────────────────────
    trend_score = momentum.get("trend_score", 50.0)
    rsi = momentum.get("rsi", 50.0)

    # Directional strength: how far from neutral (50)
    directional_strength = abs(trend_score - 50.0) * 2.0  # 0–100

    # RSI extremes boost trigger (overbought/oversold = inflection)
    rsi_boost = 0.0
    if rsi > 70 or rsi < 30:
        rsi_boost = 15.0
    elif rsi > 65 or rsi < 35:
        rsi_boost = 8.0

    momentum_score = min(directional_strength + rsi_boost, 100.0)

    # ── 3. Volatility sub-score (0–100) ──────────────────────────
    vol_score = VOL_SCORES.get(vol_state.upper(), 50.0)

    # ── 4. Composite trigger score ───────────────────────────────
    raw = (
        W_GAMMA * gamma_score
        + W_MOMENTUM * momentum_score
        + W_VOL * vol_score
    )
    trigger_score = round(max(0.0, min(100.0, raw)), 1)

    # ── 5. Classify state ────────────────────────────────────────
    if trigger_score >= 80:
        state = "TRIGGER_ACTIVE"
    elif trigger_score >= 60:
        state = "ARMED"
    else:
        state = "DORMANT"

    # ── 6. Gamma flip distance ───────────────────────────────────
    gamma_flip = gamma.get("gamma_flip_level", spot)
    if spot > 0:
        flip_distance = round(((gamma_flip - spot) / spot) * 100.0, 2)
    else:
        flip_distance = 0.0

    return {
        "ticker": ticker,
        "trigger_score": trigger_score,
        "state": state,
        "gamma_flip_distance": flip_distance,
        "vol_state": vol_state.upper(),
        "breakdown": {
            "gamma_score": round(gamma_score, 1),
            "momentum_score": round(momentum_score, 1),
            "vol_score": round(vol_score, 1),
            "positioning": positioning,
            "direction": direction,
        },
    }


# ── Example runner ───────────────────────────────────────────────────

def run_example(ticker: str = "SPY", seed: int = 42) -> dict[str, Any]:
    """Run trigger engine with live gamma + momentum data.

    Fetches gamma via synthetic chain (from gex module) and
    momentum via yfinance, then computes trigger score.

    Returns
    -------
    dict
        computed: trigger result dict.
    """
    import random
    random.seed(seed)

    from monos_engine.gamma.gex import generate_synthetic_chain, compute_gex
    from monos_engine.momentum.momentum import compute_momentum

    # Generate gamma data
    spot = 500.0
    chain = generate_synthetic_chain(spot=spot, num_strikes=20, seed=seed)
    gamma_result = compute_gex(
        spot=spot,
        strikes=chain["strikes"],
        call_gamma=chain["call_gamma"],
        put_gamma=chain["put_gamma"],
        call_oi=chain["call_oi"],
        put_oi=chain["put_oi"],
    )
    gamma_result["spot"] = spot
    gamma_result["ticker"] = ticker

    # Compute live momentum
    momentum_result = compute_momentum(ticker)

    # Pick vol state based on momentum regime
    regime = momentum_result.get("regime", "NORMAL")
    vol_map = {
        "TRENDING_UP": "NORMAL",
        "TRENDING_DOWN": "NORMAL",
        "OVERBOUGHT": "EXPANDED",
        "OVERSOLD": "EXPANDED",
        "NEUTRAL": "COMPRESSED",
    }
    vol_state = vol_map.get(regime, "NORMAL")

    # Compute trigger
    result = compute_trigger(gamma_result, momentum_result, vol_state)

    print(f"\n{'='*50}")
    print(f"  MONOS TRIGGER ENGINE — {ticker}")
    print(f"{'='*50}")
    print(f"  Trigger Score : {result['trigger_score']}")
    print(f"  State         : {result['state']}")
    print(f"  Gamma Flip    : {result['gamma_flip_distance']:+.2f}%")
    print(f"  Vol State     : {result['vol_state']}")
    print(f"  ─── Breakdown ───")
    bd = result["breakdown"]
    print(f"  Gamma Score   : {bd['gamma_score']} ({bd['positioning']})")
    print(f"  Momentum Score: {bd['momentum_score']} ({bd['direction']})")
    print(f"  Vol Score     : {bd['vol_score']}")
    print(f"{'='*50}\n")

    return {"computed": result}


if __name__ == "__main__":
    out = run_example("SPY")
    print(out)
