"""
MONOS Convex Payoff Engine
--------------------------
Replaces simple linear option return multipliers with a more realistic
convex payoff approximation.  No Black-Scholes, no external APIs —
deterministic, mode-aware, and ready to plug into the backtest.
"""

from __future__ import annotations


# ── mode multipliers ────────────────────────────────────────────────
def _mode_factors(mode: str) -> tuple[float, float]:
    """Return (gamma_mult, theta_mult) adjustments for the given mode."""
    if mode == "CONVEX":
        return 1.25, 0.75          # +25 % gamma, -25 % theta
    if mode == "HYBRID":
        return 1.125, 0.875        # midpoint
    return 1.0, 1.0                # TACTICAL / default


# ── core estimator ──────────────────────────────────────────────────
def estimate_option_return(
    structure: str,
    underlying_return_pct: float,
    hold_days: int,
    implied_vol: float = 0.30,
    dte: int = 30,
    moneyness: str = "ATM",
    mode: str = "TACTICAL",
) -> float:
    """
    Estimate option-level return (%) from an underlying move.

    Parameters
    ----------
    structure : str
        LONG_CALL | LONG_PUT | CALL_SPREAD | PUT_SPREAD
    underlying_return_pct : float
        Raw underlying return as (exit-entry)/entry * 100.
    hold_days : int
        Number of calendar days the position was held.
    implied_vol : float
        Implied volatility (decimal, e.g. 0.30 for 30 %).
    dte : int
        Days to expiration at entry.
    moneyness : str
        "ATM" or "OTM".
    mode : str
        "TACTICAL" | "HYBRID" | "CONVEX".

    Returns
    -------
    float
        Estimated option return in percent.
    """
    gamma_mult, theta_mult = _mode_factors(mode)
    raw = underlying_return_pct

    # ── LONG_CALL ───────────────────────────────────────────────────
    if structure == "LONG_CALL":
        delta = 3.0 if moneyness == "OTM" else 2.0
        option_return = raw * delta

        # gamma boost on strong moves
        if raw > 1.5:
            option_return *= 1.5 * gamma_mult

        # theta drag
        option_return -= 0.10 * hold_days * theta_mult

        # IV expansion on large absolute moves
        if abs(raw) > 2.0:
            option_return += 0.50

        return option_return

    # ── LONG_PUT ────────────────────────────────────────────────────
    if structure == "LONG_PUT":
        delta = 3.0 if moneyness == "OTM" else 2.0
        # invert: a negative underlying move is good for puts
        option_return = (-raw) * delta

        # gamma boost on strong downside moves
        if raw < -1.5:
            option_return *= 1.5 * gamma_mult

        # theta drag
        option_return -= 0.10 * hold_days * theta_mult

        # IV expansion on large absolute moves
        if abs(raw) > 2.0:
            option_return += 0.50

        return option_return

    # ── CALL_SPREAD ─────────────────────────────────────────────────
    if structure == "CALL_SPREAD":
        option_return = raw * 1.2

        # cap / floor
        option_return = min(option_return, 1.5)
        option_return = max(option_return, -2.5)

        # small theta drag
        option_return -= 0.05 * hold_days * theta_mult

        return option_return

    # ── PUT_SPREAD ──────────────────────────────────────────────────
    if structure == "PUT_SPREAD":
        option_return = (-raw) * 1.2

        # cap / floor
        option_return = min(option_return, 1.5)
        option_return = max(option_return, -2.5)

        # small theta drag
        option_return -= 0.05 * hold_days * theta_mult

        return option_return

    # fallback — unknown structure
    return underlying_return_pct


# ── example runner ──────────────────────────────────────────────────
def run_example() -> list[dict]:
    """Run a handful of representative scenarios and return results."""
    examples = [
        ("LONG_CALL",   2.0,  2, 0.35, 30, "ATM", "TACTICAL"),
        ("LONG_PUT",   -3.0,  2, 0.40, 30, "ATM", "CONVEX"),
        ("CALL_SPREAD", 1.5,  2, 0.25, 30, "ATM", "TACTICAL"),
        ("PUT_SPREAD", -2.0,  3, 0.30, 30, "ATM", "HYBRID"),
    ]

    results = []
    for struct, ret, hd, iv, dte, money, mode in examples:
        opt_ret = estimate_option_return(
            structure=struct,
            underlying_return_pct=ret,
            hold_days=hd,
            implied_vol=iv,
            dte=dte,
            moneyness=money,
            mode=mode,
        )
        row = {
            "structure": struct,
            "underlying_return_pct": ret,
            "hold_days": hd,
            "implied_vol": iv,
            "dte": dte,
            "moneyness": money,
            "mode": mode,
            "option_return_pct": round(opt_ret, 4),
        }
        results.append(row)
        print(
            f"  {struct:<14s} | UND {ret:+6.2f}% | "
            f"Hold {hd}d | IV {iv:.0%} | {money} | {mode:<8s} "
            f"→ OPT {opt_ret:+.4f}%"
        )

    return results


# ── CLI entry point ─────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 72)
    print("MONOS Convex Payoff Engine — Example Scenarios")
    print("=" * 72)
    run_example()
    print("=" * 72)
