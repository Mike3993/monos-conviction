"""
portfolio_engine.py

MONOS Portfolio Engine — tracks active positions, computes convexity
scores, P&L, and governor states.

Inputs:
    positions   — list of active position dicts
    legs        — option legs per position
    signals     — latest combined signals per ticker

Output per position:
    ticker, structure, convexity_score, governor, pnl, pnl_pct
"""

from __future__ import annotations

from typing import Any


# ── Governor thresholds ──────────────────────────────────────────────

GOVERNOR_RULES = {
    "APPROVED":    {"min_convexity": 60, "max_loss_pct": -15.0},
    "CONDITIONAL": {"min_convexity": 40, "max_loss_pct": -10.0},
    "REJECTED":    {"min_convexity": 0,  "max_loss_pct": 0.0},
}


# ── Convexity scoring ───────────────────────────────────────────────

def score_convexity(position: dict[str, Any], legs: list[dict[str, Any]]) -> float:
    """Compute convexity score (0–100) for a position.

    Scoring factors:
        - Structure type bonus (spreads > naked)
        - Number of legs (more legs = more convex)
        - Risk/reward asymmetry from legs
        - Signal alignment bonus

    Parameters
    ----------
    position : dict
        Must contain: structure_type, entry_price, current_price.
    legs : list[dict]
        Each leg must contain: side (BUY/SELL), strike, premium.

    Returns
    -------
    float
        Convexity score 0–100.
    """
    score = 0.0

    # 1. Structure type base score
    structure = position.get("structure_type", "").upper()
    STRUCTURE_SCORES = {
        "CALL_SPREAD":     55,
        "PUT_SPREAD":      55,
        "CALL_LADDER":     70,
        "CALENDAR_SPREAD": 60,
        "DIAGONAL_SPREAD": 65,
        "LONG_CALL":       40,
        "LONG_PUT":        40,
        "NO_TRADE":        0,
    }
    score += STRUCTURE_SCORES.get(structure, 30)

    # 2. Leg complexity bonus (2 legs = +5, 3+ = +10)
    n_legs = len(legs)
    if n_legs >= 3:
        score += 10
    elif n_legs >= 2:
        score += 5

    # 3. Risk/reward asymmetry from legs
    total_debit = 0.0
    total_credit = 0.0
    for leg in legs:
        premium = leg.get("premium", 0.0)
        side = leg.get("side", "BUY").upper()
        if side == "BUY":
            total_debit += abs(premium)
        else:
            total_credit += abs(premium)

    net_cost = total_debit - total_credit
    if net_cost > 0:
        # Lower net cost relative to debit = better convexity
        cost_ratio = net_cost / total_debit if total_debit > 0 else 1.0
        if cost_ratio < 0.3:
            score += 20   # very cheap spread
        elif cost_ratio < 0.5:
            score += 12
        elif cost_ratio < 0.7:
            score += 5

    # 4. Signal alignment bonus (applied externally via enrich_with_signals)

    return round(max(0.0, min(100.0, score)), 1)


# ── P&L computation ─────────────────────────────────────────────────

def compute_pnl(position: dict[str, Any]) -> dict[str, float]:
    """Compute P&L from entry and current prices.

    Parameters
    ----------
    position : dict
        Must contain: entry_price, current_price, quantity (default 1).

    Returns
    -------
    dict
        pnl (dollar), pnl_pct (percentage).
    """
    entry = position.get("entry_price", 0.0)
    current = position.get("current_price", 0.0)
    quantity = position.get("quantity", 1)

    if entry == 0:
        return {"pnl": 0.0, "pnl_pct": 0.0}

    pnl = round((current - entry) * quantity, 2)
    pnl_pct = round(((current - entry) / abs(entry)) * 100.0, 2)

    return {"pnl": pnl, "pnl_pct": pnl_pct}


# ── Governor evaluation ─────────────────────────────────────────────

def evaluate_governor(
    convexity_score: float,
    pnl_pct: float,
    current_governor: str = "PENDING",
) -> str:
    """Determine governor status based on convexity and P&L.

    Rules:
        APPROVED    — convexity >= 60 AND loss < 15%
        CONDITIONAL — convexity >= 40 AND loss < 10%
        REJECTED    — convexity < 40 OR loss >= 10%

    An APPROVED position that breaches loss threshold
    gets downgraded to CONDITIONAL or REJECTED.

    Parameters
    ----------
    convexity_score : float
    pnl_pct : float
        Percentage P&L (negative = loss).
    current_governor : str
        Current governor state (for transition logic).

    Returns
    -------
    str
        APPROVED, CONDITIONAL, or REJECTED.
    """
    # Hard reject: deep loss or terrible convexity
    if pnl_pct <= -15.0 or convexity_score < 20:
        return "REJECTED"

    # Conditional zone: moderate loss or mediocre convexity
    if pnl_pct <= -10.0 or convexity_score < 40:
        return "CONDITIONAL"

    # Approved: good convexity and acceptable loss
    if convexity_score >= 60:
        return "APPROVED"

    # Middle ground: convexity 40–60
    if current_governor == "APPROVED":
        return "APPROVED"  # don't downgrade on score alone
    return "CONDITIONAL"


# ── Position enrichment ──────────────────────────────────────────────

def enrich_with_signals(
    position_result: dict[str, Any],
    signal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Add signal alignment bonus to convexity score.

    If the latest signal direction matches the position direction,
    boost convexity by up to 10 points.
    """
    if not signal:
        return position_result

    signal_dir = signal.get("combined_signal", signal.get("signal", "NEUTRAL"))
    structure = position_result.get("structure", "").upper()

    # Determine position direction from structure
    if "CALL" in structure:
        pos_dir = "LONG"
    elif "PUT" in structure:
        pos_dir = "SHORT"
    else:
        pos_dir = "NEUTRAL"

    # Alignment bonus
    if pos_dir == signal_dir and signal_dir != "NEUTRAL":
        confidence = signal.get("confidence", 50.0)
        bonus = min(10.0, confidence / 10.0)  # up to 10 points
        position_result["convexity_score"] = round(
            min(100.0, position_result["convexity_score"] + bonus), 1
        )
        position_result["signal_aligned"] = True
    else:
        position_result["signal_aligned"] = False

    return position_result


# ── Core evaluation ──────────────────────────────────────────────────

def evaluate_position(
    position: dict[str, Any],
    legs: list[dict[str, Any]],
    signal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate a single position: score, P&L, governor.

    Parameters
    ----------
    position : dict
        Must contain: ticker, structure_type, entry_price, current_price.
        Optional: quantity, governor_status.
    legs : list[dict]
        Option legs. Each: side, strike, premium.
    signal : dict or None
        Latest combined signal for this ticker.

    Returns
    -------
    dict
        ticker, structure, convexity_score, governor, pnl, pnl_pct.
    """
    ticker = position.get("ticker", "UNKNOWN")
    structure = position.get("structure_type", "UNKNOWN")
    current_governor = position.get("governor_status", "PENDING")

    # Convexity score
    convexity = score_convexity(position, legs)

    # P&L
    pnl_result = compute_pnl(position)

    # Governor
    governor = evaluate_governor(convexity, pnl_result["pnl_pct"], current_governor)

    result = {
        "ticker": ticker,
        "structure": structure,
        "convexity_score": convexity,
        "governor": governor,
        "pnl": pnl_result["pnl"],
        "pnl_pct": pnl_result["pnl_pct"],
    }

    # Signal enrichment
    result = enrich_with_signals(result, signal)

    return result


def evaluate_portfolio(
    positions: list[dict[str, Any]],
    legs_by_ticker: dict[str, list[dict[str, Any]]],
    signals_by_ticker: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Evaluate all positions in a portfolio.

    Parameters
    ----------
    positions : list[dict]
        List of position dicts.
    legs_by_ticker : dict
        Mapping of ticker → list of leg dicts.
    signals_by_ticker : dict or None
        Mapping of ticker → latest signal dict.

    Returns
    -------
    list[dict]
        Evaluated positions sorted by convexity_score descending.
    """
    signals = signals_by_ticker or {}
    results = []

    for pos in positions:
        ticker = pos.get("ticker", "UNKNOWN")
        legs = legs_by_ticker.get(ticker, [])
        signal = signals.get(ticker)
        result = evaluate_position(pos, legs, signal)
        results.append(result)

    # Sort by convexity score descending
    results.sort(key=lambda r: r["convexity_score"], reverse=True)
    return results


# ── Example runner ───────────────────────────────────────────────────

def run_example() -> dict[str, Any]:
    """Run portfolio engine with synthetic positions.

    Returns
    -------
    dict
        portfolio: list of evaluated positions,
        summary: aggregate stats.
    """
    # Synthetic positions
    positions = [
        {
            "ticker": "SLV",
            "structure_type": "CALL_LADDER",
            "entry_price": 3.20,
            "current_price": 4.10,
            "quantity": 10,
            "governor_status": "APPROVED",
        },
        {
            "ticker": "GDX",
            "structure_type": "CALL_SPREAD",
            "entry_price": 2.50,
            "current_price": 2.15,
            "quantity": 5,
            "governor_status": "APPROVED",
        },
        {
            "ticker": "SPY",
            "structure_type": "PUT_SPREAD",
            "entry_price": 4.00,
            "current_price": 2.80,
            "quantity": 3,
            "governor_status": "CONDITIONAL",
        },
        {
            "ticker": "COPX",
            "structure_type": "LONG_CALL",
            "entry_price": 1.80,
            "current_price": 0.40,
            "quantity": 8,
            "governor_status": "PENDING",
        },
        {
            "ticker": "GLD",
            "structure_type": "DIAGONAL_SPREAD",
            "entry_price": 5.50,
            "current_price": 6.80,
            "quantity": 4,
            "governor_status": "APPROVED",
        },
        {
            "ticker": "SILJ",
            "structure_type": "CALENDAR_SPREAD",
            "entry_price": 2.00,
            "current_price": 1.60,
            "quantity": 6,
            "governor_status": "CONDITIONAL",
        },
    ]

    # Synthetic legs
    legs_by_ticker = {
        "SLV": [
            {"side": "BUY", "strike": 32, "premium": 1.80},
            {"side": "SELL", "strike": 35, "premium": 0.90},
            {"side": "SELL", "strike": 38, "premium": 0.40},
        ],
        "GDX": [
            {"side": "BUY", "strike": 45, "premium": 1.60},
            {"side": "SELL", "strike": 50, "premium": 0.80},
        ],
        "SPY": [
            {"side": "BUY", "strike": 560, "premium": 2.80},
            {"side": "SELL", "strike": 550, "premium": 1.20},
        ],
        "COPX": [
            {"side": "BUY", "strike": 25, "premium": 1.80},
        ],
        "GLD": [
            {"side": "BUY", "strike": 240, "premium": 4.50},
            {"side": "SELL", "strike": 250, "premium": 2.00},
        ],
        "SILJ": [
            {"side": "BUY", "strike": 14, "premium": 1.20},
            {"side": "SELL", "strike": 14, "premium": 0.80},
        ],
    }

    # Synthetic signals (as if from signal_combiner)
    signals_by_ticker = {
        "SLV":  {"combined_signal": "LONG",    "confidence": 78.0},
        "GDX":  {"combined_signal": "LONG",    "confidence": 62.0},
        "SPY":  {"combined_signal": "SHORT",   "confidence": 45.0},
        "COPX": {"combined_signal": "LONG",    "confidence": 30.0},
        "GLD":  {"combined_signal": "LONG",    "confidence": 85.0},
        "SILJ": {"combined_signal": "NEUTRAL", "confidence": 50.0},
    }

    results = evaluate_portfolio(positions, legs_by_ticker, signals_by_ticker)

    # Summary
    total_pnl = sum(r["pnl"] for r in results)
    approved = sum(1 for r in results if r["governor"] == "APPROVED")
    conditional = sum(1 for r in results if r["governor"] == "CONDITIONAL")
    rejected = sum(1 for r in results if r["governor"] == "REJECTED")
    avg_convexity = round(sum(r["convexity_score"] for r in results) / len(results), 1) if results else 0.0

    summary = {
        "total_positions": len(results),
        "total_pnl": round(total_pnl, 2),
        "avg_convexity": avg_convexity,
        "approved": approved,
        "conditional": conditional,
        "rejected": rejected,
    }

    print(f"\n{'='*60}")
    print(f"  MONOS PORTFOLIO ENGINE")
    print(f"{'='*60}")
    print(f"  {'TICKER':<8} {'STRUCTURE':<20} {'CONV':>5} {'GOV':<13} {'P&L':>8} {'P&L%':>7}  {'SIG':>5}")
    print(f"  {'-'*8} {'-'*20} {'-'*5} {'-'*13} {'-'*8} {'-'*7}  {'-'*5}")

    for r in results:
        aligned = "YES" if r.get("signal_aligned") else "NO"
        print(
            f"  {r['ticker']:<8} {r['structure']:<20} "
            f"{r['convexity_score']:>5.1f} {r['governor']:<13} "
            f"{r['pnl']:>+8.2f} {r['pnl_pct']:>+6.1f}%  {aligned:>5}"
        )

    print(f"  {'-'*72}")
    print(f"  Total P&L: {summary['total_pnl']:+.2f}  |  Avg Convexity: {summary['avg_convexity']}")
    print(f"  Governor: {approved} APPROVED / {conditional} CONDITIONAL / {rejected} REJECTED")
    print(f"{'='*60}\n")

    return {"portfolio": results, "summary": summary}


if __name__ == "__main__":
    run_example()
