"""
MONOS Top Trade Generator
-------------------------
Sits after the backtest engine.  Converts batch results into the top 3
actionable trade recommendations with structure, sizing, hold period,
and rationale.

Does NOT modify any backtest or engine logic — pure reporting layer.
"""

from __future__ import annotations

from typing import Any


# ── structure / sizing rules per mode ────────────────────────────────

_MODE_RULES: dict[str, dict[str, Any]] = {
    "CONVEX": {
        "structures_long":  "LONG_CALL",
        "structures_short": "LONG_PUT",
        "sizing":           "HIGH",
        "hold_label":       "10-20d",
        "rationale_tag":    "convex regime — full payoff exposure",
    },
    "TACTICAL": {
        "structures_long":  "CALL_SPREAD",
        "structures_short": "PUT_SPREAD",
        "sizing":           "MEDIUM",
        "hold_label":       "1-3d",
        "rationale_tag":    "high win-rate short-term edge",
    },
    "HYBRID": {
        "structures_long":  "CALL_SPREAD",
        "structures_short": "PUT_SPREAD",
        "sizing":           "MEDIUM-LOW",
        "hold_label":       "5-10d",
        "rationale_tag":    "sector momentum with defined risk",
    },
    "MEAN_REVERSION": {
        "structures_long":  "LONG_CALL",
        "structures_short": "LONG_PUT",
        "sizing":           "MEDIUM",
        "hold_label":       "1-3d",
        "rationale_tag":    "mean-reversion snap-back setup",
    },
}

_DEFAULT_RULE = _MODE_RULES["TACTICAL"]


# ── helpers ──────────────────────────────────────────────────────────

def _infer_direction(row: dict[str, Any]) -> str:
    """Infer LONG or SHORT from the batch result.

    Uses the last trade's signal when available, otherwise falls back
    to the sign of weighted_return (positive → LONG bias).
    """
    # If the batch row carries last_signal from the example result
    last_sig = row.get("last_signal")
    if last_sig in ("LONG", "SHORT"):
        return last_sig

    # Fallback: positive weighted return → bullish bias
    wgt = row.get("weighted_return", 0)
    return "LONG" if wgt >= 0 else "SHORT"


def _build_rationale(rank: int, row: dict[str, Any], rule: dict[str, Any]) -> str:
    """Generate a one-line rationale for the recommendation."""
    parts = []

    if rank == 1:
        parts.append("highest weighted return in system")
    elif rank == 2:
        parts.append("second-strongest weighted return")
    else:
        parts.append("third-ranked opportunity")

    wr = row.get("win_rate", 0)
    if wr >= 75:
        parts.append(f"elite {wr:.0f}% win rate")
    elif wr >= 60:
        parts.append(f"strong {wr:.0f}% win rate")

    strength = row.get("strength", "")
    if strength == "HIGH":
        parts.append("HIGH signal strength")

    parts.append(rule["rationale_tag"])

    return " + ".join(parts)


# ── main generator ───────────────────────────────────────────────────

def generate_top_trades(
    batch_results: list[dict[str, Any]],
    top_n: int = 3,
) -> dict[str, Any]:
    """Generate the top *top_n* actionable trade recommendations.

    Parameters
    ----------
    batch_results : list[dict]
        List of per-ticker batch result dicts.  Expected keys:
        ticker, mode, best_hold, win_rate, weighted_return,
        strength, trades, mr_trades, hc_trades.
        (This is exactly what ``/api/run-batch`` returns in ``results``.)
    top_n : int
        Number of top trades to return (default 3).

    Returns
    -------
    dict
        ``{"top_trades": [...], "formatted": "..."}``
    """
    # ── Step 1: rank ──────────────────────────────────────────────
    ranked = sorted(
        batch_results,
        key=lambda r: (r.get("weighted_return", 0), r.get("win_rate", 0)),
        reverse=True,
    )

    # ── Step 2: select top N ──────────────────────────────────────
    selected = ranked[:top_n]

    # ── Step 3-5: build recommendations ───────────────────────────
    trades: list[dict[str, Any]] = []

    for idx, row in enumerate(selected):
        rank = idx + 1
        mode = row.get("mode", "TACTICAL")
        rule = _MODE_RULES.get(mode, _DEFAULT_RULE)

        direction = _infer_direction(row)
        structure = rule["structures_long"] if direction == "LONG" else rule["structures_short"]

        rec = {
            "rank":             rank,
            "ticker":           row.get("ticker", "???"),
            "direction":        direction,
            "mode":             mode,
            "structure":        structure,
            "hold":             rule["hold_label"],
            "sizing":           rule["sizing"],
            "win_rate":         round(row.get("win_rate", 0), 1),
            "weighted_return":  round(row.get("weighted_return", 0), 2),
            "strength":         row.get("strength", "LOW"),
            "mr_trades":        row.get("mr_trades", 0),
            "hc_trades":        row.get("hc_trades", 0),
            "rationale":        _build_rationale(rank, row, rule),
        }
        trades.append(rec)

    # ── Step 6: formatted text output ─────────────────────────────
    lines = ["=== MONOS TOP TRADES ===", ""]
    for t in trades:
        lines.append(f"{t['rank']}. {t['ticker']}")
        lines.append(f"   Direction : {t['direction']}")
        lines.append(f"   Mode      : {t['mode']}")
        lines.append(f"   Structure : {t['structure']}")
        lines.append(f"   Hold      : {t['hold']}")
        lines.append(f"   Sizing    : {t['sizing']}")
        lines.append(f"   Win Rate  : {t['win_rate']}%")
        lines.append(f"   Wgt Return: {t['weighted_return']}%")
        lines.append(f"   Strength  : {t['strength']}")
        lines.append(f"   Rationale : {t['rationale']}")
        lines.append("")

    lines.append("Generated by MONOS Conviction Engine")
    formatted = "\n".join(lines)

    return {
        "top_trades": trades,
        "formatted":  formatted,
    }


# ── CLI test ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    sample = [
        {"ticker": "SLV", "mode": "CONVEX",   "best_hold": 2,  "win_rate": 66.7, "weighted_return": 41.93, "strength": "HIGH",   "trades": 2, "mr_trades": 2, "hc_trades": 0},
        {"ticker": "GLD", "mode": "CONVEX",   "best_hold": 5,  "win_rate": 100,  "weighted_return": 132.0, "strength": "HIGH",   "trades": 1, "mr_trades": 0, "hc_trades": 0},
        {"ticker": "SPY", "mode": "TACTICAL", "best_hold": 2,  "win_rate": 80.0, "weighted_return": 7.54,  "strength": "HIGH",   "trades": 5, "mr_trades": 0, "hc_trades": 3},
        {"ticker": "QQQ", "mode": "TACTICAL", "best_hold": 2,  "win_rate": 57.0, "weighted_return": 4.40,  "strength": "MEDIUM", "trades": 4, "mr_trades": 0, "hc_trades": 2},
        {"ticker": "SMH", "mode": "HYBRID",   "best_hold": 10, "win_rate": 75.0, "weighted_return": 5.50,  "strength": "MEDIUM", "trades": 2, "mr_trades": 0, "hc_trades": 2},
        {"ticker": "IWM", "mode": "TACTICAL", "best_hold": 2,  "win_rate": 50.0, "weighted_return": -1.20, "strength": "LOW",    "trades": 3, "mr_trades": 0, "hc_trades": 1},
    ]
    result = generate_top_trades(sample)
    print(result["formatted"])
