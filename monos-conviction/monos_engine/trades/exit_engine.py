"""
MONOS Auto Exit Recommendation Engine
--------------------------------------
Advisory-only exit logic for all open trades.
Does NOT auto-close — operator must always confirm.

Consumes trade data from the ledger and returns structured
recommendations with exit_state, urgency, explanation,
rule_tags, and action_text.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


# ── calculated fields ────────────────────────────────────────────────

def _compute_metrics(trade: dict[str, Any]) -> dict[str, float]:
    """Derive progress metrics from a trade dict.

    Returns dict with: distance_to_tp, distance_to_sl, tp_progress,
    sl_progress, time_progress.
    """
    ret = float(trade.get("unrealized_return_pct", 0) or 0)
    tp = float(trade.get("tp_target", 0) or trade.get("take_profit", 1.5) or 1.5)
    sl = float(trade.get("sl_target", 0) or trade.get("stop_loss", 1.0) or 1.0)
    hold_target = int(trade.get("hold_target", 0) or trade.get("hold_days", 2) or 2)
    days_held = int(trade.get("days_held", 0) or 0)

    # If current_mid is missing, compute from bid/ask
    bid = float(trade.get("current_bid", 0) or 0)
    ask = float(trade.get("current_ask", 0) or 0)
    mid = float(trade.get("current_mid", 0) or 0)
    if mid == 0 and (bid + ask) > 0:
        mid = round((bid + ask) / 2, 4)

    return {
        "unrealized_return_pct": ret,
        "current_mid": mid,
        "tp_target": tp,
        "sl_target": sl,
        "hold_target": hold_target,
        "days_held": days_held,
        "distance_to_tp": round(tp - ret, 4),
        "distance_to_sl": round(ret + sl, 4),
        "tp_progress": round(ret / tp, 4) if tp > 0 else 0,
        "sl_progress": round(abs(ret) / sl, 4) if sl > 0 and ret < 0 else 0,
        "time_progress": round(days_held / hold_target, 4) if hold_target > 0 else 0,
    }


# ── mode-specific exit rules ────────────────────────────────────────

def _tactical_exit(ret: float, tp: float, sl: float,
                   days: int, hold: int) -> dict[str, Any]:
    if ret >= tp:
        return {
            "exit_state": "TAKE_PROFIT",
            "urgency": "HIGH",
            "explanation": f"Return {ret:.2f}% has reached the {tp:.1f}% take-profit target. Lock in gains.",
            "rule_tags": ["TP_HIT"],
            "action_text": "Close full position",
        }
    if ret >= 0.8 * tp:
        return {
            "exit_state": "TAKE_PROFIT_SOON",
            "urgency": "MEDIUM",
            "explanation": f"Return {ret:.2f}% is within 80% of the {tp:.1f}% target. Consider closing or tightening stop.",
            "rule_tags": ["TP_NEAR"],
            "action_text": "Close or tighten stop to lock partial gains",
        }
    if ret <= -sl:
        return {
            "exit_state": "CUT_LOSS",
            "urgency": "HIGH",
            "explanation": f"Return {ret:.2f}% has breached the -{sl:.1f}% stop loss. Exit immediately to limit damage.",
            "rule_tags": ["SL_HIT"],
            "action_text": "Close full position",
        }
    if ret <= -0.8 * sl:
        return {
            "exit_state": "DANGER",
            "urgency": "MEDIUM",
            "explanation": f"Return {ret:.2f}% is approaching the -{sl:.1f}% stop loss. Prepare to exit.",
            "rule_tags": ["SL_NEAR"],
            "action_text": "Prepare exit order — close if deterioration continues",
        }
    if days >= hold:
        return {
            "exit_state": "TIME_EXIT",
            "urgency": "MEDIUM",
            "explanation": f"Held {days}d, exceeding the {hold}d target hold period. Theta decay accelerating.",
            "rule_tags": ["TIME_EXCEEDED"],
            "action_text": f"Close — held {days}d past {hold}d target",
        }
    return {
        "exit_state": "HOLD",
        "urgency": "LOW",
        "explanation": f"Position within parameters. {tp - ret:.1f}% to TP, {ret + sl:.1f}% buffer to SL, {hold - days}d remaining.",
        "rule_tags": [],
        "action_text": "Hold and reassess tomorrow",
    }


def _convex_exit(ret: float, tp: float, sl: float,
                 days: int, hold: int) -> dict[str, Any]:
    if ret >= 2 * tp:
        return {
            "exit_state": "SCALE_OUT",
            "urgency": "MEDIUM",
            "explanation": f"Return {ret:.2f}% has reached 2x the {tp:.1f}% target. Exceptional move — take partial profits and let a runner position continue.",
            "rule_tags": ["TP_HIT", "CONVEX_RUNNER"],
            "action_text": "Trim 50%, keep runner",
        }
    if ret >= tp:
        return {
            "exit_state": "TRIM_HOLD",
            "urgency": "MEDIUM",
            "explanation": f"Return {ret:.2f}% has reached the {tp:.1f}% target. Convex regime may have further upside — trim and hold remainder.",
            "rule_tags": ["TP_HIT", "CONVEX_RUNNER"],
            "action_text": "Trim 30-50%, trail stop on remainder",
        }
    if ret <= -sl:
        return {
            "exit_state": "CUT_LOSS",
            "urgency": "HIGH",
            "explanation": f"Return {ret:.2f}% has breached the -{sl:.1f}% stop loss. Convex thesis invalidated.",
            "rule_tags": ["SL_HIT"],
            "action_text": "Close full position",
        }
    if days >= hold and ret <= 0:
        return {
            "exit_state": "REVIEW",
            "urgency": "MANUAL",
            "explanation": f"Held {days}d past {hold}d target and not profitable ({ret:.2f}%). Convex move hasn't materialized — reassess thesis.",
            "rule_tags": ["TIME_EXCEEDED"],
            "action_text": "Manual review — close or extend with fresh thesis",
        }
    return {
        "exit_state": "HOLD",
        "urgency": "LOW",
        "explanation": f"Convex position within parameters. Allowing time for outsized move. {hold - days}d remaining.",
        "rule_tags": ["CONVEX_RUNNER"] if ret > 0 else [],
        "action_text": "Hold and reassess tomorrow",
    }


def _mean_reversion_exit(ret: float, tp: float, sl: float,
                         days: int, hold: int) -> dict[str, Any]:
    if ret > 0:
        return {
            "exit_state": "TAKE_PROFIT",
            "urgency": "HIGH",
            "explanation": f"Mean-reversion trade is profitable at {ret:.2f}%. The snap-back has occurred — exit to lock in reversion gain.",
            "rule_tags": ["TP_HIT", "MR_EXIT"],
            "action_text": "Close full position",
        }
    if days > 2:
        return {
            "exit_state": "TIME_EXIT",
            "urgency": "HIGH",
            "explanation": f"Mean-reversion trade held {days}d with {ret:.2f}% return. Reversion window has expired — exit to prevent further loss.",
            "rule_tags": ["TIME_EXCEEDED", "MR_EXIT"],
            "action_text": "Close full position",
        }
    if ret <= -sl:
        return {
            "exit_state": "CUT_LOSS",
            "urgency": "HIGH",
            "explanation": f"Return {ret:.2f}% has breached the -{sl:.1f}% stop. Mean-reversion thesis failed.",
            "rule_tags": ["SL_HIT", "MR_EXIT"],
            "action_text": "Close full position",
        }
    return {
        "exit_state": "HOLD",
        "urgency": "LOW",
        "explanation": f"Mean-reversion trade in progress. {2 - days}d remaining in reversion window. Watching for snap-back.",
        "rule_tags": [],
        "action_text": "Hold — reversion window still open",
    }


def _hybrid_exit(ret: float, tp: float, sl: float,
                 days: int, hold: int) -> dict[str, Any]:
    if ret >= tp:
        return {
            "exit_state": "TAKE_PROFIT",
            "urgency": "MEDIUM",
            "explanation": f"Return {ret:.2f}% has reached the {tp:.1f}% target. Sector momentum captured.",
            "rule_tags": ["TP_HIT"],
            "action_text": "Close full position",
        }
    if ret >= 0.8 * tp:
        return {
            "exit_state": "TAKE_PROFIT_SOON",
            "urgency": "MEDIUM",
            "explanation": f"Return {ret:.2f}% is within 80% of the {tp:.1f}% target. Consider locking in gains.",
            "rule_tags": ["TP_NEAR"],
            "action_text": "Close or tighten stop",
        }
    if ret <= -sl:
        return {
            "exit_state": "CUT_LOSS",
            "urgency": "HIGH",
            "explanation": f"Return {ret:.2f}% has breached the -{sl:.1f}% stop loss. Exit to limit drawdown.",
            "rule_tags": ["SL_HIT"],
            "action_text": "Close full position",
        }
    if days >= hold:
        if ret > 0:
            return {
                "exit_state": "REVIEW",
                "urgency": "MANUAL",
                "explanation": f"Held {days}d past {hold}d target but still profitable at {ret:.2f}%. Could extend if trend persists.",
                "rule_tags": ["TIME_EXCEEDED"],
                "action_text": "Manual review — close or extend",
            }
        return {
            "exit_state": "REVIEW",
            "urgency": "MANUAL",
            "explanation": f"Held {days}d past {hold}d target and not profitable ({ret:.2f}%). Sector momentum may have faded.",
            "rule_tags": ["TIME_EXCEEDED"],
            "action_text": "Manual review — likely close",
        }
    return {
        "exit_state": "HOLD",
        "urgency": "LOW",
        "explanation": f"Hybrid position within parameters. {hold - days}d remaining in hold window.",
        "rule_tags": [],
        "action_text": "Hold and reassess tomorrow",
    }


# ── main entry points ───────────────────────────────────────────────

def get_exit_recommendation(trade: dict[str, Any]) -> dict[str, Any]:
    """Generate an exit recommendation for a single open trade.

    Parameters
    ----------
    trade : dict
        Trade data from the open-pnl endpoint or ledger.
        Expected keys: mode, unrealized_return_pct, tp_target/take_profit,
        sl_target/stop_loss, days_held, hold_target/hold_days,
        current_bid, current_ask, current_mid, ticker, direction, etc.

    Returns
    -------
    dict
        {exit_state, urgency, explanation, rule_tags, action_text, metrics}
    """
    mode = (trade.get("mode") or trade.get("trade_mode") or "TACTICAL").upper()
    metrics = _compute_metrics(trade)

    ret = metrics["unrealized_return_pct"]
    tp = metrics["tp_target"]
    sl = metrics["sl_target"]
    days = metrics["days_held"]
    hold = metrics["hold_target"]

    # Dispatch to mode-specific rules
    if mode == "TACTICAL":
        rec = _tactical_exit(ret, tp, sl, days, hold)
    elif mode == "CONVEX":
        rec = _convex_exit(ret, tp, sl, days, hold)
    elif mode == "MEAN_REVERSION":
        rec = _mean_reversion_exit(ret, tp, sl, days, hold)
    elif mode == "HYBRID":
        rec = _hybrid_exit(ret, tp, sl, days, hold)
    else:
        rec = _tactical_exit(ret, tp, sl, days, hold)

    # Append MSA tag if available
    msa = trade.get("msa_state", "")
    if msa:
        tag = msa.replace("MSA_", "MSA_")  # already prefixed
        rec["rule_tags"].append(tag)

    # Attach trade identity + metrics for context
    rec["trade_id"] = trade.get("id")
    rec["ticker"] = trade.get("ticker", "")
    rec["direction"] = trade.get("direction", "")
    rec["mode"] = mode
    rec["structure"] = trade.get("structure", "")
    rec["contracts"] = trade.get("contracts", 1)
    rec["entry_price"] = trade.get("entry_price", 0)
    rec["current_mid"] = metrics["current_mid"]
    rec["unrealized_return_pct"] = ret
    rec["unrealized_pnl"] = trade.get("unrealized_pnl", 0)
    rec["days_held"] = days
    rec["metrics"] = metrics

    return rec


def get_all_exit_recommendations(
    open_trades: list[dict[str, Any]],
) -> dict[str, Any]:
    """Generate exit recommendations for all open trades.

    Parameters
    ----------
    open_trades : list[dict]
        List of open trade dicts (from /api/ledger/open-pnl).

    Returns
    -------
    dict
        {
            recommendations: list[dict],
            summary: {
                total, hold_count, action_count,
                high_urgency, medium_urgency, manual_review,
                tags_summary: {tag: count}
            },
            formatted: str  (copy-ready text)
        }
    """
    recs = [get_exit_recommendation(t) for t in open_trades]

    # Summary
    total = len(recs)
    hold_count = sum(1 for r in recs if r["exit_state"] == "HOLD")
    action_count = total - hold_count
    high_urgency = sum(1 for r in recs if r["urgency"] == "HIGH")
    medium_urgency = sum(1 for r in recs if r["urgency"] == "MEDIUM")
    manual_review = sum(1 for r in recs if r["urgency"] == "MANUAL")

    # Tag frequency
    all_tags = [tag for r in recs for tag in r["rule_tags"]]
    tags_summary = {}
    for tag in all_tags:
        tags_summary[tag] = tags_summary.get(tag, 0) + 1

    summary = {
        "total": total,
        "hold_count": hold_count,
        "action_count": action_count,
        "high_urgency": high_urgency,
        "medium_urgency": medium_urgency,
        "manual_review": manual_review,
        "tags_summary": tags_summary,
    }

    # Formatted text for clipboard
    lines = ["=== MONOS EXIT RECOMMENDATIONS ===", ""]
    lines.append(f"Open Trades: {total}")
    lines.append(f"Action Required: {action_count} ({high_urgency} HIGH, {medium_urgency} MEDIUM, {manual_review} MANUAL)")
    lines.append(f"Hold: {hold_count}")
    lines.append(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("")

    # Sort: HIGH urgency first, then MEDIUM, MANUAL, LOW
    urgency_order = {"HIGH": 0, "MEDIUM": 1, "MANUAL": 2, "LOW": 3}
    sorted_recs = sorted(recs, key=lambda r: urgency_order.get(r["urgency"], 4))

    for r in sorted_recs:
        marker = "!!!" if r["urgency"] == "HIGH" else ">> " if r["urgency"] in ("MEDIUM", "MANUAL") else "   "
        lines.append(f"{marker} #{r['trade_id']} {r['ticker']} {r['direction']} {r['mode']}")
        lines.append(f"    State: {r['exit_state']} | Urgency: {r['urgency']}")
        lines.append(f"    Return: {r['unrealized_return_pct']:.2f}% | PnL: ${r.get('unrealized_pnl', 0):.2f} | Days: {r['days_held']}")
        lines.append(f"    {r['explanation']}")
        lines.append(f"    Action: {r['action_text']}")
        if r["rule_tags"]:
            lines.append(f"    Tags: {', '.join(r['rule_tags'])}")
        lines.append("")

    lines.append("Generated by MONOS Conviction Engine")

    return {
        "recommendations": recs,
        "summary": summary,
        "formatted": "\n".join(lines),
    }
