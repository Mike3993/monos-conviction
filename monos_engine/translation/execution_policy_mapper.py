"""
MONOS Translation Layer — Execution Policy Mapper
----------------------------------------------------
Applies selected rules to trade candidates:
  1. Structure shaping   (e.g. force naked for MR, prefer spread for tactical)
  2. Hold window shaping (e.g. cap hold to 3d for MR overlay)
  3. Bounded score boost  (capped per rule, capped per trade)
  4. Audit tag attachment (full traceability)
  5. Debug trace output   (shows exactly what each rule did)

This is the APPLICATION stage of the translation pipeline:
  registry → rule_selector → execution_policy_mapper → top_trades

Constraints:
  - Score boosts are bounded by score_boost_max per rule
  - Total boost per trade is capped at MAX_TOTAL_BOOST
  - Structure/hold changes are logged, never silent
  - Every mutation is tagged for audit
  - No auto-execution — advisory only
"""

from __future__ import annotations

from typing import Any

from monos_engine.translation.rule_selector import select_rules_for_trade

# ── caps ─────────────────────────────────────────────────────────────

MAX_TOTAL_BOOST = 0.25   # max total score boost any single trade can receive
MAX_RULES_PER_TRADE = 5  # circuit breaker: max rules that can touch one trade


# ── structure policy thresholds ──────────────────────────────────────

# Mean Reversion: prefer naked only when move is large + confidence high
MR_NAKED_EXPECTED_MOVE_THRESHOLD = 3.0   # expected_return must exceed this %
MR_NAKED_CONFIDENCE_THRESHOLD = 55       # confidence must exceed this

# Tactical: strongly prefer spreads unless convex score is exceptional
TACTICAL_NAKED_SCORE_THRESHOLD = 50.0    # weighted_return must exceed this for naked override


# ── structure policies ───────────────────────────────────────────────

def _apply_structure_policy(
    policy: str,
    trade: dict[str, Any],
    trace: list[str],
) -> str | None:
    """Apply a structure policy and return the new structure (or None if no change).

    Uses CONDITIONAL logic — not hard-force. Structures are only changed
    when specific thresholds are met.

    Policies:
      no_override                — do nothing
      prefer_spread_if_tactical  — TACTICAL: strongly prefer spreads
                                   unless weighted_return > threshold (convex score)
      conditional_naked_if_mr    — MEAN_REVERSION: prefer naked only when
                                   expected_move > threshold AND confidence > threshold
                                   otherwise: default to call/put spread
    """
    mode = trade.get("mode", "")
    direction = trade.get("direction", "LONG")
    structure = trade.get("structure", "")
    confidence = float(trade.get("confidence") or trade.get("win_rate") or 0)
    expected_ret = float(trade.get("expected_return") or trade.get("weighted_return") or 0)

    if policy == "no_override":
        return None

    # ── TACTICAL: strongly prefer spreads ────────────────────────
    if policy == "prefer_spread_if_tactical" and mode == "TACTICAL":
        if structure in ("LONG_CALL", "LONG_PUT"):
            # Exception: allow naked if convex score is exceptional
            if expected_ret > TACTICAL_NAKED_SCORE_THRESHOLD:
                trace.append(
                    f"STRUCTURE: keeping {structure} — weighted_return "
                    f"{expected_ret:.1f}% exceeds threshold {TACTICAL_NAKED_SCORE_THRESHOLD}% "
                    f"(convex exception)"
                )
                return None  # no change — exceptional score overrides spread preference

            # Default: switch to spread for defined risk
            new = "CALL_SPREAD" if direction == "LONG" else "PUT_SPREAD"
            trace.append(
                f"STRUCTURE: {structure} -> {new} — TACTICAL prefers spreads "
                f"(weighted_return {expected_ret:.1f}% below threshold {TACTICAL_NAKED_SCORE_THRESHOLD}%)"
            )
            return new

    # ── MEAN REVERSION: conditional naked ────────────────────────
    if policy == "conditional_naked_if_mr" and mode == "MEAN_REVERSION":
        meets_move = expected_ret > MR_NAKED_EXPECTED_MOVE_THRESHOLD
        meets_conf = confidence > MR_NAKED_CONFIDENCE_THRESHOLD

        if structure in ("CALL_SPREAD", "PUT_SPREAD"):
            # Currently a spread — should we upgrade to naked?
            if meets_move and meets_conf:
                new = "LONG_CALL" if direction == "LONG" else "LONG_PUT"
                trace.append(
                    f"STRUCTURE: {structure} -> {new} — MR naked upgrade "
                    f"(expected {expected_ret:.1f}% > {MR_NAKED_EXPECTED_MOVE_THRESHOLD}%, "
                    f"confidence {confidence:.0f} > {MR_NAKED_CONFIDENCE_THRESHOLD})"
                )
                return new
            else:
                reasons = []
                if not meets_move:
                    reasons.append(f"expected {expected_ret:.1f}% <= {MR_NAKED_EXPECTED_MOVE_THRESHOLD}%")
                if not meets_conf:
                    reasons.append(f"confidence {confidence:.0f} <= {MR_NAKED_CONFIDENCE_THRESHOLD}")
                trace.append(
                    f"STRUCTURE: keeping {structure} — MR naked thresholds not met "
                    f"({', '.join(reasons)})"
                )
                return None

        elif structure in ("LONG_CALL", "LONG_PUT"):
            # Currently naked — should we downgrade to spread?
            if not (meets_move and meets_conf):
                new = "CALL_SPREAD" if direction == "LONG" else "PUT_SPREAD"
                reasons = []
                if not meets_move:
                    reasons.append(f"expected {expected_ret:.1f}% <= {MR_NAKED_EXPECTED_MOVE_THRESHOLD}%")
                if not meets_conf:
                    reasons.append(f"confidence {confidence:.0f} <= {MR_NAKED_CONFIDENCE_THRESHOLD}")
                trace.append(
                    f"STRUCTURE: {structure} -> {new} — MR defaulting to spread "
                    f"({', '.join(reasons)})"
                )
                return new
            else:
                trace.append(
                    f"STRUCTURE: keeping {structure} — MR thresholds met "
                    f"(expected {expected_ret:.1f}% > {MR_NAKED_EXPECTED_MOVE_THRESHOLD}%, "
                    f"confidence {confidence:.0f} > {MR_NAKED_CONFIDENCE_THRESHOLD})"
                )
                return None

    # Legacy: handle old policy name for backward compat
    if policy == "force_naked_if_mr" and mode == "MEAN_REVERSION":
        # Redirect to conditional logic
        return _apply_structure_policy("conditional_naked_if_mr", trade, trace)

    return None


# ── hold policies ────────────────────────────────────────────────────

def _apply_hold_policy(
    policy: str,
    trade: dict[str, Any],
    trace: list[str],
) -> str | None:
    """Apply a hold policy and return the new hold label (or None if no change).

    Policies:
      no_override  — do nothing
      hold_max_3d  — cap hold to 1-3d
    """
    if policy == "no_override":
        return None

    if policy == "hold_max_3d":
        current = trade.get("hold", "")
        # Only override if current hold is longer than 3d
        if current and not current.startswith("1") and current != "1-3d" and current != "2d":
            trace.append(f"HOLD: {current} -> 1-3d (hold_max_3d)")
            return "1-3d"

    return None


# ── main mapper ──────────────────────────────────────────────────────

def apply_policies(trade: dict[str, Any]) -> dict[str, Any]:
    """Apply all eligible rule policies to a single trade candidate.

    Mutates the trade dict in-place with:
      - structure (possibly changed)
      - hold (possibly changed)
      - adjusted_score (score + bounded boost)
      - rule_audit_tags: list of {rule_id, action, detail}
      - policy_trace: list of human-readable trace strings
      - policies_applied: int count

    Returns the modified trade dict.
    """
    eligible = select_rules_for_trade(trade)

    # Circuit breaker
    eligible = eligible[:MAX_RULES_PER_TRADE]

    trace: list[str] = []
    audit_tags: list[dict[str, str]] = []
    total_boost = 0.0
    original_score = float(trade.get("weighted_return", 0) or trade.get("adjusted_score", 0) or 0)

    for rule in eligible:
        rid = rule["rule_id"]
        boost_cap = rule["score_boost_max"]
        confidence = rule["confidence"]
        sharpe_d = rule["sharpe_delta"]

        # 1. Structure policy
        new_struct = _apply_structure_policy(rule["structure_policy"], trade, trace)
        if new_struct:
            trade["structure"] = new_struct
            audit_tags.append({"rule_id": rid, "action": "STRUCTURE_CHANGE", "detail": f"-> {new_struct} via {rule['structure_policy']}"})

        # 2. Hold policy
        new_hold = _apply_hold_policy(rule["hold_policy"], trade, trace)
        if new_hold:
            trade["hold"] = new_hold
            audit_tags.append({"rule_id": rid, "action": "HOLD_CHANGE", "detail": f"-> {new_hold} via {rule['hold_policy']}"})

        # 3. Score boost (bounded)
        if boost_cap > 0:
            # Boost = min(cap, confidence * sharpe_delta * 0.5)
            raw_boost = confidence * max(0, sharpe_d) * 0.5
            bounded = min(raw_boost, boost_cap)

            # Don't exceed total cap
            if total_boost + bounded > MAX_TOTAL_BOOST:
                bounded = max(0, MAX_TOTAL_BOOST - total_boost)

            if bounded > 0:
                total_boost += bounded
                trace.append(f"BOOST: +{bounded:.4f} from {rid} (cap={boost_cap}, raw={raw_boost:.4f})")
                audit_tags.append({"rule_id": rid, "action": "SCORE_BOOST", "detail": f"+{bounded:.4f}"})

        # 4. Audit tag for match
        audit_tags.append({"rule_id": rid, "action": "MATCHED", "detail": rule["match_reason"]})

    # Apply total boost
    trade["original_score"] = original_score
    trade["adjusted_score"] = round(original_score + total_boost, 6)
    trade["rule_audit_tags"] = audit_tags
    trade["policy_trace"] = trace
    trade["policies_applied"] = len(eligible)
    trade["total_boost"] = round(total_boost, 6)
    trade["translation_applied"] = len(eligible) > 0

    return trade


def apply_policies_batch(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Apply policies to all trades, then re-sort by adjusted_score.

    Returns the same list, re-sorted descending by adjusted_score.
    """
    for t in trades:
        apply_policies(t)

    trades.sort(key=lambda t: t.get("adjusted_score", 0), reverse=True)
    return trades


# ── debug trace formatter ────────────────────────────────────────────

def format_trace(trades: list[dict[str, Any]]) -> str:
    """Format a human-readable debug trace for all trades.

    Shows exactly how each rule affected each trade candidate.
    """
    lines = ["=== MONOS TRANSLATION TRACE ===", ""]

    for t in trades:
        ticker = t.get("ticker", "?")
        mode = t.get("mode", "?")
        direction = t.get("direction", "?")
        orig = t.get("original_score", 0)
        adj = t.get("adjusted_score", 0)
        n_rules = t.get("policies_applied", 0)
        boost = t.get("total_boost", 0)

        lines.append(f"--- {ticker} {direction} {mode} ---")
        lines.append(f"  Original: {orig:+.4f}  Adjusted: {adj:+.4f}  Boost: {boost:+.4f}  Rules: {n_rules}")

        trace = t.get("policy_trace", [])
        if trace:
            for line in trace:
                lines.append(f"  [{line}]")
        else:
            lines.append("  [no policies applied]")

        tags = t.get("rule_audit_tags", [])
        if tags:
            lines.append(f"  Audit tags:")
            for tag in tags:
                lines.append(f"    {tag['rule_id']}: {tag['action']} — {tag['detail']}")
        lines.append("")

    lines.append("Generated by MONOS Translation Layer")
    return "\n".join(lines)


def get_batch_summary(trades: list[dict[str, Any]]) -> dict[str, Any]:
    """Summary statistics for a translated batch."""
    total = len(trades)
    touched = sum(1 for t in trades if t.get("translation_applied"))
    total_boost = sum(t.get("total_boost", 0) for t in trades)
    struct_changes = sum(
        1 for t in trades
        for tag in t.get("rule_audit_tags", [])
        if tag["action"] == "STRUCTURE_CHANGE"
    )
    hold_changes = sum(
        1 for t in trades
        for tag in t.get("rule_audit_tags", [])
        if tag["action"] == "HOLD_CHANGE"
    )

    # Check if ranking changed
    orig_order = sorted(trades, key=lambda t: t.get("original_score", 0), reverse=True)
    adj_order = sorted(trades, key=lambda t: t.get("adjusted_score", 0), reverse=True)
    ranking_changed = [t.get("ticker") for t in orig_order] != [t.get("ticker") for t in adj_order]

    return {
        "total_trades": total,
        "trades_touched": touched,
        "total_boost": round(total_boost, 6),
        "structure_changes": struct_changes,
        "hold_changes": hold_changes,
        "ranking_changed": ranking_changed,
    }
