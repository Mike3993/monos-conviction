"""
MONOS Translation Layer — Trade Ranker
----------------------------------------
Applies research-validated rule adjustments to trade ranking
without modifying the core execution logic.

Flow:
  1. Receive batch results from the execution engine
  2. Load active rules from the rule bridge
  3. For each trade, compute rule-based adjustments
  4. Re-rank trades by adjusted score
  5. Log every adjustment for traceability

The ranker ADDS to the existing ranking. It does not replace it.
Original weighted_return remains the primary signal.
Rule adjustments are additive multipliers, capped per governance.

No auto-trading. No live writes. Advisory only.
"""

from __future__ import annotations

from typing import Any

from monos_engine.translation.rule_bridge import (
    get_active_rules,
    log_translation,
)


# ── rule-to-trade matching ───────────────────────────────────────────

def _rule_applies_to_trade(
    rule: dict[str, Any],
    trade: dict[str, Any],
) -> bool:
    """Determine if a rule is relevant to a specific trade.

    Rules match based on chapter scope:
      CH01 (MSA): applies to all trades that have MSA state
      CH02 (Spread): applies when structure is a spread
      CH03 (Vol): applies to all (future)
      CH04 (GEX): applies to all (future)
      CH05 (Ladder): applies when contracts > 1
      CH06 (Sniper): applies to all (future)
    """
    chapter = rule.get("chapter", "")
    rule_id = rule.get("rule_id", "")

    if chapter == "CH01":
        # MSA rules apply to all directional trades
        return True

    if chapter == "CH02":
        # Spread rules apply to spread structures
        structure = trade.get("structure", "")
        return "SPREAD" in structure

    if chapter == "CH04":
        # GEX rules apply when we have GEX data
        return True

    # Default: applies
    return True


def _compute_rule_adjustment(
    rule: dict[str, Any],
    trade: dict[str, Any],
) -> float:
    """Compute the ranking adjustment for a rule on a specific trade.

    The adjustment is a multiplicative factor applied to weighted_return.
    Positive adjustment = boost trade. Negative = penalize.

    MSA rules:
      - MSA aligned (LONG + BULLISH, SHORT + BEARISH) → boost
      - MSA neutral → slight boost for MR mode
      - MSA misaligned → already filtered by backtest, no adjustment

    Returns the adjustment value (not a multiplier).
    """
    influence = rule.get("influence_weight", 0)
    if influence <= 0:
        return 0.0

    chapter = rule.get("chapter", "")
    rule_id = rule.get("rule_id", "")
    sharpe_delta = rule.get("sharpe_delta", 0)

    # Scale adjustment by the rule's empirical Sharpe improvement
    # Sharpe delta acts as the "how good is this rule" signal
    effectiveness = max(0, min(1, sharpe_delta * 2))  # 0.5 Sharpe → 1.0 effectiveness

    # CH01: MSA regime rules
    if chapter == "CH01":
        mode = trade.get("mode", "TACTICAL")
        direction = trade.get("direction", "LONG")

        # C-MSA-01: Bullish gate boost
        if rule_id == "C-MSA-01" and direction == "LONG":
            return influence * effectiveness

        # C-MSA-02: Bearish gate boost
        if rule_id == "C-MSA-02" and direction == "SHORT":
            return influence * effectiveness

        # C-MSA-03: MR overlay boost
        if rule_id == "C-MSA-03" and mode == "MEAN_REVERSION":
            return influence * effectiveness * 0.8  # slight discount for MR

        # C-MSA-04: Position size boost context
        if rule_id == "C-MSA-04":
            return influence * effectiveness * 0.5  # sizing confidence

        # C-MSA-06: Cross-instrument consistency
        if rule_id == "C-MSA-06":
            return influence * effectiveness * 0.3  # minor confidence boost

    # CH02: Spread rules
    if chapter == "CH02":
        structure = trade.get("structure", "")
        if "SPREAD" in structure:
            return influence * effectiveness * 0.6

    # Default: proportional to influence and effectiveness
    return influence * effectiveness * 0.2


# ── main ranking function ────────────────────────────────────────────

def apply_rule_adjustments(
    batch_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Apply research-validated rule adjustments to batch trade ranking.

    Parameters
    ----------
    batch_results : list[dict]
        Trade results from the batch runner. Each dict must have:
        weighted_return, ticker, mode, direction, structure.

    Returns
    -------
    list[dict]
        Same trades with additional fields:
        - original_score: the unmodified weighted_return
        - rule_adjustments: list of {rule_id, adjustment, reason}
        - adjusted_score: weighted_return + sum(adjustments)
        - translation_applied: bool

        Re-sorted by adjusted_score descending.
    """
    active_rules = get_active_rules()

    if not active_rules:
        # No active rules — return original ranking untouched
        for trade in batch_results:
            trade["original_score"] = trade.get("weighted_return", 0)
            trade["adjusted_score"] = trade["original_score"]
            trade["rule_adjustments"] = []
            trade["translation_applied"] = False
        return batch_results

    # Apply adjustments
    for trade in batch_results:
        original = float(trade.get("weighted_return", 0))
        trade["original_score"] = original
        adjustments = []
        total_adj = 0.0

        for rule in active_rules:
            if not _rule_applies_to_trade(rule, trade):
                continue

            adj = _compute_rule_adjustment(rule, trade)
            if abs(adj) < 0.0001:
                continue

            adjustments.append({
                "rule_id": rule["rule_id"],
                "chapter": rule["chapter"],
                "verdict": rule["verdict"],
                "influence_weight": rule["influence_weight"],
                "adjustment": round(adj, 6),
                "reason": f"{rule['rule_id']} ({rule['verdict']}): Sharpe delta {rule['sharpe_delta']:+.4f}",
            })
            total_adj += adj

            # Log for audit trail
            log_translation(
                action="RANK_ADJUST",
                rule_id=rule["rule_id"],
                ticker=trade.get("ticker", ""),
                influence=adj,
                original=original,
                adjusted=original + total_adj,
                notes=f"verdict={rule['verdict']} conf={rule['confidence']}",
            )

        trade["rule_adjustments"] = adjustments
        trade["adjusted_score"] = round(original + total_adj, 6)
        trade["translation_applied"] = len(adjustments) > 0

    # Re-sort by adjusted score
    batch_results.sort(key=lambda t: t.get("adjusted_score", 0), reverse=True)

    return batch_results


def get_translation_summary(
    batch_results: list[dict[str, Any]],
) -> dict[str, Any]:
    """Generate a summary of translation layer activity.

    Returns overview stats about how research rules affected ranking.
    """
    active_rules = get_active_rules()
    total_trades = len(batch_results)
    trades_adjusted = sum(1 for t in batch_results if t.get("translation_applied"))
    total_adjustments = sum(len(t.get("rule_adjustments", [])) for t in batch_results)

    # Check if ranking changed
    original_order = sorted(batch_results, key=lambda t: t.get("original_score", 0), reverse=True)
    adjusted_order = sorted(batch_results, key=lambda t: t.get("adjusted_score", 0), reverse=True)
    ranking_changed = [o.get("ticker") for o in original_order] != [a.get("ticker") for a in adjusted_order]

    return {
        "active_rules": len(active_rules),
        "total_trades": total_trades,
        "trades_adjusted": trades_adjusted,
        "total_adjustments": total_adjustments,
        "ranking_changed": ranking_changed,
        "rules_applied": [
            {"rule_id": r["rule_id"], "verdict": r["verdict"], "influence": r["influence_weight"]}
            for r in active_rules
        ],
    }
