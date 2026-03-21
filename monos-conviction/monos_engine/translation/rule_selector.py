"""
MONOS Translation Layer — Rule Selector
-----------------------------------------
Reads the governed validated_rule_registry.csv and selects
which rules are eligible to influence a specific trade.

This is the FILTER stage of the translation pipeline:
  registry → rule_selector → execution_policy_mapper → top_trades

A rule is eligible if:
  1. active == true in the registry
  2. verdict is in the allowed set (INTERNAL_PASS, PROVISIONAL, EARNS_PLACE)
  3. not killed via kill_switch
  4. the rule's chapter/scope applies to the trade's mode/direction

The selector returns eligible rules with their caps and policies
but does NOT apply them.  Application is the policy mapper's job.
"""

from __future__ import annotations

import csv
import os
from typing import Any

from monos_engine.translation.rule_bridge import load_rule_state

_REGISTRY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "validated_rule_registry.csv")

_ALLOWED_VERDICTS = {"INTERNAL_PASS", "PROVISIONAL", "EARNS_PLACE"}


def load_registry() -> list[dict[str, Any]]:
    """Load the governed rule registry CSV.

    Returns list of rule dicts with typed fields.
    """
    if not os.path.exists(_REGISTRY_PATH):
        return []

    rows = []
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            with open(_REGISTRY_PATH, "r", encoding=enc) as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            break
        except (UnicodeDecodeError, UnicodeError):
            continue

    # Type-cast
    typed = []
    for r in rows:
        typed.append({
            "rule_id":          r.get("rule_id", ""),
            "chapter":          r.get("chapter", ""),
            "rule_name":        r.get("rule_name", ""),
            "rule_type":        r.get("rule_type", "A"),
            "verdict":          r.get("verdict", ""),
            "confidence":       float(r.get("confidence", 0) or 0),
            "sharpe_delta":     float(r.get("sharpe_delta", 0) or 0),
            "influence_cap":    float(r.get("influence_cap", 0) or 0),
            "structure_policy": r.get("structure_policy", "no_override"),
            "hold_policy":      r.get("hold_policy", "no_override"),
            "score_boost_max":  float(r.get("score_boost_max", 0) or 0),
            "active":           r.get("active", "false").strip().lower() == "true",
            "notes":            r.get("notes", ""),
        })
    return typed


def select_rules_for_trade(trade: dict[str, Any]) -> list[dict[str, Any]]:
    """Select all eligible rules that apply to a specific trade.

    Parameters
    ----------
    trade : dict
        Must have: ticker, mode, direction, structure.

    Returns
    -------
    list[dict]
        Eligible rules with their policies and caps.
        Each entry is the registry row enriched with:
        - eligible: True
        - match_reason: str explaining why this rule applies
    """
    registry = load_registry()
    state = load_rule_state()
    mode = trade.get("mode", "TACTICAL")
    direction = trade.get("direction", "LONG")
    structure = trade.get("structure", "")

    eligible = []

    for rule in registry:
        rid = rule["rule_id"]

        # Gate 1: active in registry
        if not rule["active"]:
            continue

        # Gate 2: allowed verdict
        if rule["verdict"] not in _ALLOWED_VERDICTS:
            continue

        # Gate 3: not killed
        rs = state.get(rid, {})
        if rs.get("killed", False) or rs.get("force_exclude", False):
            continue

        # Gate 4: score_boost_max > 0 (has something to contribute)
        if rule["score_boost_max"] <= 0 and rule["structure_policy"] == "no_override" and rule["hold_policy"] == "no_override":
            continue

        # Gate 5: chapter scope applies to this trade
        chapter = rule["chapter"]
        match_reason = ""

        if chapter == "CH01":
            # MSA rules apply to all directional trades
            # But specific rules have directional affinity
            if rid == "C-MSA-01" and direction == "LONG":
                match_reason = "MSA bullish gate boosts LONG trades"
            elif rid == "C-MSA-02" and direction == "SHORT":
                match_reason = "MSA bearish gate boosts SHORT trades"
            elif rid == "C-MSA-03" and mode == "MEAN_REVERSION":
                match_reason = "MSA neutral MR overlay applies to MEAN_REVERSION mode"
            elif rid == "C-MSA-04":
                match_reason = "MSA position size boost applies to all aligned trades"
            elif rid == "C-MSA-06":
                match_reason = "Cross-instrument consistency applies to all trades"
            else:
                # Rule exists but doesn't match this trade's direction/mode
                continue

        elif chapter == "CH02":
            if "SPREAD" in structure:
                match_reason = "Spread structure rule applies"
            else:
                continue

        else:
            # Future chapters — skip for now
            continue

        rule_copy = dict(rule)
        rule_copy["eligible"] = True
        rule_copy["match_reason"] = match_reason
        eligible.append(rule_copy)

    return eligible


def get_registry_summary() -> dict[str, Any]:
    """Summary of the registry for status display."""
    registry = load_registry()
    total = len(registry)
    active = sum(1 for r in registry if r["active"])
    by_verdict = {}
    for r in registry:
        v = r["verdict"]
        by_verdict[v] = by_verdict.get(v, 0) + 1

    return {
        "total_rules": total,
        "active_rules": active,
        "by_verdict": by_verdict,
    }
