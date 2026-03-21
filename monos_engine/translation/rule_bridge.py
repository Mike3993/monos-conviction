"""
MONOS Translation Layer — Rule Bridge
---------------------------------------
Connects the research-lab evidence system to the execution engine.

This is the ONE controlled pathway from research → execution.
All influence must flow through this bridge:

  research-lab/evidence_table.csv
       ↓
  rule_bridge.load_evidence()
       ↓
  rule_bridge.get_active_rules()
       ↓
  trade_ranker.apply_rule_adjustments()
       ↓
  top_trades output (advisory only)

Governance constraints enforced here:
  - Only INTERNAL_PASS, PROVISIONAL, EARNS_PLACE rules can influence
  - Type C rules are capped at max_influence (default 0.15)
  - CONDITIONAL / HURTS / INSUFFICIENT rules have ZERO influence
  - Kill-switched rules are immediately excluded
  - Every adjustment is logged with full traceability
"""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from typing import Any

# ── paths ────────────────────────────────────────────────────────────

_TRANSLATION_DIR = os.path.dirname(os.path.abspath(__file__))
# monos-conviction/monos_engine/translation/ → monos-conviction/ → parent → monos-research-lab/
_CONVICTION_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(_TRANSLATION_DIR)))
_RESEARCH_LAB = os.path.join(_CONVICTION_ROOT, "monos-research-lab")

# Fallback: check common locations
if not os.path.isdir(_RESEARCH_LAB):
    _alt = os.path.join(os.path.dirname(_CONVICTION_ROOT), "monos-research-lab")
    if os.path.isdir(_alt):
        _RESEARCH_LAB = _alt

_EVIDENCE_CSV = os.path.join(_RESEARCH_LAB, "reports", "evidence_table.csv")
_RULE_STATE_FILE = os.path.join(_TRANSLATION_DIR, "rule_state.json")
_AUDIT_LOG = os.path.join(_TRANSLATION_DIR, "translation_audit.csv")

# ── influence caps ───────────────────────────────────────────────────

# Maximum adjustment a single rule can apply to trade ranking
_INFLUENCE_CAPS = {
    "EARNS_PLACE": 0.25,                 # Full Canon — max influence
    "INTERNAL_PASS": 0.15,               # Empirical winner — moderate influence
    "PROVISIONAL": 0.10,                 # Provisional — light influence
    "CONDITIONAL_PENDING_TYLER": 0.08,   # Pending Tyler — very light
    "CONDITIONAL": 0.0,                  # No influence
    "REDUNDANT": 0.0,
    "HURTS": 0.0,
    "INSUFFICIENT_SAMPLE": 0.0,
}

# Type C rules: additional cap regardless of verdict
_TYPE_C_MAX_INFLUENCE = 0.15


# ── evidence loading ─────────────────────────────────────────────────

def load_evidence() -> list[dict[str, Any]]:
    """Load the latest evidence for each rule from the research lab.

    Reads evidence_table.csv and returns the MOST RECENT verdict
    per rule_id (in case of re-runs).
    """
    if not os.path.exists(_EVIDENCE_CSV):
        return []

    rows = []
    try:
        # Try UTF-8 first, fall back to latin-1 for Windows-edited files
        for encoding in ("utf-8", "latin-1", "cp1252"):
            try:
                with open(_EVIDENCE_CSV, "r", encoding=encoding) as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                break
            except (UnicodeDecodeError, UnicodeError):
                continue
    except Exception:
        return []

    # Keep only the latest verdict per rule_id
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        rid = row.get("rule_id", "")
        if rid:
            latest[rid] = row  # later rows overwrite earlier

    return list(latest.values())


def load_rule_state() -> dict[str, dict[str, Any]]:
    """Load the live rule state (kill switches, manual overrides).

    Returns dict keyed by rule_id.
    """
    if not os.path.exists(_RULE_STATE_FILE):
        return {}
    try:
        with open(_RULE_STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_rule_state(state: dict[str, dict[str, Any]]) -> None:
    """Persist rule state to disk."""
    os.makedirs(os.path.dirname(_RULE_STATE_FILE), exist_ok=True)
    with open(_RULE_STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ── active rules computation ────────────────────────────────────────

def get_active_rules() -> list[dict[str, Any]]:
    """Get all rules that are eligible to influence trade ranking.

    Returns list of rule dicts with computed influence_weight.
    Rules that are kill-switched, HURTS, CONDITIONAL, etc. are excluded.
    """
    evidence = load_evidence()
    state = load_rule_state()

    active = []
    for ev in evidence:
        rule_id = ev.get("rule_id", "")
        verdict = ev.get("verdict", "")
        rule_type = ev.get("rule_type", "A")
        chapter = ev.get("chapter", "")
        confidence = float(ev.get("confidence", 0) or 0)
        sharpe_delta = float(ev.get("sharpe_delta", 0) or 0)

        # Check kill switch
        rule_st = state.get(rule_id, {})
        if rule_st.get("killed", False):
            continue

        # Check manual override
        if rule_st.get("force_exclude", False):
            continue

        # Compute base influence from verdict
        base_influence = _INFLUENCE_CAPS.get(verdict, 0.0)
        if base_influence <= 0:
            continue

        # Scale by confidence
        influence = base_influence * confidence

        # Type C cap
        if rule_type == "C":
            influence = min(influence, _TYPE_C_MAX_INFLUENCE)

        # Manual influence override (operator can tune)
        manual_weight = rule_st.get("manual_influence")
        if manual_weight is not None:
            influence = float(manual_weight)

        active.append({
            "rule_id": rule_id,
            "chapter": chapter,
            "rule_type": rule_type,
            "verdict": verdict,
            "confidence": confidence,
            "sharpe_delta": sharpe_delta,
            "influence_weight": round(influence, 4),
            "source": "evidence_table",
            "killed": False,
        })

    return active


# ── audit logging ────────────────────────────────────────────────────

_AUDIT_HEADERS = [
    "timestamp", "action", "rule_id", "ticker", "influence_applied",
    "original_score", "adjusted_score", "notes",
]


def log_translation(
    action: str,
    rule_id: str = "",
    ticker: str = "",
    influence: float = 0,
    original: float = 0,
    adjusted: float = 0,
    notes: str = "",
) -> None:
    """Log a translation action for governance traceability."""
    os.makedirs(os.path.dirname(_AUDIT_LOG), exist_ok=True)
    exists = os.path.exists(_AUDIT_LOG)
    with open(_AUDIT_LOG, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_AUDIT_HEADERS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow({
            "timestamp": datetime.now().isoformat(),
            "action": action,
            "rule_id": rule_id,
            "ticker": ticker,
            "influence_applied": round(influence, 6),
            "original_score": round(original, 6),
            "adjusted_score": round(adjusted, 6),
            "notes": notes,
        })
