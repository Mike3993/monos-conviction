"""
governor_rules.py

Rule definitions for the portfolio governor.

Each rule is a callable that takes (structure, overlay, scenarios) and
returns (pass: bool, reason: str).
"""

import logging

logger = logging.getLogger(__name__)


def rule_construction(structure, overlay, scenarios) -> tuple[bool, str]:
    """
    Verify the structure has at least 2 legs and defined risk.
    """
    if len(structure.legs) < 2:
        return False, "Structure must have >= 2 legs"
    return True, "construction_ok"


def rule_conviction_floor(structure, overlay, scenarios) -> tuple[bool, str]:
    """
    Convexity score must meet minimum threshold (50).
    """
    if structure.convexity_score < 50:
        return False, f"convexity_score {structure.convexity_score} < 50"
    return True, "conviction_ok"


def rule_hedge_floor(structure, overlay, scenarios) -> tuple[bool, str]:
    """
    In high-complexity environments (complexity >= 40), structure must
    include protective legs (at least one SHORT leg to offset risk).
    """
    if overlay.complexity_index >= 40:
        has_short = any(l.direction == "SHORT" for l in structure.legs)
        if not has_short:
            return False, "high complexity requires hedge leg"
    return True, "hedge_ok"


def rule_correlation_exposure(structure, overlay, scenarios) -> tuple[bool, str]:
    """
    Placeholder: check correlation concentration.
    Phase 2 will implement cross-asset correlation checks.
    """
    return True, "correlation_ok"


def rule_gex_phase(structure, overlay, scenarios) -> tuple[bool, str]:
    """
    Block aggressive long-only structures in NEGATIVE gamma regime
    unless convexity score is very high (>= 75).
    """
    if overlay.gamma_regime == "NEGATIVE":
        all_long = all(l.direction == "LONG" for l in structure.legs)
        if all_long and structure.convexity_score < 75:
            return False, "all-long blocked in NEGATIVE gamma regime"
    return True, "gex_phase_ok"


# ── Rule registry ─────────────────────────────────────────────────

ALL_RULES = [
    ("construction",        rule_construction),
    ("conviction_floor",    rule_conviction_floor),
    ("hedge_floor",         rule_hedge_floor),
    ("correlation_exposure", rule_correlation_exposure),
    ("gex_phase",           rule_gex_phase),
]
