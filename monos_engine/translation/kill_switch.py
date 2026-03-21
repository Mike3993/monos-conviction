"""
MONOS Translation Layer — Kill Switch
---------------------------------------
Immediate rule disable mechanism.

When a rule is killed:
  - It is excluded from all future trade ranking adjustments
  - The kill is logged with timestamp and reason
  - The kill persists across server restarts (written to rule_state.json)
  - A killed rule can be revived by the operator

This is a safety mechanism. No rule should influence trades
without the ability to be instantly disabled.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from monos_engine.translation.rule_bridge import (
    load_rule_state,
    save_rule_state,
    log_translation,
)


def kill_rule(rule_id: str, reason: str = "") -> dict[str, Any]:
    """Immediately disable a rule from influencing trade ranking.

    Returns the updated rule state entry.
    """
    state = load_rule_state()
    state[rule_id] = state.get(rule_id, {})
    state[rule_id]["killed"] = True
    state[rule_id]["killed_at"] = datetime.now().isoformat()
    state[rule_id]["kill_reason"] = reason
    save_rule_state(state)

    log_translation(
        action="KILL_RULE",
        rule_id=rule_id,
        notes=f"Rule killed: {reason}",
    )

    return {"rule_id": rule_id, "killed": True, "reason": reason}


def revive_rule(rule_id: str, reason: str = "") -> dict[str, Any]:
    """Re-enable a previously killed rule.

    Returns the updated rule state entry.
    """
    state = load_rule_state()
    if rule_id in state:
        state[rule_id]["killed"] = False
        state[rule_id]["revived_at"] = datetime.now().isoformat()
        state[rule_id]["revive_reason"] = reason
    save_rule_state(state)

    log_translation(
        action="REVIVE_RULE",
        rule_id=rule_id,
        notes=f"Rule revived: {reason}",
    )

    return {"rule_id": rule_id, "killed": False, "reason": reason}


def set_manual_influence(rule_id: str, weight: float, reason: str = "") -> dict[str, Any]:
    """Override a rule's computed influence with a manual weight.

    Set weight=None to remove the override and revert to computed.
    """
    state = load_rule_state()
    state[rule_id] = state.get(rule_id, {})
    state[rule_id]["manual_influence"] = weight
    state[rule_id]["manual_set_at"] = datetime.now().isoformat()
    state[rule_id]["manual_reason"] = reason
    save_rule_state(state)

    log_translation(
        action="SET_INFLUENCE",
        rule_id=rule_id,
        influence=weight or 0,
        notes=f"Manual influence set to {weight}: {reason}",
    )

    return {"rule_id": rule_id, "manual_influence": weight, "reason": reason}


def force_exclude(rule_id: str, reason: str = "") -> dict[str, Any]:
    """Force-exclude a rule without killing it.

    Useful for temporarily removing a rule during investigation.
    """
    state = load_rule_state()
    state[rule_id] = state.get(rule_id, {})
    state[rule_id]["force_exclude"] = True
    state[rule_id]["excluded_at"] = datetime.now().isoformat()
    state[rule_id]["exclude_reason"] = reason
    save_rule_state(state)

    log_translation(
        action="FORCE_EXCLUDE",
        rule_id=rule_id,
        notes=f"Rule force-excluded: {reason}",
    )

    return {"rule_id": rule_id, "force_exclude": True, "reason": reason}


def remove_exclusion(rule_id: str) -> dict[str, Any]:
    """Remove a force-exclusion."""
    state = load_rule_state()
    if rule_id in state:
        state[rule_id]["force_exclude"] = False
    save_rule_state(state)

    log_translation(action="REMOVE_EXCLUSION", rule_id=rule_id)
    return {"rule_id": rule_id, "force_exclude": False}


def get_all_rule_states() -> dict[str, dict[str, Any]]:
    """Return the full rule state for inspection."""
    return load_rule_state()


def kill_all(reason: str = "Emergency kill-all") -> int:
    """Kill ALL active rules. Emergency stop.

    Returns the count of rules killed.
    """
    from monos_engine.translation.rule_bridge import get_active_rules
    active = get_active_rules()
    count = 0
    for r in active:
        kill_rule(r["rule_id"], reason=reason)
        count += 1

    log_translation(
        action="KILL_ALL",
        notes=f"Emergency: {count} rules killed. Reason: {reason}",
    )

    return count
