"""
MONOS Research Lab — Rule Validator
------------------------------------
Core verdict function for Canon rules.

Takes rule metadata + test statistics + evidence criteria
and produces a governance verdict:

  EARNS_PLACE        — Rule passes all gates, ready to crystallize
  CONDITIONAL        — Passes most gates, needs more evidence or Tyler
  REDUNDANT          — Adds no incremental value over existing rules
  HURTS              — Degrades performance vs baseline
  INSUFFICIENT_SAMPLE — Not enough data to make a determination

This is the judicial layer of the MONOS Canon.
No rule enters production without a verdict.
"""

from __future__ import annotations

from typing import Any


# ── verdict constants ────────────────────────────────────────────────
# Governance-aligned verdict taxonomy:
#   EARNS_PLACE                — All gates pass including Tyler. Canon-ready.
#   INTERNAL_PASS              — All empirical gates pass. Tyler not yet received.
#   PROVISIONAL                — Strong evidence, minor gates pending.
#   CONDITIONAL_PENDING_TYLER  — Passes empirical gates but Type C Tyler required.
#   CONDITIONAL                — Partial pass, needs more evidence.
#   REDUNDANT                  — Adds no incremental value.
#   HURTS                      — Degrades performance.
#   INSUFFICIENT_SAMPLE        — Not enough data.

EARNS_PLACE = "EARNS_PLACE"
INTERNAL_PASS = "INTERNAL_PASS"
PROVISIONAL = "PROVISIONAL"
CONDITIONAL_PENDING_TYLER = "CONDITIONAL_PENDING_TYLER"
CONDITIONAL = "CONDITIONAL"
REDUNDANT = "REDUNDANT"
HURTS = "HURTS"
INSUFFICIENT_SAMPLE = "INSUFFICIENT_SAMPLE"


# ── evidence criteria thresholds ─────────────────────────────────────

MIN_OCCURRENCES = 30           # minimum trades for statistical validity
MIN_SHARPE_DELTA = 0.05        # candidate must improve Sharpe by at least this
MIN_WIN_RATE_DELTA = -0.02     # win rate can drop slightly if Sharpe improves
MAX_DRAWDOWN_WORSENING = -0.02 # drawdown can worsen by at most 2pp
CROSS_INSTRUMENT_MIN = 3       # must work on at least 3 instruments
DEFLATED_SHARPE_THRESHOLD = 0.0  # Sharpe after multiple-testing adjustment must be > 0


# ── main verdict function ────────────────────────────────────────────

def validate_rule(
    rule_meta: dict[str, Any],
    test_stats: dict[str, Any],
    scorecard: dict[str, Any],
) -> dict[str, Any]:
    """Produce a governance verdict for a candidate rule.

    Parameters
    ----------
    rule_meta : dict
        Rule registry entry: rule_id, chapter, rule_type, tyler_required, etc.
    test_stats : dict
        Statistical evidence from experiment:
        - n_occurrences: int
        - in_sample_pass: bool
        - out_of_sample_pass: bool
        - cross_instrument_count: int
        - cross_instrument_pass: bool
        - deflated_sharpe: float
        - deflated_sharpe_pass: bool
        - sharpe_delta: float
    scorecard : dict
        Output from scorer.compute_scorecard():
        - pass_count, overall_pass, metrics

    Returns
    -------
    dict
        verdict: str (EARNS_PLACE / CONDITIONAL / REDUNDANT / HURTS / INSUFFICIENT_SAMPLE)
        confidence: float (0-1)
        gates: dict of gate_name -> {passed, value, threshold, note}
        explanation: str
        rule_id: str
        chapter: str
    """
    rule_id = rule_meta.get("rule_id", "UNKNOWN")
    chapter = rule_meta.get("chapter", "")
    rule_type = rule_meta.get("rule_type", "A")
    tyler_required = rule_meta.get("tyler_required", False)

    n = test_stats.get("n_occurrences", 0)
    is_pass = test_stats.get("in_sample_pass", False)
    oos_pass = test_stats.get("out_of_sample_pass", False)
    xi_count = test_stats.get("cross_instrument_count", 0)
    xi_pass = test_stats.get("cross_instrument_pass", False)
    defl_sharpe = test_stats.get("deflated_sharpe", 0)
    defl_pass = test_stats.get("deflated_sharpe_pass", False)
    sharpe_delta = test_stats.get("sharpe_delta", 0)

    sc_pass = scorecard.get("overall_pass", False)
    sc_count = scorecard.get("pass_count", 0)

    # ── Gate evaluation ──────────────────────────────────────────
    gates = {}

    # Gate 1: Sample size
    gates["sample_size"] = {
        "passed": n >= MIN_OCCURRENCES,
        "value": n,
        "threshold": MIN_OCCURRENCES,
        "note": f"{n} occurrences {'meets' if n >= MIN_OCCURRENCES else 'below'} minimum {MIN_OCCURRENCES}",
    }

    # Gate 2: In-sample performance
    gates["in_sample"] = {
        "passed": is_pass,
        "value": is_pass,
        "threshold": True,
        "note": "In-sample test " + ("passed" if is_pass else "failed"),
    }

    # Gate 3: Out-of-sample holdout
    gates["out_of_sample"] = {
        "passed": oos_pass,
        "value": oos_pass,
        "threshold": True,
        "note": "Out-of-sample holdout " + ("passed" if oos_pass else "failed"),
    }

    # Gate 4: Cross-instrument generalization
    gates["cross_instrument"] = {
        "passed": xi_pass and xi_count >= CROSS_INSTRUMENT_MIN,
        "value": xi_count,
        "threshold": CROSS_INSTRUMENT_MIN,
        "note": f"Works on {xi_count}/{CROSS_INSTRUMENT_MIN} instruments",
    }

    # Gate 5: Deflated Sharpe (multiple-testing adjusted)
    gates["deflated_sharpe"] = {
        "passed": defl_pass and defl_sharpe > DEFLATED_SHARPE_THRESHOLD,
        "value": defl_sharpe,
        "threshold": DEFLATED_SHARPE_THRESHOLD,
        "note": f"Deflated Sharpe {defl_sharpe:.4f} {'>' if defl_sharpe > 0 else '<='} 0",
    }

    # Gate 6: Scorecard (5-metric)
    gates["scorecard"] = {
        "passed": sc_pass,
        "value": sc_count,
        "threshold": 4,
        "note": f"Scorecard {sc_count}/5 — {'PASS' if sc_pass else 'FAIL'}",
    }

    # Gate 7: Tyler validation (if required)
    tyler_resolved = True
    if tyler_required and rule_type == "C":
        tyler_data = test_stats.get("tyler_validated", False)
        tyler_resolved = tyler_data
        gates["tyler_validation"] = {
            "passed": tyler_data,
            "value": tyler_data,
            "threshold": True,
            "note": "Tyler validation " + ("received" if tyler_data else "PENDING — cannot crystallize without Tyler"),
        }

    # ── Verdict determination ────────────────────────────────────

    passed_gates = sum(1 for g in gates.values() if g["passed"])
    total_gates = len(gates)

    # All empirical gates (excluding Tyler)
    empirical_gates = {k: v for k, v in gates.items() if k != "tyler_validation"}
    empirical_passed = sum(1 for g in empirical_gates.values() if g["passed"])
    empirical_total = len(empirical_gates)
    all_empirical_pass = empirical_passed == empirical_total

    # Insufficient sample — can't decide anything
    if not gates["sample_size"]["passed"]:
        verdict = INSUFFICIENT_SAMPLE
        confidence = 0.0
        explanation = f"Rule {rule_id}: only {n} occurrences — need {MIN_OCCURRENCES} minimum to evaluate."

    # Hurts — scorecard fails AND Sharpe delta negative
    elif not sc_pass and sharpe_delta < -MIN_SHARPE_DELTA:
        verdict = HURTS
        confidence = min(0.9, abs(sharpe_delta) / 0.5)
        explanation = f"Rule {rule_id} degrades performance: Sharpe delta {sharpe_delta:+.4f}, scorecard {sc_count}/5."

    # Redundant — passes sample but no incremental improvement
    elif abs(sharpe_delta) < MIN_SHARPE_DELTA and sc_count <= 2:
        verdict = REDUNDANT
        confidence = 0.5
        explanation = f"Rule {rule_id} adds no incremental edge: Sharpe delta {sharpe_delta:+.4f}, only {sc_count}/5 metrics improved."

    # EARNS_PLACE — all gates pass INCLUDING Tyler
    elif all_empirical_pass and tyler_resolved:
        verdict = EARNS_PLACE
        confidence = min(1.0, passed_gates / total_gates)
        explanation = f"Rule {rule_id} passes ALL gates ({passed_gates}/{total_gates}) including Tyler. Sharpe delta {sharpe_delta:+.4f}. CANON READY."

    # INTERNAL_PASS — all empirical gates pass, Type C awaiting Tyler
    elif all_empirical_pass and tyler_required and rule_type == "C" and not tyler_resolved:
        verdict = INTERNAL_PASS
        confidence = empirical_passed / total_gates
        explanation = f"Rule {rule_id} passes all empirical gates ({empirical_passed}/{empirical_total}). Sharpe delta {sharpe_delta:+.4f}. INTERNAL WINNER — awaiting Tyler validation for Canon status."

    # CONDITIONAL_PENDING_TYLER — strong but not all empirical, Type C
    elif empirical_passed >= empirical_total - 1 and tyler_required and not tyler_resolved:
        verdict = CONDITIONAL_PENDING_TYLER
        failed_emp = [name for name, g in empirical_gates.items() if not g["passed"]]
        confidence = empirical_passed / total_gates
        explanation = f"Rule {rule_id} near-passes empirically ({empirical_passed}/{empirical_total}, failed: {', '.join(failed_emp)}). Tyler validation required. Cannot promote to Canon without Tyler."

    # PROVISIONAL — most gates pass, not Type C or Tyler not needed
    elif empirical_passed >= empirical_total - 1 and not tyler_required:
        verdict = PROVISIONAL
        failed = [name for name, g in gates.items() if not g["passed"]]
        confidence = passed_gates / total_gates
        explanation = f"Rule {rule_id} provisionally validated ({passed_gates}/{total_gates}). Minor gaps: {', '.join(failed)}. No Tyler required — can promote with operator review."

    # CONDITIONAL — partial pass
    else:
        verdict = CONDITIONAL
        failed = [name for name, g in gates.items() if not g["passed"]]
        confidence = passed_gates / total_gates
        explanation = f"Rule {rule_id} partially validated ({passed_gates}/{total_gates}). Failed: {', '.join(failed)}. Needs more evidence."

    return {
        "verdict": verdict,
        "confidence": round(confidence, 4),
        "gates": gates,
        "explanation": explanation,
        "rule_id": rule_id,
        "chapter": chapter,
        "rule_type": rule_type,
        "tyler_required": tyler_required,
        "sharpe_delta": sharpe_delta,
        "scorecard_pass_count": sc_count,
    }
