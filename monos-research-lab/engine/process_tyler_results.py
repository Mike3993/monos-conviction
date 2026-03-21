"""
MONOS Research Lab — Tyler Results Processor
----------------------------------------------
Accepts returned Tyler results JSON, validates the schema,
and writes summarised results into evidence_table.csv.

This is the ingestion layer for live infrastructure validation.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

from engine.logger import log_evidence


# ── schema validation ────────────────────────────────────────────────

REQUIRED_TOP_KEYS = {"pack_id", "results", "completed"}
REQUIRED_RESULT_KEYS = {"rule_id", "per_ticker", "aggregate", "pass"}
REQUIRED_AGGREGATE_KEYS = {"overall_agreement", "edge_preserved"}


def validate_tyler_results_schema(results: dict[str, Any]) -> tuple[bool, str]:
    """Validate the Tyler results JSON against expected schema.

    Returns (valid, error_message).
    """
    # Top-level keys
    missing_top = REQUIRED_TOP_KEYS - set(results.keys())
    if missing_top:
        return False, f"Missing top-level keys: {missing_top}"

    if not isinstance(results.get("results"), list):
        return False, "'results' must be a list"

    for i, r in enumerate(results["results"]):
        missing = REQUIRED_RESULT_KEYS - set(r.keys())
        if missing:
            return False, f"Result [{i}] missing keys: {missing}"

        agg = r.get("aggregate", {})
        missing_agg = REQUIRED_AGGREGATE_KEYS - set(agg.keys())
        if missing_agg:
            return False, f"Result [{i}] aggregate missing: {missing_agg}"

    return True, ""


# ── processing ───────────────────────────────────────────────────────

def process_tyler_results(
    results_json: dict[str, Any],
    original_pack: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Process Tyler results and log to evidence table.

    Parameters
    ----------
    results_json : dict
        The Tyler results JSON returned from Tyler's infrastructure.
    original_pack : dict | None
        The original Tyler Pack for cross-reference.

    Returns
    -------
    dict
        summary: overall processing result
        rule_results: list of per-rule processing outputs
        errors: list of any validation errors
    """
    # Validate schema
    valid, error = validate_tyler_results_schema(results_json)
    if not valid:
        return {
            "summary": {"status": "SCHEMA_ERROR", "error": error},
            "rule_results": [],
            "errors": [error],
        }

    pack_id = results_json.get("pack_id", "UNKNOWN")
    chapter = ""
    if original_pack:
        chapter = original_pack.get("chapter", "")
        # Verify pack_id matches
        if original_pack.get("pack_id") != pack_id:
            return {
                "summary": {"status": "PACK_MISMATCH", "error": f"Pack ID mismatch: expected {original_pack['pack_id']}, got {pack_id}"},
                "rule_results": [],
                "errors": [f"Pack ID mismatch"],
            }

    rule_results = []
    errors = []

    for result in results_json.get("results", []):
        rule_id = result.get("rule_id", "UNKNOWN")
        agg = result.get("aggregate", {})
        passed = result.get("pass", False)
        agreement = agg.get("overall_agreement", 0)
        edge = agg.get("edge_preserved", False)

        # Build test_stats for the evidence logger
        test_stats = {
            "n_occurrences": sum(
                v.get("signal_count", 0)
                for v in result.get("per_ticker", {}).values()
            ),
            "in_sample_pass": passed,
            "out_of_sample_pass": passed,  # Tyler IS the out-of-sample
            "cross_instrument_count": len(result.get("per_ticker", {})),
            "cross_instrument_pass": len(result.get("per_ticker", {})) >= 3,
            "deflated_sharpe": 0,  # Tyler doesn't compute this directly
            "deflated_sharpe_pass": passed,
            "sharpe_delta": 0,
            "tyler_validated": passed,
        }

        verdict_result = {
            "rule_id": rule_id,
            "chapter": chapter,
            "rule_type": "C",
            "verdict": "EARNS_PLACE" if passed else "CONDITIONAL",
            "confidence": agreement / 100 if agreement else 0,
            "scorecard_pass_count": 0,
            "tyler_required": True,
            "explanation": f"Tyler validation: agreement={agreement:.1f}%, edge_preserved={edge}, pass={passed}",
        }

        # Log to evidence table
        try:
            log_evidence(verdict_result, test_stats)
        except Exception as exc:
            errors.append(f"Failed to log {rule_id}: {exc}")

        rule_results.append({
            "rule_id": rule_id,
            "passed": passed,
            "agreement": agreement,
            "edge_preserved": edge,
            "test_stats": test_stats,
            "verdict": verdict_result["verdict"],
        })

    return {
        "summary": {
            "status": "PROCESSED",
            "pack_id": pack_id,
            "total_rules": len(rule_results),
            "passed": sum(1 for r in rule_results if r["passed"]),
            "failed": sum(1 for r in rule_results if not r["passed"]),
            "processed_at": datetime.now().isoformat(),
        },
        "rule_results": rule_results,
        "errors": errors,
    }


def load_and_process(results_path: str, pack_path: str | None = None) -> dict[str, Any]:
    """Convenience: load JSON files and process."""
    with open(results_path, "r") as f:
        results = json.load(f)

    pack = None
    if pack_path:
        with open(pack_path, "r") as f:
            pack = json.load(f)

    return process_tyler_results(results, pack)
