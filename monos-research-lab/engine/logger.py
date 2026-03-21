"""
MONOS Research Lab — Experiment Logger
---------------------------------------
Writes experiment and test outputs to CSV files for audit trail.

Two output files:
  reports/experiment_log.csv  — one row per experiment run
  reports/evidence_table.csv  — one row per rule verdict
"""

from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import Any

_REPORTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports")

EXPERIMENT_LOG = os.path.join(_REPORTS_DIR, "experiment_log.csv")
EVIDENCE_TABLE = os.path.join(_REPORTS_DIR, "evidence_table.csv")

# ── CSV headers ──────────────────────────────────────────────────────

EXPERIMENT_HEADERS = [
    "timestamp",
    "experiment_name",
    "chapter",
    "universe",
    "date_range_start",
    "date_range_end",
    "baseline_name",
    "candidate_name",
    "n_baseline_trades",
    "n_candidate_trades",
    "sharpe_delta",
    "win_rate_delta",
    "max_dd_delta",
    "theta_eff_delta",
    "convexity_retained",
    "scorecard_pass_count",
    "overall_pass",
    "notes",
]

EVIDENCE_HEADERS = [
    "timestamp",
    "rule_id",
    "chapter",
    "rule_type",
    "verdict",
    "confidence",
    "n_occurrences",
    "in_sample_pass",
    "out_of_sample_pass",
    "cross_instrument_pass",
    "cross_instrument_count",
    "deflated_sharpe",
    "deflated_sharpe_pass",
    "sharpe_delta",
    "scorecard_pass_count",
    "tyler_required",
    "tyler_validated",
    "explanation",
]


# ── ensure files exist with headers ──────────────────────────────────

def _ensure_csv(path: str, headers: list[str]) -> None:
    """Create CSV with headers if it doesn't exist."""
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", newline="") as f:
            csv.writer(f).writerow(headers)


def _append_row(path: str, headers: list[str], row: dict[str, Any]) -> None:
    """Append a single row to a CSV file."""
    _ensure_csv(path, headers)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writerow(row)


# ── public logging functions ─────────────────────────────────────────

def log_experiment(
    experiment_name: str,
    chapter: str,
    universe: list[str],
    date_range: dict[str, str],
    baseline_name: str,
    candidate_name: str,
    n_baseline: int,
    n_candidate: int,
    scorecard: dict[str, Any],
    notes: str = "",
) -> None:
    """Log a completed experiment run to experiment_log.csv."""
    metrics = {m["name"]: m for m in scorecard.get("metrics", [])}

    row = {
        "timestamp": datetime.now().isoformat(),
        "experiment_name": experiment_name,
        "chapter": chapter,
        "universe": "|".join(universe) if isinstance(universe, list) else str(universe),
        "date_range_start": date_range.get("start", date_range.get("in_sample", {}).get("start", "")),
        "date_range_end": date_range.get("end", date_range.get("out_of_sample", {}).get("end", "")),
        "baseline_name": baseline_name,
        "candidate_name": candidate_name,
        "n_baseline_trades": n_baseline,
        "n_candidate_trades": n_candidate,
        "sharpe_delta": metrics.get("sharpe_ratio_delta", {}).get("delta", ""),
        "win_rate_delta": metrics.get("win_rate_delta", {}).get("delta", ""),
        "max_dd_delta": metrics.get("max_drawdown_delta", {}).get("delta", ""),
        "theta_eff_delta": metrics.get("theta_efficiency_delta", {}).get("delta", ""),
        "convexity_retained": metrics.get("convexity_retention", {}).get("candidate", ""),
        "scorecard_pass_count": scorecard.get("pass_count", ""),
        "overall_pass": scorecard.get("overall_pass", ""),
        "notes": notes,
    }
    _append_row(EXPERIMENT_LOG, EXPERIMENT_HEADERS, row)


def log_evidence(
    verdict_result: dict[str, Any],
    test_stats: dict[str, Any],
) -> None:
    """Log a rule verdict to evidence_table.csv."""
    row = {
        "timestamp": datetime.now().isoformat(),
        "rule_id": verdict_result.get("rule_id", ""),
        "chapter": verdict_result.get("chapter", ""),
        "rule_type": verdict_result.get("rule_type", ""),
        "verdict": verdict_result.get("verdict", ""),
        "confidence": verdict_result.get("confidence", ""),
        "n_occurrences": test_stats.get("n_occurrences", ""),
        "in_sample_pass": test_stats.get("in_sample_pass", ""),
        "out_of_sample_pass": test_stats.get("out_of_sample_pass", ""),
        "cross_instrument_pass": test_stats.get("cross_instrument_pass", ""),
        "cross_instrument_count": test_stats.get("cross_instrument_count", ""),
        "deflated_sharpe": test_stats.get("deflated_sharpe", ""),
        "deflated_sharpe_pass": test_stats.get("deflated_sharpe_pass", ""),
        "sharpe_delta": test_stats.get("sharpe_delta", ""),
        "scorecard_pass_count": verdict_result.get("scorecard_pass_count", ""),
        "tyler_required": verdict_result.get("tyler_required", ""),
        "tyler_validated": test_stats.get("tyler_validated", ""),
        "explanation": verdict_result.get("explanation", ""),
    }
    _append_row(EVIDENCE_TABLE, EVIDENCE_HEADERS, row)
