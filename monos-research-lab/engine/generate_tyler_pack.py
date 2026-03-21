"""
MONOS Research Lab — Tyler Pack Generator
-------------------------------------------
Creates exportable structured JSON for Tyler-required tests.

A Tyler Pack contains:
  - The rules that need live validation
  - The specific tests to run on Tyler's infrastructure
  - The expected output format
  - The evidence criteria for pass/fail

Tyler packs are the bridge between the research lab (backtest proxy)
and live infrastructure (Tyler's execution environment).

Type C rules CANNOT be crystallized without Tyler validation.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any


_REPORTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports")


def generate_tyler_pack(
    chapter_id: str,
    rules: list[dict[str, Any]],
    experiment_spec: dict[str, Any] | None = None,
    universe: list[str] | None = None,
    date_range: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Generate a Tyler Pack JSON for a chapter's Type C rules.

    Parameters
    ----------
    chapter_id : str
        e.g. "CH01"
    rules : list[dict]
        Rule registry entries that are tyler_required=true.
    experiment_spec : dict | None
        Associated experiment specification.
    universe : list[str] | None
        Tickers to test. Defaults to core universe.
    date_range : dict | None
        Validation period.

    Returns
    -------
    dict
        The Tyler Pack — structured for export to Tyler's environment.
    """
    if universe is None:
        universe = ["SPY", "QQQ", "IWM", "SMH", "GLD", "SLV"]
    if date_range is None:
        date_range = {"start": "2025-07-01", "end": "2026-03-01"}

    # Filter to Tyler-required rules only
    tyler_rules = [r for r in rules if r.get("tyler_required", False)]

    tests = []
    for rule in tyler_rules:
        tests.append({
            "rule_id": rule.get("rule_id", ""),
            "rule_name": rule.get("rule_name", ""),
            "rule_type": rule.get("rule_type", "C"),
            "hypothesis": rule.get("hypothesis", ""),
            "test_description": f"Compare live {rule.get('rule_name', '')} output against backtest proxy for {', '.join(universe)} over validation period.",
            "expected_outputs": {
                "per_ticker": {
                    "signal_agreement_pct": "Percentage of days where live and proxy produce the same signal",
                    "signal_count": "Number of signals generated",
                    "return_correlation": "Correlation between live-signal returns and proxy-signal returns",
                },
                "aggregate": {
                    "overall_agreement": "Mean agreement across all tickers",
                    "edge_preserved": "Whether live signals preserve the backtest edge (Sharpe > 0)",
                },
            },
            "pass_criteria": {
                "signal_agreement_pct": ">= 80%",
                "return_correlation": ">= 0.70",
                "edge_preserved": True,
            },
        })

    pack = {
        "pack_id": f"TYLER-{chapter_id}-{datetime.now().strftime('%Y%m%d%H%M')}",
        "chapter": chapter_id,
        "generated": datetime.now().isoformat(),
        "generator": "monos-research-lab",
        "universe": universe,
        "date_range": date_range,
        "tests": tests,
        "notes": "Type C rules require live infrastructure validation. Backtest proxy uses yfinance historical data. Tyler must run the same rules on live/intraday data and return results in the expected_outputs format.",
        "return_schema": {
            "pack_id": "string — must match this pack_id",
            "results": [
                {
                    "rule_id": "string",
                    "per_ticker": {
                        "TICKER": {
                            "signal_agreement_pct": "float",
                            "signal_count": "int",
                            "return_correlation": "float",
                        }
                    },
                    "aggregate": {
                        "overall_agreement": "float",
                        "edge_preserved": "bool",
                    },
                    "pass": "bool",
                    "notes": "string",
                }
            ],
            "completed": "ISO datetime string",
        },
    }

    return pack


def export_tyler_pack(pack: dict[str, Any], output_dir: str | None = None) -> str:
    """Write a Tyler Pack to a JSON file and return the path."""
    if output_dir is None:
        output_dir = _REPORTS_DIR
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{pack['pack_id']}.json"
    path = os.path.join(output_dir, filename)
    with open(path, "w") as f:
        json.dump(pack, f, indent=2)
    return path
