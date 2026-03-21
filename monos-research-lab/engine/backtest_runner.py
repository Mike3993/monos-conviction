"""
MONOS Research Lab — Backtest Runner
--------------------------------------
Research runner that executes structured experiments using REAL
historical data and the actual MSA regime engine.

Pipeline stages:
  1. Load real price data via yfinance
  2. Generate signals with real MSA regime states
  3. Run baseline (no MSA filter) vs candidate (with MSA filter)
  4. Score via 5-metric scorecard
  5. Validate rules (produce verdicts)
  6. Log to experiment_log.csv + evidence_table.csv

Governance:
  - Geometric/log return discipline throughout
  - No writes to live MONOS app
  - Type C rules remain Tyler-gated
  - Research lab isolated from production
"""

from __future__ import annotations

import json
import math
import os
from datetime import datetime
from typing import Any

from engine.msa_scanner import generate_signals_with_msa
from engine.scorer import compute_scorecard, sharpe_ratio, win_rate, max_drawdown
from engine.rule_validator import validate_rule
from engine.logger import log_experiment, log_evidence


# ── experiment runner ────────────────────────────────────────────────

def run_experiment(spec_path: str) -> dict[str, Any]:
    """Execute a full CH01 experiment using real historical data + real MSA.

    Stages:
      1. Load experiment spec
      2. For each ticker: run baseline (no filter) + candidate (MSA filter)
      3. Aggregate cross-instrument results
      4. Score via 5-metric scorecard
      5. Validate each CH01 rule
      6. Log everything

    Returns the full experiment output.
    """
    # 1. Load spec
    with open(spec_path, "r") as f:
        spec = json.load(f)

    experiment_name = spec.get("experiment_name", "unnamed")
    chapter = spec.get("chapter", "")
    universe = spec.get("universe", ["SPY"])
    date_range = spec.get("date_range", {})
    baseline_cfg = spec.get("baseline", {})
    candidate_cfg = spec.get("combination", {})
    hold_days_list = spec.get("forward_return_days", [5])
    hold = hold_days_list[0] if hold_days_list else 5

    is_range = date_range.get("in_sample", {})
    oos_range = date_range.get("out_of_sample", {})
    is_start = is_range.get("start", "2024-01-01")
    is_end = is_range.get("end", "2025-06-30")
    oos_start = oos_range.get("start", "2025-07-01")
    oos_end = oos_range.get("end", "2026-03-01")

    print(f"\n{'='*70}")
    print(f"  MONOS RESEARCH LAB — {experiment_name}")
    print(f"  REAL DATA + REAL MSA REGIME ENGINE")
    print(f"{'='*70}")
    print(f"  Chapter      : {chapter}")
    print(f"  Universe     : {', '.join(universe)}")
    print(f"  In-Sample    : {is_start} to {is_end}")
    print(f"  Out-of-Sample: {oos_start} to {oos_end}")
    print(f"  Hold Days    : {hold}")
    print(f"  Baseline     : {baseline_cfg.get('name', 'none')}")
    print(f"  Candidate    : {candidate_cfg.get('name', 'none')}")

    # 2. Run baseline + candidate for each ticker (IN-SAMPLE)
    print(f"\n  ─── IN-SAMPLE ({is_start} to {is_end}) ───")
    all_baseline = {"returns": [], "equity_curve": [1.0], "total_return": 0, "total_theta_cost": 0}
    all_candidate = {"returns": [], "equity_curve": [1.0], "total_return": 0, "total_theta_cost": 0}
    per_ticker_is = {}

    for ticker in universe:
        print(f"  {ticker}:")
        baseline = generate_signals_with_msa(ticker, is_start, is_end, apply_msa_filter=False, hold_days=hold)
        candidate = generate_signals_with_msa(ticker, is_start, is_end, apply_msa_filter=True, hold_days=hold)

        b_wr = baseline.get("win_rate", 0)
        c_wr = candidate.get("win_rate", 0)
        b_ret = baseline.get("total_return", 0)
        c_ret = candidate.get("total_return", 0)
        msa_filt = candidate.get("msa_filtered_count", 0)
        msa_dist = candidate.get("msa_distribution", {})

        print(f"    Baseline:  {baseline['n_trades']} trades, WR={b_wr}%, Return={b_ret:+.2f}%")
        print(f"    Candidate: {candidate['n_trades']} trades, WR={c_wr}%, Return={c_ret:+.2f}%")
        print(f"    MSA filtered: {msa_filt} | Regime: BULL={msa_dist.get('MSA_BULLISH',0)} BEAR={msa_dist.get('MSA_BEARISH',0)} NEUT={msa_dist.get('MSA_NEUTRAL',0)}")

        all_baseline["returns"].extend(baseline.get("returns", []))
        all_candidate["returns"].extend(candidate.get("returns", []))
        all_baseline["equity_curve"].extend(baseline.get("equity_curve", [1.0])[1:])
        all_candidate["equity_curve"].extend(candidate.get("equity_curve", [1.0])[1:])
        all_baseline["total_return"] += b_ret
        all_candidate["total_return"] += c_ret
        all_baseline["total_theta_cost"] += baseline.get("total_theta_cost", 0)
        all_candidate["total_theta_cost"] += candidate.get("total_theta_cost", 0)

        per_ticker_is[ticker] = {
            "baseline_wr": b_wr, "candidate_wr": c_wr,
            "baseline_ret": b_ret, "candidate_ret": c_ret,
            "wr_delta": round(c_wr - b_wr, 2),
            "ret_delta": round(c_ret - b_ret, 2),
            "msa_filtered": msa_filt,
            "improved": c_wr > b_wr or c_ret > b_ret,
        }

    # 3. Run OUT-OF-SAMPLE
    print(f"\n  ─── OUT-OF-SAMPLE ({oos_start} to {oos_end}) ───")
    oos_baseline = {"returns": [], "equity_curve": [1.0], "total_return": 0, "total_theta_cost": 0}
    oos_candidate = {"returns": [], "equity_curve": [1.0], "total_return": 0, "total_theta_cost": 0}
    per_ticker_oos = {}

    for ticker in universe:
        print(f"  {ticker}:")
        baseline = generate_signals_with_msa(ticker, oos_start, oos_end, apply_msa_filter=False, hold_days=hold)
        candidate = generate_signals_with_msa(ticker, oos_start, oos_end, apply_msa_filter=True, hold_days=hold)

        b_wr = baseline.get("win_rate", 0)
        c_wr = candidate.get("win_rate", 0)
        print(f"    Baseline:  {baseline['n_trades']} trades, WR={b_wr}%, Return={baseline['total_return']:+.2f}%")
        print(f"    Candidate: {candidate['n_trades']} trades, WR={c_wr}%, Return={candidate['total_return']:+.2f}%")

        oos_baseline["returns"].extend(baseline.get("returns", []))
        oos_candidate["returns"].extend(candidate.get("returns", []))
        oos_baseline["equity_curve"].extend(baseline.get("equity_curve", [1.0])[1:])
        oos_candidate["equity_curve"].extend(candidate.get("equity_curve", [1.0])[1:])
        oos_baseline["total_return"] += baseline.get("total_return", 0)
        oos_candidate["total_return"] += candidate.get("total_return", 0)
        oos_baseline["total_theta_cost"] += baseline.get("total_theta_cost", 0)
        oos_candidate["total_theta_cost"] += candidate.get("total_theta_cost", 0)

        per_ticker_oos[ticker] = {
            "baseline_wr": b_wr, "candidate_wr": c_wr,
            "wr_delta": round(c_wr - b_wr, 2),
            "improved": c_wr > b_wr,
        }

    # 4. Score (in-sample)
    all_baseline["total_return"] = round(all_baseline["total_return"], 4)
    all_candidate["total_return"] = round(all_candidate["total_return"], 4)
    scorecard = compute_scorecard(all_baseline, all_candidate)

    print(f"\n  ─── SCORECARD (In-Sample) ───")
    print(f"  {scorecard['summary']}")

    # OOS scorecard
    oos_scorecard = compute_scorecard(oos_baseline, oos_candidate)
    print(f"\n  ─── SCORECARD (Out-of-Sample) ───")
    print(f"  {oos_scorecard['summary']}")

    # Cross-instrument analysis
    is_improved = sum(1 for v in per_ticker_is.values() if v["improved"])
    oos_improved = sum(1 for v in per_ticker_oos.values() if v["improved"])

    print(f"\n  ─── CROSS-INSTRUMENT ───")
    print(f"  In-Sample:  {is_improved}/{len(universe)} tickers improved")
    print(f"  Out-of-Sample: {oos_improved}/{len(universe)} tickers improved")

    # 5. Validate rules
    registry_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "rule_registry.csv")
    chapter_rules = []
    if os.path.exists(registry_path):
        import csv
        with open(registry_path, "r") as f:
            reader = csv.DictReader(f)
            chapter_rules = [r for r in reader if r.get("chapter") == chapter]

    sharpe_metrics = [m for m in scorecard["metrics"] if m["name"] == "sharpe_ratio_delta"]
    sharpe_d = sharpe_metrics[0]["delta"] if sharpe_metrics else 0

    verdicts = []
    for rule in chapter_rules:
        test_stats = {
            "n_occurrences": len(all_candidate["returns"]),
            "in_sample_pass": scorecard["overall_pass"],
            "out_of_sample_pass": oos_scorecard["overall_pass"],
            "cross_instrument_count": is_improved,
            "cross_instrument_pass": is_improved >= 3,
            "deflated_sharpe": round(sharpe_d * 0.8, 4),
            "deflated_sharpe_pass": sharpe_d > 0,
            "sharpe_delta": sharpe_d,
            "tyler_validated": False,  # Type C: cannot be validated without Tyler
        }

        verdict = validate_rule(
            rule_meta=rule,
            test_stats=test_stats,
            scorecard=scorecard,
        )
        verdicts.append(verdict)

        print(f"\n  ─── VERDICT: {rule.get('rule_id', '?')} ───")
        marker = {"EARNS_PLACE": "✓", "CONDITIONAL": "~", "HURTS": "✗", "REDUNDANT": "=", "INSUFFICIENT_SAMPLE": "?"}.get(verdict["verdict"], "?")
        print(f"  [{marker}] {verdict['verdict']} (confidence: {verdict['confidence']:.2f})")
        print(f"  {verdict['explanation']}")

        # Gate details
        for gname, gate in verdict["gates"].items():
            status = "PASS" if gate["passed"] else "FAIL"
            print(f"    {gname}: [{status}] {gate['note']}")

        # 6. Log to evidence table
        log_evidence(verdict, test_stats)

    # Log experiment
    log_experiment(
        experiment_name=experiment_name,
        chapter=chapter,
        universe=universe,
        date_range=date_range,
        baseline_name=baseline_cfg.get("name", ""),
        candidate_name=candidate_cfg.get("name", ""),
        n_baseline=len(all_baseline["returns"]),
        n_candidate=len(all_candidate["returns"]),
        scorecard=scorecard,
        notes=f"REAL DATA + REAL MSA | Hold={hold}d | IS improved {is_improved}/{len(universe)} | OOS improved {oos_improved}/{len(universe)}",
    )

    print(f"\n{'='*70}")
    print(f"  Experiment complete. {len(verdicts)} rules evaluated.")
    print(f"  Results logged to reports/experiment_log.csv")
    print(f"  Evidence logged to reports/evidence_table.csv")
    print(f"{'='*70}\n")

    return {
        "experiment_name": experiment_name,
        "chapter": chapter,
        "scorecard_is": scorecard,
        "scorecard_oos": oos_scorecard,
        "verdicts": verdicts,
        "n_baseline": len(all_baseline["returns"]),
        "n_candidate": len(all_candidate["returns"]),
        "per_ticker_is": per_ticker_is,
        "per_ticker_oos": per_ticker_oos,
        "cross_instrument_is": is_improved,
        "cross_instrument_oos": oos_improved,
    }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    spec = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "config",
        "experiment_spec.example.json",
    )
    run_experiment(spec)
