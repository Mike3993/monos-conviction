"""
MONOS Research Lab — 5-Metric Scorecard
----------------------------------------
Compares a candidate rule's performance against a baseline
across five institutional-grade metrics.

Uses log/geometric returns throughout to preserve compounding
discipline and avoid arithmetic-return bias.

Metrics:
  1. Sharpe ratio delta
  2. Win rate delta
  3. Max drawdown delta
  4. Theta efficiency delta
  5. Convexity retention (positive or not)
"""

from __future__ import annotations

import math
from typing import Any


# ── metric computation helpers ───────────────────────────────────────

def sharpe_ratio(returns: list[float], risk_free_annual: float = 0.05) -> float:
    """Annualised Sharpe ratio from a list of per-trade log returns.

    Uses geometric (log) return discipline:
    - daily risk-free = log(1 + annual_rf) / 252
    - excess = return - rf_per_period
    """
    if len(returns) < 2:
        return 0.0
    rf_per = math.log(1 + risk_free_annual) / 252
    excess = [r - rf_per for r in returns]
    mean_ex = sum(excess) / len(excess)
    var = sum((r - mean_ex) ** 2 for r in excess) / (len(excess) - 1)
    std = math.sqrt(var) if var > 0 else 1e-9
    return round((mean_ex / std) * math.sqrt(252), 4)


def win_rate(returns: list[float]) -> float:
    """Fraction of trades with positive return."""
    if not returns:
        return 0.0
    wins = sum(1 for r in returns if r > 0)
    return round(wins / len(returns), 4)


def max_drawdown(equity_curve: list[float]) -> float:
    """Maximum peak-to-trough drawdown from an equity curve.

    Returns a negative number (e.g. -0.12 = -12%).
    """
    if len(equity_curve) < 2:
        return 0.0
    peak = equity_curve[0]
    mdd = 0.0
    for v in equity_curve:
        if v > peak:
            peak = v
        dd = (v - peak) / peak if peak != 0 else 0
        if dd < mdd:
            mdd = dd
    return round(mdd, 4)


def theta_efficiency(total_return: float, total_theta_cost: float) -> float:
    """Return captured per unit of theta spent.

    theta_efficiency = total_return / abs(total_theta_cost)
    Higher is better — means each dollar of theta decay
    bought more directional return.
    """
    if total_theta_cost == 0:
        return 0.0
    return round(total_return / abs(total_theta_cost), 4)


def convexity_retained(returns: list[float], threshold_pct: float = 5.0) -> bool:
    """Check whether the candidate retains convex (outsized) gains.

    True if at least one trade exceeded the threshold.
    This prevents rules that clip the upside tail.
    """
    return any(r > threshold_pct for r in returns)


# ── scorecard ────────────────────────────────────────────────────────

def compute_scorecard(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
) -> dict[str, Any]:
    """Compare candidate vs baseline across the 5-metric scorecard.

    Parameters
    ----------
    baseline, candidate : dict
        Each must contain:
        - returns: list[float]         (per-trade log returns, %)
        - equity_curve: list[float]    (cumulative equity)
        - total_return: float          (sum of returns)
        - total_theta_cost: float      (estimated total theta drag)

    Returns
    -------
    dict with:
        metrics: list of {name, baseline, candidate, delta, passed}
        pass_count: int
        total_metrics: int
        overall_pass: bool
        summary: str
    """
    b_ret = baseline.get("returns", [])
    c_ret = candidate.get("returns", [])
    b_eq = baseline.get("equity_curve", [1.0])
    c_eq = candidate.get("equity_curve", [1.0])

    # 1. Sharpe ratio delta
    b_sharpe = sharpe_ratio(b_ret)
    c_sharpe = sharpe_ratio(c_ret)
    sharpe_d = round(c_sharpe - b_sharpe, 4)

    # 2. Win rate delta
    b_wr = win_rate(b_ret)
    c_wr = win_rate(c_ret)
    wr_d = round(c_wr - b_wr, 4)

    # 3. Max drawdown delta (improvement = less negative = positive delta)
    b_mdd = max_drawdown(b_eq)
    c_mdd = max_drawdown(c_eq)
    mdd_d = round(c_mdd - b_mdd, 4)  # positive = improvement

    # 4. Theta efficiency delta
    b_theta = theta_efficiency(
        baseline.get("total_return", 0),
        baseline.get("total_theta_cost", 1),
    )
    c_theta = theta_efficiency(
        candidate.get("total_return", 0),
        candidate.get("total_theta_cost", 1),
    )
    theta_d = round(c_theta - b_theta, 4)

    # 5. Convexity retention
    b_convex = convexity_retained(b_ret)
    c_convex = convexity_retained(c_ret)

    metrics = [
        {
            "name": "sharpe_ratio_delta",
            "baseline": b_sharpe,
            "candidate": c_sharpe,
            "delta": sharpe_d,
            "passed": sharpe_d > 0,
        },
        {
            "name": "win_rate_delta",
            "baseline": b_wr,
            "candidate": c_wr,
            "delta": wr_d,
            "passed": wr_d >= 0,  # non-negative is acceptable
        },
        {
            "name": "max_drawdown_delta",
            "baseline": b_mdd,
            "candidate": c_mdd,
            "delta": mdd_d,
            "passed": mdd_d >= 0,  # less negative = improvement
        },
        {
            "name": "theta_efficiency_delta",
            "baseline": b_theta,
            "candidate": c_theta,
            "delta": theta_d,
            "passed": theta_d >= 0,
        },
        {
            "name": "convexity_retention",
            "baseline": b_convex,
            "candidate": c_convex,
            "delta": None,
            "passed": c_convex,  # candidate must retain convexity
        },
    ]

    pass_count = sum(1 for m in metrics if m["passed"])
    total_metrics = len(metrics)
    overall = pass_count >= 4  # 4 of 5 required

    labels = []
    for m in metrics:
        mark = "PASS" if m["passed"] else "FAIL"
        if m["delta"] is not None:
            labels.append(f"{m['name']}: {m['delta']:+.4f} [{mark}]")
        else:
            labels.append(f"{m['name']}: {m['candidate']} [{mark}]")

    summary = f"{pass_count}/{total_metrics} passed — {'OVERALL PASS' if overall else 'OVERALL FAIL'}\n" + "\n".join(labels)

    return {
        "metrics": metrics,
        "pass_count": pass_count,
        "total_metrics": total_metrics,
        "overall_pass": overall,
        "summary": summary,
    }
