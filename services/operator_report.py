"""
operator_report.py

Production-safe read-only report that summarises every MONOS analytics
output stored in Supabase.  Designed to be run ad-hoc or after a nightly
pipeline execution.

Sections:
  1. Portfolio Overview    — ladders / positions / legs counts
  2. Latest Market Prices  — most recent price per ticker
  3. Portfolio Risk        — average Greeks from latest snapshots
  4. Top Convexity         — highest conviction scores
  5. Gamma Concentration   — strike-level gamma heatmap
  6. Dealer Gamma Exposure — net dealer GEX by strike
  7. Pipeline Health       — latest briefing + recent task_runs

Usage:
    python services/operator_report.py
"""

import json
import logging
import os
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.supabase_helpers import get_supabase_client

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)

LINE = "=" * 60
DASH = "-" * 60


# ─────────────────────────────────────────────────────────────────────
# Safe data loader
# ─────────────────────────────────────────────────────────────────────

def fetch_table(sb, name: str, *, limit: int | None = None,
                select: str = "*", order: str | None = None,
                desc: bool = True, eq: tuple | None = None) -> list[dict]:
    """
    Fetch rows from a Supabase table with optional limit, ordering,
    and equality filter.  Returns an empty list on any error so the
    report never crashes.
    """
    try:
        query = sb.table(name).select(select)
        if eq:
            query = query.eq(eq[0], eq[1])
        if order:
            query = query.order(order, desc=desc)
        if limit:
            query = query.limit(limit)
        return query.execute().data or []
    except Exception:
        logger.exception("Failed to fetch table '%s'", name)
        return []


# ─────────────────────────────────────────────────────────────────────
# Report sections
# ─────────────────────────────────────────────────────────────────────

def section_portfolio(sb) -> None:
    ladders   = fetch_table(sb, "ladders")
    positions = fetch_table(sb, "positions")
    legs      = fetch_table(sb, "position_legs")

    print()
    print("1.  Portfolio Overview")
    print(DASH)
    print(f"  Ladders:        {len(ladders)}")
    print(f"  Positions:      {len(positions)}")
    print(f"  Option legs:    {len(legs)}")

    tickers = sorted({p.get("ticker", "?") for p in positions})
    print(f"  Tickers:        {', '.join(tickers) if tickers else '(none)'}")


def section_market(sb) -> None:
    rows = fetch_table(sb, "market_snapshots", limit=50,
                       order="created_at", desc=True)

    print()
    print("2.  Latest Market Prices")
    print(DASH)

    if not rows:
        print("  (no market snapshots)")
        return

    seen: dict[str, dict] = {}
    for r in rows:
        t = r.get("ticker")
        if t and t not in seen:
            seen[t] = r

    for ticker in sorted(seen):
        r = seen[ticker]
        ts = (r.get("created_at") or "")[:19].replace("T", " ")
        price = r.get("price", 0)
        vol   = r.get("volume", 0) or 0
        print(f"  {ticker:6s}  ${price:>10,.2f}    vol={vol:>14,.0f}    @ {ts}")


def section_greeks(sb) -> None:
    rows = fetch_table(sb, "greeks_snapshots", limit=200,
                       order="created_at", desc=True)

    print()
    print(f"3.  Portfolio Risk  (latest {len(rows)} snapshots)")
    print(DASH)

    if not rows:
        print("  (no greeks data)")
        return

    n = len(rows)
    avg_delta = sum(float(r.get("delta") or 0) for r in rows) / n
    avg_gamma = sum(float(r.get("gamma") or 0) for r in rows) / n
    avg_theta = sum(float(r.get("theta") or 0) for r in rows) / n
    avg_vega  = sum(float(r.get("vega")  or 0) for r in rows) / n

    print(f"  avg delta:  {avg_delta:>+10.6f}")
    print(f"  avg gamma:  {avg_gamma:>10.6f}")
    print(f"  avg theta:  {avg_theta:>+10.4f}")
    print(f"  avg vega:   {avg_vega:>+10.4f}")


def _parse_result(data) -> list | dict | None:
    """Safely parse a simulation_runs result field (may be JSON string or native)."""
    if data is None:
        return None
    if isinstance(data, str):
        try:
            return json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return None
    return data


def section_conviction(sb) -> None:
    runs = fetch_table(sb, "simulation_runs", limit=1,
                       order="created_at", desc=True,
                       eq=("engine", "conviction_map"))

    print()
    print("4.  Top Convexity Positions (Conviction Scores)")
    print(DASH)

    if not runs:
        print("  (no conviction data)")
        return

    result = _parse_result(runs[0].get("result"))
    scores = []
    if isinstance(result, dict):
        scores = result.get("scores", [])
    elif isinstance(result, list):
        scores = result

    if not scores:
        print("  (conviction run has no scores)")
        return

    top = sorted(scores,
                 key=lambda x: x.get("conviction_score", 0),
                 reverse=True)[:5]

    print(f"  {'Ticker':<6} {'Leg':<14} {'Strike':>8} {'Expiry':>12} {'Score':>8}")
    print(f"  {'------':<6} {'---':<14} {'------':>8} {'-----':>12} {'-----':>8}")
    for p in top:
        print(f"  {p.get('ticker','?'):<6} {p.get('leg_type','?'):<14} "
              f"{p.get('strike',0):>8.0f} {p.get('expiration','?'):>12} "
              f"{p.get('conviction_score',0):>8.2f}")


def section_gamma_heatmap(sb) -> None:
    runs = fetch_table(sb, "simulation_runs", limit=1,
                       order="created_at", desc=True,
                       eq=("engine", "convexity_heatmap"))

    print()
    print("5.  Gamma Concentration by Strike")
    print(DASH)

    if not runs:
        print("  (no heatmap data)")
        return

    records = _parse_result(runs[0].get("result"))
    if not isinstance(records, list) or not records:
        print("  (heatmap run has no records)")
        return

    top = sorted(records, key=lambda x: x.get("gamma", 0), reverse=True)[:10]

    print(f"  {'Strike':>10} | {'Gamma':>12}")
    print(f"  {'-'*10}-+-{'-'*12}")
    for r in top:
        print(f"  {r.get('strike',0):>10.0f} | {r.get('gamma',0):>12.6f}")


def section_dealer_gex(sb) -> None:
    runs = fetch_table(sb, "simulation_runs", limit=1,
                       order="created_at", desc=True,
                       eq=("engine", "dealer_gamma_exposure"))

    print()
    print("6.  Dealer Gamma Exposure (GEX)")
    print(DASH)

    if not runs:
        print("  (no GEX data)")
        return

    records = _parse_result(runs[0].get("result"))
    if not isinstance(records, list) or not records:
        print("  (GEX run has no records)")
        return

    print(f"  {'Strike':>10}   {'Dealer Gamma':>14}")
    print(f"  {'-'*10}   {'-'*14}")
    for r in records:
        print(f"  {r.get('strike',0):>10.0f}   {r.get('dealer_gamma',0):>+14.6f}")


def section_pipeline_health(sb) -> None:
    briefings = fetch_table(sb, "briefing_reports", limit=1,
                            order="created_at", desc=True)
    task_runs = fetch_table(sb, "task_runs", limit=3,
                            order="created_at", desc=True)

    print()
    print("7.  Pipeline Health")
    print(DASH)

    if briefings:
        b = briefings[0]
        ts = (b.get("created_at") or "")[:19].replace("T", " ")
        print(f"  Latest briefing:   {b.get('report_date', '?')}  (created {ts})")
    else:
        print("  Latest briefing:   (none)")

    if task_runs:
        for tr in task_runs:
            ts = (tr.get("created_at") or "")[:19].replace("T", " ")
            print(f"  Task run:          {tr.get('task_name','?')} → "
                  f"{tr.get('status','?')}  @ {ts}")
    else:
        print("  Task runs:         (none)")


# ─────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────

def main():
    sb = get_supabase_client()

    print()
    print(LINE)
    print("         MONOS Operator Report")
    print(f"         {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(LINE)

    section_portfolio(sb)
    section_market(sb)
    section_greeks(sb)
    section_conviction(sb)
    section_gamma_heatmap(sb)
    section_dealer_gex(sb)
    section_pipeline_health(sb)

    print()
    print(LINE)
    print("  Operator report complete.")
    print(LINE)
    print()


if __name__ == "__main__":
    main()
