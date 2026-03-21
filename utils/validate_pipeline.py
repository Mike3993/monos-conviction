"""
validate_pipeline.py

Read-only diagnostic tool that connects to Supabase, pulls recent data
from all pipeline tables, and prints a formatted validation report.

Usage:
    python utils/validate_pipeline.py
"""

import os
import sys
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.supabase_helpers import get_supabase_client


def main():
    sb = get_supabase_client()

    # ── Portfolio counts ──────────────────────────────────────────────

    ladders = sb.table("ladders").select("*").execute().data
    positions = sb.table("positions").select("*").execute().data
    legs = sb.table("position_legs").select("*").execute().data

    # ── Market snapshots (latest per ticker) ──────────────────────────

    market = (sb.table("market_snapshots")
              .select("ticker, price, volume, created_at")
              .order("created_at", desc=True)
              .limit(50)
              .execute().data)

    latest_prices: dict[str, dict] = {}
    for row in market:
        t = row["ticker"]
        if t not in latest_prices:
            latest_prices[t] = row

    # ── Greeks snapshots (latest batch) ───────────────────────────────

    greeks = (sb.table("greeks_snapshots")
              .select("delta, gamma, theta, vega, created_at")
              .order("created_at", desc=True)
              .limit(100)
              .execute().data)

    if greeks:
        n = len(greeks)
        avg_delta = sum(float(g["delta"] or 0) for g in greeks) / n
        avg_gamma = sum(float(g["gamma"] or 0) for g in greeks) / n
        avg_theta = sum(float(g["theta"] or 0) for g in greeks) / n
        avg_vega = sum(float(g["vega"] or 0) for g in greeks) / n
    else:
        avg_delta = avg_gamma = avg_theta = avg_vega = 0.0
        n = 0

    # ── Conviction scores (from simulation_runs) ─────────────────────

    conviction_run = (sb.table("simulation_runs")
                      .select("result")
                      .eq("engine", "conviction_map")
                      .order("created_at", desc=True)
                      .limit(1)
                      .execute().data)

    conviction_scores: list[dict] = []
    if conviction_run and conviction_run[0].get("result"):
        result_data = conviction_run[0]["result"]
        if isinstance(result_data, str):
            import json
            result_data = json.loads(result_data)
        conviction_scores = result_data.get("scores", [])

    top_conviction = sorted(conviction_scores,
                            key=lambda x: x.get("conviction_score", 0),
                            reverse=True)[:5]

    # ── Gamma heatmap (from simulation_runs) ──────────────────────────

    heatmap_run = (sb.table("simulation_runs")
                   .select("result")
                   .eq("engine", "convexity_heatmap")
                   .order("created_at", desc=True)
                   .limit(1)
                   .execute().data)

    heatmap_records: list[dict] = []
    if heatmap_run and heatmap_run[0].get("result"):
        result_data = heatmap_run[0]["result"]
        if isinstance(result_data, str):
            import json
            result_data = json.loads(result_data)
        heatmap_records = result_data if isinstance(result_data, list) else []

    top_gamma = sorted(heatmap_records,
                       key=lambda x: x.get("gamma", 0),
                       reverse=True)[:5]

    # ── Briefing reports ──────────────────────────────────────────────

    briefings = (sb.table("briefing_reports")
                 .select("report_date, created_at")
                 .order("created_at", desc=True)
                 .limit(1)
                 .execute().data)

    # ── Task runs ─────────────────────────────────────────────────────

    task_runs = (sb.table("task_runs")
                 .select("task_name, status, created_at")
                 .order("created_at", desc=True)
                 .limit(3)
                 .execute().data)

    # ══════════════════════════════════════════════════════════════════
    # Print report
    # ══════════════════════════════════════════════════════════════════

    print()
    print("=" * 56)
    print("         MONOS Validation Report")
    print(f"         {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 56)

    # Portfolio
    print()
    print("Portfolio")
    print("-" * 56)
    print(f"  Ladders:        {len(ladders)}")
    print(f"  Positions:      {len(positions)}")
    print(f"  Option legs:    {len(legs)}")

    tickers = sorted({p.get("ticker", "?") for p in positions})
    print(f"  Tickers:        {', '.join(tickers)}")

    # Market data
    print()
    print("Latest Market Prices")
    print("-" * 56)
    if latest_prices:
        for ticker in sorted(latest_prices):
            row = latest_prices[ticker]
            ts = row["created_at"][:19].replace("T", " ")
            print(f"  {ticker:6s}  ${row['price']:>10,.2f}    vol={row['volume']:>14,.0f}    @ {ts}")
    else:
        print("  (no market snapshots)")

    # Greeks
    print()
    print(f"Greeks Summary  (latest {n} snapshots)")
    print("-" * 56)
    print(f"  avg delta:  {avg_delta:>+10.6f}")
    print(f"  avg gamma:  {avg_gamma:>10.6f}")
    print(f"  avg theta:  {avg_theta:>+10.4f}")
    print(f"  avg vega:   {avg_vega:>+10.4f}")

    # Conviction
    print()
    print("Top 5 Positions by Conviction Score")
    print("-" * 56)
    if top_conviction:
        print(f"  {'Ticker':<6} {'Leg':<12} {'Strike':>8} {'Exp':>12} {'Score':>8}")
        for p in top_conviction:
            print(f"  {p.get('ticker','?'):<6} {p.get('leg_type','?'):<12} "
                  f"{p.get('strike',0):>8.0f} {p.get('expiration','?'):>12} "
                  f"{p.get('conviction_score',0):>8.2f}")
    else:
        print("  (no conviction scores)")

    # Gamma heatmap
    print()
    print("Convexity Hotspots (Gamma by Strike)")
    print("-" * 56)
    if top_gamma:
        print(f"  {'Strike':>10} | {'Gamma':>12}")
        print(f"  {'-'*10}-+-{'-'*12}")
        for row in top_gamma:
            print(f"  {row['strike']:>10.0f} | {row['gamma']:>12.6f}")
    else:
        print("  (no heatmap data)")

    # Pipeline health
    print()
    print("Pipeline Health")
    print("-" * 56)
    if briefings:
        b = briefings[0]
        print(f"  Latest briefing:   {b['report_date']}  (created {b['created_at'][:19]})")
    else:
        print("  Latest briefing:   (none)")

    if task_runs:
        for tr in task_runs:
            ts = tr["created_at"][:19].replace("T", " ")
            print(f"  Task run:          {tr['task_name']} -> {tr['status']}  @ {ts}")
    else:
        print("  Task runs:         (none)")

    print()
    print("=" * 56)
    print("  Validation complete.")
    print("=" * 56)
    print()


if __name__ == "__main__":
    main()
