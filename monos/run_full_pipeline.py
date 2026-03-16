"""
run_full_pipeline.py

MONOS Convex Opportunity Scanner — Full Pipeline Orchestrator

Execution order:
    1. candidate_universe_engine   — build ticker universe
    2. dealer_positioning_engine   — compute dealer GEX per ticker
    3. risk_overlay_engine         — assemble composite risk overlay
    4. scanner_engine              — score opportunities, filter, recommend
    5. structure_builder           — construct option structures
    6. scenario_engine             — simulate payoff surfaces
    7. portfolio_governor          — approve / block structures
    8. heatmap_engine              — generate convex opportunity heatmap
    9. flow_engine                 — capture options flow (skeleton)

All results written to Supabase.
"""

import logging
import sys
import time
import traceback
from datetime import datetime

# Ensure monos package is importable
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from monos.storage.supabase_client import write_agent_log

logger = logging.getLogger(__name__)

AGENT = "monos_pipeline"


def run_pipeline(tickers_limit: int | None = None):
    """
    Execute the full 9-step MONOS scanner pipeline.

    Parameters
    ----------
    tickers_limit : optional cap on universe size for faster runs
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    start = time.time()
    step_times = {}

    print()
    print("=" * 64)
    print("  MONOS Convex Opportunity Scanner — Full Pipeline")
    print(f"  {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 64)

    try:
        # ── Step 1: Universe ──────────────────────────────────────
        t0 = time.time()
        print("\n[1/9] Building candidate universe...")
        from monos.universe.candidate_universe_engine import CandidateUniverseEngine
        universe_engine = CandidateUniverseEngine()
        universe = universe_engine.build()
        tickers = universe.tickers
        if tickers_limit:
            tickers = tickers[:tickers_limit]
        print(f"       Universe: {len(tickers)} tickers")
        step_times["1_universe"] = round(time.time() - t0, 1)

        # ── Step 2: Dealer Positioning ────────────────────────────
        t0 = time.time()
        print("\n[2/9] Computing dealer positioning...")
        from monos.dealer.dealer_positioning_engine import DealerPositioningEngine
        dealer_engine = DealerPositioningEngine()
        dealer_results = dealer_engine.run(tickers)
        dealer_map = {dp.ticker: dp for dp in dealer_results}
        dealer_regimes = {dp.ticker: dp.gamma_regime for dp in dealer_results}
        print(f"       Positioned: {len(dealer_results)} tickers")
        step_times["2_dealer"] = round(time.time() - t0, 1)

        # ── Step 3: Risk Overlay ──────────────────────────────────
        t0 = time.time()
        print("\n[3/9] Building risk overlay...")
        from monos.risk.risk_overlay_engine import RiskOverlayEngine
        risk_engine = RiskOverlayEngine()
        overlay = risk_engine.build(dealer_regimes)
        print(f"       Gamma={overlay.gamma_regime} Vol={overlay.volatility_regime} "
              f"Macro={overlay.macro_regime} Complexity={overlay.complexity_index}")
        step_times["3_risk"] = round(time.time() - t0, 1)

        # ── Step 4: Scanner ───────────────────────────────────────
        t0 = time.time()
        print("\n[4/9] Scanning opportunities...")
        from monos.scanner.scanner_engine import ScannerEngine
        scanner = ScannerEngine()
        candidates = scanner.scan(tickers, dealer_map, overlay)
        print(f"       Candidates: {len(candidates)} passed threshold")
        step_times["4_scanner"] = round(time.time() - t0, 1)

        # ── Step 5: Structure Builder ─────────────────────────────
        t0 = time.time()
        print("\n[5/9] Building option structures...")
        from monos.builder.structure_builder import StructureBuilder
        builder = StructureBuilder()
        # Build spot map from dealer results
        spot_map = {dp.ticker: dp.spot for dp in dealer_results}
        structures = builder.build_all(candidates, spot_map)
        print(f"       Structures: {len(structures)} built")
        step_times["5_builder"] = round(time.time() - t0, 1)

        # ── Step 6: Scenario Engine ───────────────────────────────
        t0 = time.time()
        print("\n[6/9] Simulating scenarios...")
        from monos.scenario.scenario_engine import ScenarioEngine
        scenario_engine = ScenarioEngine()
        all_scenarios = scenario_engine.run(structures, spot_map)
        print(f"       Scenario points: {len(all_scenarios)}")
        step_times["6_scenario"] = round(time.time() - t0, 1)

        # ── Step 7: Portfolio Governor ────────────────────────────
        t0 = time.time()
        print("\n[7/9] Running portfolio governor...")
        from monos.governance.scanner_governor_bridge import PortfolioGovernor
        governor = PortfolioGovernor()
        decisions = governor.run(structures, overlay)
        approved = sum(1 for d in decisions if d.status == "APPROVED")
        blocked  = sum(1 for d in decisions if d.status == "BLOCKED")
        cond     = sum(1 for d in decisions if d.status == "CONDITIONAL")
        print(f"       Approved={approved} Blocked={blocked} Conditional={cond}")
        step_times["7_governor"] = round(time.time() - t0, 1)

        # ── Step 8: Heatmap ───────────────────────────────────────
        t0 = time.time()
        print("\n[8/9] Generating opportunity heatmap...")
        from monos.heatmap.heatmap_engine import HeatmapEngine
        heatmap = HeatmapEngine()
        cells = heatmap.generate(candidates, structures, decisions)
        print(f"       Heatmap cells: {len(cells)}")
        if cells:
            print(f"       Top: {cells[0].ticker} heat={cells[0].heat_score} "
                  f"struct={cells[0].recommended_structure}")
        step_times["8_heatmap"] = round(time.time() - t0, 1)

        # ── Step 9: Flow Engine ───────────────────────────────────
        t0 = time.time()
        print("\n[9/9] Capturing flow data (placeholder)...")
        from monos.flow.flow_engine import FlowEngine
        flow = FlowEngine()
        flow_snaps = flow.run(tickers[:10])  # Limit flow to top 10
        print(f"       Flow snapshots: {len(flow_snaps)}")
        step_times["9_flow"] = round(time.time() - t0, 1)

        # ── Summary ───────────────────────────────────────────────
        elapsed = round(time.time() - start, 1)
        print()
        print("=" * 64)
        print("  Pipeline Complete")
        print(f"  Total time: {elapsed}s")
        print()
        print("  Step Timings:")
        for step, t in step_times.items():
            print(f"    {step:20s} {t:>6.1f}s")
        print()
        print(f"  Universe:    {len(tickers)} tickers")
        print(f"  Candidates:  {len(candidates)} passed")
        print(f"  Structures:  {len(structures)} built")
        print(f"  Scenarios:   {len(all_scenarios)} points")
        print(f"  Governor:    {approved}A / {blocked}B / {cond}C")
        print(f"  Heatmap:     {len(cells)} cells")
        print(f"  Flow:        {len(flow_snaps)} snapshots")
        print("=" * 64)

        write_agent_log(AGENT, "run_pipeline", "success", {
            "elapsed_sec": elapsed,
            "tickers": len(tickers),
            "candidates": len(candidates),
            "structures": len(structures),
            "scenarios": len(all_scenarios),
            "heatmap_cells": len(cells),
            "governor": {"approved": approved, "blocked": blocked, "conditional": cond},
            "step_times": step_times,
        })

    except Exception as e:
        elapsed = round(time.time() - start, 1)
        logger.exception("Pipeline failed after %.1fs", elapsed)
        print(f"\n  PIPELINE FAILED after {elapsed}s: {e}")
        traceback.print_exc()

        write_agent_log(AGENT, "run_pipeline", "failed", {
            "error": str(e),
            "elapsed_sec": elapsed,
            "step_times": step_times,
        })
        sys.exit(1)


# ── CLI ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="MONOS Full Pipeline")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap universe size for faster runs")
    args = parser.parse_args()
    run_pipeline(tickers_limit=args.limit)
