"""
run_full_pipeline.py

MONOS Convex Opportunity Scanner — Full Pipeline Orchestrator

Execution order (12 steps):
    1.  candidate_universe_engine   — build ticker universe
    2.  dealer_positioning_engine   — compute dealer GEX per ticker
    3.  risk_overlay_engine         — assemble composite risk overlay
    4.  vol_surface_engine          — compute volatility surface metrics
    5.  convexity_trigger_engine    — detect convexity trigger events (placeholder)
    6.  scanner_engine              — score opportunities, filter, recommend
    7.  structure_builder           — construct option structures
    8.  scenario_engine             — simulate payoff surfaces
    9.  portfolio_governor          — approve / block structures
   10.  heatmap_engine              — generate convex opportunity heatmap
   11.  flow_engine                 — capture options flow (skeleton)
   12.  regime_probability_engine   — estimate regime path probabilities

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
TOTAL_STEPS = 12


def run_pipeline(tickers_limit: int | None = None):
    """
    Execute the full 12-step MONOS scanner pipeline.

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
        print(f"\n[1/{TOTAL_STEPS}] Building candidate universe...")
        from monos.universe.candidate_universe_engine import CandidateUniverseEngine
        universe_engine = CandidateUniverseEngine()
        universe = universe_engine.build()
        tickers = universe.tickers
        if tickers_limit:
            tickers = tickers[:tickers_limit]
        print(f"       Universe: {len(tickers)} tickers")
        step_times["01_universe"] = round(time.time() - t0, 1)

        # ── Step 2: Dealer Positioning ────────────────────────────
        t0 = time.time()
        print(f"\n[2/{TOTAL_STEPS}] Computing dealer positioning...")
        from monos.dealer.dealer_positioning_engine import DealerPositioningEngine
        dealer_engine = DealerPositioningEngine()
        dealer_results = dealer_engine.run(tickers)
        dealer_map = {dp.ticker: dp for dp in dealer_results}
        dealer_regimes = {dp.ticker: dp.gamma_regime for dp in dealer_results}
        print(f"       Positioned: {len(dealer_results)} tickers")
        step_times["02_dealer"] = round(time.time() - t0, 1)

        # ── Step 3: Risk Overlay ──────────────────────────────────
        t0 = time.time()
        print(f"\n[3/{TOTAL_STEPS}] Building risk overlay...")
        from monos.risk.risk_overlay_engine import RiskOverlayEngine
        risk_engine = RiskOverlayEngine()
        overlay = risk_engine.build(dealer_regimes)
        print(f"       Gamma={overlay.gamma_regime} Vol={overlay.volatility_regime} "
              f"Macro={overlay.macro_regime} Complexity={overlay.complexity_index}")
        step_times["03_risk"] = round(time.time() - t0, 1)

        # ── Step 4: Vol Surface ───────────────────────────────────
        t0 = time.time()
        print(f"\n[4/{TOTAL_STEPS}] Computing volatility surfaces...")
        from monos.volatility.vol_surface_engine import VolSurfaceEngine
        vol_engine = VolSurfaceEngine()
        vol_results = vol_engine.run(tickers)
        print(f"       Vol surfaces: {len(vol_results)} computed")
        step_times["04_vol_surface"] = round(time.time() - t0, 1)

        # ── Step 5: Convexity Trigger (placeholder) ───────────────
        t0 = time.time()
        print(f"\n[5/{TOTAL_STEPS}] Detecting convexity triggers (placeholder)...")
        # Phase-2: from monos.trigger.convexity_trigger_engine import ConvexityTriggerEngine
        # trigger_engine = ConvexityTriggerEngine()
        # triggers = trigger_engine.run(tickers, vol_results, dealer_results)
        triggers = []  # placeholder until engine is built
        print(f"       Triggers: {len(triggers)} (placeholder — engine not yet built)")
        step_times["05_convexity_trigger"] = round(time.time() - t0, 1)

        # ── Step 6: Scanner ───────────────────────────────────────
        t0 = time.time()
        print(f"\n[6/{TOTAL_STEPS}] Scanning opportunities...")
        from monos.scanner.scanner_engine import ScannerEngine
        scanner = ScannerEngine()
        candidates = scanner.scan(tickers, dealer_map, overlay)
        print(f"       Candidates: {len(candidates)} passed threshold")
        step_times["06_scanner"] = round(time.time() - t0, 1)

        # ── Step 7: Structure Builder ─────────────────────────────
        t0 = time.time()
        print(f"\n[7/{TOTAL_STEPS}] Building option structures...")
        from monos.builder.structure_builder import StructureBuilder
        builder = StructureBuilder()
        spot_map = {dp.ticker: dp.spot for dp in dealer_results}
        structures = builder.build_all(candidates, spot_map)
        print(f"       Structures: {len(structures)} built")
        step_times["07_builder"] = round(time.time() - t0, 1)

        # ── Step 8: Scenario Engine ───────────────────────────────
        t0 = time.time()
        print(f"\n[8/{TOTAL_STEPS}] Simulating scenarios...")
        from monos.scenario.scenario_engine import ScenarioEngine
        scenario_engine = ScenarioEngine()
        all_scenarios = scenario_engine.run(structures, spot_map)
        print(f"       Scenario points: {len(all_scenarios)}")
        step_times["08_scenario"] = round(time.time() - t0, 1)

        # ── Step 9: Portfolio Governor ────────────────────────────
        t0 = time.time()
        print(f"\n[9/{TOTAL_STEPS}] Running portfolio governor...")
        from monos.governance.scanner_governor_bridge import PortfolioGovernor
        governor = PortfolioGovernor()
        decisions = governor.run(structures, overlay)
        approved = sum(1 for d in decisions if d.status == "APPROVED")
        blocked  = sum(1 for d in decisions if d.status == "BLOCKED")
        cond     = sum(1 for d in decisions if d.status == "CONDITIONAL")
        print(f"       Approved={approved} Blocked={blocked} Conditional={cond}")
        step_times["09_governor"] = round(time.time() - t0, 1)

        # ── Step 10: Heatmap ──────────────────────────────────────
        t0 = time.time()
        print(f"\n[10/{TOTAL_STEPS}] Generating opportunity heatmap...")
        from monos.heatmap.heatmap_engine import HeatmapEngine
        heatmap = HeatmapEngine()
        cells = heatmap.generate(candidates, structures, decisions)
        print(f"       Heatmap cells: {len(cells)}")
        if cells:
            print(f"       Top: {cells[0].ticker} heat={cells[0].heat_score} "
                  f"struct={cells[0].recommended_structure}")
        step_times["10_heatmap"] = round(time.time() - t0, 1)

        # ── Step 11: Flow Engine ──────────────────────────────────
        t0 = time.time()
        print(f"\n[11/{TOTAL_STEPS}] Capturing flow data (placeholder)...")
        from monos.flow.flow_engine import FlowEngine
        flow = FlowEngine()
        flow_snaps = flow.run(tickers[:10])  # Limit flow to top 10
        print(f"       Flow snapshots: {len(flow_snaps)}")
        step_times["11_flow"] = round(time.time() - t0, 1)

        # ── Step 12: Regime Probability ───────────────────────────
        t0 = time.time()
        print(f"\n[12/{TOTAL_STEPS}] Computing regime probabilities...")
        from monos.probability.regime_probability_engine import RegimeProbabilityEngine
        prob_engine = RegimeProbabilityEngine()
        prob_results = prob_engine.run(tickers)
        print(f"       Regime probabilities: {len(prob_results)} tickers")
        step_times["12_regime_probability"] = round(time.time() - t0, 1)

        # ── Summary ───────────────────────────────────────────────
        elapsed = round(time.time() - start, 1)
        print()
        print("=" * 64)
        print("  Pipeline Complete")
        print(f"  Total time: {elapsed}s")
        print()
        print("  Step Timings:")
        for step, t in step_times.items():
            print(f"    {step:28s} {t:>6.1f}s")
        print()
        print(f"  Universe:       {len(tickers)} tickers")
        print(f"  Vol Surfaces:   {len(vol_results)} computed")
        print(f"  Triggers:       {len(triggers)} detected")
        print(f"  Candidates:     {len(candidates)} passed")
        print(f"  Structures:     {len(structures)} built")
        print(f"  Scenarios:      {len(all_scenarios)} points")
        print(f"  Governor:       {approved}A / {blocked}B / {cond}C")
        print(f"  Heatmap:        {len(cells)} cells")
        print(f"  Flow:           {len(flow_snaps)} snapshots")
        print(f"  Probabilities:  {len(prob_results)} tickers")
        print("=" * 64)

        write_agent_log(AGENT, "run_pipeline", "success", {
            "elapsed_sec": elapsed,
            "tickers": len(tickers),
            "vol_surfaces": len(vol_results),
            "triggers": len(triggers),
            "candidates": len(candidates),
            "structures": len(structures),
            "scenarios": len(all_scenarios),
            "heatmap_cells": len(cells),
            "governor": {"approved": approved, "blocked": blocked, "conditional": cond},
            "flow_snapshots": len(flow_snaps),
            "probabilities": len(prob_results),
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
