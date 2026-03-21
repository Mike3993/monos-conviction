"""
heatmap_engine.py

Generates the MONOS Convex Opportunity Heatmap.

heat_score formula:
    heat_score = opportunity_score + 0.25 × (convexity_score − 50)
    if governor_status == BLOCKED: heat_score *= 0.4
    clamp to 0–100

Clusters:
    INDEX, MEGACAP_TECH, METALS, ENERGY, CRYPTO, PORTFOLIO
"""

import logging
from dataclasses import dataclass
from datetime import datetime

from monos.scanner.scanner_engine import ScanResult
from monos.builder.structure_builder import Structure
from monos.governance.scanner_governor_bridge import GovernorDecision
from monos.storage.heatmap_repository import write_heatmap_run, write_heatmap_cells
from monos.storage.supabase_client import write_agent_log

logger = logging.getLogger(__name__)

AGENT = "heatmap_engine"

# ── Cluster assignments ───────────────────────────────────────────

CLUSTER_MAP = {
    "SPY": "INDEX", "QQQ": "INDEX", "IWM": "INDEX", "DIA": "INDEX",
    "NVDA": "MEGACAP_TECH", "AAPL": "MEGACAP_TECH", "MSFT": "MEGACAP_TECH",
    "AMZN": "MEGACAP_TECH", "TSLA": "MEGACAP_TECH", "META": "MEGACAP_TECH",
    "GOOG": "MEGACAP_TECH",
    "GLD": "METALS", "SLV": "METALS", "GDX": "METALS", "SIL": "METALS",
    "GDXJ": "METALS",
    "XLE": "ENERGY", "USO": "ENERGY", "XOP": "ENERGY",
    "BITO": "CRYPTO", "MSTR": "CRYPTO",
}


def _get_cluster(ticker: str) -> str:
    return CLUSTER_MAP.get(ticker, "PORTFOLIO")


@dataclass
class HeatmapCell:
    ticker: str
    heat_score: int
    deployable_convexity: float
    recommended_structure: str
    governor_status: str
    badges: list[str]
    cluster_key: str


class HeatmapEngine:
    """
    Produces the MONOS opportunity heatmap from scanner + governor outputs.
    """

    def _compute_heat_score(self, opp_score: int, convexity_score: int,
                            gov_status: str) -> int:
        raw = opp_score + 0.25 * (convexity_score - 50)
        if gov_status == "BLOCKED":
            raw *= 0.4
        return int(min(100, max(0, round(raw))))

    def _assign_badges(self, scan: ScanResult, gov: GovernorDecision) -> list[str]:
        badges = []
        if scan.opportunity_score >= 80:
            badges.append("HIGH_CONVICTION")
        if scan.gamma_state == "POSITIVE":
            badges.append("GEX_ALIGNED")
        if scan.vol_regime == "COMPRESSED":
            badges.append("VOL_CHEAP")
        if gov.status == "APPROVED":
            badges.append("GOVERNOR_APPROVED")
        if gov.status == "BLOCKED":
            badges.append("GOVERNOR_BLOCKED")
        if scan.iv_rank < 30:
            badges.append("LOW_IV_RANK")
        return badges

    def generate(self,
                 candidates: list[ScanResult],
                 structures: list[Structure],
                 decisions: list[GovernorDecision],
                 scanner_run_id: str | None = None) -> list[HeatmapCell]:
        """
        Generate the heatmap from scanner/builder/governor outputs.
        """
        logger.info("=== Heatmap engine started (%d candidates) ===",
                     len(candidates))

        # Index structures and decisions by ticker
        struct_map = {s.ticker: s for s in structures}
        decision_map = {d.ticker: d for d in decisions}

        cells: list[HeatmapCell] = []

        for scan in candidates:
            struct = struct_map.get(scan.ticker)
            decision = decision_map.get(scan.ticker)
            if not struct or not decision:
                continue

            heat = self._compute_heat_score(
                scan.opportunity_score,
                struct.convexity_score,
                decision.status,
            )
            badges = self._assign_badges(scan, decision)
            deploy_convex = struct.convexity_score / 100.0

            cell = HeatmapCell(
                ticker=scan.ticker,
                heat_score=heat,
                deployable_convexity=round(deploy_convex, 4),
                recommended_structure=struct.structure_type,
                governor_status=decision.status,
                badges=badges,
                cluster_key=_get_cluster(scan.ticker),
            )
            cells.append(cell)

        cells.sort(key=lambda c: c.heat_score, reverse=True)

        # Persist
        run_row = {
            "scanner_run_id": scanner_run_id,
            "as_of_ts": datetime.utcnow().isoformat(),
            "universe_name": "MONOS_DEFAULT",
        }
        run_id = write_heatmap_run(run_row)

        cell_rows = [{
            "heatmap_run_id": run_id,
            "ticker": c.ticker,
            "heat_score": c.heat_score,
            "deployable_convexity": c.deployable_convexity,
            "recommended_structure": c.recommended_structure,
            "governor_status": c.governor_status,
            "badges": c.badges,
            "cluster_key": c.cluster_key,
        } for c in cells]

        write_heatmap_cells(cell_rows)

        write_agent_log(AGENT, "generate", "success", {
            "cells": len(cells),
            "clusters": list({c.cluster_key for c in cells}),
            "top_ticker": cells[0].ticker if cells else None,
            "top_heat_score": cells[0].heat_score if cells else 0,
        })

        logger.info("Heatmap: %d cells across %d clusters",
                     len(cells), len({c.cluster_key for c in cells}))
        logger.info("=== Heatmap engine complete ===")
        return cells
