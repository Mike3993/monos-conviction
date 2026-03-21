"""
scanner_governor_bridge.py

Portfolio Governor — approves, blocks, or conditionally approves
structures based on governance rules.

Output statuses:
    APPROVED    — all rules pass
    BLOCKED     — one or more hard rules fail
    CONDITIONAL — soft rules flagged, needs manual review

Updates scanner.structure_library.governor_status.
"""

import logging
from dataclasses import dataclass

from monos.builder.structure_builder import Structure
from monos.risk.risk_overlay_engine import RiskOverlay
from monos.scenario.scenario_engine import ScenarioResult
from monos.governance.governor_rules import ALL_RULES
from monos.storage.scanner_repository import write_structures, read_structures
from monos.storage.supabase_client import get_client, write_agent_log

logger = logging.getLogger(__name__)

AGENT = "portfolio_governor"

# Rules that produce BLOCKED (vs CONDITIONAL)
HARD_RULES = {"construction", "conviction_floor", "gex_phase"}


@dataclass
class GovernorDecision:
    ticker: str
    structure_type: str
    status: str          # APPROVED / BLOCKED / CONDITIONAL
    passed_rules: list[str]
    failed_rules: list[str]
    reasons: list[str]


class PortfolioGovernor:
    """
    Evaluates every structure against governance rules and
    updates the governor_status field.
    """

    def evaluate(self, structure: Structure,
                 overlay: RiskOverlay,
                 scenarios: list[ScenarioResult] | None = None) -> GovernorDecision:
        """
        Run all rules against a structure. Return decision.
        """
        passed = []
        failed = []
        reasons = []

        for rule_name, rule_fn in ALL_RULES:
            try:
                ok, reason = rule_fn(structure, overlay, scenarios or [])
                if ok:
                    passed.append(rule_name)
                else:
                    failed.append(rule_name)
                    reasons.append(reason)
            except Exception as e:
                failed.append(rule_name)
                reasons.append(f"rule error: {e}")

        # Determine status
        if not failed:
            status = "APPROVED"
        elif any(r in HARD_RULES for r in failed):
            status = "BLOCKED"
        else:
            status = "CONDITIONAL"

        return GovernorDecision(
            ticker=structure.ticker,
            structure_type=structure.structure_type,
            status=status,
            passed_rules=passed,
            failed_rules=failed,
            reasons=reasons,
        )

    def run(self, structures: list[Structure],
            overlay: RiskOverlay,
            scenario_map: dict[str, list[ScenarioResult]] | None = None
            ) -> list[GovernorDecision]:
        """
        Evaluate all structures and persist governor decisions.
        """
        logger.info("=== Portfolio governor started (%d structures) ===",
                     len(structures))

        decisions = []
        status_counts = {"APPROVED": 0, "BLOCKED": 0, "CONDITIONAL": 0}

        for s in structures:
            scenarios = (scenario_map or {}).get(s.ticker, [])
            decision = self.evaluate(s, overlay, scenarios)
            decisions.append(decision)
            status_counts[decision.status] += 1

            logger.info("%s %s → %s (passed=%d failed=%d)",
                        s.ticker, s.structure_type, decision.status,
                        len(decision.passed_rules), len(decision.failed_rules))
            if decision.reasons:
                for r in decision.reasons:
                    logger.info("  reason: %s", r)

        # Update governor_status in Supabase
        # (Match by ticker + structure_type since we don't have IDs yet)
        sb = get_client()
        for s, d in zip(structures, decisions):
            try:
                sb.table("scanner_structure_library") \
                  .update({"governor_status": d.status}) \
                  .eq("ticker", s.ticker) \
                  .eq("structure_type", s.structure_type) \
                  .execute()
            except Exception:
                logger.warning("Could not update governor_status for %s", s.ticker)

        write_agent_log(AGENT, "run", "success", {
            "total": len(decisions),
            **status_counts,
        })

        logger.info("Governor: %s", status_counts)
        logger.info("=== Portfolio governor complete ===")
        return decisions
