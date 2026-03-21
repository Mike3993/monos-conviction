"""
structure_builder.py

Constructs option structures for scanner candidates.

Supported structures:
    CALL_LADDER
    PUT_LADDER
    VERTICAL_SPREAD
    DIAGONAL_SPREAD
    CALENDAR_SPREAD
    BROKEN_WING_BUTTERFLY
    RATIO_SPREAD

Each structure is a list of legs with strike, type, ratio, and tier.
"""

import logging
from dataclasses import dataclass, field, asdict

from monos.builder.strike_selector import select_strikes, TIER_TARGETS
from monos.builder.dte_selector import select_dte
from monos.scanner.scanner_engine import ScanResult
from monos.storage.scanner_repository import write_structures
from monos.storage.supabase_client import write_agent_log

logger = logging.getLogger(__name__)

AGENT = "structure_builder"


@dataclass
class Leg:
    strike: float
    option_type: str     # CALL / PUT
    direction: str       # LONG / SHORT
    ratio: float         # 1.0 for normal, 2.0 for ratio spread
    tier: str            # ITM_ANCHOR / ATM_CORE / OTM_CONVEX / DEEP_OTM
    dte: int


@dataclass
class Structure:
    ticker: str
    structure_type: str
    legs: list[Leg] = field(default_factory=list)
    convexity_score: int = 0
    risk_profile: dict = field(default_factory=dict)
    tier_allocation: dict = field(default_factory=dict)


class StructureBuilder:
    """
    Builds option structures based on scanner recommendations.
    """

    def _build_call_ladder(self, ticker: str, spot: float,
                           iv: float, dte: int) -> Structure:
        """3-leg call ladder: long ATM, long OTM, short DEEP_OTM."""
        strikes = select_strikes(spot, iv, dte, is_call=True)
        legs = [
            Leg(strikes["ATM_CORE"]["strike"], "CALL", "LONG", 1.0, "ATM_CORE", dte),
            Leg(strikes["OTM_CONVEX"]["strike"], "CALL", "LONG", 1.0, "OTM_CONVEX", dte),
            Leg(strikes["DEEP_OTM"]["strike"], "CALL", "SHORT", 2.0, "DEEP_OTM", dte),
        ]
        return Structure(ticker, "CALL_LADDER", legs, tier_allocation={
            t: c["alloc"] for t, c in TIER_TARGETS.items()})

    def _build_put_ladder(self, ticker: str, spot: float,
                          iv: float, dte: int) -> Structure:
        """3-leg put ladder: long ATM, long OTM, short DEEP_OTM."""
        strikes = select_strikes(spot, iv, dte, is_call=False)
        legs = [
            Leg(strikes["ATM_CORE"]["strike"], "PUT", "LONG", 1.0, "ATM_CORE", dte),
            Leg(strikes["OTM_CONVEX"]["strike"], "PUT", "LONG", 1.0, "OTM_CONVEX", dte),
            Leg(strikes["DEEP_OTM"]["strike"], "PUT", "SHORT", 2.0, "DEEP_OTM", dte),
        ]
        return Structure(ticker, "PUT_LADDER", legs, tier_allocation={
            t: c["alloc"] for t, c in TIER_TARGETS.items()})

    def _build_vertical(self, ticker: str, spot: float,
                        iv: float, dte: int, is_call: bool = True) -> Structure:
        """2-leg vertical spread: long ATM, short OTM."""
        strikes = select_strikes(spot, iv, dte, is_call=is_call)
        ot = "CALL" if is_call else "PUT"
        legs = [
            Leg(strikes["ATM_CORE"]["strike"], ot, "LONG", 1.0, "ATM_CORE", dte),
            Leg(strikes["OTM_CONVEX"]["strike"], ot, "SHORT", 1.0, "OTM_CONVEX", dte),
        ]
        return Structure(ticker, "VERTICAL_SPREAD", legs)

    def _build_diagonal(self, ticker: str, spot: float,
                        iv: float, dte: int) -> Structure:
        """2-leg diagonal: long ITM far, short OTM near."""
        strikes_far = select_strikes(spot, iv, dte + 30, is_call=True)
        strikes_near = select_strikes(spot, iv, dte, is_call=True)
        legs = [
            Leg(strikes_far["ITM_ANCHOR"]["strike"], "CALL", "LONG", 1.0,
                "ITM_ANCHOR", dte + 30),
            Leg(strikes_near["OTM_CONVEX"]["strike"], "CALL", "SHORT", 1.0,
                "OTM_CONVEX", dte),
        ]
        return Structure(ticker, "DIAGONAL_SPREAD", legs)

    def _build_calendar(self, ticker: str, spot: float,
                        iv: float, dte: int) -> Structure:
        """2-leg calendar: short near ATM, long far ATM."""
        strikes = select_strikes(spot, iv, dte, is_call=True)
        atm_strike = strikes["ATM_CORE"]["strike"]
        legs = [
            Leg(atm_strike, "CALL", "SHORT", 1.0, "ATM_CORE", dte),
            Leg(atm_strike, "CALL", "LONG", 1.0, "ATM_CORE", dte + 30),
        ]
        return Structure(ticker, "CALENDAR_SPREAD", legs)

    def _build_broken_wing(self, ticker: str, spot: float,
                           iv: float, dte: int) -> Structure:
        """Broken wing butterfly: long ATM, 2x short OTM, long DEEP_OTM."""
        strikes = select_strikes(spot, iv, dte, is_call=True)
        legs = [
            Leg(strikes["ATM_CORE"]["strike"], "CALL", "LONG", 1.0, "ATM_CORE", dte),
            Leg(strikes["OTM_CONVEX"]["strike"], "CALL", "SHORT", 2.0, "OTM_CONVEX", dte),
            Leg(strikes["DEEP_OTM"]["strike"], "CALL", "LONG", 1.0, "DEEP_OTM", dte),
        ]
        return Structure(ticker, "BROKEN_WING_BUTTERFLY", legs)

    def _build_ratio(self, ticker: str, spot: float,
                     iv: float, dte: int) -> Structure:
        """Ratio spread: long 1x ATM, short 2x OTM."""
        strikes = select_strikes(spot, iv, dte, is_call=True)
        legs = [
            Leg(strikes["ATM_CORE"]["strike"], "CALL", "LONG", 1.0, "ATM_CORE", dte),
            Leg(strikes["OTM_CONVEX"]["strike"], "CALL", "SHORT", 2.0, "OTM_CONVEX", dte),
        ]
        return Structure(ticker, "RATIO_SPREAD", legs)

    # ── Dispatch ──────────────────────────────────────────────────

    BUILDERS = {
        "CALL_LADDER":          "_build_call_ladder",
        "PUT_LADDER":           "_build_put_ladder",
        "VERTICAL_SPREAD":      "_build_vertical",
        "DIAGONAL_SPREAD":      "_build_diagonal",
        "CALENDAR_SPREAD":      "_build_calendar",
        "BROKEN_WING_BUTTERFLY":"_build_broken_wing",
        "RATIO_SPREAD":         "_build_ratio",
    }

    def build_structure(self, candidate: ScanResult,
                        spot: float, iv: float = 0.25) -> Structure:
        """
        Build the recommended structure for a scanner candidate.
        """
        dte = select_dte(candidate.gamma_state, candidate.vol_regime)
        method_name = self.BUILDERS.get(candidate.recommended_structure,
                                         "_build_vertical")
        method = getattr(self, method_name)
        structure = method(candidate.ticker, spot, iv, dte)

        # Convexity score heuristic
        otm_legs = [l for l in structure.legs if l.tier in ("OTM_CONVEX", "DEEP_OTM")]
        structure.convexity_score = min(100, candidate.opportunity_score +
                                        len(otm_legs) * 10)

        structure.risk_profile = {
            "max_loss": "defined" if len(structure.legs) >= 3 else "spread_width",
            "gamma_state": candidate.gamma_state,
            "vol_regime": candidate.vol_regime,
        }

        return structure

    def build_all(self, candidates: list[ScanResult],
                  spot_map: dict[str, float]) -> list[Structure]:
        """
        Build structures for all candidates and persist.
        """
        logger.info("=== Structure builder started (%d candidates) ===",
                     len(candidates))

        structures = []
        for c in candidates:
            try:
                spot = spot_map.get(c.ticker, 100.0)
                s = self.build_structure(c, spot)
                structures.append(s)
                logger.info("%s: %s (%d legs) convexity=%d",
                            s.ticker, s.structure_type, len(s.legs),
                            s.convexity_score)
            except Exception:
                logger.exception("Failed to build structure for %s", c.ticker)

        # Persist
        rows = [{
            "ticker": s.ticker,
            "structure_type": s.structure_type,
            "legs": [asdict(l) for l in s.legs],
            "convexity_score": s.convexity_score,
            "risk_profile": s.risk_profile,
            "tier_allocation": s.tier_allocation,
            "governor_status": "PENDING",
        } for s in structures]

        write_structures(rows)

        write_agent_log(AGENT, "build_all", "success", {
            "structures_built": len(structures),
            "types": {s.structure_type: sum(1 for x in structures
                     if x.structure_type == s.structure_type)
                     for s in structures},
        })

        logger.info("=== Structure builder complete ===")
        return structures
