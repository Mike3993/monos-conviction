"""
scenario_engine.py

Simulates payoff surfaces across price, volatility, and time dimensions.

Price scenarios:  -20%, -10%, -5%, 0%, +5%, +10%, +20%
Vol scenarios:    -50%, unchanged, +50%
Time scenarios:   7, 30, 60, 90 days remaining

For each combination, computes expected P&L and Greeks-at-scenario.
Results persisted to scanner.scenarios.
"""

import logging
import math
from dataclasses import dataclass
from itertools import product

from scipy.stats import norm

from monos.builder.structure_builder import Structure, Leg
from monos.storage.scanner_repository import write_scenarios
from monos.storage.supabase_client import write_agent_log

logger = logging.getLogger(__name__)

AGENT = "scenario_engine"

PRICE_SCENARIOS  = [-0.20, -0.10, -0.05, 0.0, +0.05, +0.10, +0.20]
VOL_SCENARIOS    = [-0.50, 0.0, +0.50]
TIME_SCENARIOS   = [7, 30, 60, 90]

DEFAULT_IV = 0.25
RISK_FREE  = 0.05


# ── Black-Scholes helpers ─────────────────────────────────────────

def _bs_price(spot: float, strike: float, T: float, iv: float,
              is_call: bool = True) -> float:
    """Black-Scholes option price."""
    if T <= 0 or iv <= 0:
        if is_call:
            return max(0, spot - strike)
        return max(0, strike - spot)

    d1 = (math.log(spot / strike) + (RISK_FREE + 0.5 * iv**2) * T) / (iv * math.sqrt(T))
    d2 = d1 - iv * math.sqrt(T)

    if is_call:
        return spot * norm.cdf(d1) - strike * math.exp(-RISK_FREE * T) * norm.cdf(d2)
    return strike * math.exp(-RISK_FREE * T) * norm.cdf(-d2) - spot * norm.cdf(-d1)


def _bs_delta(spot: float, strike: float, T: float, iv: float,
              is_call: bool = True) -> float:
    if T <= 0 or iv <= 0:
        return 1.0 if (is_call and spot > strike) else 0.0
    d1 = (math.log(spot / strike) + (RISK_FREE + 0.5 * iv**2) * T) / (iv * math.sqrt(T))
    return norm.cdf(d1) if is_call else norm.cdf(d1) - 1


def _bs_gamma(spot: float, strike: float, T: float, iv: float) -> float:
    if T <= 0 or iv <= 0 or spot <= 0:
        return 0.0
    d1 = (math.log(spot / strike) + (RISK_FREE + 0.5 * iv**2) * T) / (iv * math.sqrt(T))
    return norm.pdf(d1) / (spot * iv * math.sqrt(T))


# ── Scenario simulation ──────────────────────────────────────────

@dataclass
class ScenarioResult:
    structure_id: str | None
    ticker: str
    price_scenario_pct: float
    vol_scenario_pct: float
    dte_remaining: int
    expected_pnl: float
    expected_pnl_pct: float
    greeks_at_scenario: dict


class ScenarioEngine:
    """
    Runs a full scenario matrix for each structure.
    """

    def _evaluate_leg(self, leg: Leg, spot_now: float, spot_scenario: float,
                      iv_scenario: float, dte_remaining: int) -> dict:
        """Evaluate a single leg at a scenario point."""
        T = dte_remaining / 365.0
        is_call = leg.option_type == "CALL"
        sign = 1.0 if leg.direction == "LONG" else -1.0

        price_now = _bs_price(spot_now, leg.strike, leg.dte / 365.0,
                              DEFAULT_IV, is_call)
        price_scenario = _bs_price(spot_scenario, leg.strike, T,
                                   iv_scenario, is_call)

        pnl = (price_scenario - price_now) * sign * leg.ratio
        delta = _bs_delta(spot_scenario, leg.strike, T, iv_scenario, is_call) * sign * leg.ratio
        gamma = _bs_gamma(spot_scenario, leg.strike, T, iv_scenario) * sign * leg.ratio

        return {
            "pnl": pnl,
            "delta": delta,
            "gamma": gamma,
        }

    def simulate(self, structure: Structure, spot: float,
                 structure_id: str | None = None) -> list[ScenarioResult]:
        """
        Run the full scenario matrix for one structure.
        """
        results = []

        for price_pct, vol_pct, dte_rem in product(PRICE_SCENARIOS,
                                                     VOL_SCENARIOS,
                                                     TIME_SCENARIOS):
            spot_scenario = spot * (1 + price_pct)
            iv_scenario = max(0.05, DEFAULT_IV * (1 + vol_pct))

            total_pnl = 0.0
            total_delta = 0.0
            total_gamma = 0.0

            for leg in structure.legs:
                ev = self._evaluate_leg(leg, spot, spot_scenario,
                                        iv_scenario, dte_rem)
                total_pnl += ev["pnl"]
                total_delta += ev["delta"]
                total_gamma += ev["gamma"]

            # PnL as % of spot
            pnl_pct = (total_pnl / spot * 100) if spot > 0 else 0.0

            results.append(ScenarioResult(
                structure_id=structure_id,
                ticker=structure.ticker,
                price_scenario_pct=price_pct * 100,
                vol_scenario_pct=vol_pct * 100,
                dte_remaining=dte_rem,
                expected_pnl=round(total_pnl, 4),
                expected_pnl_pct=round(pnl_pct, 4),
                greeks_at_scenario={
                    "delta": round(total_delta, 6),
                    "gamma": round(total_gamma, 6),
                },
            ))

        return results

    def run(self, structures: list[Structure],
            spot_map: dict[str, float]) -> list[ScenarioResult]:
        """
        Simulate all structures and persist results.
        """
        logger.info("=== Scenario engine started (%d structures) ===",
                     len(structures))

        all_results = []
        for s in structures:
            spot = spot_map.get(s.ticker, 100.0)
            results = self.simulate(s, spot)
            all_results.extend(results)
            logger.info("%s %s: %d scenario points",
                        s.ticker, s.structure_type, len(results))

        # Persist
        rows = [{
            "structure_id": r.structure_id,
            "ticker": r.ticker,
            "price_scenario_pct": r.price_scenario_pct,
            "vol_scenario_pct": r.vol_scenario_pct,
            "dte_remaining": r.dte_remaining,
            "expected_pnl": r.expected_pnl,
            "expected_pnl_pct": r.expected_pnl_pct,
            "greeks_at_scenario": r.greeks_at_scenario,
        } for r in all_results]

        write_scenarios(rows)

        write_agent_log(AGENT, "run", "success", {
            "structures": len(structures),
            "scenario_points": len(all_results),
        })

        logger.info("=== Scenario engine complete (%d points) ===",
                     len(all_results))
        return all_results
