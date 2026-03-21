"""
risk_overlay_engine.py

Combines regime signals into a unified risk overlay state consumed
by the scanner and governor.

Inputs
------
- Macro regime         (from position data / manual)
- Dealer positioning   (from dealer_positioning_engine)
- Volatility regime    (VIX level → COMPRESSED / NORMAL / ELEVATED / EXTREME)
- Credit spreads       (placeholder — Phase 2)
- Rotation state       (placeholder — Phase 2)

Output
------
RiskOverlay dataclass containing all regime flags plus a
composite complexity_index (0-100).
"""

import logging
import math
from dataclasses import dataclass, asdict

import yfinance as yf

from monos.storage.supabase_client import write_agent_log

logger = logging.getLogger(__name__)

AGENT = "risk_overlay_engine"


@dataclass
class RiskOverlay:
    gamma_regime: str          # POSITIVE / NEGATIVE
    volatility_regime: str     # COMPRESSED / NORMAL / ELEVATED / EXTREME
    macro_regime: str          # RISK_ON / RISK_OFF / NEUTRAL
    credit_regime: str         # TIGHT / NORMAL / WIDENING  (placeholder)
    rotation_state: str        # OFFENSIVE / DEFENSIVE / NEUTRAL (placeholder)
    vix_level: float
    complexity_index: int      # 0-100: higher = more complex/risky environment


# ── VIX regime thresholds ─────────────────────────────────────────

VIX_THRESHOLDS = {
    "COMPRESSED": (0, 14),
    "NORMAL":     (14, 20),
    "ELEVATED":   (20, 28),
    "EXTREME":    (28, 100),
}


def _classify_vix(vix: float) -> str:
    for regime, (lo, hi) in VIX_THRESHOLDS.items():
        if lo <= vix < hi:
            return regime
    return "EXTREME"


# ── Complexity index ──────────────────────────────────────────────

COMPLEXITY_WEIGHTS = {
    "NEGATIVE":    25,   # gamma regime
    "ELEVATED":    15,   # vol regime
    "EXTREME":     30,
    "RISK_OFF":    15,   # macro
    "WIDENING":    10,   # credit
    "DEFENSIVE":    5,   # rotation
}


def _compute_complexity(overlay: dict) -> int:
    """Sum penalty points for adverse regime flags."""
    score = 0
    for key, penalty in COMPLEXITY_WEIGHTS.items():
        for v in overlay.values():
            if v == key:
                score += penalty
    return min(score, 100)


class RiskOverlayEngine:
    """
    Assembles a composite risk overlay from multiple regime signals.
    """

    def _get_vix(self) -> float:
        """Fetch VIX level via yfinance."""
        try:
            t = yf.Ticker("^VIX")
            vix = float(t.fast_info.get("lastPrice", 0)
                        or t.fast_info.get("previousClose", 18))
            logger.info("VIX: %.2f", vix)
            return vix if vix > 0 else 18.0
        except Exception:
            logger.warning("VIX fetch failed, defaulting to 18.0")
            return 18.0

    def _determine_macro_regime(self) -> str:
        """
        Phase-1: simple heuristic based on SPY trend.
        Phase-2: incorporate yield curve, PMI, credit spreads.
        """
        try:
            spy = yf.Ticker("SPY")
            hist = spy.history(period="60d")
            if len(hist) < 20:
                return "NEUTRAL"
            sma20 = hist["Close"].iloc[-20:].mean()
            current = hist["Close"].iloc[-1]
            if current > sma20 * 1.01:
                return "RISK_ON"
            elif current < sma20 * 0.99:
                return "RISK_OFF"
            return "NEUTRAL"
        except Exception:
            logger.warning("Macro regime detection failed")
            return "NEUTRAL"

    def build(self, dealer_regimes: dict[str, str] | None = None) -> RiskOverlay:
        """
        Build the composite risk overlay.

        Parameters
        ----------
        dealer_regimes : dict mapping ticker→gamma_regime from dealer engine.
                         The dominant regime across core tickers is used.
        """
        logger.info("=== Risk overlay engine started ===")

        vix = self._get_vix()
        vol_regime = _classify_vix(vix)
        macro = self._determine_macro_regime()

        # Aggregate dealer regime — majority vote
        gamma_regime = "NEUTRAL"
        if dealer_regimes:
            neg = sum(1 for v in dealer_regimes.values() if v == "NEGATIVE")
            pos = sum(1 for v in dealer_regimes.values() if v == "POSITIVE")
            gamma_regime = "NEGATIVE" if neg > pos else "POSITIVE"

        # Placeholders for Phase 2
        credit_regime = "NORMAL"
        rotation_state = "NEUTRAL"

        overlay_dict = {
            "gamma_regime": gamma_regime,
            "volatility_regime": vol_regime,
            "macro_regime": macro,
            "credit_regime": credit_regime,
            "rotation_state": rotation_state,
        }

        complexity = _compute_complexity(overlay_dict)

        overlay = RiskOverlay(
            gamma_regime=gamma_regime,
            volatility_regime=vol_regime,
            macro_regime=macro,
            credit_regime=credit_regime,
            rotation_state=rotation_state,
            vix_level=round(vix, 2),
            complexity_index=complexity,
        )

        logger.info("Risk overlay: gamma=%s vol=%s macro=%s complexity=%d",
                     gamma_regime, vol_regime, macro, complexity)

        write_agent_log(AGENT, "build", "success", asdict(overlay))

        logger.info("=== Risk overlay engine complete ===")
        return overlay


# ── CLI ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
    engine = RiskOverlayEngine()
    ov = engine.build({"SPY": "POSITIVE", "QQQ": "NEGATIVE"})
    print(f"\nRisk Overlay: {asdict(ov)}")
