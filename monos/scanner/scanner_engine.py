"""
scanner_engine.py

Evaluates convex opportunity candidates from the universe.

Pipeline:
    1. Load universe
    2. Load risk overlay
    3. Load vol surface (placeholder — Phase 2)
    4. Calculate opportunity_score per ticker
    5. Filter below threshold
    6. Assign recommended structure type

Opportunity score weights:
    Vol surface      25%
    Momentum         20%
    GEX alignment    15%
    Thesis health    15%
    Symmetry break   15%
    Rotation align   10%

Filter: score < 55 → excluded
"""

import logging
import random
from dataclasses import dataclass, asdict

import yfinance as yf

from monos.risk.risk_overlay_engine import RiskOverlay
from monos.dealer.dealer_positioning_engine import DealerPositioning
from monos.storage.scanner_repository import write_candidates
from monos.storage.supabase_client import write_agent_log

logger = logging.getLogger(__name__)

AGENT = "scanner_engine"
SCORE_THRESHOLD = 55

# ── Score weights ────────────────────────────────────────────────

WEIGHTS = {
    "vol_surface":      0.25,
    "momentum":         0.20,
    "gex_alignment":    0.15,
    "thesis_health":    0.15,
    "symmetry_break":   0.15,
    "rotation_align":   0.10,
}

# ── Structure recommendation rules ──────────────────────────────

def _recommend_structure(gamma_regime: str, vol_regime: str, score: int) -> str:
    """Heuristic structure recommendation based on regime + score."""
    if gamma_regime == "NEGATIVE" and vol_regime in ("ELEVATED", "EXTREME"):
        return "PUT_LADDER"
    if gamma_regime == "NEGATIVE":
        return "VERTICAL_SPREAD"
    if vol_regime == "COMPRESSED" and score >= 75:
        return "CALL_LADDER"
    if vol_regime == "COMPRESSED":
        return "CALENDAR_SPREAD"
    if score >= 80:
        return "CALL_LADDER"
    if score >= 65:
        return "DIAGONAL_SPREAD"
    return "VERTICAL_SPREAD"


@dataclass
class ScanResult:
    ticker: str
    opportunity_score: int
    recommended_structure: str
    gamma_state: str
    vol_regime: str
    iv_rank: float
    thesis_health: str
    complexity_index: int
    risk_overlay: dict
    score_breakdown: dict


class ScannerEngine:
    """
    Scores every ticker in the universe for convex opportunity quality.
    """

    def _get_iv_rank(self, ticker: str) -> float:
        """
        Compute IV rank as percentile of current IV vs. 52-week range.
        Phase-1: approximate via historical volatility.
        """
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="6mo")
            if len(hist) < 20:
                return 50.0
            returns = hist["Close"].pct_change().dropna()
            hv_20 = returns.iloc[-20:].std() * (252 ** 0.5) * 100
            hv_full = returns.std() * (252 ** 0.5) * 100
            if hv_full == 0:
                return 50.0
            rank = min(100, max(0, (hv_20 / hv_full) * 50))
            return round(rank, 1)
        except Exception:
            return 50.0

    def _momentum_score(self, ticker: str) -> float:
        """Score 0-100: how strong is the trend."""
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="60d")
            if len(hist) < 20:
                return 50.0
            sma20 = hist["Close"].iloc[-20:].mean()
            current = hist["Close"].iloc[-1]
            pct_above = ((current - sma20) / sma20) * 100
            return min(100, max(0, 50 + pct_above * 5))
        except Exception:
            return 50.0

    def _compute_score(self, ticker: str,
                       dealer: DealerPositioning | None,
                       overlay: RiskOverlay) -> ScanResult:
        """Score a single ticker."""

        iv_rank = self._get_iv_rank(ticker)
        momentum = self._momentum_score(ticker)

        # Component scores (0-100 each)
        vol_surface_score = max(0, 100 - iv_rank)  # low IV = better entry
        gex_score = 70 if (dealer and dealer.gamma_regime == "POSITIVE") else 40
        thesis_score = random.randint(55, 85)  # Phase 2: real thesis engine
        symmetry_score = random.randint(45, 80)
        rotation_score = 65 if overlay.macro_regime == "RISK_ON" else 35

        breakdown = {
            "vol_surface":   round(vol_surface_score, 1),
            "momentum":      round(momentum, 1),
            "gex_alignment": gex_score,
            "thesis_health": thesis_score,
            "symmetry_break": symmetry_score,
            "rotation_align": rotation_score,
        }

        weighted = sum(breakdown[k] * WEIGHTS[k] for k in WEIGHTS)
        score = round(min(100, max(0, weighted)))

        gamma_state = dealer.gamma_regime if dealer else "UNKNOWN"
        rec_structure = _recommend_structure(gamma_state, overlay.volatility_regime, score)
        thesis = "CONFIRMED" if thesis_score >= 65 else "WEAK"

        return ScanResult(
            ticker=ticker,
            opportunity_score=score,
            recommended_structure=rec_structure,
            gamma_state=gamma_state,
            vol_regime=overlay.volatility_regime,
            iv_rank=iv_rank,
            thesis_health=thesis,
            complexity_index=overlay.complexity_index,
            risk_overlay=asdict(overlay) if hasattr(overlay, '__dataclass_fields__') else {},
            score_breakdown=breakdown,
        )

    def scan(self,
             tickers: list[str],
             dealer_map: dict[str, DealerPositioning],
             overlay: RiskOverlay) -> list[ScanResult]:
        """
        Score all tickers, filter below threshold, return sorted results.
        """
        logger.info("=== Scanner engine started (%d tickers) ===", len(tickers))

        results: list[ScanResult] = []
        filtered = 0

        for ticker in tickers:
            try:
                dealer = dealer_map.get(ticker)
                result = self._compute_score(ticker, dealer, overlay)

                if result.opportunity_score < SCORE_THRESHOLD:
                    filtered += 1
                    logger.debug("%s scored %d — below threshold, skipping",
                                 ticker, result.opportunity_score)
                    continue

                results.append(result)
                logger.info("%s: score=%d struct=%s gamma=%s iv_rank=%.1f",
                            ticker, result.opportunity_score,
                            result.recommended_structure,
                            result.gamma_state, result.iv_rank)
            except Exception:
                logger.exception("Failed to scan %s", ticker)

        results.sort(key=lambda r: r.opportunity_score, reverse=True)

        # Persist
        rows = [asdict(r) for r in results]
        write_candidates(rows)

        write_agent_log(AGENT, "scan", "success", {
            "scanned": len(tickers),
            "passed": len(results),
            "filtered": filtered,
            "top_ticker": results[0].ticker if results else None,
        })

        logger.info("Scanner: %d passed / %d filtered / %d total",
                     len(results), filtered, len(tickers))
        logger.info("=== Scanner engine complete ===")
        return results


# ── CLI ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
    from monos.risk.risk_overlay_engine import RiskOverlayEngine
    overlay = RiskOverlayEngine().build()
    engine = ScannerEngine()
    results = engine.scan(["SPY", "QQQ", "GLD"], {}, overlay)
    for r in results:
        print(f"  {r.ticker}: {r.opportunity_score} → {r.recommended_structure}")
