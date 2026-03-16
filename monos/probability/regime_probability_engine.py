"""
regime_probability_engine.py

Estimates the probability of major market paths using the full MONOS
signal stack.  Transparent weighted model — no black-box ML.

Output States (per ticker)
--------------------------
    BULLISH_CONTINUATION    — trend persists higher
    CORRECTIVE_PULLBACK     — near-term retracement within trend
    DEEPER_BREAKDOWN        — structural break / trend reversal
    VOL_EXPANSION           — implied vol rising regime
    VOL_COMPRESSION         — implied vol falling / mean-reverting
    CONVEXITY_TRIGGER       — option convexity inflection point

Primary Regime State
--------------------
    BULLISH_CONTINUATION | CORRECTIVE_PULLBACK | BREAKDOWN_RISK |
    VOL_EXPANSION | VOL_COMPRESSION | TRANSITION

Each probability 0–100.  Confidence 0–100.
"""

import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime

from monos.storage.supabase_client import get_client, write_agent_log
from monos.storage.probability_repository import write_probabilities

logger = logging.getLogger(__name__)

AGENT = "regime_probability_engine"


# =====================================================================
# Data container
# =====================================================================

@dataclass
class ProbabilityInputs:
    """Aggregated feature vector for one ticker from all MONOS sources."""
    ticker: str

    # Dealer positioning
    gamma_regime: str = "NEUTRAL"
    gamma_flip_distance: float = 0.0     # % distance from flip
    call_wall_distance: float = 0.0      # % distance from call wall
    put_wall_distance: float = 0.0       # % distance from put wall

    # Vol surface
    iv_rank: float = 50.0
    skew_slope: float = 0.0
    term_structure: float = 1.0
    iv_rv_gap: float = 0.0

    # Scanner / conviction
    opportunity_score: int = 50
    convexity_score: int = 50
    thesis_health: str = "NEUTRAL"       # CONFIRMED / WEAK / NEUTRAL

    # Flow
    flow_signal: str = "NEUTRAL"         # BULLISH_FLOW / BEARISH_FLOW / NEUTRAL
    call_put_ratio: float = 1.0

    # Risk overlay
    macro_regime: str = "NEUTRAL"        # RISK_ON / RISK_OFF / NEUTRAL
    rotation_state: str = "NEUTRAL"      # OFFENSIVE / DEFENSIVE / NEUTRAL
    complexity_index: int = 0

    # Governor
    governor_status: str = "PENDING"     # APPROVED / BLOCKED / CONDITIONAL


@dataclass
class RegimeProbability:
    """Probability output for one ticker."""
    ticker: str
    primary_regime_state: str
    bullish_continuation_prob: int
    corrective_pullback_prob: int
    deeper_breakdown_prob: int
    vol_expansion_prob: int
    vol_compression_prob: int
    convexity_trigger_prob: int
    confidence_score: int
    reason_codes: list[str] = field(default_factory=list)


# =====================================================================
# Helper: clamp a raw score to 0-100
# =====================================================================

def _clamp(v: float) -> int:
    return int(max(0, min(100, round(v))))


# =====================================================================
# Feature loaders
# =====================================================================

def load_probability_inputs(tickers: list[str]) -> dict[str, ProbabilityInputs]:
    """
    Load all feature data from MONOS Supabase tables and assemble
    a ProbabilityInputs vector per ticker.
    """
    sb = get_client()
    inputs: dict[str, ProbabilityInputs] = {}

    for t in tickers:
        inputs[t] = ProbabilityInputs(ticker=t)

    # ── Dealer positioning ────────────────────────────────────────
    try:
        dp_rows = sb.table("dealer_positioning").select("*") \
            .order("timestamp", desc=True).limit(100).execute().data
        seen = set()
        for r in dp_rows:
            tk = r.get("ticker")
            if tk in inputs and tk not in seen:
                seen.add(tk)
                inp = inputs[tk]
                inp.gamma_regime = r.get("gamma_regime", "NEUTRAL")
                flip = float(r.get("gamma_flip", 0) or 0)
                cwall = float(r.get("call_wall", 0) or 0)
                pwall = float(r.get("put_wall", 0) or 0)
                # Distances as % — we'll estimate spot from flip midpoint
                spot_est = flip if flip > 0 else 100
                inp.gamma_flip_distance = 0.0  # at flip by default
                if cwall > 0 and spot_est > 0:
                    inp.call_wall_distance = (cwall - spot_est) / spot_est * 100
                if pwall > 0 and spot_est > 0:
                    inp.put_wall_distance = (spot_est - pwall) / spot_est * 100
    except Exception:
        logger.warning("Could not load dealer_positioning")

    # ── Vol surface ───────────────────────────────────────────────
    try:
        vol_rows = sb.table("vol_surface").select("*") \
            .order("timestamp", desc=True).limit(100).execute().data
        seen = set()
        for r in vol_rows:
            tk = r.get("ticker")
            if tk in inputs and tk not in seen:
                seen.add(tk)
                inp = inputs[tk]
                inp.iv_rank = float(r.get("iv_rank", 50) or 50)
                inp.skew_slope = float(r.get("skew_slope", 0) or 0)
                inp.term_structure = float(r.get("term_structure", 1) or 1)
                inp.iv_rv_gap = float(r.get("iv_rv_gap", 0) or 0)
    except Exception:
        logger.warning("Could not load vol_surface")

    # ── Scanner candidates ────────────────────────────────────────
    try:
        sc_rows = sb.table("scanner_candidates").select("*") \
            .order("created_at", desc=True).limit(100).execute().data
        seen = set()
        for r in sc_rows:
            tk = r.get("ticker")
            if tk in inputs and tk not in seen:
                seen.add(tk)
                inp = inputs[tk]
                inp.opportunity_score = int(r.get("opportunity_score", 50) or 50)
                inp.thesis_health = r.get("thesis_health", "NEUTRAL") or "NEUTRAL"
                inp.complexity_index = int(r.get("complexity_index", 0) or 0)
                inp.gamma_regime = r.get("gamma_state", inp.gamma_regime) or inp.gamma_regime
                overlay = r.get("risk_overlay") or {}
                if isinstance(overlay, dict):
                    inp.macro_regime = overlay.get("macro_regime", inp.macro_regime)
                    inp.rotation_state = overlay.get("rotation_state", inp.rotation_state)
    except Exception:
        logger.warning("Could not load scanner_candidates")

    # ── Structure library ─────────────────────────────────────────
    try:
        st_rows = sb.table("scanner_structure_library").select("*") \
            .order("created_at", desc=True).limit(100).execute().data
        seen = set()
        for r in st_rows:
            tk = r.get("ticker")
            if tk in inputs and tk not in seen:
                seen.add(tk)
                inp = inputs[tk]
                inp.convexity_score = int(r.get("convexity_score", 50) or 50)
                inp.governor_status = r.get("governor_status", "PENDING") or "PENDING"
    except Exception:
        logger.warning("Could not load scanner_structure_library")

    # ── Flow snapshots ────────────────────────────────────────────
    try:
        fl_rows = sb.table("flow_snapshots").select("*") \
            .order("timestamp", desc=True).limit(100).execute().data
        seen = set()
        for r in fl_rows:
            tk = r.get("ticker")
            if tk in inputs and tk not in seen:
                seen.add(tk)
                inp = inputs[tk]
                inp.flow_signal = r.get("flow_signal", "NEUTRAL") or "NEUTRAL"
                inp.call_put_ratio = float(r.get("call_put_ratio", 1) or 1)
    except Exception:
        logger.warning("Could not load flow_snapshots")

    logger.info("Loaded probability inputs for %d tickers", len(inputs))
    return inputs


# =====================================================================
# Scoring functions — each returns 0-100
# =====================================================================

def score_bullish_continuation(inp: ProbabilityInputs) -> int:
    """
    Bullish Continuation Probability
        25% thesis_health
        20% dealer_positioning
        15% trigger_score (convexity_score proxy)
        15% vol_surface_favorability
        15% rotation_alignment
        10% flow_confirmation
    """
    # Thesis health: CONFIRMED=85, NEUTRAL=50, WEAK=20
    thesis = {"CONFIRMED": 85, "NEUTRAL": 50, "WEAK": 20}.get(
        inp.thesis_health, 50)

    # Dealer: POSITIVE gamma = bullish continuation friendly
    dealer = 75 if inp.gamma_regime == "POSITIVE" else 30
    # Bonus if near call wall (magnet effect)
    if inp.call_wall_distance < 3:
        dealer += 10

    # Convexity score as trigger proxy
    trigger = min(100, inp.convexity_score)

    # Vol surface: low IV rank = room to run, narrow skew = less fear
    vol_fav = max(0, 100 - inp.iv_rank)  # low rank = favorable
    if inp.skew_slope < 0.1:
        vol_fav += 10

    # Rotation: OFFENSIVE = bullish
    rotation = {"OFFENSIVE": 80, "NEUTRAL": 50, "DEFENSIVE": 20}.get(
        inp.rotation_state, 50)

    # Flow: bullish flow confirms
    flow = {"BULLISH_FLOW": 80, "NEUTRAL": 45, "BEARISH_FLOW": 15}.get(
        inp.flow_signal, 45)

    raw = (0.25 * thesis + 0.20 * dealer + 0.15 * trigger +
           0.15 * vol_fav + 0.15 * rotation + 0.10 * flow)
    return _clamp(raw)


def score_corrective_pullback(inp: ProbabilityInputs) -> int:
    """
    Corrective Pullback Probability
        30% price below gamma flip (gamma_flip_distance)
        20% weakening thesis
        20% vol expansion signal
        15% negative flow
        15% high complexity
    """
    # Below gamma flip → higher pullback risk
    below_flip = 70 if inp.gamma_regime == "NEGATIVE" else 25
    if inp.gamma_flip_distance < -2:
        below_flip += 15

    # Weak thesis
    thesis_weak = {"WEAK": 80, "NEUTRAL": 45, "CONFIRMED": 15}.get(
        inp.thesis_health, 45)

    # Vol expansion: high IV-RV gap + elevated term structure = stress
    vol_exp = 0
    if inp.iv_rv_gap > 0.3:
        vol_exp += 50
    if inp.term_structure > 1.1:
        vol_exp += 30
    vol_exp = min(100, vol_exp)

    # Negative flow
    neg_flow = {"BEARISH_FLOW": 80, "NEUTRAL": 40, "BULLISH_FLOW": 10}.get(
        inp.flow_signal, 40)

    # Complexity
    complexity = min(100, inp.complexity_index * 2)

    raw = (0.30 * below_flip + 0.20 * thesis_weak + 0.20 * vol_exp +
           0.15 * neg_flow + 0.15 * complexity)
    return _clamp(raw)


def score_deeper_breakdown(inp: ProbabilityInputs) -> int:
    """
    Deeper Breakdown Probability
        30% spot below put wall
        20% trigger failure (low convexity score)
        20% thesis contradiction
        15% rotation defensive
        15% high complexity
    """
    # Near or below put wall
    below_put = 20
    if inp.put_wall_distance < 0:
        below_put = 85
    elif inp.put_wall_distance < 2:
        below_put = 60

    # Trigger failure: low convexity score
    trigger_fail = max(0, 100 - inp.convexity_score)

    # Thesis contradiction
    thesis_contra = {"WEAK": 75, "NEUTRAL": 40, "CONFIRMED": 10}.get(
        inp.thesis_health, 40)

    # Defensive rotation
    rotation_def = {"DEFENSIVE": 80, "NEUTRAL": 40, "OFFENSIVE": 10}.get(
        inp.rotation_state, 40)

    # Complexity
    complexity = min(100, inp.complexity_index * 2)

    raw = (0.30 * below_put + 0.20 * trigger_fail + 0.20 * thesis_contra +
           0.15 * rotation_def + 0.15 * complexity)
    return _clamp(raw)


def score_vol_expansion(inp: ProbabilityInputs) -> int:
    """
    Vol Expansion Probability
        35% compressed vol regime (low IV rank)
        20% low IV rank → room to expand
        15% gamma instability (negative gamma)
        15% event proximity (high term structure = backwardation)
        15% flow imbalance
    """
    # Compressed vol: IV rank < 30 → high expansion probability
    compressed = max(0, 100 - inp.iv_rank * 1.5)

    # Low IV rank (redundant but different weighting emphasis)
    low_rank = max(0, 80 - inp.iv_rank)

    # Gamma instability: negative gamma = dealer hedging amplifies moves
    gamma_inst = 70 if inp.gamma_regime == "NEGATIVE" else 25

    # Term structure backwardation (> 1.05) = near-term stress
    event_prox = min(100, max(0, (inp.term_structure - 0.95) * 200))

    # Flow imbalance: extreme C/P ratio in either direction
    cp_deviation = abs(inp.call_put_ratio - 1.0)
    flow_imbal = min(100, cp_deviation * 80)

    raw = (0.35 * compressed + 0.20 * low_rank + 0.15 * gamma_inst +
           0.15 * event_prox + 0.15 * flow_imbal)
    return _clamp(raw)


def score_vol_compression(inp: ProbabilityInputs) -> int:
    """
    Vol Compression Probability
        35% elevated IV rank → mean-reversion downward
        20% positive gamma (dealer dampening)
        15% stable thesis
        15% low realized vol (IV-RV gap large = overpriced)
        15% narrowing term structure
    """
    # Elevated rank → compression likely
    elevated = min(100, inp.iv_rank * 1.2)

    # Positive gamma: dealer long gamma dampens vol
    pos_gamma = 75 if inp.gamma_regime == "POSITIVE" else 30

    # Stable thesis
    stable = {"CONFIRMED": 80, "NEUTRAL": 50, "WEAK": 20}.get(
        inp.thesis_health, 50)

    # High IV-RV gap = IV overpriced → likely to compress
    overpriced = min(100, max(0, inp.iv_rv_gap * 80))

    # Narrowing term structure (< 1.0 = contango, normal)
    narrow = max(0, 100 - (inp.term_structure - 0.9) * 150)

    raw = (0.35 * elevated + 0.20 * pos_gamma + 0.15 * stable +
           0.15 * overpriced + 0.15 * narrow)
    return _clamp(raw)


def score_convexity_trigger(inp: ProbabilityInputs) -> int:
    """
    Convexity Trigger Probability
        30% trigger_score (convexity_score)
        25% gamma flip proximity
        20% support proximity (put wall distance as proxy)
        15% flow shift
        10% structure momentum (opportunity_score)
    """
    # Convexity score directly
    trigger = min(100, inp.convexity_score)

    # Gamma flip proximity: within 2% of flip → high trigger probability
    flip_prox = max(0, 100 - abs(inp.gamma_flip_distance) * 15)

    # Support proximity: near put wall → convexity zone
    support = max(0, 100 - abs(inp.put_wall_distance) * 10)

    # Flow shift: extreme flow signals = inflection
    flow_shift = 50
    if inp.flow_signal == "BULLISH_FLOW" and inp.call_put_ratio > 1.5:
        flow_shift = 80
    elif inp.flow_signal == "BEARISH_FLOW" and inp.call_put_ratio < 0.7:
        flow_shift = 75

    # Structure momentum
    struct_mom = min(100, inp.opportunity_score)

    raw = (0.30 * trigger + 0.25 * flip_prox + 0.20 * support +
           0.15 * flow_shift + 0.10 * struct_mom)
    return _clamp(raw)


# =====================================================================
# State selection
# =====================================================================

def select_primary_regime_state(
        bull: int, pullback: int, breakdown: int,
        vol_exp: int, vol_comp: int, trigger: int) -> str:
    """
    Select the primary regime state from the 6 probability scores.

    Logic:
        1. If breakdown > 65  → BREAKDOWN_RISK
        2. If pullback > bull and pullback > 55  → CORRECTIVE_PULLBACK
        3. If vol_exp > 70 and vol_exp > vol_comp  → VOL_EXPANSION
        4. If vol_comp > 70 and vol_comp > vol_exp → VOL_COMPRESSION
        5. If bull > 60  → BULLISH_CONTINUATION
        6. Otherwise → TRANSITION
    """
    if breakdown > 65:
        return "BREAKDOWN_RISK"
    if pullback > bull and pullback > 55:
        return "CORRECTIVE_PULLBACK"
    if vol_exp > 70 and vol_exp > vol_comp:
        return "VOL_EXPANSION"
    if vol_comp > 70 and vol_comp > vol_exp:
        return "VOL_COMPRESSION"
    if bull > 60:
        return "BULLISH_CONTINUATION"
    return "TRANSITION"


# =====================================================================
# Reason codes
# =====================================================================

def build_reason_codes(inp: ProbabilityInputs,
                       bull: int, pullback: int, breakdown: int,
                       vol_exp: int, vol_comp: int, trigger: int) -> list[str]:
    """
    Generate human-readable reason codes that explain the probability output.
    """
    codes = []

    # Gamma
    if inp.gamma_regime == "NEGATIVE":
        codes.append("NEGATIVE_GAMMA")
    elif inp.gamma_regime == "POSITIVE":
        codes.append("POSITIVE_GAMMA")

    if abs(inp.gamma_flip_distance) < 2:
        codes.append("NEAR_GAMMA_FLIP")

    if inp.put_wall_distance < 2:
        codes.append("NEAR_PUT_WALL")
    if inp.call_wall_distance < 2:
        codes.append("NEAR_CALL_WALL")

    # Thesis
    if inp.thesis_health == "CONFIRMED":
        codes.append("THESIS_STRONG")
    elif inp.thesis_health == "WEAK":
        codes.append("THESIS_WEAK")

    # Vol
    if inp.iv_rank < 25:
        codes.append("LOW_IV_RANK")
    elif inp.iv_rank > 75:
        codes.append("HIGH_IV_RANK")

    if inp.iv_rv_gap > 0.5:
        codes.append("IV_OVERPRICED")
    elif inp.iv_rv_gap < -0.2:
        codes.append("IV_UNDERPRICED")

    if inp.term_structure > 1.15:
        codes.append("BACKWARDATION")
    elif inp.term_structure < 0.9:
        codes.append("CONTANGO")

    if inp.skew_slope > 0.3:
        codes.append("STEEP_SKEW")

    # Flow
    if inp.flow_signal == "BULLISH_FLOW":
        codes.append("BULLISH_FLOW")
    elif inp.flow_signal == "BEARISH_FLOW":
        codes.append("BEARISH_FLOW")

    # Trigger
    if trigger > 70:
        codes.append("TRIGGER_ACTIVE")
    elif trigger > 50:
        codes.append("TRIGGER_WARMING")

    # Governor
    if inp.governor_status == "BLOCKED":
        codes.append("GOVERNOR_BLOCKED")
    elif inp.governor_status == "APPROVED":
        codes.append("GOVERNOR_APPROVED")

    # Complexity
    if inp.complexity_index > 50:
        codes.append("HIGH_COMPLEXITY")

    # Macro
    if inp.macro_regime == "RISK_OFF":
        codes.append("MACRO_RISK_OFF")
    elif inp.macro_regime == "RISK_ON":
        codes.append("MACRO_RISK_ON")

    return codes


# =====================================================================
# Confidence calculation
# =====================================================================

def _compute_confidence(probs: dict[str, int], inputs: ProbabilityInputs) -> int:
    """
    Confidence score (0-100) based on:
        - Signal agreement (if dominant state is far ahead of second)
        - Input completeness (how many signals are non-default)
    """
    sorted_probs = sorted(probs.values(), reverse=True)
    spread = sorted_probs[0] - sorted_probs[1] if len(sorted_probs) > 1 else 0

    # Signal agreement: big spread = high confidence
    agreement = min(50, spread)

    # Input quality: non-default signals
    quality = 0
    if inputs.gamma_regime != "NEUTRAL":
        quality += 8
    if inputs.thesis_health != "NEUTRAL":
        quality += 8
    if inputs.flow_signal != "NEUTRAL":
        quality += 7
    if inputs.macro_regime != "NEUTRAL":
        quality += 7
    if inputs.iv_rank != 50:
        quality += 5
    if inputs.convexity_score != 50:
        quality += 5
    if inputs.opportunity_score != 50:
        quality += 5
    if inputs.skew_slope != 0:
        quality += 5
    quality = min(50, quality)

    return _clamp(agreement + quality)


# =====================================================================
# Persistence
# =====================================================================

def persist_probabilities(results: list[RegimeProbability]) -> int:
    """Write regime probabilities to Supabase."""
    rows = [{
        "ticker": r.ticker,
        "primary_regime_state": r.primary_regime_state,
        "bullish_continuation_prob": r.bullish_continuation_prob,
        "corrective_pullback_prob": r.corrective_pullback_prob,
        "deeper_breakdown_prob": r.deeper_breakdown_prob,
        "vol_expansion_prob": r.vol_expansion_prob,
        "vol_compression_prob": r.vol_compression_prob,
        "convexity_trigger_prob": r.convexity_trigger_prob,
        "confidence_score": r.confidence_score,
        "reason_codes": r.reason_codes,
    } for r in results]
    return write_probabilities(rows)


# =====================================================================
# Main engine
# =====================================================================

class RegimeProbabilityEngine:
    """
    Estimates probability of major market paths using the full MONOS
    signal stack.  Transparent weighted model.
    """

    def compute(self, inp: ProbabilityInputs) -> RegimeProbability:
        """Compute all probabilities for a single ticker."""

        bull     = score_bullish_continuation(inp)
        pullback = score_corrective_pullback(inp)
        breakdown = score_deeper_breakdown(inp)
        vol_exp  = score_vol_expansion(inp)
        vol_comp = score_vol_compression(inp)
        trigger  = score_convexity_trigger(inp)

        primary = select_primary_regime_state(
            bull, pullback, breakdown, vol_exp, vol_comp, trigger)

        reasons = build_reason_codes(
            inp, bull, pullback, breakdown, vol_exp, vol_comp, trigger)

        probs = {
            "bull": bull, "pullback": pullback, "breakdown": breakdown,
            "vol_exp": vol_exp, "vol_comp": vol_comp,
        }
        confidence = _compute_confidence(probs, inp)

        return RegimeProbability(
            ticker=inp.ticker,
            primary_regime_state=primary,
            bullish_continuation_prob=bull,
            corrective_pullback_prob=pullback,
            deeper_breakdown_prob=breakdown,
            vol_expansion_prob=vol_exp,
            vol_compression_prob=vol_comp,
            convexity_trigger_prob=trigger,
            confidence_score=confidence,
            reason_codes=reasons,
        )

    def run(self, tickers: list[str]) -> list[RegimeProbability]:
        """
        Load inputs from Supabase, compute probabilities, persist, and
        print report.
        """
        logger.info("=== Regime probability engine started (%d tickers) ===",
                     len(tickers))

        # Load all feature data
        all_inputs = load_probability_inputs(tickers)

        results: list[RegimeProbability] = []
        for ticker in tickers:
            inp = all_inputs.get(ticker)
            if not inp:
                continue
            try:
                rp = self.compute(inp)
                results.append(rp)
                logger.info("%s: %s (bull=%d pull=%d break=%d vExp=%d "
                            "vComp=%d trigger=%d conf=%d)",
                            ticker, rp.primary_regime_state,
                            rp.bullish_continuation_prob,
                            rp.corrective_pullback_prob,
                            rp.deeper_breakdown_prob,
                            rp.vol_expansion_prob,
                            rp.vol_compression_prob,
                            rp.convexity_trigger_prob,
                            rp.confidence_score)
            except Exception:
                logger.exception("Failed to compute probabilities for %s", ticker)

        # Persist
        persist_probabilities(results)

        # Agent log
        write_agent_log(AGENT, "run", "success", {
            "tickers": len(results),
            "states": {r.ticker: r.primary_regime_state for r in results},
        })

        # Print report
        print()
        print("Regime Probability Report")
        print("=" * 100)
        print(f"  {'Ticker':<8} {'State':<24} {'Bull':>5} {'Pull':>5} "
              f"{'Break':>5} {'VExp':>5} {'VComp':>5} {'Trig':>5} "
              f"{'Conf':>5}  Reasons")
        print(f"  {'------':<8} {'-----':<24} {'----':>5} {'----':>5} "
              f"{'-----':>5} {'----':>5} {'-----':>5} {'----':>5} "
              f"{'----':>5}  -------")
        for r in sorted(results, key=lambda x: x.confidence_score, reverse=True):
            reasons_str = ", ".join(r.reason_codes[:4])
            print(f"  {r.ticker:<8} {r.primary_regime_state:<24} "
                  f"{r.bullish_continuation_prob:>5} "
                  f"{r.corrective_pullback_prob:>5} "
                  f"{r.deeper_breakdown_prob:>5} "
                  f"{r.vol_expansion_prob:>5} "
                  f"{r.vol_compression_prob:>5} "
                  f"{r.convexity_trigger_prob:>5} "
                  f"{r.confidence_score:>5}  {reasons_str}")
        print()

        logger.info("=== Regime probability engine complete ===")
        return results


# ── CLI ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(levelname)s %(message)s")

    tickers = sys.argv[1:] if len(sys.argv) > 1 else ["SPY", "QQQ", "GLD", "SLV", "GDX"]
    engine = RegimeProbabilityEngine()
    engine.run(tickers)
