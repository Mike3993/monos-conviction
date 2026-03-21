"""
MONOS Trade Dialogue Generator
-------------------------------
Converts raw trade data into operator-friendly reasoning narratives.
Explains WHY each trade was selected, structured, and sized —
plus what was rejected and why.

Pure reporting layer — does not modify any trade logic.
"""

from __future__ import annotations
from typing import Any


# ── structure rationale map ──────────────────────────────────────────

_STRUCTURE_WHY = {
    "LONG_CALL":    "Naked call selected for full upside convexity — no cap on gains.",
    "LONG_PUT":     "Naked put selected for full downside convexity — maximum payoff on selloff.",
    "CALL_SPREAD":  "Vertical call spread selected for defined risk — capped gain but lower cost basis.",
    "PUT_SPREAD":   "Vertical put spread selected for defined risk — capped gain with controlled downside.",
}

_MODE_WHY = {
    "TACTICAL":        "TACTICAL mode — short-duration, high-probability, defined-risk trades on liquid indices.",
    "HYBRID":          "HYBRID mode — sector ETF with moderate volatility, blending spread and naked strategies.",
    "CONVEX":          "CONVEX mode — high-volatility asset with outsized move potential, full payoff exposure.",
    "MEAN_REVERSION":  "MEAN_REVERSION mode — extended price detected in neutral regime, snap-back expected.",
}

_HOLD_WHY = {
    "TACTICAL":        "Hold 1-3 days — short duration captures momentum edge before theta erodes value.",
    "HYBRID":          "Hold 5-10 days — moderate duration balances theta decay against sector trend persistence.",
    "CONVEX":          "Hold 10-20 days — longer duration gives the convex move time to develop.",
    "MEAN_REVERSION":  "Hold 1-3 days — quick exit expected as price reverts to mean.",
}


# ── filter descriptions ──────────────────────────────────────────────

def _describe_filters(trade: dict[str, Any], batch_row: dict[str, Any] | None = None) -> list[str]:
    """Build a list of filter-status descriptions for the trade."""
    lines = []
    conf = trade.get("confidence") or trade.get("win_rate", 0)
    mode = trade.get("mode", "TACTICAL")

    # Confidence
    if conf >= 75:
        lines.append(f"Confidence {conf:.0f}% — HIGH conviction, above the 60% threshold for full sizing.")
    elif conf >= 60:
        lines.append(f"Confidence {conf:.0f}% — solid conviction, meets minimum for {mode} mode.")
    elif conf >= 45:
        lines.append(f"Confidence {conf:.0f}% — moderate conviction, acceptable for mean-reversion setups.")
    else:
        lines.append(f"Confidence {conf:.0f}% — below standard thresholds, reduced sizing applied.")

    # MSA
    msa = trade.get("msa_state", "")
    direction = trade.get("direction", "LONG")
    if msa:
        if msa == "MSA_BULLISH" and direction == "LONG":
            lines.append("MSA BULLISH — regime aligned with long direction. Position size boosted 1.5x.")
        elif msa == "MSA_BEARISH" and direction == "SHORT":
            lines.append("MSA BEARISH — regime aligned with short direction. Position size boosted 1.5x.")
        elif msa == "MSA_NEUTRAL":
            lines.append("MSA NEUTRAL — no strong regime bias. Mean-reversion overlay eligible.")
        elif msa == "MSA_BULLISH" and direction == "SHORT":
            lines.append("MSA BULLISH vs SHORT signal — normally filtered, but override conditions met.")
        elif msa == "MSA_BEARISH" and direction == "LONG":
            lines.append("MSA BEARISH vs LONG signal — normally filtered, but override conditions met.")
    else:
        lines.append("MSA state not captured — regime filter not applied to this trade.")

    # Mode-specific filter notes
    if mode == "MEAN_REVERSION":
        lines.append("Mean reversion trigger ACTIVE — RSI or 3-day move exceeded reversal threshold.")
        lines.append("Extension filter bypassed — extended move IS the setup for this mode.")
    elif mode == "CONVEX":
        lines.append("Trend filter SKIPPED — CONVEX mode does not require trend confirmation.")
        lines.append("Timing filter SKIPPED — CONVEX mode relies on volatility regime, not entry timing.")
        lines.append("Shock filter ACTIVE — checked prior 2 days for single-day moves > 1.5%.")
    elif mode == "HYBRID":
        lines.append("Trend filter ACTIVE — required short MA > long MA for entry confirmation.")
        lines.append("Timing filter SKIPPED — HYBRID mode bypasses entry timing gate.")
        lines.append("Shock filter ACTIVE — checked prior 2 days for single-day moves > 1.5%.")
    elif mode == "TACTICAL":
        lines.append("All filters ACTIVE — confidence, MSA, extension, and relaxed timing applied.")
        lines.append("Timing filter: close must be within 0.5% of short-term MA.")

    return lines


def _build_rejected(trade: dict[str, Any], all_trades: list[dict[str, Any]] | None = None) -> list[dict[str, str]]:
    """Generate 1-2 rejected alternative trades with reasoning."""
    rejected = []
    mode = trade.get("mode", "TACTICAL")
    direction = trade.get("direction", "LONG")
    ticker = trade.get("ticker", "")

    # Rejection 1: opposite direction
    opp_dir = "SHORT" if direction == "LONG" else "LONG"
    if mode in ("TACTICAL", "HYBRID"):
        rejected.append({
            "alternative": f"{ticker} {opp_dir} (same mode)",
            "reason": f"{opp_dir} signal rejected — combined signal direction was {direction} with higher conviction. Counter-trend trade would fight the primary momentum reading.",
        })
    elif mode == "CONVEX":
        rejected.append({
            "alternative": f"{ticker} {opp_dir} CONVEX",
            "reason": f"{opp_dir} direction rejected — volatility regime favors {direction} bias based on recent price action and dealer positioning.",
        })
    elif mode == "MEAN_REVERSION":
        rejected.append({
            "alternative": f"{ticker} trend-following {direction}",
            "reason": "Trend-following rejected — price is extended beyond 2 standard deviations. Mean-reversion setup has higher expected value at this location.",
        })

    # Rejection 2: alternative structure
    structure = trade.get("structure", "")
    if "SPREAD" in structure:
        rejected.append({
            "alternative": f"{ticker} naked {'call' if direction == 'LONG' else 'put'} instead of spread",
            "reason": f"Naked option rejected for {mode} mode — defined risk (spread) preferred to limit downside. Win rate is higher with capped structures in this regime.",
        })
    elif "LONG_CALL" in structure or "LONG_PUT" in structure:
        rejected.append({
            "alternative": f"{ticker} {'call' if direction == 'LONG' else 'put'} spread instead of naked",
            "reason": f"Spread rejected for {mode} mode — capping gains would eliminate the convex payoff that justifies the trade. Full exposure needed for outsized moves.",
        })

    # Rejection 3: alternative mode (if not already 2)
    if len(rejected) < 2:
        alt_mode = "TACTICAL" if mode != "TACTICAL" else "HYBRID"
        rejected.append({
            "alternative": f"{ticker} in {alt_mode} mode",
            "reason": f"{alt_mode} mode rejected — {mode} mode better matches the current volatility regime and asset behavior profile for {ticker}.",
        })

    return rejected[:2]


# ── main generator ───────────────────────────────────────────────────

def generate_dialogue(
    top_trades: list[dict[str, Any]],
    batch_results: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Generate operator-friendly reasoning narratives for each trade.

    Parameters
    ----------
    top_trades : list[dict]
        Trade recommendations from generate_top_trades().
    batch_results : list[dict] | None
        Full batch results for additional context.

    Returns
    -------
    list[dict]
        Each entry has: ticker, direction, mode, narrative sections.
    """
    dialogues = []

    for trade in top_trades:
        ticker = trade.get("ticker", "???")
        direction = trade.get("direction", "LONG")
        mode = trade.get("mode", "TACTICAL")
        structure = trade.get("structure", "LONG_CALL")
        hold = trade.get("hold", "2d")
        sizing = trade.get("sizing", "MEDIUM")
        wr = trade.get("win_rate", 0)
        wgt = trade.get("weighted_return", 0)
        strength = trade.get("strength", "LOW")

        # Signal breakdown
        signal_breakdown = {
            "confidence": f"{wr:.1f}%",
            "direction": direction,
            "mode": mode,
            "structure": structure,
            "strength": strength,
            "weighted_return": f"{wgt:.2f}%",
        }

        # Filter analysis
        filters = _describe_filters(trade)

        # Decision logic
        decision = {
            "why_selected": f"{ticker} ranked #{trade.get('rank', '?')} by weighted return ({wgt:.2f}%) with {wr:.0f}% win rate. {_MODE_WHY.get(mode, '')}",
            "why_structure": _STRUCTURE_WHY.get(structure, f"{structure} selected based on mode config."),
            "why_hold": _HOLD_WHY.get(mode, f"Hold period {hold} based on mode default."),
            "why_sizing": f"Position size: {sizing}. " + (
                "Full allocation — high conviction setup with aligned regime."
                if sizing == "HIGH" else
                "Standard allocation — solid setup but not peak conviction."
                if sizing == "MEDIUM" else
                "Reduced allocation — lower conviction or unfavorable regime alignment."
            ),
        }

        # Rejected alternatives
        rejected = _build_rejected(trade, batch_results)

        # Build narrative
        narrative_lines = []
        narrative_lines.append(f"TRADE: {ticker} {direction}")
        narrative_lines.append(f"Mode: {mode}")
        narrative_lines.append("")
        narrative_lines.append("SIGNAL BREAKDOWN")
        narrative_lines.append(f"  Confidence: {wr:.1f}%")
        narrative_lines.append(f"  Direction: {direction}")
        narrative_lines.append(f"  Weighted Return: {wgt:.2f}%")
        narrative_lines.append(f"  Signal Strength: {strength}")
        narrative_lines.append("")
        narrative_lines.append("FILTER STATUS")
        for f in filters:
            narrative_lines.append(f"  {f}")
        narrative_lines.append("")
        narrative_lines.append("DECISION LOGIC")
        narrative_lines.append(f"  Selection: {decision['why_selected']}")
        narrative_lines.append(f"  Structure: {decision['why_structure']}")
        narrative_lines.append(f"  Hold: {decision['why_hold']}")
        narrative_lines.append(f"  Sizing: {decision['why_sizing']}")
        narrative_lines.append("")
        narrative_lines.append("REJECTED ALTERNATIVES")
        for r in rejected:
            narrative_lines.append(f"  X {r['alternative']}")
            narrative_lines.append(f"    {r['reason']}")
        narrative_lines.append("")
        narrative_lines.append(f"CONCLUSION: {'High' if strength == 'HIGH' else 'Medium' if strength == 'MEDIUM' else 'Low'} conviction {direction.lower()} opportunity in {mode.lower()} regime.")

        dialogues.append({
            "ticker": ticker,
            "rank": trade.get("rank", 0),
            "direction": direction,
            "mode": mode,
            "structure": structure,
            "signal_breakdown": signal_breakdown,
            "filters": filters,
            "decision": decision,
            "rejected": rejected,
            "narrative": "\n".join(narrative_lines),
        })

    return dialogues
