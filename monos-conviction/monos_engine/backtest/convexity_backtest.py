"""
monos_engine.backtest.convexity_backtest

Convexity Backtest Engine — simulates option structure performance
based on MONOS signals.  Maps underlying returns to approximate
option behaviour per structure type.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import yfinance as yf

from monos_engine.gamma.gex import compute_gex, generate_synthetic_chain
from monos_engine.momentum.momentum import (
    _compute_rsi,
    _compute_return,
    _compute_trend_score,
    _classify_direction,
)
from monos_engine.combiner.signal_combiner import combine_signals
from monos_engine.convexity.structure_engine import select_structure
from monos_engine.mode.mode_engine import classify_mode, get_mode_config, get_asset_hold_override
from monos_engine.msa.msa_engine import get_msa_state_on_date
from monos_engine.options.convex_payoff_engine import estimate_option_return


# ── Option return mapping ────────────────────────────────────────────

def _map_option_return(raw_return_pct: float, structure: str) -> float:
    """Map a raw underlying return (%) to an approximate option return (%).

    Parameters
    ----------
    raw_return_pct : float
        Raw percentage return of the underlying:
        ``((exit - entry) / entry) * 100``.  Positive = price rose.
    structure : str
        LONG_CALL, LONG_PUT, CALL_SPREAD, PUT_SPREAD.

    Returns
    -------
    float
        Approximate option return as a percentage.
    """
    if structure == "LONG_CALL":
        return raw_return_pct * 2.0
    if structure == "LONG_PUT":
        return (-raw_return_pct) * 2.0
    if structure == "CALL_SPREAD":
        return min(raw_return_pct * 1.2, 1.5)
    if structure == "PUT_SPREAD":
        return min((-raw_return_pct) * 1.2, 1.5)
    return 0.0


# ── Internal helpers (shared with backtest_engine) ───────────────────

def _compute_momentum_from_window(ticker: str, closes: np.ndarray) -> dict[str, Any]:
    """Compute momentum signal from a price window."""
    rsi = _compute_rsi(closes, period=14)
    ret_5d = _compute_return(closes, 5)
    ret_20d = _compute_return(closes, 20)
    trend_score = _compute_trend_score(rsi, ret_5d, ret_20d)
    direction = _classify_direction(trend_score)

    if abs(ret_20d) > 0.01:
        velocity = round(ret_5d / ret_20d, 4)
    else:
        velocity = 0.0

    return {
        "ticker": ticker,
        "trend_score": trend_score,
        "velocity": velocity,
        "rsi": rsi,
        "signal_direction": direction,
    }


def _compute_gamma_from_spot(ticker: str, spot: float, seed: int = 42) -> dict[str, Any]:
    """Compute GEX from a synthetic chain at a given spot price."""
    chain = generate_synthetic_chain(spot, seed=seed)
    result = compute_gex(
        spot=spot,
        strikes=chain["strikes"],
        call_gamma=chain["call_gamma"],
        put_gamma=chain["put_gamma"],
        call_oi=chain["call_oi"],
        put_oi=chain["put_oi"],
    )
    result["ticker"] = ticker
    result["spot"] = spot
    return result


# ── RSI proxy for mean-reversion detection ──────────────────────────

def _rsi_proxy(closes: np.ndarray, period: int = 5) -> float:
    """Fast RSI approximation over the last *period* closes.

    Uses a simple average-gain / average-loss ratio — good enough for
    detecting extended conditions without importing extra libraries.
    Returns a value in [0, 100].
    """
    if len(closes) < period + 1:
        return 50.0  # neutral fallback
    deltas = np.diff(closes[-(period + 1):])
    gains = deltas[deltas > 0]
    losses = -deltas[deltas < 0]
    avg_gain = float(np.mean(gains)) if len(gains) > 0 else 0.0
    avg_loss = float(np.mean(losses)) if len(losses) > 0 else 0.0
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 2)


# ── Confidence bucket analysis ───────────────────────────────────────

CONFIDENCE_BUCKETS = [
    (0, 40, "0-40"),
    (40, 60, "40-60"),
    (60, 80, "60-80"),
    (80, 100, "80-100"),
]


def _analyse_confidence_buckets(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group trades by confidence bucket and compute per-bucket stats."""
    results = []
    for lo, hi, label in CONFIDENCE_BUCKETS:
        if hi == 100:
            bucket = [t for t in trades if lo <= t.get("confidence", 0) <= hi]
        else:
            bucket = [t for t in trades if lo <= t.get("confidence", 0) < hi]
        count = len(bucket)
        wins = sum(1 for t in bucket if t["win"])
        wr = round((wins / count) * 100.0, 2) if count > 0 else 0.0
        avg = round(float(np.mean([t["option_return_pct"] for t in bucket])), 4) if count > 0 else 0.0
        results.append({
            "bucket": label,
            "trades": count,
            "win_rate": wr,
            "avg_return": avg,
        })
    return results


# ── Core backtest ────────────────────────────────────────────────────

def run_convexity_backtest(
    ticker: str,
    lookback_days: int = 60,
    hold_days: int | None = None,
    seed: int = 42,
) -> dict[str, Any]:
    """Backtest option structures over historical price data.

    For each bar in the lookback window:
        1. Compute gamma + momentum → combined signal + confidence
        2. Select structure via structure_engine
        3. Apply SMA-5 entry filter
        4. Get MSA state and apply regime filter / sizing adjustment
        5. Compute underlying return over hold period (tiered TP / SL)
        6. Map underlying return to option return via structure multiplier
        7. Apply confidence-based position sizing with MSA adjustment

    Parameters
    ----------
    ticker : str
        Equity or ETF symbol.
    lookback_days : int
        Trading days to evaluate.
    hold_days : int | None
        Max days to hold each trade.  If ``None``, uses the
        mode-specific default from :func:`get_mode_config`.
    seed : int
        Base random seed for synthetic gamma chains.

    Returns
    -------
    dict
        ticker, total_trades, win_rate, avg_return, total_return,
        weighted_total_return, confidence_analysis, trades.
    """
    # Resolve mode config for this ticker
    asset_mode = classify_mode(ticker)
    mode_cfg = get_mode_config(asset_mode)

    if hold_days is None:
        override = get_asset_hold_override(ticker, asset_mode)
        hold_days = override if override is not None else mode_cfg["hold_days"]

    fetch_days = lookback_days + 30 + hold_days
    period = f"{fetch_days}d"
    hist = yf.Ticker(ticker).history(period=period)

    if hist.empty or len(hist) < lookback_days + 21:
        raise ValueError(
            f"Insufficient history for {ticker}: got {len(hist)} bars, "
            f"need at least {lookback_days + 21}"
        )

    closes = hist["Close"].values
    opens = hist["Open"].values
    dates = hist.index.tolist()

    start_idx = max(20, len(closes) - lookback_days)
    end_idx = len(closes) - hold_days

    trades: list[dict[str, Any]] = []
    skipped_trades = 0          # total skips (SMA filter + no-trade + MSA filter + extension)
    msa_filtered_trades = 0     # skips due to MSA regime conflict only
    extension_filtered_trades = 0  # skips due to extended recent move
    shock_filtered_trades = 0  # skips due to EXPLOSIVE single-day shock move
    trend_filtered_trades = 0  # skips due to EXPLOSIVE trend confirmation failure
    confidence_filtered_trades = 0  # skips due to min confidence
    high_conviction_count = 0   # trades with MSA + gamma alignment
    mean_reversion_count = 0    # trades that triggered MEAN_REVERSION overlay
    timing_filtered_trades = 0  # skips due to entry timing filter
    filters_skipped_by_mode = 0 # filters bypassed because mode doesn't require them
    i = start_idx

    while i < end_idx:
        window = closes[: i + 1]

        # Momentum signal from price window
        momentum = _compute_momentum_from_window(ticker, window)

        # Gamma signal from synthetic chain seeded per day
        spot = closes[i]
        gamma = _compute_gamma_from_spot(ticker, spot, seed=seed + i)

        # Combine signals
        combined = combine_signals(gamma, momentum)
        signal = combined["combined_signal"]
        confidence = combined["confidence"]

        if signal in ("LONG", "SHORT"):
            # Minimum confidence filter from mode config
            if confidence < mode_cfg["min_confidence"]:
                skipped_trades += 1
                confidence_filtered_trades += 1
                i += 1
                continue

            # Select structure — mode config drives spread vs naked
            if mode_cfg["use_spreads"]:
                structure = "CALL_SPREAD" if signal == "LONG" else "PUT_SPREAD"
            else:
                structure = "LONG_CALL" if signal == "LONG" else "LONG_PUT"

            # SMA-5 entry filter
            sma5 = float(np.mean(closes[max(0, i - 4) : i + 1]))
            if signal == "LONG" and spot >= sma5:
                i += 1
                continue
            if signal == "SHORT" and spot <= sma5:
                i += 1
                continue

            # Extension filter — skip trades after outsized 3-day moves
            if i >= 3:
                recent_move = ((closes[i] - closes[i - 3]) / closes[i - 3]) * 100.0
                if signal == "LONG" and recent_move > 2.0:
                    skipped_trades += 1
                    extension_filtered_trades += 1
                    i += 1
                    continue
                if signal == "SHORT" and recent_move < -2.0:
                    skipped_trades += 1
                    extension_filtered_trades += 1
                    i += 1
                    continue

            # Trend confirmation filter — HYBRID only
            # (CONVEX and MEAN_REVERSION skip this filter)
            asset_mode = classify_mode(ticker)
            if asset_mode in ("HYBRID",) and i >= 7:
                ma_fast = float(np.mean(closes[i - 3 : i]))
                ma_slow = float(np.mean(closes[i - 7 : i]))
                if signal == "LONG" and ma_fast <= ma_slow:
                    skipped_trades += 1
                    trend_filtered_trades += 1
                    i += 1
                    continue
                if signal == "SHORT" and ma_fast >= ma_slow:
                    skipped_trades += 1
                    trend_filtered_trades += 1
                    i += 1
                    continue
            elif asset_mode in ("CONVEX",) and i >= 7:
                # CONVEX: trend filter skipped by mode
                filters_skipped_by_mode += 1

            # Shock move filter — HYBRID and CONVEX assets, check prior 2 days
            if asset_mode in ("HYBRID", "CONVEX") and i >= 3:
                recent_1d = ((closes[i - 1] - closes[i - 2]) / closes[i - 2]) * 100.0
                recent_2d = ((closes[i - 2] - closes[i - 3]) / closes[i - 3]) * 100.0
                if signal == "LONG" and (recent_1d > 1.5 or recent_2d > 1.5):
                    skipped_trades += 1
                    shock_filtered_trades += 1
                    i += 1
                    continue
                if signal == "SHORT" and (recent_1d < -1.5 or recent_2d < -1.5):
                    skipped_trades += 1
                    shock_filtered_trades += 1
                    i += 1
                    continue

            entry_idx = i + 1 if (i + 1) < len(opens) else i
            entry_price = opens[entry_idx]
            entry_date = str(dates[entry_idx].date())

            # MSA regime filter — computed as of entry date
            msa_state = get_msa_state_on_date(ticker, entry_date)

            # Skip trade if MSA is strongly opposite to signal
            if signal == "LONG" and msa_state == "MSA_BEARISH":
                skipped_trades += 1
                msa_filtered_trades += 1
                i += 1
                continue
            if signal == "SHORT" and msa_state == "MSA_BULLISH":
                skipped_trades += 1
                msa_filtered_trades += 1
                i += 1
                continue

            # ── Mean-reversion overlay detection ─────────────────────
            # Dynamically switch to MEAN_REVERSION when MSA is neutral
            # and price is extended (RSI or 3-day move).
            trade_mode = asset_mode  # default: keep original mode
            trade_mode_cfg = mode_cfg

            if msa_state == "MSA_NEUTRAL":
                rsi_val = _rsi_proxy(closes[: i + 1], period=5)
                recent_3d = (
                    ((closes[i] - closes[i - 3]) / closes[i - 3]) * 100.0
                    if i >= 3
                    else 0.0
                )

                trigger_mr = False
                if signal == "LONG" and (rsi_val < 35 or recent_3d < -2.5):
                    trigger_mr = True
                elif signal == "SHORT" and (rsi_val > 65 or recent_3d > 2.5):
                    trigger_mr = True

                if trigger_mr:
                    trade_mode = "MEAN_REVERSION"
                    trade_mode_cfg = get_mode_config("MEAN_REVERSION")
                    mean_reversion_count += 1
                    # Override structure: always naked for MR
                    structure = "LONG_CALL" if signal == "LONG" else "LONG_PUT"

            # ── Entry timing filter (mode-dependent) ──────────────────
            # TACTICAL: relaxed short_ma check (0.5% tolerance)
            # HYBRID / CONVEX / MEAN_REVERSION: timing filter skipped
            if i >= 7:
                if trade_mode == "TACTICAL":
                    short_ma = float(np.mean(closes[i - 3 : i]))
                    timing_pass = True
                    if signal == "LONG" and closes[i] < short_ma * 0.995:
                        timing_pass = False
                    elif signal == "SHORT" and closes[i] > short_ma * 1.005:
                        timing_pass = False

                    if not timing_pass:
                        skipped_trades += 1
                        timing_filtered_trades += 1
                        i += 1
                        continue
                else:
                    # HYBRID, CONVEX, MEAN_REVERSION skip timing filter
                    filters_skipped_by_mode += 1

            # Use trade-level mode config for exit parameters
            # Exit logic: mode-driven TP / SL, max hold fallback
            tp_target = trade_mode_cfg["take_profit"]
            sl_target = trade_mode_cfg["stop_loss"]
            sl_threshold = -(sl_target + 0.01 * confidence)

            # Use trade-level hold days (MR may override)
            trade_hold = trade_mode_cfg["hold_days"] if trade_mode == "MEAN_REVERSION" else hold_days

            exit_idx = min(entry_idx + trade_hold, len(closes) - 1)
            exit_reason = "MAX_HOLD"
            actual_hold = trade_hold

            for d in range(1, trade_hold + 1):
                check_idx = entry_idx + d
                if check_idx >= len(closes):
                    break
                check_price = closes[check_idx]

                if signal == "LONG":
                    day_pnl = ((check_price / entry_price) - 1.0) * 100.0
                else:
                    day_pnl = ((entry_price / check_price) - 1.0) * 100.0

                if day_pnl >= tp_target * 2:
                    exit_idx = check_idx
                    exit_reason = "TAKE_PROFIT_2"
                    actual_hold = d
                    break
                if day_pnl >= tp_target:
                    exit_idx = check_idx
                    exit_reason = "TAKE_PROFIT_1"
                    actual_hold = d
                    break
                if day_pnl <= sl_threshold:
                    exit_idx = check_idx
                    exit_reason = "STOP_LOSS"
                    actual_hold = d
                    break

            exit_price = closes[exit_idx]
            exit_date = str(dates[exit_idx].date())

            # Raw underlying return — always (exit - entry) / entry
            raw_underlying_return_pct = round(
                float(((exit_price - entry_price) / entry_price) * 100.0), 4
            )

            # Map to option return via Convex Payoff Engine
            option_return_pct = round(
                float(estimate_option_return(
                    structure=structure,
                    underlying_return_pct=raw_underlying_return_pct,
                    hold_days=actual_hold,
                    implied_vol=0.30,
                    dte=30,
                    moneyness="ATM",
                    mode=trade_mode,
                )), 4
            )

            # Option-level TP / SL override from trade-level mode config
            if option_return_pct >= trade_mode_cfg["take_profit"]:
                if exit_reason == "MAX_HOLD":
                    exit_reason = "TAKE_PROFIT"
            elif option_return_pct <= -trade_mode_cfg["stop_loss"]:
                if exit_reason == "MAX_HOLD":
                    exit_reason = "STOP_LOSS"

            # High-conviction classification
            # Gamma is "supportive" when dealer positioning is NEGATIVE
            # (short gamma — dealers must hedge INTO moves, amplifying them).
            dealer_pos = gamma.get("dealer_positioning", "NEUTRAL")
            gamma_supportive = dealer_pos == "NEGATIVE"

            high_conviction_confidence_threshold = 60
            msa_aligned = (
                (signal == "LONG" and msa_state == "MSA_BULLISH")
                or (signal == "SHORT" and msa_state == "MSA_BEARISH")
            )
            is_spread = structure in ("CALL_SPREAD", "PUT_SPREAD")

            # HC if: (MSA aligned AND confidence >= threshold)
            #     OR: (MSA aligned AND structure is a spread)
            # Gamma supportive is an additional boost but no longer required
            high_conviction = False
            if msa_aligned and confidence >= high_conviction_confidence_threshold:
                high_conviction = True
            elif msa_aligned and is_spread:
                high_conviction = True

            if high_conviction:
                high_conviction_count += 1

            # Position sizing based on confidence
            if confidence >= 60:
                position_size = 1.5
            elif 40 <= confidence < 60:
                position_size = 0.5
            else:
                position_size = 0.3

            # High-conviction boost
            if high_conviction:
                position_size *= 1.25

            # Mode-driven position scale (uses trade-level config)
            position_size = position_size * trade_mode_cfg["position_scale"]

            # MSA regime adjustment to position size
            msa_multiplier = 1.0
            if signal == "LONG" and msa_state == "MSA_BULLISH":
                msa_multiplier = 1.5
            elif signal == "SHORT" and msa_state == "MSA_BEARISH":
                msa_multiplier = 1.5
            elif msa_state == "MSA_NEUTRAL":
                msa_multiplier = 1.0
            # Note: strongly opposite already filtered out above

            msa_adjusted_size = round(position_size * msa_multiplier, 4)
            weighted_option_return = round(option_return_pct * position_size, 4)
            msa_weighted_return = round(option_return_pct * msa_adjusted_size, 4)

            trades.append({
                "entry_date": entry_date,
                "exit_date": exit_date,
                "signal": signal,
                "trade_mode": trade_mode,
                "structure": structure,
                "confidence": confidence,
                "msa_state": msa_state,
                "entry_price": round(float(entry_price), 2),
                "exit_price": round(float(exit_price), 2),
                "underlying_return_pct": raw_underlying_return_pct,
                "option_return_pct": option_return_pct,
                "position_size": position_size,
                "msa_multiplier": msa_multiplier,
                "msa_adjusted_size": msa_adjusted_size,
                "weighted_option_return": weighted_option_return,
                "msa_weighted_return": msa_weighted_return,
                "high_conviction": high_conviction,
                "exit_reason": exit_reason,
                "hold_days": actual_hold,
                "win": option_return_pct > 0,
            })

            # Skip past exit
            i = exit_idx + 1
        else:
            i += 1

    # Aggregate
    total_trades = len(trades)
    wins = sum(1 for t in trades if t["win"])
    losses = total_trades - wins
    win_rate = round((wins / total_trades) * 100.0, 2) if total_trades > 0 else 0.0
    option_returns = [t["option_return_pct"] for t in trades]
    weighted_returns = [t["weighted_option_return"] for t in trades]
    msa_weighted_returns = [t["msa_weighted_return"] for t in trades]
    avg_return = round(float(np.mean(option_returns)), 4) if option_returns else 0.0
    total_return = round(float(sum(option_returns)), 4)
    weighted_total_return = round(float(sum(weighted_returns)), 4)
    msa_weighted_total_return = round(float(sum(msa_weighted_returns)), 4)

    # MSA regime distribution
    msa_bullish_trades = sum(1 for t in trades if t["msa_state"] == "MSA_BULLISH")
    msa_bearish_trades = sum(1 for t in trades if t["msa_state"] == "MSA_BEARISH")
    msa_neutral_trades = sum(1 for t in trades if t["msa_state"] == "MSA_NEUTRAL")

    # MSA-filtered win rate (trades where MSA aligned with signal)
    msa_aligned_trades = [
        t for t in trades
        if (t["signal"] == "LONG" and t["msa_state"] == "MSA_BULLISH")
        or (t["signal"] == "SHORT" and t["msa_state"] == "MSA_BEARISH")
    ]
    msa_aligned_count = len(msa_aligned_trades)
    msa_aligned_wins = sum(1 for t in msa_aligned_trades if t["win"])
    msa_filtered_win_rate = (
        round((msa_aligned_wins / msa_aligned_count) * 100.0, 2)
        if msa_aligned_count > 0
        else 0.0
    )

    # High-conviction metrics
    hc_trades = [t for t in trades if t["high_conviction"]]
    hc_count = len(hc_trades)
    hc_wins = sum(1 for t in hc_trades if t["win"])
    high_conviction_win_rate = (
        round((hc_wins / hc_count) * 100.0, 2) if hc_count > 0 else 0.0
    )
    hc_weighted_returns = [t["msa_weighted_return"] for t in hc_trades]
    high_conviction_weighted_return = (
        round(float(sum(hc_weighted_returns)), 4) if hc_weighted_returns else 0.0
    )

    # Mean-reversion metrics
    mr_trades = [t for t in trades if t.get("trade_mode") == "MEAN_REVERSION"]
    mr_count = len(mr_trades)
    mr_wins = sum(1 for t in mr_trades if t["win"])
    mean_reversion_win_rate = (
        round((mr_wins / mr_count) * 100.0, 2) if mr_count > 0 else 0.0
    )
    mr_returns = [t["option_return_pct"] for t in mr_trades]
    mean_reversion_total_return = (
        round(float(sum(mr_returns)), 4) if mr_returns else 0.0
    )

    confidence_analysis = _analyse_confidence_buckets(trades)

    return {
        "ticker": ticker,
        "mode": asset_mode,
        "mode_config": mode_cfg,
        "lookback_days": lookback_days,
        "hold_days": hold_days,
        "total_trades": total_trades,
        "skipped_trades": skipped_trades,
        "msa_filtered_trades": msa_filtered_trades,
        "extension_filtered_trades": extension_filtered_trades,
        "shock_filtered_trades": shock_filtered_trades,
        "trend_filtered_trades": trend_filtered_trades,
        "confidence_filtered_trades": confidence_filtered_trades,
        "timing_filtered_trades": timing_filtered_trades,
        "filters_skipped_by_mode": filters_skipped_by_mode,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "avg_return": avg_return,
        "total_return": total_return,
        "weighted_total_return": weighted_total_return,
        "msa_weighted_total_return": msa_weighted_total_return,
        "msa_filtered_win_rate": msa_filtered_win_rate,
        "msa_aligned_trades": msa_aligned_count,
        "msa_bullish_trades": msa_bullish_trades,
        "msa_bearish_trades": msa_bearish_trades,
        "msa_neutral_trades": msa_neutral_trades,
        "high_conviction_trades": hc_count,
        "high_conviction_win_rate": high_conviction_win_rate,
        "high_conviction_weighted_return": high_conviction_weighted_return,
        "mean_reversion_trades": mr_count,
        "mean_reversion_win_rate": mean_reversion_win_rate,
        "mean_reversion_total_return": mean_reversion_total_return,
        "confidence_analysis": confidence_analysis,
        "trades": trades,
    }


# ── Example runner ───────────────────────────────────────────────────

def run_example(ticker: str = "SPY", lookback_days: int = 60) -> dict[str, Any]:
    """Run convexity backtest and print summary.

    Returns
    -------
    dict
        Full backtest results.
    """
    result = run_convexity_backtest(ticker, lookback_days=lookback_days)

    print(f"\n{'='*60}")
    print(f"  MONOS CONVEXITY BACKTEST — {ticker}  [{result['mode']}]")
    print(f"{'='*60}")
    print(f"  Hold Days          : {result['hold_days']}  (TP: {result['mode_config']['take_profit']}% / SL: {result['mode_config']['stop_loss']}%)")
    print(f"  Total Trades      : {result['total_trades']}")
    print(f"  Skipped Trades     : {result['skipped_trades']}  (Conf: {result['confidence_filtered_trades']}, MSA: {result['msa_filtered_trades']}, Ext: {result['extension_filtered_trades']}, Shock: {result['shock_filtered_trades']}, Trend: {result['trend_filtered_trades']}, Timing: {result['timing_filtered_trades']})")
    print(f"  Filters Skipped    : {result['filters_skipped_by_mode']}  (bypassed by mode)")
    print(f"  Win Rate           : {result['win_rate']}%")
    print(f"  Avg Option Return  : {result['avg_return']:.4f}%")
    print(f"  Total Option Return: {result['total_return']:.4f}%")
    print(f"  Weighted Total     : {result['weighted_total_return']:.4f}%")
    print(f"  MSA Weighted Total : {result['msa_weighted_total_return']:.4f}%")
    print(f"  MSA Filtered WR    : {result['msa_filtered_win_rate']}% ({result['msa_aligned_trades']} aligned trades)")
    print(f"\n  ─── MSA Regime Distribution ───")
    print(f"  Bullish Trades     : {result['msa_bullish_trades']}")
    print(f"  Bearish Trades     : {result['msa_bearish_trades']}")
    print(f"  Neutral Trades     : {result['msa_neutral_trades']}")
    print(f"\n  ─── High Conviction ───")
    print(f"  HC Trades          : {result['high_conviction_trades']}")
    print(f"  HC Win Rate        : {result['high_conviction_win_rate']}%")
    print(f"  HC Weighted Return : {result['high_conviction_weighted_return']:.4f}%")
    print(f"\n  ─── Mean Reversion ───")
    print(f"  MR Trades          : {result['mean_reversion_trades']}")
    print(f"  MR Win Rate        : {result['mean_reversion_win_rate']}%")
    print(f"  MR Total Return    : {result['mean_reversion_total_return']:.4f}%")

    if result["trades"]:
        print(f"\n  ─── Sample Trades ───")
        print(f"  {'DATE':<12} {'SIG':<6} {'STRUCT':<14} {'MODE':<16} {'CONF':>5} {'MSA':<13} {'HC':<3} {'UND%':>7} {'OPT%':>7} {'EXIT':<14}")
        for t in result["trades"][:8]:
            hc_flag = "Y" if t["high_conviction"] else ""
            print(
                f"  {t['entry_date']:<12} {t['signal']:<6} {t['structure']:<14} "
                f"{t['trade_mode']:<16} "
                f"{t['confidence']:>5.1f} {t['msa_state']:<13} {hc_flag:<3} "
                f"{t['underlying_return_pct']:>+6.2f}% "
                f"{t['option_return_pct']:>+6.2f}% {t['exit_reason']:<14}"
            )

    if result["confidence_analysis"]:
        print(f"\n  ─── Confidence Buckets ───")
        print(f"  {'BUCKET':<10} {'TRADES':>7} {'WIN%':>7} {'AVG RET':>9}")
        for b in result["confidence_analysis"]:
            print(
                f"  {b['bucket']:<10} {b['trades']:>7} "
                f"{b['win_rate']:>6.1f}% {b['avg_return']:>+8.4f}%"
            )

    print(f"{'='*60}\n")

    return result


def run_multi_hold(
    ticker: str = "SPY",
    lookback_days: int = 60,
    hold_days_list: list[int] | None = None,
) -> dict[str, dict[str, Any]]:
    """Run convexity backtest across multiple hold periods and print comparison.

    Parameters
    ----------
    ticker : str
        Equity / ETF symbol.
    lookback_days : int
        Trading days to evaluate.
    hold_days_list : list[int] | None
        Hold periods to test.  Defaults to ``[2, 5, 10, 20]``.

    Returns
    -------
    dict[str, dict]
        Keyed by hold label (e.g. ``"2d"``, ``"5d"``).  Each value
        contains: total_trades, win_rate, avg_return, total_return,
        weighted_total_return, plus the full backtest result under
        ``"full_result"``.
    """
    if hold_days_list is None:
        hold_days_list = [2, 5, 10, 20]

    summary: dict[str, dict[str, Any]] = {}
    results: list[dict[str, Any]] = []

    for hd in hold_days_list:
        r = run_convexity_backtest(ticker, lookback_days=lookback_days, hold_days=hd)
        results.append(r)
        key = f"{hd}d"
        summary[key] = {
            "hold_days": hd,
            "total_trades": r["total_trades"],
            "win_rate": r["win_rate"],
            "avg_return": r["avg_return"],
            "total_return": r["total_return"],
            "weighted_total_return": r["weighted_total_return"],
            "msa_weighted_total_return": r["msa_weighted_total_return"],
            "high_conviction_trades": r["high_conviction_trades"],
            "high_conviction_win_rate": r["high_conviction_win_rate"],
            "full_result": r,
        }

    # Print comparison table
    print(f"\n{'='*72}")
    print(f"  MONOS HOLD PERIOD COMPARISON — {ticker}  (lookback={lookback_days}d)")
    print(f"{'='*72}")
    print(
        f"  {'HOLD':>6}  {'TRADES':>7}  {'WIN%':>7}  "
        f"{'TOT RET':>9}  {'WGT RET':>9}  {'MSA WGT':>9}  {'AVG RET':>9}"
    )
    print(f"  {'-'*62}")
    for r in results:
        print(
            f"  {r['hold_days']:>4}d  {r['total_trades']:>7}  "
            f"{r['win_rate']:>6.1f}%  "
            f"{r['total_return']:>+8.4f}%  "
            f"{r['weighted_total_return']:>+8.4f}%  "
            f"{r['msa_weighted_total_return']:>+8.4f}%  "
            f"{r['avg_return']:>+8.4f}%"
        )
    print(f"{'='*72}\n")

    return summary


if __name__ == "__main__":
    import sys

    tickers = sys.argv[1:] or ["SPY"]
    for t in tickers:
        run_example(t)
