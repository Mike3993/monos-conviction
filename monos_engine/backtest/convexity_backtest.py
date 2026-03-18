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
from monos_engine.msa.msa_engine import get_msa_state_on_date


# ── Option return mapping ────────────────────────────────────────────

def _map_option_return(underlying_return: float, structure: str) -> float:
    """Map an underlying return to an approximate option return.

    Parameters
    ----------
    underlying_return : float
        Fractional return of the underlying (e.g. 0.02 = +2%).
    structure : str
        LONG_CALL, LONG_PUT, CALL_SPREAD, PUT_SPREAD.

    Returns
    -------
    float
        Approximate option return as a fraction.
    """
    if structure == "LONG_CALL":
        return underlying_return * 2.0
    if structure == "LONG_PUT":
        return -underlying_return * 2.0
    if structure == "CALL_SPREAD":
        return min(underlying_return * 1.2, 1.5)
    if structure == "PUT_SPREAD":
        return min(-underlying_return * 1.2, 1.5)
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
    hold_days: int = 2,
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
    hold_days : int
        Max days to hold each trade.
    seed : int
        Base random seed for synthetic gamma chains.

    Returns
    -------
    dict
        ticker, total_trades, win_rate, avg_return, total_return,
        weighted_total_return, confidence_analysis, trades.
    """
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
    skipped_trades = 0          # total skips (SMA filter + no-trade + MSA filter)
    msa_filtered_trades = 0     # skips due to MSA regime conflict only
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
            # Select structure based on signal + confidence
            structure_result = select_structure(ticker, signal, confidence)
            structure = structure_result["structure"]

            if structure == "NO_TRADE":
                i += 1
                continue

            # SMA-5 entry filter
            sma5 = float(np.mean(closes[max(0, i - 4) : i + 1]))
            if signal == "LONG" and spot >= sma5:
                i += 1
                continue
            if signal == "SHORT" and spot <= sma5:
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

            # Exit logic: tiered TP, adaptive SL, max hold fallback
            sl_threshold = -(0.5 + 0.01 * confidence)

            exit_idx = min(entry_idx + hold_days, len(closes) - 1)
            exit_reason = "MAX_HOLD"
            actual_hold = hold_days

            for d in range(1, hold_days + 1):
                check_idx = entry_idx + d
                if check_idx >= len(closes):
                    break
                check_price = closes[check_idx]

                if signal == "LONG":
                    day_pnl = ((check_price / entry_price) - 1.0) * 100.0
                else:
                    day_pnl = ((entry_price / check_price) - 1.0) * 100.0

                if day_pnl >= 2.0:
                    exit_idx = check_idx
                    exit_reason = "TAKE_PROFIT_2"
                    actual_hold = d
                    break
                if day_pnl >= 1.0:
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

            # Underlying return (fractional)
            if signal == "LONG":
                underlying_return = (exit_price - entry_price) / entry_price
            else:
                underlying_return = (entry_price - exit_price) / entry_price

            # Map to option return
            option_return = _map_option_return(underlying_return, structure)
            option_return_pct = round(float(option_return * 100.0), 4)

            # Position sizing based on confidence
            if confidence >= 60:
                position_size = 1.5
            elif 40 <= confidence < 60:
                position_size = 0.5
            else:
                position_size = 0.3

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
                "structure": structure,
                "confidence": confidence,
                "msa_state": msa_state,
                "entry_price": round(float(entry_price), 2),
                "exit_price": round(float(exit_price), 2),
                "underlying_return_pct": round(float(underlying_return * 100.0), 4),
                "option_return_pct": option_return_pct,
                "position_size": position_size,
                "msa_multiplier": msa_multiplier,
                "msa_adjusted_size": msa_adjusted_size,
                "weighted_option_return": weighted_option_return,
                "msa_weighted_return": msa_weighted_return,
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

    confidence_analysis = _analyse_confidence_buckets(trades)

    return {
        "ticker": ticker,
        "lookback_days": lookback_days,
        "hold_days": hold_days,
        "total_trades": total_trades,
        "skipped_trades": skipped_trades,
        "msa_filtered_trades": msa_filtered_trades,
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
    print(f"  MONOS CONVEXITY BACKTEST — {ticker}")
    print(f"{'='*60}")
    print(f"  Total Trades      : {result['total_trades']}")
    print(f"  Skipped Trades     : {result['skipped_trades']}  (MSA filtered: {result['msa_filtered_trades']})")
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

    if result["trades"]:
        print(f"\n  ─── Sample Trades ───")
        print(f"  {'DATE':<12} {'SIG':<6} {'STRUCT':<14} {'CONF':>5} {'MSA':<13} {'UND%':>7} {'OPT%':>7} {'EXIT':<14}")
        for t in result["trades"][:8]:
            print(
                f"  {t['entry_date']:<12} {t['signal']:<6} {t['structure']:<14} "
                f"{t['confidence']:>5.1f} {t['msa_state']:<13} "
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


if __name__ == "__main__":
    import sys

    tickers = sys.argv[1:] or ["SPY"]
    for t in tickers:
        run_example(t)
