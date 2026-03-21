"""
monos_engine.backtest.backtest_engine

Backtesting engine for the MONOS Conviction Engine.
Evaluates combined gamma + momentum signals over historical price data.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import yfinance as yf

from monos_engine.gamma.gex import compute_gex, generate_synthetic_chain
from monos_engine.momentum.momentum import _compute_rsi, _compute_return, _compute_trend_score, _classify_direction
from monos_engine.combiner.signal_combiner import combine_signals


# ── Backtest core ────────────────────────────────────────────────────

def run_backtest(
    ticker: str,
    lookback_days: int = 60,
    hold_days: int = 2,
    seed: int = 42,
) -> dict[str, Any]:
    """Run a historical backtest of combined signals.

    Parameters
    ----------
    ticker : str
        Equity or ETF symbol.
    lookback_days : int
        Number of trading days to evaluate (default 60).
    hold_days : int
        Days to hold each trade (default 2).
    seed : int
        Base random seed for synthetic gamma chains.

    Returns
    -------
    dict
        ticker, total_trades, wins, losses, win_rate, avg_return,
        total_return, trades.
    """
    # Fetch enough history: lookback + warm-up for RSI(14) + hold buffer
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

    # We evaluate signals starting from index `start_idx` so we have
    # enough warm-up bars for RSI-14 and 20-day return.
    start_idx = max(20, len(closes) - lookback_days)
    end_idx = len(closes) - hold_days  # need room to hold

    trades: list[dict[str, Any]] = []
    i = start_idx

    while i < end_idx:
        window = closes[:i + 1]

        # Momentum signal from price window
        momentum = _compute_momentum_from_window(ticker, window)

        # Gamma signal from synthetic chain seeded per day
        spot = closes[i]
        gamma = _compute_gamma_from_spot(ticker, spot, seed=seed + i)

        # Combine
        combined = combine_signals(gamma, momentum)
        signal = combined["combined_signal"]
        confidence = combined["confidence"]

        if signal in ("LONG", "SHORT"):
            # SMA-5 entry filter
            sma5 = float(np.mean(closes[max(0, i - 4):i + 1]))
            if signal == "LONG" and spot >= sma5:
                i += 1
                continue
            if signal == "SHORT" and spot <= sma5:
                i += 1
                continue

            entry_idx = i + 1 if (i + 1) < len(opens) else i
            entry_price = opens[entry_idx]
            entry_date = str(dates[entry_idx].date())

            # Exit logic: tiered TP, fixed SL, max hold fallback
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

            if signal == "LONG":
                pnl_pct = ((exit_price / entry_price) - 1.0) * 100.0
            else:
                pnl_pct = ((entry_price / exit_price) - 1.0) * 100.0

            if confidence >= 60:
                position_size = 1.5
            elif 40 <= confidence < 60:
                position_size = 0.5
            else:
                position_size = 0.3
            weighted_pnl = pnl_pct * position_size

            trades.append({
                "entry_date": entry_date,
                "exit_date": exit_date,
                "signal": signal,
                "confidence": confidence,
                "entry_price": round(float(entry_price), 2),
                "exit_price": round(float(exit_price), 2),
                "pnl_pct": round(float(pnl_pct), 4),
                "position_size": position_size,
                "weighted_pnl": round(float(weighted_pnl), 4),
                "exit_reason": exit_reason,
                "hold_days": actual_hold,
                "win": pnl_pct > 0,
            })

            # Skip past exit to avoid overlapping trades
            i = exit_idx + 1
        else:
            i += 1

    # Aggregate
    total_trades = len(trades)
    wins = sum(1 for t in trades if t["win"])
    losses = total_trades - wins
    win_rate = round((wins / total_trades) * 100.0, 2) if total_trades > 0 else 0.0
    returns = [t["pnl_pct"] for t in trades]
    weighted_returns = [t["weighted_pnl"] for t in trades]
    avg_return = round(float(np.mean(returns)), 4) if returns else 0.0
    total_return = round(float(sum(returns)), 4)
    weighted_avg_return = round(float(np.mean(weighted_returns)), 4) if weighted_returns else 0.0
    weighted_total_return = round(float(sum(weighted_returns)), 4)

    # Confidence bucket analysis
    confidence_analysis = _analyse_confidence_buckets(trades)

    return {
        "ticker": ticker,
        "lookback_days": lookback_days,
        "hold_days": hold_days,
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "avg_return": avg_return,
        "total_return": total_return,
        "weighted_avg_return": weighted_avg_return,
        "weighted_total_return": weighted_total_return,
        "confidence_analysis": confidence_analysis,
        "trades": trades,
    }


# ── Internal helpers ─────────────────────────────────────────────────

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
        bucket = [t for t in trades if lo <= t.get("confidence", 0) < hi]
        # Include upper bound for the last bucket
        if hi == 100:
            bucket = [t for t in trades if lo <= t.get("confidence", 0) <= hi]
        count = len(bucket)
        wins = sum(1 for t in bucket if t["win"])
        wr = round((wins / count) * 100.0, 2) if count > 0 else 0.0
        avg = round(float(np.mean([t["pnl_pct"] for t in bucket])), 4) if count > 0 else 0.0
        results.append({
            "bucket": label,
            "trades": count,
            "win_rate": wr,
            "avg_return": avg,
        })
    return results


def _compute_momentum_from_window(ticker: str, closes: np.ndarray) -> dict[str, Any]:
    """Compute momentum signal from a price window without fetching data."""
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


# ── Example runner ───────────────────────────────────────────────────

def run_example(ticker: str, lookback_days: int = 60) -> dict[str, Any]:
    """Run backtest and return results.

    Parameters
    ----------
    ticker : str
        Equity or ETF symbol.
    lookback_days : int
        Trading days to evaluate.

    Returns
    -------
    dict
        Full backtest results.
    """
    return run_backtest(ticker, lookback_days=lookback_days)


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json
    import sys

    tickers = sys.argv[1:] or ["SPY"]
    for t in tickers:
        res = run_example(t)
        # Summary without individual trades for cleaner output
        summary = {k: v for k, v in res.items() if k != "trades"}
        summary["sample_trades"] = res["trades"][:5]
        print(json.dumps(summary, indent=2, default=str))
