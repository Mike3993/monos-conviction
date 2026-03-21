"""
MONOS Research Lab — MSA Regime Scanner
-----------------------------------------
Real MSA (Market Structure Analysis) regime classifier for historical data.

Uses the same logic as the production MSA engine:
  - 50-day SMA for trend direction
  - 20-day slope (linear regression) for momentum
  - Classification: MSA_BULLISH / MSA_BEARISH / MSA_NEUTRAL

This replaces the placeholder filter in the research lab with actual
regime computation over real historical price data via yfinance.

Governance notes:
  - Uses geometric returns where applicable
  - Does NOT write to the live MONOS app
  - Type C rules remain Tyler-gated even with real proxy data
  - This is the backtest proxy — Tyler validation still required for crystallization
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import yfinance as yf


# ── core MSA computation ─────────────────────────────────────────────

def classify_msa(
    closes: np.ndarray,
    sma_window: int = 50,
    slope_window: int = 20,
) -> str:
    """Classify MSA regime from a price array.

    Uses the final bar's position relative to:
      - SMA(sma_window): price above = bullish bias
      - Slope(slope_window): positive slope = uptrend momentum

    Returns: "MSA_BULLISH", "MSA_BEARISH", or "MSA_NEUTRAL"
    """
    if len(closes) < max(sma_window, slope_window):
        return "MSA_NEUTRAL"

    price = float(closes[-1])

    # 50-day SMA
    actual_sma_win = min(sma_window, len(closes))
    sma = float(np.mean(closes[-actual_sma_win:]))

    # 20-day slope via linear regression
    actual_slope_win = min(slope_window, len(closes))
    recent = closes[-actual_slope_win:]
    x = np.arange(actual_slope_win, dtype=float)
    slope = float(np.polyfit(x, recent, 1)[0])

    if price > sma and slope > 0:
        return "MSA_BULLISH"
    if price < sma and slope < 0:
        return "MSA_BEARISH"
    return "MSA_NEUTRAL"


# ── historical regime snapshots ──────────────────────────────────────

def generate_regime_snapshots(
    ticker: str,
    start: str,
    end: str,
    sma_window: int = 50,
    slope_window: int = 20,
) -> list[dict[str, Any]]:
    """Generate daily MSA regime snapshots for a ticker over a date range.

    Downloads historical data via yfinance and computes MSA state
    for each trading day.

    Returns list of {date, close, sma50, slope20, msa_state}.
    """
    # Fetch extra history for the SMA lookback
    fetch_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=sma_window * 2)).strftime("%Y-%m-%d")

    hist = yf.Ticker(ticker).history(start=fetch_start, end=end)
    if hist.empty:
        return []

    closes = hist["Close"].values
    dates = hist.index.tolist()

    # Find the start index for output
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    if hist.index.tz is not None:
        start_dt = start_dt.replace(tzinfo=hist.index.tz)

    snapshots = []
    for i in range(sma_window, len(closes)):
        dt = dates[i]
        if dt < start_dt:
            continue

        window = closes[:i + 1]
        msa = classify_msa(window, sma_window, slope_window)

        # Compute components for transparency
        actual_sma_win = min(sma_window, len(window))
        sma = float(np.mean(window[-actual_sma_win:]))
        actual_slope_win = min(slope_window, len(window))
        recent = window[-actual_slope_win:]
        x = np.arange(actual_slope_win, dtype=float)
        slope = float(np.polyfit(x, recent, 1)[0])

        snapshots.append({
            "date": str(dt.date()) if hasattr(dt, 'date') else str(dt)[:10],
            "close": round(float(closes[i]), 4),
            "sma50": round(sma, 4),
            "slope20": round(slope, 6),
            "msa_state": msa,
        })

    return snapshots


def generate_universe_snapshots(
    universe: list[str],
    start: str,
    end: str,
) -> dict[str, list[dict[str, Any]]]:
    """Generate MSA regime snapshots for all tickers in the universe."""
    result = {}
    for ticker in universe:
        print(f"  Scanning {ticker}...")
        try:
            snaps = generate_regime_snapshots(ticker, start, end)
            result[ticker] = snaps
            # Summary
            states = [s["msa_state"] for s in snaps]
            bull = states.count("MSA_BULLISH")
            bear = states.count("MSA_BEARISH")
            neut = states.count("MSA_NEUTRAL")
            total = len(states)
            print(f"    {total} days: BULL={bull} ({bull/total*100:.0f}%) BEAR={bear} ({bear/total*100:.0f}%) NEUT={neut} ({neut/total*100:.0f}%)")
        except Exception as e:
            print(f"    ERROR: {e}")
            result[ticker] = []
    return result


# ── signal generation with real MSA filter ───────────────────────────

def generate_signals_with_msa(
    ticker: str,
    start: str,
    end: str,
    apply_msa_filter: bool = True,
    hold_days: int = 5,
) -> dict[str, Any]:
    """Generate trade signals with real MSA regime filtering.

    Uses real historical data:
      1. Downloads price history via yfinance
      2. Computes momentum signals (5-day return direction)
      3. Optionally applies MSA regime filter
      4. Computes forward returns (log returns for geometric discipline)

    Returns dict with signals, trades, returns, and regime stats.
    """
    fetch_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=120)).strftime("%Y-%m-%d")
    hist = yf.Ticker(ticker).history(start=fetch_start, end=end)
    if hist.empty or len(hist) < 60:
        return {"ticker": ticker, "error": "insufficient data", "trades": [], "returns": []}

    closes = hist["Close"].values
    dates = hist.index.tolist()

    # Find start index
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    if hist.index.tz is not None:
        start_dt = start_dt.replace(tzinfo=hist.index.tz)

    start_idx = 0
    for i, d in enumerate(dates):
        if d >= start_dt:
            start_idx = i
            break

    start_idx = max(start_idx, 50)  # need history for MSA

    trades = []
    msa_filtered = 0
    msa_distribution = {"MSA_BULLISH": 0, "MSA_BEARISH": 0, "MSA_NEUTRAL": 0}

    i = start_idx
    while i < len(closes) - hold_days:
        # Momentum signal: 5-day log return direction
        if i < 5:
            i += 1
            continue
        ret_5d = math.log(closes[i] / closes[i - 5])
        direction = "LONG" if ret_5d > 0 else "SHORT"
        confidence = min(90, max(30, abs(ret_5d) * 3000))

        # MSA state at this bar
        window = closes[:i + 1]
        msa_state = classify_msa(window)
        msa_distribution[msa_state] = msa_distribution.get(msa_state, 0) + 1

        # Apply MSA filter
        if apply_msa_filter:
            if direction == "LONG" and msa_state == "MSA_BEARISH":
                msa_filtered += 1
                i += 3
                continue
            if direction == "SHORT" and msa_state == "MSA_BULLISH":
                msa_filtered += 1
                i += 3
                continue

        # Compute forward return (log return — geometric discipline)
        entry_price = closes[i]
        exit_idx = min(i + hold_days, len(closes) - 1)
        exit_price = closes[exit_idx]

        log_return = math.log(exit_price / entry_price) if entry_price > 0 else 0
        if direction == "SHORT":
            log_return = -log_return

        pct_return = log_return * 100

        date_str = str(dates[i].date()) if hasattr(dates[i], 'date') else str(dates[i])[:10]
        exit_date = str(dates[exit_idx].date()) if hasattr(dates[exit_idx], 'date') else str(dates[exit_idx])[:10]

        trades.append({
            "date": date_str,
            "exit_date": exit_date,
            "ticker": ticker,
            "direction": direction,
            "msa_state": msa_state,
            "confidence": round(confidence, 1),
            "entry_price": round(float(entry_price), 4),
            "exit_price": round(float(exit_price), 4),
            "log_return": round(log_return, 6),
            "pct_return": round(pct_return, 4),
            "win": pct_return > 0,
        })

        i += hold_days  # skip to next entry window

    returns = [t["pct_return"] for t in trades]
    wins = sum(1 for t in trades if t["win"])

    # Build equity curve from log returns
    equity = [1.0]
    for t in trades:
        equity.append(equity[-1] * math.exp(t["log_return"]))

    return {
        "ticker": ticker,
        "msa_filtered": apply_msa_filter,
        "n_trades": len(trades),
        "n_wins": wins,
        "win_rate": round((wins / len(trades)) * 100, 2) if trades else 0,
        "returns": returns,
        "equity_curve": equity,
        "total_return": round(sum(returns), 4),
        "total_theta_cost": round(len(trades) * hold_days * 0.1, 4),
        "msa_filtered_count": msa_filtered,
        "msa_distribution": msa_distribution,
        "trades": trades,
    }


# ── CLI test ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== MONOS MSA Scanner — Real Data Test ===\n")

    ticker = "SPY"
    start = "2025-07-01"
    end = "2026-03-01"

    print(f"Generating regime snapshots for {ticker}...")
    snaps = generate_regime_snapshots(ticker, start, end)
    if snaps:
        states = [s["msa_state"] for s in snaps]
        print(f"  {len(snaps)} days scanned")
        print(f"  BULLISH: {states.count('MSA_BULLISH')}")
        print(f"  BEARISH: {states.count('MSA_BEARISH')}")
        print(f"  NEUTRAL: {states.count('MSA_NEUTRAL')}")
        print(f"  Last: {snaps[-1]['date']} → {snaps[-1]['msa_state']} (close={snaps[-1]['close']}, sma={snaps[-1]['sma50']})")

    print(f"\nGenerating signals with MSA filter for {ticker}...")
    filtered = generate_signals_with_msa(ticker, start, end, apply_msa_filter=True)
    baseline = generate_signals_with_msa(ticker, start, end, apply_msa_filter=False)
    print(f"  Baseline: {baseline['n_trades']} trades, WR={baseline['win_rate']}%, Return={baseline['total_return']:.2f}%")
    print(f"  Filtered: {filtered['n_trades']} trades, WR={filtered['win_rate']}%, Return={filtered['total_return']:.2f}%")
    print(f"  MSA filtered out: {filtered['msa_filtered_count']} trades")
