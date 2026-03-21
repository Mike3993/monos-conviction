"""
monos_engine.regime.market_mode

Market Mode Classifier — labels an asset as CONTROLLED or EXPLOSIVE
based on short-term realized volatility and ATR-to-price ratio.

CONTROLLED  — low vol, mean-reverting behaviour likely.
EXPLOSIVE   — high vol, momentum / breakout behaviour likely.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import yfinance as yf


# ── Thresholds ───────────────────────────────────────────────────────

RVOL_PERIOD = 20          # 20-day realized volatility window
ATR_PERIOD = 14           # 14-day ATR window
RVOL_THRESHOLD = 0.20     # annualised; below = CONTROLLED
ATR_PRICE_THRESHOLD = 0.015  # ATR / price; below = CONTROLLED


# ── Internal helpers ─────────────────────────────────────────────────

def _realized_vol(closes: np.ndarray, period: int = RVOL_PERIOD) -> float:
    """Annualised realized volatility over the last *period* bars.

    Uses log returns and scales by sqrt(252).
    """
    if len(closes) < period + 1:
        return 0.0
    log_returns = np.diff(np.log(closes[-(period + 1) :]))
    return float(np.std(log_returns, ddof=1) * np.sqrt(252))


def _atr(
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    period: int = ATR_PERIOD,
) -> float:
    """Average True Range over the last *period* bars."""
    if len(closes) < period + 1:
        return 0.0

    trs: list[float] = []
    for j in range(len(closes) - period, len(closes)):
        hi_lo = highs[j] - lows[j]
        hi_pc = abs(highs[j] - closes[j - 1])
        lo_pc = abs(lows[j] - closes[j - 1])
        trs.append(max(hi_lo, hi_pc, lo_pc))

    return float(np.mean(trs))


# ── Core classifier ─────────────────────────────────────────────────

def classify_market_mode(
    ticker: str,
    closes: np.ndarray,
    highs: np.ndarray | None = None,
    lows: np.ndarray | None = None,
) -> dict[str, Any]:
    """Classify *ticker* as CONTROLLED or EXPLOSIVE.

    Uses two independent measures and requires **both** to be below
    their thresholds for CONTROLLED.  If either signals elevated
    volatility the mode is EXPLOSIVE.

    Parameters
    ----------
    ticker : str
        Equity / ETF symbol.
    closes : np.ndarray
        Close prices (at least ``RVOL_PERIOD + 1`` bars).
    highs : np.ndarray | None
        High prices (same length as *closes*).  If ``None``, ATR is
        skipped and classification relies on realized vol alone.
    lows : np.ndarray | None
        Low prices (same length as *closes*).

    Returns
    -------
    dict
        ticker, mode, realized_vol, atr, atr_price_ratio.
    """
    rvol = _realized_vol(closes, RVOL_PERIOD)

    atr_val = 0.0
    atr_price_ratio = 0.0
    if highs is not None and lows is not None and len(highs) >= ATR_PERIOD + 1:
        atr_val = _atr(highs, lows, closes, ATR_PERIOD)
        price = closes[-1]
        atr_price_ratio = round(atr_val / price, 6) if price > 0 else 0.0

    # Both must be below threshold for CONTROLLED
    vol_controlled = rvol < RVOL_THRESHOLD
    atr_controlled = atr_price_ratio < ATR_PRICE_THRESHOLD if atr_val > 0 else True

    mode = "CONTROLLED" if (vol_controlled and atr_controlled) else "EXPLOSIVE"

    return {
        "ticker": ticker,
        "mode": mode,
        "realized_vol": round(rvol, 4),
        "atr": round(atr_val, 4),
        "atr_price_ratio": round(atr_price_ratio, 6),
    }


# ── Live runner ──────────────────────────────────────────────────────

def run_market_mode(ticker: str = "SPY", lookback: int = 60) -> dict[str, Any]:
    """Classify market mode for *ticker* using recent price data.

    Parameters
    ----------
    ticker : str
        Equity / ETF symbol.
    lookback : int
        Trading days of history to fetch.

    Returns
    -------
    dict
        Same as :func:`classify_market_mode`.
    """
    period = f"{lookback + 30}d"
    hist = yf.Ticker(ticker).history(period=period)

    if hist.empty or len(hist) < RVOL_PERIOD + 1:
        return {
            "ticker": ticker,
            "mode": "CONTROLLED",
            "realized_vol": 0.0,
            "atr": 0.0,
            "atr_price_ratio": 0.0,
        }

    closes = hist["Close"].values
    highs = hist["High"].values
    lows = hist["Low"].values

    return classify_market_mode(ticker, closes, highs, lows)


# ── Example runner ───────────────────────────────────────────────────

def run_example(ticker: str = "SPY") -> dict[str, Any]:
    """Run market mode classifier and print result."""
    result = run_market_mode(ticker)

    print(f"\n{'='*50}")
    print(f"  MONOS MARKET MODE — {ticker}")
    print(f"{'='*50}")
    print(f"  Mode             : {result['mode']}")
    print(f"  Realized Vol (20d): {result['realized_vol']:.4f}")
    print(f"  ATR (14d)        : {result['atr']:.4f}")
    print(f"  ATR / Price      : {result['atr_price_ratio']:.6f}")
    print(f"{'='*50}\n")

    return result


if __name__ == "__main__":
    import sys

    tickers = sys.argv[1:] or ["SPY"]
    for t in tickers:
        run_example(t)
