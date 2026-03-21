"""
MONOS Options Data Provider
----------------------------
Abstraction layer for live options chain + quote data.
Primary: Polygon.io (free tier — delayed quotes + prev-close).
Fallback-ready for Tradier / other providers.

Does NOT execute trades — read-only market data.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

import requests

# ── configuration ────────────────────────────────────────────────────

POLYGON_API_KEY = os.environ.get(
    "POLYGON_API_KEY",
    "vyFTFJpl_S8C7aJycZ9aCxEI8ff1vDG2",
)
POLYGON_BASE = "https://api.polygon.io"

# DTE targeting per mode
_DTE_TARGETS: dict[str, tuple[int, int]] = {
    "TACTICAL":        (7, 45),
    "HYBRID":          (14, 60),
    "CONVEX":          (21, 90),
    "MEAN_REVERSION":  (7, 45),
}


# ── low-level helpers ────────────────────────────────────────────────

def _pg(path: str, params: dict | None = None) -> dict:
    """GET from Polygon with API key injected."""
    p = dict(params or {})
    p["apiKey"] = POLYGON_API_KEY
    r = requests.get(f"{POLYGON_BASE}{path}", params=p, timeout=12)
    r.raise_for_status()
    return r.json()


def _days_until(date_str: str) -> int:
    exp = datetime.strptime(date_str, "%Y-%m-%d").date()
    return (exp - datetime.now().date()).days


def _quote_contract(symbol: str) -> dict[str, Any]:
    """Get bid/ask from the delayed quotes endpoint, fall back to prev-close."""
    bid, ask = 0.0, 0.0

    # Try quotes (delayed but has bid/ask)
    try:
        data = _pg(f"/v3/quotes/{symbol}", {"limit": 1, "order": "desc", "sort": "timestamp"})
        results = data.get("results", [])
        if results:
            bid = float(results[0].get("bid_price", 0) or 0)
            ask = float(results[0].get("ask_price", 0) or 0)
    except Exception:
        pass

    # Fallback: prev-close for last traded price
    last_close = 0.0
    try:
        data = _pg(f"/v2/aggs/ticker/{symbol}/prev")
        results = data.get("results", [])
        if results:
            last_close = float(results[0].get("c", 0) or 0)
    except Exception:
        pass

    # If quotes came back empty, approximate from close
    if bid == 0 and ask == 0 and last_close > 0:
        bid = round(last_close * 0.98, 4)
        ask = round(last_close * 1.02, 4)
    elif bid == 0 and ask > 0:
        bid = round(ask * 0.95, 4)
    elif ask == 0 and bid > 0:
        ask = round(bid * 1.05, 4)

    mid = round((bid + ask) / 2, 4) if (bid + ask) > 0 else last_close
    spread = round(ask - bid, 4) if ask > bid else 0

    return {
        "bid": round(bid, 4),
        "ask": round(ask, 4),
        "mid": mid,
        "spread": spread,
        "last_close": round(last_close, 4),
    }


# ── public API ───────────────────────────────────────────────────────

def get_spot_price(ticker: str) -> float | None:
    """Fetch latest spot price via Polygon prev-close."""
    try:
        data = _pg(f"/v2/aggs/ticker/{ticker.upper()}/prev")
        results = data.get("results", [])
        if results:
            return float(results[0].get("c", 0))
    except Exception:
        pass
    return None


def get_expirations(ticker: str, mode: str = "TACTICAL") -> list[str]:
    """Available expiration dates filtered to the mode's DTE window."""
    dte_lo, dte_hi = _DTE_TARGETS.get(mode, (14, 45))
    today = datetime.now().date()
    date_from = (today + timedelta(days=dte_lo)).isoformat()
    date_to = (today + timedelta(days=dte_hi)).isoformat()

    try:
        data = _pg(
            "/v3/reference/options/contracts",
            {
                "underlying_ticker": ticker.upper(),
                "expiration_date.gte": date_from,
                "expiration_date.lte": date_to,
                "limit": 250,
                "order": "asc",
                "sort": "expiration_date",
            },
        )
        dates = sorted(set(
            c["expiration_date"]
            for c in data.get("results", [])
            if "expiration_date" in c
        ))
        return dates
    except Exception:
        return []


def get_chain(
    ticker: str,
    expiration: str,
    direction: str = "LONG",
    structure: str = "LONG_CALL",
    strike_range: float = 10.0,
) -> list[dict[str, Any]]:
    """Fetch options chain for a ticker + expiration near ATM.

    Returns contracts sorted by distance from ATM with live pricing.
    """
    spot = get_spot_price(ticker)
    if not spot:
        return []

    # call vs put
    if structure in ("LONG_CALL", "CALL_SPREAD"):
        ct = "call"
    elif structure in ("LONG_PUT", "PUT_SPREAD"):
        ct = "put"
    else:
        ct = "call" if direction == "LONG" else "put"

    strike_lo = spot - strike_range
    strike_hi = spot + strike_range

    try:
        data = _pg(
            "/v3/reference/options/contracts",
            {
                "underlying_ticker": ticker.upper(),
                "expiration_date": expiration,
                "contract_type": ct,
                "strike_price.gte": strike_lo,
                "strike_price.lte": strike_hi,
                "limit": 30,
                "order": "asc",
                "sort": "strike_price",
            },
        )
    except Exception:
        return []

    contracts = []
    for ref in data.get("results", []):
        symbol = ref.get("ticker", "")
        strike = ref.get("strike_price", 0)
        exp = ref.get("expiration_date", expiration)

        # Fetch pricing
        pricing = _quote_contract(symbol)

        otm_pct = round(((strike - spot) / spot) * 100, 2)

        contracts.append({
            "contract_symbol": symbol,
            "underlying": ticker.upper(),
            "strike": strike,
            "expiration": exp,
            "contract_type": ct,
            "bid": pricing["bid"],
            "ask": pricing["ask"],
            "mid": pricing["mid"],
            "spread": pricing["spread"],
            "last_close": pricing["last_close"],
            "dte": _days_until(exp),
            "otm_pct": otm_pct,
            "spot": spot,
        })

    # Sort by distance from ATM
    contracts.sort(key=lambda c: abs(c["otm_pct"]))
    return contracts


def get_quote(contract_symbol: str) -> dict[str, Any] | None:
    """Fetch live quote for a single options contract."""
    try:
        pricing = _quote_contract(contract_symbol)
        if pricing["mid"] == 0 and pricing["last_close"] == 0:
            return None

        mid = pricing["mid"]
        spread = pricing["spread"]

        return {
            "contract_symbol": contract_symbol,
            "bid": pricing["bid"],
            "ask": pricing["ask"],
            "mid": mid,
            "spread": spread,
            "last_close": pricing["last_close"],
            "suggested_exit_price": round(mid - (spread * 0.10), 4),
        }
    except Exception:
        return None


# ── delta-based strike selection rules ────────────────────────────────

_DELTA_TARGETS: dict[str, tuple[float, float]] = {
    "TACTICAL":        (0.35, 0.50),
    "HYBRID":          (0.30, 0.50),
    "CONVEX":          (0.20, 0.35),
    "MEAN_REVERSION":  (0.40, 0.60),
}

# OTM% fallback when delta is unavailable
# Maps to approximate delta targets
_OTM_FALLBACK: dict[str, tuple[float, float]] = {
    "TACTICAL":        (0.0, 3.0),     # ATM to 3% OTM ≈ 0.35-0.50 delta
    "HYBRID":          (0.0, 4.0),     # ATM to 4% OTM
    "CONVEX":          (2.0, 6.0),     # 2-6% OTM ≈ 0.20-0.35 delta
    "MEAN_REVERSION":  (0.0, 2.0),     # ATM to 2% OTM ≈ 0.40-0.60 delta
}


def _score_contract(c: dict[str, Any], mode: str) -> float:
    """Score a contract based on delta target proximity.

    Returns 0.0 (perfect) to 1.0+ (poor). Lower is better.
    """
    delta_lo, delta_hi = _DELTA_TARGETS.get(mode, (0.30, 0.50))
    delta = c.get("delta")

    if delta is not None:
        abs_delta = abs(delta)
        if delta_lo <= abs_delta <= delta_hi:
            # Inside target range — score by distance to midpoint
            mid_target = (delta_lo + delta_hi) / 2
            return abs(abs_delta - mid_target) / (delta_hi - delta_lo)
        elif abs_delta < delta_lo:
            return 0.5 + (delta_lo - abs_delta)  # too OTM
        else:
            return 0.5 + (abs_delta - delta_hi)  # too ITM
    else:
        # Fallback: use OTM%
        otm_lo, otm_hi = _OTM_FALLBACK.get(mode, (0.0, 4.0))
        abs_otm = abs(c.get("otm_pct", 0))
        if otm_lo <= abs_otm <= otm_hi:
            mid_target = (otm_lo + otm_hi) / 2
            return abs(abs_otm - mid_target) / max(otm_hi - otm_lo, 0.01)
        elif abs_otm < otm_lo:
            return 0.5 + (otm_lo - abs_otm)
        else:
            return 0.5 + (abs_otm - otm_hi)


def pick_best_contract(
    ticker: str,
    mode: str,
    direction: str,
    structure: str,
    return_candidates: bool = False,
) -> dict[str, Any] | None:
    """Auto-select the best contract for a given trade setup.

    Uses delta-based strike selection rules per mode:
      TACTICAL:        delta 0.35–0.50
      HYBRID:          delta 0.30–0.50
      CONVEX:          delta 0.20–0.35
      MEAN_REVERSION:  delta 0.40–0.60

    Falls back to OTM% approximation when delta is unavailable.

    Parameters
    ----------
    return_candidates : bool
        If True, attaches a 'strike_candidates' list (top 3) to the
        returned contract for audit purposes.
    """
    expirations = get_expirations(ticker, mode=mode)
    if not expirations:
        return None

    # Prefer monthly expirations (typically 3rd Fri — day 15-21)
    # as they have best liquidity.  Try up to 3 expirations.
    ordered = []
    monthlies = [e for e in expirations if 15 <= int(e.split("-")[2]) <= 21]
    ordered.extend(monthlies)
    ordered.extend([e for e in expirations if e not in monthlies])

    spot = get_spot_price(ticker) or 0

    for target_exp in ordered[:3]:
        chain = get_chain(ticker, target_exp, direction=direction, structure=structure)
        priced = [c for c in chain if c["mid"] > 0]
        if not priced:
            continue

        # Score each contract by delta proximity to mode target
        scored = [(c, _score_contract(c, mode)) for c in priced]
        scored.sort(key=lambda x: x[1])

        best = scored[0][0]

        # Enrich with delta / moneyness metadata
        best["suggested_entry_price"] = round(best["mid"] + (best["spread"] * 0.10), 4)
        best["strike_delta"] = best.get("delta")
        best["moneyness_pct"] = best.get("otm_pct", 0)

        # Approximate delta from OTM% if actual delta missing
        if best["strike_delta"] is None and spot > 0:
            otm = abs(best.get("otm_pct", 0))
            # Rough approximation: ATM ≈ 0.50, 5% OTM ≈ 0.25
            best["strike_delta"] = round(max(0.05, 0.50 - otm * 0.05), 4)

        # Attach top 3 candidates for audit
        if return_candidates or True:
            candidates = []
            for c, score in scored[:3]:
                candidates.append({
                    "contract_symbol": c["contract_symbol"],
                    "strike": c["strike"],
                    "delta": c.get("delta"),
                    "otm_pct": c.get("otm_pct"),
                    "mid": c["mid"],
                    "score": round(score, 4),
                })
            best["strike_candidates"] = candidates

        return best

    return None


# ── CLI test ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Testing Polygon options provider...\n")

    spot = get_spot_price("SPY")
    print(f"SPY spot: ${spot}")

    exps = get_expirations("SPY", mode="TACTICAL")
    print(f"SPY TACTICAL expirations: {exps[:5]}")

    if exps:
        chain = get_chain("SPY", exps[0], direction="LONG", structure="LONG_CALL")
        print(f"SPY chain ({exps[0]}): {len(chain)} contracts")
        for c in chain[:3]:
            print(f"  {c['contract_symbol']} strike={c['strike']} bid={c['bid']} ask={c['ask']} mid={c['mid']} dte={c['dte']}")

    print()
    best = pick_best_contract("SPY", "TACTICAL", "LONG", "CALL_SPREAD")
    if best:
        print(f"Best contract: {best['contract_symbol']}")
        print(f"  Strike: {best['strike']}, Bid: ${best['bid']}, Ask: ${best['ask']}, Mid: ${best['mid']}")
        print(f"  Suggested entry: ${best['suggested_entry_price']}")
    else:
        print("No contract found")
