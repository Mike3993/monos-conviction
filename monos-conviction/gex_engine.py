"""
GEX ENGINE -- Gamma Exposure snapshot per ticker
=================================================
Fetches options data from Polygon (using endpoints available
on Starter plan), computes gamma via Black-Scholes, then
derives GEX metrics per strike.

Endpoint strategy (Polygon Starter tier compatible):
  1. Spot price  -> /v2/aggs/ticker/{T}/prev  (prev day close)
  2. Contracts   -> /v3/reference/options/contracts  (paginated list)
  3. Snapshots   -> /v3/snapshot?ticker=O:...  (per-contract OI + price)
  4. Gamma       -> Black-Scholes computation (no greeks endpoint needed)

Usage:
    python gex_engine.py          # live run
    python gex_engine.py --dry    # compute only, skip DB write
"""

import os
import sys
import json
import math
import datetime
from pathlib import Path

# ============================================================
# ENV + DEPENDENCIES
# ============================================================

try:
    from dotenv import load_dotenv
except ImportError:
    print("[gex] python-dotenv not installed. Run: pip install python-dotenv")
    sys.exit(1)

env_path = Path(__file__).resolve().parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
    print(f"[gex] Loaded env from {env_path}")
else:
    env_up = Path(__file__).resolve().parent.parent / ".env"
    if env_up.exists():
        load_dotenv(env_up)
        print(f"[gex] Loaded env from {env_up}")
    else:
        print("[gex] WARNING: No .env file found")

try:
    import requests
except ImportError:
    print("[gex] requests not installed. Run: pip install requests")
    sys.exit(1)

try:
    from supabase import create_client
except ImportError:
    print("[gex] supabase not installed. Run: pip install supabase")
    sys.exit(1)

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")

DRY_RUN = "--dry" in sys.argv

_DEFAULT_TICKERS = ["SLV", "GLD", "GDX", "SILJ", "SIL"]

def _load_universe():
    """Load tickers from ticker_universe table, fall back to defaults."""
    try:
        import requests as _rq
        r = _rq.get(
            SUPABASE_URL + "/rest/v1/ticker_universe?select=ticker&order=ticker.asc",
            headers={"apikey": SUPABASE_KEY, "Authorization": "Bearer " + SUPABASE_KEY},
            timeout=10
        )
        if r.status_code == 200:
            tickers = [row["ticker"] for row in r.json() if row.get("ticker")]
            if tickers:
                print(f"[gex] Loaded {len(tickers)} tickers from ticker_universe")
                return tickers
    except Exception as e:
        print(f"[gex] ticker_universe fetch failed: {e}")
    return _DEFAULT_TICKERS

TICKERS = _load_universe()

# Max contracts to snapshot per ticker (API budget control)
# 100 contracts ~ 50s per ticker, 5 tickers ~ 4 min total
MAX_CONTRACTS_PER_TICKER = 100
# Risk-free rate assumption for Black-Scholes
RISK_FREE_RATE = 0.05
# Implied vol estimate (used when we can't solve for IV)
DEFAULT_IV = 0.35

# ============================================================
# BLACK-SCHOLES GAMMA
# ============================================================

def norm_cdf(x):
    """Standard normal CDF approximation."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

def norm_pdf(x):
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)

def bs_gamma(S, K, T, r, sigma):
    """
    Black-Scholes gamma.
    S = spot, K = strike, T = time to expiry (years),
    r = risk-free rate, sigma = implied volatility.
    Gamma is the same for calls and puts.
    """
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
        gamma = norm_pdf(d1) / (S * sigma * math.sqrt(T))
        return gamma
    except (ValueError, ZeroDivisionError, OverflowError):
        return 0.0

def estimate_iv(option_price, S, K, T, r, contract_type):
    """
    Simple bisection IV solver. Returns implied vol or DEFAULT_IV.
    """
    if T <= 0 or option_price <= 0 or S <= 0:
        return DEFAULT_IV
    try:
        low, high = 0.01, 3.0
        for _ in range(50):
            mid = (low + high) / 2
            price = bs_price(S, K, T, r, mid, contract_type)
            if price > option_price:
                high = mid
            else:
                low = mid
            if abs(high - low) < 0.001:
                break
        return (low + high) / 2
    except Exception:
        return DEFAULT_IV

def bs_price(S, K, T, r, sigma, contract_type):
    """Black-Scholes option price."""
    if T <= 0 or sigma <= 0:
        return 0.0
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        if contract_type == "call":
            return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
        else:
            return K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)
    except (ValueError, ZeroDivisionError, OverflowError):
        return 0.0

# ============================================================
# SUPABASE CLIENT
# ============================================================

def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[gex] ERROR: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set")
        return None
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ============================================================
# POLYGON: FETCH SPOT PRICE (prev day close)
# ============================================================

def fetch_spot_price(ticker):
    """Get previous day close from Polygon aggs endpoint."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev"
    params = {"adjusted": "true", "apiKey": POLYGON_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code != 200:
            print(f"[gex] WARNING: Spot price fetch failed for {ticker}: HTTP {r.status_code}")
            return None
        data = r.json()
        results = data.get("results", [])
        if results and len(results) > 0:
            return float(results[0].get("c", 0))
        print(f"[gex] WARNING: No spot price in response for {ticker}")
        return None
    except Exception as e:
        print(f"[gex] WARNING: Spot price error for {ticker}: {e}")
        return None

# ============================================================
# POLYGON: FETCH OPTIONS CONTRACTS LIST
# ============================================================

def fetch_contract_list(ticker, spot_price=None):
    """
    Get list of active options contracts from reference endpoint.
    Filters to strikes within +-30% of spot to focus on liquid strikes.
    Returns list of {ticker, strike_price, expiration_date, contract_type}.
    """
    today = datetime.date.today()
    max_expiry = today + datetime.timedelta(days=90)

    params = {
        "underlying_ticker": ticker,
        "expiration_date.gte": today.isoformat(),
        "expiration_date.lte": max_expiry.isoformat(),
        "expired": "false",
        "limit": 250,
        "apiKey": POLYGON_API_KEY,
    }
    # Filter near-ATM if we have spot
    if spot_price:
        params["strike_price.gte"] = round(spot_price * 0.70, 2)
        params["strike_price.lte"] = round(spot_price * 1.30, 2)

    url = "https://api.polygon.io/v3/reference/options/contracts"

    all_contracts = []
    try:
        while url and len(all_contracts) < MAX_CONTRACTS_PER_TICKER:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code != 200:
                print(f"[gex] WARNING: Contract list fetch failed for {ticker}: HTTP {r.status_code}")
                break
            data = r.json()
            results = data.get("results", [])
            all_contracts.extend(results)

            next_url = data.get("next_url")
            if next_url and len(all_contracts) < MAX_CONTRACTS_PER_TICKER:
                url = next_url
                params = {"apiKey": POLYGON_API_KEY}
            else:
                break

        print(f"[gex] {ticker}: found {len(all_contracts)} active contracts (next 120 days)")
        return all_contracts[:MAX_CONTRACTS_PER_TICKER]

    except Exception as e:
        print(f"[gex] WARNING: Contract list error for {ticker}: {e}")
        return []

# ============================================================
# POLYGON: FETCH PER-CONTRACT SNAPSHOTS (batched)
# ============================================================

def fetch_contract_snapshots(contract_tickers):
    """
    Fetch snapshots for a list of option tickers using the
    universal snapshot endpoint. Batches to avoid URL length limits.
    Returns dict of {option_ticker: snapshot_data}.
    """
    snapshots = {}
    batch_size = 10  # fetch 10 at a time via comma-separated tickers

    for i in range(0, len(contract_tickers), batch_size):
        batch = contract_tickers[i:i + batch_size]
        tickers_param = ",".join(batch)
        url = "https://api.polygon.io/v3/snapshot"
        params = {"ticker": tickers_param, "apiKey": POLYGON_API_KEY}

        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code != 200:
                # Try one-by-one fallback
                for oticker in batch:
                    try:
                        r2 = requests.get(url, params={"ticker": oticker, "apiKey": POLYGON_API_KEY}, timeout=10)
                        if r2.status_code == 200:
                            for res in r2.json().get("results", []):
                                if "error" not in res:
                                    snapshots[res.get("ticker", oticker)] = res
                    except Exception:
                        pass
                continue

            for res in r.json().get("results", []):
                if "error" not in res:
                    snapshots[res.get("ticker", "")] = res

        except Exception as e:
            print(f"[gex] WARNING: Snapshot batch error: {e}")
            continue

    return snapshots

# ============================================================
# BUILD ENRICHED CONTRACTS
# ============================================================

def build_enriched_contracts(contract_list, snapshots, spot_price):
    """
    Combine reference data + snapshot data, compute gamma via BS.
    Returns list of dicts ready for GEX computation.
    """
    today = datetime.date.today()
    enriched = []

    for c in contract_list:
        oticker = c.get("ticker", "")
        snap = snapshots.get(oticker)
        if not snap:
            continue

        strike = float(c.get("strike_price", 0))
        contract_type = c.get("contract_type", "").lower()
        exp_str = c.get("expiration_date", "")
        oi = snap.get("open_interest", 0)

        if not strike or not contract_type or not exp_str or not oi or oi <= 0:
            continue

        # Time to expiry in years
        try:
            exp_date = datetime.date.fromisoformat(exp_str)
            days_to_exp = (exp_date - today).days
            if days_to_exp <= 0:
                continue
            T = days_to_exp / 365.0
        except ValueError:
            continue

        # Option mid price from session data
        session = snap.get("session", {})
        opt_close = session.get("close", 0)
        opt_price = float(opt_close) if opt_close else 0

        # Estimate IV from option price, or use default
        if opt_price > 0:
            iv = estimate_iv(opt_price, spot_price, strike, T, RISK_FREE_RATE, contract_type)
        else:
            iv = DEFAULT_IV

        # Compute gamma via Black-Scholes
        gamma = bs_gamma(spot_price, strike, T, RISK_FREE_RATE, iv)

        if gamma <= 0:
            continue

        enriched.append({
            "details": {"strike_price": strike, "contract_type": contract_type},
            "greeks": {"gamma": gamma},
            "open_interest": oi,
        })

    return enriched

# ============================================================
# COMPUTE GEX
# ============================================================

def compute_gex(contracts, spot_price):
    """
    Compute GEX metrics from enriched contracts.
    Returns dict with net_gex, gamma_flip, put_wall, call_wall, etc.
    """
    if not contracts or not spot_price:
        return None

    multiplier = 100
    gex_by_strike = {}

    for c in contracts:
        details = c.get("details", {})
        greeks = c.get("greeks", {})

        strike = details.get("strike_price")
        contract_type = details.get("contract_type", "").lower()
        gamma = greeks.get("gamma")
        oi = c.get("open_interest", 0)

        if strike is None or gamma is None:
            continue
        if oi <= 0 or gamma == 0:
            continue

        # GEX = gamma * OI * multiplier * spot^2 / 100
        gex = float(gamma) * float(oi) * multiplier * (float(spot_price) ** 2) / 100.0

        if contract_type == "put":
            gex = gex * -1

        strike = float(strike)
        gex_by_strike[strike] = gex_by_strike.get(strike, 0.0) + gex

    if not gex_by_strike:
        return None

    # Net GEX
    net_gex = sum(gex_by_strike.values())

    # Sort strikes
    sorted_strikes = sorted(gex_by_strike.keys())

    # Gamma flip: strike where cumulative GEX crosses zero
    gamma_flip = None
    cumulative = 0.0
    prev_sign = None
    for strike in sorted_strikes:
        cumulative += gex_by_strike[strike]
        curr_sign = 1 if cumulative >= 0 else -1
        if prev_sign is not None and curr_sign != prev_sign:
            gamma_flip = strike
            break
        prev_sign = curr_sign

    # Put wall: strike with most negative GEX
    put_wall = None
    min_gex = 0
    for strike, gex in gex_by_strike.items():
        if gex < min_gex:
            min_gex = gex
            put_wall = strike

    # Call wall: strike with most positive GEX
    call_wall = None
    max_gex = 0
    for strike, gex in gex_by_strike.items():
        if gex > max_gex:
            max_gex = gex
            call_wall = strike

    # Regime
    if net_gex > 0:
        gex_regime = "POSITIVE"
        dealer_bias = "LONG_GAMMA"
    elif net_gex < 0:
        gex_regime = "NEGATIVE"
        dealer_bias = "SHORT_GAMMA"
    else:
        gex_regime = "NEUTRAL"
        dealer_bias = "NEUTRAL"

    # Top 10 strikes by absolute GEX
    top_strikes = sorted(gex_by_strike.items(), key=lambda x: abs(x[1]), reverse=True)[:10]
    top_strikes_json = []
    for strike, gex in top_strikes:
        if strike == put_wall:
            stype = "put_wall"
        elif strike == call_wall:
            stype = "call_wall"
        else:
            stype = "neutral"
        top_strikes_json.append({
            "strike": strike,
            "gex": round(gex, 2),
            "type": stype
        })

    return {
        "net_gex": round(net_gex, 2),
        "gamma_flip": gamma_flip,
        "put_wall": put_wall,
        "call_wall": call_wall,
        "gex_regime": gex_regime,
        "dealer_bias": dealer_bias,
        "top_strikes": top_strikes_json,
    }

# ============================================================
# WRITE TO SUPABASE
# ============================================================

def write_snapshot(sb, ticker, spot_price, gex_data):
    """Insert one row into public.gex_snapshots."""
    row = {
        "ticker": ticker,
        "spot_price": spot_price,
        "net_gex": gex_data["net_gex"],
        "gamma_flip": gex_data["gamma_flip"],
        "put_wall": gex_data["put_wall"],
        "call_wall": gex_data["call_wall"],
        "gex_regime": gex_data["gex_regime"],
        "dealer_bias": gex_data["dealer_bias"],
        "top_strikes": json.dumps(gex_data["top_strikes"]),
        "run_ts": datetime.datetime.utcnow().isoformat(),
    }
    try:
        result = sb.table("gex_snapshots").insert(row).execute()
        return True
    except Exception as e:
        msg = str(e)
        if "404" in msg or "PGRST" in msg or "does not exist" in msg or "schema cache" in msg:
            print(f"[gex] ERROR: Table gex_snapshots not found -- create it in Supabase first")
        else:
            print(f"[gex] ERROR writing {ticker}: {e}")
        return False

# ============================================================
# MAIN
# ============================================================

def main():
    now = datetime.datetime.now()
    print("=" * 60)
    print("GEX ENGINE -- RUN START")
    print(f"Date: {now.strftime('%Y-%m-%d')}")
    print(f"Time: {now.strftime('%H:%M:%S')}")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"Supabase: {SUPABASE_URL[:50]}..." if SUPABASE_URL else "Supabase: NOT SET")
    print(f"Polygon key: {'SET' if POLYGON_API_KEY else 'NOT SET'}")
    print(f"Tickers: {', '.join(TICKERS)}")
    print("=" * 60)

    if not POLYGON_API_KEY:
        print("[gex] FATAL: POLYGON_API_KEY not set in .env")
        sys.exit(1)

    sb = None
    if not DRY_RUN:
        sb = get_supabase()
        if not sb:
            print("[gex] FATAL: Cannot connect to Supabase")
            sys.exit(1)

    results = []
    skipped = []

    for ticker in TICKERS:
        print(f"\n--- {ticker} ---")

        # Step 1: Fetch spot price (prev day close)
        spot = fetch_spot_price(ticker)
        if spot:
            print(f"[gex] {ticker} spot (prev close): ${spot:.2f}")
        else:
            print(f"[gex] WARNING: Cannot get spot price for {ticker} -- skipping")
            skipped.append(ticker)
            continue

        # Step 2: Fetch contract list from reference endpoint (near ATM)
        contract_list = fetch_contract_list(ticker, spot)
        if not contract_list:
            print(f"[gex] WARNING: No contracts found for {ticker} -- skipping")
            skipped.append(ticker)
            continue

        # Step 3: Fetch per-contract snapshots (OI + session price)
        contract_tickers = [c["ticker"] for c in contract_list if "ticker" in c]
        print(f"[gex] {ticker}: fetching snapshots for {len(contract_tickers)} contracts...")
        snapshots = fetch_contract_snapshots(contract_tickers)
        print(f"[gex] {ticker}: got {len(snapshots)} snapshots with data")

        if not snapshots:
            print(f"[gex] WARNING: No snapshot data for {ticker} -- skipping")
            skipped.append(ticker)
            continue

        # Step 4: Enrich contracts (compute gamma via BS)
        enriched = build_enriched_contracts(contract_list, snapshots, spot)
        print(f"[gex] {ticker}: {len(enriched)} contracts with OI + gamma")

        if not enriched:
            print(f"[gex] WARNING: No usable contracts for {ticker} -- skipping")
            skipped.append(ticker)
            continue

        # Step 5: Compute GEX
        gex_data = compute_gex(enriched, spot)
        if not gex_data:
            print(f"[gex] WARNING: GEX computation returned no data for {ticker} -- skipping")
            skipped.append(ticker)
            continue

        # Step 6: Write to Supabase
        written = False
        if not DRY_RUN and sb:
            written = write_snapshot(sb, ticker, spot, gex_data)
            if written:
                print(f"[gex] {ticker}: snapshot written to gex_snapshots")
        elif DRY_RUN:
            print(f"[gex] {ticker}: DRY RUN -- skipping DB write")

        results.append({
            "ticker": ticker,
            "spot": spot,
            "written": written,
            **gex_data,
        })

    # == Summary ==
    print()
    print("=" * 60)
    print("GEX ENGINE -- RUN COMPLETE")
    print("=" * 60)
    print(f"Tickers processed: {len(results)}")
    print(f"Tickers skipped:   {len(skipped)}")
    if skipped:
        print(f"  Skipped: {', '.join(skipped)}")
    print()

    for r in results:
        net_abs = abs(r["net_gex"])
        if net_abs >= 1e9:
            net_s = f"{r['net_gex'] / 1e9:.2f}B"
        elif net_abs >= 1e6:
            net_s = f"{r['net_gex'] / 1e6:.2f}M"
        elif net_abs >= 1e3:
            net_s = f"{r['net_gex'] / 1e3:.2f}K"
        else:
            net_s = f"{r['net_gex']:.2f}"

        print(f"  {r['ticker']:5s} | spot ${r['spot']:.2f} | net_gex: {net_s}")
        flip_s = f"${r['gamma_flip']:.2f}" if r['gamma_flip'] else "N/A"
        pw_s = f"${r['put_wall']:.2f}" if r['put_wall'] else "N/A"
        cw_s = f"${r['call_wall']:.2f}" if r['call_wall'] else "N/A"
        print(f"        Flip: {flip_s} | Put wall: {pw_s} | Call wall: {cw_s}")
        print(f"        Regime: {r['gex_regime']} | Dealer: {r['dealer_bias']}")
        print()

    print("=" * 60)


if __name__ == "__main__":
    main()
