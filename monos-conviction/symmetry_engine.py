"""
SYMMETRY ENGINE -- MONOS Conviction Pipeline
Computes IV-RV gap, ATM straddle cost, implied move, skew,
and convexity state for each ticker. Writes to symmetry_snapshots
and adjusts scenario_synthesis confidence scores.

Usage:
    python symmetry_engine.py          # full run
    python symmetry_engine.py --dry    # evaluate only, skip writes
"""

import os
import sys
import json
import math
import time
import statistics
import requests
from datetime import date, timedelta, datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# ENV
# ---------------------------------------------------------------------------
def load_env():
    """Load .env from current dir or one level up."""
    for p in [Path(__file__).parent / '.env', Path(__file__).parent.parent / '.env']:
        if p.exists():
            with open(p) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#') or '=' not in line:
                        continue
                    k, v = line.split('=', 1)
                    os.environ.setdefault(k.strip(), v.strip())
            print(f"[symmetry] Loaded env from {p}")
            return
    print("[!] No .env found")

load_env()

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY', '')
TRADING_VOLATILITY_API_KEY = os.environ.get('TRADING_VOLATILITY_API_KEY', '')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("[FAIL] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    sys.exit(1)
if not POLYGON_API_KEY:
    print("[FAIL] Missing POLYGON_API_KEY")
    sys.exit(1)
if not TRADING_VOLATILITY_API_KEY:
    print("[WARN] TRADING_VOLATILITY_API_KEY not set -- IV data will use fallbacks")

HEADERS_SB = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal'
}

TODAY = date.today()
TODAY_ISO = TODAY.isoformat()
DRY_RUN = "--dry" in sys.argv

# ---------------------------------------------------------------------------
# TICKERS -- load from ticker_universe, fall back to metals
# ---------------------------------------------------------------------------
_DEFAULT_TICKERS = ['SLV', 'GLD', 'GDX', 'SILJ', 'SIL']

def _load_universe():
    """Load tickers from ticker_universe table, fall back to defaults."""
    try:
        r = requests.get(
            SUPABASE_URL + "/rest/v1/ticker_universe?select=ticker&order=ticker.asc",
            headers={"apikey": SUPABASE_KEY, "Authorization": "Bearer " + SUPABASE_KEY},
            timeout=10
        )
        if r.status_code == 200:
            tickers = [row["ticker"] for row in r.json() if row.get("ticker")]
            if tickers:
                print(f"[symmetry] Loaded {len(tickers)} tickers from ticker_universe")
                return tickers
    except Exception as e:
        print(f"[symmetry] ticker_universe fetch failed: {e}")
    return _DEFAULT_TICKERS

TICKERS = _load_universe()

# ---------------------------------------------------------------------------
# STEP 2 -- FETCH PRICE DATA FROM POLYGON (with rate limiting)
# ---------------------------------------------------------------------------
_last_polygon_call = 0.0

def _polygon_throttle():
    """Enforce 5 req/min (12s between calls) for Polygon Starter tier."""
    global _last_polygon_call
    elapsed = time.time() - _last_polygon_call
    if elapsed < 13:
        wait = 13 - elapsed
        print(f"  [rate-limit] Polygon pause {wait:.0f}s...")
        time.sleep(wait)
    _last_polygon_call = time.time()


def fetch_daily_closes(ticker):
    """Fetch up to 30 daily closes from Polygon aggs endpoint."""
    _polygon_throttle()
    end = TODAY
    start = end - timedelta(days=45)
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
    params = {
        'adjusted': 'true',
        'sort': 'asc',
        'limit': 30,
        'apiKey': POLYGON_API_KEY
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
            results = data.get('results', [])
            closes = [bar['c'] for bar in results if 'c' in bar]
            if closes:
                print(f"  [{ticker}] Got {len(closes)} daily bars, spot=${closes[-1]:.2f}")
                return closes
            else:
                print(f"  [{ticker}] No bars returned from Polygon")
        elif r.status_code == 429:
            print(f"  [{ticker}] Polygon 429 -- waiting 60s and retrying...")
            time.sleep(60)
            _last_polygon_call = time.time()
            r2 = requests.get(url, params=params, timeout=15)
            if r2.status_code == 200:
                results = r2.json().get('results', [])
                closes = [bar['c'] for bar in results if 'c' in bar]
                if closes:
                    print(f"  [{ticker}] Got {len(closes)} daily bars (retry), spot=${closes[-1]:.2f}")
                    return closes
            print(f"  [{ticker}] Retry also failed: HTTP {r2.status_code}")
        else:
            print(f"  [{ticker}] Polygon HTTP {r.status_code}")
    except Exception as e:
        print(f"  [{ticker}] Polygon fetch error: {e}")
    return []


def realized_vol(closes, window):
    """Compute annualized realized volatility over a window of closes."""
    if len(closes) < window + 1:
        return None
    returns = [
        math.log(closes[i] / closes[i - 1])
        for i in range(len(closes) - window, len(closes))
    ]
    std = statistics.stdev(returns)
    return round(std * math.sqrt(252) * 100, 2)

# ---------------------------------------------------------------------------
# STEP 3 -- FETCH IV DATA FROM TRADING VOLATILITY
# ---------------------------------------------------------------------------
def fetch_trading_volatility(ticker, is_first=False):
    """
    Fetch IV data from Trading Volatility API v2.
    Base URL: https://stocks.tradingvolatility.net/api/v2
    Auth: Authorization: Bearer <key>
    Returns dict with atm_iv, skew_slope, iv_rank, etc.
    """
    if not TRADING_VOLATILITY_API_KEY:
        return {}

    TV_BASE = 'https://stocks.tradingvolatility.net/api/v2'
    tv_headers = {'Authorization': f'Bearer {TRADING_VOLATILITY_API_KEY}'}
    result = {}

    # --- Endpoint 1: /tickers/{ticker} (canonical snapshot) ---
    try:
        url = f'{TV_BASE}/tickers/{ticker}'
        r = requests.get(url, headers=tv_headers, timeout=15)
        print(f"  [TV] /tickers/{ticker}: status={r.status_code}")
        if is_first:
            print(f"  [TV] Response ({ticker}): {r.text[:500]}")
        if r.status_code == 200:
            data = r.json()
            d = data.get('data', data) if isinstance(data, dict) else data

            # ATM IV -- try flat keys first (decimal: 0.25 = 25%)
            for key in ['atm_iv', 'iv', 'implied_volatility']:
                if key in d and d[key] is not None:
                    val = float(d[key])
                    result['atm_iv'] = round(val * 100, 2) if val < 1 else round(val, 2)
                    break

            # Back-calculate ATM IV from expected_move_pct_30d if not found
            # expected_move_pct_30d ~ IV * sqrt(30/365) (in pct)
            # So IV ~ expected_move_pct_30d / sqrt(30/365)
            if 'atm_iv' not in result:
                em = d.get('expected_move', {})
                em30 = em.get('expected_move_pct_30d')
                if em30 is not None:
                    iv_est = float(em30) / math.sqrt(30 / 365)
                    result['atm_iv'] = round(iv_est, 2)
                    print(f"  [TV] ATM IV back-calc from 30d EM ({em30:.1f}%): {result['atm_iv']}%")

            # IV rank (0-100)
            if 'iv_rank' in d and d['iv_rank'] is not None:
                result['iv_rank'] = round(float(d['iv_rank']), 1)

    except Exception as e:
        print(f"  [TV] /tickers/{ticker} error: {e}")

    # --- Endpoint 1b: /tickers/{ticker}/series for direct atm_iv if still missing ---
    if 'atm_iv' not in result:
        try:
            url = f'{TV_BASE}/tickers/{ticker}/series'
            params = {'metrics': 'atm_iv,iv_rank', 'window': '5d'}
            r = requests.get(url, headers=tv_headers, params=params, timeout=15)
            print(f"  [TV] /series: status={r.status_code}")
            if r.status_code == 200:
                sdata = r.json()
                sd = sdata.get('data', sdata) if isinstance(sdata, dict) else sdata
                # Series returns time-series array; take latest point
                points = sd if isinstance(sd, list) else sd.get('points', sd.get('series', []))
                if isinstance(points, list) and points:
                    latest = points[-1]
                    if 'atm_iv' in latest and latest['atm_iv'] is not None:
                        val = float(latest['atm_iv'])
                        result['atm_iv'] = round(val * 100, 2) if val < 1 else round(val, 2)
                        print(f"  [TV] ATM IV from series: {result['atm_iv']}%")
                    if 'iv_rank' in latest and latest['iv_rank'] is not None:
                        result['iv_rank'] = round(float(latest['iv_rank']), 1)
        except Exception as e:
            print(f"  [TV] /series error: {e}")

    # --- Endpoint 2: /tickers/{ticker}/market-structure (skew + levels) ---
    try:
        url = f'{TV_BASE}/tickers/{ticker}/market-structure'
        r = requests.get(url, headers=tv_headers, timeout=15)
        print(f"  [TV] /market-structure: status={r.status_code}")
        if is_first and r.status_code == 200:
            print(f"  [TV] market-structure ({ticker}): {r.text[:500]}")
        if r.status_code == 200:
            data = r.json()
            d = data.get('data', data) if isinstance(data, dict) else data

            # Skew from supporting_factors
            sf = d.get('supporting_factors', {})
            if sf:
                # put_call_25d_iv_premium_pct is already in pct units
                skew_val = sf.get('put_call_25d_iv_premium_pct')
                if skew_val is not None:
                    result['skew_slope'] = round(float(skew_val), 4)

                # PCR
                pcr_oi = sf.get('pcr_oi')
                if pcr_oi is not None:
                    result['pcr_oi'] = round(float(pcr_oi), 3)

                pcr_vol = sf.get('pcr_volume')
                if pcr_vol is not None:
                    result['pcr_volume'] = round(float(pcr_vol), 3)

            # Key levels
            kl = d.get('key_levels', {})
            if kl:
                result['gamma_flip'] = kl.get('gamma_flip')

    except Exception as e:
        print(f"  [TV] /market-structure error: {e}")

    if result:
        print(f"  [TV] Parsed for {ticker}: {result}")
    else:
        print(f"  [TV] No data returned for {ticker}")

    return result

# ---------------------------------------------------------------------------
# STEP 3b -- FALLBACK: IV from existing Supabase tables
# ---------------------------------------------------------------------------
def fetch_iv_fallback(ticker):
    """Try to get ATM IV from gex_snapshots or vol_surface in Supabase."""
    # Try gex_snapshots first (often has atm_iv)
    for table in ['gex_snapshots', 'vol_surface']:
        try:
            url = (f"{SUPABASE_URL}/rest/v1/{table}"
                   f"?ticker=eq.{ticker}&select=*"
                   f"&order=run_ts.desc&limit=1")
            r = requests.get(url, headers={
                'apikey': SUPABASE_KEY,
                'Authorization': f'Bearer {SUPABASE_KEY}',
            }, timeout=10)
            if r.status_code == 200 and r.json():
                row = r.json()[0]
                for key in ['atm_iv', 'iv', 'implied_vol']:
                    if key in row and row[key] is not None:
                        val = float(row[key])
                        print(f"  [{ticker}] IV fallback from {table}.{key}: {val}")
                        return val
        except Exception:
            pass
    return None

# ---------------------------------------------------------------------------
# STEP 4 -- COMPUTE ATM STRADDLE
# ---------------------------------------------------------------------------
def compute_straddle(atm_iv, spot_price, dte=30):
    """
    ATM straddle approximation (Kris Abdelmessih formula):
    ATM straddle ~ 0.8 x IV x sqrt(DTE/365) x spot
    """
    if atm_iv is None or spot_price is None:
        return None, None, None
    cost = round(0.8 * (atm_iv / 100) * math.sqrt(dte / 365) * spot_price, 2)
    pct = round(cost / spot_price * 100, 2) if spot_price else None
    return cost, pct, cost  # cost, pct, pts

# ---------------------------------------------------------------------------
# STEP 6 -- CLASSIFY CONVEXITY STATE
# ---------------------------------------------------------------------------
def classify_convexity(iv_rv_gap_20d, skew_slope):
    """
    Classify convexity state from IV-RV gap.
    Returns (convexity_state, symmetry_score).
    """
    if iv_rv_gap_20d is None:
        return 'UNKNOWN', 50

    if iv_rv_gap_20d < -3:
        state = 'CONVEXITY_CHEAP'
        score = 75
    elif iv_rv_gap_20d < 2:
        state = 'CONVEXITY_FAIR'
        score = 50
    elif iv_rv_gap_20d < 6:
        state = 'CONVEXITY_RICH'
        score = 30
    else:
        state = 'CONVEXITY_VERY_RICH'
        score = 15

    # Adjust by skew -- elevated put skew = asymmetric downside fear
    if skew_slope and skew_slope > 0.1:
        score = max(0, score - 10)

    return state, score

# ---------------------------------------------------------------------------
# STEP 7 -- WRITE TO SUPABASE
# ---------------------------------------------------------------------------
def sb_delete(ticker):
    """Delete today's symmetry_snapshots rows for ticker."""
    url = (f"{SUPABASE_URL}/rest/v1/symmetry_snapshots"
           f"?ticker=eq.{ticker}&run_ts=gte.{TODAY_ISO}")
    try:
        r = requests.delete(url, headers=HEADERS_SB, timeout=10)
        return r.status_code in (200, 204)
    except Exception:
        return False


def sb_insert(row):
    """Insert a symmetry_snapshots row."""
    url = f"{SUPABASE_URL}/rest/v1/symmetry_snapshots"
    try:
        r = requests.post(url, headers=HEADERS_SB, json=row, timeout=10)
        if r.status_code in (200, 201):
            return True
        else:
            print(f"  [!] Insert failed: HTTP {r.status_code} -- {r.text[:200]}")
            return False
    except Exception as e:
        print(f"  [!] Insert error: {e}")
        return False

# ---------------------------------------------------------------------------
# STEP 8 -- UPDATE SCENARIO SYNTHESIS
# ---------------------------------------------------------------------------
def update_scenario_synthesis(ticker, symmetry_score):
    """Adjust today's scenario_synthesis confidence_score by symmetry."""
    url = (f"{SUPABASE_URL}/rest/v1/scenario_synthesis"
           f"?ticker=eq.{ticker}&run_ts=gte.{TODAY_ISO}"
           f"&select=id,confidence_score")
    try:
        r = requests.get(url, headers={
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}',
        }, timeout=10)
        if r.status_code != 200:
            return 0
        rows = r.json()
        updated = 0
        for row in rows:
            old_score = row.get('confidence_score')
            if old_score is None:
                continue
            # Symmetry score adjusts confidence
            # CONVEXITY_CHEAP = boost, CONVEXITY_RICH = reduce
            sym_modifier = (symmetry_score - 50) / 500
            new_score = min(0.95, max(0.05, float(old_score) + sym_modifier))
            new_score = round(new_score, 4)
            upd_url = (f"{SUPABASE_URL}/rest/v1/scenario_synthesis"
                       f"?id=eq.{row['id']}")
            upd_headers = dict(HEADERS_SB)
            upd_headers['Prefer'] = 'return=minimal'
            ur = requests.patch(upd_url, headers=upd_headers,
                                json={'confidence_score': new_score}, timeout=10)
            if ur.status_code in (200, 204):
                updated += 1
        return updated
    except Exception as e:
        print(f"  [{ticker}] Scenario update error: {e}")
        return 0

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("SYMMETRY ENGINE -- MONOS Conviction Pipeline")
    print(f"Date: {TODAY_ISO}  |  Tickers: {len(TICKERS)}  |  Dry run: {DRY_RUN}")
    print("=" * 60)

    results = []
    is_first = True
    ticker_count = 0

    for ticker in TICKERS:
        ticker_count += 1
        print(f"\n--- {ticker} ({ticker_count}/{len(TICKERS)}) ---")

        # STEP 2: Price data
        closes = fetch_daily_closes(ticker)
        if not closes:
            print(f"  [{ticker}] Skipping -- no price data")
            continue

        spot_price = closes[-1]
        hv5 = realized_vol(closes, 5)
        hv10 = realized_vol(closes, 10)
        hv20 = realized_vol(closes, 20)
        print(f"  [{ticker}] HV5={hv5}%  HV10={hv10}%  HV20={hv20}%")

        # STEP 3: IV data from Trading Volatility
        tv_data = fetch_trading_volatility(ticker, is_first=is_first)
        is_first = False
        atm_iv = tv_data.get('atm_iv')
        skew_slope = tv_data.get('skew_slope')
        put_skew = tv_data.get('put_skew')
        call_skew = tv_data.get('call_skew')

        # Fallback chain for ATM IV
        if atm_iv is None:
            atm_iv = fetch_iv_fallback(ticker)
        if atm_iv is None and hv20 is not None:
            atm_iv = round(hv20 * 1.10, 2)
            print(f"  [{ticker}] Using HV20 proxy for IV: {atm_iv}%")

        # STEP 4: ATM straddle
        atm_straddle_cost, implied_move_pct, implied_move_pts = \
            compute_straddle(atm_iv, spot_price, dte=30)
        print(f"  [{ticker}] ATM IV={atm_iv}%  Straddle=${atm_straddle_cost}  "
              f"Implied move={implied_move_pct}%")

        # STEP 5: IV-RV gaps
        iv_rv_gap_5d = round(atm_iv - hv5, 2) if (atm_iv and hv5) else None
        iv_rv_gap_20d = round(atm_iv - hv20, 2) if (atm_iv and hv20) else None
        print(f"  [{ticker}] IV-RV gap (5d)={iv_rv_gap_5d}  (20d)={iv_rv_gap_20d}")

        # STEP 6: Classify
        convexity_state, symmetry_score = classify_convexity(iv_rv_gap_20d, skew_slope)
        print(f"  [{ticker}] State={convexity_state}  Score={symmetry_score}")

        # Build row
        row = {
            'ticker': ticker,
            'spot_price': spot_price,
            'atm_iv': atm_iv,
            'atm_straddle_cost': atm_straddle_cost,
            'implied_move_pct': implied_move_pct,
            'implied_move_pts': implied_move_pts,
            'hv5': hv5,
            'hv10': hv10,
            'hv20': hv20,
            'iv_rv_gap_5d': iv_rv_gap_5d,
            'iv_rv_gap_20d': iv_rv_gap_20d,
            'skew_slope': skew_slope,
            'put_skew': put_skew,
            'call_skew': call_skew,
            'term_structure': None,
            'convexity_state': convexity_state,
            'symmetry_score': symmetry_score,
        }
        results.append(row)

        # STEP 7: Write
        if not DRY_RUN:
            sb_delete(ticker)
            ok = sb_insert(row)
            if ok:
                print(f"  [{ticker}] Written to symmetry_snapshots")
            # STEP 8: Update scenario synthesis
            n = update_scenario_synthesis(ticker, symmetry_score)
            if n:
                print(f"  [{ticker}] Updated {n} scenario_synthesis rows")

        # Rate limit pause every 5 tickers to avoid Polygon 429s
        if ticker_count % 5 == 0 and ticker_count < len(TICKERS):
            print(f'\n[symmetry] Rate limit pause (60s)...')
            time.sleep(60)

    # STEP 9: Summary
    print("\n" + "=" * 60)
    print("SYMMETRY ENGINE -- RUN COMPLETE")
    print("=" * 60)
    for r in results:
        t = r['ticker']
        sp = r['spot_price'] or 0
        iv = r['atm_iv'] or 0
        sc = r['atm_straddle_cost'] or 0
        imp = r['implied_move_pct'] or 0
        h5 = r['hv5'] or 0
        h20 = r['hv20'] or 0
        gap = r['iv_rv_gap_20d']
        gap_str = f"{gap:+.2f}%" if gap is not None else "N/A"
        state = r['convexity_state']
        score = r['symmetry_score']

        print(f"  {t:5s} | spot ${sp:>8.2f} | ATM IV {iv:>5.1f}% | "
              f"straddle ${sc:>6.2f} ({imp:.1f}%)")
        print(f"        | HV5: {h5:>5.1f}%  HV20: {h20:>5.1f}%")
        print(f"        | IV-RV gap (20d): {gap_str}")
        arrow = ""
        if state == 'CONVEXITY_CHEAP':
            arrow = "  << CHEAP -- good to own convexity"
        elif state == 'CONVEXITY_VERY_RICH':
            arrow = "  !! VERY RICH -- sell premium context"
        print(f"        | Convexity: {state} | Score: {score}{arrow}")

    print(f"\nProcessed {len(results)} tickers. "
          f"{'(DRY RUN -- no writes)' if DRY_RUN else ''}")


if __name__ == '__main__':
    main()
