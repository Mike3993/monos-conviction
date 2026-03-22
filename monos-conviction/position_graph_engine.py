"""
MONOS Position Graph Engine
Computes Black-Scholes greeks and scenario P&L grid for all active
option positions. Writes results to public.position_graph in Supabase.

Scenario grid: 17 price points (-40% to +40% in 5% steps) x 5 dates
(today, +7d, +30d, +60d, +90d).
"""

import os
import sys
import math
import json
import time
import requests
from datetime import date, datetime, timedelta, timezone
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
            return
    print("[!] No .env found")

load_env()

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')
POLYGON_KEY  = os.environ.get('POLYGON_API_KEY', '')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("[FAIL] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    sys.exit(1)

HEADERS_SB = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal'
}

RISK_FREE_RATE = 0.053

# ---------------------------------------------------------------------------
# scipy.stats.norm -- inline fallback if scipy not installed
# ---------------------------------------------------------------------------
try:
    from scipy.stats import norm as _norm
    def norm_cdf(x):
        return _norm.cdf(x)
    def norm_pdf(x):
        return _norm.pdf(x)
except ImportError:
    print("[pos_graph] scipy not found, using math approximation for norm")
    def norm_cdf(x):
        """Abramowitz & Stegun approximation."""
        a1, a2, a3, a4, a5 = (
            0.254829592, -0.284496736, 1.421413741,
            -1.453152027, 1.061405429)
        p = 0.3275911
        sign = 1 if x >= 0 else -1
        x = abs(x)
        t = 1.0 / (1.0 + p * x)
        y = 1.0 - (((((a5*t + a4)*t) + a3)*t + a2)*t + a1) * t * math.exp(-x*x/2)
        return 0.5 * (1.0 + sign * y)
    def norm_pdf(x):
        return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


# ---------------------------------------------------------------------------
# SUPABASE HELPERS
# ---------------------------------------------------------------------------
def sb_get(table, params):
    """Generic Supabase REST GET. Returns list of rows or []."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    try:
        r = requests.get(url, headers={
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}',
        }, params=params, timeout=15)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"  [sb_get] {table} returned {r.status_code}: {r.text[:200]}")
            return []
    except Exception as e:
        print(f"  [sb_get] {table} exception: {e}")
        return []


def sb_delete(table, params_str):
    """Delete rows from Supabase table."""
    url = f"{SUPABASE_URL}/rest/v1/{table}?{params_str}"
    try:
        requests.delete(url, headers=HEADERS_SB, timeout=10)
    except Exception:
        pass


def sb_insert(table, rows):
    """Insert rows into Supabase table. Returns True on success."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    try:
        r = requests.post(url, headers=HEADERS_SB, json=rows, timeout=30)
        if r.status_code in (200, 201, 204):
            return True
        else:
            print(f"  [sb_insert] {table} error {r.status_code}: {r.text[:200]}")
            return False
    except Exception as e:
        print(f"  [sb_insert] {table} exception: {e}")
        return False


# ---------------------------------------------------------------------------
# POLYGON SPOT PRICE (with rate limiting)
# ---------------------------------------------------------------------------
_last_polygon_call = 0

def _polygon_throttle():
    """Enforce 13-second gap between Polygon calls (Starter tier: 5 req/min)."""
    global _last_polygon_call
    elapsed = time.time() - _last_polygon_call
    if elapsed < 13:
        wait = 13 - elapsed
        print(f"    [throttle] waiting {wait:.0f}s for Polygon rate limit...")
        time.sleep(wait)
    _last_polygon_call = time.time()


def fetch_spot_polygon(ticker):
    """Get previous-day close from Polygon."""
    if not POLYGON_KEY:
        print(f"    [{ticker}] No POLYGON_API_KEY, skipping spot fetch")
        return None
    _polygon_throttle()
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev"
    try:
        r = requests.get(url, params={'apikey': POLYGON_KEY}, timeout=15)
        if r.status_code == 429:
            print(f"    [{ticker}] Polygon 429 -- waiting 60s and retrying...")
            time.sleep(60)
            _last_polygon_call = time.time()
            r = requests.get(url, params={'apikey': POLYGON_KEY}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            results = data.get('results', [])
            if results:
                return results[0].get('c')
    except Exception as e:
        print(f"    [{ticker}] Polygon error: {e}")
    return None


# ---------------------------------------------------------------------------
# BLACK-SCHOLES GREEKS + PRICING
# ---------------------------------------------------------------------------
def bs_price(S, K, T, r, sigma, option_type):
    """Black-Scholes option price."""
    if T <= 0:
        if option_type == 'call':
            return max(0, S - K)
        else:
            return max(0, K - S)
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if option_type == 'call':
        return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)


def bs_greeks(S, K, T, r, sigma, option_type):
    """
    Black-Scholes greeks.
    Returns: (delta, gamma, theta_per_day, vega_per_pct)
    """
    if T <= 0:
        # Expired: intrinsic only, no greeks
        return 0.0, 0.0, 0.0, 0.0
    if sigma <= 0:
        sigma = 0.001  # avoid division by zero

    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)

    gamma = norm_pdf(d1) / (S * sigma * math.sqrt(T))
    vega  = S * norm_pdf(d1) * math.sqrt(T) / 100  # per 1% IV move

    if option_type == 'call':
        delta = norm_cdf(d1)
        theta = (
            -S * norm_pdf(d1) * sigma / (2 * math.sqrt(T))
            - r * K * math.exp(-r * T) * norm_cdf(d2)
        ) / 365
    else:
        delta = norm_cdf(d1) - 1
        theta = (
            -S * norm_pdf(d1) * sigma / (2 * math.sqrt(T))
            + r * K * math.exp(-r * T) * norm_cdf(-d2)
        ) / 365

    return delta, gamma, theta, vega


# ---------------------------------------------------------------------------
# STEP 2 -- FETCH POSITIONS AND LEGS
# ---------------------------------------------------------------------------
def fetch_positions_and_legs():
    """Load all active positions and their legs, grouped by ticker."""
    positions = sb_get('positions', {
        'select': 'id,ticker,structure_type,state',
        'is_active': 'eq.true',
    })
    if not positions:
        print("[pos_graph] No active positions found")
        return {}

    position_ids = [p['id'] for p in positions]

    # Fetch all legs (31 total, well under default limit)
    legs = sb_get('position_legs', {
        'select': '*',
    })

    # Group legs by ticker via position_id
    pos_map = {p['id']: p['ticker'] for p in positions}

    ticker_legs = {}
    for leg in legs:
        pid = leg.get('position_id')
        if pid not in pos_map:
            continue
        tk = pos_map[pid]
        if tk not in ticker_legs:
            ticker_legs[tk] = []
        ticker_legs[tk].append(leg)

    return ticker_legs


# ---------------------------------------------------------------------------
# STEP 3 -- FETCH SPOT AND IV
# ---------------------------------------------------------------------------
def fetch_spot_and_iv(ticker, gex_data=None):
    """Get spot price and ATM IV for a ticker."""
    # Try GEX snapshot spot first (no API call needed)
    spot = None
    if gex_data:
        spot = gex_data.get('spot_price')

    # Fallback to Polygon
    if not spot:
        spot = fetch_spot_polygon(ticker)

    if not spot:
        print(f"    [{ticker}] Could not get spot price")
        return None, None

    # ATM IV from symmetry_snapshots
    sym = sb_get('symmetry_snapshots', {
        'select': 'atm_iv',
        'ticker': f'eq.{ticker}',
        'order': 'run_ts.desc',
        'limit': '1',
    })
    atm_iv = 0.30  # default
    if sym and sym[0].get('atm_iv'):
        raw_iv = sym[0]['atm_iv']
        # If stored as percentage (>1), convert to decimal
        atm_iv = raw_iv / 100 if raw_iv > 1 else raw_iv

    return spot, atm_iv


# ---------------------------------------------------------------------------
# STEP 4 -- COMPUTE GREEKS PER LEG
# ---------------------------------------------------------------------------
def parse_leg(leg, spot, atm_iv, today):
    """Parse a single leg and compute its greeks."""
    strike = leg.get('strike')
    exp_str = leg.get('expiration')
    qty_raw = leg.get('quantity', 0)
    entry_price = leg.get('entry_price') or 0
    leg_type = (leg.get('leg_type') or '').upper()

    if not strike or not exp_str:
        return None

    try:
        expiry = date.fromisoformat(str(exp_str).split('T')[0])
    except (ValueError, TypeError):
        return None

    T = max(0, (expiry - today).days) / 365.0

    # Determine option type and direction from leg_type
    if 'CALL' in leg_type:
        option_type = 'call'
    else:
        option_type = 'put'

    # quantity is already signed: negative = short
    qty = abs(qty_raw)
    direction = -1 if qty_raw < 0 or 'SHORT' in leg_type else 1

    delta, gamma, theta, vega = bs_greeks(spot, strike, T, RISK_FREE_RATE, atm_iv, option_type)

    multiplier = 100
    return {
        'strike': strike,
        'expiry': expiry,
        'T': T,
        'option_type': option_type,
        'qty': qty,
        'direction': direction,
        'multiplier': multiplier,
        'entry_price': entry_price,
        'leg_type': leg_type,
        'delta': delta * qty * multiplier * direction,
        'gamma': gamma * qty * multiplier * direction,
        'theta': theta * qty * multiplier * direction,
        'vega':  vega  * qty * multiplier * direction,
    }


# ---------------------------------------------------------------------------
# STEP 5 -- SCENARIO P&L GRID
# ---------------------------------------------------------------------------
def compute_scenario_grid(parsed_legs, spot, atm_iv, today):
    """Compute P&L grid across price/date scenarios."""
    price_pcts = list(range(-40, 45, 5))  # -40 to +40 in 5% steps
    price_scenarios = [spot * (1 + pct / 100.0) for pct in price_pcts]

    date_scenarios = [
        today,
        today + timedelta(days=7),
        today + timedelta(days=30),
        today + timedelta(days=60),
        today + timedelta(days=90),
    ]

    # Total entry cost for pnl_pct calculation
    total_entry_cost = 0
    for leg in parsed_legs:
        total_entry_cost += abs(leg['entry_price'] * leg['qty'] * leg['multiplier'] * leg['direction'])

    results = []
    for sc_price in price_scenarios:
        for sc_date in date_scenarios:
            net_delta = 0.0
            net_gamma = 0.0
            net_theta = 0.0
            net_vega  = 0.0
            net_pnl   = 0.0

            for leg in parsed_legs:
                T_sc = max(0, (leg['expiry'] - sc_date).days) / 365.0

                # Scenario greeks
                d, g, th, v = bs_greeks(
                    sc_price, leg['strike'], T_sc,
                    RISK_FREE_RATE, atm_iv, leg['option_type'])
                net_delta += d * leg['qty'] * leg['multiplier'] * leg['direction']
                net_gamma += g * leg['qty'] * leg['multiplier'] * leg['direction']
                net_theta += th * leg['qty'] * leg['multiplier'] * leg['direction']
                net_vega  += v * leg['qty'] * leg['multiplier'] * leg['direction']

                # Scenario option value
                sc_val = bs_price(
                    sc_price, leg['strike'], T_sc,
                    RISK_FREE_RATE, atm_iv, leg['option_type'])

                leg_pnl = (sc_val - leg['entry_price']) * leg['qty'] * leg['multiplier'] * leg['direction']
                net_pnl += leg_pnl

            pnl_pct = net_pnl / (total_entry_cost + 1) if total_entry_cost else 0

            results.append({
                'scenario_price': round(sc_price, 2),
                'scenario_date': sc_date.isoformat(),
                'net_delta': round(net_delta, 2),
                'net_gamma': round(net_gamma, 4),
                'net_theta': round(net_theta, 2),
                'net_vega': round(net_vega, 2),
                'estimated_pnl': round(net_pnl, 2),
                'pnl_pct': round(pnl_pct, 4),
                'leg_count': len(parsed_legs),
            })

    return results, price_scenarios, price_pcts, date_scenarios


# ---------------------------------------------------------------------------
# STEP 6 -- WRITE TO SUPABASE
# ---------------------------------------------------------------------------
def write_results(ticker, results):
    """Delete today's rows and insert new scenario grid."""
    today_start = datetime.now(timezone.utc).strftime('%Y-%m-%dT00:00:00+00:00')
    sb_delete('position_graph', f'ticker=eq.{ticker}&run_ts=gte.{today_start}')

    now_ts = datetime.now(timezone.utc).isoformat()
    rows = []
    for r in results:
        rows.append({
            'run_ts': now_ts,
            'ticker': ticker,
            'scenario_price': r['scenario_price'],
            'scenario_date': r['scenario_date'],
            'net_delta': r['net_delta'],
            'net_gamma': r['net_gamma'],
            'net_theta': r['net_theta'],
            'net_vega': r['net_vega'],
            'estimated_pnl': r['estimated_pnl'],
            'pnl_pct': r['pnl_pct'],
            'leg_count': r['leg_count'],
        })

    # Insert in batches of 50 to avoid payload limits
    ok = True
    for i in range(0, len(rows), 50):
        batch = rows[i:i+50]
        if not sb_insert('position_graph', batch):
            ok = False
    return ok


# ---------------------------------------------------------------------------
# STEP 7 -- PRINT SUMMARY
# ---------------------------------------------------------------------------
def print_summary(ticker, spot, atm_iv, parsed_legs, results, price_pcts,
                  gex_data, fib_data):
    """Print detailed P&L table for a ticker."""
    print(f"\n{'='*60}")
    print(f"  {ticker} | Spot: ${spot:.2f} | IV: {atm_iv*100:.1f}%")
    print(f"  Legs evaluated: {len(parsed_legs)}")
    print(f"{'='*60}")

    # Show leg breakdown
    print(f"\n  Position Legs:")
    for leg in parsed_legs:
        exp_str = leg['expiry'].strftime('%Y-%m-%d')
        d_str = 'short' if leg['direction'] < 0 else 'long'
        print(f"    {leg['leg_type']:12s} {leg['strike']:>7.1f} "
              f"{exp_str}  x{leg['qty']:>3d} ({d_str})  "
              f"entry=${leg['entry_price']:.2f}  "
              f"delta={leg['delta']:>+8.1f}")

    # Net greeks at current spot, today
    today_at_spot = [r for r in results
                     if abs(r['scenario_price'] - spot) < 0.01
                     and r['scenario_date'] == date.today().isoformat()]
    if today_at_spot:
        g = today_at_spot[0]
        print(f"\n  Current Net Greeks:")
        print(f"    Delta: {g['net_delta']:>+10.1f}  "
              f"Gamma: {g['net_gamma']:>+10.4f}  "
              f"Theta: {g['net_theta']:>+10.2f}  "
              f"Vega: {g['net_vega']:>+10.2f}")

    # P&L table -- today only
    print(f"\n  Price Scenario P&L (today):")
    print(f"  {'Price':>10s}  {'Change':>8s}  {'Est P&L':>12s}  {'P&L %':>8s}  {'Net Delta':>10s}")
    print(f"  {'-'*10}  {'-'*8}  {'-'*12}  {'-'*8}  {'-'*10}")

    today_str = date.today().isoformat()
    today_rows = sorted(
        [r for r in results if r['scenario_date'] == today_str],
        key=lambda r: r['scenario_price']
    )

    for row in today_rows:
        sc_price = row['scenario_price']
        pct = ((sc_price - spot) / spot) * 100
        pnl = row['estimated_pnl']
        pnl_pct = row['pnl_pct'] * 100
        delta = row['net_delta']

        # Format P&L
        if abs(pnl) >= 1000:
            pnl_str = f"${pnl/1000:>+7.1f}K"
        else:
            pnl_str = f"${pnl:>+8.0f}"

        marker = "  <-- current" if abs(pct) < 2.5 else ""
        print(f"  ${sc_price:>9.2f}  ({pct:>+5.0f}%)  {pnl_str:>12s}  "
              f"{pnl_pct:>+7.1f}%  {delta:>+10.1f}{marker}")

    # P&L across dates at current spot
    print(f"\n  Time Decay at Current Spot (${spot:.2f}):")
    print(f"  {'Date':>12s}  {'Days':>5s}  {'Est P&L':>12s}  {'Theta':>10s}")
    print(f"  {'-'*12}  {'-'*5}  {'-'*12}  {'-'*10}")

    spot_rows = sorted(
        [r for r in results if abs(r['scenario_price'] - spot) < 0.01],
        key=lambda r: r['scenario_date']
    )
    for row in spot_rows:
        sc_date = row['scenario_date']
        days_out = (date.fromisoformat(sc_date) - date.today()).days
        pnl = row['estimated_pnl']
        if abs(pnl) >= 1000:
            pnl_str = f"${pnl/1000:>+7.1f}K"
        else:
            pnl_str = f"${pnl:>+8.0f}"
        print(f"  {sc_date:>12s}  {days_out:>+4d}d  {pnl_str:>12s}  "
              f"{row['net_theta']:>+10.2f}")

    # Mark key levels
    print(f"\n  Key Levels:")
    put_wall = gex_data.get('put_wall') if gex_data else None
    call_wall = gex_data.get('call_wall') if gex_data else None
    gamma_flip = gex_data.get('gamma_flip') if gex_data else None

    if put_wall:
        print(f"    Put wall:    ${put_wall:.2f}")
    if call_wall:
        print(f"    Call wall:   ${call_wall:.2f}")
    if gamma_flip:
        print(f"    Gamma flip:  ${gamma_flip:.2f}")

    if fib_data:
        np = fib_data.get('nearest_price')
        nl = fib_data.get('nearest_level')
        if np and nl:
            print(f"    Fib {nl}: ${np:.2f}")
        # Show extension levels
        for key, label in [('e_1272', '127.2%'), ('e_1618', '161.8%'), ('e_2618', '261.8%')]:
            val = fib_data.get(key)
            if val:
                print(f"    Fib {label}:  ${val:.2f}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("POSITION GRAPH ENGINE")
    print("=" * 60)
    print(f"Run: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("-" * 60)

    today = date.today()

    # Fetch all positions grouped by ticker
    ticker_legs = fetch_positions_and_legs()
    if not ticker_legs:
        print("[pos_graph] No active positions -- nothing to compute")
        return

    tickers = sorted(ticker_legs.keys())
    print(f"Active tickers: {', '.join(tickers)}")
    print(f"Total legs across all tickers: {sum(len(v) for v in ticker_legs.values())}")

    for ticker in tickers:
        legs = ticker_legs[ticker]
        print(f"\n[{ticker}] Processing {len(legs)} legs...")

        # GEX data for spot + walls
        gex_rows = sb_get('gex_snapshots', {
            'select': '*',
            'ticker': f'eq.{ticker}',
            'order': 'run_ts.desc',
            'limit': '1',
        })
        gex_data = gex_rows[0] if gex_rows else None

        # Fib data for key levels
        fib_rows = sb_get('fib_levels', {
            'select': '*',
            'ticker': f'eq.{ticker}',
            'order': 'run_ts.desc',
            'limit': '1',
        })
        fib_data = fib_rows[0] if fib_rows else None

        # Spot + IV
        spot, atm_iv = fetch_spot_and_iv(ticker, gex_data)
        if not spot:
            print(f"  [{ticker}] Skipping -- no spot price")
            continue

        print(f"  [{ticker}] Spot: ${spot:.2f} | ATM IV: {atm_iv*100:.1f}%")

        # Parse all legs
        parsed_legs = []
        for leg in legs:
            parsed = parse_leg(leg, spot, atm_iv, today)
            if parsed:
                parsed_legs.append(parsed)

        if not parsed_legs:
            print(f"  [{ticker}] No parseable legs -- skipping")
            continue

        print(f"  [{ticker}] Parsed {len(parsed_legs)} legs, computing scenario grid...")

        # Compute scenario grid
        results, price_scenarios, price_pcts, date_scenarios = \
            compute_scenario_grid(parsed_legs, spot, atm_iv, today)

        print(f"  [{ticker}] {len(results)} scenario points computed")

        # Write to Supabase
        ok = write_results(ticker, results)
        status = 'OK' if ok else 'FAIL'
        print(f"  [{ticker}] Supabase write: [{status}]")

        # Print detailed summary
        print_summary(ticker, spot, atm_iv, parsed_legs, results,
                      price_pcts, gex_data, fib_data)

    print("\n" + "=" * 60)
    print("POSITION GRAPH ENGINE -- RUN COMPLETE")
    print("=" * 60)


if __name__ == '__main__':
    main()
