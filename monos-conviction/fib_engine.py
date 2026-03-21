"""
MONOS Fibonacci Engine
Reads T0-entered swing points from ledger.fib_swings (or defaults for SLV),
computes retracement + extension levels, finds nearest level to current price,
and writes results to public.fib_levels in Supabase.
"""

import os
import sys
import json
import requests
from datetime import date, datetime, timezone
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
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY', '')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("[FAIL] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"); sys.exit(1)
if not POLYGON_API_KEY:
    print("[FAIL] Missing POLYGON_API_KEY"); sys.exit(1)

TICKERS = ['SLV', 'GLD', 'GDX', 'SILJ', 'SIL']
HEADERS_SB = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal'
}

# SLV default swings (used if no T0 data exists)
SLV_DEFAULTS = {
    'swing_high': 34.86,
    'swing_high_date': '2024-05-20',
    'swing_low': 17.54,
    'swing_low_date': '2022-09-01',
    'direction': 'up',
}


# ---------------------------------------------------------------------------
# STEP 2 -- READ SWINGS FROM SUPABASE
# ---------------------------------------------------------------------------
def fetch_swings():
    """Fetch T0-entered swing points from fib_swings table."""
    url = f"{SUPABASE_URL}/rest/v1/fib_swings"
    params = {
        'select': '*',
        'order': 'entered_at.desc',
    }
    try:
        r = requests.get(url, headers={
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}',
        }, params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"  [swings] Fetch returned {r.status_code}: {r.text[:200]}")
            return []
    except Exception as e:
        print(f"  [swings] Fetch error: {e}")
        return []


def build_swing_map(swings):
    """Group swings by ticker, keep most recent per ticker."""
    swing_map = {}
    for row in swings:
        ticker = row.get('ticker')
        if ticker and ticker not in swing_map:
            swing_map[ticker] = row
    return swing_map


# ---------------------------------------------------------------------------
# STEP 3 -- FETCH CURRENT PRICE
# ---------------------------------------------------------------------------
def fetch_current_price(ticker):
    """Fetch previous day close from Polygon."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev"
    params = {'adjusted': 'true', 'apiKey': POLYGON_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        results = data.get('results', [])
        if results:
            return results[0]['c']
        print(f"  [{ticker}] No price results from Polygon")
        return None
    except Exception as e:
        print(f"  [{ticker}] Price fetch error: {e}")
        return None


# ---------------------------------------------------------------------------
# STEP 4 -- COMPUTE FIB LEVELS
# ---------------------------------------------------------------------------
def compute_fibs(swing_high, swing_low, direction):
    """Compute Fibonacci retracement and extension levels."""
    diff = swing_high - swing_low

    if direction == 'up':
        # Bullish -- retracements down from high
        r_236 = swing_high - diff * 0.236
        r_382 = swing_high - diff * 0.382
        r_500 = swing_high - diff * 0.500
        r_618 = swing_high - diff * 0.618
        r_786 = swing_high - diff * 0.786
        # Extensions up from low
        e_100 = swing_low + diff * 1.0
        e_1272 = swing_low + diff * 1.272
        e_1618 = swing_low + diff * 1.618
        e_2618 = swing_low + diff * 2.618
    else:
        # Bearish -- retracements up from low
        r_236 = swing_low + diff * 0.236
        r_382 = swing_low + diff * 0.382
        r_500 = swing_low + diff * 0.500
        r_618 = swing_low + diff * 0.618
        r_786 = swing_low + diff * 0.786
        # Extensions down from high
        e_100 = swing_high - diff * 1.0
        e_1272 = swing_high - diff * 1.272
        e_1618 = swing_high - diff * 1.618
        e_2618 = swing_high - diff * 2.618

    return {
        'r_236': round(r_236, 4),
        'r_382': round(r_382, 4),
        'r_500': round(r_500, 4),
        'r_618': round(r_618, 4),
        'r_786': round(r_786, 4),
        'e_100': round(e_100, 4),
        'e_1272': round(e_1272, 4),
        'e_1618': round(e_1618, 4),
        'e_2618': round(e_2618, 4),
    }


def find_nearest(fibs, current_price):
    """Find the Fibonacci level nearest to the current price."""
    levels = {
        '23.6%': fibs['r_236'],
        '38.2%': fibs['r_382'],
        '50.0%': fibs['r_500'],
        '61.8%': fibs['r_618'],
        '78.6%': fibs['r_786'],
        '100%': fibs['e_100'],
        '127.2%': fibs['e_1272'],
        '161.8%': fibs['e_1618'],
        '261.8%': fibs['e_2618'],
    }
    nearest = min(levels, key=lambda k: abs(levels[k] - current_price))
    nearest_price = levels[nearest]
    nearest_distance_pct = abs(nearest_price - current_price) / current_price * 100
    return nearest, nearest_price, round(nearest_distance_pct, 2)


# ---------------------------------------------------------------------------
# STEP 5 -- WRITE TO SUPABASE
# ---------------------------------------------------------------------------
def delete_today_rows(ticker):
    """Delete any existing rows for this ticker from today."""
    today_start = datetime.now(timezone.utc).strftime('%Y-%m-%dT00:00:00+00:00')
    url = (f"{SUPABASE_URL}/rest/v1/fib_levels"
           f"?ticker=eq.{ticker}&run_ts=gte.{today_start}")
    try:
        requests.delete(url, headers=HEADERS_SB, timeout=10)
    except Exception:
        pass


def write_fib_row(ticker, swing_data, fibs, current_price, nearest, nearest_price, nearest_dist):
    """Insert one row into public.fib_levels."""
    now_ts = datetime.now(timezone.utc).isoformat()
    row = {
        'run_ts': now_ts,
        'ticker': ticker,
        'timeframe': 'daily',
        'swing_high': float(swing_data['swing_high']),
        'swing_high_date': swing_data.get('swing_high_date'),
        'swing_low': float(swing_data['swing_low']),
        'swing_low_date': swing_data.get('swing_low_date'),
        'direction': swing_data['direction'],
        'r_236': fibs['r_236'],
        'r_382': fibs['r_382'],
        'r_500': fibs['r_500'],
        'r_618': fibs['r_618'],
        'r_786': fibs['r_786'],
        'e_100': fibs['e_100'],
        'e_1272': fibs['e_1272'],
        'e_1618': fibs['e_1618'],
        'e_2618': fibs['e_2618'],
        'current_price': float(current_price),
        'nearest_level': nearest,
        'nearest_price': float(nearest_price),
        'nearest_distance_pct': float(nearest_dist),
    }
    url = f"{SUPABASE_URL}/rest/v1/fib_levels"
    try:
        r = requests.post(url, headers=HEADERS_SB, json=row, timeout=10)
        if r.status_code in (200, 201, 204):
            return True
        else:
            print(f"  [{ticker}] Supabase write error {r.status_code}: {r.text}")
            return False
    except Exception as e:
        print(f"  [{ticker}] Supabase write exception: {e}")
        return False


# ---------------------------------------------------------------------------
# STEP 6 -- MAIN + SUMMARY
# ---------------------------------------------------------------------------
def main():
    print("=" * 56)
    print("FIB ENGINE -- Fibonacci Level Scanner")
    print("=" * 56)
    print(f"Run: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"Tickers: {', '.join(TICKERS)}")
    print("-" * 56)

    # Fetch swings from Supabase
    print("\nFetching swing points from fib_swings...")
    raw_swings = fetch_swings()
    swing_map = build_swing_map(raw_swings)
    print(f"  Found swings for: {list(swing_map.keys()) if swing_map else 'NONE'}")

    results = []

    for ticker in TICKERS:
        print(f"\n[{ticker}] Processing...")

        # Get swing data
        if ticker in swing_map:
            sw = swing_map[ticker]
            swing_data = {
                'swing_high': float(sw.get('swing_high', 0)),
                'swing_high_date': sw.get('swing_high_date'),
                'swing_low': float(sw.get('swing_low', 0)),
                'swing_low_date': sw.get('swing_low_date'),
                'direction': sw.get('direction', 'up'),
            }
            print(f"  [{ticker}] Using T0 swing data")
        elif ticker == 'SLV':
            swing_data = SLV_DEFAULTS.copy()
            print(f"  [{ticker}] Using SLV defaults")
        else:
            print(f"  [{ticker}] No swing data -- skipping")
            results.append((ticker, None, None, None, None, None, None))
            continue

        # Fetch current price
        current_price = fetch_current_price(ticker)
        if current_price is None:
            print(f"  [{ticker}] No current price -- skipping")
            results.append((ticker, swing_data, None, None, None, None, None))
            continue

        # Compute fibs
        fibs = compute_fibs(
            swing_data['swing_high'],
            swing_data['swing_low'],
            swing_data['direction']
        )

        # Find nearest
        nearest, nearest_price, nearest_dist = find_nearest(fibs, current_price)

        # Write
        delete_today_rows(ticker)
        ok = write_fib_row(ticker, swing_data, fibs, current_price,
                           nearest, nearest_price, nearest_dist)
        status = '[OK]' if ok else '[FAIL]'

        print(f"  [{ticker}] ${swing_data['swing_low']:.2f} -> "
              f"${swing_data['swing_high']:.2f} | {swing_data['direction']} {status}")
        print(f"    Current: ${current_price:.2f} | "
              f"Nearest: {nearest} ${nearest_price:.2f} ({nearest_dist:.1f}% away)")

        results.append((ticker, swing_data, fibs, current_price,
                         nearest, nearest_price, nearest_dist))

    # Summary
    print("\n" + "=" * 56)
    print("FIB ENGINE -- RUN COMPLETE")
    print("=" * 56)
    for ticker, sw, fibs, price, nearest, nprice, ndist in results:
        if sw is None:
            print(f"{ticker:<6} -- no swing data")
            continue
        if fibs is None:
            print(f"{ticker:<6} -- swing ${sw['swing_low']:.2f}->${sw['swing_high']:.2f} | no price")
            continue

        star = lambda lvl: ' <--' if nearest == lvl else ''
        print(f"\n{ticker} | ${sw['swing_low']:.2f} -> ${sw['swing_high']:.2f} | "
              f"{sw['direction']} | Current: ${price:.2f}")
        print(f"  Nearest: {nearest} ${nprice:.2f} ({ndist:.1f}% away)")
        print(f"  23.6%: ${fibs['r_236']:.2f}{star('23.6%')}  "
              f"38.2%: ${fibs['r_382']:.2f}{star('38.2%')}  "
              f"50.0%: ${fibs['r_500']:.2f}{star('50.0%')}")
        print(f"  61.8%: ${fibs['r_618']:.2f}{star('61.8%')}  "
              f"78.6%: ${fibs['r_786']:.2f}{star('78.6%')}  "
              f"100%:  ${fibs['e_100']:.2f}{star('100%')}")
        print(f"  127.2%: ${fibs['e_1272']:.2f}{star('127.2%')}  "
              f"161.8%: ${fibs['e_1618']:.2f}{star('161.8%')}  "
              f"261.8%: ${fibs['e_2618']:.2f}{star('261.8%')}")
    print("\n" + "=" * 56)


if __name__ == '__main__':
    main()
