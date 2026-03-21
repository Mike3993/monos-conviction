"""
MONOS DeMarK Engine -- TD Sequential Setup Scanner
Scans SLV, GLD, GDX, SILJ, SIL for TD Buy/Sell Setups on daily bars.
Writes results to public.demark_signals in Supabase.
"""

import os
import sys
import json
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
                print(f"[demark] Loaded {len(tickers)} tickers from ticker_universe")
                return tickers
    except Exception as e:
        print(f"[demark] ticker_universe fetch failed: {e}")
    return _DEFAULT_TICKERS

TICKERS = _load_universe()
HEADERS_SB = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal'
}

# ---------------------------------------------------------------------------
# STEP 2 -- FETCH PRICE DATA
# ---------------------------------------------------------------------------
def fetch_daily_bars(ticker):
    """Fetch up to 60 daily bars from Polygon aggs endpoint."""
    end = date.today()
    start = end - timedelta(days=90)
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
    params = {
        'adjusted': 'true',
        'sort': 'asc',
        'limit': 60,
        'apiKey': POLYGON_API_KEY
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        results = data.get('results', [])
        if not results or len(results) < 13:
            print(f"  [{ticker}] Only {len(results) if results else 0} bars -- skipping (need >=13)")
            return None
        closes = [bar['c'] for bar in results]
        highs  = [bar['h'] for bar in results]
        lows   = [bar['l'] for bar in results]
        return {'closes': closes, 'highs': highs, 'lows': lows}
    except Exception as e:
        print(f"  [{ticker}] Fetch error: {e}")
        return None

# ---------------------------------------------------------------------------
# STEP 3 -- TD SETUP COUNTER
# ---------------------------------------------------------------------------
def td_setup(closes, highs, lows):
    """Run TD Sequential Setup logic on bar arrays."""
    buy_count = 0
    sell_count = 0
    buy_complete = False
    sell_complete = False
    buy_perfect = False
    sell_perfect = False

    last_buy_complete = False
    last_sell_complete = False
    recycled = False

    for i in range(4, len(closes)):
        # Buy setup: close < close 4 bars ago
        if closes[i] < closes[i - 4]:
            if last_buy_complete:
                # Recycle: new buy setup starting after a completed one
                buy_count = 1
                last_buy_complete = False
                buy_complete = False
                buy_perfect = False
                recycled = True
            else:
                buy_count += 1
            sell_count = 0
            if buy_count == 9:
                buy_complete = True
                last_buy_complete = True
                bar8_low = lows[i - 1]
                bar9_low = lows[i]
                ref_low = min(lows[i - 3], lows[i - 2])
                buy_perfect = (bar8_low <= ref_low or bar9_low <= ref_low)
        # Sell setup: close > close 4 bars ago
        elif closes[i] > closes[i - 4]:
            if last_sell_complete:
                sell_count = 1
                last_sell_complete = False
                sell_complete = False
                sell_perfect = False
                recycled = True
            else:
                sell_count += 1
            buy_count = 0
            if sell_count == 9:
                sell_complete = True
                last_sell_complete = True
                bar8_high = highs[i - 1]
                bar9_high = highs[i]
                ref_high = max(highs[i - 3], highs[i - 2])
                sell_perfect = (bar8_high >= ref_high or bar9_high >= ref_high)
        else:
            buy_count = 0
            sell_count = 0

    # Use whichever is active
    if buy_count > sell_count:
        return {
            'setup_direction': 'buy',
            'setup_count': buy_count,
            'setup_complete': buy_complete,
            'setup_perfect': buy_perfect if buy_complete else None,
            'recycled': recycled,
        }
    else:
        return {
            'setup_direction': 'sell',
            'setup_count': sell_count,
            'setup_complete': sell_complete,
            'setup_perfect': sell_perfect if sell_complete else None,
            'recycled': recycled,
        }

# ---------------------------------------------------------------------------
# STEP 4 -- SIGNAL STATE + STRENGTH
# ---------------------------------------------------------------------------
def compute_signal(setup):
    """Derive signal_state and signal_strength from setup results."""
    if setup['setup_complete'] and setup['setup_perfect']:
        return 'SETUP_9_PERFECT', 0.45
    elif setup['setup_complete']:
        return 'SETUP_9_IMPERFECT', 0.30
    elif setup['setup_count'] >= 6:
        return 'SETUP_IN_PROGRESS', round(setup['setup_count'] / 9 * 0.25, 2)
    else:
        return 'IDLE', 0.0

# ---------------------------------------------------------------------------
# STEP 5 -- WRITE TO SUPABASE
# ---------------------------------------------------------------------------
def write_signal(ticker, setup, signal_state, signal_strength):
    """Insert one row into public.demark_signals."""
    now_ts = datetime.now(timezone.utc).isoformat()
    row = {
        'run_ts': now_ts,
        'ticker': ticker,
        'timeframe': 'daily',
        'setup_direction': setup['setup_direction'],
        'setup_count': setup['setup_count'],
        'setup_complete': setup['setup_complete'],
        'setup_perfect': setup['setup_perfect'],
        'countdown_active': False,
        'countdown_count': 0,
        'countdown_complete': False,
        'signal_state': signal_state,
        'signal_strength': float(signal_strength),
        'recycled': setup.get('recycled', False),
    }
    url = f"{SUPABASE_URL}/rest/v1/demark_signals"
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
STATE_ICONS = {
    'IDLE': 'O',
    'SETUP_IN_PROGRESS': '@',
    'SETUP_9_IMPERFECT': '#',
    'SETUP_9_PERFECT': '*',
}

def main():
    print("=" * 50)
    print("DEMARK ENGINE -- TD Sequential Scanner")
    print("=" * 50)
    print(f"Run: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"Tickers: {', '.join(TICKERS)}")
    print("-" * 50)

    results = []

    for ticker in TICKERS:
        print(f"\n[{ticker}] Fetching daily bars...")
        bars = fetch_daily_bars(ticker)
        if bars is None:
            results.append((ticker, None, 'SKIP', 0.0))
            continue

        print(f"  [{ticker}] {len(bars['closes'])} bars loaded")
        setup = td_setup(bars['closes'], bars['highs'], bars['lows'])
        signal_state, signal_strength = compute_signal(setup)

        ok = write_signal(ticker, setup, signal_state, signal_strength)
        status = '[OK]' if ok else '[FAIL]'
        print(f"  [{ticker}] {setup['setup_direction'].upper()} {setup['setup_count']}/9 "
              f"-> {signal_state} (str={signal_strength}) {status}")
        results.append((ticker, setup, signal_state, signal_strength))

    # Summary table
    print("\n" + "=" * 50)
    print("DEMARK ENGINE -- RUN COMPLETE")
    print("=" * 50)
    print(f"{'TICKER':<8} {'DIR':<6} {'COUNT':<8} {'STATE':<22} {'STR':<6} {'ICON'}")
    print("-" * 58)
    for ticker, setup, state, strength in results:
        if setup is None:
            print(f"{ticker:<8} {'--':<6} {'--':<8} {'SKIP':<22} {'--':<6} -")
        else:
            icon = STATE_ICONS.get(state, '?')
            direction = setup['setup_direction'].upper()
            count_str = f"{setup['setup_count']}/9"
            perfect_tag = ''
            if setup['setup_complete'] and setup['setup_perfect']:
                perfect_tag = ' [PERFECT]'
            elif setup['setup_complete']:
                perfect_tag = ' [IMPERFECT]'
            print(f"{ticker:<8} {direction:<6} {count_str:<8} {state:<22} {strength:<6} {icon}{perfect_tag}")
    print("=" * 50)

if __name__ == '__main__':
    main()
