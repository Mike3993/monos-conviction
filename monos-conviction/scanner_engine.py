"""
MONOS Scanner Engine
Scans the full ticker universe across all clusters.
Fetches latest signals from GEX, DeMark, Scenario, Briefing, and Fib engines.
Computes an opportunity score (0-100) and classifies signal strength.
Writes results to public.scanner_candidates in Supabase.
"""

import os
import sys
import json
import time
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

SB_URL = os.environ.get('SUPABASE_URL', '')
SB_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')
POLYGON_KEY = os.environ.get('POLYGON_API_KEY', '')

if not SB_URL or not SB_KEY:
    print("[FATAL] SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set")
    sys.exit(1)

# ---------------------------------------------------------------------------
# UNIVERSE
# ---------------------------------------------------------------------------
CLUSTERS = {
    'METALS':    ['SLV', 'GLD', 'SIL', 'SILJ', 'GDX', 'COPX'],
    'INDEX':     ['SPY', 'QQQ', 'IWM', 'DIA'],
    'ENERGY':    ['XLE', 'USO', 'XOP'],
    'CRYPTO':    ['BITO', 'MSTR', 'COIN'],
    'PORTFOLIO': ['TLT', 'HYG', 'UUP'],
}

METALS_TICKERS = set(CLUSTERS['METALS'])

# ---------------------------------------------------------------------------
# SUPABASE REST HELPERS
# ---------------------------------------------------------------------------
HEADERS = {
    'apikey': SB_KEY,
    'Authorization': 'Bearer ' + SB_KEY,
    'Content-Type': 'application/json',
    'Prefer': 'return=representation',
}

def sb_select(table, params):
    """GET from Supabase REST. params is a dict of query string params."""
    url = SB_URL + '/rest/v1/' + table
    r = requests.get(url, headers=HEADERS, params=params, timeout=15)
    if r.status_code >= 400:
        return []
    try:
        return r.json()
    except Exception:
        return []

def sb_delete(table, params):
    """DELETE from Supabase REST."""
    url = SB_URL + '/rest/v1/' + table
    h = dict(HEADERS)
    h['Prefer'] = 'return=minimal'
    r = requests.delete(url, headers=h, params=params, timeout=15)
    return r.status_code < 400

def sb_insert(table, rows):
    """POST bulk insert to Supabase REST."""
    if not rows:
        return True
    url = SB_URL + '/rest/v1/' + table
    r = requests.post(url, headers=HEADERS, json=rows, timeout=30)
    if r.status_code >= 400:
        print("  [!] Insert error:", r.status_code, r.text[:200])
        return False
    return True

# ---------------------------------------------------------------------------
# POLYGON PRICE FETCH
# ---------------------------------------------------------------------------
_price_cache = {}
_polygon_calls = 0

def get_price_polygon(ticker):
    """Fetch previous close from Polygon. Rate-limited for starter tier."""
    global _polygon_calls
    if ticker in _price_cache:
        return _price_cache[ticker]
    if not POLYGON_KEY:
        return None
    if _polygon_calls >= 5:
        print("  [*] Polygon rate limit pause (60s)...")
        time.sleep(62)
        _polygon_calls = 0
    url = 'https://api.polygon.io/v2/aggs/ticker/{}/prev'.format(ticker)
    try:
        r = requests.get(url, params={'apiKey': POLYGON_KEY}, timeout=10)
        _polygon_calls += 1
        if r.status_code == 200:
            data = r.json()
            results = data.get('results', [])
            if results:
                price = results[0].get('c')
                _price_cache[ticker] = price
                return price
    except Exception as e:
        print("  [!] Polygon error for {}: {}".format(ticker, e))
    _price_cache[ticker] = None
    return None

# ---------------------------------------------------------------------------
# FETCH ENGINE DATA FOR ONE TICKER
# ---------------------------------------------------------------------------
def fetch_ticker_data(ticker, cluster):
    """Fetch latest signals from all engines for a single ticker."""
    data = {}

    # GEX snapshot
    rows = sb_select('gex_snapshots', {
        'select': '*',
        'ticker': 'eq.' + ticker,
        'order': 'run_ts.desc',
        'limit': '1',
    })
    data['gex'] = rows[0] if rows else None

    # DeMark signal (daily)
    rows = sb_select('demark_signals', {
        'select': '*',
        'ticker': 'eq.' + ticker,
        'timeframe': 'eq.daily',
        'order': 'run_ts.desc',
        'limit': '1',
    })
    data['demark'] = rows[0] if rows else None

    # Scenario synthesis
    rows = sb_select('scenario_synthesis', {
        'select': '*',
        'ticker': 'eq.' + ticker,
        'order': 'run_ts.desc',
        'limit': '1',
    })
    data['scenario'] = rows[0] if rows else None

    # Briefing report
    rows = sb_select('briefing_reports', {
        'select': '*',
        'ticker': 'eq.' + ticker,
        'order': 'created_at.desc',
        'limit': '1',
    })
    data['briefing'] = rows[0] if rows else None

    # Fib levels
    rows = sb_select('fib_levels', {
        'select': '*',
        'ticker': 'eq.' + ticker,
        'order': 'run_ts.desc',
        'limit': '1',
    })
    data['fib'] = rows[0] if rows else None

    # Price: Polygon for METALS, else gex spot_price
    if cluster == 'METALS':
        data['spot'] = get_price_polygon(ticker)
    else:
        data['spot'] = float(data['gex']['spot_price']) if data['gex'] and data['gex'].get('spot_price') else None

    return data

# ---------------------------------------------------------------------------
# OPPORTUNITY SCORE (0-100)
# ---------------------------------------------------------------------------
def compute_score(data):
    """Compute opportunity score from 5 components."""
    score = 0.0
    details = {}

    # COMPONENT 1 -- DeMark signal (0-25)
    dm = data.get('demark')
    dm_pts = 0
    if dm:
        state = (dm.get('signal_state') or '').upper()
        count = dm.get('setup_count') or 0
        if 'PERFECT' in state and '9' in state:
            dm_pts = 25
        elif '9' in state:
            dm_pts = 18
        elif count >= 7:
            dm_pts = 12
        elif count >= 5:
            dm_pts = 7
    score += dm_pts
    details['demark'] = dm_pts

    # COMPONENT 2 -- Scenario confidence (0-25)
    sc = data.get('scenario')
    sc_pts = 0
    if sc:
        conf = sc.get('confidence_score')
        if conf is None:
            # fallback: use primary_probability
            conf = sc.get('primary_probability')
        if conf is not None:
            sc_pts = round(float(conf) * 25, 1)
    score += sc_pts
    details['scenario'] = sc_pts

    # COMPONENT 3 -- GEX alignment (0-20)
    gex = data.get('gex')
    gex_pts = 0
    if gex and sc:
        gex_regime = (gex.get('gex_regime') or '').upper()
        bias = (sc.get('primary_bias') or '').upper()
        if bias == 'BEARISH' and gex_regime == 'NEGATIVE':
            gex_pts = 20
        elif bias == 'BULLISH' and gex_regime == 'POSITIVE':
            gex_pts = 20
        elif gex_regime == 'NEUTRAL':
            gex_pts = 10
        # misaligned = 0
    elif gex:
        gex_regime = (gex.get('gex_regime') or '').upper()
        if gex_regime == 'NEUTRAL':
            gex_pts = 10
    score += gex_pts
    details['gex'] = gex_pts

    # COMPONENT 4 -- Regime from briefing (0-20)
    br = data.get('briefing')
    br_pts = 0
    if br:
        regime = (br.get('regime_label') or '').upper()
        if 'BULL' in regime:
            br_pts = 20
        elif 'NEUTRAL' in regime:
            br_pts = 10
        elif 'BEAR' in regime:
            br_pts = 5
    score += br_pts
    details['regime'] = br_pts

    # COMPONENT 5 -- Fib proximity (0-10)
    fb = data.get('fib')
    fib_pts = 0
    if fb:
        dist = fb.get('nearest_distance_pct')
        if dist is not None:
            dist = abs(float(dist))
            if dist <= 2:
                fib_pts = 10
            elif dist <= 5:
                fib_pts = 7
            elif dist <= 10:
                fib_pts = 4
    score += fib_pts
    details['fib'] = fib_pts

    # Signal classification
    score = round(score, 1)
    if score >= 70:
        signal = 'STRONG_SIGNAL'
    elif score >= 50:
        signal = 'MODERATE_SIGNAL'
    elif score >= 30:
        signal = 'WEAK_SIGNAL'
    else:
        signal = 'NO_SIGNAL'

    return score, signal, details

# ---------------------------------------------------------------------------
# BUILD ROW
# ---------------------------------------------------------------------------
def build_row(ticker, cluster, data, score, signal):
    """Build a scanner_candidates row dict."""
    gex = data.get('gex')
    dm = data.get('demark')
    sc = data.get('scenario')
    fb = data.get('fib')

    return {
        'ticker': ticker,
        'cluster': cluster,
        'opportunity_score': score,
        'regime': (sc.get('primary_bias') or 'UNKNOWN').upper() if sc else 'UNKNOWN',
        'signal': signal,
        'iv_rank': None,  # not computed yet
        'gex_regime': (gex.get('gex_regime') or '').upper() if gex else None,
        'demark_state': dm.get('signal_state') if dm else None,
        'demark_count': dm.get('setup_count') if dm else None,
        'scenario_bias': (sc.get('primary_bias') or '').upper() if sc else None,
        'scenario_prob': float(sc.get('primary_probability')) if sc and sc.get('primary_probability') else None,
        'spot_price': data.get('spot'),
        'approved': False,
    }

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=" * 50)
    print("SCANNER ENGINE -- RUN START")
    print("=" * 50)
    print("Date:", date.today().isoformat())
    print("Universe:", sum(len(v) for v in CLUSTERS.values()), "tickers across", len(CLUSTERS), "clusters")
    print()

    all_rows = []

    for cluster, tickers in CLUSTERS.items():
        print("[{}] {} tickers: {}".format(cluster, len(tickers), ', '.join(tickers)))
        for ticker in tickers:
            data = fetch_ticker_data(ticker, cluster)

            # Quick summary of what we found
            found = []
            if data['gex']:
                found.append('GEX')
            if data['demark']:
                found.append('DM')
            if data['scenario']:
                found.append('SC')
            if data['briefing']:
                found.append('BR')
            if data['fib']:
                found.append('FIB')
            if data['spot']:
                found.append('$' + str(round(data['spot'], 2)))

            score, signal, details = compute_score(data)

            row = build_row(ticker, cluster, data, score, signal)
            all_rows.append(row)

            icon = '*' if signal == 'STRONG_SIGNAL' else '+' if signal == 'MODERATE_SIGNAL' else '.' if signal == 'WEAK_SIGNAL' else ' '
            print("  {} {} score={:5.1f}  {}  [{}]".format(
                icon, ticker.ljust(5), score, signal.ljust(16), ' '.join(found)))

        print()

    # Delete today's rows
    today_str = date.today().isoformat()
    print("Deleting today's rows (>= {})...".format(today_str))
    sb_delete('scanner_candidates', {'run_ts': 'gte.' + today_str})

    # Insert
    print("Inserting {} rows...".format(len(all_rows)))
    ok = sb_insert('scanner_candidates', all_rows)
    if ok:
        print("Insert OK")
    else:
        print("[!] Insert failed")

    # Summary
    strong = [r for r in all_rows if r['signal'] == 'STRONG_SIGNAL']
    moderate = [r for r in all_rows if r['signal'] == 'MODERATE_SIGNAL']
    weak = [r for r in all_rows if r['signal'] == 'WEAK_SIGNAL']
    top5 = sorted(all_rows, key=lambda r: r['opportunity_score'], reverse=True)[:5]

    print()
    print("=" * 50)
    print("SCANNER ENGINE -- RUN COMPLETE")
    print("=" * 50)
    print("Tickers evaluated: {}".format(len(all_rows)))
    print("Strong signals:    {}".format(len(strong)))
    print("Moderate signals:  {}".format(len(moderate)))
    print("Weak signals:      {}".format(len(weak)))
    print()
    print("Top 5 by score:")
    print("  {:<6} {:<10} {:>5}  {:<16} {:<8} {}".format(
        'TICKER', 'CLUSTER', 'SCORE', 'SIGNAL', 'BIAS', 'DEMARK'))
    print("  " + "-" * 60)
    for r in top5:
        dm_str = (r['demark_state'] or '--')[:12]
        if r['demark_count']:
            dm_str = dm_str + ' ' + str(r['demark_count']) + '/9'
        print("  {:<6} {:<10} {:>5.1f}  {:<16} {:<8} {}".format(
            r['ticker'],
            r['cluster'],
            r['opportunity_score'],
            r['signal'],
            (r['scenario_bias'] or '--')[:8],
            dm_str,
        ))
    print()


if __name__ == '__main__':
    main()
