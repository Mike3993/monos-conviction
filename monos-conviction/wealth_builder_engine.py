"""
MONOS Wealth Builder Engine
Scores every ticker in ticker_universe across 5 dimensions (0-20 each = 100 total):
  1. Alignment     - engine agreement on direction
  2. Convexity     - is it cheap to own convexity here
  3. Zone          - proximity to structural zones
  4. Microstructure - GEX regime, DeMark, flow quality
  5. Portfolio Fit - diversification, tier, bias alignment

Outputs ranked opportunity_queue table with deployment readiness,
portfolio fit label, draftability, top reason, and block reason.
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

if not SUPABASE_URL or not SUPABASE_KEY:
    print("[FAIL] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    sys.exit(1)

HEADERS_SB = {
    'apikey': SUPABASE_KEY,
    'Authorization': 'Bearer ' + SUPABASE_KEY,
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal',
}

_DEFAULT_TICKERS = ['SLV', 'GLD', 'GDX', 'SILJ', 'SIL']


# ---------------------------------------------------------------------------
# SUPABASE HELPERS
# ---------------------------------------------------------------------------
def sb_get(table, params):
    """Generic Supabase REST GET. Returns list of rows or []."""
    url = SUPABASE_URL + '/rest/v1/' + table
    try:
        r = requests.get(url, headers={
            'apikey': SUPABASE_KEY,
            'Authorization': 'Bearer ' + SUPABASE_KEY,
        }, params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
        return []
    except Exception:
        return []


def sb_delete(table, params):
    """Delete rows matching params."""
    url = SUPABASE_URL + '/rest/v1/' + table
    try:
        r = requests.delete(url, headers=HEADERS_SB, params=params, timeout=10)
        return r.status_code in (200, 204)
    except Exception:
        return False


def sb_insert(table, rows):
    """Bulk insert rows."""
    url = SUPABASE_URL + '/rest/v1/' + table
    try:
        r = requests.post(url, headers=HEADERS_SB, json=rows, timeout=15)
        if r.status_code in (200, 201):
            return True
        print("  [!] Insert error %d: %s" % (r.status_code, r.text[:200]))
        return False
    except Exception as e:
        print("  [!] Insert exception: %s" % e)
        return False


# ---------------------------------------------------------------------------
# LOAD TICKER UNIVERSE
# ---------------------------------------------------------------------------
def load_universe():
    """Load tickers and their tiers from ticker_universe."""
    rows = sb_get('ticker_universe', {
        'select': 'ticker,tier',
        'is_active': 'eq.true',
        'order': 'ticker.asc',
    })
    if rows:
        print("[wealth] Loaded %d tickers from ticker_universe" % len(rows))
        return rows
    print("[wealth] ticker_universe empty, using defaults")
    return [{'ticker': t, 'tier': 'WATCHLIST'} for t in _DEFAULT_TICKERS]


# ---------------------------------------------------------------------------
# FETCH ENGINE DATA FOR ONE TICKER
# ---------------------------------------------------------------------------
def fetch_engine_data(ticker):
    """Fetch most recent row from each engine table for a ticker."""
    def latest(table, extra=None):
        params = {'select': '*', 'ticker': 'eq.' + ticker,
                  'order': 'run_ts.desc', 'limit': '1'}
        if extra:
            params.update(extra)
        rows = sb_get(table, params)
        return rows[0] if rows else None

    scenario = latest('scenario_synthesis')
    gex      = latest('gex_snapshots')
    sym      = latest('symmetry_snapshots')
    dm       = latest('demark_signals', {'timeframe': 'eq.daily'})
    fib      = latest('fib_levels')
    con      = latest('conflict_states')
    trig     = latest('trigger_state')
    flow     = latest('flow_snapshots')

    return scenario, gex, sym, dm, fib, con, trig, flow


# ---------------------------------------------------------------------------
# LOAD ACTIVE POSITIONS (for portfolio fit)
# ---------------------------------------------------------------------------
def load_active_positions():
    """Get list of tickers with active positions."""
    rows = sb_get('positions', {
        'select': 'ticker',
        'is_active': 'eq.true',
    })
    return list(set(r['ticker'] for r in rows if r.get('ticker')))


# ---------------------------------------------------------------------------
# SCORING FUNCTIONS
# ---------------------------------------------------------------------------
def score_alignment(con, trig):
    """DIMENSION 1 -- Alignment (0-20). How well do engines agree?"""
    alignment = 0
    if con:
        cs = con.get('conflict_state', '')
        if cs == 'ALIGNED':
            alignment += 12
        elif cs == 'MIXED':
            alignment += 6
        # CONTRADICTED = 0
    if trig:
        ts = trig.get('trigger_state', '')
        if ts == 'ACTIVE':
            alignment += 8
        elif ts == 'NEAR':
            alignment += 5
        elif ts == 'FORMING':
            alignment += 2
    return min(20, alignment)


def score_convexity(sym):
    """DIMENSION 2 -- Convexity Value (0-20). Is it cheap to own?"""
    if not sym:
        return 10  # neutral if unknown
    cs = sym.get('convexity_state', '')
    if cs == 'CHEAP':
        return 20
    elif cs == 'FAIR':
        return 12
    elif cs == 'RICH':
        return 5
    elif cs == 'VERY_RICH':
        return 0
    return 10


def score_zone(fib, gex):
    """DIMENSION 3 -- Zone Proximity (0-20). Near constructive zone?"""
    zone = 0
    if fib:
        d = fib.get('nearest_distance_pct', 999)
        if isinstance(d, (int, float)):
            if d <= 2:
                zone += 10
            elif d <= 5:
                zone += 6
            elif d <= 10:
                zone += 3
    if gex:
        spot = gex.get('spot_price', 0) or 0
        pw = gex.get('put_wall', 0) or 0
        gf = gex.get('gamma_flip', 0) or 0
        if spot and pw:
            dist_pw = abs(spot - pw) / spot * 100
            if dist_pw <= 3:
                zone += 5
        if spot and gf:
            dist_gf = abs(spot - gf) / spot * 100
            if dist_gf <= 3:
                zone += 5
    return min(20, zone)


def score_microstructure(gex, dm, flow):
    """DIMENSION 4 -- Microstructure Quality (0-20)."""
    micro = 0
    if gex:
        regime = gex.get('gex_regime', '')
        if regime == 'POSITIVE':
            micro += 8
        elif regime == 'NEUTRAL':
            micro += 4
        # NEGATIVE = 0 (amplifying = risky for new entries)
    if dm:
        state = dm.get('signal_state', '') or ''
        count = dm.get('setup_count', 0) or dm.get('active_count', 0) or 0
        if isinstance(count, str):
            try:
                count = int(count)
            except ValueError:
                count = 0
        if 'PERFECT' in state.upper():
            micro += 8
        elif 'IMPERFECT' in state.upper():
            micro += 6
        elif 'COUNTDOWN' in state.upper():
            micro += 7
        elif count >= 7:
            micro += 4
        elif count >= 5:
            micro += 2
    if flow:
        sig = flow.get('flow_signal', '') or ''
        conv = flow.get('conviction_score', 0) or 0
        if isinstance(conv, str):
            try:
                conv = int(float(conv))
            except ValueError:
                conv = 0
        if 'BULLISH' in sig.upper():
            micro += min(4, conv // 20)
    return min(20, micro)


def score_portfolio_fit(ticker, scenario, tier, active_tickers):
    """DIMENSION 5 -- Portfolio Fit (0-20)."""
    fit = 0
    if ticker in active_tickers:
        fit += 8   # known ticker, existing thesis
    else:
        fit += 12  # new ticker, diversification value
    if tier == 'WATCHLIST':
        fit += 5
    elif tier == 'POSITION':
        fit += 3
    elif tier == 'RESEARCH':
        fit += 1
    if scenario:
        bias = scenario.get('overall_bias', '')
        if bias == 'BULLISH':
            fit += 3
    return min(20, fit)


# ---------------------------------------------------------------------------
# LABEL FUNCTIONS
# ---------------------------------------------------------------------------
def label_deployment(composite):
    if composite >= 75:
        return 'DEPLOY_NOW'
    elif composite >= 55:
        return 'DEPLOY_SOON'
    elif composite >= 35:
        return 'WATCH'
    return 'WAIT'


def label_fit(fit):
    if fit >= 16:
        return 'EXCELLENT'
    elif fit >= 11:
        return 'GOOD'
    elif fit >= 6:
        return 'MODERATE'
    return 'POOR'


def label_draftability(gex, fib, sym):
    has_gex = gex is not None
    has_fib = fib is not None
    has_iv  = sym is not None
    if has_gex and has_fib and has_iv:
        return 'READY'
    elif has_gex and has_iv:
        return 'PARTIAL'
    return 'MISSING_DATA'


def compute_reasons(alignment, convexity, zone, micro, fit):
    reasons = []
    if alignment >= 16:
        reasons.append('ALIGNED engines')
    if convexity >= 16:
        reasons.append('Cheap convexity')
    if zone >= 14:
        reasons.append('Near key zone')
    if micro >= 14:
        reasons.append('Strong microstructure')
    if fit >= 14:
        reasons.append('Good portfolio fit')
    return ' + '.join(reasons[:2]) if reasons else 'No strong catalyst'


def compute_blocks(con, sym):
    blocks = []
    if con and con.get('conflict_state') == 'CONTRADICTED':
        blocks.append('CONTRADICTED')
    if sym and sym.get('convexity_state') == 'VERY_RICH':
        blocks.append('CONVEXITY_VERY_RICH')
    return ', '.join(blocks) if blocks else None


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print()
    print("=" * 55)
    print("  WEALTH BUILDER ENGINE")
    print("=" * 55)
    print()

    universe = load_universe()
    universe_map = {r['ticker']: r for r in universe}
    tickers = [r['ticker'] for r in universe]
    active_tickers = load_active_positions()
    print("[wealth] Active position tickers: %s" % ', '.join(active_tickers) if active_tickers else "[wealth] No active positions found")

    run_ts = datetime.now(timezone.utc).isoformat()
    results = []

    for ticker in tickers:
        print("  [%s] fetching engine data..." % ticker, end=' ')
        scenario, gex, sym, dm, fib, con, trig, flow = fetch_engine_data(ticker)

        tier = universe_map.get(ticker, {}).get('tier', 'RESEARCH')

        # Score 5 dimensions
        s_alignment = score_alignment(con, trig)
        s_convexity = score_convexity(sym)
        s_zone      = score_zone(fib, gex)
        s_micro     = score_microstructure(gex, dm, flow)
        s_fit       = score_portfolio_fit(ticker, scenario, tier, active_tickers)
        composite   = s_alignment + s_convexity + s_zone + s_micro + s_fit

        # Labels
        deployment  = label_deployment(composite)
        fit_label   = label_fit(s_fit)
        draftable   = label_draftability(gex, fib, sym)
        top_reason  = compute_reasons(s_alignment, s_convexity, s_zone, s_micro, s_fit)
        block       = compute_blocks(con, sym)

        # Engine state passthrough
        trigger_st  = trig.get('trigger_state', '') if trig else None
        conflict_st = con.get('conflict_state', '') if con else None
        convex_st   = sym.get('convexity_state', '') if sym else None
        bias_st     = scenario.get('overall_bias', '') if scenario else None

        results.append({
            'run_ts': run_ts,
            'ticker': ticker,
            'alignment_score': s_alignment,
            'convexity_score': s_convexity,
            'zone_score': s_zone,
            'microstructure_score': s_micro,
            'portfolio_fit_score': s_fit,
            'composite_score': composite,
            'deployment_readiness': deployment,
            'portfolio_fit': fit_label,
            'draftability': draftable,
            'trigger_state': trigger_st,
            'conflict_state': conflict_st,
            'convexity_state': convex_st,
            'overall_bias': bias_st,
            'top_reason': top_reason,
            'block_reason': block,
        })

        flags = []
        if gex: flags.append('gex')
        if dm:  flags.append('dm')
        if fib: flags.append('fib')
        if sym: flags.append('sym')
        if con: flags.append('con')
        if trig: flags.append('trig')
        if flow: flags.append('flow')
        if scenario: flags.append('sc')
        print("score=%d %s [%s]" % (composite, deployment, ','.join(flags)))

    # Rank by composite descending
    results.sort(key=lambda r: r['composite_score'], reverse=True)
    for i, r in enumerate(results):
        r['overall_rank'] = i + 1

    # Write to Supabase
    today_str = date.today().isoformat()
    sb_delete('opportunity_queue', {
        'run_ts': 'gte.' + today_str + 'T00:00:00Z',
    })

    ok = sb_insert('opportunity_queue', results)

    # Print summary
    print()
    print("=" * 72)
    print("  WEALTH BUILDER ENGINE -- RUN COMPLETE")
    print("=" * 72)
    print("  Tickers evaluated: %d" % len(results))
    print("  Written to Supabase: %s" % ("OK" if ok else "FAILED"))
    print()
    print("  Top opportunities:")
    print("  %-4s %-8s %5s  %-12s %-10s %-9s  %s" % (
        'RANK', 'TICKER', 'SCORE', 'READINESS', 'FIT', 'DRAFT', 'REASON'))
    print("  " + "-" * 70)
    for r in results:
        print("  %-4d %-8s %5d  %-12s %-10s %-9s  %s" % (
            r['overall_rank'], r['ticker'], r['composite_score'],
            r['deployment_readiness'], r['portfolio_fit'],
            r['draftability'], r['top_reason']))
    print()

    blocked = [r for r in results if r.get('block_reason')]
    if blocked:
        print("  Blocked tickers:")
        for r in blocked:
            print("    %s -- %s" % (r['ticker'], r['block_reason']))
        print()

    print("  Dimension breakdown (A=align C=convex Z=zone M=micro F=fit):")
    print("  %-8s  %3s %3s %3s %3s %3s = %5s" % (
        'TICKER', 'A', 'C', 'Z', 'M', 'F', 'TOTAL'))
    for r in results:
        print("  %-8s  %3d %3d %3d %3d %3d = %5d" % (
            r['ticker'],
            r['alignment_score'], r['convexity_score'],
            r['zone_score'], r['microstructure_score'],
            r['portfolio_fit_score'], r['composite_score']))
    print()


if __name__ == '__main__':
    main()
