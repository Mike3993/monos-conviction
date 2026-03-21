"""
MONOS Scenario Synthesis Engine
Reads latest signals from GEX, DeMark, Reload, Briefing engines.
Scores directional agreement, synthesizes probability-weighted scenarios,
and writes results to public.scenario_synthesis in Supabase.
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
                print(f"[scenario] Loaded {len(tickers)} tickers from ticker_universe")
                return tickers
    except Exception as e:
        print(f"[scenario] ticker_universe fetch failed: {e}")
    return _DEFAULT_TICKERS

TICKERS = _load_universe()
HEADERS_SB = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal'
}

# ---------------------------------------------------------------------------
# STEP 2 -- FETCH ENGINE OUTPUTS
# ---------------------------------------------------------------------------
def sb_get(table, params):
    """Generic Supabase REST GET. Returns list of rows or []."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    try:
        r = requests.get(url, headers={
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}',
        }, params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
        else:
            return []
    except Exception:
        return []


def fetch_engine_data(ticker):
    """Fetch most recent row from each engine table for a ticker."""

    # GEX
    gex_rows = sb_get('gex_snapshots', {
        'select': '*',
        'ticker': f'eq.{ticker}',
        'order': 'run_ts.desc',
        'limit': '1',
    })
    gex = gex_rows[0] if gex_rows else None

    # DeMark
    dm_rows = sb_get('demark_signals', {
        'select': '*',
        'ticker': f'eq.{ticker}',
        'timeframe': 'eq.daily',
        'order': 'run_ts.desc',
        'limit': '1',
    })
    dm = dm_rows[0] if dm_rows else None

    # Reload
    reload_rows = sb_get('reload_stage_log', {
        'select': '*',
        'ticker': f'eq.{ticker}',
        'order': 'run_ts.desc',
        'limit': '1',
    })
    reload = reload_rows[0] if reload_rows else None

    # Briefing (regime)
    brief_rows = sb_get('briefing_reports', {
        'select': '*',
        'ticker': f'eq.{ticker}',
        'order': 'created_at.desc',
        'limit': '1',
    })
    brief = brief_rows[0] if brief_rows else None

    return gex, dm, reload, brief


# ---------------------------------------------------------------------------
# STEP 3 -- SCORE EACH ENGINE
# ---------------------------------------------------------------------------
def score_gex(gex):
    """GEX signal: NEGATIVE -> -1, POSITIVE -> +1, else 0."""
    if not gex:
        return None
    regime = (gex.get('gex_regime') or '').upper()
    if regime == 'NEGATIVE':
        return -1
    elif regime == 'POSITIVE':
        return 1
    return 0


def score_demark(dm):
    """DeMark signal based on direction + strength threshold."""
    if not dm:
        return None
    direction = (dm.get('setup_direction') or '').lower()
    strength = dm.get('signal_strength') or 0
    if direction == 'buy' and strength > 0.3:
        return 1
    elif direction == 'sell' and strength > 0.3:
        return -1
    return 0


def score_reload(reload):
    """Reload signal from reload_stage."""
    if not reload:
        return None
    stage = (reload.get('reload_stage') or '').upper()
    if stage == 'ACCUMULATION':
        return 1
    elif stage == 'EXHAUSTION':
        return -1
    elif stage == 'ZONE_WATCH':
        return 0
    return 0


def score_regime(brief):
    """Regime signal from briefing_reports regime_label."""
    if not brief:
        return None
    label = (brief.get('regime_label') or '').upper()
    if 'BULL' in label:
        return 1
    elif 'BEAR' in label:
        return -1
    elif 'NEUTRAL' in label:
        return 0
    return 0


def score_ew():
    """EW signal -- placeholder, no engine yet."""
    return 0


# ---------------------------------------------------------------------------
# STEP 4 -- SYNTHESIZE SCENARIOS
# ---------------------------------------------------------------------------
def synthesize(gex_signal, demark_signal, reload_signal, regime_signal, ew_signal):
    """Combine engine signals into scenario probabilities."""
    signals = [gex_signal, demark_signal, reload_signal, regime_signal, ew_signal]
    active = [s for s in signals if s is not None and s != 0]
    score = sum(active) / len(active) if active else 0

    # Count agreements
    if active and score != 0:
        sign = 1 if score > 0 else -1
        engines_agreement = sum(1 for s in active if s == sign)
    else:
        engines_agreement = 0
    engines_total = len([s for s in signals if s is not None])

    if score <= -0.5:
        primary_scenario = 'CONTINUATION_DOWN'
        primary_probability = min(0.45 + abs(score) * 0.2, 0.75)
        alt_scenario = 'CHOP_CONSOLIDATION'
        alt_probability = round(1 - primary_probability - 0.1, 2)
        low_prob_scenario = 'SHARP_REVERSAL'
        low_prob_probability = 0.10
        overall_bias = 'BEARISH'
    elif score >= 0.5:
        primary_scenario = 'BULLISH_CONTINUATION'
        primary_probability = min(0.45 + score * 0.2, 0.75)
        alt_scenario = 'CHOP_CONSOLIDATION'
        alt_probability = round(1 - primary_probability - 0.1, 2)
        low_prob_scenario = 'SHARP_REVERSAL_DOWN'
        low_prob_probability = 0.10
        overall_bias = 'BULLISH'
    else:
        primary_scenario = 'CHOP_CONSOLIDATION'
        primary_probability = 0.50
        alt_scenario = 'CONTINUATION_DOWN'
        alt_probability = 0.30
        low_prob_scenario = 'BULLISH_REVERSAL'
        low_prob_probability = 0.20
        overall_bias = 'NEUTRAL'

    confidence_score = round(abs(score) * 0.8 + 0.1, 2)

    engine_signals = {
        'gex': gex_signal,
        'demark': demark_signal,
        'reload': reload_signal,
        'regime': regime_signal,
        'ew': ew_signal,
        'raw_score': round(score, 2),
    }

    return {
        'primary_scenario': primary_scenario,
        'primary_probability': round(primary_probability, 2),
        'alt_scenario': alt_scenario,
        'alt_probability': round(alt_probability, 2),
        'low_prob_scenario': low_prob_scenario,
        'low_prob_probability': round(low_prob_probability, 2),
        'overall_bias': overall_bias,
        'confidence_score': confidence_score,
        'engines_agreement': engines_agreement,
        'engines_total': engines_total,
        'engine_signals': engine_signals,
    }


# ---------------------------------------------------------------------------
# STEP 5 -- WRITE TO SUPABASE
# ---------------------------------------------------------------------------
def delete_today_rows(ticker):
    """Delete any existing rows for this ticker from today's date."""
    today_start = datetime.now(timezone.utc).strftime('%Y-%m-%dT00:00:00+00:00')
    url = (f"{SUPABASE_URL}/rest/v1/scenario_synthesis"
           f"?ticker=eq.{ticker}&run_ts=gte.{today_start}")
    try:
        requests.delete(url, headers=HEADERS_SB, timeout=10)
    except Exception:
        pass


def write_synthesis(ticker, result):
    """Insert one row into public.scenario_synthesis."""
    now_ts = datetime.now(timezone.utc).isoformat()
    row = {
        'run_ts': now_ts,
        'ticker': ticker,
        'primary_scenario': result['primary_scenario'],
        'primary_probability': result['primary_probability'],
        'alt_scenario': result['alt_scenario'],
        'alt_probability': result['alt_probability'],
        'low_prob_scenario': result['low_prob_scenario'],
        'low_prob_probability': result['low_prob_probability'],
        'overall_bias': result['overall_bias'],
        'confidence_score': result['confidence_score'],
        'engines_agreement': result['engines_agreement'],
        'engines_total': result['engines_total'],
        'engine_signals': json.dumps(result['engine_signals']),
    }
    url = f"{SUPABASE_URL}/rest/v1/scenario_synthesis"
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
def fmt_signal(s):
    """Format a signal value for display."""
    if s is None:
        return '--'
    return f'{s:+d}'


def main():
    print("=" * 56)
    print("SCENARIO SYNTHESIS ENGINE")
    print("=" * 56)
    print(f"Run: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"Tickers: {', '.join(TICKERS)}")
    print("-" * 56)

    for ticker in TICKERS:
        print(f"\n[{ticker}] Fetching engine data...")

        gex, dm, reload, brief = fetch_engine_data(ticker)

        sources = []
        if gex:   sources.append('GEX')
        if dm:    sources.append('DeMark')
        if reload: sources.append('Reload')
        if brief: sources.append('Regime')
        print(f"  [{ticker}] Sources: {', '.join(sources) if sources else 'NONE'}")

        gex_signal = score_gex(gex)
        demark_signal = score_demark(dm)
        reload_signal = score_reload(reload)
        regime_signal = score_regime(brief)
        ew_signal = score_ew()

        result = synthesize(gex_signal, demark_signal, reload_signal,
                            regime_signal, ew_signal)

        delete_today_rows(ticker)
        ok = write_synthesis(ticker, result)
        status = '[OK]' if ok else '[FAIL]'

        print(f"  [{ticker}] {result['overall_bias']} | "
              f"confidence: {int(result['confidence_score'] * 100)}% {status}")
        print(f"    Primary:  {result['primary_scenario']} "
              f"({int(result['primary_probability'] * 100)}%)")
        print(f"    Alt:      {result['alt_scenario']} "
              f"({int(result['alt_probability'] * 100)}%)")
        print(f"    Low prob: {result['low_prob_scenario']} "
              f"({int(result['low_prob_probability'] * 100)}%)")
        print(f"    Engines:  GEX=[{fmt_signal(gex_signal)}] "
              f"DeMark=[{fmt_signal(demark_signal)}] "
              f"Reload=[{fmt_signal(reload_signal)}] "
              f"Regime=[{fmt_signal(regime_signal)}] "
              f"EW=[{fmt_signal(ew_signal)}]")
        print(f"    Score:    {result['engine_signals']['raw_score']}")

    print("\n" + "=" * 56)
    print("SCENARIO SYNTHESIS -- RUN COMPLETE")
    print("=" * 56)


if __name__ == '__main__':
    main()
