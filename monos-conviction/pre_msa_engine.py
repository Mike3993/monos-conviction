"""
PRE-MSA ENGINE -- Accumulation / Distribution Detection
=========================================================
Detects PRE-MSA bull (accumulation) and bear (distribution) conditions
BEFORE the MSA state formally flips. This is the early-warning layer
that feeds into megabrain's MSA gating logic.

BULL detection: dark pool accumulation proxy, GEX moving negative,
    vol compression -> expansion, downside momentum exhaustion, flow support.
BEAR detection: distribution proxy, GEX moving positive,
    vol expansion after calm, upside momentum exhaustion, bearish flow.

Labels: PRE-TRIGGER (>= 1.5), BUILDING (>= 1.0), EARLY (>= 0.5), NONE

Output: pre_msa_scores table + merge into scanner_candidates
Deterministic: same input = same output.

Usage:
    python pre_msa_engine.py          # full run
    python pre_msa_engine.py --dry    # evaluate only, skip writes
"""

import os
import sys
import io
import json
import math
from datetime import date, datetime, timezone
from pathlib import Path

# Force UTF-8 stdout
sys.stdout = io.TextIOWrapper(
    sys.stdout.buffer,
    encoding='utf-8',
    errors='replace'
)

import requests

# ---------------------------------------------------------------------------
# ENV
# ---------------------------------------------------------------------------
def load_env():
    for p in [Path(__file__).parent / '.env', Path(__file__).parent.parent / '.env']:
        if p.exists():
            with open(p) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#') or '=' not in line:
                        continue
                    k, v = line.split('=', 1)
                    os.environ.setdefault(k.strip(), v.strip())
            print(f"[pre_msa] Loaded env from {p}")
            return
    print("[pre_msa] No .env found")

load_env()

SB_URL = os.environ.get('SUPABASE_URL', '')
SB_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')

if not SB_URL or not SB_KEY:
    print("[FATAL] SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set")
    sys.exit(1)

TODAY = date.today()
TODAY_ISO = TODAY.isoformat()
DRY_RUN = "--dry" in sys.argv

HEADERS = {
    'apikey': SB_KEY,
    'Authorization': 'Bearer ' + SB_KEY,
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal',
}


# ---------------------------------------------------------------------------
# WEIGHTS
# ---------------------------------------------------------------------------
# Bull weights
W_BULL_ACCUMULATION     = 0.30
W_BULL_GEX_NEG          = 0.25
W_BULL_VOL_TRANSITION   = 0.20
W_BULL_MOM_EXHAUST_DOWN = 0.15
W_BULL_FLOW_SUPPORT     = 0.10

# Bear weights
W_BEAR_DISTRIBUTION     = 0.30
W_BEAR_GEX_POS          = 0.25
W_BEAR_VOL_EXPANSION    = 0.20
W_BEAR_MOM_EXHAUST_UP   = 0.15
W_BEAR_FLOW_BEARISH     = 0.10

# Label thresholds
LABEL_PRE_TRIGGER = 1.5
LABEL_BUILDING    = 1.0
LABEL_EARLY       = 0.5


# ---------------------------------------------------------------------------
# SUPABASE HELPERS
# ---------------------------------------------------------------------------
def sb_get(table, params):
    url = SB_URL + '/rest/v1/' + table
    try:
        r = requests.get(url, headers={
            'apikey': SB_KEY, 'Authorization': 'Bearer ' + SB_KEY,
        }, params=params, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"  [!] sb_get {table}: {e}")
    return []


def sb_delete_today(table, ts_col='as_of'):
    url = SB_URL + '/rest/v1/' + table
    try:
        r = requests.delete(url, headers=HEADERS, params={
            ts_col: 'gte.' + TODAY_ISO,
        }, timeout=15)
        return r.status_code in (200, 204)
    except Exception:
        return False


def sb_insert(table, rows):
    if not rows:
        return True
    url = SB_URL + '/rest/v1/' + table
    try:
        r = requests.post(url, headers=HEADERS, json=rows, timeout=30)
        if r.status_code in (200, 201, 204):
            return True
        print(f"  [!] {table} insert: HTTP {r.status_code} -- {r.text[:300]}")
    except Exception as e:
        print(f"  [!] {table} insert error: {e}")
    return False


def sb_patch(table, params, body):
    url = SB_URL + '/rest/v1/' + table
    try:
        r = requests.patch(url, headers=HEADERS, params=params, json=body, timeout=15)
        return r.status_code in (200, 204)
    except Exception as e:
        print(f"  [!] patch {table}: {e}")
        return False


# ---------------------------------------------------------------------------
# LOAD INPUTS (bulk fetch)
# ---------------------------------------------------------------------------
def load_all_inputs():
    """Fetch upstream data needed for pre-MSA scoring."""

    # Ticker universe
    tickers = sb_get('ticker_universe', {
        'select': 'ticker',
        'order': 'ticker.asc',
    })
    ticker_list = [r['ticker'] for r in tickers if r.get('ticker')]

    # GEX snapshots (latest + history for delta)
    gex_rows = sb_get('gex_snapshots', {
        'select': 'ticker,net_gex,gex_regime,run_ts',
        'order': 'run_ts.desc',
        'limit': '500',
    })
    # Group by ticker, keep ordering
    gex_history = {}
    for r in gex_rows:
        t = r['ticker']
        if t not in gex_history:
            gex_history[t] = []
        gex_history[t].append(r)

    # Flow snapshots (latest + history)
    flow_rows = sb_get('flow_snapshots', {
        'select': 'ticker,net_notional,call_premium,put_premium,run_ts',
        'order': 'run_ts.desc',
        'limit': '500',
    })
    flow_history = {}
    for r in flow_rows:
        t = r['ticker']
        if t not in flow_history:
            flow_history[t] = []
        flow_history[t].append(r)

    # Symmetry snapshots (vol data)
    sym_rows = sb_get('symmetry_snapshots', {
        'select': 'ticker,convexity_state,symmetry_score,atm_iv,hv20,iv_rv_gap_20d,run_ts',
        'order': 'run_ts.desc',
        'limit': '500',
    })
    sym_history = {}
    for r in sym_rows:
        t = r['ticker']
        if t not in sym_history:
            sym_history[t] = []
        sym_history[t].append(r)

    # Scenario synthesis (bias + vol regime)
    scen_rows = sb_get('scenario_synthesis', {
        'select': 'ticker,overall_bias,vol_regime,run_ts',
        'order': 'run_ts.desc',
        'limit': '500',
    })
    scen_history = {}
    for r in scen_rows:
        t = r['ticker']
        if t not in scen_history:
            scen_history[t] = []
        scen_history[t].append(r)

    # DeMark signals (latest per ticker)
    dm_rows = sb_get('demark_signals', {
        'select': 'ticker,setup_direction,setup_count,signal_state,run_ts',
        'order': 'run_ts.desc',
        'limit': '100',
    })
    dm_map = {}
    for r in dm_rows:
        if r['ticker'] not in dm_map:
            dm_map[r['ticker']] = r

    # Conflict states (latest per ticker)
    con_rows = sb_get('conflict_states', {
        'select': 'ticker,conflict_state,active_count',
        'order': 'run_ts.desc',
        'limit': '100',
    })
    con_map = {}
    for r in con_rows:
        if r['ticker'] not in con_map:
            con_map[r['ticker']] = r

    return ticker_list, gex_history, flow_history, sym_history, scen_history, dm_map, con_map


# ---------------------------------------------------------------------------
# COMPONENT SCORES
# ---------------------------------------------------------------------------

def _safe_float(val, default=0.0):
    """Safely convert to float."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def compute_accumulation_score(flow_hist, gex_hist):
    """
    Dark pool accumulation proxy:
    - Positive net flow trend (call-heavy, positive net notional)
    - Sustained buying over multiple observations
    Returns 0..3 (higher = stronger accumulation signal)
    """
    if not flow_hist or len(flow_hist) < 2:
        return 0.0

    # Recent vs older net notional
    recent = flow_hist[:3]  # last 3 observations
    older = flow_hist[3:8]  # 3-8 observations back

    recent_avg = sum(_safe_float(r.get('net_notional')) for r in recent) / len(recent)
    older_avg = sum(_safe_float(r.get('net_notional')) for r in older) / max(len(older), 1)

    # Call skew: call_premium > put_premium in recent
    call_skew_count = 0
    for r in recent:
        cp = _safe_float(r.get('call_premium'))
        pp = _safe_float(r.get('put_premium'))
        if cp > pp:
            call_skew_count += 1
    call_skew_ratio = call_skew_count / len(recent)

    score = 0.0

    # Positive and increasing net flow
    if recent_avg > 0:
        score += 1.0
    if older_avg != 0 and recent_avg > older_avg * 1.1:
        score += 0.5

    # Call skew
    score += call_skew_ratio * 1.0

    # Sustained positive flow (3+ of last 5)
    positive_count = sum(1 for r in flow_hist[:5] if _safe_float(r.get('net_notional')) > 0)
    if positive_count >= 3:
        score += 0.5

    return min(3.0, round(score, 4))


def compute_distribution_score(flow_hist, gex_hist):
    """
    Dark pool distribution proxy:
    - Negative net flow trend (put-heavy, negative net notional)
    - Sustained selling
    Returns 0..3
    """
    if not flow_hist or len(flow_hist) < 2:
        return 0.0

    recent = flow_hist[:3]
    older = flow_hist[3:8]

    recent_avg = sum(_safe_float(r.get('net_notional')) for r in recent) / len(recent)
    older_avg = sum(_safe_float(r.get('net_notional')) for r in older) / max(len(older), 1)

    # Put skew
    put_skew_count = 0
    for r in recent:
        cp = _safe_float(r.get('call_premium'))
        pp = _safe_float(r.get('put_premium'))
        if pp > cp:
            put_skew_count += 1
    put_skew_ratio = put_skew_count / len(recent)

    score = 0.0

    if recent_avg < 0:
        score += 1.0
    if older_avg != 0 and recent_avg < older_avg * 0.9:
        score += 0.5

    score += put_skew_ratio * 1.0

    negative_count = sum(1 for r in flow_hist[:5] if _safe_float(r.get('net_notional')) < 0)
    if negative_count >= 3:
        score += 0.5

    return min(3.0, round(score, 4))


def compute_gex_shift_negative(gex_hist):
    """
    GEX moving toward negative gamma (bullish for convexity).
    Returns 0..3
    """
    if not gex_hist or len(gex_hist) < 2:
        return 0.0

    score = 0.0
    regime_map = {'NEGATIVE': -1, 'NEUTRAL': 0, 'POSITIVE': 1}

    # Current regime
    current_regime = (gex_hist[0].get('gex_regime') or '').upper()
    if current_regime == 'NEGATIVE':
        score += 1.5

    # Delta: moving toward negative
    if len(gex_hist) >= 2:
        curr_gex = _safe_float(gex_hist[0].get('net_gex'))
        prev_gex = _safe_float(gex_hist[1].get('net_gex'))
        if curr_gex < prev_gex:
            score += 0.5
        # Accelerating negative
        if len(gex_hist) >= 3:
            prev2_gex = _safe_float(gex_hist[2].get('net_gex'))
            d1 = curr_gex - prev_gex
            d2 = prev_gex - prev2_gex
            if d1 < d2:  # accelerating downward
                score += 0.5

    # Regime flip detection
    if len(gex_hist) >= 2:
        prev_regime = (gex_hist[1].get('gex_regime') or '').upper()
        curr_val = regime_map.get(current_regime, 0)
        prev_val = regime_map.get(prev_regime, 0)
        if curr_val < prev_val:
            score += 0.5

    return min(3.0, round(score, 4))


def compute_gex_shift_positive(gex_hist):
    """
    GEX moving toward positive gamma (bearish signal -- dealers hedging).
    Returns 0..3
    """
    if not gex_hist or len(gex_hist) < 2:
        return 0.0

    score = 0.0
    regime_map = {'NEGATIVE': -1, 'NEUTRAL': 0, 'POSITIVE': 1}

    current_regime = (gex_hist[0].get('gex_regime') or '').upper()
    if current_regime == 'POSITIVE':
        score += 1.5

    if len(gex_hist) >= 2:
        curr_gex = _safe_float(gex_hist[0].get('net_gex'))
        prev_gex = _safe_float(gex_hist[1].get('net_gex'))
        if curr_gex > prev_gex:
            score += 0.5
        if len(gex_hist) >= 3:
            prev2_gex = _safe_float(gex_hist[2].get('net_gex'))
            d1 = curr_gex - prev_gex
            d2 = prev_gex - prev2_gex
            if d1 > d2:
                score += 0.5

    if len(gex_hist) >= 2:
        prev_regime = (gex_hist[1].get('gex_regime') or '').upper()
        curr_val = regime_map.get(current_regime, 0)
        prev_val = regime_map.get(prev_regime, 0)
        if curr_val > prev_val:
            score += 0.5

    return min(3.0, round(score, 4))


VOL_REGIME_NUM = {
    'COMPRESSED': -1, 'LOW': -1, 'CALM': -1,
    'NORMAL': 0, 'NEUTRAL': 0,
    'ELEVATED': 1, 'HIGH': 1,
    'EXPANDED': 2, 'EXTREME': 2, 'CRISIS': 2,
}


def compute_vol_transition_bull(sym_hist, scen_hist):
    """
    Vol compression -> expansion (bullish setup -- vol is cheap, about to move).
    Returns 0..3
    """
    score = 0.0

    # Check IV vs HV: vol compression = IV < HV
    if sym_hist and len(sym_hist) >= 1:
        latest = sym_hist[0]
        atm_iv = _safe_float(latest.get('atm_iv'))
        hv20 = _safe_float(latest.get('hv20'))
        if atm_iv > 0 and hv20 > 0:
            ratio = atm_iv / hv20
            if ratio < 0.85:    # deep compression
                score += 1.5
            elif ratio < 1.0:   # mild compression
                score += 0.75

        # Convexity state
        cs = (latest.get('convexity_state') or '').upper()
        if 'CHEAP' in cs:
            score += 1.0
        elif 'FAIR' in cs:
            score += 0.3

    # Vol regime transition: was compressed, now expanding
    if scen_hist and len(scen_hist) >= 2:
        def _vol_num(r):
            vr = (r.get('vol_regime') or '').upper()
            for k, v in VOL_REGIME_NUM.items():
                if k in vr:
                    return v
            return 0

        curr = _vol_num(scen_hist[0])
        prev = _vol_num(scen_hist[1])
        if curr > prev and prev <= 0:
            score += 0.5

    return min(3.0, round(score, 4))


def compute_vol_expansion_bear(sym_hist, scen_hist):
    """
    Vol expansion after calm (bearish -- vol spiking, fear entering).
    Returns 0..3
    """
    score = 0.0

    if sym_hist and len(sym_hist) >= 1:
        latest = sym_hist[0]
        atm_iv = _safe_float(latest.get('atm_iv'))
        hv20 = _safe_float(latest.get('hv20'))
        if atm_iv > 0 and hv20 > 0:
            ratio = atm_iv / hv20
            if ratio > 1.3:
                score += 1.5
            elif ratio > 1.1:
                score += 0.75

        cs = (latest.get('convexity_state') or '').upper()
        if 'VERY_RICH' in cs:
            score += 0.5
        elif 'RICH' in cs:
            score += 0.3

    if scen_hist and len(scen_hist) >= 2:
        def _vol_num(r):
            vr = (r.get('vol_regime') or '').upper()
            for k, v in VOL_REGIME_NUM.items():
                if k in vr:
                    return v
            return 0

        curr = _vol_num(scen_hist[0])
        prev = _vol_num(scen_hist[1])
        if curr > prev and curr >= 1:
            score += 1.0

    return min(3.0, round(score, 4))


def compute_momentum_exhaustion_down(dm):
    """
    Downside momentum exhaustion (bullish reversal signal).
    DeMark BUY setup nearing completion = sellers exhausted.
    Returns 0..3
    """
    if not dm:
        return 0.0

    direction = (dm.get('setup_direction') or '').upper()
    count = dm.get('setup_count') or 0
    state = (dm.get('signal_state') or '').upper()

    score = 0.0

    if direction == 'BUY' or 'BUY' in state:
        if count >= 9 or 'PERFECT' in state or '9' in state:
            score += 2.5
        elif count >= 7:
            score += 1.5
        elif count >= 5:
            score += 0.5

    return min(3.0, round(score, 4))


def compute_momentum_exhaustion_up(dm):
    """
    Upside momentum exhaustion (bearish reversal signal).
    DeMark SELL setup nearing completion = buyers exhausted.
    Returns 0..3
    """
    if not dm:
        return 0.0

    direction = (dm.get('setup_direction') or '').upper()
    count = dm.get('setup_count') or 0
    state = (dm.get('signal_state') or '').upper()

    score = 0.0

    if direction == 'SELL' or 'SELL' in state:
        if count >= 9 or 'PERFECT' in state or '9' in state:
            score += 2.5
        elif count >= 7:
            score += 1.5
        elif count >= 5:
            score += 0.5

    return min(3.0, round(score, 4))


def compute_flow_support(flow_hist):
    """
    Bullish flow confirmation: sustained positive net notional, call-heavy.
    Returns 0..3
    """
    if not flow_hist:
        return 0.0

    recent = flow_hist[:5]
    pos = sum(1 for r in recent if _safe_float(r.get('net_notional')) > 0)

    score = 0.0
    if pos >= 4:
        score += 1.5
    elif pos >= 3:
        score += 0.75

    # Call premium dominance
    call_dom = sum(1 for r in recent
                   if _safe_float(r.get('call_premium')) > _safe_float(r.get('put_premium')))
    if call_dom >= 3:
        score += 0.75

    # Strong absolute flow
    avg_flow = sum(_safe_float(r.get('net_notional')) for r in recent) / max(len(recent), 1)
    if avg_flow > 0:
        score += 0.5

    return min(3.0, round(score, 4))


def compute_bearish_flow(flow_hist):
    """
    Bearish flow confirmation: sustained negative net notional, put-heavy.
    Returns 0..3
    """
    if not flow_hist:
        return 0.0

    recent = flow_hist[:5]
    neg = sum(1 for r in recent if _safe_float(r.get('net_notional')) < 0)

    score = 0.0
    if neg >= 4:
        score += 1.5
    elif neg >= 3:
        score += 0.75

    put_dom = sum(1 for r in recent
                  if _safe_float(r.get('put_premium')) > _safe_float(r.get('call_premium')))
    if put_dom >= 3:
        score += 0.75

    avg_flow = sum(_safe_float(r.get('net_notional')) for r in recent) / max(len(recent), 1)
    if avg_flow < 0:
        score += 0.5

    return min(3.0, round(score, 4))


# ---------------------------------------------------------------------------
# LABEL
# ---------------------------------------------------------------------------
def label_pre_msa(score):
    """Label a pre-MSA score."""
    if score >= LABEL_PRE_TRIGGER:
        return 'PRE-TRIGGER'
    elif score >= LABEL_BUILDING:
        return 'BUILDING'
    elif score >= LABEL_EARLY:
        return 'EARLY'
    else:
        return 'NONE'


# ---------------------------------------------------------------------------
# PROCESS TICKER
# ---------------------------------------------------------------------------
def process_ticker(ticker, gex_hist, flow_hist, sym_hist, scen_hist, dm, con):
    """Full pre-MSA pipeline for one ticker."""

    gex_h = gex_hist.get(ticker, [])
    flow_h = flow_hist.get(ticker, [])
    sym_h = sym_hist.get(ticker, [])
    scen_h = scen_hist.get(ticker, [])
    dm_data = dm.get(ticker)
    con_data = con.get(ticker)

    # ---- BULL SCORE ----
    accumulation = compute_accumulation_score(flow_h, gex_h)
    gex_neg = compute_gex_shift_negative(gex_h)
    vol_trans = compute_vol_transition_bull(sym_h, scen_h)
    mom_down = compute_momentum_exhaustion_down(dm_data)
    flow_sup = compute_flow_support(flow_h)

    bull_score = (
        W_BULL_ACCUMULATION     * accumulation +
        W_BULL_GEX_NEG          * gex_neg +
        W_BULL_VOL_TRANSITION   * vol_trans +
        W_BULL_MOM_EXHAUST_DOWN * mom_down +
        W_BULL_FLOW_SUPPORT     * flow_sup
    )
    bull_score = round(bull_score, 4)
    bull_label = label_pre_msa(bull_score)

    # ---- BEAR SCORE ----
    distribution = compute_distribution_score(flow_h, gex_h)
    gex_pos = compute_gex_shift_positive(gex_h)
    vol_exp = compute_vol_expansion_bear(sym_h, scen_h)
    mom_up = compute_momentum_exhaustion_up(dm_data)
    bear_flow = compute_bearish_flow(flow_h)

    bear_score = (
        W_BEAR_DISTRIBUTION     * distribution +
        W_BEAR_GEX_POS          * gex_pos +
        W_BEAR_VOL_EXPANSION    * vol_exp +
        W_BEAR_MOM_EXHAUST_UP   * mom_up +
        W_BEAR_FLOW_BEARISH     * bear_flow
    )
    bear_score = round(bear_score, 4)
    bear_label = label_pre_msa(bear_score)

    components = {
        'bull': {
            'accumulation': accumulation,
            'gex_shift_negative': gex_neg,
            'vol_transition': vol_trans,
            'momentum_exhaustion_down': mom_down,
            'flow_support': flow_sup,
        },
        'bear': {
            'distribution': distribution,
            'gex_shift_positive': gex_pos,
            'vol_expansion': vol_exp,
            'momentum_exhaustion_up': mom_up,
            'bearish_flow': bear_flow,
        },
    }

    row = {
        'ticker': ticker,
        'as_of': datetime.now(timezone.utc).isoformat(),
        'pre_msa_bull_score': bull_score,
        'pre_msa_bear_score': bear_score,
        'pre_msa_label_bull': bull_label,
        'pre_msa_label_bear': bear_label,
        'components': json.dumps(components),
    }

    # Scanner patch
    scanner_patch = {
        'pre_msa_bull_score': bull_score,
        'pre_msa_bear_score': bear_score,
    }

    return row, scanner_patch, components


# ---------------------------------------------------------------------------
# WRITE
# ---------------------------------------------------------------------------
def write_pre_msa_scores(rows):
    sb_delete_today('pre_msa_scores')
    return sb_insert('pre_msa_scores', rows)


def merge_into_scanner(patches):
    updated = 0
    for ticker, patch in patches.items():
        ok = sb_patch('scanner_candidates', {
            'ticker': 'eq.' + ticker,
            'run_ts': 'gte.' + TODAY_ISO,
        }, patch)
        if ok:
            updated += 1
    return updated


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=" * 64)
    print("PRE-MSA ENGINE -- Accumulation / Distribution Detection")
    print(f"Date: {TODAY_ISO}")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"Supabase: {SB_URL[:45]}...")
    print("=" * 64)

    print("\n[pre_msa] Loading upstream data...")
    ticker_list, gex_hist, flow_hist, sym_hist, scen_hist, dm_map, con_map = load_all_inputs()
    print(f"  Tickers:  {len(ticker_list)}")
    print(f"  GEX:      {len(gex_hist)} tickers")
    print(f"  Flow:     {len(flow_hist)} tickers")
    print(f"  Symmetry: {len(sym_hist)} tickers")
    print(f"  Scenario: {len(scen_hist)} tickers")
    print(f"  DeMark:   {len(dm_map)} tickers")

    if not ticker_list:
        print("\n[pre_msa] No tickers. Exiting.")
        return

    print(f"\n[pre_msa] Processing {len(ticker_list)} tickers...\n")
    rows = []
    scanner_patches = {}

    for ticker in ticker_list:
        row, scanner_patch, components = process_ticker(
            ticker, gex_hist, flow_hist, sym_hist, scen_hist, dm_map, con_map
        )
        rows.append(row)
        scanner_patches[ticker] = scanner_patch

        # Debug print
        bs = row['pre_msa_bull_score']
        brs = row['pre_msa_bear_score']
        bl = row['pre_msa_label_bull']
        brl = row['pre_msa_label_bear']

        # Pick dominant side
        if bs > brs:
            icon = '+' if bl in ('PRE-TRIGGER', 'BUILDING') else '.'
            dominant = f"BULL={bs:.2f}({bl})"
        elif brs > bs:
            icon = '-' if brl in ('PRE-TRIGGER', 'BUILDING') else '.'
            dominant = f"BEAR={brs:.2f}({brl})"
        else:
            icon = '='
            dominant = f"EVEN bull={bs:.2f} bear={brs:.2f}"

        print(f"  [{icon}] {ticker:<6s} | bull={bs:.3f} ({bl:<12s}) | "
              f"bear={brs:.3f} ({brl:<12s}) | {dominant}")

    # Write
    if not DRY_RUN:
        print(f"\n[pre_msa] Writing {len(rows)} rows to pre_msa_scores...")
        ok = write_pre_msa_scores(rows)
        print(f"  pre_msa_scores: {'OK' if ok else 'FAILED'}")

        print(f"\n[pre_msa] Merging into scanner_candidates...")
        n = merge_into_scanner(scanner_patches)
        print(f"  scanner_candidates updated: {n}/{len(scanner_patches)}")

    # Summary
    print()
    print("=" * 64)
    print("PRE-MSA ENGINE -- RUN COMPLETE")
    print("=" * 64)
    print(f"Tickers processed: {len(rows)}")

    # Bull summary
    bull_trigger = [r for r in rows if r['pre_msa_label_bull'] == 'PRE-TRIGGER']
    bull_building = [r for r in rows if r['pre_msa_label_bull'] == 'BUILDING']
    bull_early = [r for r in rows if r['pre_msa_label_bull'] == 'EARLY']
    print(f"\n  BULL -- PRE-TRIGGER: {len(bull_trigger)}  BUILDING: {len(bull_building)}  EARLY: {len(bull_early)}")

    if bull_trigger:
        print("  --- Bull pre-triggers ---")
        for r in sorted(bull_trigger, key=lambda x: x['pre_msa_bull_score'], reverse=True):
            print(f"    {r['ticker']:<6s} | score={r['pre_msa_bull_score']:.3f}")

    # Bear summary
    bear_trigger = [r for r in rows if r['pre_msa_label_bear'] == 'PRE-TRIGGER']
    bear_building = [r for r in rows if r['pre_msa_label_bear'] == 'BUILDING']
    bear_early = [r for r in rows if r['pre_msa_label_bear'] == 'EARLY']
    print(f"\n  BEAR -- PRE-TRIGGER: {len(bear_trigger)}  BUILDING: {len(bear_building)}  EARLY: {len(bear_early)}")

    if bear_trigger:
        print("  --- Bear pre-triggers ---")
        for r in sorted(bear_trigger, key=lambda x: x['pre_msa_bear_score'], reverse=True):
            print(f"    {r['ticker']:<6s} | score={r['pre_msa_bear_score']:.3f}")

    if DRY_RUN:
        print(f"\n  (DRY RUN -- no writes performed)")

    print("=" * 64)


if __name__ == '__main__':
    main()
