"""
ROTATION ENGINE -- Structural Rotation State Detection
========================================================
Detects the rotation state of each ticker's structural regime:
  BULL -> BUILDING_TOP -> ROTATING -> BEAR_BUILDING -> BEAR

Rotation score combines:
  - Weakening MSA (scenario bias deteriorating)
  - Vol expansion
  - Distribution signals (from pre_msa bear score)
  - GEX flipping positive (dealers gaining control)

When rotation_score is high: trigger exit signals, downgrade convexity,
prepare opposite structures.

Writes to rotation_states table + merges into scanner_candidates.
Deterministic: same input = same output.

Usage:
    python rotation_engine.py          # full run
    python rotation_engine.py --dry    # evaluate only, skip writes
"""

import os
import sys
import io
import json
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
            print(f"[rotation] Loaded env from {p}")
            return
    print("[rotation] No .env found")

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
# ROTATION THRESHOLDS
# ---------------------------------------------------------------------------
# rotation_score ranges determine state:
#   >= 2.5  -> BEAR
#   >= 1.8  -> BEAR_BUILDING
#   >= 1.2  -> ROTATING
#   >= 0.6  -> BUILDING_TOP
#   < 0.6   -> BULL

ROT_BEAR           = 2.5
ROT_BEAR_BUILDING  = 1.8
ROT_ROTATING       = 1.2
ROT_BUILDING_TOP   = 0.6

# Component weights
W_MSA_WEAK      = 0.30
W_VOL_EXPAND    = 0.25
W_DISTRIBUTION  = 0.25
W_GEX_FLIP_POS  = 0.20


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
# LOAD INPUTS
# ---------------------------------------------------------------------------
def _safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def load_all_inputs():
    """Fetch all upstream data for rotation detection."""

    # Ticker universe
    tickers = sb_get('ticker_universe', {
        'select': 'ticker',
        'order': 'ticker.asc',
    })
    ticker_list = [r['ticker'] for r in tickers if r.get('ticker')]

    # Scenario synthesis history (for MSA weakening detection)
    scen_rows = sb_get('scenario_synthesis', {
        'select': 'ticker,overall_bias,vol_regime,confidence_score,run_ts',
        'order': 'run_ts.desc',
        'limit': '500',
    })
    scen_history = {}
    for r in scen_rows:
        t = r['ticker']
        if t not in scen_history:
            scen_history[t] = []
        scen_history[t].append(r)

    # GEX history (for GEX flip detection)
    gex_rows = sb_get('gex_snapshots', {
        'select': 'ticker,net_gex,gex_regime,run_ts',
        'order': 'run_ts.desc',
        'limit': '500',
    })
    gex_history = {}
    for r in gex_rows:
        t = r['ticker']
        if t not in gex_history:
            gex_history[t] = []
        gex_history[t].append(r)

    # Symmetry snapshots (vol expansion)
    sym_rows = sb_get('symmetry_snapshots', {
        'select': 'ticker,convexity_state,atm_iv,hv20,iv_rv_gap_20d,run_ts',
        'order': 'run_ts.desc',
        'limit': '500',
    })
    sym_history = {}
    for r in sym_rows:
        t = r['ticker']
        if t not in sym_history:
            sym_history[t] = []
        sym_history[t].append(r)

    # Pre-MSA scores (for distribution signal)
    pre_msa_rows = sb_get('pre_msa_scores', {
        'select': 'ticker,pre_msa_bull_score,pre_msa_bear_score,pre_msa_label_bull,pre_msa_label_bear',
        'as_of': 'gte.' + TODAY_ISO,
    })
    pre_msa_map = {}
    for r in pre_msa_rows:
        pre_msa_map[r['ticker']] = r

    # Scanner candidates (for opportunity_score in patch)
    scan_rows = sb_get('scanner_candidates', {
        'select': 'ticker,opportunity_score',
        'run_ts': 'gte.' + TODAY_ISO,
    })
    scan_map = {}
    for r in scan_rows:
        scan_map[r['ticker']] = r

    return ticker_list, scen_history, gex_history, sym_history, pre_msa_map, scan_map


# ---------------------------------------------------------------------------
# COMPONENT SCORES
# ---------------------------------------------------------------------------

BIAS_STRENGTH = {
    'STRONG_BULL': 2, 'BULLISH': 1, 'MILD_BULL': 0.5, 'SLIGHT_BULL': 0.5,
    'NEUTRAL': 0, 'MIXED': 0, 'CHOP': 0,
    'SLIGHT_BEAR': -0.5, 'MILD_BEAR': -0.5, 'BEARISH': -1, 'STRONG_BEAR': -2,
}


def _bias_to_num(bias_str):
    """Convert overall_bias string to numeric value."""
    if not bias_str:
        return 0
    b = bias_str.upper().strip()
    for key, val in BIAS_STRENGTH.items():
        if key in b:
            return val
    if 'BULL' in b:
        return 1
    if 'BEAR' in b:
        return -1
    return 0


def compute_msa_weakening(scen_hist):
    """
    Detect MSA weakening: scenario bias deteriorating over time.
    A drop from bullish to neutral/bearish = high rotation signal.
    Returns 0..3
    """
    if not scen_hist or len(scen_hist) < 2:
        return 0.0

    curr_bias = _bias_to_num(scen_hist[0].get('overall_bias'))
    prev_bias = _bias_to_num(scen_hist[1].get('overall_bias'))

    score = 0.0

    # Bias deterioration
    delta = curr_bias - prev_bias
    if delta < -1:
        score += 2.0
    elif delta < -0.5:
        score += 1.0
    elif delta < 0:
        score += 0.5

    # Check longer trend (3+ observations)
    if len(scen_hist) >= 3:
        older_bias = _bias_to_num(scen_hist[2].get('overall_bias'))
        trend = curr_bias - older_bias
        if trend < -1:
            score += 0.5

    # Confidence dropping
    if len(scen_hist) >= 2:
        curr_conf = _safe_float(scen_hist[0].get('confidence_score'))
        prev_conf = _safe_float(scen_hist[1].get('confidence_score'))
        if curr_conf < prev_conf and curr_conf < 50:
            score += 0.5

    return min(3.0, round(score, 4))


VOL_REGIME_NUM = {
    'COMPRESSED': -1, 'LOW': -1, 'CALM': -1,
    'NORMAL': 0, 'NEUTRAL': 0,
    'ELEVATED': 1, 'HIGH': 1,
    'EXPANDED': 2, 'EXTREME': 2, 'CRISIS': 2,
}


def compute_vol_expansion(sym_hist, scen_hist):
    """
    Vol expansion = rising IV, regime moving to ELEVATED/EXPANDED.
    Returns 0..3
    """
    score = 0.0

    if sym_hist and len(sym_hist) >= 2:
        curr_iv = _safe_float(sym_hist[0].get('atm_iv'))
        prev_iv = _safe_float(sym_hist[1].get('atm_iv'))
        if curr_iv > 0 and prev_iv > 0:
            iv_change = (curr_iv - prev_iv) / prev_iv
            if iv_change > 0.15:
                score += 1.5
            elif iv_change > 0.05:
                score += 0.75

        cs = (sym_hist[0].get('convexity_state') or '').upper()
        if 'VERY_RICH' in cs:
            score += 0.75
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
        if curr > prev:
            score += 0.75

    return min(3.0, round(score, 4))


def compute_distribution_signal(pre_msa):
    """
    Distribution signal from pre_msa bear score.
    Returns 0..3
    """
    if not pre_msa:
        return 0.0

    bear_score = _safe_float(pre_msa.get('pre_msa_bear_score'))
    bear_label = (pre_msa.get('pre_msa_label_bear') or '').upper()

    if bear_label == 'PRE-TRIGGER':
        return 3.0
    elif bear_label == 'BUILDING':
        return 2.0
    elif bear_label == 'EARLY':
        return 1.0
    elif bear_score > 0.3:
        return 0.5
    return 0.0


def compute_gex_flip_positive(gex_hist):
    """
    GEX flipping positive = dealers gaining gamma control = less explosive moves.
    Returns 0..3
    """
    if not gex_hist or len(gex_hist) < 2:
        return 0.0

    score = 0.0
    regime_map = {'NEGATIVE': -1, 'NEUTRAL': 0, 'POSITIVE': 1}

    curr_regime = (gex_hist[0].get('gex_regime') or '').upper()
    prev_regime = (gex_hist[1].get('gex_regime') or '').upper()

    curr_val = regime_map.get(curr_regime, 0)
    prev_val = regime_map.get(prev_regime, 0)

    # Regime flip from NEGATIVE -> NEUTRAL or POSITIVE
    if curr_val > prev_val:
        score += 1.0
        if curr_val == 1:  # now POSITIVE
            score += 0.5

    # Already POSITIVE
    if curr_regime == 'POSITIVE':
        score += 1.0

    # Net GEX increasing
    curr_gex = _safe_float(gex_hist[0].get('net_gex'))
    prev_gex = _safe_float(gex_hist[1].get('net_gex'))
    if curr_gex > prev_gex:
        score += 0.5

    return min(3.0, round(score, 4))


# ---------------------------------------------------------------------------
# ROTATION STATE
# ---------------------------------------------------------------------------
def compute_rotation_score(msa_weak, vol_exp, distrib, gex_flip):
    """Weighted rotation score."""
    score = (
        W_MSA_WEAK      * msa_weak +
        W_VOL_EXPAND    * vol_exp +
        W_DISTRIBUTION  * distrib +
        W_GEX_FLIP_POS  * gex_flip
    )
    return round(score, 4)


def label_rotation(score):
    """Derive rotation_state from score."""
    if score >= ROT_BEAR:
        return 'BEAR'
    elif score >= ROT_BEAR_BUILDING:
        return 'BEAR_BUILDING'
    elif score >= ROT_ROTATING:
        return 'ROTATING'
    elif score >= ROT_BUILDING_TOP:
        return 'BUILDING_TOP'
    else:
        return 'BULL'


# ---------------------------------------------------------------------------
# PROCESS TICKER
# ---------------------------------------------------------------------------
def process_ticker(ticker, scen_hist, gex_hist, sym_hist, pre_msa_map, scan_map):
    """Full rotation pipeline for one ticker."""

    scen_h = scen_hist.get(ticker, [])
    gex_h = gex_hist.get(ticker, [])
    sym_h = sym_hist.get(ticker, [])
    pre_msa = pre_msa_map.get(ticker)

    # Components
    msa_weak = compute_msa_weakening(scen_h)
    vol_exp = compute_vol_expansion(sym_h, scen_h)
    distrib = compute_distribution_signal(pre_msa)
    gex_flip = compute_gex_flip_positive(gex_h)

    rotation_score = compute_rotation_score(msa_weak, vol_exp, distrib, gex_flip)
    rotation_state = label_rotation(rotation_score)

    components = {
        'msa_weakening': msa_weak,
        'vol_expansion': vol_exp,
        'distribution': distrib,
        'gex_flip_positive': gex_flip,
    }

    row = {
        'ticker': ticker,
        'as_of': datetime.now(timezone.utc).isoformat(),
        'rotation_score': rotation_score,
        'rotation_state': rotation_state,
        'components': json.dumps(components),
    }

    scanner_patch = {
        'rotation_state': rotation_state,
    }

    return row, scanner_patch, components


# ---------------------------------------------------------------------------
# WRITE
# ---------------------------------------------------------------------------
def write_rotation_states(rows):
    sb_delete_today('rotation_states')
    return sb_insert('rotation_states', rows)


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
    print("ROTATION ENGINE -- Structural Rotation State Detection")
    print(f"Date: {TODAY_ISO}")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"Supabase: {SB_URL[:45]}...")
    print("=" * 64)

    print("\n[rotation] Loading upstream data...")
    ticker_list, scen_hist, gex_hist, sym_hist, pre_msa_map, scan_map = load_all_inputs()
    print(f"  Tickers:   {len(ticker_list)}")
    print(f"  Scenarios: {len(scen_hist)} tickers")
    print(f"  GEX:       {len(gex_hist)} tickers")
    print(f"  Symmetry:  {len(sym_hist)} tickers")
    print(f"  Pre-MSA:   {len(pre_msa_map)} tickers")

    if not ticker_list:
        print("\n[rotation] No tickers. Exiting.")
        return

    print(f"\n[rotation] Processing {len(ticker_list)} tickers...\n")
    rows = []
    scanner_patches = {}

    for ticker in ticker_list:
        row, scanner_patch, components = process_ticker(
            ticker, scen_hist, gex_hist, sym_hist, pre_msa_map, scan_map
        )
        rows.append(row)
        scanner_patches[ticker] = scanner_patch

        rs = row['rotation_score']
        state = row['rotation_state']
        icon = {
            'BEAR': '!', 'BEAR_BUILDING': '-', 'ROTATING': '~',
            'BUILDING_TOP': '*', 'BULL': '.',
        }.get(state, '?')

        print(f"  [{icon}] {ticker:<6s} | score={rs:.3f} | {state:<15s} | "
              f"msa_w={components['msa_weakening']:.1f} "
              f"vol_e={components['vol_expansion']:.1f} "
              f"dist={components['distribution']:.1f} "
              f"gex_f={components['gex_flip_positive']:.1f}")

    # Write
    if not DRY_RUN:
        print(f"\n[rotation] Writing {len(rows)} rows to rotation_states...")
        ok = write_rotation_states(rows)
        print(f"  rotation_states: {'OK' if ok else 'FAILED'}")

        print(f"\n[rotation] Merging into scanner_candidates...")
        n = merge_into_scanner(scanner_patches)
        print(f"  scanner_candidates updated: {n}/{len(scanner_patches)}")

    # Summary
    print()
    print("=" * 64)
    print("ROTATION ENGINE -- RUN COMPLETE")
    print("=" * 64)
    print(f"Tickers processed: {len(rows)}")

    state_counts = {}
    for r in rows:
        s = r['rotation_state']
        state_counts[s] = state_counts.get(s, 0) + 1
    for s in ['BULL', 'BUILDING_TOP', 'ROTATING', 'BEAR_BUILDING', 'BEAR']:
        if s in state_counts:
            print(f"  {s}: {state_counts[s]}")

    # Flag any rotating or worse
    rotating = [r for r in rows if r['rotation_state'] in ('ROTATING', 'BEAR_BUILDING', 'BEAR')]
    if rotating:
        print()
        print("--- ROTATION WARNINGS ---------------------------------------")
        for r in sorted(rotating, key=lambda x: x['rotation_score'], reverse=True):
            print(f"  {r['ticker']:<6s} | score={r['rotation_score']:.3f} | {r['rotation_state']}")

    if DRY_RUN:
        print(f"\n  (DRY RUN -- no writes performed)")

    print("=" * 64)


if __name__ == '__main__':
    main()
