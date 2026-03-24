"""
SHIFT ENGINE -- Early Regime Transition Detection
===================================================
Detects when a ticker is approaching a structural shift BEFORE price
confirms. Computes a shift_score from GEX, flow, vol regime, and
DeMark momentum deltas, then writes to shift_scores and merges
results into scanner_candidates.

This is a STATE + TRANSITION detection layer, not an indicator blend.
Output is deterministic: same input = same output.

Usage:
    python shift_engine.py          # full run
    python shift_engine.py --dry    # evaluate only, skip writes
"""

import os
import sys
import io
import json
import math
import statistics
from datetime import date, datetime, timezone
from pathlib import Path

# Force UTF-8 stdout to prevent encoding errors on Windows
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
            print(f"[shift] Loaded env from {p}")
            return
    print("[shift] No .env found")

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
# CONFIGURATION
# ---------------------------------------------------------------------------

# Feature weights (must sum to 1.0)
W_GEX_DELTA   = 0.30
W_VOL_PROXY   = 0.25
W_VIX_SLOPE   = 0.20
W_FLOW_DELTA  = 0.15
W_MOMENTUM    = 0.10

# Z-score history window
Z_WINDOW_MIN = 5     # minimum observations needed
Z_WINDOW_MAX = 60    # max lookback

# Shift label thresholds
LABEL_IMMINENT = 1.5
LABEL_RISING   = 1.0
LABEL_BUILDING = 0.5

# Vol regime mapping
VOL_REGIME_MAP = {
    'COMPRESSED':  -1,
    'NORMAL':       0,
    'CALM':         0,
    'LOW':         -1,
    'NEUTRAL':      0,
    'ELEVATED':     1,
    'HIGH':         1,
    'EXPANDED':     2,
    'EXTREME':      2,
    'CRISIS':       2,
}


# ---------------------------------------------------------------------------
# SUPABASE REST HELPERS
# ---------------------------------------------------------------------------
def sb_get(table, params):
    """GET rows from Supabase REST API."""
    url = SB_URL + '/rest/v1/' + table
    try:
        r = requests.get(url, headers={
            'apikey': SB_KEY,
            'Authorization': 'Bearer ' + SB_KEY,
        }, params=params, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"  [!] sb_get {table} error: {e}")
    return []


def sb_upsert(table, rows):
    """UPSERT rows into Supabase (requires unique constraint)."""
    if not rows:
        return True
    url = SB_URL + '/rest/v1/' + table
    h = dict(HEADERS)
    h['Prefer'] = 'resolution=merge-duplicates,return=minimal'
    try:
        r = requests.post(url, headers=h, json=rows, timeout=30)
        if r.status_code in (200, 201, 204):
            return True
        print(f"  [!] Upsert {table}: HTTP {r.status_code} -- {r.text[:200]}")
    except Exception as e:
        print(f"  [!] Upsert {table} error: {e}")
    return False


def sb_delete_today(table):
    """Delete today's rows from a table."""
    url = SB_URL + '/rest/v1/' + table
    h = dict(HEADERS)
    h['Prefer'] = 'return=minimal'
    try:
        r = requests.delete(url, headers=h, params={
            'as_of': 'gte.' + TODAY_ISO,
        }, timeout=15)
        return r.status_code in (200, 204)
    except Exception:
        return False


def sb_patch(table, params, body):
    """PATCH (update) rows matching params."""
    url = SB_URL + '/rest/v1/' + table
    h = dict(HEADERS)
    h['Prefer'] = 'return=minimal'
    try:
        r = requests.patch(url, headers=h, params=params, json=body, timeout=15)
        return r.status_code in (200, 204)
    except Exception as e:
        print(f"  [!] Patch {table} error: {e}")
        return False


# ---------------------------------------------------------------------------
# STEP 0 -- LOAD TICKER UNIVERSE
# ---------------------------------------------------------------------------
def load_tickers():
    """Load tickers from ticker_universe table."""
    rows = sb_get('ticker_universe', {
        'select': 'ticker',
        'order': 'ticker.asc',
    })
    tickers = [r['ticker'] for r in rows if r.get('ticker')]
    if tickers:
        print(f"[shift] Loaded {len(tickers)} tickers from ticker_universe")
        return tickers
    # Fallback
    fallback = ['SLV', 'GLD', 'GDX', 'SILJ', 'SIL', 'COPX', 'SPY',
                'QQQ', 'IWM', 'XLE', 'USO', 'BITO', 'TLT']
    print(f"[shift] Using fallback tickers ({len(fallback)})")
    return fallback


# ---------------------------------------------------------------------------
# STEP 1 -- FETCH HISTORICAL DATA FOR FEATURE DELTAS
# ---------------------------------------------------------------------------
def fetch_history(table, ticker, select_cols, limit=60, order_col='run_ts'):
    """Fetch last N rows from a table for a ticker, ordered newest first."""
    return sb_get(table, {
        'select': select_cols,
        'ticker': 'eq.' + ticker,
        'order': order_col + '.desc',
        'limit': str(limit),
    })


def compute_feature_deltas(ticker):
    """
    STEP 1: Compute raw feature deltas for a ticker.
    Returns dict of raw features and their histories for z-scoring.
    """
    features = {}
    histories = {}

    # --- 1a. GEX delta ---
    gex_rows = fetch_history('gex_snapshots', ticker,
                             'net_gex,gex_regime,run_ts', limit=Z_WINDOW_MAX)
    if len(gex_rows) >= 2:
        gex_values = [float(r['net_gex']) for r in gex_rows if r.get('net_gex') is not None]
        if len(gex_values) >= 2:
            features['gex_delta'] = gex_values[0] - gex_values[1]
            # Build delta history for z-score
            histories['gex_delta'] = [
                gex_values[i] - gex_values[i + 1]
                for i in range(len(gex_values) - 1)
            ]
        else:
            # Regime flip proxy: map regime changes to numeric
            regime_map = {'NEGATIVE': -1, 'NEUTRAL': 0, 'POSITIVE': 1}
            r0 = regime_map.get((gex_rows[0].get('gex_regime') or '').upper(), 0)
            r1 = regime_map.get((gex_rows[1].get('gex_regime') or '').upper(), 0)
            features['gex_delta'] = float(r0 - r1)
            histories['gex_delta'] = [features['gex_delta']]
    else:
        features['gex_delta'] = 0.0
        histories['gex_delta'] = []

    # --- 1b. Flow delta ---
    flow_rows = fetch_history('flow_snapshots', ticker,
                              'net_notional,run_ts', limit=Z_WINDOW_MAX)
    if len(flow_rows) >= 2:
        flow_values = [float(r['net_notional']) for r in flow_rows
                       if r.get('net_notional') is not None]
        if len(flow_values) >= 2:
            features['flow_delta'] = flow_values[0] - flow_values[1]
            histories['flow_delta'] = [
                flow_values[i] - flow_values[i + 1]
                for i in range(len(flow_values) - 1)
            ]
        else:
            features['flow_delta'] = 0.0
            histories['flow_delta'] = []
    else:
        features['flow_delta'] = 0.0
        histories['flow_delta'] = []

    # --- 1c. Vol proxy (scenario synthesis vol_regime change) ---
    scen_rows = fetch_history('scenario_synthesis', ticker,
                              'vol_regime,run_ts', limit=Z_WINDOW_MAX)
    if len(scen_rows) >= 2:
        def vol_to_num(regime_str):
            if not regime_str:
                return 0
            for key, val in VOL_REGIME_MAP.items():
                if key in regime_str.upper():
                    return val
            return 0

        vol_values = [vol_to_num(r.get('vol_regime')) for r in scen_rows]
        features['vol_proxy'] = float(vol_values[0] - vol_values[1])
        histories['vol_proxy'] = [
            float(vol_values[i] - vol_values[i + 1])
            for i in range(len(vol_values) - 1)
        ]
    else:
        features['vol_proxy'] = 0.0
        histories['vol_proxy'] = []

    # --- 1d. VIX slope proxy (same vol_regime delta, different weighting) ---
    # Re-use the vol delta but shift the frame by 1 observation
    if len(scen_rows) >= 3:
        def vol_to_num2(regime_str):
            if not regime_str:
                return 0
            for key, val in VOL_REGIME_MAP.items():
                if key in regime_str.upper():
                    return val
            return 0

        vol_vals = [vol_to_num2(r.get('vol_regime')) for r in scen_rows]
        # Slope = (latest delta) - (previous delta) = acceleration
        d0 = vol_vals[0] - vol_vals[1]
        d1 = vol_vals[1] - vol_vals[2] if len(vol_vals) >= 3 else 0
        features['vix_slope'] = float(d0 - d1)
        histories['vix_slope'] = []
        for i in range(len(vol_vals) - 2):
            dx = vol_vals[i] - vol_vals[i + 1]
            dx1 = vol_vals[i + 1] - vol_vals[i + 2]
            histories['vix_slope'].append(float(dx - dx1))
    else:
        features['vix_slope'] = 0.0
        histories['vix_slope'] = []

    # --- 1e. Momentum flag (DeMark) ---
    dm_rows = fetch_history('demark_signals', ticker,
                            'setup_direction,setup_count,signal_state,run_ts',
                            limit=1)
    if dm_rows:
        dm = dm_rows[0]
        count = dm.get('setup_count') or 0
        direction = (dm.get('setup_direction') or '').upper()
        state = (dm.get('signal_state') or '').upper()

        if count >= 7 or 'PERFECT' in state or '9' in state:
            if direction == 'BUY' or 'BUY' in state:
                features['momentum_flag'] = 1.0
            elif direction == 'SELL' or 'SELL' in state:
                features['momentum_flag'] = -1.0
            else:
                features['momentum_flag'] = 0.0
        else:
            features['momentum_flag'] = 0.0
    else:
        features['momentum_flag'] = 0.0

    # momentum_flag doesn't need z-scoring (it's already categorical)
    histories['momentum_flag'] = []

    return features, histories


# ---------------------------------------------------------------------------
# STEP 2 -- NORMALIZE FEATURES (Z-SCORE)
# ---------------------------------------------------------------------------
def z_score(value, history, min_obs=Z_WINDOW_MIN):
    """
    Compute z-score of value against history.
    Returns clipped z-score in [-3, +3].
    If insufficient history, return the raw value clipped to [-3, +3].
    """
    if not history or len(history) < min_obs:
        # Not enough data for proper z-score -- clip raw value
        return max(-3.0, min(3.0, value))

    mean = statistics.mean(history)
    stdev = statistics.stdev(history) if len(history) >= 2 else 0.0

    if stdev < 1e-10:
        # No variance -- return 0 if value matches mean, else direction
        if abs(value - mean) < 1e-10:
            return 0.0
        return 3.0 if value > mean else -3.0

    z = (value - mean) / stdev
    return max(-3.0, min(3.0, round(z, 4)))


def normalize_features(features, histories):
    """STEP 2: Compute z-scores for each feature."""
    z_scores = {}

    # Z-score the continuous features
    for key in ['gex_delta', 'flow_delta', 'vol_proxy', 'vix_slope']:
        z_scores[key] = z_score(features[key], histories.get(key, []))

    # Momentum flag is already categorical (-1, 0, +1) -- pass through
    z_scores['momentum_flag'] = features['momentum_flag']

    return z_scores


# ---------------------------------------------------------------------------
# STEP 3 -- COMPUTE SHIFT SCORE
# ---------------------------------------------------------------------------
def compute_shift_score(z_scores):
    """
    STEP 3: Weighted combination of z-scored features.
    shift_score = 0.30*z_gex + 0.25*z_vol + 0.20*z_vix + 0.15*z_flow + 0.10*momentum
    """
    score = (
        W_GEX_DELTA  * z_scores['gex_delta'] +
        W_VOL_PROXY  * z_scores['vol_proxy'] +
        W_VIX_SLOPE  * z_scores['vix_slope'] +
        W_FLOW_DELTA * z_scores['flow_delta'] +
        W_MOMENTUM   * z_scores['momentum_flag']
    )
    return round(score, 4)


# ---------------------------------------------------------------------------
# STEP 4 -- LABEL
# ---------------------------------------------------------------------------
def label_shift(score):
    """STEP 4: Classify shift score into a label."""
    abs_score = abs(score)
    if abs_score >= LABEL_IMMINENT:
        return "IMMINENT"
    elif abs_score >= LABEL_RISING:
        return "RISING"
    elif abs_score >= LABEL_BUILDING:
        return "BUILDING"
    else:
        return "LOW"


# ---------------------------------------------------------------------------
# STEP 5 -- BUILD OUTPUT
# ---------------------------------------------------------------------------
def build_output(ticker, score, label, z_scores):
    """STEP 5: Build the output dict."""
    return {
        "ticker": ticker,
        "as_of": datetime.now(timezone.utc).isoformat(),
        "shift_score": score,
        "shift_label": label,
        "components": json.dumps({
            "gex_delta_z": z_scores['gex_delta'],
            "vol_proxy_z": z_scores['vol_proxy'],
            "vix_slope_z": z_scores['vix_slope'],
            "flow_delta_z": z_scores['flow_delta'],
            "momentum_flag": z_scores['momentum_flag'],
        }),
    }


# ---------------------------------------------------------------------------
# STEP 6 -- WRITE TO SUPABASE (shift_scores)
# ---------------------------------------------------------------------------
def write_shift_scores(rows):
    """STEP 6: Delete today's rows, then insert fresh."""
    if not rows:
        return True

    # Delete today's rows
    sb_delete_today('shift_scores')

    # Insert (not upsert -- we've already deleted today's)
    url = SB_URL + '/rest/v1/shift_scores'
    try:
        r = requests.post(url, headers=HEADERS, json=rows, timeout=30)
        if r.status_code in (200, 201, 204):
            return True
        print(f"  [!] shift_scores insert: HTTP {r.status_code} -- {r.text[:300]}")
        return False
    except Exception as e:
        print(f"  [!] shift_scores insert error: {e}")
        return False


# ---------------------------------------------------------------------------
# STEP 7 -- MERGE INTO SCANNER
# ---------------------------------------------------------------------------
def merge_into_scanner(results):
    """
    STEP 7: Update scanner_candidates with shift_score and shift_label.
    Patches each ticker's latest scanner row.
    """
    updated = 0
    for result in results:
        ticker = result['ticker']
        ok = sb_patch('scanner_candidates', {
            'ticker': 'eq.' + ticker,
            'run_ts': 'gte.' + TODAY_ISO,
        }, {
            'shift_score': result['shift_score'],
            'shift_label': result['shift_label'],
        })
        if ok:
            updated += 1
    return updated


# ---------------------------------------------------------------------------
# STEP 8 -- BATCH RUNNER
# ---------------------------------------------------------------------------
def process_ticker(ticker):
    """Process a single ticker through the full shift pipeline."""
    try:
        # Step 1: Feature deltas
        features, histories = compute_feature_deltas(ticker)

        # Step 2: Normalize
        z_scores = normalize_features(features, histories)

        # Step 3: Compute score
        shift_score = compute_shift_score(z_scores)

        # Step 4: Label
        shift_label = label_shift(shift_score)

        # Step 5: Build output
        output = build_output(ticker, shift_score, shift_label, z_scores)

        return output, features, z_scores

    except Exception as e:
        print(f"  [!] {ticker} processing error: {e}")
        return None, None, None


def run_batch(tickers):
    """STEP 8: Process all tickers in batch."""
    results = []
    feature_log = []

    for ticker in tickers:
        output, features, z_scores = process_ticker(ticker)
        if output:
            results.append(output)
            feature_log.append((ticker, features, z_scores, output))

    return results, feature_log


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=" * 64)
    print("SHIFT ENGINE -- Early Regime Transition Detection")
    print(f"Date: {TODAY_ISO}")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"Supabase: {SB_URL[:45]}...")
    print("=" * 64)

    # Load universe
    tickers = load_tickers()
    if not tickers:
        print("[shift] No tickers found. Exiting.")
        return

    # Process all tickers
    print(f"\n[shift] Processing {len(tickers)} tickers...\n")
    results, feature_log = run_batch(tickers)

    # Debug print per ticker
    for ticker, features, z_scores, output in feature_log:
        label = output['shift_label']
        score = output['shift_score']
        icon = '!' if label == 'IMMINENT' else '*' if label == 'RISING' else '+' if label == 'BUILDING' else '.'
        print(f"  [{icon}] {ticker:<6s} | score={score:+.4f} | {label:<10s} | "
              f"gex_z={z_scores['gex_delta']:+.2f} "
              f"vol_z={z_scores['vol_proxy']:+.2f} "
              f"vix_z={z_scores['vix_slope']:+.2f} "
              f"flow_z={z_scores['flow_delta']:+.2f} "
              f"mom={z_scores['momentum_flag']:+.0f}")

    # Write results
    if not DRY_RUN and results:
        print(f"\n[shift] Writing {len(results)} rows to shift_scores...")
        ok = write_shift_scores(results)
        print(f"  shift_scores: {'OK' if ok else 'FAILED'}")

        print(f"\n[shift] Merging into scanner_candidates...")
        n = merge_into_scanner(results)
        print(f"  scanner_candidates updated: {n}/{len(results)}")

    # Summary
    print()
    print("=" * 64)
    print("SHIFT ENGINE -- RUN COMPLETE")
    print("=" * 64)
    print(f"Tickers processed:  {len(results)}")

    imminent = [r for r in results if r['shift_label'] == 'IMMINENT']
    rising   = [r for r in results if r['shift_label'] == 'RISING']
    building = [r for r in results if r['shift_label'] == 'BUILDING']
    low      = [r for r in results if r['shift_label'] == 'LOW']

    print(f"  IMMINENT:  {len(imminent)}")
    print(f"  RISING:    {len(rising)}")
    print(f"  BUILDING:  {len(building)}")
    print(f"  LOW:       {len(low)}")

    if imminent:
        print()
        print("--- IMMINENT SHIFTS -----------------------------------------")
        for r in sorted(imminent, key=lambda x: abs(x['shift_score']), reverse=True):
            direction = "UP" if r['shift_score'] > 0 else "DOWN"
            print(f"  {r['ticker']:<6s} | score={r['shift_score']:+.4f} | {direction}")

    if rising:
        print()
        print("--- RISING SHIFTS -------------------------------------------")
        for r in sorted(rising, key=lambda x: abs(x['shift_score']), reverse=True):
            direction = "UP" if r['shift_score'] > 0 else "DOWN"
            print(f"  {r['ticker']:<6s} | score={r['shift_score']:+.4f} | {direction}")

    # Top 5 by absolute shift score
    top5 = sorted(results, key=lambda x: abs(x['shift_score']), reverse=True)[:5]
    if top5:
        print()
        print("--- TOP 5 BY ABS SHIFT SCORE --------------------------------")
        print(f"  {'TICKER':<8s} {'SCORE':>8s}  {'LABEL':<10s}  DIR")
        print(f"  {'-'*40}")
        for r in top5:
            direction = "UP" if r['shift_score'] > 0 else "DOWN"
            print(f"  {r['ticker']:<8s} {r['shift_score']:>+8.4f}  {r['shift_label']:<10s}  {direction}")

    if DRY_RUN:
        print(f"\n  (DRY RUN -- no writes performed)")

    print("=" * 64)


if __name__ == '__main__':
    main()
