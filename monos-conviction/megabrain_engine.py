"""
MEGABRAIN ENGINE v2 -- MSA-Gated Convexity Rotation System
=============================================================
The governing intelligence layer for MONOS. Combines:
  1. MSA State (BULLISH/BEARISH/NEUTRAL/INVALID) -- HIGHEST PRIORITY
  2. Pre-MSA scores (bull + bear accumulation/distribution)
  3. Shift scores (timing/acceleration)
  4. Rotation state (BULL -> BUILDING_TOP -> ROTATING -> BEAR_BUILDING -> BEAR)
  5. Transition probability (sigmoid combination)
  6. Convexity readiness (GOOD/OK/POOR)
  7. Action state (BUILD_LADDER / WATCH / USE_DEFINED_RISK / WAIT)
  8. Bidirectional ladder trigger (CALL_LADDER / PUT_LADDER / SPREAD_ONLY)
  9. Ranking score = opportunity + (shift * 10) + (pre_msa * 8)

CRITICAL RULES:
  - MSA OVERRIDES ALL OTHER SIGNALS (no exceptions)
  - IF msa_state != BULLISH -> NO aggressive long convexity
  - IF msa_state != BEARISH -> NO aggressive short convexity
  - Bull ladder requires: msa_state==BULLISH AND pre_msa_bull>=1.25 AND shift>=1.0
  - Bear ladder requires: msa_state==BEARISH AND pre_msa_bear>=1.25 AND shift>=1.0
  - All outputs deterministic. All logic explainable.

Usage:
    python megabrain_engine.py          # full run
    python megabrain_engine.py --dry    # evaluate only, skip writes
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
            print(f"[megabrain] Loaded env from {p}")
            return
    print("[megabrain] No .env found")

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
    h = dict(HEADERS)
    try:
        r = requests.delete(url, headers=h, params={
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
    h = dict(HEADERS)
    try:
        r = requests.patch(url, headers=h, params=params, json=body, timeout=15)
        return r.status_code in (200, 204)
    except Exception as e:
        print(f"  [!] patch {table}: {e}")
        return False


# ---------------------------------------------------------------------------
# SIGMOID
# ---------------------------------------------------------------------------
def sigmoid(x):
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    else:
        z = math.exp(x)
        return z / (1.0 + z)


def _safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# LOAD ALL INPUTS (bulk -- efficient)
# ---------------------------------------------------------------------------
def load_all_inputs():
    """Fetch all upstream data in bulk."""

    # Ticker universe
    tickers = sb_get('ticker_universe', {
        'select': 'ticker',
        'order': 'ticker.asc',
    })
    ticker_list = [r['ticker'] for r in tickers if r.get('ticker')]

    # Shift scores (today)
    shift_rows = sb_get('shift_scores', {
        'select': 'ticker,shift_score,shift_label,components',
        'as_of': 'gte.' + TODAY_ISO,
    })
    shift_map = {r['ticker']: r for r in shift_rows}

    # Pre-MSA scores (today)
    pre_msa_rows = sb_get('pre_msa_scores', {
        'select': 'ticker,pre_msa_bull_score,pre_msa_bear_score,'
                  'pre_msa_label_bull,pre_msa_label_bear',
        'as_of': 'gte.' + TODAY_ISO,
    })
    pre_msa_map = {r['ticker']: r for r in pre_msa_rows}

    # Rotation states (today)
    rot_rows = sb_get('rotation_states', {
        'select': 'ticker,rotation_score,rotation_state',
        'as_of': 'gte.' + TODAY_ISO,
    })
    rot_map = {r['ticker']: r for r in rot_rows}

    # Scenario synthesis (latest per ticker)
    scen_rows = sb_get('scenario_synthesis', {
        'select': 'ticker,overall_bias,primary_scenario,primary_probability,'
                  'confidence_score,engines_agreement,engines_total,vol_regime',
        'order': 'run_ts.desc',
        'limit': '100',
    })
    scen_map = {}
    for r in scen_rows:
        if r['ticker'] not in scen_map:
            scen_map[r['ticker']] = r

    # GEX snapshots (latest per ticker)
    gex_rows = sb_get('gex_snapshots', {
        'select': 'ticker,gex_regime,net_gex,gamma_flip,spot_price',
        'order': 'run_ts.desc',
        'limit': '100',
    })
    gex_map = {}
    for r in gex_rows:
        if r['ticker'] not in gex_map:
            gex_map[r['ticker']] = r

    # Symmetry snapshots (latest per ticker)
    sym_rows = sb_get('symmetry_snapshots', {
        'select': 'ticker,convexity_state,symmetry_score,atm_iv,hv20,iv_rv_gap_20d',
        'order': 'run_ts.desc',
        'limit': '100',
    })
    sym_map = {}
    for r in sym_rows:
        if r['ticker'] not in sym_map:
            sym_map[r['ticker']] = r

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

    # Scanner candidates (today)
    scan_rows = sb_get('scanner_candidates', {
        'select': 'ticker,opportunity_score,gex_regime,demark_state,scenario_bias',
        'run_ts': 'gte.' + TODAY_ISO,
    })
    scan_map = {r['ticker']: r for r in scan_rows}

    return (ticker_list, shift_map, pre_msa_map, rot_map,
            scen_map, gex_map, sym_map, con_map, scan_map)


# ---------------------------------------------------------------------------
# MSA STATE (HIGHEST PRIORITY)
# ---------------------------------------------------------------------------
def compute_msa_state(scen, pre_msa, rotation, con):
    """
    Derive MSA state -- the structural permission layer.
    MSA = what direction is structurally allowed.

    Returns: BULLISH, BEARISH, NEUTRAL, INVALID
    """
    # Start from scenario bias
    bias = (scen.get('overall_bias') or '').upper() if scen else ''
    confidence = _safe_float(scen.get('confidence_score')) if scen else 0

    # Get rotation state
    rot_state = (rotation.get('rotation_state') or '').upper() if rotation else 'BULL'

    # Get conflict state
    conflict = (con.get('conflict_state') or '').upper() if con else ''
    active_count = con.get('active_count') or 0 if con else 0

    # Pre-MSA scores
    bull_score = _safe_float(pre_msa.get('pre_msa_bull_score')) if pre_msa else 0
    bear_score = _safe_float(pre_msa.get('pre_msa_bear_score')) if pre_msa else 0

    # --- INVALID: engines deeply contradicted ---
    if conflict == 'CONTRADICTED' and active_count <= 1:
        return 'INVALID'

    # --- Rotation override: if already ROTATING or worse, degrade MSA ---
    if rot_state in ('BEAR', 'BEAR_BUILDING'):
        # Strong rotation toward bear -- MSA must be BEARISH or NEUTRAL
        if 'BULL' in bias:
            return 'NEUTRAL'  # bias says bull but rotation says bear -- conflict
        return 'BEARISH'

    if rot_state == 'ROTATING':
        # Transitioning -- MSA goes NEUTRAL regardless
        return 'NEUTRAL'

    # --- Standard MSA from bias ---
    if 'BULL' in bias:
        # Confirm with pre-MSA: if bear pre-MSA is building, degrade
        if bear_score >= 1.0 and bull_score < 0.5:
            return 'NEUTRAL'  # Bear building undermines bull MSA
        return 'BULLISH'

    if 'BEAR' in bias:
        # Confirm: if bull pre-MSA is building, degrade
        if bull_score >= 1.0 and bear_score < 0.5:
            return 'NEUTRAL'  # Bull building undermines bear MSA
        return 'BEARISH'

    # Neutral / chop / mixed
    # But check if pre-MSA is strongly signaling a direction
    if bull_score >= 1.5 and bear_score < 0.5:
        return 'BULLISH'  # Pre-MSA sees accumulation even in chop
    if bear_score >= 1.5 and bull_score < 0.5:
        return 'BEARISH'  # Pre-MSA sees distribution even in chop

    return 'NEUTRAL'


# ---------------------------------------------------------------------------
# TRANSITION PROBABILITY
# ---------------------------------------------------------------------------
def compute_transition_probability(shift_score, gex, sym, con):
    """
    Sigmoid combination:
      1.2 * shift_score
    + 0.5 * (gamma == NEGATIVE)
    + 0.4 * (thesis == ALIGNED + active >= 3)
    - 0.4 * (vol very rich)
    """
    x = 1.2 * shift_score

    gex_regime = (gex.get('gex_regime') or '').upper() if gex else ''
    if gex_regime == 'NEGATIVE':
        x += 0.5

    if con:
        conflict = (con.get('conflict_state') or '').upper()
        active = con.get('active_count') or 0
        if conflict == 'ALIGNED' and active >= 3:
            x += 0.4
        elif conflict == 'ALIGNED':
            x += 0.2

    if sym:
        conv_state = (sym.get('convexity_state') or '').upper()
        atm_iv = _safe_float(sym.get('atm_iv'))
        hv20 = _safe_float(sym.get('hv20'))
        iv_rich = False
        if conv_state == 'CONVEXITY_VERY_RICH':
            iv_rich = True
        elif atm_iv > 0 and hv20 > 0 and atm_iv > hv20 * 1.3:
            iv_rich = True
        if iv_rich:
            x -= 0.4

    return round(sigmoid(x), 4)


# ---------------------------------------------------------------------------
# CONVEXITY READINESS
# ---------------------------------------------------------------------------
def compute_convexity_readiness(sym, shift_label):
    """
    GOOD: vol not too rich + move not fully priced
    OK:   mixed conditions
    POOR: vol too expensive
    """
    if not sym:
        return 'OK'

    conv_state = (sym.get('convexity_state') or '').upper()
    iv_rv_gap = sym.get('iv_rv_gap_20d')

    # POOR: vol already expensive
    if conv_state == 'CONVEXITY_VERY_RICH':
        return 'POOR'
    if iv_rv_gap is not None and float(iv_rv_gap) > 8:
        return 'POOR'

    # GOOD: vol cheap/fair + shift at least building
    if shift_label in ('IMMINENT', 'RISING'):
        if conv_state in ('CONVEXITY_CHEAP', 'CONVEXITY_FAIR', 'UNKNOWN'):
            return 'GOOD'
        if conv_state == 'CONVEXITY_RICH' and shift_label == 'IMMINENT':
            return 'OK'
        return 'OK'

    if shift_label == 'BUILDING':
        if conv_state in ('CONVEXITY_CHEAP', 'CONVEXITY_FAIR'):
            return 'GOOD'
        return 'OK'

    # LOW shift
    if conv_state == 'CONVEXITY_CHEAP':
        return 'GOOD'
    if conv_state == 'CONVEXITY_RICH':
        return 'POOR'
    return 'OK'


# ---------------------------------------------------------------------------
# ACTION STATE
# ---------------------------------------------------------------------------
def compute_action_state(msa_state, transition_prob, convexity_readiness,
                         rotation_state):
    """
    Action state respects MSA gating:
      BUILD_LADDER:      prob >= 0.75 AND readiness == GOOD AND MSA directional
      WATCH_FOR_TRIGGER: prob >= 0.55 AND MSA not INVALID
      USE_DEFINED_RISK:  MSA == NEUTRAL or rotation active
      WAIT:              everything else / MSA == INVALID
    """
    # MSA INVALID = no action permitted
    if msa_state == 'INVALID':
        return 'WAIT'

    # Rotation degradation
    if rotation_state in ('ROTATING', 'BEAR_BUILDING', 'BEAR'):
        if transition_prob >= 0.55:
            return 'USE_DEFINED_RISK'  # can still play, but defined risk only
        return 'WAIT'

    # Standard flow
    if transition_prob >= 0.75 and convexity_readiness == 'GOOD':
        if msa_state in ('BULLISH', 'BEARISH'):
            return 'BUILD_LADDER'
        return 'WATCH_FOR_TRIGGER'  # NEUTRAL MSA -- close but no permission

    if transition_prob >= 0.55:
        return 'WATCH_FOR_TRIGGER'

    if msa_state == 'NEUTRAL':
        return 'USE_DEFINED_RISK'

    return 'WAIT'


# ---------------------------------------------------------------------------
# CONVEXITY LADDER TRIGGER (MSA-GATED)
# ---------------------------------------------------------------------------
def compute_ladder_trigger(msa_state, pre_msa, shift_score, shift_label,
                           gex, con, convexity_readiness, rotation_state):
    """
    Bidirectional ladder trigger with MSA gating.

    BULL LADDER: msa==BULLISH AND pre_msa_bull>=1.25 AND shift>=1.0
                 AND gamma==NEGATIVE AND convexity!=POOR
    BEAR LADDER: msa==BEARISH AND pre_msa_bear>=1.25 AND shift>=1.0
                 AND gamma==NEGATIVE AND convexity!=POOR

    If MSA disagrees: BLOCK ladder triggers.
    """
    gex_regime = (gex.get('gex_regime') or '').upper() if gex else ''
    conflict = (con.get('conflict_state') or '').upper() if con else ''

    bull_score = _safe_float(pre_msa.get('pre_msa_bull_score')) if pre_msa else 0
    bear_score = _safe_float(pre_msa.get('pre_msa_bear_score')) if pre_msa else 0

    # Block if rotation is active
    if rotation_state in ('ROTATING', 'BEAR_BUILDING', 'BEAR'):
        # Only allow bear ladder if MSA is BEARISH during rotation
        if msa_state == 'BEARISH' and bear_score >= 1.25:
            if abs(shift_score) >= 1.0 and convexity_readiness != 'POOR':
                urgency = _shift_urgency(shift_label)
                return True, 'PUT_LADDER', urgency
        return False, 'NONE', 'NONE'

    # --- BULL LADDER ---
    if msa_state == 'BULLISH':
        bull_trigger = (
            bull_score >= 1.25
            and shift_score >= 1.0
            and gex_regime == 'NEGATIVE'
            and convexity_readiness != 'POOR'
        )
        if bull_trigger:
            if shift_score >= 1.5:
                style = 'CALL_LADDER'
            else:
                style = 'SPREAD_ONLY'
            urgency = _shift_urgency(shift_label)
            return True, style, urgency

        # Near-trigger: suggest small structures
        if bull_score >= 0.75 and shift_score >= 0.5:
            return False, 'SPREAD_ONLY', _shift_urgency(shift_label)

    # --- BEAR LADDER ---
    if msa_state == 'BEARISH':
        bear_trigger = (
            bear_score >= 1.25
            and abs(shift_score) >= 1.0
            and gex_regime == 'NEGATIVE'  # negative gamma = expansion both ways
            and convexity_readiness != 'POOR'
        )
        if bear_trigger:
            if abs(shift_score) >= 1.5:
                style = 'PUT_LADDER'
            else:
                style = 'SPREAD_ONLY'
            urgency = _shift_urgency(shift_label)
            return True, style, urgency

        if bear_score >= 0.75 and abs(shift_score) >= 0.5:
            return False, 'SPREAD_ONLY', _shift_urgency(shift_label)

    # --- NEUTRAL / INVALID: no ladder ---
    return False, 'NONE', 'NONE'


def _shift_urgency(shift_label):
    """Map shift_label to urgency."""
    return {
        'IMMINENT': 'HIGH',
        'RISING': 'MEDIUM',
        'BUILDING': 'LOW',
        'LOW': 'NONE',
    }.get(shift_label, 'NONE')


# ---------------------------------------------------------------------------
# REASON CODES
# ---------------------------------------------------------------------------
def build_reason_codes(msa_state, pre_msa, shift_label, gex, sym, con,
                       transition_prob, convexity_readiness, rotation_state):
    """Build explainable reason codes array."""
    codes = []

    # MSA state
    codes.append(f'MSA_{msa_state}')

    # Pre-MSA
    if pre_msa:
        bl = (pre_msa.get('pre_msa_label_bull') or '').upper()
        brl = (pre_msa.get('pre_msa_label_bear') or '').upper()
        if bl in ('PRE-TRIGGER', 'BUILDING'):
            codes.append(f'PRE_MSA_BULL_{bl.replace("-", "_")}')
        if brl in ('PRE-TRIGGER', 'BUILDING'):
            codes.append(f'PRE_MSA_BEAR_{brl.replace("-", "_")}')

    # Rotation
    if rotation_state in ('ROTATING', 'BEAR_BUILDING', 'BEAR'):
        codes.append(f'ROTATION_{rotation_state}')

    # Shift
    if shift_label in ('IMMINENT', 'RISING'):
        codes.append(f'SHIFT_{shift_label}')

    # GEX
    gex_regime = (gex.get('gex_regime') or '').upper() if gex else ''
    if gex_regime == 'NEGATIVE':
        codes.append('GEX_NEGATIVE_GAMMA')
    elif gex_regime == 'POSITIVE':
        codes.append('GEX_POSITIVE_GAMMA')

    # Conflict
    conflict = (con.get('conflict_state') or '').upper() if con else ''
    if conflict == 'ALIGNED':
        codes.append('ENGINES_ALIGNED')
    elif conflict == 'CONTRADICTED':
        codes.append('ENGINES_CONTRADICTED')

    # Vol
    conv_state = (sym.get('convexity_state') or '').upper() if sym else ''
    if 'CHEAP' in conv_state:
        codes.append('VOL_CHEAP')
    elif 'VERY_RICH' in conv_state:
        codes.append('VOL_VERY_RICH')
    elif 'RICH' in conv_state:
        codes.append('VOL_RICH')

    # Transition probability
    if transition_prob >= 0.75:
        codes.append('HIGH_TRANSITION_PROB')
    elif transition_prob >= 0.55:
        codes.append('MODERATE_TRANSITION_PROB')

    # Convexity readiness
    if convexity_readiness == 'GOOD':
        codes.append('CONVEXITY_READY')
    elif convexity_readiness == 'POOR':
        codes.append('CONVEXITY_NOT_READY')

    return codes


# ---------------------------------------------------------------------------
# PROCESS ONE TICKER
# ---------------------------------------------------------------------------
def process_ticker(ticker, shift_map, pre_msa_map, rot_map,
                   scen_map, gex_map, sym_map, con_map, scan_map):
    """Full megabrain v2 pipeline for a single ticker."""

    shift = shift_map.get(ticker, {})
    pre_msa = pre_msa_map.get(ticker)
    rotation = rot_map.get(ticker)
    scen = scen_map.get(ticker)
    gex = gex_map.get(ticker)
    sym = sym_map.get(ticker)
    con = con_map.get(ticker)
    scan = scan_map.get(ticker)

    shift_score = _safe_float(shift.get('shift_score'))
    shift_label = shift.get('shift_label') or 'LOW'
    rotation_state = (rotation.get('rotation_state') or 'BULL').upper() if rotation else 'BULL'

    # 1. MSA STATE (HIGHEST PRIORITY)
    msa_state = compute_msa_state(scen, pre_msa, rotation, con)

    # 2. Transition probability
    transition_prob = compute_transition_probability(shift_score, gex, sym, con)

    # 3. Convexity readiness
    convexity_readiness = compute_convexity_readiness(sym, shift_label)

    # 4. Action state (MSA-gated)
    action_state = compute_action_state(
        msa_state, transition_prob, convexity_readiness, rotation_state
    )

    # 5. Ladder trigger (MSA-gated, bidirectional)
    ladder_trigger, ladder_style, ladder_urgency = compute_ladder_trigger(
        msa_state, pre_msa, shift_score, shift_label,
        gex, con, convexity_readiness, rotation_state
    )

    # 6. Reason codes
    reason_codes = build_reason_codes(
        msa_state, pre_msa, shift_label, gex, sym, con,
        transition_prob, convexity_readiness, rotation_state
    )

    # 7. Ranking score = opportunity + (|shift| * 10) + (max(pre_msa) * 8)
    opp_score = _safe_float(scan.get('opportunity_score')) if scan else 0
    bull_pre = _safe_float(pre_msa.get('pre_msa_bull_score')) if pre_msa else 0
    bear_pre = _safe_float(pre_msa.get('pre_msa_bear_score')) if pre_msa else 0
    max_pre_msa = max(bull_pre, bear_pre)
    ranking_score = round(opp_score + (abs(shift_score) * 10) + (max_pre_msa * 8), 2)

    # Build megabrain state row
    mb_row = {
        'ticker': ticker,
        'as_of': datetime.now(timezone.utc).isoformat(),
        'msa_state': msa_state,
        'pre_msa_bull': bull_pre,
        'pre_msa_bear': bear_pre,
        'market_state': msa_state,  # keep for backward compat
        'shift_label': shift_label,
        'shift_score': shift_score,
        'transition_probability': transition_prob,
        'convexity_readiness': convexity_readiness,
        'action_state': action_state,
        'rotation_state': rotation_state,
        'ladder_trigger': ladder_trigger,
        'ladder_style': ladder_style,
        'ladder_urgency': ladder_urgency,
        'ranking_score': ranking_score,
        'reason_codes': json.dumps(reason_codes),
    }

    # Scanner patch
    scanner_patch = {
        'shift_score': shift_score,
        'shift_label': shift_label,
        'msa_state': msa_state,
        'rotation_state': rotation_state,
        'ladder_trigger': ladder_trigger,
        'ladder_style': ladder_style,
        'ladder_urgency': ladder_urgency,
        'ranking_score': ranking_score,
    }

    return mb_row, scanner_patch, reason_codes


# ---------------------------------------------------------------------------
# WRITE MEGABRAIN STATES
# ---------------------------------------------------------------------------
def write_megabrain_states(rows):
    sb_delete_today('megabrain_states')
    return sb_insert('megabrain_states', rows)


# ---------------------------------------------------------------------------
# MERGE INTO SCANNER
# ---------------------------------------------------------------------------
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
    print("MEGABRAIN ENGINE v2 -- MSA-Gated Convexity Rotation System")
    print(f"Date: {TODAY_ISO}")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"Supabase: {SB_URL[:45]}...")
    print("=" * 64)

    # Load all inputs
    print("\n[megabrain] Loading upstream data...")
    (ticker_list, shift_map, pre_msa_map, rot_map,
     scen_map, gex_map, sym_map, con_map, scan_map) = load_all_inputs()

    print(f"  Tickers:    {len(ticker_list)}")
    print(f"  Shifts:     {len(shift_map)}")
    print(f"  Pre-MSA:    {len(pre_msa_map)}")
    print(f"  Rotation:   {len(rot_map)}")
    print(f"  Scenarios:  {len(scen_map)}")
    print(f"  GEX:        {len(gex_map)}")
    print(f"  Symmetry:   {len(sym_map)}")
    print(f"  Conflict:   {len(con_map)}")
    print(f"  Scanner:    {len(scan_map)}")

    if not ticker_list:
        print("\n[megabrain] No tickers. Exiting.")
        return

    # Process all tickers
    print(f"\n[megabrain] Processing {len(ticker_list)} tickers...\n")
    mb_rows = []
    scanner_patches = {}
    all_results = []

    for ticker in ticker_list:
        mb_row, scanner_patch, reason_codes = process_ticker(
            ticker, shift_map, pre_msa_map, rot_map,
            scen_map, gex_map, sym_map, con_map, scan_map
        )
        mb_rows.append(mb_row)
        scanner_patches[ticker] = scanner_patch

        r = mb_row
        act_icon = {
            'BUILD_LADDER': '!',
            'WATCH_FOR_TRIGGER': '*',
            'USE_DEFINED_RISK': '~',
            'WAIT': '.',
        }.get(r['action_state'], '?')

        msa_icon = {
            'BULLISH': 'B+', 'BEARISH': 'B-', 'NEUTRAL': 'N ', 'INVALID': 'X ',
        }.get(r['msa_state'], '? ')

        lad_str = f"LAD={r['ladder_style']}" if r['ladder_trigger'] else "no-trigger"
        rot_str = r['rotation_state'][:4] if r['rotation_state'] != 'BULL' else ""

        print(f"  [{act_icon}] {ticker:<6s} | "
              f"MSA={msa_icon} | "
              f"prob={r['transition_probability']:.2f} | "
              f"conv={r['convexity_readiness']:<5s} | "
              f"act={r['action_state']:<20s} | "
              f"rot={rot_str:<5s} | "
              f"{lad_str}")

        all_results.append(mb_row)

    # Write
    if not DRY_RUN:
        print(f"\n[megabrain] Writing {len(mb_rows)} rows to megabrain_states...")
        ok = write_megabrain_states(mb_rows)
        print(f"  megabrain_states: {'OK' if ok else 'FAILED'}")

        print(f"\n[megabrain] Merging into scanner_candidates...")
        n = merge_into_scanner(scanner_patches)
        print(f"  scanner_candidates updated: {n}/{len(scanner_patches)}")

    # Summary
    print()
    print("=" * 64)
    print("MEGABRAIN v2 -- RUN COMPLETE")
    print("=" * 64)
    print(f"Tickers processed: {len(all_results)}")

    # MSA state distribution
    msa_counts = {}
    for r in all_results:
        m = r['msa_state']
        msa_counts[m] = msa_counts.get(m, 0) + 1
    print("\n  MSA Distribution:")
    for m in ['BULLISH', 'BEARISH', 'NEUTRAL', 'INVALID']:
        if m in msa_counts:
            print(f"    {m}: {msa_counts[m]}")

    # Action state counts
    action_counts = {}
    for r in all_results:
        a = r['action_state']
        action_counts[a] = action_counts.get(a, 0) + 1
    print("\n  Action States:")
    for a in ['BUILD_LADDER', 'WATCH_FOR_TRIGGER', 'USE_DEFINED_RISK', 'WAIT']:
        if a in action_counts:
            print(f"    {a}: {action_counts[a]}")

    # Rotation summary
    rot_counts = {}
    for r in all_results:
        rs = r['rotation_state']
        rot_counts[rs] = rot_counts.get(rs, 0) + 1
    non_bull_rot = {k: v for k, v in rot_counts.items() if k != 'BULL'}
    if non_bull_rot:
        print("\n  Rotation Warnings:")
        for rs, cnt in sorted(non_bull_rot.items()):
            print(f"    {rs}: {cnt}")

    # Ladder triggers
    triggered = [r for r in all_results if r['ladder_trigger']]
    if triggered:
        print()
        print("--- LADDER TRIGGERS (MSA-GATED) -----------------------------")
        for r in sorted(triggered, key=lambda x: abs(x['shift_score']), reverse=True):
            print(f"  {r['ticker']:<6s} | MSA={r['msa_state']:<8s} | "
                  f"shift={r['shift_score']:+.4f} | "
                  f"style={r['ladder_style']:<15s} | "
                  f"urgency={r['ladder_urgency']:<6s} | "
                  f"prob={r['transition_probability']:.2f}")

    # Top 5 by ranking score
    top5 = sorted(all_results, key=lambda x: x['ranking_score'], reverse=True)[:5]
    if top5:
        print()
        print("--- TOP 5 BY RANKING SCORE ----------------------------------")
        print(f"  {'TICKER':<8s} {'RANK':>6s}  {'MSA':<8s}  {'SHIFT':>7s}  "
              f"{'ACTION':<20s}  {'CONV':<5s}  {'ROT':<10s}")
        print(f"  {'-'*85}")
        for r in top5:
            print(f"  {r['ticker']:<8s} {r['ranking_score']:>6.1f}  "
                  f"{r['msa_state']:<8s}  {r['shift_score']:>+7.4f}  "
                  f"{r['action_state']:<20s}  {r['convexity_readiness']:<5s}  "
                  f"{r['rotation_state']}")

    # Convexity readiness summary
    good = sum(1 for r in all_results if r['convexity_readiness'] == 'GOOD')
    ok_count = sum(1 for r in all_results if r['convexity_readiness'] == 'OK')
    poor = sum(1 for r in all_results if r['convexity_readiness'] == 'POOR')
    print(f"\n  Convexity readiness: GOOD={good}  OK={ok_count}  POOR={poor}")

    if DRY_RUN:
        print(f"\n  (DRY RUN -- no writes performed)")

    print("=" * 64)


if __name__ == '__main__':
    main()
