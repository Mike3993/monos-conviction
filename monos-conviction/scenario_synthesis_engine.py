"""
MONOS Scenario Synthesis Engine
3-Layer Weighted Architecture:
  Layer 1 - Directional Thesis (40%): Regime, DeMark, Reload
  Layer 2 - Mechanical Structure (35%): GEX, Fib, Spot Structure
  Layer 3 - Convexity Valuation (25%): Symmetry, VIX modifier
Reads latest signals from all engines, scores directional agreement,
synthesizes probability-weighted scenarios, and writes results to
public.scenario_synthesis in Supabase.
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
# SUPABASE HELPER
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


# ---------------------------------------------------------------------------
# FETCH ENGINE OUTPUTS
# ---------------------------------------------------------------------------
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

    # Fib levels
    fib_rows = sb_get('fib_levels', {
        'select': '*',
        'ticker': f'eq.{ticker}',
        'order': 'run_ts.desc',
        'limit': '1',
    })
    fib = fib_rows[0] if fib_rows else None

    # Symmetry snapshots
    sym_rows = sb_get('symmetry_snapshots', {
        'select': '*',
        'ticker': f'eq.{ticker}',
        'order': 'run_ts.desc',
        'limit': '1',
    })
    sym = sym_rows[0] if sym_rows else None

    # VIX regime
    vix_rows = sb_get('vix_regime', {
        'select': '*',
        'order': 'run_ts.desc',
        'limit': '1',
    })
    vix = vix_rows[0] if vix_rows else None

    # Conflict states
    conflict_rows = sb_get('conflict_states', {
        'select': 'conflict_state',
        'ticker': f'eq.{ticker}',
        'order': 'run_ts.desc',
        'limit': '1',
    })
    conflict = conflict_rows[0] if conflict_rows else None

    return gex, dm, reload, brief, fib, sym, vix, conflict


# ---------------------------------------------------------------------------
# LAYER 1 -- DIRECTIONAL THESIS (weight 0.40)
# ---------------------------------------------------------------------------
def score_regime(brief):
    """Regime signal from briefing_reports regime_label."""
    if not brief:
        return 0
    label = (brief.get('regime_label') or '').upper()
    if 'BULL' in label:
        return 1
    elif 'BEAR' in label:
        return -1
    return 0


def score_demark(dm):
    """DeMark signal based on direction + strength threshold."""
    if not dm:
        return 0
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
        return 0
    stage = (reload.get('reload_stage') or '').upper()
    if stage == 'ACCUMULATION':
        return 1
    elif stage == 'EXHAUSTION':
        return -1
    return 0


def compute_layer1(regime_signal, demark_signal, reload_signal):
    """Layer 1: Directional Thesis -- average of active signals."""
    signals = [regime_signal, demark_signal, reload_signal]
    active = [s for s in signals if s != 0]
    score = sum(active) / len(active) if active else 0
    return score, signals


# ---------------------------------------------------------------------------
# LAYER 2 -- MECHANICAL STRUCTURE (weight 0.35)
# ---------------------------------------------------------------------------
def score_gex(gex):
    """GEX signal: NEGATIVE -> -1, POSITIVE -> +1, else 0."""
    if not gex:
        return 0
    regime = (gex.get('gex_regime') or '').upper()
    if regime == 'NEGATIVE':
        return -1
    elif regime == 'POSITIVE':
        return 1
    return 0


def score_fib(fib, spot_price):
    """Fib signal: near support in downtrend = +1, near resistance in uptrend = -1."""
    if not fib or not spot_price:
        return 0
    # Check nearest fib level distance
    nearest_pct = fib.get('nearest_distance_pct')
    direction = (fib.get('direction') or '').lower()
    if nearest_pct is None:
        # Try to compute from fib level data
        level_price = fib.get('nearest_level_price') or fib.get('fib_level_price')
        if level_price and spot_price:
            nearest_pct = abs(spot_price - level_price) / spot_price * 100
        else:
            return 0
    if nearest_pct <= 3.0:
        if direction == 'down':
            return 1   # near support in downtrend = bullish bounce potential
        else:
            return -1  # near resistance in uptrend = bearish rejection potential
    return 0


def score_spot_structure(gex):
    """Spot structure: where is spot relative to put/call walls."""
    if not gex:
        return 0
    put_wall = gex.get('put_wall')
    call_wall = gex.get('call_wall')
    spot = gex.get('spot_price')
    if not put_wall or not call_wall or not spot:
        return 0
    if spot < put_wall:
        return -1   # below put wall = bearish
    elif spot > call_wall:
        return 1    # above call wall = bullish
    else:
        midpoint = (put_wall + call_wall) / 2
        if spot < midpoint:
            return -1  # lower half of range
        else:
            return 1   # upper half of range


def compute_layer2(gex_signal, fib_signal, spot_structure_signal):
    """Layer 2: Mechanical Structure -- average of active signals."""
    signals = [gex_signal, fib_signal, spot_structure_signal]
    active = [s for s in signals if s != 0]
    score = sum(active) / len(active) if active else 0
    return score, signals


# ---------------------------------------------------------------------------
# LAYER 3 -- CONVEXITY VALUATION (weight 0.25)
# ---------------------------------------------------------------------------
def score_symmetry(sym):
    """Symmetry signal from convexity_state."""
    if not sym:
        return 0
    state = (sym.get('convexity_state') or '').upper()
    if state == 'CONVEXITY_CHEAP':
        return 1
    elif state == 'CONVEXITY_FAIR':
        return 0
    elif state == 'CONVEXITY_RICH':
        return -0.5
    elif state == 'CONVEXITY_VERY_RICH':
        return -1
    return 0


def score_vix_modifier(vix):
    """VIX regime modifier applied to confidence."""
    if not vix:
        return 0
    regime = (vix.get('vol_regime_state') or '').upper()
    if regime == 'CALM_EXPANSIONARY':
        return 0.05
    elif regime == 'NEUTRAL':
        return 0
    elif regime == 'ELEVATED_CAUTION':
        return -0.08
    elif regime == 'CRISIS_WATCH':
        return -0.20
    return 0


def compute_layer3(symmetry_signal):
    """Layer 3: Convexity Valuation -- symmetry signal is the score."""
    return symmetry_signal


# ---------------------------------------------------------------------------
# SYNTHESIS
# ---------------------------------------------------------------------------
def synthesize(ticker, gex, dm, reload_data, brief, fib, sym, vix, conflict):
    """3-layer weighted synthesis of all engine signals."""

    spot_price = gex.get('spot_price') if gex else None

    # --- Layer 1: Directional Thesis (0.40) ---
    regime_signal = score_regime(brief)
    demark_signal = score_demark(dm)
    reload_signal = score_reload(reload_data)
    layer1_score, layer1_signals = compute_layer1(
        regime_signal, demark_signal, reload_signal)

    # --- Layer 2: Mechanical Structure (0.35) ---
    gex_signal = score_gex(gex)
    fib_signal = score_fib(fib, spot_price)
    spot_structure_signal = score_spot_structure(gex)
    layer2_score, layer2_signals = compute_layer2(
        gex_signal, fib_signal, spot_structure_signal)

    # --- Layer 3: Convexity Valuation (0.25) ---
    symmetry_signal = score_symmetry(sym)
    vix_modifier = score_vix_modifier(vix)
    layer3_score = compute_layer3(symmetry_signal)

    # --- Weighted composite ---
    composite_score = (
        0.40 * layer1_score +
        0.35 * layer2_score +
        0.25 * layer3_score
    )

    # --- Scenario determination ---
    if composite_score <= -0.25:
        primary_scenario = 'CONTINUATION_DOWN'
        primary_probability = min(0.75, 0.45 + abs(composite_score) * 0.25)
        alt_scenario = 'CHOP_CONSOLIDATION'
        alt_probability = round(1 - primary_probability - 0.10, 2)
        low_prob_scenario = 'SHARP_REVERSAL'
        low_prob_probability = 0.10
        overall_bias = 'BEARISH'
    elif composite_score >= 0.25:
        primary_scenario = 'BULLISH_CONTINUATION'
        primary_probability = min(0.75, 0.45 + composite_score * 0.25)
        alt_scenario = 'CHOP_CONSOLIDATION'
        alt_probability = round(1 - primary_probability - 0.10, 2)
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

    # --- Confidence score: agreement-based ---
    all_signals = layer1_signals + layer2_signals + [layer3_score]
    active_all = [s for s in all_signals if s != 0]
    positives = sum(1 for s in active_all if s > 0)
    negatives = sum(1 for s in active_all if s < 0)
    total_active = len(active_all)
    agreement_ratio = (max(positives, negatives) / total_active
                       if total_active > 0 else 0)

    # Conflict penalty
    conflict_penalty = 0
    if conflict:
        cs = (conflict.get('conflict_state') or '').upper()
        if cs == 'CONTRADICTED':
            conflict_penalty = 0.15
        elif cs == 'MIXED':
            conflict_penalty = 0.07

    confidence_score = max(0.05, min(0.95,
        agreement_ratio * 0.8
        - conflict_penalty
        + vix_modifier
    ))

    # Count agreements for engines_agreement field
    engines_total = total_active
    if active_all and composite_score != 0:
        sign = 1 if composite_score > 0 else -1
        engines_agreement = sum(1 for s in active_all
                                if (s > 0) == (sign > 0))
    else:
        engines_agreement = 0

    # Engine signals dict for transparency
    engine_signals = {
        'gex': gex_signal,
        'demark': demark_signal,
        'reload': reload_signal,
        'regime': regime_signal,
        'ew': 0,
        'fib': fib_signal,
        'spot_structure': spot_structure_signal,
        'symmetry': symmetry_signal,
        'layer1_score': round(layer1_score, 2),
        'layer2_score': round(layer2_score, 2),
        'layer3_score': round(layer3_score, 2),
        'composite_score': round(composite_score, 2),
        'conflict_penalty': conflict_penalty,
        'vix_modifier': vix_modifier,
        'agreement_ratio': round(agreement_ratio, 2),
    }

    return {
        'primary_scenario': primary_scenario,
        'primary_probability': round(primary_probability, 2),
        'alt_scenario': alt_scenario,
        'alt_probability': round(alt_probability, 2),
        'low_prob_scenario': low_prob_scenario,
        'low_prob_probability': round(low_prob_probability, 2),
        'overall_bias': overall_bias,
        'confidence_score': round(confidence_score, 2),
        'engines_agreement': engines_agreement,
        'engines_total': engines_total,
        'engine_signals': engine_signals,
    }


# ---------------------------------------------------------------------------
# WRITE TO SUPABASE
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
# MAIN + SUMMARY
# ---------------------------------------------------------------------------
def fmt_signal(s):
    """Format a signal value for display."""
    if s is None:
        return '--'
    if isinstance(s, float):
        return f'{s:+.1f}'
    return f'{s:+d}'


def main():
    print("=" * 60)
    print("SCENARIO SYNTHESIS ENGINE -- 3-Layer Weighted Architecture")
    print("=" * 60)
    print(f"Run: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"Tickers: {', '.join(TICKERS)}")
    print(f"Weights: L1=0.40 (Directional) | L2=0.35 (Mechanical) | L3=0.25 (Convexity)")
    print("-" * 60)

    for ticker in TICKERS:
        print(f"\n{'='*50}")
        print(f"  {ticker}")
        print(f"{'='*50}")

        gex, dm, reload_data, brief, fib, sym, vix, conflict = \
            fetch_engine_data(ticker)

        sources = []
        if gex:     sources.append('GEX')
        if dm:      sources.append('DeMark')
        if reload_data: sources.append('Reload')
        if brief:   sources.append('Regime')
        if fib:     sources.append('Fib')
        if sym:     sources.append('Symmetry')
        if vix:     sources.append('VIX')
        if conflict: sources.append('Conflict')
        print(f"  Sources: {', '.join(sources) if sources else 'NONE'}")

        result = synthesize(ticker, gex, dm, reload_data, brief,
                            fib, sym, vix, conflict)

        es = result['engine_signals']

        # Header line
        bias = result['overall_bias']
        comp = es['composite_score']
        conf = int(result['confidence_score'] * 100)
        print(f"  {ticker} | {bias} | composite: {comp:+.2f} | confidence: {conf}%")

        # Layer 1
        l1 = es['layer1_score']
        print(f"  L1 (directional 40%): {l1:+.2f}")
        print(f"    DeMark=[{fmt_signal(es['demark'])}]  "
              f"Reload=[{fmt_signal(es['reload'])}]  "
              f"Regime=[{fmt_signal(es['regime'])}]")

        # Layer 2
        l2 = es['layer2_score']
        print(f"  L2 (mechanical 35%): {l2:+.2f}")
        print(f"    GEX=[{fmt_signal(es['gex'])}]  "
              f"Fib=[{fmt_signal(es['fib'])}]  "
              f"Structure=[{fmt_signal(es['spot_structure'])}]")

        # Layer 3
        l3 = es['layer3_score']
        print(f"  L3 (convexity 25%): {l3:+.2f}")
        print(f"    Symmetry=[{fmt_signal(es['symmetry'])}]  "
              f"VIX_mod=[{es['vix_modifier']:+.2f}]")

        # Conflict
        cp = es['conflict_penalty']
        if cp > 0:
            print(f"  Conflict penalty: -{cp:.2f}")
        else:
            print(f"  Conflict penalty: 0")

        # Scenario
        print(f"  Primary: {result['primary_scenario']} "
              f"({int(result['primary_probability'] * 100)}%)")
        print(f"  Alt:     {result['alt_scenario']} "
              f"({int(result['alt_probability'] * 100)}%)")

        # Write
        delete_today_rows(ticker)
        ok = write_synthesis(ticker, result)
        status = 'OK' if ok else 'FAIL'
        print(f"  Write: [{status}]")

    print("\n" + "=" * 60)
    print("SCENARIO SYNTHESIS -- RUN COMPLETE")
    print("=" * 60)


if __name__ == '__main__':
    main()
