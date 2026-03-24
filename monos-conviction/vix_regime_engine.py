"""
VIX Regime Engine -- MONOS Conviction Pipeline
Fetches VIX family data from iVolatility API, classifies vol regime,
adjusts scenario synthesis confidence scores.
"""

import os
import sys
import io
import json
import requests
from datetime import date, datetime, timedelta, timezone

# Force UTF-8 stdout to prevent encoding errors on Windows
sys.stdout = io.TextIOWrapper(
    sys.stdout.buffer,
    encoding='utf-8',
    errors='replace'
)

from dotenv import load_dotenv
from supabase import create_client

# -- Load .env ----------------------------------------------
env_path = os.path.join(os.path.dirname(__file__), '.env')
if not os.path.exists(env_path):
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(env_path)

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
IVOLATILITY_API_KEY = os.environ.get('IVOLATILITY_API_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    print('[VIX] FATAL: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set')
    sys.exit(1)
if not IVOLATILITY_API_KEY:
    print('[VIX] FATAL: IVOLATILITY_API_KEY not set in .env')
    sys.exit(1)

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# -- STEP 1: Ensure output table exists (print SQL for operator) --
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS public.vix_regime (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_ts                TIMESTAMPTZ NOT NULL DEFAULT now(),
  vix                   NUMERIC,
  vix9d                 NUMERIC,
  vix3m                 NUMERIC,
  vvix                  NUMERIC,
  vix_regime            TEXT NOT NULL,
  term_structure_state  TEXT,
  near_term_inversion   BOOLEAN DEFAULT false,
  vol_regime_state      TEXT NOT NULL,
  confidence_modifier   NUMERIC,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# -- STEP 2: Fetch VIX data from iVolatility --------------
def fetch_ivolatility(symbol):
    try:
        url = 'https://restapi.ivolatility.com/equities/eod/implied-volatility'
        params = {
            'apiKey': IVOLATILITY_API_KEY,
            'ticker': symbol,
            'startDate': (date.today() - timedelta(days=5)).isoformat(),
            'endDate': date.today().isoformat()
        }
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        if data and isinstance(data, list) and len(data) > 0:
            return float(data[-1].get('impliedVolatility', 0)) * 100
        return None
    except Exception as e:
        print(f'  [iVol] {symbol} fetch failed: {e}')
        return None


# -- STEP 3: Classify VIX regime ---------------------------
def classify_vix_regime(vix):
    if vix is None:
        return 'UNKNOWN'
    if vix < 15:
        return 'LOW_VOL'
    elif vix < 25:
        return 'MID_VOL'
    elif vix < 35:
        return 'HIGH_VOL'
    else:
        return 'EXTREME_VOL'


def classify_term_structure(vix, vix9d):
    if vix9d is None or vix is None:
        return 'UNKNOWN'
    if vix9d > vix + 1:
        return 'BACKWARDATION'
    elif vix9d < vix - 1:
        return 'CONTANGO'
    else:
        return 'FLAT'


def check_near_term_inversion(vix, vix9d):
    if vix9d is None or vix is None:
        return False
    return vix9d > vix


# -- STEP 4: Composite vol regime state --------------------
def compute_vol_regime(vix, vvix, near_term_inversion, term_structure_state):
    crisis_conditions = (
        (vix is not None and vix > 35) or
        (vvix is not None and vvix > 115) or
        (near_term_inversion and term_structure_state == 'BACKWARDATION')
    )
    caution_conditions = (
        (vix is not None and vix > 25) or
        (vvix is not None and vvix > 100) or
        term_structure_state == 'BACKWARDATION'
    )

    if crisis_conditions:
        return 'CRISIS_WATCH', -0.20
    elif caution_conditions:
        return 'ELEVATED_CAUTION', -0.08
    elif vix is not None and vix < 20:
        return 'CALM_EXPANSIONARY', 0.05
    else:
        return 'NEUTRAL', 0.0


# -- STEP 5: Update scenario synthesis confidence ----------
def update_scenario_scores(confidence_modifier):
    if confidence_modifier == 0.0:
        print('[VIX] Confidence modifier is 0 -- no scenario adjustments needed')
        return 0

    today_str = date.today().isoformat()
    try:
        result = sb.table('scenario_synthesis') \
            .select('id, confidence_score') \
            .gte('run_ts', today_str) \
            .execute()
        rows = result.data or []
    except Exception as e:
        print(f'[VIX] Failed to fetch scenario_synthesis rows: {e}')
        return 0

    updated = 0
    for row in rows:
        old_score = row.get('confidence_score')
        if old_score is None:
            continue
        new_score = min(0.95, max(0.05, float(old_score) + confidence_modifier))
        try:
            sb.table('scenario_synthesis') \
                .update({'confidence_score': new_score}) \
                .eq('id', row['id']) \
                .execute()
            updated += 1
        except Exception as e:
            print(f'[VIX] Failed to update row {row["id"]}: {e}')

    return updated


# -- STEP 6: Write to Supabase -----------------------------
def write_vix_regime(row_data):
    today_str = date.today().isoformat()

    # Delete today's rows first
    try:
        sb.table('vix_regime') \
            .delete() \
            .gte('run_ts', today_str) \
            .execute()
    except Exception as e:
        print(f'[VIX] Warning: Could not delete old rows: {e}')

    # Insert new row
    try:
        sb.table('vix_regime').insert(row_data).execute()
        print('[VIX] Row inserted into vix_regime')
    except Exception as e:
        print(f'[VIX] FATAL: Insert failed: {e}')
        # If table doesn't exist, print the CREATE SQL
        if '42P01' in str(e) or 'does not exist' in str(e):
            print('[VIX] Table may not exist. Run this SQL in Supabase:')
            print(CREATE_SQL)
        sys.exit(1)


# -- MAIN --------------------------------------------------
def main():
    print('='*50)
    print('VIX REGIME ENGINE -- RUN STARTING')
    print('='*50)
    print()

    # Step 2: Fetch from iVolatility
    print('[VIX] Fetching VIX family from iVolatility...')
    # VIX proxies from iVolatility
    # Use SPY ATM IV as VIX proxy if direct VIX not available
    vix = fetch_ivolatility('VIX') or fetch_ivolatility('SPY')
    vix9d = fetch_ivolatility('VIX9D')
    vix3m = fetch_ivolatility('VIX3M')
    vvix = fetch_ivolatility('VVIX')
    print(f'  vix   = {vix}')
    print(f'  vix9d = {vix9d}')
    print(f'  vix3m = {vix3m}')
    print(f'  vvix  = {vvix}')
    print()

    # Step 3: Classify
    vix_regime = classify_vix_regime(vix)
    term_structure_state = classify_term_structure(vix, vix9d)
    near_term_inversion = check_near_term_inversion(vix, vix9d)

    # Step 4: Composite
    vol_regime_state, confidence_modifier = compute_vol_regime(
        vix, vvix, near_term_inversion, term_structure_state
    )

    # Step 6: Write (before step 5 so the row exists)
    row_data = {
        'run_ts': datetime.now(timezone.utc).isoformat(),
        'vix': float(vix) if vix is not None else None,
        'vix9d': float(vix9d) if vix9d is not None else None,
        'vix3m': float(vix3m) if vix3m is not None else None,
        'vvix': float(vvix) if vvix is not None else None,
        'vix_regime': vix_regime,
        'term_structure_state': term_structure_state,
        'near_term_inversion': near_term_inversion,
        'vol_regime_state': vol_regime_state,
        'confidence_modifier': float(confidence_modifier),
    }
    write_vix_regime(row_data)

    # Step 5: Update scenario synthesis
    adjusted_count = update_scenario_scores(confidence_modifier)

    # Step 7: Print summary
    print()
    print('VIX REGIME ENGINE -- RUN COMPLETE')
    print('='*50)
    print(f'  VIX:    {vix if vix is not None else "--":>6}  -> {vix_regime}')
    print(f'  VIX9D:  {vix9d if vix9d is not None else "--":>6}')
    print(f'  VIX3M:  {vix3m if vix3m is not None else "--":>6}')
    print(f'  VVIX:   {vvix if vvix is not None else "--":>6}')
    print(f'  Term structure: {term_structure_state}')
    print(f'  Near-term inversion: {"YES" if near_term_inversion else "NO"}')
    print(f'  VOL REGIME: {vol_regime_state}')
    print(f'  Confidence modifier: {confidence_modifier:+.2f}')
    print(f'  Scenario scores adjusted: {adjusted_count} rows')
    print('='*50)


if __name__ == '__main__':
    main()
