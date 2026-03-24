"""
MONOS Position Loader -- March 22, 2026 CSV export
===================================================
Loads portfolio positions into Supabase.
  - Adds 40C Jan27 x15 leg to existing SLV CALL_LADDER
  - Inserts 7 new positions (GLD, GDX, SIL, SILJ, COPX, 2x SPY)
  - Promotes tickers to POSITION tier in ticker_universe
"""

import os
import sys
import json
from datetime import date, datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

# ---------------------------------------------------------------------------
# ENV
# ---------------------------------------------------------------------------
script_dir = Path(__file__).resolve().parent
for env_path in [script_dir / ".env", script_dir.parent / ".env"]:
    if env_path.exists():
        load_dotenv(env_path)
        print(f"[loader] Loaded env from {env_path}")
        break
else:
    load_dotenv()
    print("[loader] Using default dotenv search")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")
    sys.exit(1)

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
TODAY = date.today().isoformat()

# Counters
positions_inserted = 0
legs_inserted = 0
skipped = 0


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def insert_position(ticker, structure_type, strategic_role,
                    layer, pod, thesis, legs):
    """
    Insert a position + legs into Supabase.
    Skips if ticker + structure_type + layer already exists.
    """
    global positions_inserted, legs_inserted, skipped

    # Check for duplicate -- match on notes JSON content since
    # strategic_role/layer/pod are stored in notes
    existing = sb.table('positions') \
        .select('id, ticker, structure_type, notes') \
        .eq('ticker', ticker) \
        .eq('structure_type', structure_type) \
        .eq('is_active', True) \
        .execute().data or []

    # Filter by layer in notes JSON
    for row in existing:
        notes = row.get('notes')
        if notes:
            try:
                n = json.loads(notes) if isinstance(notes, str) else notes
                if n.get('layer') == layer:
                    print(f"  [SKIP] {ticker} {structure_type} {layer} already exists")
                    skipped += 1
                    return
            except (json.JSONDecodeError, TypeError):
                pass

    # Build notes JSON
    notes_json = json.dumps({
        'strategic_role': strategic_role,
        'layer': layer,
        'pod': pod,
        'monos_state_at_open': 'IN_TRADE'
    })

    # Insert position
    result = sb.table('positions').insert({
        'ticker': ticker,
        'structure_type': structure_type,
        'entry_date': TODAY,
        'thesis': thesis,
        'state': 'IN_TRADE',
        'is_active': True,
        'notes': notes_json
    }).execute()

    pos = result.data[0]
    pos_id = pos['id']
    positions_inserted += 1

    # Insert legs
    leg_rows = []
    for leg in legs:
        # Quantity: negative for shorts
        qty = leg['qty']
        if leg['type'].startswith('SHORT'):
            qty = -abs(qty)
        else:
            qty = abs(qty)

        leg_rows.append({
            'position_id': pos_id,
            'leg_type': leg['type'],
            'strike': leg['strike'],
            'expiration': leg['expiry'],
            'quantity': qty,
            'entry_price': leg['premium'],
            'is_hedge': leg.get('is_hedge', False),
            'delta': leg.get('delta'),
            'gamma': leg.get('gamma'),
            'theta': leg.get('theta'),
            'vega': leg.get('vega'),
        })

    if leg_rows:
        sb.table('position_legs').insert(leg_rows).execute()
        legs_inserted += len(leg_rows)

    print(f"  [OK] {ticker} {structure_type} {strategic_role} -- {len(legs)} legs")


# ---------------------------------------------------------------------------
# STEP 1 -- SLV ADDITION
# ---------------------------------------------------------------------------
def step1_slv_addition():
    global legs_inserted
    print()
    print("=" * 55)
    print("  STEP 1 -- SLV LEAPS ADDITION")
    print("=" * 55)

    # Find existing SLV CALL_LADDER LONG_TERM position
    existing = sb.table('positions') \
        .select('id, ticker, structure_type, notes') \
        .eq('ticker', 'SLV') \
        .eq('structure_type', 'CALL_LADDER') \
        .eq('is_active', True) \
        .execute().data or []

    # Filter for LONG_TERM layer in notes
    target = None
    for row in existing:
        notes = row.get('notes')
        if notes:
            try:
                n = json.loads(notes) if isinstance(notes, str) else notes
                if n.get('layer') == 'LONG_TERM':
                    target = row
                    break
            except (json.JSONDecodeError, TypeError):
                pass

    if not target:
        # Fallback: use any SLV CALL_LADDER
        if existing:
            target = existing[0]
            print(f"  [!] No LONG_TERM layer found, using first SLV CALL_LADDER")
        else:
            print("  [!] No SLV CALL_LADDER position found -- skipping leg addition")
            return

    pos_id = target['id']
    print(f"  Found SLV CALL_LADDER position: {pos_id}")

    # Check if this leg already exists
    existing_legs = sb.table('position_legs') \
        .select('id') \
        .eq('position_id', pos_id) \
        .eq('strike', 40) \
        .eq('expiration', '2027-01-15') \
        .execute().data or []

    if existing_legs:
        print("  [SKIP] 40C Jan27 leg already exists on this position")
        return

    # Insert new leg
    sb.table('position_legs').insert({
        'position_id': pos_id,
        'leg_type': 'LONG_CALL',
        'strike': 40.0,
        'expiration': '2027-01-15',
        'quantity': 15,
        'entry_price': 7.22,
        'is_hedge': False,
        'delta': 0.8827,
        'gamma': 0.0065,
        'theta': -0.0061,
        'vega': 0.1058
    }).execute()

    legs_inserted += 1
    print("  [SLV] Added 40C Jan27 x15 to existing LEAPS position")


# ---------------------------------------------------------------------------
# STEP 2 -- INSERT NEW POSITIONS
# ---------------------------------------------------------------------------
def step2_new_positions():
    print()
    print("=" * 55)
    print("  STEP 2 -- NEW POSITIONS")
    print("=" * 55)

    # GLD -- Income overlay on PHYS
    insert_position(
        ticker='GLD',
        structure_type='CUSTOM',
        strategic_role='INCOME',
        layer='SHORT_TERM',
        pod='GOLD_CORE',
        thesis='Short call overlay against PHYS gold position. Income generation against core gold holding.',
        legs=[
            {'type': 'SHORT_CALL', 'strike': 445, 'expiry': '2026-04-17',
             'qty': 3, 'premium': 14.41, 'is_hedge': False,
             'delta': 0.2187, 'gamma': 0.0082, 'theta': -0.1879, 'vega': 0.3394}
        ]
    )

    # GDX -- Income call
    insert_position(
        ticker='GDX',
        structure_type='CUSTOM',
        strategic_role='INCOME',
        layer='LONG_TERM',
        pod='GOLD_MINERS',
        thesis='Short call overlay on GDX miners ETF. Income generation against miners thesis.',
        legs=[
            {'type': 'SHORT_CALL', 'strike': 95, 'expiry': '2026-09-18',
             'qty': 10, 'premium': 13.60, 'is_hedge': False,
             'delta': 0.3956, 'gamma': 0.0143, 'theta': -0.0277, 'vega': 0.2189}
        ]
    )

    # SIL -- Call ladder
    insert_position(
        ticker='SIL',
        structure_type='CALL_LADDER',
        strategic_role='CORE',
        layer='LONG_TERM',
        pod='SILVER_MINERS',
        thesis='Long call ladder on SIL silver miners ETF. Multi-expiry convexity across Apr/Jul/Oct 2026 and Jan 2027 at 105-110 strikes.',
        legs=[
            {'type': 'LONG_CALL', 'strike': 105, 'expiry': '2026-04-17',
             'qty': 5, 'premium': 11.47,
             'delta': 0.0970, 'gamma': 0.0111, 'theta': -0.0463, 'vega': 0.0379},
            {'type': 'LONG_CALL', 'strike': 110, 'expiry': '2026-07-17',
             'qty': 5, 'premium': 14.48,
             'delta': 0.2627, 'gamma': 0.0111, 'theta': -0.0397, 'vega': 0.1484},
            {'type': 'LONG_CALL', 'strike': 110, 'expiry': '2026-10-16',
             'qty': 10, 'premium': 19.01,
             'delta': 0.3420, 'gamma': 0.0100, 'theta': -0.0312, 'vega': 0.2220},
            {'type': 'LONG_CALL', 'strike': 110, 'expiry': '2027-01-15',
             'qty': 10, 'premium': 20.98,
             'delta': 0.3904, 'gamma': 0.0094, 'theta': -0.0255, 'vega': 0.2744}
        ]
    )

    # SILJ -- Call ladder with financed spread
    insert_position(
        ticker='SILJ',
        structure_type='CALL_LADDER',
        strategic_role='CORE',
        layer='LONG_TERM',
        pod='SILVER_MINERS',
        thesis='SILJ call ladder May/Aug 2026 and Jan 2027 at 35-40 strikes. Short 54C Jan27 x75 to finance the long calls. Net spread structure.',
        legs=[
            {'type': 'LONG_CALL', 'strike': 35, 'expiry': '2026-05-15',
             'qty': 20, 'premium': 2.66,
             'delta': 0.2138, 'gamma': 0.0377, 'theta': -0.0198, 'vega': 0.0300},
            {'type': 'LONG_CALL', 'strike': 40, 'expiry': '2026-08-21',
             'qty': 50, 'premium': 6.78,
             'delta': 0.2636, 'gamma': 0.0269, 'theta': -0.0126, 'vega': 0.0559},
            {'type': 'LONG_CALL', 'strike': 40, 'expiry': '2027-01-15',
             'qty': 75, 'premium': 11.25,
             'delta': 0.3757, 'gamma': 0.0247, 'theta': -0.0096, 'vega': 0.0895},
            {'type': 'SHORT_CALL', 'strike': 54, 'expiry': '2027-01-15',
             'qty': 75, 'premium': 7.41,
             'delta': 0.2331, 'gamma': 0.0185, 'theta': -0.0082, 'vega': 0.0715}
        ]
    )

    # COPX -- Short call income ladder
    insert_position(
        ticker='COPX',
        structure_type='CALL_LADDER',
        strategic_role='INCOME',
        layer='LONG_TERM',
        pod='COPPER_PROXY',
        thesis='Short call income ladder on COPX copper ETF. Jul 2026 and Jan 2027 expiries at 85-120 strikes. Premium income against copper thesis.',
        legs=[
            {'type': 'SHORT_CALL', 'strike': 85, 'expiry': '2026-07-17',
             'qty': 37, 'premium': 10.05,
             'delta': 0.3169, 'gamma': 0.0163, 'theta': -0.0319, 'vega': 0.1403},
            {'type': 'SHORT_CALL', 'strike': 120, 'expiry': '2026-07-17',
             'qty': 50, 'premium': 5.25,
             'delta': 0.0861, 'gamma': 0.0064, 'theta': -0.0156, 'vega': 0.0622},
            {'type': 'SHORT_CALL', 'strike': 80, 'expiry': '2027-01-15',
             'qty': 4, 'premium': 13.86,
             'delta': 0.4885, 'gamma': 0.0122, 'theta': -0.0202, 'vega': 0.2484},
            {'type': 'SHORT_CALL', 'strike': 95, 'expiry': '2027-01-15',
             'qty': 40, 'premium': 13.30,
             'delta': 0.3508, 'gamma': 0.0112, 'theta': -0.0193, 'vega': 0.2317}
        ]
    )

    # SPY -- Put spread macro hedge
    insert_position(
        ticker='SPY',
        structure_type='PUT_LADDER',
        strategic_role='HEDGE',
        layer='SHORT_TERM',
        pod='SPY_BALLAST',
        thesis='SPY put spread Apr30 2026. Short 650P x100 funded by long 670P x50 + 690P x50. Macro downside protection and portfolio ballast.',
        legs=[
            {'type': 'SHORT_PUT', 'strike': 650, 'expiry': '2026-04-30',
             'qty': 100, 'premium': 11.06, 'is_hedge': True,
             'delta': -0.4311, 'gamma': 0.0080, 'theta': -0.2441, 'vega': 0.8596},
            {'type': 'LONG_PUT', 'strike': 670, 'expiry': '2026-04-30',
             'qty': 50, 'premium': 15.41, 'is_hedge': True,
             'delta': -0.6121, 'gamma': 0.0093, 'theta': -0.2126, 'vega': 0.8353},
            {'type': 'LONG_PUT', 'strike': 690, 'expiry': '2026-04-30',
             'qty': 50, 'premium': 17.51, 'is_hedge': True,
             'delta': -0.8293, 'gamma': 0.0087, 'theta': -0.1394, 'vega': 0.5543}
        ]
    )

    # SPY -- Call ladder upside convexity
    insert_position(
        ticker='SPY',
        structure_type='CALL_LADDER',
        strategic_role='CORE',
        layer='LONG_TERM',
        pod='SPY_BALLAST',
        thesis='SPY call ladder Jun-Dec 2026. Long upside convexity at 700-800 strikes across Jun/Sep/Dec expiries. Participates in market recovery.',
        legs=[
            {'type': 'LONG_CALL', 'strike': 720, 'expiry': '2026-06-18',
             'qty': 20, 'premium': 17.98,
             'delta': 0.1344, 'gamma': 0.0045, 'theta': -0.0576, 'vega': 0.7156},
            {'type': 'LONG_CALL', 'strike': 700, 'expiry': '2026-06-30',
             'qty': 12, 'premium': 24.73,
             'delta': 0.2702, 'gamma': 0.0058, 'theta': -0.0920, 'vega': 1.1534},
            {'type': 'LONG_CALL', 'strike': 710, 'expiry': '2026-06-30',
             'qty': 10, 'premium': 24.00,
             'delta': 0.2055, 'gamma': 0.0053, 'theta': -0.0749, 'vega': 0.9989},
            {'type': 'LONG_CALL', 'strike': 800, 'expiry': '2026-09-18',
             'qty': 62, 'premium': 2.42,
             'delta': 0.0354, 'gamma': 0.0012, 'theta': -0.0133, 'vega': 0.3882},
            {'type': 'LONG_CALL', 'strike': 760, 'expiry': '2026-09-30',
             'qty': 15, 'premium': 11.37,
             'delta': 0.1176, 'gamma': 0.0029, 'theta': -0.0344, 'vega': 0.9755},
            {'type': 'LONG_CALL', 'strike': 780, 'expiry': '2026-09-30',
             'qty': 15, 'premium': 6.85,
             'delta': 0.0696, 'gamma': 0.0020, 'theta': -0.0225, 'vega': 0.6767},
            {'type': 'LONG_CALL', 'strike': 770, 'expiry': '2026-12-31',
             'qty': 6, 'premium': 14.47,
             'delta': None, 'gamma': None, 'theta': None, 'vega': None}
        ]
    )


# ---------------------------------------------------------------------------
# STEP 3 -- PROMOTE TICKERS
# ---------------------------------------------------------------------------
def step3_promote_tickers():
    print()
    print("=" * 55)
    print("  STEP 3 -- PROMOTE TO POSITION TIER")
    print("=" * 55)

    promote = ['GLD', 'GDX', 'SIL', 'SILJ', 'COPX', 'SPY']
    for ticker in promote:
        try:
            sb.table('ticker_universe') \
                .update({'tier': 'POSITION'}) \
                .eq('ticker', ticker) \
                .execute()
            print(f"  [OK] {ticker} -> POSITION tier")
        except Exception as e:
            print(f"  [!] {ticker} tier update failed: {e}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print()
    print("=" * 55)
    print("  MONOS POSITION LOADER -- March 22, 2026")
    print("=" * 55)
    print(f"  Supabase: {SUPABASE_URL[:40]}...")
    print(f"  Date: {TODAY}")

    step1_slv_addition()
    step2_new_positions()
    step3_promote_tickers()

    # Summary
    print()
    print("=" * 55)
    print("  POSITION LOADER -- COMPLETE")
    print("=" * 55)
    print(f"  New positions inserted:  {positions_inserted}")
    print(f"  Total legs inserted:     {legs_inserted}")
    print(f"  Skipped (duplicates):    {skipped}")
    print()
    print("  Breakdown:")
    print("    SLV  40C Jan27 x15 added to existing ladder")
    print("    GLD  CUSTOM INCOME        1 leg")
    print("    GDX  CUSTOM INCOME        1 leg")
    print("    SIL  CALL_LADDER CORE     4 legs")
    print("    SILJ CALL_LADDER CORE     4 legs")
    print("    COPX CALL_LADDER INCOME   4 legs")
    print("    SPY  PUT_LADDER HEDGE     3 legs")
    print("    SPY  CALL_LADDER CORE     7 legs")
    print()
    print("  Tickers promoted to POSITION tier:")
    print("    GLD, GDX, SIL, SILJ, COPX, SPY")
    print("=" * 55)


if __name__ == '__main__':
    main()
