"""
MONOS Ticker Universe Setup
Seeds ticker_universe table with tiered ticker list.
Syncs POSITION tier from active ledger positions.
Safe to re-run -- uses upsert on ticker conflict.
"""

from supabase import create_client
from dotenv import load_dotenv
from datetime import date
import os
import sys

load_dotenv()

SB_URL = os.environ.get('SUPABASE_URL', '')
SB_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')

if not SB_URL or not SB_KEY:
    print("[FATAL] SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set")
    sys.exit(1)

sb = create_client(SB_URL, SB_KEY)

# ---------------------------------------------------------------------------
# SEED DATA
# ---------------------------------------------------------------------------
SEED = [
    ('SLV',  'POSITION',  'METALS'),
    ('GLD',  'WATCHLIST', 'METALS'),
    ('GDX',  'WATCHLIST', 'METALS'),
    ('SILJ', 'WATCHLIST', 'METALS'),
    ('SIL',  'WATCHLIST', 'METALS'),
    ('COPX', 'RESEARCH',  'METALS'),
    ('SPY',  'RESEARCH',  'INDEX'),
    ('QQQ',  'RESEARCH',  'INDEX'),
    ('IWM',  'RESEARCH',  'INDEX'),
    ('XLE',  'RESEARCH',  'ENERGY'),
    ('USO',  'RESEARCH',  'ENERGY'),
    ('BITO', 'RESEARCH',  'CRYPTO'),
    ('TLT',  'RESEARCH',  'PORTFOLIO'),
]

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=" * 50)
    print("TICKER UNIVERSE SETUP")
    print("=" * 50)
    print("Date:", date.today().isoformat())
    print()

    # STEP 2 -- Seed initial universe via upsert
    print("[1] Seeding initial universe ({} tickers)...".format(len(SEED)))
    seeded = 0
    for ticker, tier, cluster in SEED:
        row = {
            'ticker': ticker,
            'tier': tier,
            'cluster': cluster,
            'added_by': 'T0',
            'is_active': True,
        }
        try:
            res = sb.table('ticker_universe').upsert(
                row, on_conflict='ticker'
            ).execute()
            data = res.data if res.data else []
            if data:
                existing_tier = data[0].get('tier', '')
                if existing_tier == tier:
                    print("  . {} ({} / {})".format(ticker, tier, cluster))
                else:
                    print("  ~ {} tier updated: {} -> {}".format(ticker, existing_tier, tier))
                seeded += 1
            else:
                print("  + {} ({} / {})".format(ticker, tier, cluster))
                seeded += 1
        except Exception as e:
            print("  [!] {} error: {}".format(ticker, str(e)[:200]))
    print("  Processed: {}".format(seeded))
    print()

    # STEP 3 -- Sync position tickers from active positions
    print("[2] Syncing POSITION tier from active positions...")
    try:
        pos_res = sb.table('positions').select('ticker').eq('is_active', True).execute()
        pos_tickers = list(set(r['ticker'] for r in (pos_res.data or []) if r.get('ticker')))
    except Exception as e:
        print("  [!] Positions query error: {}".format(e))
        pos_tickers = []

    print("  Active position tickers: {}".format(pos_tickers if pos_tickers else 'none found'))

    for t in pos_tickers:
        try:
            sb.table('ticker_universe').upsert(
                {
                    'ticker': t,
                    'tier': 'POSITION',
                    'added_by': 'SYSTEM',
                    'is_active': True,
                },
                on_conflict='ticker'
            ).execute()
            print("  ~ {} -> POSITION (from ledger)".format(t))
        except Exception as e:
            print("  [!] {} upsert error: {}".format(t, str(e)[:200]))
    print()

    # STEP 4 -- Print summary
    print("[3] Reading final universe...")
    try:
        all_res = sb.table('ticker_universe').select('*').eq('is_active', True).order('tier').order('ticker').execute()
        all_rows = all_res.data or []
    except Exception as e:
        print("  [!] Read error: {}".format(e))
        all_rows = []

    by_tier = {}
    for r in all_rows:
        tier = r.get('tier', 'UNKNOWN')
        if tier not in by_tier:
            by_tier[tier] = []
        by_tier[tier].append(r['ticker'])

    print()
    print("=" * 50)
    print("TICKER UNIVERSE SETUP COMPLETE")
    print("=" * 50)
    for tier in ['POSITION', 'WATCHLIST', 'RESEARCH']:
        tickers = by_tier.get(tier, [])
        print("{:<12} {}".format(tier + ':', ', '.join(tickers) if tickers else '(none)'))
    # Any other tiers
    for tier, tickers in by_tier.items():
        if tier not in ['POSITION', 'WATCHLIST', 'RESEARCH']:
            print("{:<12} {}".format(tier + ':', ', '.join(tickers)))
    print("Total: {} tickers".format(len(all_rows)))
    print()


if __name__ == '__main__':
    main()
