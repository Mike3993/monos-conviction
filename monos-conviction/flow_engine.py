"""
FLOW ENGINE -- MONOS Conviction Pipeline
Fetches options flow data from Unusual Whales API, filters for
signal quality, classifies prints, and writes summaries + individual
prints to Supabase for the dashboard OptionsFlowTab.

Usage:
    python flow_engine.py          # full run
    python flow_engine.py --dry    # evaluate only, skip writes
"""

import os
import sys
import json
import requests
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

# -- Load .env -------------------------------------------------------
script_dir = Path(__file__).resolve().parent
for env_path in [script_dir / ".env", script_dir.parent / ".env"]:
    if env_path.exists():
        load_dotenv(env_path)
        print(f"[flow] Loaded env from {env_path}")
        break
else:
    load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
UNUSUAL_WHALES_API_KEY = os.environ.get("UNUSUAL_WHALES_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("[flow] FATAL: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set")
    sys.exit(1)
if not UNUSUAL_WHALES_API_KEY:
    print("[flow] WARNING: UNUSUAL_WHALES_API_KEY not set -- will use mock fallback")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
TODAY = date.today()
TODAY_ISO = TODAY.isoformat()
DRY_RUN = "--dry" in sys.argv

# -- Table DDL --------------------------------------------------------
CREATE_SQL_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS public.flow_snapshots (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
  ticker          TEXT NOT NULL,
  call_volume     INTEGER,
  put_volume      INTEGER,
  call_put_ratio  NUMERIC,
  net_premium     NUMERIC,
  net_notional    NUMERIC,
  flow_signal     TEXT,
  conviction_score NUMERIC,
  largest_trade   JSONB,
  top_prints      JSONB,
  snapshot_ts     TIMESTAMPTZ DEFAULT now(),
  created_at      TIMESTAMPTZ DEFAULT now()
);
"""

CREATE_SQL_PRINTS = """
CREATE TABLE IF NOT EXISTS public.flow_prints (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
  ticker          TEXT NOT NULL,
  leg_type        TEXT,
  strike          NUMERIC,
  expiry          DATE,
  expiry_window   TEXT,
  volume          INTEGER,
  open_interest   INTEGER,
  vol_oi_ratio    NUMERIC,
  notional        NUMERIC,
  premium         NUMERIC,
  print_type      TEXT,
  side            TEXT,
  is_unusual      BOOLEAN DEFAULT false,
  created_at      TIMESTAMPTZ DEFAULT now()
);
"""

# =====================================================================
# STEP 2 -- CONSTANTS
# =====================================================================

# Signal quality filters (Dark Flow methodology)
MIN_VOL_OI_RATIO = 1.5
MIN_NOTIONAL = 50_000

# Print classification thresholds
PRINT_TYPES = {
    'MEGA_BLOCK': 5_000_000,
    'BLOCK':      2_000_000,
    'SWEEP':        500_000,
    'LARGE':        100_000,
}

_DEFAULT_TICKERS = ['SLV', 'GLD', 'GDX', 'SILJ', 'SIL']

def _load_universe():
    """Load tickers from ticker_universe table, fall back to defaults."""
    try:
        rows = sb.table("ticker_universe").select("ticker").order("ticker").execute()
        tickers = [r["ticker"] for r in (rows.data or []) if r.get("ticker")]
        if tickers:
            print(f"[flow] Loaded {len(tickers)} tickers from ticker_universe")
            return tickers
    except Exception as e:
        print(f"[flow] ticker_universe fetch failed: {e}")
    return _DEFAULT_TICKERS

TICKERS = _load_universe()


def classify_expiry_window(days_to_expiry):
    if days_to_expiry <= 7:
        return 'WEEKLY'
    if days_to_expiry <= 30:
        return 'MONTHLY'
    if days_to_expiry <= 90:
        return 'QUARTERLY'
    return 'LONG_DATED'


def classify_print_type(notional):
    for label, threshold in PRINT_TYPES.items():
        if notional >= threshold:
            return label
    return 'STANDARD'


# =====================================================================
# STEP 3 -- FETCH FROM UNUSUAL WHALES
# =====================================================================

UW_HEADERS = {
    'Accept': 'application/json',
}

ENDPOINTS = [
    ('flow-alerts',       '/api/stock/{ticker}/flow-alerts'),
    ('options-activity',  '/api/stock/{ticker}/options-activity'),
    ('option-contracts',  '/api/stock/{ticker}/option-contracts'),
]


def fetch_unusual_whales(ticker, is_first=False):
    """Try Unusual Whales endpoints in order. Return (raw_data, endpoint_name) or (None, None)."""
    if not UNUSUAL_WHALES_API_KEY:
        return None, None

    headers = {
        **UW_HEADERS,
        'Authorization': f'Bearer {UNUSUAL_WHALES_API_KEY}',
    }

    for ep_name, ep_path in ENDPOINTS:
        url = f'https://api.unusualwhales.com{ep_path.format(ticker=ticker)}'
        try:
            resp = requests.get(url, headers=headers, params={'limit': 100}, timeout=15)

            if is_first:
                print(f"  [API] {ep_name} -> status {resp.status_code}")
                # Print raw response for field name verification
                raw_text = resp.text[:500]
                print(f"  [API] Raw response (first 500 chars):")
                print(f"  {raw_text}")
                print()

            if resp.status_code in (401, 403):
                print(f"  [API] {ep_name} -> {resp.status_code} (auth error)")
                continue

            if resp.status_code != 200:
                print(f"  [API] {ep_name} -> {resp.status_code}")
                continue

            data = resp.json()

            # Unusual Whales wraps data in various keys
            if isinstance(data, dict):
                # Try common wrapper keys
                for key in ['data', 'results', 'alerts', 'contracts', 'activity', 'options']:
                    if key in data and isinstance(data[key], list):
                        data = data[key]
                        break
                else:
                    # Maybe it's already a list at top level
                    if not isinstance(data, list):
                        if is_first:
                            print(f"  [API] {ep_name} -> dict with keys: {list(data.keys())}")
                        continue

            if isinstance(data, list) and len(data) > 0:
                if is_first:
                    print(f"  [API] {ep_name} -> {len(data)} results")
                    print(f"  [API] First record keys: {list(data[0].keys()) if isinstance(data[0], dict) else 'not a dict'}")
                    print()
                return data, ep_name

            if is_first:
                print(f"  [API] {ep_name} -> empty results")

        except Exception as e:
            print(f"  [API] {ep_name} -> ERROR: {e}")

    return None, None


# =====================================================================
# STEP 4 -- PARSE AND FILTER PRINTS
# =====================================================================

def parse_prints(raw_data, ticker):
    """Parse raw API data into normalized print dicts. Filter for signal quality."""
    parsed = []

    for r in raw_data:
        if not isinstance(r, dict):
            continue

        # Extract fields -- try common Unusual Whales field names
        leg_type_raw = (
            r.get('option_type') or r.get('type') or
            r.get('put_call') or r.get('contract_type') or ''
        ).lower().strip()

        if 'call' in leg_type_raw:
            leg_type = 'call'
        elif 'put' in leg_type_raw:
            leg_type = 'put'
        else:
            continue  # skip if we can't determine type

        strike = _float(
            r.get('strike') or r.get('strike_price') or
            r.get('strikePrice') or 0
        )

        expiry_raw = (
            r.get('expiry') or r.get('expiration_date') or
            r.get('expiration') or r.get('expires') or ''
        )
        try:
            expiry_date = date.fromisoformat(str(expiry_raw)[:10])
            dte = (expiry_date - TODAY).days
        except Exception:
            expiry_date = None
            dte = 999

        volume = _int(
            r.get('volume') or r.get('size') or
            r.get('total_volume') or 0
        )

        open_interest = _int(
            r.get('open_interest') or r.get('oi') or
            r.get('openInterest') or 0
        )

        premium = _float(
            r.get('premium') or r.get('price') or
            r.get('ask') or r.get('mid_price') or 0
        )

        notional_raw = _float(
            r.get('total_premium') or r.get('notional') or
            r.get('total_cost') or r.get('cost_basis') or 0
        )

        # Compute notional if not provided directly
        if notional_raw > 0:
            notional = notional_raw
        elif premium > 0 and volume > 0:
            notional = premium * volume * 100
        else:
            notional = 0

        # Vol/OI ratio
        vol_oi_ratio = volume / open_interest if open_interest > 0 else 0

        # Signal quality filter
        if vol_oi_ratio < MIN_VOL_OI_RATIO and notional < MIN_NOTIONAL:
            continue

        # Classify
        print_type = classify_print_type(notional)
        exp_window = classify_expiry_window(dte) if dte < 999 else 'UNKNOWN'
        side = 'BULLISH' if leg_type == 'call' else 'BEARISH'
        is_unusual = print_type in ('MEGA_BLOCK', 'BLOCK', 'SWEEP', 'LARGE')

        parsed.append({
            'ticker': ticker,
            'leg_type': leg_type,
            'strike': strike,
            'expiry': str(expiry_date) if expiry_date else None,
            'expiry_window': exp_window,
            'volume': volume,
            'open_interest': open_interest,
            'vol_oi_ratio': round(vol_oi_ratio, 2),
            'notional': round(notional, 2),
            'premium': round(premium, 4),
            'print_type': print_type,
            'side': side,
            'is_unusual': is_unusual,
            'dte': dte,
        })

    return parsed


def _float(v):
    try:
        return float(v) if v else 0.0
    except (ValueError, TypeError):
        return 0.0


def _int(v):
    try:
        return int(float(v)) if v else 0
    except (ValueError, TypeError):
        return 0


# =====================================================================
# STEP 5 -- COMPUTE TICKER SUMMARY
# =====================================================================

def compute_summary(prints, ticker):
    """Compute flow snapshot summary from filtered prints."""
    call_prints = [p for p in prints if p['side'] == 'BULLISH']
    put_prints = [p for p in prints if p['side'] == 'BEARISH']

    call_volume = sum(p['volume'] for p in call_prints)
    put_volume = sum(p['volume'] for p in put_prints)
    call_notional = sum(p['notional'] for p in call_prints)
    put_notional = sum(p['notional'] for p in put_prints)
    net_notional = call_notional - put_notional
    net_premium = (
        sum(p['premium'] for p in call_prints)
        - sum(p['premium'] for p in put_prints)
    )
    call_put_ratio = put_volume / call_volume if call_volume > 0 else 1.0

    bullish_count = len(call_prints)
    bearish_count = len(put_prints)

    # Flow signal classification
    if call_put_ratio < 0.7 and bullish_count >= 2:
        flow_signal = 'BULLISH_SWEEP'
    elif call_put_ratio > 1.3 and bearish_count >= 2:
        flow_signal = 'BEARISH_SWEEP'
    elif call_put_ratio < 0.7:
        flow_signal = 'BULLISH_SINGLE'
    elif call_put_ratio > 1.3:
        flow_signal = 'BEARISH_SINGLE'
    else:
        flow_signal = 'NEUTRAL'

    # Conviction score 0-100
    net_abs = abs(net_notional)
    conviction_score = min(100, round(
        (net_abs / 1_000_000) * 20
        + max(bullish_count, bearish_count) * 5
    ))

    # Largest unusual print
    unusual = sorted(
        [p for p in prints if p['is_unusual']],
        key=lambda x: x['notional'], reverse=True
    )
    largest_trade = None
    if unusual:
        u = unusual[0]
        largest_trade = {
            'type': u['leg_type'],
            'strike': u['strike'],
            'expiry': u['expiry'],
            'notional': u['notional'],
            'print_type': u['print_type'],
            'expiry_window': u['expiry_window'],
        }

    # Top 10 unusual prints for tape display
    top_prints = [{
        'leg_type': p['leg_type'],
        'strike': p['strike'],
        'expiry': p['expiry'],
        'expiry_window': p['expiry_window'],
        'volume': p['volume'],
        'vol_oi_ratio': p['vol_oi_ratio'],
        'notional': p['notional'],
        'print_type': p['print_type'],
        'side': p['side'],
    } for p in unusual[:10]]

    return {
        'ticker': ticker,
        'call_volume': call_volume,
        'put_volume': put_volume,
        'call_put_ratio': round(call_put_ratio, 3),
        'net_premium': round(net_premium, 2),
        'net_notional': round(net_notional, 2),
        'flow_signal': flow_signal,
        'conviction_score': conviction_score,
        'largest_trade': json.dumps(largest_trade) if largest_trade else None,
        'top_prints': json.dumps(top_prints) if top_prints else None,
        'unusual_prints': unusual,
        'print_counts': {
            'MEGA_BLOCK': sum(1 for p in unusual if p['print_type'] == 'MEGA_BLOCK'),
            'BLOCK': sum(1 for p in unusual if p['print_type'] == 'BLOCK'),
            'SWEEP': sum(1 for p in unusual if p['print_type'] == 'SWEEP'),
            'LARGE': sum(1 for p in unusual if p['print_type'] == 'LARGE'),
        },
        'source': 'API',
    }


# =====================================================================
# STEP 6 -- MOCK FALLBACK
# =====================================================================

def generate_mock_summary(ticker):
    """Generate mock flow summary from scenario_synthesis data."""
    flow_signal = 'NO_DATA'
    call_put_ratio = 1.0
    net_notional = 0
    conviction_score = 0
    source = 'NO_DATA'

    try:
        result = sb.table('scenario_synthesis') \
            .select('*') \
            .eq('ticker', ticker) \
            .order('run_ts', desc=True) \
            .limit(1) \
            .execute()
        sc = result.data
    except Exception:
        sc = None

    if sc and len(sc) > 0:
        bias = (sc[0].get('primary_bias') or sc[0].get('overall_bias') or '').upper()
        conf = sc[0].get('confidence_score', 0.5) or 0.5

        if 'BEAR' in bias:
            flow_signal = 'BEARISH_SWEEP'
            call_put_ratio = 1.40
            net_notional = -500_000
        elif 'BULL' in bias:
            flow_signal = 'BULLISH_SWEEP'
            call_put_ratio = 0.65
            net_notional = 500_000
        else:
            flow_signal = 'NEUTRAL'
            call_put_ratio = 1.00
            net_notional = 0

        conviction_score = round(float(conf) * 60)
        source = 'MOCK_FROM_SYNTHESIS'

    return {
        'ticker': ticker,
        'call_volume': 0,
        'put_volume': 0,
        'call_put_ratio': round(call_put_ratio, 3),
        'net_premium': 0,
        'net_notional': round(net_notional, 2),
        'flow_signal': flow_signal,
        'conviction_score': conviction_score,
        'largest_trade': None,
        'top_prints': None,
        'unusual_prints': [],
        'print_counts': {'MEGA_BLOCK': 0, 'BLOCK': 0, 'SWEEP': 0, 'LARGE': 0},
        'source': source,
    }


# =====================================================================
# STEP 7 -- WRITE TO SUPABASE
# =====================================================================

def write_snapshot(summary):
    """Write flow snapshot row for a ticker."""
    if DRY_RUN:
        return

    ticker = summary['ticker']
    now_iso = datetime.now(timezone.utc).isoformat()

    # Delete today's rows for this ticker
    try:
        sb.table('flow_snapshots').delete() \
            .eq('ticker', ticker) \
            .gte('run_ts', TODAY_ISO) \
            .execute()
    except Exception:
        pass

    row = {
        'run_ts': now_iso,
        'ticker': ticker,
        'call_volume': summary['call_volume'],
        'put_volume': summary['put_volume'],
        'call_put_ratio': summary['call_put_ratio'],
        'net_premium': summary['net_premium'],
        'net_notional': summary['net_notional'],
        'flow_signal': summary['flow_signal'],
        'conviction_score': summary['conviction_score'],
        'largest_trade': summary['largest_trade'],
        'top_prints': summary['top_prints'],
    }

    try:
        sb.table('flow_snapshots').insert(row).execute()
    except Exception as e:
        print(f"  [flow] Insert flow_snapshots failed for {ticker}: {e}")
        if '42P01' in str(e) or 'does not exist' in str(e):
            print("  [flow] Table may not exist. Run this SQL:")
            print(CREATE_SQL_SNAPSHOTS)


def write_prints(prints, ticker):
    """Write individual unusual prints for a ticker (max 50)."""
    if DRY_RUN:
        return

    # Delete today's rows
    try:
        sb.table('flow_prints').delete() \
            .eq('ticker', ticker) \
            .gte('run_ts', TODAY_ISO) \
            .execute()
    except Exception:
        pass

    unusual = [p for p in prints if p.get('is_unusual', False)][:50]
    if not unusual:
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    for p in unusual:
        rows.append({
            'run_ts': now_iso,
            'ticker': ticker,
            'leg_type': p['leg_type'],
            'strike': p['strike'],
            'expiry': p['expiry'],
            'expiry_window': p['expiry_window'],
            'volume': p['volume'],
            'open_interest': p['open_interest'],
            'vol_oi_ratio': p['vol_oi_ratio'],
            'notional': p['notional'],
            'premium': p['premium'],
            'print_type': p['print_type'],
            'side': p['side'],
            'is_unusual': p['is_unusual'],
        })

    try:
        sb.table('flow_prints').insert(rows).execute()
    except Exception as e:
        print(f"  [flow] Insert flow_prints failed for {ticker}: {e}")
        if '42P01' in str(e) or 'does not exist' in str(e):
            print("  [flow] Table may not exist. Run this SQL:")
            print(CREATE_SQL_PRINTS)


# =====================================================================
# MAIN
# =====================================================================

def main():
    print("=" * 50)
    print("FLOW ENGINE -- RUN STARTING")
    print(f"Date: {TODAY_ISO}")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"API key: {'set' if UNUSUAL_WHALES_API_KEY else 'NOT SET'}")
    print("=" * 50)
    print()

    summaries = []
    api_count = 0
    mock_count = 0

    for i, ticker in enumerate(TICKERS):
        is_first = (i == 0)
        print(f"[{i+1}/{len(TICKERS)}] {ticker}...")

        # Try API fetch
        raw_data, endpoint = fetch_unusual_whales(ticker, is_first=is_first)

        if raw_data:
            # Parse and filter
            prints = parse_prints(raw_data, ticker)
            print(f"  Parsed {len(prints)} filtered prints from {endpoint}")

            if prints:
                summary = compute_summary(prints, ticker)
                api_count += 1
            else:
                print(f"  No prints passed signal quality filter -- using mock")
                summary = generate_mock_summary(ticker)
                mock_count += 1
        else:
            if is_first and UNUSUAL_WHALES_API_KEY:
                print(f"  All API endpoints failed for {ticker} -- using mock fallback")
            summary = generate_mock_summary(ticker)
            mock_count += 1
            prints = []

        summaries.append(summary)

        # Write to Supabase
        write_snapshot(summary)
        write_prints(prints, ticker)

        # Per-ticker output
        sig_color = (
            'BULL' if 'BULLISH' in summary['flow_signal']
            else 'BEAR' if 'BEARISH' in summary['flow_signal']
            else '----'
        )
        print(f"  {ticker:5s} | {summary['flow_signal']:16s} | "
              f"C/P {summary['call_put_ratio']:.2f} | "
              f"Net ${summary['net_notional']:>10,.0f} | "
              f"Conv {summary['conviction_score']:>3} | "
              f"[{summary['source']}]")

        unusual = summary.get('unusual_prints', [])
        if unusual:
            pc = summary['print_counts']
            print(f"  Unusual prints: {len(unusual)} "
                  f"(MEGA:{pc['MEGA_BLOCK']} BLOCK:{pc['BLOCK']} "
                  f"SWEEP:{pc['SWEEP']} LARGE:{pc['LARGE']})")
            lt = summary.get('largest_trade')
            if lt and isinstance(lt, str):
                lt = json.loads(lt)
            if lt:
                print(f"  Largest: {lt['print_type']} {lt['type']} "
                      f"${lt['strike']} {lt.get('expiry_window', '')}")
        print()

    # -- Summary -------------------------------------------------------
    print("=" * 50)
    print("FLOW ENGINE -- RUN COMPLETE")
    print("=" * 50)
    print(f"Tickers: {len(TICKERS)} processed, "
          f"{api_count} from API, {mock_count} from mock")
    print()

    for s in summaries:
        sig = s['flow_signal']
        print(f"  {s['ticker']:5s} | {sig:16s} | "
              f"C/P {s['call_put_ratio']:.2f} | "
              f"Net ${s['net_notional']:>10,.0f} | "
              f"Conv {s['conviction_score']:>3}")

    if DRY_RUN:
        print()
        print("DRY RUN -- no rows written")

    print("=" * 50)


if __name__ == "__main__":
    main()
