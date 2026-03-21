"""
RELOAD ENGINE — Stage-based reload evaluation
================================================
Evaluates each active ticker against 5 conditions to determine
reload stage (EXHAUSTION -> ZONE_WATCH -> ACCUMULATION).

Designed for MONOS governance-first metals options platform.

Usage:
    python reload_engine.py          # full run
    python reload_engine.py --dry    # evaluate only, skip writes
"""

import os
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

# ===============================================================
# CONFIGURATION
# ===============================================================

script_dir = Path(__file__).resolve().parent
for env_path in [script_dir / ".env", script_dir.parent / ".env"]:
    if env_path.exists():
        load_dotenv(env_path)
        print(f"[reload] Loaded env from {env_path}")
        break
else:
    load_dotenv()
    print("[reload] Using default dotenv search")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")
    sys.exit(1)

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
TODAY = date.today()
DRY_RUN = "--dry" in sys.argv

# ===============================================================
# STEP 1 — ENSURE OUTPUT TABLE EXISTS
# ===============================================================

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.reload_stage_log (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_ts                TIMESTAMPTZ NOT NULL DEFAULT now(),
  ticker                TEXT NOT NULL,
  reload_stage          TEXT NOT NULL,
  reload_allowed        BOOLEAN NOT NULL,
  reload_confidence     NUMERIC,
  conditions_met_count  INTEGER,
  conditions_required   INTEGER,
  next_trigger          TEXT,
  invalidation_breached BOOLEAN NOT NULL DEFAULT false,
  reload_blocked_reason TEXT,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

PROJECT_ID = SUPABASE_URL.split("//")[1].split(".")[0]


def ensure_table():
    """Try to verify the table exists by querying it."""
    try:
        sb.table("reload_stage_log").select("id").limit(1).execute()
        print("[reload] Table reload_stage_log exists [OK]")
        return True
    except Exception as e:
        err_str = str(e)
        if any(s in err_str.lower() for s in ["404", "42p01", "does not exist", "pgrst", "schema cache"]):
            print("[reload] Table reload_stage_log not found.")
            print("[reload] Please run this SQL in the Supabase SQL Editor:\n")
            print(CREATE_TABLE_SQL)
            print(f"[reload] SQL Editor: https://supabase.com/dashboard/project/{PROJECT_ID}/sql/new")
            return False
        print(f"[reload] Table check error: {e}")
        return False


# ===============================================================
# STEP 2 — READ ACTIVE TICKERS
# ===============================================================


def get_active_tickers():
    """Get distinct tickers from positions where active."""
    rows = []

    # Try is_active=true first (convexity_desk schema)
    try:
        res = sb.table("positions").select("ticker").eq("is_active", True).execute()
        rows = res.data or []
    except Exception:
        pass

    # Fallback: state=ACTIVE (convexity_engine schema)
    if not rows:
        try:
            res = sb.table("positions").select("ticker").eq("state", "ACTIVE").execute()
            rows = res.data or []
        except Exception as e:
            print(f"[reload] Error fetching tickers: {e}")

    return sorted(set(r["ticker"] for r in rows if r.get("ticker")))


# ===============================================================
# STEP 3 — STAGE EVALUATION LOGIC
# ===============================================================


def evaluate_ticker(ticker):
    """Evaluate 5 conditions for a ticker and determine reload stage."""
    conditions = {}

    # C1 — Guardian rows exist for this ticker
    try:
        res = sb.table("guardian_position_state").select("id").eq("ticker", ticker).limit(1).execute()
        conditions["C1_guardian"] = len(res.data or []) > 0
    except Exception:
        conditions["C1_guardian"] = False

    # C2 — Vol regime clear (placeholder — always TRUE)
    conditions["C2_vol_clear"] = True

    # C3 — At key zone (placeholder — always FALSE)
    conditions["C3_at_zone"] = False

    # C4 — Zone stall confirmed (placeholder — always FALSE)
    conditions["C4_zone_stall"] = False

    # C5 — Momentum reversal (placeholder — always FALSE)
    conditions["C5_momentum"] = False

    conditions_met = sum(1 for v in conditions.values() if v)
    conditions_required = 3

    # Determine stage
    if conditions_met >= 3:
        stage = "ACCUMULATION"
        allowed = True
        confidence = 0.55
        next_trigger = "CONFIRMATION_HIGHER_LOW"
    elif conditions_met >= 1:
        stage = "ZONE_WATCH"
        allowed = False
        confidence = 0.25
        next_trigger = "WAIT_FOR_ZONE"
    else:
        stage = "EXHAUSTION"
        allowed = False
        confidence = 0.10
        next_trigger = "STABILIZATION_AT_NEXT_ZONE"

    result = {
        "ticker": ticker,
        "reload_stage": stage,
        "reload_allowed": allowed,
        "reload_confidence": confidence,
        "conditions_met_count": conditions_met,
        "conditions_required": conditions_required,
        "next_trigger": next_trigger,
        "invalidation_breached": False,
        "reload_blocked_reason": None,
    }

    return result, conditions


# ===============================================================
# STEP 4 — WRITE OUTPUT
# ===============================================================


def write_result(result):
    """Delete today's rows for ticker, then insert fresh row."""
    ticker = result["ticker"]

    # Delete today's existing rows for this ticker
    try:
        sb.table("reload_stage_log").delete().eq("ticker", ticker).gte(
            "run_ts", TODAY.isoformat()
        ).execute()
    except Exception as e:
        print(f"  [warn] Delete failed for {ticker}: {e}")

    # Insert new row
    try:
        sb.table("reload_stage_log").insert(result).execute()
    except Exception as e:
        print(f"  [error] Insert failed for {ticker}: {e}")


# ===============================================================
# STEP 5 — MAIN
# ===============================================================

STAGE_ICONS = {
    "EXHAUSTION": "O",
    "ZONE_WATCH": "@",
    "ACCUMULATION": "#",
    "BLOCKED": "X",
}


def main():
    print("=" * 60)
    print("RELOAD ENGINE — RUN START")
    print(f"Date: {TODAY}")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"Supabase: {SUPABASE_URL[:45]}…")
    print("=" * 60)

    # Step 1 — ensure table
    if not ensure_table():
        if not DRY_RUN:
            print("\n[reload] Cannot write results — table missing. Exiting.")
            sys.exit(1)
        print("\n[reload] DRY RUN — continuing without table.")

    # Step 2 — get tickers
    tickers = get_active_tickers()
    print(f"\n[reload] Active tickers: {tickers if tickers else 'none found'}")

    if not tickers:
        print("[reload] No active tickers. Nothing to evaluate.")
        print("=" * 60)
        return

    # Step 3 + 4 — evaluate and write
    results = []
    for ticker in tickers:
        result, conditions = evaluate_ticker(ticker)
        results.append((result, conditions))
        if not DRY_RUN:
            write_result(result)

    # Step 5 — summary
    print()
    print("=" * 60)
    print("RELOAD ENGINE — RUN COMPLETE")
    print("=" * 60)
    print(f"Tickers evaluated:  {len(results)}")
    if not DRY_RUN:
        print(f"Rows written:       {len(results)}")
    print()
    print("O = EXHAUSTION  @ = ZONE_WATCH  # = ACCUMULATION  X = BLOCKED")
    print()

    for result, conditions in results:
        stage = result["reload_stage"]
        icon = STAGE_ICONS.get(stage, "?")
        allowed = "Y" if result["reload_allowed"] else "N"
        conf = result["reload_confidence"]

        cond_parts = []
        for k, v in conditions.items():
            cond_parts.append(f"{'Y' if v else '.'}{k}")

        print(f"  {icon} {result['ticker']:<6} | {stage:<22} | allowed={allowed} | confidence={conf:.2f}")
        print(f"    Conditions: [{result['conditions_met_count']}/{result['conditions_required']}] {' '.join(cond_parts)}")
        print(f"    Next: {result['next_trigger']}")
        if result["reload_blocked_reason"]:
            print(f"    [!] BLOCKED: {result['reload_blocked_reason']}")
        print()

    print("=" * 60)


if __name__ == "__main__":
    main()
