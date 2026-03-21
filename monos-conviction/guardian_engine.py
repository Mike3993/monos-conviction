"""
GUARDIAN ENGINE — Nightly position health evaluator
====================================================
Reads all open positions + legs from Supabase, evaluates each leg
against governance rules, and writes guidance rows to
guardian_position_state.

Designed for MONOS governance-first metals options platform.

Usage:
    python guardian_engine.py          # full run
    python guardian_engine.py --dry    # evaluate only, skip writes
"""

import os
import sys
import json
from datetime import date, datetime
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

# ===============================================================
# CONFIGURATION
# ===============================================================

# Load .env — check script directory first, then one level up
script_dir = Path(__file__).resolve().parent
for env_path in [script_dir / ".env", script_dir.parent / ".env"]:
    if env_path.exists():
        load_dotenv(env_path)
        print(f"[guardian] Loaded env from {env_path}")
        break
else:
    load_dotenv()
    print("[guardian] Using default dotenv search")

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
CREATE TABLE IF NOT EXISTS guardian_position_state (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_ts            TIMESTAMPTZ NOT NULL DEFAULT now(),
    position_id       UUID NOT NULL,
    leg_id            UUID NOT NULL,
    ticker            TEXT NOT NULL,
    leg_type          TEXT NOT NULL,
    strike            NUMERIC,
    expiry            DATE,
    days_to_expiry    INTEGER,
    is_hedge          BOOLEAN,
    action            TEXT NOT NULL,
    urgency           TEXT,
    reason            TEXT,
    bounce_exit_level NUMERIC,
    next_zone         NUMERIC,
    guardian_state     TEXT,
    t0_acknowledged   BOOLEAN NOT NULL DEFAULT false,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def ensure_table():
    """Create guardian_position_state if it doesn't exist via Supabase RPC/SQL."""
    import httpx

    # Try a test select first
    r = httpx.get(
        f"{SUPABASE_URL}/rest/v1/guardian_position_state?select=id&limit=1",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        },
    )
    if r.status_code == 200:
        print("[guardian] Table guardian_position_state exists [OK]")
        return True

    # Table doesn't exist — create it via SQL endpoint
    print("[guardian] Creating table guardian_position_state...")
    r2 = httpx.post(
        f"{SUPABASE_URL}/rest/v1/rpc/",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
        },
        json={"query": CREATE_TABLE_SQL},
    )

    # If RPC doesn't work, try the SQL API endpoint
    if r2.status_code not in (200, 201, 204):
        # Try via the pg_net / SQL execution
        r3 = httpx.post(
            f"{SUPABASE_URL}/pg/query",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
            },
            json={"query": CREATE_TABLE_SQL},
        )
        if r3.status_code not in (200, 201, 204):
            print(f"[guardian] WARNING: Could not auto-create table.")
            print(f"  REST API status: {r2.status_code}")
            print(f"  Please run this SQL in the Supabase SQL Editor:")
            print()
            print(CREATE_TABLE_SQL)
            print()
            print("  Then add RLS policies:")
            print("  ALTER TABLE guardian_position_state ENABLE ROW LEVEL SECURITY;")
            print("  CREATE POLICY \"gps_all\" ON guardian_position_state FOR ALL USING (true) WITH CHECK (true);")
            return False

    # Verify it was created
    r_check = httpx.get(
        f"{SUPABASE_URL}/rest/v1/guardian_position_state?select=id&limit=1",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        },
    )
    if r_check.status_code == 200:
        print("[guardian] Table created [OK]")
        return True

    print("[guardian] WARNING: Table creation may have failed. Check Supabase SQL Editor.")
    return False


# ===============================================================
# STEP 2 — FETCH OPEN POSITIONS + LEGS
# ===============================================================

def fetch_positions():
    """
    Fetch all active positions and their legs.
    Handles two schema variants:
      A) position_legs.position_id is populated -> use FK join
      B) position_legs.position_id is NULL -> match by ticker
    """
    # Fetch positions — try is_active=true first, fall back to state=ACTIVE
    positions = []
    try:
        positions = (
            sb.table("positions")
            .select("*")
            .eq("is_active", True)
            .execute()
            .data
        ) or []
    except Exception:
        pass
    if not positions:
        try:
            positions = (
                sb.table("positions")
                .select("*")
                .eq("state", "ACTIVE")
                .execute()
                .data
            ) or []
        except Exception:
            pass

    if not positions:
        return []

    # Fetch all legs
    all_legs = sb.table("position_legs").select("*").execute().data

    # Attach legs to positions
    for pos in positions:
        # Try FK match first, then fall back to ticker match
        fk_legs = [l for l in all_legs if l.get("position_id") == pos["id"]]
        if fk_legs:
            pos["legs"] = fk_legs
        else:
            # Match by ticker (for schemas where position_id is NULL)
            pos["legs"] = [l for l in all_legs if l.get("ticker") == pos["ticker"]]

    return positions


# ===============================================================
# STEP 3 — EVALUATE EACH LEG
# ===============================================================

def evaluate_leg(leg):
    """
    Apply governance rules to a single leg.
    Returns (action, urgency, reason).

    Rules applied in priority order (first match wins):
      1. TIME DECAY EXIT — hedge with DTE <= 7
      2. CONTINUATION HOLD — hedge with DTE > 7
      3. LEAPS / CORE HOLD — non-hedge or long options
    """
    # Compute DTE from expiration field (handles both 'expiry' and 'expiration' column names)
    expiry_str = leg.get("expiry") or leg.get("expiration")
    if not expiry_str:
        return "REVIEW", "LOW", "No expiry date set — manual review needed"

    try:
        dte = (date.fromisoformat(str(expiry_str)[:10]) - TODAY).days
    except (ValueError, TypeError):
        return "REVIEW", "LOW", f"Invalid expiry date: {expiry_str}"

    is_hedge = leg.get("is_hedge", False)
    leg_type = leg.get("leg_type", "")

    # RULE 1 — TIME DECAY EXIT (highest priority)
    if is_hedge and dte <= 14:
        return (
            "CLOSE_BEFORE_DECAY",
            "HIGH",
            f"DTE {dte} <= 14 — review hedge before decay accelerates",
        )

    # RULE 2 — CONTINUATION HOLD (impulse down active)
    if is_hedge and dte > 14:
        return (
            "HOLD",
            None,
            "Impulse down active — let hedge work",
        )

    # RULE 3 — LEAPS / CORE HOLD
    if not is_hedge or leg_type in ("LONG_CALL", "LONG_PUT"):
        return (
            "HOLD",
            None,
            "Core position — hold unless invalidation triggered",
        )

    # Fallback
    return "REVIEW", "LOW", "No rule matched — manual review"


def compute_guardian_state(evaluations):
    """
    Determine position-level guardian state from leg evaluations.
      Any HIGH urgency -> ACTIVE_PROTECTION_ALERT
      All HOLD         -> ACTIVE_PROTECTION
      No hedge legs    -> UNHEDGED_REVIEW
    """
    has_hedge = any(e["is_hedge"] for e in evaluations)
    has_high = any(e["urgency"] == "HIGH" for e in evaluations)

    if has_high:
        return "ACTIVE_PROTECTION_ALERT"
    if not has_hedge:
        return "UNHEDGED_REVIEW"
    return "ACTIVE_PROTECTION"


# ===============================================================
# STEP 4 — WRITE OUTPUT ROWS
# ===============================================================

def write_results(position, evaluations, table_exists):
    """Write evaluation rows to guardian_position_state."""
    if DRY_RUN or not table_exists:
        return 0

    pos_id = position["id"]

    # Delete today's existing rows for this position to avoid duplicates
    try:
        sb.table("guardian_position_state") \
            .delete() \
            .eq("position_id", pos_id) \
            .gte("run_ts", TODAY.isoformat()) \
            .execute()
    except Exception:
        pass  # Table might be empty or row might not exist

    # Insert new rows
    rows = []
    for e in evaluations:
        rows.append({
            "position_id": pos_id,
            "leg_id": e["leg_id"],
            "ticker": e["ticker"],
            "leg_type": e["leg_type"],
            "strike": e["strike"],
            "expiry": e["expiry"],
            "days_to_expiry": e["dte"],
            "is_hedge": e["is_hedge"],
            "action": e["action"],
            "urgency": e["urgency"],
            "reason": e["reason"],
            "bounce_exit_level": None,
            "next_zone": None,
            "guardian_state": e["guardian_state"],
            "t0_acknowledged": False,
        })

    if rows:
        sb.table("guardian_position_state").insert(rows).execute()

    return len(rows)


# ===============================================================
# MAIN
# ===============================================================

def main():
    print("=" * 60)
    print("GUARDIAN ENGINE — RUN START")
    print(f"Date: {TODAY.isoformat()}")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"Supabase: {SUPABASE_URL[:40]}…")
    print("=" * 60)

    # Step 1 — Ensure output table
    table_exists = ensure_table()

    # Step 2 — Fetch positions
    print("\n[guardian] Fetching open positions...")
    positions = fetch_positions()

    if not positions:
        print("\nNo open positions found. Nothing to evaluate.")
        print("=" * 60)
        return

    print(f"[guardian] Found {len(positions)} active positions")

    # Step 3 — Evaluate
    all_evaluations = []
    total_legs = 0
    high_alerts = []
    hold_rows = []
    total_written = 0

    for pos in positions:
        legs = pos.get("legs", [])
        if not legs:
            continue

        # Deduplicate legs by id to avoid double-counting from ticker matching
        seen_ids = set()
        unique_legs = []
        for l in legs:
            lid = l.get("id", id(l))
            if lid not in seen_ids:
                seen_ids.add(lid)
                unique_legs.append(l)

        pos_evaluations = []
        for leg in unique_legs:
            total_legs += 1
            action, urgency, reason = evaluate_leg(leg)

            expiry_str = leg.get("expiry") or leg.get("expiration")
            try:
                dte = (date.fromisoformat(str(expiry_str)[:10]) - TODAY).days
            except (ValueError, TypeError):
                dte = None

            evaluation = {
                "leg_id": leg.get("id", "unknown"),
                "ticker": leg.get("ticker") or pos.get("ticker", "?"),
                "leg_type": leg.get("leg_type", "?"),
                "strike": leg.get("strike"),
                "expiry": str(expiry_str)[:10] if expiry_str else None,
                "dte": dte,
                "is_hedge": leg.get("is_hedge", False),
                "action": action,
                "urgency": urgency,
                "reason": reason,
                "guardian_state": None,  # set below
            }
            pos_evaluations.append(evaluation)

        # Compute position-level guardian state
        guardian_state = compute_guardian_state(pos_evaluations)
        for e in pos_evaluations:
            e["guardian_state"] = guardian_state

        # Collect for summary
        for e in pos_evaluations:
            if e["urgency"] == "HIGH":
                high_alerts.append(e)
            else:
                hold_rows.append(e)

        all_evaluations.extend(pos_evaluations)

        # Step 4 — Write
        written = write_results(pos, pos_evaluations, table_exists)
        total_written += written

    # -- Step 5 — Print Summary ------------------------------
    print()
    print("=" * 60)
    print("GUARDIAN ENGINE — RUN COMPLETE")
    print("=" * 60)
    print(f"Positions evaluated:  {len(positions)}")
    print(f"Legs evaluated:       {total_legs}")
    print(f"HIGH urgency alerts:  {len(high_alerts)}")
    print()

    if high_alerts:
        print("--- HIGH URGENCY --------------------------------------")
        for e in high_alerts:
            print(
                f"  [!] {e['ticker']:5s} | {e['leg_type']:12s} | "
                f"Strike {e['strike'] or '—':>6} | Exp {e['expiry'] or '—'} | "
                f"DTE {e['dte'] or '?':>3} | {e['action']}"
            )
        print()

    if hold_rows:
        print("--- HOLD ----------------------------------------------")
        for e in hold_rows:
            print(
                f"  [OK] {e['ticker']:5s} | {e['leg_type']:12s} | "
                f"Strike {e['strike'] or '—':>6} | Exp {e['expiry'] or '—'} | "
                f"DTE {e['dte'] or '?':>3} | {e['action']}"
            )
        print()

    if DRY_RUN:
        print(f"DRY RUN — no rows written (use without --dry to write)")
    elif table_exists:
        print(f"Rows written to guardian_position_state: {total_written}")
    else:
        print("Table not available — rows not written.")
        print("Create the table in Supabase SQL Editor, then re-run.")

    print("=" * 60)


if __name__ == "__main__":
    main()
