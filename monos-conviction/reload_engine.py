"""
RELOAD ENGINE -- 5-Stage reload evaluation + Trigger State
============================================================
Evaluates each active ticker against 5 conditions to determine
reload stage and deployment window.

Stages:  EXHAUSTION -> FORMING -> ZONE_WATCH -> NEAR -> ACCUMULATION
Trigger: NONE       -> FORMING -> NEAR       -> NEAR -> ACTIVE

Writes to:
  1. reload_stage_log  -- stage assessment per ticker
  2. trigger_state     -- deployment window + distances

Usage:
    python reload_engine.py          # full run
    python reload_engine.py --dry    # evaluate only, skip writes
"""

import os
import sys
from datetime import date, datetime, timezone
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
TODAY_ISO = TODAY.isoformat()
DRY_RUN = "--dry" in sys.argv

PROJECT_ID = SUPABASE_URL.split("//")[1].split(".")[0]


# ===============================================================
# TABLE CHECKS
# ===============================================================

def ensure_table(table_name):
    """Verify a table exists by querying it."""
    try:
        sb.table(table_name).select("id").limit(1).execute()
        print(f"[reload] Table {table_name} exists [OK]")
        return True
    except Exception as e:
        err_str = str(e).lower()
        if any(s in err_str for s in ["404", "42p01", "does not exist", "pgrst", "schema cache"]):
            print(f"[reload] Table {table_name} not found -- create it first")
            return False
        print(f"[reload] Table check error ({table_name}): {e}")
        return False


# ===============================================================
# READ ACTIVE TICKERS
# ===============================================================

def get_active_tickers():
    """Get distinct tickers from positions where active."""
    rows = []
    try:
        res = sb.table("positions").select("ticker").eq("is_active", True).execute()
        rows = res.data or []
    except Exception:
        pass

    if not rows:
        try:
            res = sb.table("positions").select("ticker").eq("state", "ACTIVE").execute()
            rows = res.data or []
        except Exception as e:
            print(f"[reload] Error fetching tickers: {e}")

    return sorted(set(r["ticker"] for r in rows if r.get("ticker")))


# ===============================================================
# FETCH ENGINE DATA
# ===============================================================

def fetch_latest(table, ticker, order_col="run_ts"):
    """Fetch most recent row from a table for a ticker."""
    try:
        res = sb.table(table).select("*").eq("ticker", ticker)\
            .order(order_col, desc=True).limit(1).execute()
        return (res.data or [None])[0]
    except Exception:
        return None


def fetch_latest_global(table, order_col="run_ts"):
    """Fetch most recent row from a table (no ticker filter)."""
    try:
        res = sb.table(table).select("*")\
            .order(order_col, desc=True).limit(1).execute()
        return (res.data or [None])[0]
    except Exception:
        return None


def fetch_engine_data(ticker):
    """Fetch all engine data needed for evaluation."""
    gex = fetch_latest("gex_snapshots", ticker)
    fib = fetch_latest("fib_levels", ticker)
    dm = fetch_latest("demark_signals", ticker)
    sym = fetch_latest("symmetry_snapshots", ticker)
    con = fetch_latest("conflict_states", ticker)
    vix = fetch_latest_global("vix_regime")
    scenario = fetch_latest("scenario_synthesis", ticker)

    # Guardian check: any rows for this ticker today
    guardian_exists = False
    try:
        res = sb.table("guardian_position_state").select("id")\
            .eq("ticker", ticker).limit(1).execute()
        guardian_exists = len(res.data or []) > 0
    except Exception:
        pass

    return {
        "gex": gex,
        "fib": fib,
        "dm": dm,
        "sym": sym,
        "con": con,
        "vix": vix,
        "scenario": scenario,
        "guardian_exists": guardian_exists,
    }


# ===============================================================
# COMPUTE DISTANCES
# ===============================================================

def compute_distances(data):
    """Compute distances from spot to key levels as percentages."""
    gex = data["gex"]
    fib = data["fib"]
    dm = data["dm"]
    sym = data["sym"]

    spot = gex.get("spot_price") if gex else None
    distances = {
        "spot": spot,
        "dist_gamma_flip": None,
        "dist_put_wall": None,
        "dist_call_wall": None,
        "dist_fib": None,
        "demark_bars_remaining": None,
        "symmetry_score": None,
    }

    if not spot:
        return distances

    # GEX distances
    gamma_flip = gex.get("gamma_flip") if gex else None
    put_wall = gex.get("put_wall") if gex else None
    call_wall = gex.get("call_wall") if gex else None

    if gamma_flip:
        distances["dist_gamma_flip"] = round(abs(spot - gamma_flip) / spot * 100, 2)
    if put_wall:
        distances["dist_put_wall"] = round(abs(spot - put_wall) / spot * 100, 2)
    if call_wall:
        distances["dist_call_wall"] = round(abs(spot - call_wall) / spot * 100, 2)

    # Fib distance
    if fib:
        nd = fib.get("nearest_distance_pct")
        if nd is not None:
            distances["dist_fib"] = round(nd, 2)

    # DeMark bars remaining
    if dm:
        setup_count = dm.get("setup_count") or 0
        setup_complete = dm.get("setup_complete")
        signal_state = (dm.get("signal_state") or "").upper()
        if setup_complete or signal_state == "SETUP_9_PERFECT":
            distances["demark_bars_remaining"] = 0
        else:
            distances["demark_bars_remaining"] = max(0, 9 - setup_count)

    # Symmetry score
    if sym:
        distances["symmetry_score"] = sym.get("symmetry_score")

    return distances


# ===============================================================
# FIVE CONDITIONS
# ===============================================================

def evaluate_conditions(data, distances):
    """Evaluate the 5 reload conditions."""
    conditions = {}

    # C1 -- Guardian proxy: GEX data exists for this ticker
    conditions["C1_guardian"] = data["gex"] is not None

    # C2 -- Vol regime clear
    vix = data["vix"]
    vol_state = (vix.get("vol_regime_state") or "").upper() if vix else ""
    conditions["C2_vol_clear"] = vol_state in ["CALM_EXPANSIONARY", "NEUTRAL", ""]

    # C3 -- At key zone (near fib, put wall, or gamma flip)
    dist_fib = distances.get("dist_fib")
    dist_put = distances.get("dist_put_wall")
    dist_flip = distances.get("dist_gamma_flip")
    at_zone = False
    if dist_fib is not None and dist_fib <= 5.0:
        at_zone = True
    if dist_put is not None and dist_put <= 3.0:
        at_zone = True
    if dist_flip is not None and dist_flip <= 3.0:
        at_zone = True
    conditions["C3_at_zone"] = at_zone

    # C4 -- Zone stall confirmed (DeMark setup nearly complete)
    bars_rem = distances.get("demark_bars_remaining")
    conditions["C4_zone_stall"] = (bars_rem is not None and bars_rem <= 2)

    # C5 -- Momentum reversal (aligned signals + scenario matches demark)
    con = data["con"]
    conflict_state = (con.get("conflict_state") or "").upper() if con else ""
    scenario = data["scenario"]
    dm = data["dm"]

    momentum_ok = False
    if conflict_state == "ALIGNED" and scenario and dm:
        scenario_bias = (scenario.get("overall_bias") or "").upper()
        dm_direction = (dm.get("setup_direction") or "").lower()
        if scenario_bias == "BULLISH" and dm_direction == "buy":
            momentum_ok = True
        elif scenario_bias == "BEARISH" and dm_direction == "sell":
            momentum_ok = True
    conditions["C5_momentum"] = momentum_ok

    return conditions


# ===============================================================
# DEPLOYMENT BLOCKS + STAGE DETERMINATION
# ===============================================================

def determine_stage(conditions, data):
    """Determine 5-stage reload classification and deployment permission."""
    conditions_met = sum(1 for v in conditions.values() if v)
    conditions_required = 3

    # Deployment blocks
    con = data["con"]
    conflict_state = (con.get("conflict_state") or "").upper() if con else ""

    vix = data["vix"]
    vol_state = (vix.get("vol_regime_state") or "").upper() if vix else ""

    sym = data["sym"]
    convexity_state = (sym.get("convexity_state") or "").upper() if sym else ""

    block_contradiction = (conflict_state == "CONTRADICTED")
    block_vol_crisis = (vol_state == "CRISIS_WATCH")
    block_convexity = (convexity_state == "CONVEXITY_VERY_RICH")

    deployment_permitted = (
        conditions_met >= 3
        and not block_contradiction
        and not block_vol_crisis
        and not block_convexity
    )

    # Stage determination
    blocks = []
    notes = ""

    if conditions_met == 0:
        stage = "EXHAUSTION"
        allowed = False
        confidence = 0.05
        next_trigger = "STABILIZATION_AT_NEXT_ZONE"
    elif conditions_met == 1:
        stage = "FORMING"
        allowed = False
        confidence = 0.15
        next_trigger = "WAIT_FOR_MORE_CONDITIONS"
    elif conditions_met == 2:
        stage = "ZONE_WATCH"
        allowed = False
        confidence = 0.30
        next_trigger = "WAIT_FOR_ZONE"
    elif conditions_met >= 3 and not deployment_permitted:
        stage = "NEAR"
        allowed = False
        confidence = 0.50
        next_trigger = "DEPLOYMENT_WINDOW_PENDING"
        if block_contradiction:
            blocks.append("CONTRADICTED")
        if block_vol_crisis:
            blocks.append("VOL_CRISIS")
        if block_convexity:
            blocks.append("CONVEXITY_RICH")
        notes = f"Blocked: {', '.join(blocks)}"
    else:
        stage = "ACCUMULATION"
        allowed = True
        confidence = 0.65
        next_trigger = "CONFIRMATION_HIGHER_LOW"

    # Trigger state mapping
    if stage in ["EXHAUSTION", "FORMING"]:
        trigger = "NONE"
    elif stage == "ZONE_WATCH":
        trigger = "FORMING"
    elif stage == "NEAR":
        trigger = "NEAR"
    else:
        trigger = "ACTIVE"

    return {
        "stage": stage,
        "allowed": allowed,
        "confidence": confidence,
        "next_trigger": next_trigger,
        "conditions_met": conditions_met,
        "conditions_required": conditions_required,
        "deployment_permitted": deployment_permitted,
        "trigger": trigger,
        "notes": notes,
        "blocks": blocks,
    }


# ===============================================================
# WRITE RESULTS
# ===============================================================

def write_reload_stage(ticker, result):
    """Write to reload_stage_log."""
    try:
        sb.table("reload_stage_log").delete().eq("ticker", ticker)\
            .gte("run_ts", TODAY_ISO + "T00:00:00+00:00").execute()
    except Exception:
        pass

    row = {
        "ticker": ticker,
        "reload_stage": result["stage"],
        "reload_allowed": result["allowed"],
        "reload_confidence": result["confidence"],
        "conditions_met_count": result["conditions_met"],
        "conditions_required": result["conditions_required"],
        "next_trigger": result["next_trigger"],
        "invalidation_breached": False,
        "reload_blocked_reason": result["notes"] if result["notes"] else None,
    }
    try:
        sb.table("reload_stage_log").insert(row).execute()
        return True
    except Exception as e:
        print(f"  [error] reload_stage_log insert failed for {ticker}: {e}")
        return False


def write_trigger_state(ticker, result, distances, data):
    """Write to trigger_state."""
    try:
        sb.table("trigger_state").delete().eq("ticker", ticker)\
            .gte("run_ts", TODAY_ISO + "T00:00:00+00:00").execute()
    except Exception:
        pass

    con = data["con"]
    conflict_state = (con.get("conflict_state") or "") if con else ""

    row = {
        "ticker": ticker,
        "trigger_state": result["trigger"],
        "dist_to_gamma_flip": distances.get("dist_gamma_flip"),
        "dist_to_put_wall": distances.get("dist_put_wall"),
        "dist_to_call_wall": distances.get("dist_call_wall"),
        "dist_to_fib_level": distances.get("dist_fib"),
        "demark_bars_remaining": distances.get("demark_bars_remaining"),
        "symmetry_score": distances.get("symmetry_score"),
        "conflict_state": conflict_state,
        "conditions_met": result["conditions_met"],
        "conditions_required": result["conditions_required"],
        "deployment_permitted": result["deployment_permitted"],
        "notes": result["notes"] if result["notes"] else None,
    }
    try:
        sb.table("trigger_state").insert(row).execute()
        return True
    except Exception as e:
        print(f"  [error] trigger_state insert failed for {ticker}: {e}")
        return False


# ===============================================================
# MAIN
# ===============================================================

STAGE_ICONS = {
    "EXHAUSTION": "o",
    "FORMING": "O",
    "ZONE_WATCH": "@",
    "NEAR": "*",
    "ACCUMULATION": "#",
}

TRIGGER_ICONS = {
    "NONE": "x",
    "FORMING": "~",
    "NEAR": "+",
    "ACTIVE": "!",
}


def main():
    print("=" * 64)
    print("RELOAD ENGINE -- 5-Stage + Trigger State")
    print(f"Date: {TODAY}")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"Supabase: {SUPABASE_URL[:45]}...")
    print("=" * 64)

    # Ensure tables
    tables_ok = True
    for tbl in ["reload_stage_log", "trigger_state"]:
        if not ensure_table(tbl):
            tables_ok = False

    if not tables_ok and not DRY_RUN:
        print("\n[reload] Cannot write results -- tables missing. Exiting.")
        sys.exit(1)

    # Get tickers
    tickers = get_active_tickers()
    print(f"\n[reload] Active tickers: {tickers if tickers else 'none found'}")

    if not tickers:
        print("[reload] No active tickers. Nothing to evaluate.")
        print("=" * 64)
        return

    # Evaluate each ticker
    all_results = []
    for ticker in tickers:
        print(f"\n[{ticker}] Fetching engine data...")
        data = fetch_engine_data(ticker)

        sources = []
        if data["gex"]:        sources.append("GEX")
        if data["fib"]:        sources.append("Fib")
        if data["dm"]:         sources.append("DeMark")
        if data["sym"]:        sources.append("Symmetry")
        if data["con"]:        sources.append("Conflict")
        if data["vix"]:        sources.append("VIX")
        if data["scenario"]:   sources.append("Scenario")
        if data["guardian_exists"]: sources.append("Guardian")
        print(f"  [{ticker}] Sources: {', '.join(sources) if sources else 'NONE'}")

        distances = compute_distances(data)
        conditions = evaluate_conditions(data, distances)
        result = determine_stage(conditions, data)

        print(f"  [{ticker}] Stage: {result['stage']} | "
              f"Trigger: {result['trigger']} | "
              f"Conditions: {result['conditions_met']}/{result['conditions_required']}")

        if not DRY_RUN:
            ok1 = write_reload_stage(ticker, result)
            ok2 = write_trigger_state(ticker, result, distances, data)
            status = "OK" if (ok1 and ok2) else "PARTIAL"
            print(f"  [{ticker}] Write: [{status}]")

        all_results.append((ticker, data, distances, conditions, result))

    # Summary
    print()
    print("=" * 64)
    print("RELOAD ENGINE -- RUN COMPLETE")
    print("=" * 64)
    print(f"Tickers evaluated: {len(all_results)}")
    print()
    print("Stages: o=EXHAUSTION  O=FORMING  @=ZONE_WATCH  *=NEAR  #=ACCUMULATION")
    print("Trigger: x=NONE  ~=FORMING  +=NEAR  !=ACTIVE")
    print()

    for ticker, data, distances, conditions, result in all_results:
        stage = result["stage"]
        trigger = result["trigger"]
        si = STAGE_ICONS.get(stage, "?")
        ti = TRIGGER_ICONS.get(trigger, "?")

        print(f"  [{si}] {ticker:<6} | {stage:<14} | trigger=[{ti} {trigger}]")

        # Conditions
        c_parts = []
        for k, v in conditions.items():
            label = k.split("_", 1)[0]
            c_parts.append(f"{label}:{'Y' if v else 'N'}")
        print(f"    {' '.join(c_parts)}")
        print(f"    Conditions: {result['conditions_met']}/{result['conditions_required']} required")

        # Distances
        dg = distances.get("dist_gamma_flip")
        dp = distances.get("dist_put_wall")
        dc = distances.get("dist_call_wall")
        df = distances.get("dist_fib")
        bars = distances.get("demark_bars_remaining")

        dist_parts = []
        if dg is not None:
            dist_parts.append(f"Gamma flip: {dg:.1f}%")
        if dp is not None:
            dist_parts.append(f"Put wall: {dp:.1f}%")
        if dc is not None:
            dist_parts.append(f"Call wall: {dc:.1f}%")
        if df is not None:
            dist_parts.append(f"Fib: {df:.1f}%")
        if dist_parts:
            print(f"    Dist {' | '.join(dist_parts)}")

        if bars is not None:
            print(f"    DeMark bars remaining: {bars}")

        # Deployment
        if result["deployment_permitted"]:
            print(f"    Deployment: PERMITTED")
        else:
            if result["notes"]:
                print(f"    Deployment: BLOCKED -- {result['notes']}")
            else:
                reason = "insufficient conditions" if result["conditions_met"] < 3 else "unknown"
                print(f"    Deployment: BLOCKED -- {reason}")
        print()

    print("=" * 64)


if __name__ == "__main__":
    main()
