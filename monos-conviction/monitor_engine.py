"""
MONITOR ENGINE -- MONOS Conviction Pipeline
Runs nightly AFTER all other engines. Checks for known conditions
and surfaces alerts to public.monitor_alerts so T0 sees them in
the dashboard without having to remember to check.

Usage:
    python monitor_engine.py          # full run
    python monitor_engine.py --dry    # evaluate only, skip writes
"""

import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

# -- Load .env -------------------------------------------------------
script_dir = Path(__file__).resolve().parent
for env_path in [script_dir / ".env", script_dir.parent / ".env"]:
    if env_path.exists():
        load_dotenv(env_path)
        print(f"[monitor] Loaded env from {env_path}")
        break
else:
    load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("[monitor] FATAL: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set")
    sys.exit(1)

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
TODAY = date.today()
TODAY_ISO = TODAY.isoformat()
DRY_RUN = "--dry" in sys.argv

# -- Table DDL (print for operator if needed) -------------------------
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS public.monitor_alerts (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_ts       TIMESTAMPTZ NOT NULL DEFAULT now(),
  alert_code   TEXT NOT NULL,
  ticker       TEXT,
  severity     TEXT NOT NULL,
  title        TEXT NOT NULL,
  body         TEXT NOT NULL,
  action       TEXT,
  auto_resolve BOOLEAN DEFAULT false,
  resolved     BOOLEAN DEFAULT false,
  created_at   TIMESTAMPTZ DEFAULT now()
);
"""

SEVERITY_ICON = {
    "CRITICAL": "x",
    "WARNING": "!",
    "HIGH": "!",
    "INFO": "o",
}

# =====================================================================
# HELPERS
# =====================================================================

def safe_query(table, select="*", filters=None, order=None, limit=None):
    """Run a Supabase select with graceful error handling."""
    try:
        q = sb.table(table).select(select)
        if filters:
            for method, args in filters:
                q = getattr(q, method)(*args)
        if order:
            q = q.order(order[0], desc=order[1])
        if limit:
            q = q.limit(limit)
        result = q.execute()
        return result.data or []
    except Exception as e:
        print(f"  [monitor] Query {table} failed: {e}")
        return []


def get_existing_unresolved(alert_code, ticker=None):
    """Fetch unresolved alerts for a given code (optionally per ticker)."""
    filters = [("eq", ("alert_code", alert_code)), ("eq", ("resolved", False))]
    if ticker:
        filters.append(("eq", ("ticker", ticker)))
    return safe_query("monitor_alerts", "id, alert_code, ticker, created_at", filters)


def already_alerted_today(alert_code, ticker=None):
    """Check if an alert for this code was already inserted today."""
    filters = [
        ("eq", ("alert_code", alert_code)),
        ("gte", ("created_at", TODAY_ISO)),
    ]
    if ticker:
        filters.append(("eq", ("ticker", ticker)))
    rows = safe_query("monitor_alerts", "id", filters)
    return len(rows) > 0


def insert_alert(alert):
    """Insert a new alert row. Returns True on success."""
    if DRY_RUN:
        return True
    try:
        sb.table("monitor_alerts").insert(alert).execute()
        return True
    except Exception as e:
        print(f"  [monitor] Insert failed for {alert.get('alert_code')}: {e}")
        if "42P01" in str(e) or "does not exist" in str(e):
            print("  [monitor] Table may not exist. Run this SQL:")
            print(CREATE_SQL)
        return False


def auto_resolve(alert_code, ticker=None):
    """Resolve old alerts for a code when condition is no longer met."""
    existing = get_existing_unresolved(alert_code, ticker)
    resolved = 0
    for row in existing:
        if DRY_RUN:
            resolved += 1
            continue
        try:
            sb.table("monitor_alerts") \
                .update({"resolved": True}) \
                .eq("id", row["id"]) \
                .execute()
            resolved += 1
        except Exception:
            pass
    return resolved


# =====================================================================
# CHECK DEFINITIONS
# =====================================================================

def check_mon_001():
    """MON-001: DeMark setup completing / complete but synthesis unchanged."""
    alerts = []
    resolved = 0

    rows = safe_query(
        "demark_signals", "ticker, setup_count, setup_complete, setup_direction, signal_strength",
        order=("run_ts", True),
    )
    if not rows:
        return alerts, resolved

    # Dedupe to latest per ticker
    seen = {}
    for r in rows:
        t = r.get("ticker")
        if t and t not in seen:
            seen[t] = r

    for ticker, r in seen.items():
        setup_count = r.get("setup_count") or 0
        setup_complete = r.get("setup_complete", False)
        strength = r.get("signal_strength", 0)

        # Sub-check A: setup approaching completion (8/9)
        if setup_count >= 8 and not setup_complete:
            code = "MON-001"
            if not already_alerted_today(code, ticker):
                alerts.append({
                    "alert_code": code,
                    "ticker": ticker,
                    "severity": "INFO",
                    "title": f"DeMark setup {setup_count}/9 approaching completion",
                    "body": (f"{ticker} DeMark {r.get('setup_direction','').upper()} setup "
                             f"at {setup_count}/9. 1-2 bars from completion."),
                    "action": "Monitor next session for setup completion",
                    "auto_resolve": True,
                })
            # Don't resolve yet -- still approaching

        # Sub-check B: setup complete but synthesis unchanged
        elif setup_complete:
            # Check scenario_synthesis for this ticker
            synth = safe_query(
                "scenario_synthesis", "ticker, primary_bias, confidence_score",
                filters=[("eq", ("ticker", ticker))],
                order=("run_ts", True), limit=1,
            )
            bias = synth[0].get("primary_bias", "") if synth else ""
            if bias and "CHOP" in bias.upper():
                code = "MON-001"
                if not already_alerted_today(code, ticker):
                    alerts.append({
                        "alert_code": code,
                        "ticker": ticker,
                        "severity": "WARNING",
                        "title": "DeMark 9 complete but synthesis unchanged",
                        "body": (f"LIM-001: DeMark threshold may need lowering from 0.3 to 0.15 "
                                 f"for {ticker}. Setup complete at strength {strength} "
                                 f"but synthesis still shows CHOP."),
                        "action": "Review scenario_synthesis_engine.py signal_strength threshold",
                        "auto_resolve": True,
                    })
            else:
                # Condition cleared -- auto-resolve old MON-001 for ticker
                resolved += auto_resolve("MON-001", ticker)
        else:
            # Low count -- auto-resolve any stale approaching alerts
            resolved += auto_resolve("MON-001", ticker)

    return alerts, resolved


def check_mon_002():
    """MON-002: VIX data missing."""
    alerts = []
    resolved = 0
    code = "MON-002"

    rows = safe_query(
        "vix_regime", "vix, vix9d, vix3m, vvix, run_ts",
        order=("run_ts", True), limit=1,
    )
    if not rows:
        if not already_alerted_today(code):
            alerts.append({
                "alert_code": code,
                "ticker": None,
                "severity": "WARNING",
                "title": "VIX regime running on defaults",
                "body": ("LIM-002: No vix_regime rows found. "
                         "iVolatility API may not be configured. "
                         "Vol regime defaulting to NEUTRAL."),
                "action": "Debug iVolatility endpoint or switch to VIXM ETF proxy via Polygon",
                "auto_resolve": True,
            })
        return alerts, resolved

    vix_val = rows[0].get("vix")
    if vix_val is None:
        if not already_alerted_today(code):
            alerts.append({
                "alert_code": code,
                "ticker": None,
                "severity": "WARNING",
                "title": "VIX regime running on defaults",
                "body": ("LIM-002: VIX/VVIX data not available. "
                         "iVolatility API returning None. "
                         "Vol regime defaulting to NEUTRAL."),
                "action": "Debug iVolatility endpoint or switch to VIXM ETF proxy via Polygon",
                "auto_resolve": True,
            })
    else:
        # VIX is populated -- auto-resolve
        resolved += auto_resolve(code)

    return alerts, resolved


def check_mon_003():
    """MON-003: Fib levels missing for watchlist tickers."""
    alerts = []
    resolved = 0
    code = "MON-003"
    watchlist = ["GLD", "GDX", "SILJ", "SIL"]

    rows = safe_query(
        "fib_levels", "ticker",
        filters=[("gte", ("created_at", TODAY_ISO))],
    )
    tickers_with_fibs = set(r.get("ticker") for r in rows if r.get("ticker"))

    missing = [t for t in watchlist if t not in tickers_with_fibs]

    if missing:
        if not already_alerted_today(code):
            missing_str = ", ".join(missing)
            alerts.append({
                "alert_code": code,
                "ticker": missing_str,
                "severity": "INFO",
                "title": f"Fib levels missing for {missing_str}",
                "body": (f"LIM-003: No swing points entered for {missing_str}. "
                         f"Fib component contributing 0 to scanner scores. "
                         f"Enter via PIE Config tab."),
                "action": "Get coach swing high/low -- enter in PIE Config",
                "auto_resolve": True,
            })
    else:
        resolved += auto_resolve(code)

    return alerts, resolved


def check_mon_004():
    """MON-004: Guardian HIGH urgency unreviewed with DTE <= 7."""
    alerts = []
    resolved = 0
    code = "MON-004"

    rows = safe_query(
        "guardian_position_state",
        "ticker, leg_type, strike, days_to_expiry, t0_acknowledged",
        filters=[
            ("eq", ("urgency", "HIGH")),
            ("eq", ("t0_acknowledged", False)),
            ("lte", ("days_to_expiry", 7)),
        ],
    )

    if rows:
        for r in rows:
            ticker = r.get("ticker", "?")
            leg_type = r.get("leg_type", "?")
            strike = r.get("strike", "?")
            dte = r.get("days_to_expiry", "?")

            if not already_alerted_today(code, ticker):
                alerts.append({
                    "alert_code": code,
                    "ticker": ticker,
                    "severity": "CRITICAL",
                    "title": f"Unreviewed HIGH urgency alert -- DTE <= 7",
                    "body": (f"{ticker} {leg_type} {strike} expires in "
                             f"{dte} days. Not yet reviewed by T0."),
                    "action": "Review Guardian panel -- CV3 Positions tab",
                    "auto_resolve": True,
                })
    else:
        resolved += auto_resolve(code)

    return alerts, resolved


def check_mon_005():
    """MON-005: Scenario synthesis stale (>25 hours old)."""
    alerts = []
    resolved = 0
    code = "MON-005"

    rows = safe_query(
        "scenario_synthesis", "run_ts",
        order=("run_ts", True), limit=1,
    )

    if not rows:
        if not already_alerted_today(code):
            alerts.append({
                "alert_code": code,
                "ticker": None,
                "severity": "WARNING",
                "title": "Scenario synthesis not updated today",
                "body": "Pipeline may not have run. No scenario_synthesis rows found.",
                "action": "Check nightly_log.txt -- run pipeline manually",
                "auto_resolve": True,
            })
        return alerts, resolved

    run_ts_str = rows[0].get("run_ts", "")
    try:
        # Handle both Z and +00:00 formats
        ts = run_ts_str.replace("Z", "+00:00")
        if "+" not in ts and "-" not in ts[10:]:
            ts += "+00:00"
        run_ts = datetime.fromisoformat(ts)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=25)
        is_stale = run_ts < cutoff
    except Exception:
        is_stale = True
        run_ts_str = "(parse error)"

    if is_stale:
        if not already_alerted_today(code):
            alerts.append({
                "alert_code": code,
                "ticker": None,
                "severity": "WARNING",
                "title": "Scenario synthesis not updated today",
                "body": f"Pipeline may not have run. Last run: {run_ts_str}",
                "action": "Check nightly_log.txt -- run pipeline manually",
                "auto_resolve": True,
            })
    else:
        resolved += auto_resolve(code)

    return alerts, resolved


def check_mon_006():
    """MON-006: DeMark buy setup complete across multiple tickers."""
    alerts = []
    resolved = 0
    code = "MON-006"

    rows = safe_query(
        "demark_signals", "ticker, setup_complete, setup_direction",
        order=("run_ts", True),
    )
    if not rows:
        resolved += auto_resolve(code)
        return alerts, resolved

    # Dedupe to latest per ticker
    seen = {}
    for r in rows:
        t = r.get("ticker")
        if t and t not in seen:
            seen[t] = r

    buy_complete = [
        t for t, r in seen.items()
        if r.get("setup_complete") and r.get("setup_direction", "").lower() == "buy"
    ]

    if len(buy_complete) >= 3:
        if not already_alerted_today(code):
            tickers_str = ", ".join(sorted(buy_complete))
            alerts.append({
                "alert_code": code,
                "ticker": tickers_str,
                "severity": "HIGH",
                "title": "Multi-ticker DeMark buy setup complete",
                "body": (f"{len(buy_complete)} tickers showing completed buy setup 9: "
                         f"{tickers_str}. Metals complex exhaustion signal. "
                         f"Review reload stage and scenario synthesis."),
                "action": "Check Reload Engine -- consider accumulation if conditions_met >= 3",
                "auto_resolve": True,
            })
    else:
        resolved += auto_resolve(code)

    return alerts, resolved


# =====================================================================
# MAIN
# =====================================================================

ALL_CHECKS = [
    ("MON-001", "DeMark setup completing", check_mon_001),
    ("MON-002", "VIX data missing", check_mon_002),
    ("MON-003", "Fib levels missing for watchlist", check_mon_003),
    ("MON-004", "Guardian HIGH urgency unreviewed", check_mon_004),
    ("MON-005", "Scenario synthesis stale", check_mon_005),
    ("MON-006", "DeMark multi-ticker buy setup", check_mon_006),
]


def main():
    print("=" * 50)
    print("MONITOR ENGINE -- RUN STARTING")
    print(f"Date: {TODAY_ISO}")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print("=" * 50)
    print()

    total_new = 0
    total_resolved = 0
    all_new_alerts = []

    for code, label, check_fn in ALL_CHECKS:
        print(f"[{code}] {label}...")
        try:
            new_alerts, resolved = check_fn()
        except Exception as e:
            print(f"  ERROR: {e}")
            new_alerts, resolved = [], 0

        for alert in new_alerts:
            alert["run_ts"] = datetime.now(timezone.utc).isoformat()
            success = insert_alert(alert)
            if success:
                total_new += 1
                all_new_alerts.append(alert)
                sev = alert["severity"]
                icon = SEVERITY_ICON.get(sev, "?")
                print(f"  [{icon}] {sev}: {alert['title']}")

        if resolved > 0:
            print(f"  Auto-resolved {resolved} old alert(s)")
        total_resolved += resolved

        if not new_alerts and resolved == 0:
            print("  OK -- no issues")

    # -- Summary -------------------------------------------------------
    print()
    print("=" * 50)
    print("MONITOR ENGINE -- RUN COMPLETE")
    print("=" * 50)
    print(f"Checks run:    {len(ALL_CHECKS)}")
    print(f"New alerts:    {total_new}")
    print(f"Resolved:      {total_resolved}")

    if all_new_alerts:
        print()
        print("Active alerts:")
        for a in all_new_alerts:
            sev = a["severity"]
            icon = SEVERITY_ICON.get(sev, "?")
            ticker = a.get("ticker") or "--"
            print(f"  [{icon}] {a['alert_code']} {ticker:8s} -- {a['title']}")

    if DRY_RUN:
        print()
        print("DRY RUN -- no rows written")

    print("=" * 50)


if __name__ == "__main__":
    main()
