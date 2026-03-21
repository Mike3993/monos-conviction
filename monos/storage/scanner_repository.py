"""
scanner_repository.py

Persistence for scanner.candidates, scanner.structure_library, and scanner.scenarios.
Uses the public schema (Supabase REST) with table names prefixed for clarity.
If custom schemas aren't accessible via REST, falls back to public-schema tables.
"""

import logging
from datetime import date

from .supabase_client import get_client, write_agent_log

logger = logging.getLogger(__name__)

AGENT = "scanner_repository"

# ── Table names (public-schema fallback if custom schemas aren't exposed) ──
T_CANDIDATES = "scanner_candidates"
T_STRUCTURES = "scanner_structure_library"
T_SCENARIOS  = "scanner_scenarios"


def _ensure_tables():
    """Create public-schema mirror tables if they don't exist yet."""
    sb = get_client()
    # We rely on Supabase migrations for DDL.  This is a no-op guard.
    pass


def write_candidates(rows: list[dict]) -> int:
    """Insert candidate rows. Returns count written."""
    if not rows:
        return 0
    sb = get_client()
    for row in rows:
        row.setdefault("scan_date", date.today().isoformat())
    sb.table(T_CANDIDATES).insert(rows).execute()
    logger.info("Wrote %d candidates", len(rows))
    write_agent_log(AGENT, "write_candidates", "success",
                    {"count": len(rows)})
    return len(rows)


def write_structures(rows: list[dict]) -> int:
    """Insert structure_library rows."""
    if not rows:
        return 0
    sb = get_client()
    sb.table(T_STRUCTURES).insert(rows).execute()
    logger.info("Wrote %d structures", len(rows))
    write_agent_log(AGENT, "write_structures", "success",
                    {"count": len(rows)})
    return len(rows)


def write_scenarios(rows: list[dict]) -> int:
    """Insert scenario rows."""
    if not rows:
        return 0
    sb = get_client()
    sb.table(T_SCENARIOS).insert(rows).execute()
    logger.info("Wrote %d scenarios", len(rows))
    write_agent_log(AGENT, "write_scenarios", "success",
                    {"count": len(rows)})
    return len(rows)


def read_candidates(scan_date: str | None = None) -> list[dict]:
    """Read candidates, optionally filtered by scan_date."""
    sb = get_client()
    q = sb.table(T_CANDIDATES).select("*").order("opportunity_score", desc=True)
    if scan_date:
        q = q.eq("scan_date", scan_date)
    return q.execute().data


def read_structures(ticker: str | None = None) -> list[dict]:
    """Read structure_library rows."""
    sb = get_client()
    q = sb.table(T_STRUCTURES).select("*")
    if ticker:
        q = q.eq("ticker", ticker)
    return q.order("created_at", desc=True).execute().data


def update_governor_status(structure_id: str, status: str) -> None:
    """Update governor_status on a structure."""
    sb = get_client()
    sb.table(T_STRUCTURES).update({"governor_status": status}).eq("id", structure_id).execute()
    logger.info("Updated structure %s -> %s", structure_id, status)
