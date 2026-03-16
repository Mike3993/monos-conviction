"""
vol_repository.py

Persistence for vol.surface (public.vol_surface table).
"""

import logging

from .supabase_client import get_client, write_agent_log

logger = logging.getLogger(__name__)

AGENT = "vol_repository"
T_SURFACE = "vol_surface"


def write_surface(rows: list[dict]) -> int:
    """Insert vol surface rows. Returns count written."""
    if not rows:
        return 0
    sb = get_client()
    sb.table(T_SURFACE).insert(rows).execute()
    logger.info("Wrote %d vol_surface rows", len(rows))
    write_agent_log(AGENT, "write_surface", "success",
                    {"count": len(rows)})
    return len(rows)


def read_surface(ticker: str | None = None, limit: int = 50) -> list[dict]:
    """Read latest vol surface data."""
    sb = get_client()
    q = sb.table(T_SURFACE).select("*").order("timestamp", desc=True)
    if ticker:
        q = q.eq("ticker", ticker)
    return q.limit(limit).execute().data
