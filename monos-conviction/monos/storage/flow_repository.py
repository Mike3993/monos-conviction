"""
flow_repository.py

Persistence for flow.snapshots.
"""

import logging

from .supabase_client import get_client, write_agent_log

logger = logging.getLogger(__name__)

AGENT = "flow_repository"
T_SNAPSHOTS = "flow_snapshots"


def write_snapshots(rows: list[dict]) -> int:
    """Insert flow snapshot rows."""
    if not rows:
        return 0
    sb = get_client()
    sb.table(T_SNAPSHOTS).insert(rows).execute()
    logger.info("Wrote %d flow snapshots", len(rows))
    write_agent_log(AGENT, "write_snapshots", "success",
                    {"count": len(rows)})
    return len(rows)


def read_snapshots(ticker: str | None = None) -> list[dict]:
    """Read latest flow snapshots."""
    sb = get_client()
    q = sb.table(T_SNAPSHOTS).select("*").order("timestamp", desc=True)
    if ticker:
        q = q.eq("ticker", ticker)
    return q.limit(50).execute().data


def structure_repository():
    """Alias — structure persistence lives in scanner_repository."""
    from . import scanner_repository
    return scanner_repository
