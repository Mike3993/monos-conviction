"""
dealer_repository.py

Persistence for dealer.positioning.
"""

import logging

from .supabase_client import get_client, write_agent_log

logger = logging.getLogger(__name__)

AGENT = "dealer_repository"
T_POSITIONING = "dealer_positioning"


def write_positioning(rows: list[dict]) -> int:
    """Insert dealer positioning rows."""
    if not rows:
        return 0
    sb = get_client()
    sb.table(T_POSITIONING).insert(rows).execute()
    logger.info("Wrote %d dealer positioning rows", len(rows))
    write_agent_log(AGENT, "write_positioning", "success",
                    {"count": len(rows)})
    return len(rows)


def read_positioning(ticker: str | None = None) -> list[dict]:
    """Read latest dealer positioning."""
    sb = get_client()
    q = sb.table(T_POSITIONING).select("*").order("timestamp", desc=True)
    if ticker:
        q = q.eq("ticker", ticker)
    return q.limit(50).execute().data
