"""
probability_repository.py

Persistence for probability.regime_probabilities (public.regime_probabilities).
"""

import logging

from .supabase_client import get_client, write_agent_log

logger = logging.getLogger(__name__)

AGENT = "probability_repository"
T_REGIME = "regime_probabilities"


def write_probabilities(rows: list[dict]) -> int:
    """Insert regime probability rows. Returns count written."""
    if not rows:
        return 0
    sb = get_client()
    sb.table(T_REGIME).insert(rows).execute()
    logger.info("Wrote %d regime_probabilities rows", len(rows))
    write_agent_log(AGENT, "write_probabilities", "success",
                    {"count": len(rows)})
    return len(rows)


def read_probabilities(ticker: str | None = None,
                       limit: int = 50) -> list[dict]:
    """Read latest regime probabilities."""
    sb = get_client()
    q = sb.table(T_REGIME).select("*").order("as_of_ts", desc=True)
    if ticker:
        q = q.eq("ticker", ticker)
    return q.limit(limit).execute().data
