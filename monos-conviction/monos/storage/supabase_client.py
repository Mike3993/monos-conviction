"""
supabase_client.py

Centralised Supabase client factory for the MONOS scanner layer.
Re-exports write_agent_log for observability across all modules.
"""

import logging
import os

from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

_client: Client | None = None


def get_client() -> Client:
    """Return a singleton Supabase client."""
    global _client
    if _client is None:
        _client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        logger.info("Supabase client initialised")
    return _client


def write_agent_log(
    agent: str,
    action: str,
    result: str,
    metadata: dict | None = None,
) -> None:
    """Write structured observability log to agent_logs."""
    row = {
        "agent": agent,
        "action": action,
        "result": result,
        "metadata": metadata or {},
    }
    try:
        get_client().table("agent_logs").insert(row).execute()
    except Exception:
        logger.exception("Failed to write agent_log: %s", row)
