"""
supabase_helpers.py

Shared Supabase client factory and agent_logs writer used across
all MONOS modules for consistent observability.
"""

import json
import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")


def get_supabase_client() -> Client:
    """Return a configured Supabase client."""
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def write_agent_log(
    sb: Client,
    agent: str,
    action: str,
    result: str,
    metadata: dict | None = None,
) -> None:
    """
    Write a row to the agent_logs table for pipeline observability.

    Parameters
    ----------
    sb       : Supabase client
    agent    : module or engine name (e.g. 'market_service', 'greeks_engine')
    action   : what happened (e.g. 'fetch_prices', 'snapshot_and_store')
    result   : outcome string (e.g. 'success', 'failed')
    metadata : optional dict with extra context
    """
    row = {
        "agent": agent,
        "action": action,
        "result": result,
        "metadata": metadata or {},
    }
    try:
        sb.table("agent_logs").insert(row).execute()
        logger.debug("agent_log written: %s / %s / %s", agent, action, result)
    except Exception:
        logger.exception("Failed to write agent_log: %s", row)
