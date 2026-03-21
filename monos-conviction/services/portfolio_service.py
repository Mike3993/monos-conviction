"""
portfolio_service.py

Ingests sample position data (ladders, positions, position_legs) from
the local JSON file and upserts into Supabase. Uses structured logging
throughout for observability.
"""

import json
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.supabase_helpers import get_supabase_client, write_agent_log

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

AGENT_NAME = "portfolio_service"

# ---------------------------------------------------------------------------
# Supabase client
# ---------------------------------------------------------------------------

supabase = get_supabase_client()
logger.info("Supabase client initialized")

# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "sample_positions.json")


def ingest_positions(path: str = DATA_PATH) -> None:
    """Load JSON and insert ladders, positions, and position_legs into Supabase."""

    logger.info("MONOS portfolio ingestion started")
    logger.info("Loading JSON from %s ...", path)

    with open(path) as f:
        data = json.load(f)

    logger.info("JSON loaded - ladders: %d | positions: %d | legs: %d",
                len(data["ladders"]), len(data["positions"]), len(data["position_legs"]))

    # -- ladders --
    logger.info("Inserting ladders...")
    for ladder in data["ladders"]:
        try:
            supabase.table("ladders").insert(ladder).execute()
            logger.debug("Inserted ladder: %s", ladder.get("name"))
        except Exception:
            logger.exception("Failed to insert ladder %s", ladder)

    # -- positions --
    logger.info("Inserting positions...")
    for position in data["positions"]:
        try:
            supabase.table("positions").insert(position).execute()
            logger.debug("Inserted position: %s", position.get("ticker"))
        except Exception:
            logger.exception("Failed to insert position %s", position)

    # -- position legs --
    logger.info("Inserting position legs...")
    for leg in data["position_legs"]:
        try:
            supabase.table("position_legs").insert(leg).execute()
            logger.debug("Inserted leg: %s K=%s", leg.get("leg_type"), leg.get("strike"))
        except Exception:
            logger.exception("Failed to insert leg %s", leg)

    logger.info("MONOS Position Registry populated successfully")

    write_agent_log(supabase, AGENT_NAME, "ingest_positions",
                    "success", {
                        "ladders": len(data["ladders"]),
                        "positions": len(data["positions"]),
                        "legs": len(data["position_legs"]),
                    })


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ingest_positions()
