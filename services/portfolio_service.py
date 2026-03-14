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
from supabase import create_client

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ---------------------------------------------------------------------------
# Supabase client
# ---------------------------------------------------------------------------

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

logger.info("Supabase URL: %s", SUPABASE_URL)
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

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


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ingest_positions()
