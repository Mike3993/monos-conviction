"""
monos_engine.tests.test_supabase_write

Smoke test — inserts a sample convexity signal into Supabase
and prints the response.  Run from the project root:

    python -m monos_engine.tests.test_supabase_write
"""

from __future__ import annotations

import json
import logging
import sys
import os

# Ensure project root is on sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from monos_engine.db.writes import insert_convexity_signal, InsertError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

SAMPLE = {
    "ticker": "SPY",
    "signal_strength": 0.8,
    "convexity_score": 7.5,
    "implied_vol": 0.22,
    "realized_vol": 0.18,
    "skew": -0.02,
    "structure_type": "call_spread",
    "metadata": {"test": True},
}


def main() -> None:
    print()
    print("=" * 60)
    print("  MONOS Engine — Supabase Write Test")
    print("=" * 60)

    try:
        result = insert_convexity_signal(SAMPLE)
        print()
        print("  INSERT SUCCEEDED")
        print(f"  Returned row:")
        print(json.dumps(result, indent=4, default=str))
        print()

    except ValueError as ve:
        logger.error("Validation error: %s", ve)
        print(f"\n  VALIDATION ERROR: {ve}")
        sys.exit(1)

    except InsertError as ie:
        logger.error("Insert error: %s", ie)
        print(f"\n  INSERT ERROR: {ie}")
        if ie.original:
            print(f"  Original exception: {ie.original}")
        sys.exit(1)

    except Exception as exc:
        logger.exception("Unexpected error")
        print(f"\n  UNEXPECTED ERROR: {exc}")
        sys.exit(1)

    print("  Test passed.")
    print("=" * 60)


if __name__ == "__main__":
    main()
