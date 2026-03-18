"""
monos_engine.tests.test_engine_writes

Smoke test — inserts one momentum signal and one gamma exposure
snapshot into Supabase and prints the responses.

    python -m monos_engine.tests.test_engine_writes
"""

from __future__ import annotations

import json
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from monos_engine.db.writes import (
    insert_momentum_signal,
    insert_gamma_exposure,
    InsertError,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

MOMENTUM_SAMPLE = {
    "ticker": "QQQ",
    "trend_score": 72.5,
    "velocity": 1.8,
    "rsi": 61.3,
    "regime": "BULLISH",
    "signal_direction": "LONG",
    "metadata": {"test": True},
}

GAMMA_SAMPLE = {
    "ticker": "SPY",
    "total_gamma": 1.25e9,
    "call_gamma": 8.5e8,
    "put_gamma": 4.0e8,
    "gamma_flip_level": 542.50,
    "dealer_positioning": "POSITIVE",
    "metadata": {"test": True},
}


def main() -> None:
    print()
    print("=" * 60)
    print("  MONOS Engine — Extended Write Tests")
    print("=" * 60)
    failed = False

    # ── Momentum Signal ──────────────────────────────────────
    print("\n  [1/2] Inserting momentum signal...")
    try:
        result = insert_momentum_signal(MOMENTUM_SAMPLE)
        print("  MOMENTUM INSERT SUCCEEDED")
        print(json.dumps(result, indent=4, default=str))
    except (ValueError, InsertError) as e:
        logger.error("Momentum insert failed: %s", e)
        print(f"  MOMENTUM ERROR: {e}")
        failed = True
    except Exception as e:
        logger.exception("Unexpected error")
        print(f"  UNEXPECTED ERROR: {e}")
        failed = True

    # ── Gamma Exposure ───────────────────────────────────────
    print("\n  [2/2] Inserting gamma exposure...")
    try:
        result = insert_gamma_exposure(GAMMA_SAMPLE)
        print("  GAMMA INSERT SUCCEEDED")
        print(json.dumps(result, indent=4, default=str))
    except (ValueError, InsertError) as e:
        logger.error("Gamma insert failed: %s", e)
        print(f"  GAMMA ERROR: {e}")
        failed = True
    except Exception as e:
        logger.exception("Unexpected error")
        print(f"  UNEXPECTED ERROR: {e}")
        failed = True

    # ── Summary ──────────────────────────────────────────────
    print()
    if failed:
        print("  Some tests failed.")
        print("=" * 60)
        sys.exit(1)
    else:
        print("  All tests passed.")
        print("=" * 60)


if __name__ == "__main__":
    main()
