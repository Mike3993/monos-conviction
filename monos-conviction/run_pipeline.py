"""
MONOS Nightly Pipeline
========================
Runs all conviction engines in sequence:
  1. Guardian Engine -- position health evaluation
  2. Reload Engine  -- reload stage scoring

Usage:
    python run_pipeline.py          # run all engines
    python run_pipeline.py --dry    # dry run (no writes)
"""

import sys
import os
import importlib
from datetime import datetime
from pathlib import Path

# Ensure script directory is on sys.path so engines can be imported
script_dir = Path(__file__).resolve().parent
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))

# Pass through --dry flag to child engines via sys.argv
DRY_RUN = "--dry" in sys.argv


def main():
    print("=" * 60)
    print("MONOS NIGHTLY PIPELINE")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Mode:      {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print("=" * 60)

    errors = []

    # --- Guardian Engine ---
    print("\n[pipeline] Running Guardian Engine...")
    try:
        import guardian_engine
        importlib.reload(guardian_engine)
        guardian_engine.main()
        print("[pipeline] Guardian Engine complete")
    except Exception as e:
        print(f"[pipeline] Guardian Engine error: {e}")
        errors.append(("Guardian Engine", str(e)))

    # --- Reload Engine ---
    print("\n[pipeline] Running Reload Engine...")
    try:
        import reload_engine
        importlib.reload(reload_engine)
        reload_engine.main()
        print("[pipeline] Reload Engine complete")
    except Exception as e:
        print(f"[pipeline] Reload Engine error: {e}")
        errors.append(("Reload Engine", str(e)))

    # --- Summary ---
    print()
    print("=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    if errors:
        print(f"Errors: {len(errors)}")
        for name, err in errors:
            print(f"  [FAIL] {name}: {err}")
    else:
        print("All engines ran successfully [OK]")
    print("=" * 60)


if __name__ == "__main__":
    main()
