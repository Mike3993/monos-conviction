"""
monos_engine.pipeline.run_pipeline

End-to-end MONOS pipeline: signal generation → structure selection.
"""

from __future__ import annotations

from typing import Any

from monos_engine.combiner.signal_combiner import run_example as run_signal
from monos_engine.convexity.structure_engine import build_structure
from monos_engine.db.writes import insert_convexity_signal


def run_example(ticker: str) -> dict[str, Any]:
    """Run the full MONOS pipeline for a single ticker.

    Steps:
        1. Compute gamma + momentum → combined signal + confidence
        2. Select options structure based on signal + confidence

    Returns
    -------
    dict
        ticker, signal, confidence, structure.
    """
    signal_output = run_signal(ticker)

    signal = signal_output["combined_signal"]
    confidence = signal_output["confidence"]

    structure_output = build_structure({
        "ticker": ticker,
        "combined_signal": signal,
        "confidence": confidence,
    })

    structure = structure_output["structure"]

    insert_convexity_signal({
        "ticker": ticker,
        "signal_strength": confidence,
        "convexity_score": confidence,
        "structure_type": structure,
        "metadata": {
            "signal": signal,
            "source": "pipeline",
        },
    })

    return {
        "ticker": ticker,
        "signal": signal,
        "confidence": confidence,
        "structure": structure,
    }


if __name__ == "__main__":
    import json
    import sys

    tickers = sys.argv[1:] or ["SPY"]
    for t in tickers:
        res = run_example(t)
        print(json.dumps(res, indent=2, default=str))
