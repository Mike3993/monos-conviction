"""
monos_engine.db.writes

Write layer for the MONOS Conviction Engine.
All inserts into the engine schema go through this module.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from monos_engine.db.supabase_client import get_supabase

logger = logging.getLogger(__name__)

# ── Table names ──────────────────────────────────────────────────────

T_CONVEXITY = "convexity_signals"
T_MOMENTUM = "momentum_signals"
T_GAMMA = "gamma_exposure"

# ── Required fields per table ────────────────────────────────────────

REQUIRED_CONVEXITY: list[str] = ["ticker", "convexity_score", "signal_strength"]
REQUIRED_MOMENTUM: list[str] = ["ticker", "trend_score"]
REQUIRED_GAMMA: list[str] = ["ticker", "total_gamma"]


# ── Shared helpers ───────────────────────────────────────────────────

class InsertError(Exception):
    """Raised when a Supabase insert fails."""

    def __init__(self, table: str, detail: str, original: Exception | None = None):
        self.table = table
        self.detail = detail
        self.original = original
        super().__init__(f"Insert into {table} failed: {detail}")


def _validate(data: dict[str, Any], required: list[str], table: str) -> None:
    """Raise ``ValueError`` if any required field is missing or empty."""
    missing = [f for f in required if f not in data or data[f] is None]
    if missing:
        raise ValueError(
            f"Missing required field(s) for {table}: {', '.join(missing)}"
        )


def _prepare(data: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of *data* with defaults applied.

    * Adds ``timestamp`` (UTC ISO-8601) when absent.
    * Ensures ``metadata`` is at least an empty dict.
    """
    row = dict(data)
    if "timestamp" not in row or row["timestamp"] is None:
        row["timestamp"] = datetime.now(timezone.utc).isoformat()
    if "metadata" not in row or row["metadata"] is None:
        row["metadata"] = {}
    return row


def _insert(table: str, row: dict[str, Any]) -> dict[str, Any]:
    """Execute insert and return the inserted row."""
    try:
        sb = get_supabase()
        response = sb.table(table).insert(row).execute()
    except Exception as exc:
        logger.error("Supabase insert failed for %s: %s", table, exc)
        raise InsertError(table, str(exc), original=exc) from exc

    if not response.data:
        msg = "Insert returned no data (possible RLS / permission issue)"
        logger.error(msg)
        raise InsertError(table, msg)

    inserted = response.data[0]
    logger.info(
        "Inserted into %s: id=%s ticker=%s",
        table,
        inserted.get("id", "?"),
        inserted.get("ticker"),
    )
    return inserted


# ── convexity_signals ────────────────────────────────────────────────

def insert_convexity_signal(data: dict[str, Any]) -> dict[str, Any]:
    """Insert a single row into ``convexity_signals``.

    Required: ticker, convexity_score, signal_strength.
    """
    _validate(data, REQUIRED_CONVEXITY, T_CONVEXITY)
    row = _prepare(data)

    logger.info(
        "Inserting convexity signal: ticker=%s strength=%.2f score=%.2f",
        row["ticker"],
        row["signal_strength"],
        row["convexity_score"],
    )
    return _insert(T_CONVEXITY, row)


# ── momentum_signals ─────────────────────────────────────────────────

def insert_momentum_signal(data: dict[str, Any]) -> dict[str, Any]:
    """Insert a single row into ``momentum_signals``.

    Required: ticker, trend_score.
    Optional: velocity, rsi, regime, signal_direction, metadata.
    """
    _validate(data, REQUIRED_MOMENTUM, T_MOMENTUM)
    row = _prepare(data)

    logger.info(
        "Inserting momentum signal: ticker=%s trend_score=%.2f",
        row["ticker"],
        row["trend_score"],
    )
    return _insert(T_MOMENTUM, row)


# ── gamma_exposure ───────────────────────────────────────────────────

def insert_gamma_exposure(data: dict[str, Any]) -> dict[str, Any]:
    """Insert a single row into ``gamma_exposure``.

    Required: ticker, total_gamma.
    Optional: call_gamma, put_gamma, gamma_flip_level, dealer_positioning, metadata.
    """
    _validate(data, REQUIRED_GAMMA, T_GAMMA)
    row = _prepare(data)

    logger.info(
        "Inserting gamma exposure: ticker=%s total_gamma=%.4f",
        row["ticker"],
        row["total_gamma"],
    )
    return _insert(T_GAMMA, row)
