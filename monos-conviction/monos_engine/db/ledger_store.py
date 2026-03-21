"""
MONOS Ledger Store
------------------
Dual-write persistence: local JSON (always) + Supabase (when available).
Local JSON is the source of truth for the dashboard session.
Supabase is the auditable cloud backup.
"""

from __future__ import annotations

import json
import os
from typing import Any

# ── local JSON persistence ───────────────────────────────────────────

_LEDGER_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
_LEDGER_FILE = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "dashboard",
    "ledger_data.json",
)


def load_local() -> tuple[list[dict[str, Any]], int]:
    """Load ledger from local JSON. Returns (entries, next_id)."""
    if os.path.exists(_LEDGER_FILE):
        try:
            with open(_LEDGER_FILE, "r") as f:
                data = json.load(f)
            return data.get("entries", []), data.get("next_id", 0)
        except Exception:
            pass
    return [], 0


def save_local(entries: list[dict[str, Any]], next_id: int) -> None:
    """Persist ledger to local JSON."""
    try:
        with open(_LEDGER_FILE, "w") as f:
            json.dump({"entries": entries, "next_id": next_id}, f, indent=2)
    except Exception:
        pass


# ── Supabase persistence ────────────────────────────────────────────

_TABLE = "trades_ledger"
_supabase_available = None


def _get_sb():
    """Lazy-load Supabase client. Returns client or None."""
    global _supabase_available
    if _supabase_available is False:
        return None
    try:
        from monos_engine.db.supabase_client import get_supabase
        sb = get_supabase()
        # Quick connectivity check on first call
        if _supabase_available is None:
            sb.table(_TABLE).select("id").limit(1).execute()
            _supabase_available = True
        return sb
    except Exception:
        _supabase_available = False
        return None


def _entry_to_row(entry: dict[str, Any]) -> dict[str, Any]:
    """Map in-memory entry dict to Supabase row dict."""
    return {
        "id":                     entry.get("id"),
        "date_open":              entry.get("date_open") or None,
        "date_close":             entry.get("date_close") or None,
        "ticker":                 entry.get("ticker", ""),
        "direction":              entry.get("direction", ""),
        "trade_mode":             entry.get("mode", ""),
        "structure":              entry.get("structure", ""),
        "contract_symbol":        entry.get("contract_symbol", ""),
        "expiration":             entry.get("expiration") or None,
        "strike":                 entry.get("strike", ""),
        "strike_delta":           entry.get("strike_delta"),
        "moneyness_pct":          entry.get("moneyness_pct"),
        "contracts":              entry.get("contracts", 1),
        "hold_days":              entry.get("hold_days"),
        "confidence":             entry.get("confidence"),
        "msa_state":              entry.get("msa_state", ""),
        "expected_return":        entry.get("expected_return"),
        "quoted_bid_open":        entry.get("quoted_bid_open"),
        "quoted_ask_open":        entry.get("quoted_ask_open"),
        "quoted_mid_open":        entry.get("quoted_mid_open"),
        "suggested_entry_price":  entry.get("suggested_entry_price"),
        "actual_entry_price":     entry.get("actual_entry_price"),
        "quoted_bid_close":       entry.get("quoted_bid_close"),
        "quoted_ask_close":       entry.get("quoted_ask_close"),
        "quoted_mid_close":       entry.get("quoted_mid_close"),
        "suggested_exit_price":   entry.get("suggested_exit_price"),
        "actual_exit_price":      entry.get("actual_exit_price"),
        "realized_pnl":           entry.get("realized_pnl"),
        "realized_return_pct":    entry.get("realized_return_pct"),
        "slippage_open":          entry.get("slippage_open"),
        "slippage_close":         entry.get("slippage_close"),
        "win":                    entry.get("win"),
        "status":                 entry.get("status", "OPEN"),
        "notes":                  entry.get("notes", ""),
        "close_notes":            entry.get("close_notes", ""),
        "strike_candidates":      json.dumps(entry.get("strike_candidates")) if entry.get("strike_candidates") else None,
    }


def sync_add(entry: dict[str, Any]) -> bool:
    """Write a new trade to Supabase. Returns True on success."""
    sb = _get_sb()
    if not sb:
        return False
    try:
        row = _entry_to_row(entry)
        sb.table(_TABLE).upsert(row, on_conflict="id").execute()
        return True
    except Exception as exc:
        print(f"[ledger_store] Supabase add failed: {exc}")
        return False


def sync_close(entry: dict[str, Any]) -> bool:
    """Update a closed trade in Supabase. Returns True on success."""
    sb = _get_sb()
    if not sb:
        return False
    try:
        row = _entry_to_row(entry)
        sb.table(_TABLE).upsert(row, on_conflict="id").execute()
        return True
    except Exception as exc:
        print(f"[ledger_store] Supabase close failed: {exc}")
        return False


def is_supabase_available() -> bool:
    """Check if Supabase is reachable."""
    _get_sb()
    return _supabase_available is True
