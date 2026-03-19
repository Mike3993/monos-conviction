"""
MONOS Translation Layer — Decision Logger
--------------------------------------------
Logs every trade decision with full translation context
for post-hoc measurement of translation layer impact.

One row per closed trade in decision_log.csv.
This is the measurement backbone — no UI, just data.
"""

from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import Any


_LOG_DIR = os.path.dirname(os.path.abspath(__file__))
DECISION_LOG = os.path.join(_LOG_DIR, "decision_log.csv")

HEADERS = [
    "timestamp",
    "trade_id",
    "ticker",
    "mode",
    "signal",
    "entry_date",
    "exit_date",
    "original_structure",
    "translated_structure",
    "original_score",
    "adjusted_score",
    "total_boost",
    "matched_rule_ids",
    "rule_count",
    "hold_days",
    "exit_reason",
    "und_return",
    "opt_return",
    "win_flag",
]


def _ensure_log() -> None:
    if not os.path.exists(DECISION_LOG):
        with open(DECISION_LOG, "w", newline="") as f:
            csv.writer(f).writerow(HEADERS)


def _resolve_score(td: dict[str, Any], trade: dict[str, Any], key: str) -> float:
    """Resolve a score value, never returning blank.

    Fallback chain:
      1. translation_data[key]
      2. trade[key]
      3. trade["weighted_return"]
      4. trade["opt_return"] or trade["option_return_pct"] or trade["realized_return_pct"]
      5. 0.0
    """
    # 1. From translation context
    val = td.get(key)
    if val is not None and val != "":
        try:
            return round(float(val), 6)
        except (ValueError, TypeError):
            pass

    # 2. From trade directly
    val = trade.get(key)
    if val is not None and val != "":
        try:
            return round(float(val), 6)
        except (ValueError, TypeError):
            pass

    # 3. From weighted_return
    val = trade.get("weighted_return")
    if val is not None and val != "":
        try:
            return round(float(val), 6)
        except (ValueError, TypeError):
            pass

    # 4. From opt_return / option_return_pct / realized_return_pct
    for fallback_key in ("opt_return", "option_return_pct", "realized_return_pct"):
        val = trade.get(fallback_key)
        if val is not None and val != "":
            try:
                return round(float(val), 6)
            except (ValueError, TypeError):
                pass

    return 0.0


def log_decision(
    trade_entry: dict[str, Any],
    translation_data: dict[str, Any] | None = None,
) -> None:
    """Log a single closed trade decision to decision_log.csv.

    Parameters
    ----------
    trade_entry : dict
        The closed ledger entry. Must have at minimum:
        id, ticker, direction, mode, structure, actual_entry_price,
        actual_exit_price, realized_pnl, realized_return_pct, win, status.
    translation_data : dict | None
        Translation context captured at batch time. If the trade was
        committed through the batch flow, this carries:
        original_score, adjusted_score, total_boost, rule_audit_tags,
        original_structure (if structure was changed).
    """
    _ensure_log()

    td = translation_data or {}
    tags = td.get("rule_audit_tags", [])

    # Extract matched rule IDs from audit tags
    matched_ids = sorted(set(
        tag["rule_id"]
        for tag in tags
        if tag.get("action") == "MATCHED"
    ))

    # Detect structure change
    orig_struct = td.get("original_structure", trade_entry.get("structure", ""))
    trans_struct = trade_entry.get("structure", "")
    struct_changed = orig_struct != trans_struct and td.get("original_structure") is not None

    win = trade_entry.get("win")
    win_str = "Y" if win is True else "N" if win is False else ""

    # Resolve scores — never blank
    orig_score = _resolve_score(td, trade_entry, "original_score")
    adj_score = _resolve_score(td, trade_entry, "adjusted_score")
    # If adjusted equals 0 but original is set, adjusted = original + boost
    if adj_score == 0.0 and orig_score != 0.0:
        adj_score = round(orig_score + float(td.get("total_boost", 0) or 0), 6)

    row = {
        "timestamp":            datetime.now().isoformat(),
        "trade_id":             trade_entry.get("id", ""),
        "ticker":               trade_entry.get("ticker", ""),
        "mode":                 trade_entry.get("mode", ""),
        "signal":               trade_entry.get("direction", trade_entry.get("signal", "")),
        "entry_date":           trade_entry.get("date_open", trade_entry.get("entry_date", "")),
        "exit_date":            trade_entry.get("date_close", trade_entry.get("exit_date", "")),
        "original_structure":   orig_struct,
        "translated_structure": trans_struct,
        "original_score":       orig_score,
        "adjusted_score":       adj_score,
        "total_boost":          float(td.get("total_boost", 0) or 0),
        "matched_rule_ids":     "|".join(matched_ids) if matched_ids else "",
        "rule_count":           len(matched_ids),
        "hold_days":            trade_entry.get("hold_days", ""),
        "exit_reason":          trade_entry.get("exit_decision", trade_entry.get("exit_reason", "")),
        "und_return":           trade_entry.get("underlying_return_pct", ""),
        "opt_return":           trade_entry.get("realized_return_pct", trade_entry.get("option_return_pct", "")),
        "win_flag":             win_str,
    }

    with open(DECISION_LOG, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS, extrasaction="ignore")
        writer.writerow(row)


def log_batch_trade(
    trade: dict[str, Any],
    translation_data: dict[str, Any] | None = None,
) -> None:
    """Log a trade from a batch/backtest run (not a ledger close).

    Used by the convexity_backtest pipeline to log every trade
    with its translation context.
    """
    _ensure_log()

    td = translation_data or {}
    tags = td.get("rule_audit_tags", [])
    matched_ids = sorted(set(
        tag["rule_id"] for tag in tags if tag.get("action") == "MATCHED"
    )) if tags else []

    orig_struct = td.get("original_structure", trade.get("structure", ""))
    trans_struct = trade.get("structure", "")
    win = trade.get("win")
    win_str = "Y" if win is True else "N" if win is False else ""

    # Resolve scores — never blank
    orig_score = _resolve_score(td, trade, "original_score")
    adj_score = _resolve_score(td, trade, "adjusted_score")
    if adj_score == 0.0 and orig_score != 0.0:
        adj_score = round(orig_score + float(td.get("total_boost", 0) or 0), 6)

    row = {
        "timestamp":            datetime.now().isoformat(),
        "trade_id":             trade.get("id", ""),
        "ticker":               trade.get("ticker", ""),
        "mode":                 trade.get("trade_mode", trade.get("mode", "")),
        "signal":               trade.get("signal", trade.get("direction", "")),
        "entry_date":           trade.get("entry_date", ""),
        "exit_date":            trade.get("exit_date", ""),
        "original_structure":   orig_struct,
        "translated_structure": trans_struct,
        "original_score":       orig_score,
        "adjusted_score":       adj_score,
        "total_boost":          float(td.get("total_boost", 0) or 0),
        "matched_rule_ids":     "|".join(matched_ids) if matched_ids else "",
        "rule_count":           len(matched_ids),
        "hold_days":            trade.get("hold_days", ""),
        "exit_reason":          trade.get("exit_reason", ""),
        "und_return":           trade.get("underlying_return_pct", ""),
        "opt_return":           trade.get("option_return_pct", ""),
        "win_flag":             win_str,
    }

    with open(DECISION_LOG, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS, extrasaction="ignore")
        writer.writerow(row)


def load_decision_log() -> list[dict[str, Any]]:
    """Load all rows from decision_log.csv."""
    if not os.path.exists(DECISION_LOG):
        return []
    rows = []
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            with open(DECISION_LOG, "r", encoding=enc) as f:
                rows = list(csv.DictReader(f))
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
    # Type-cast numeric fields
    for r in rows:
        for fld in ("original_score", "adjusted_score", "total_boost", "und_return", "opt_return"):
            try:
                r[fld] = float(r[fld]) if r.get(fld, "") != "" else None
            except (ValueError, TypeError):
                r[fld] = None
        try:
            r["rule_count"] = int(r.get("rule_count", 0) or 0)
        except (ValueError, TypeError):
            r["rule_count"] = 0
        wf = r.get("win_flag", "").strip().upper()
        r["win_flag"] = True if wf == "Y" else False if wf == "N" else None
        r["structure_changed"] = (
            r.get("original_structure", "") != r.get("translated_structure", "")
            and r.get("original_structure", "") != ""
        )
    return rows
