"""
MONOS Backtest Dashboard — Flask backend.

Thin reporting/UI wrapper around the existing backtest engine.
Does NOT modify trading logic, payoff, mode, or filter behaviour.
"""

from __future__ import annotations

import json
import traceback
from typing import Any

from flask import Flask, jsonify, render_template, request

from monos_engine.backtest.convexity_backtest import run_convexity_backtest, run_multi_hold
from monos_engine.trades.top_trades import generate_top_trades
from monos_engine.trades.trade_dialogue import generate_dialogue
from monos_engine.trades.exit_engine import get_exit_recommendation, get_all_exit_recommendations
from monos_engine.translation.execution_policy_mapper import (
    apply_policies_batch, format_trace, get_batch_summary,
)
from monos_engine.translation.rule_selector import get_registry_summary
from monos_engine.translation.rule_bridge import get_active_rules, load_evidence
from monos_engine.translation.decision_logger import log_decision, log_batch_trade, load_decision_log
from monos_engine.translation.kill_switch import (
    kill_rule, revive_rule, set_manual_influence,
    get_all_rule_states, kill_all,
)
from monos_engine.options.provider import (
    pick_best_contract,
    get_expirations,
    get_chain,
    get_quote,
    get_spot_price,
)

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static",
)


# ── helpers ─────────────────────────────────────────────────────────

def _safe_json(obj: Any) -> Any:
    """Make numpy / non-serialisable types safe for json.dumps."""
    import numpy as np

    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, dict):
        return {k: _safe_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe_json(v) for v in obj]
    return obj


def _run_example(ticker: str) -> dict[str, Any]:
    """Run convexity backtest for *ticker* and return the result dict."""
    return _safe_json(run_convexity_backtest(ticker))


def _run_multi(ticker: str) -> dict[str, Any]:
    """Run multi-hold comparison and strip the heavy full_result."""
    raw = run_multi_hold(ticker)
    # Strip full_result to keep payload small
    summary: dict[str, Any] = {}
    for key, val in raw.items():
        entry = {k: v for k, v in val.items() if k != "full_result"}
        summary[key] = entry
    return _safe_json(summary)


# ── routes ──────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/run-example", methods=["POST"])
def api_run_example():
    data = request.get_json(silent=True) or {}
    ticker = (data.get("ticker") or "SPY").upper().strip()
    try:
        result = _run_example(ticker)
        return jsonify({"ok": True, "data": result})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "trace": traceback.format_exc()}), 500


@app.route("/api/run-multi-hold", methods=["POST"])
def api_run_multi_hold():
    data = request.get_json(silent=True) or {}
    ticker = (data.get("ticker") or "SPY").upper().strip()
    try:
        result = _run_multi(ticker)
        return jsonify({"ok": True, "data": result})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "trace": traceback.format_exc()}), 500


@app.route("/api/run-both", methods=["POST"])
def api_run_both():
    data = request.get_json(silent=True) or {}
    ticker = (data.get("ticker") or "SPY").upper().strip()
    try:
        example = _run_example(ticker)
        multi = _run_multi(ticker)
        return jsonify({"ok": True, "data": {"example": example, "multi_hold": multi}})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "trace": traceback.format_exc()}), 500


DEFAULT_BATCH_TICKERS = ["SPY", "QQQ", "IWM", "SMH", "GLD", "SLV"]


@app.route("/api/run-batch", methods=["POST"])
def api_run_batch():
    """Run example + multi-hold for each ticker and return a ranked summary."""
    data = request.get_json(silent=True) or {}
    tickers = data.get("tickers") or DEFAULT_BATCH_TICKERS
    tickers = [t.upper().strip() for t in tickers]

    results = []
    errors = []

    for ticker in tickers:
        try:
            example = _run_example(ticker)
            multi = _run_multi(ticker)

            # Find best hold by highest weighted_total_return
            best_key = None
            best_wgt = None
            for key, val in multi.items():
                wgt = val.get("weighted_total_return", 0)
                if best_wgt is None or wgt > best_wgt:
                    best_wgt = wgt
                    best_key = key

            best_hold = multi[best_key]["hold_days"] if best_key else example.get("hold_days", 2)
            best_wr = multi[best_key]["win_rate"] if best_key else example.get("win_rate", 0)
            best_wgt = best_wgt if best_wgt is not None else 0

            # Signal strength
            if best_wgt > 5 and best_wr > 60:
                strength = "HIGH"
            elif best_wgt > 1:
                strength = "MEDIUM"
            else:
                strength = "LOW"

            # Extract last trade signal for direction inference
            ex_trades = example.get("trades", [])
            last_signal = ex_trades[-1]["signal"] if ex_trades else None

            results.append({
                "ticker": ticker,
                "mode": example.get("mode", "TACTICAL"),
                "best_hold": best_hold,
                "win_rate": round(best_wr, 2),
                "weighted_return": round(best_wgt, 4),
                "trades": example.get("total_trades", 0),
                "strength": strength,
                "mr_trades": example.get("mean_reversion_trades", 0),
                "hc_trades": example.get("high_conviction_trades", 0),
                "last_signal": last_signal,
                "multi_hold": multi,
            })
        except Exception as exc:
            errors.append({"ticker": ticker, "error": str(exc)})

    # Sort by weighted_return descending
    results.sort(key=lambda r: r["weighted_return"], reverse=True)

    # Apply translation layer: registry-governed policy mapping
    trace_text = ""
    try:
        results = apply_policies_batch(results)
        translation = get_batch_summary(results)
        trace_text = format_trace(results)
    except Exception:
        translation = {"total_trades": len(results), "trades_touched": 0, "ranking_changed": False}

    # Log batch-level summary trades to decision_log.csv
    # (one row per ticker from the batch results, not per-backtest-trade)
    try:
        for res in results:
            wgt_ret = res.get("weighted_return", 0) or 0
            orig = res.get("original_score", wgt_ret)
            adj = res.get("adjusted_score", wgt_ret)

            summary_trade = {
                "ticker": res.get("ticker", ""),
                "mode": res.get("mode", ""),
                "direction": res.get("last_signal", ""),
                "date_open": "",
                "date_close": "",
                "structure": res.get("structure", ""),
                "hold_days": res.get("best_hold", ""),
                "exit_reason": "BATCH_SUMMARY",
                "underlying_return_pct": "",
                "option_return_pct": wgt_ret,
                "weighted_return": wgt_ret,
                "win": wgt_ret > 0,
            }
            translation_ctx = {
                "original_score": orig if orig is not None else wgt_ret,
                "adjusted_score": adj if adj is not None else wgt_ret,
                "total_boost": res.get("total_boost", 0) or 0,
                "rule_audit_tags": res.get("rule_audit_tags", []),
            }
            log_batch_trade(summary_trade, translation_ctx)
    except Exception:
        pass  # logging must never block batch response

    # Generate top trade recommendations (from potentially re-ranked results)
    top = generate_top_trades(results)

    return jsonify({
        "ok": True,
        "results": results,
        "top_trades": top["top_trades"],
        "top_trades_formatted": top["formatted"],
        "translation": translation,
        "translation_trace": trace_text,
        "errors": errors,
    })


# ── trade generator logic ───────────────────────────────────────────

_EXEC_RULES: dict[str, dict[str, Any]] = {
    "CONVEX": {
        "dte": "45-75 DTE",
        "strike": "ATM",
        "spread": False,
        "expiry_note": "Monthly cycle preferred",
    },
    "TACTICAL": {
        "dte": "14-21 DTE",
        "strike": "ATM / ATM+2",
        "spread": True,
        "expiry_note": "Weekly or bi-weekly cycle",
    },
    "HYBRID": {
        "dte": "21-45 DTE",
        "strike": "ATM / ATM+2",
        "spread": True,
        "expiry_note": "2-4 week cycle",
    },
    "MEAN_REVERSION": {
        "dte": "30-60 DTE",
        "strike": "ATM",
        "spread": False,
        "expiry_note": "Monthly cycle, theta buffer",
    },
}


@app.route("/api/generate-exec", methods=["POST"])
def api_generate_exec():
    """Turn top-trade recs into execution-ready trade specs."""
    data = request.get_json(silent=True) or {}
    top_trades = data.get("top_trades", [])

    specs = []
    for t in top_trades:
        mode = t.get("mode", "TACTICAL")
        rule = _EXEC_RULES.get(mode, _EXEC_RULES["TACTICAL"])
        direction = t.get("direction", "LONG")

        if rule["spread"]:
            action = "Buy Call Spread" if direction == "LONG" else "Buy Put Spread"
        else:
            action = "Buy Call" if direction == "LONG" else "Buy Put"

        specs.append({
            "ticker": t.get("ticker"),
            "direction": direction,
            "mode": mode,
            "action": action,
            "dte": rule["dte"],
            "strike": rule["strike"],
            "spread": rule["spread"],
            "expiry_note": rule["expiry_note"],
            "sizing": t.get("sizing", "MEDIUM"),
            "hold": t.get("hold", "2d"),
            "win_rate": t.get("win_rate", 0),
            "weighted_return": t.get("weighted_return", 0),
            "strength": t.get("strength", "LOW"),
        })

    return jsonify({"ok": True, "specs": specs})


@app.route("/api/trade-dialogue", methods=["POST"])
def api_trade_dialogue():
    """Generate operator-friendly reasoning narratives for top trades."""
    data = request.get_json(silent=True) or {}
    top_trades = data.get("top_trades", [])
    try:
        dialogues = generate_dialogue(top_trades)
        return jsonify({"ok": True, "dialogues": dialogues})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "trace": traceback.format_exc()}), 500


# ── decision measurement endpoints ──────────────────────────────────

@app.route("/api/decision-report", methods=["GET"])
def api_decision_report():
    """Decision vs outcome measurement report."""
    try:
        from monos_engine.translation.decision_vs_outcome_report import generate_full_report
        report = generate_full_report()
        return jsonify({"ok": True, **report})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "trace": traceback.format_exc()}), 500


# ── translation layer endpoints ─────────────────────────────────────

@app.route("/api/translation/status", methods=["GET"])
def api_translation_status():
    """Get current translation layer status: registry, active rules, states."""
    try:
        registry = get_registry_summary()
        active = get_active_rules()
        states = get_all_rule_states()
        evidence = load_evidence()
        return jsonify({
            "ok": True,
            "registry": registry,
            "active_rules": active,
            "rule_states": states,
            "evidence_count": len(evidence),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/translation/kill", methods=["POST"])
def api_translation_kill():
    """Kill a rule immediately."""
    data = request.get_json(silent=True) or {}
    rule_id = data.get("rule_id", "")
    reason = data.get("reason", "Operator kill")
    if not rule_id:
        return jsonify({"ok": False, "error": "rule_id required"}), 400
    result = kill_rule(rule_id, reason)
    return jsonify({"ok": True, **result})


@app.route("/api/translation/revive", methods=["POST"])
def api_translation_revive():
    """Revive a killed rule."""
    data = request.get_json(silent=True) or {}
    rule_id = data.get("rule_id", "")
    reason = data.get("reason", "Operator revive")
    result = revive_rule(rule_id, reason)
    return jsonify({"ok": True, **result})


@app.route("/api/translation/kill-all", methods=["POST"])
def api_translation_kill_all():
    """Emergency: kill all active rules."""
    data = request.get_json(silent=True) or {}
    reason = data.get("reason", "Emergency kill-all")
    count = kill_all(reason)
    return jsonify({"ok": True, "killed": count, "reason": reason})


@app.route("/api/translation/set-influence", methods=["POST"])
def api_translation_set_influence():
    """Manually override a rule's influence weight."""
    data = request.get_json(silent=True) or {}
    rule_id = data.get("rule_id", "")
    weight = data.get("weight")
    reason = data.get("reason", "Manual override")
    result = set_manual_influence(rule_id, weight, reason)
    return jsonify({"ok": True, **result})


# ── live options data endpoints ─────────────────────────────────────

@app.route("/api/options/prefill", methods=["POST"])
def api_options_prefill():
    """Pick the best contract for a trade and return prefilled data."""
    data = request.get_json(silent=True) or {}
    ticker = (data.get("ticker") or "").upper()
    mode = data.get("mode", "TACTICAL")
    direction = data.get("direction", "LONG")
    structure = data.get("structure", "LONG_CALL")

    try:
        contract = pick_best_contract(ticker, mode, direction, structure)
        if not contract:
            return jsonify({"ok": True, "contract": None, "message": "No contracts found"})

        return jsonify({"ok": True, "contract": contract})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/options/expirations", methods=["POST"])
def api_options_expirations():
    """Get available expirations for a ticker + mode."""
    data = request.get_json(silent=True) or {}
    ticker = (data.get("ticker") or "").upper()
    mode = data.get("mode", "TACTICAL")
    try:
        exps = get_expirations(ticker, mode=mode)
        return jsonify({"ok": True, "expirations": exps})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/options/chain", methods=["POST"])
def api_options_chain():
    """Get options chain for ticker + expiration."""
    data = request.get_json(silent=True) or {}
    ticker = (data.get("ticker") or "").upper()
    expiration = data.get("expiration", "")
    direction = data.get("direction", "LONG")
    structure = data.get("structure", "LONG_CALL")
    try:
        chain = get_chain(ticker, expiration, direction=direction, structure=structure)
        return jsonify({"ok": True, "chain": chain})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/options/quote", methods=["POST"])
def api_options_quote():
    """Get live quote for a specific contract symbol."""
    data = request.get_json(silent=True) or {}
    symbol = data.get("contract_symbol", "")
    try:
        quote = get_quote(symbol)
        return jsonify({"ok": True, "quote": quote})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


# ── persistent ledger (local JSON + Supabase dual-write) ────────────

from monos_engine.db.ledger_store import (
    load_local, save_local,
    sync_add, sync_close,
    is_supabase_available,
)

_ledger, _ledger_id = load_local()


def _save_ledger() -> None:
    save_local(_ledger, _ledger_id)


@app.route("/api/ledger", methods=["GET"])
def api_ledger_list():
    """Return current ledger."""
    return jsonify({
        "ok": True,
        "ledger": _ledger,
        "supabase_connected": is_supabase_available(),
    })


@app.route("/api/ledger/add", methods=["POST"])
def api_ledger_add():
    """Add a trade to the ledger with full pricing capture."""
    global _ledger_id
    data = request.get_json(silent=True) or {}
    _ledger_id += 1
    entry = {
        "id": _ledger_id,
        "date_open": data.get("date_open") or data.get("date", ""),
        "date_close": None,
        "ticker": (data.get("ticker") or "").upper(),
        "mode": data.get("mode", ""),
        "structure": data.get("structure", ""),
        "direction": data.get("direction", ""),
        "contract_symbol": data.get("contract_symbol", ""),
        "expiration": data.get("expiration", ""),
        "strike": data.get("strike", ""),
        "strike_delta": data.get("strike_delta"),
        "moneyness_pct": data.get("moneyness_pct"),
        "strike_candidates": data.get("strike_candidates"),
        "contracts": data.get("contracts", 1),
        "hold_days": data.get("hold_days"),
        "confidence": data.get("confidence"),
        "msa_state": data.get("msa_state", ""),
        "expected_return": data.get("expected_return"),
        # Open pricing
        "quoted_bid_open": data.get("quoted_bid_open"),
        "quoted_ask_open": data.get("quoted_ask_open"),
        "quoted_mid_open": data.get("quoted_mid_open"),
        "suggested_entry_price": data.get("suggested_entry_price"),
        "actual_entry_price": data.get("actual_entry_price") or data.get("entry_price"),
        # Close pricing (filled on close)
        "quoted_bid_close": None,
        "quoted_ask_close": None,
        "quoted_mid_close": None,
        "suggested_exit_price": None,
        "actual_exit_price": None,
        # Results
        "realized_pnl": None,
        "realized_return_pct": None,
        "slippage_open": None,
        "slippage_close": None,
        "win": None,
        "notes": data.get("notes", ""),
        "close_notes": None,
        "status": "OPEN",
    }
    # Compute slippage at open
    mid_open = entry.get("quoted_mid_open")
    actual_open = entry.get("actual_entry_price")
    if mid_open and actual_open:
        entry["slippage_open"] = round(actual_open - mid_open, 4)

    _ledger.append(entry)
    _save_ledger()
    sb_ok = sync_add(entry)
    return jsonify({"ok": True, "entry": entry, "supabase_synced": sb_ok})


@app.route("/api/ledger/close", methods=["POST"])
def api_ledger_close():
    """Close a trade with full exit pricing capture."""
    data = request.get_json(silent=True) or {}
    trade_id = data.get("id")
    date_close = data.get("date_close", "")

    for entry in _ledger:
        if entry["id"] == trade_id:
            entry["date_close"] = date_close
            entry["quoted_bid_close"] = data.get("quoted_bid_close")
            entry["quoted_ask_close"] = data.get("quoted_ask_close")
            entry["quoted_mid_close"] = data.get("quoted_mid_close")
            entry["suggested_exit_price"] = data.get("suggested_exit_price")
            entry["actual_exit_price"] = data.get("actual_exit_price") or data.get("exit_price")
            entry["close_notes"] = data.get("close_notes", "")

            actual_exit = entry["actual_exit_price"]
            actual_entry = entry.get("actual_entry_price")

            # Auto-compute return
            if actual_exit is not None and actual_entry and actual_entry > 0:
                entry["realized_return_pct"] = round(
                    ((actual_exit - actual_entry) / actual_entry) * 100, 4
                )

            # Compute PnL: (exit - entry) * contracts * 100
            if actual_exit is not None and actual_entry:
                contracts = entry.get("contracts", 1) or 1
                entry["realized_pnl"] = round(
                    (actual_exit - actual_entry) * contracts * 100, 2
                )

            # Slippage at close
            mid_close = entry.get("quoted_mid_close")
            if mid_close and actual_exit:
                entry["slippage_close"] = round(actual_exit - mid_close, 4)

            entry["win"] = (entry.get("realized_return_pct") or 0) > 0 if entry.get("realized_return_pct") is not None else None
            entry["status"] = "CLOSED"

            # Capture exit decision context at close time
            entry["exit_decision"] = data.get("exit_decision") or entry.get("exit_state_at_close")
            entry["exit_urgency_at_close"] = data.get("exit_urgency")
            entry["exit_rule_tags_at_close"] = data.get("exit_rule_tags")

            # Compute optimal outcome: what would have happened if held longer?
            # This gets filled by the decision-quality analysis later
            # For now, store the decision timing data
            exp_ret = entry.get("expected_return")
            act_ret = entry.get("realized_return_pct")
            if exp_ret is not None and act_ret is not None:
                entry["return_gap"] = round(act_ret - exp_ret, 4)
                # Did we exit at the right time?
                mode = entry.get("mode", "TACTICAL")
                tp = {"TACTICAL": 1.5, "HYBRID": 2.0, "CONVEX": 5.0, "MEAN_REVERSION": 2.0}.get(mode, 1.5)
                sl = {"TACTICAL": 1.0, "HYBRID": 1.5, "CONVEX": 2.5, "MEAN_REVERSION": 1.0}.get(mode, 1.0)

                if act_ret >= tp:
                    entry["exit_quality"] = "OPTIMAL"  # hit TP
                elif act_ret > 0 and act_ret < tp:
                    entry["exit_quality"] = "EARLY"    # profitable but left money on table
                elif act_ret <= -sl:
                    entry["exit_quality"] = "STOPPED"  # hit SL
                elif act_ret < 0 and act_ret > -sl:
                    entry["exit_quality"] = "LATE"     # lost money, should have exited sooner
                else:
                    entry["exit_quality"] = "NEUTRAL"
            else:
                entry["return_gap"] = None
                entry["exit_quality"] = None

            # Backward compat
            entry["exit_price"] = actual_exit
            entry["entry_price"] = actual_entry
            entry["actual_return"] = entry["realized_return_pct"]
            entry["pnl"] = entry["realized_pnl"]

            # Log decision for measurement — ensure scores are never blank
            try:
                # Compute fallback score from realized return if no translation data
                fallback_score = entry.get("realized_return_pct", 0) or 0
                translation_ctx = {
                    "original_score": entry.get("original_score") or entry.get("expected_return") or fallback_score,
                    "adjusted_score": entry.get("adjusted_score") or fallback_score,
                    "total_boost": entry.get("total_boost", 0) or 0,
                    "rule_audit_tags": entry.get("rule_audit_tags", []),
                    "original_structure": entry.get("original_structure"),
                }
                log_decision(entry, translation_ctx)
            except Exception:
                pass  # decision logging must never block trade close

            _save_ledger()
            sb_ok = sync_close(entry)
            return jsonify({"ok": True, "entry": entry, "supabase_synced": sb_ok})

    return jsonify({"ok": False, "error": f"Trade #{trade_id} not found"}), 404


@app.route("/api/ledger/partial-close", methods=["POST"])
def api_ledger_partial_close():
    """Partially close a trade — reduce contracts, log the closed portion."""
    global _ledger_id
    data = request.get_json(silent=True) or {}
    trade_id = data.get("id")
    close_contracts = data.get("close_contracts", 1)
    exit_price = data.get("exit_price")
    date_close = data.get("date_close", "")

    for entry in _ledger:
        if entry["id"] == trade_id and entry.get("status") == "OPEN":
            total_contracts = entry.get("contracts", 1) or 1

            if close_contracts >= total_contracts:
                return jsonify({"ok": False, "error": f"Cannot partial close {close_contracts} of {total_contracts} contracts. Use full close instead."}), 400

            # Create a new CLOSED entry for the closed portion
            _ledger_id += 1
            closed_portion = dict(entry)
            closed_portion["id"] = _ledger_id
            closed_portion["contracts"] = close_contracts
            closed_portion["actual_exit_price"] = exit_price
            closed_portion["exit_price"] = exit_price
            closed_portion["date_close"] = date_close
            closed_portion["status"] = "CLOSED"
            closed_portion["close_notes"] = f"Partial close from trade #{trade_id}"

            actual_entry = entry.get("actual_entry_price") or 0
            if exit_price is not None and actual_entry and actual_entry > 0:
                closed_portion["realized_return_pct"] = round(
                    ((exit_price - actual_entry) / actual_entry) * 100, 4
                )
                closed_portion["realized_pnl"] = round(
                    (exit_price - actual_entry) * close_contracts * 100, 2
                )
                closed_portion["actual_return"] = closed_portion["realized_return_pct"]
                closed_portion["pnl"] = closed_portion["realized_pnl"]
                closed_portion["win"] = closed_portion["realized_return_pct"] > 0

            _ledger.append(closed_portion)

            # Reduce contracts on the original trade
            entry["contracts"] = total_contracts - close_contracts
            entry["notes"] = (entry.get("notes", "") + f" | Partial closed {close_contracts} ctrs @ ${exit_price}").strip(" |")

            _save_ledger()
            sync_add(closed_portion)
            sync_close(closed_portion)

            return jsonify({
                "ok": True,
                "closed_portion": closed_portion,
                "remaining": entry,
                "supabase_synced": is_supabase_available(),
            })

    return jsonify({"ok": False, "error": f"Open trade #{trade_id} not found"}), 404


@app.route("/api/ledger/stats", methods=["GET"])
def api_ledger_stats():
    """Compute performance summary from ledger."""
    closed = [e for e in _ledger if e["status"] == "CLOSED"]
    total = len(closed)
    wins = sum(1 for e in closed if e.get("win"))
    win_rate = round((wins / total) * 100, 1) if total > 0 else 0
    returns = [e["actual_return"] for e in closed if e.get("actual_return") is not None]
    avg_ret = round(sum(returns) / len(returns), 4) if returns else 0

    # By mode
    modes = {}
    for e in closed:
        m = e.get("mode", "UNKNOWN")
        if m not in modes:
            modes[m] = {"trades": 0, "wins": 0, "returns": []}
        modes[m]["trades"] += 1
        if e.get("win"):
            modes[m]["wins"] += 1
        if e.get("actual_return") is not None:
            modes[m]["returns"].append(e["actual_return"])

    mode_stats = {}
    for m, v in modes.items():
        mode_stats[m] = {
            "trades": v["trades"],
            "win_rate": round((v["wins"] / v["trades"]) * 100, 1) if v["trades"] else 0,
            "avg_return": round(sum(v["returns"]) / len(v["returns"]), 4) if v["returns"] else 0,
        }

    # Best / worst ticker
    tickers: dict[str, list[float]] = {}
    for e in closed:
        t = e.get("ticker", "?")
        if t not in tickers:
            tickers[t] = []
        if e.get("actual_return") is not None:
            tickers[t].append(e["actual_return"])

    ticker_avg = {t: round(sum(r) / len(r), 4) for t, r in tickers.items() if r}
    best_ticker = max(ticker_avg, key=ticker_avg.get) if ticker_avg else None
    worst_ticker = min(ticker_avg, key=ticker_avg.get) if ticker_avg else None
    best_mode = max(mode_stats, key=lambda m: mode_stats[m]["avg_return"]) if mode_stats else None
    worst_mode = min(mode_stats, key=lambda m: mode_stats[m]["avg_return"]) if mode_stats else None

    # Slippage diagnostics
    open_count = sum(1 for e in _ledger if e["status"] == "OPEN")
    slips_open = [e["slippage_open"] for e in closed if e.get("slippage_open") is not None]
    slips_close = [e["slippage_close"] for e in closed if e.get("slippage_close") is not None]
    avg_slip_open = round(sum(slips_open) / len(slips_open), 4) if slips_open else None
    avg_slip_close = round(sum(slips_close) / len(slips_close), 4) if slips_close else None

    # Expected vs actual
    exp_vs_act = []
    for e in closed:
        exp = e.get("expected_return")
        act = e.get("realized_return_pct") or e.get("actual_return")
        if exp is not None and act is not None:
            exp_vs_act.append(round(act - exp, 4))
    avg_exp_vs_act = round(sum(exp_vs_act) / len(exp_vs_act), 4) if exp_vs_act else None

    return jsonify({
        "ok": True,
        "total_trades": total,
        "open_trades": open_count,
        "closed_trades": total,
        "win_rate": win_rate,
        "avg_return": avg_ret,
        "mode_stats": mode_stats,
        "best_ticker": best_ticker,
        "worst_ticker": worst_ticker,
        "best_mode": best_mode,
        "worst_mode": worst_mode,
        "avg_slippage_open": avg_slip_open,
        "avg_slippage_close": avg_slip_close,
        "avg_expected_vs_actual": avg_exp_vs_act,
    })


# ── expected vs actual analysis ──────────────────────────────────────

@app.route("/api/ledger/eva", methods=["GET"])
def api_ledger_eva():
    """Expected vs Actual analysis on closed trades."""
    closed = [e for e in _ledger if e["status"] == "CLOSED"]

    # Build trade-level detail
    trades = []
    for e in closed:
        exp = e.get("expected_return")
        act = e.get("realized_return_pct") or e.get("actual_return")
        gap = round(act - exp, 4) if (exp is not None and act is not None) else None
        trades.append({
            "id": e["id"],
            "ticker": e.get("ticker", ""),
            "mode": e.get("mode", ""),
            "structure": e.get("structure", ""),
            "direction": e.get("direction", ""),
            "expected_return": exp,
            "actual_return": act,
            "gap": gap,
            "win": e.get("win"),
        })

    # Overall stats
    total = len(trades)
    exp_vals = [t["expected_return"] for t in trades if t["expected_return"] is not None]
    act_vals = [t["actual_return"] for t in trades if t["actual_return"] is not None]
    gap_vals = [t["gap"] for t in trades if t["gap"] is not None]
    wins = sum(1 for t in trades if t.get("win"))

    avg_expected = round(sum(exp_vals) / len(exp_vals), 4) if exp_vals else None
    avg_actual = round(sum(act_vals) / len(act_vals), 4) if act_vals else None
    avg_gap = round(sum(gap_vals) / len(gap_vals), 4) if gap_vals else None
    win_rate = round((wins / total) * 100, 1) if total > 0 else 0

    # Trades that beat expectations
    beat_count = sum(1 for t in trades if t["gap"] is not None and t["gap"] > 0)
    beat_rate = round((beat_count / total) * 100, 1) if total > 0 else 0

    # By mode
    mode_map: dict[str, dict] = {}
    for t in trades:
        m = t.get("mode", "UNKNOWN")
        if m not in mode_map:
            mode_map[m] = {"trades": 0, "wins": 0, "expected": [], "actual": [], "gaps": []}
        mode_map[m]["trades"] += 1
        if t.get("win"):
            mode_map[m]["wins"] += 1
        if t["expected_return"] is not None:
            mode_map[m]["expected"].append(t["expected_return"])
        if t["actual_return"] is not None:
            mode_map[m]["actual"].append(t["actual_return"])
        if t["gap"] is not None:
            mode_map[m]["gaps"].append(t["gap"])

    mode_stats = {}
    for m, v in mode_map.items():
        avg_e = round(sum(v["expected"]) / len(v["expected"]), 4) if v["expected"] else None
        avg_a = round(sum(v["actual"]) / len(v["actual"]), 4) if v["actual"] else None
        avg_g = round(sum(v["gaps"]) / len(v["gaps"]), 4) if v["gaps"] else None
        total_ret = round(sum(v["actual"]), 4) if v["actual"] else 0
        wr = round((v["wins"] / v["trades"]) * 100, 1) if v["trades"] else 0

        # Rating
        if avg_a is not None and avg_a > 3 and wr >= 60:
            rating = "STRONG"
        elif avg_a is not None and avg_a > 0:
            rating = "OK"
        else:
            rating = "WEAK"

        mode_stats[m] = {
            "trades": v["trades"],
            "avg_expected": avg_e,
            "avg_actual": avg_a,
            "avg_gap": avg_g,
            "total_return": total_ret,
            "win_rate": wr,
            "rating": rating,
        }

    # Best / worst mode by avg actual return
    best_mode = max(mode_stats, key=lambda m: mode_stats[m]["avg_actual"] or -999) if mode_stats else None
    worst_mode = min(mode_stats, key=lambda m: mode_stats[m]["avg_actual"] or 999) if mode_stats else None

    return jsonify({
        "ok": True,
        "total": total,
        "avg_expected": avg_expected,
        "avg_actual": avg_actual,
        "avg_gap": avg_gap,
        "win_rate": win_rate,
        "beat_rate": beat_rate,
        "best_mode": best_mode,
        "worst_mode": worst_mode,
        "mode_stats": mode_stats,
        "trades": trades,
    })


# ── decision quality analysis ────────────────────────────────────────

@app.route("/api/ledger/decision-quality", methods=["GET"])
def api_ledger_decision_quality():
    """Analyze exit decision quality across all closed trades."""
    closed = [e for e in _ledger if e["status"] == "CLOSED"]

    trades = []
    for e in closed:
        exp = e.get("expected_return")
        act = e.get("realized_return_pct") or e.get("actual_return")
        gap = e.get("return_gap")
        if gap is None and exp is not None and act is not None:
            gap = round(act - exp, 4)

        quality = e.get("exit_quality")
        if quality is None and exp is not None and act is not None:
            mode = e.get("mode", "TACTICAL")
            tp = {"TACTICAL": 1.5, "HYBRID": 2.0, "CONVEX": 5.0, "MEAN_REVERSION": 2.0}.get(mode, 1.5)
            sl = {"TACTICAL": 1.0, "HYBRID": 1.5, "CONVEX": 2.5, "MEAN_REVERSION": 1.0}.get(mode, 1.0)
            if act >= tp:
                quality = "OPTIMAL"
            elif act > 0 and act < tp:
                quality = "EARLY"
            elif act <= -sl:
                quality = "STOPPED"
            elif act < 0 and act > -sl:
                quality = "LATE"
            else:
                quality = "NEUTRAL"

        trades.append({
            "id": e["id"],
            "ticker": e.get("ticker", ""),
            "mode": e.get("mode", ""),
            "direction": e.get("direction", ""),
            "structure": e.get("structure", ""),
            "expected_return": exp,
            "actual_return": act,
            "return_gap": gap,
            "exit_decision": e.get("exit_decision", ""),
            "exit_quality": quality,
            "win": e.get("win"),
            "days_held": None,
        })
        # Compute days held if dates present
        d_open = e.get("date_open", "")
        d_close = e.get("date_close", "")
        if d_open and d_close:
            try:
                from datetime import datetime as _ddt
                days = (_ddt.strptime(str(d_close), "%Y-%m-%d") - _ddt.strptime(str(d_open), "%Y-%m-%d")).days
                trades[-1]["days_held"] = days
            except Exception:
                pass

    total = len(trades)
    if total == 0:
        return jsonify({"ok": True, "total": 0, "trades": [], "quality_distribution": {},
                         "mode_quality": {}, "accuracy": {}, "formatted": ""})

    # Quality distribution
    q_dist = {}
    for t in trades:
        q = t["exit_quality"] or "UNKNOWN"
        q_dist[q] = q_dist.get(q, 0) + 1

    # Accuracy metrics
    wins = sum(1 for t in trades if t.get("win"))
    win_rate = round((wins / total) * 100, 1)

    optimal_count = q_dist.get("OPTIMAL", 0)
    early_count = q_dist.get("EARLY", 0)
    late_count = q_dist.get("LATE", 0)
    stopped_count = q_dist.get("STOPPED", 0)

    # "Good decisions": OPTIMAL + trades where we cut loss correctly (STOPPED when losing)
    good_exits = optimal_count + stopped_count
    good_rate = round((good_exits / total) * 100, 1) if total else 0

    # "Precision": OPTIMAL / (OPTIMAL + EARLY) — how often we exit at the right time when winning
    winners = optimal_count + early_count
    exit_precision = round((optimal_count / winners) * 100, 1) if winners else 0

    # "Discipline": STOPPED / (STOPPED + LATE) — how often we cut loss vs hold too long
    losers = stopped_count + late_count
    exit_discipline = round((stopped_count / losers) * 100, 1) if losers else 0

    # Expected vs actual gap stats
    gaps = [t["return_gap"] for t in trades if t["return_gap"] is not None]
    avg_gap = round(sum(gaps) / len(gaps), 4) if gaps else None
    beat_count = sum(1 for g in gaps if g > 0)
    beat_rate = round((beat_count / len(gaps)) * 100, 1) if gaps else 0

    accuracy = {
        "win_rate": win_rate,
        "good_exit_rate": good_rate,
        "exit_precision": exit_precision,
        "exit_discipline": exit_discipline,
        "avg_return_gap": avg_gap,
        "beat_rate": beat_rate,
    }

    # Mode-level quality
    mode_quality = {}
    for t in trades:
        m = t.get("mode", "UNKNOWN")
        if m not in mode_quality:
            mode_quality[m] = {"trades": 0, "wins": 0, "optimal": 0, "early": 0,
                                "late": 0, "stopped": 0, "gaps": [], "returns": []}
        mq = mode_quality[m]
        mq["trades"] += 1
        if t.get("win"):
            mq["wins"] += 1
        q = t["exit_quality"] or ""
        if q == "OPTIMAL":
            mq["optimal"] += 1
        elif q == "EARLY":
            mq["early"] += 1
        elif q == "LATE":
            mq["late"] += 1
        elif q == "STOPPED":
            mq["stopped"] += 1
        if t["return_gap"] is not None:
            mq["gaps"].append(t["return_gap"])
        if t["actual_return"] is not None:
            mq["returns"].append(t["actual_return"])

    mode_summary = {}
    for m, mq in mode_quality.items():
        mode_summary[m] = {
            "trades": mq["trades"],
            "win_rate": round((mq["wins"] / mq["trades"]) * 100, 1) if mq["trades"] else 0,
            "optimal": mq["optimal"],
            "early": mq["early"],
            "late": mq["late"],
            "stopped": mq["stopped"],
            "avg_gap": round(sum(mq["gaps"]) / len(mq["gaps"]), 4) if mq["gaps"] else None,
            "avg_return": round(sum(mq["returns"]) / len(mq["returns"]), 4) if mq["returns"] else None,
            "precision": round((mq["optimal"] / (mq["optimal"] + mq["early"])) * 100, 1) if (mq["optimal"] + mq["early"]) else 0,
            "discipline": round((mq["stopped"] / (mq["stopped"] + mq["late"])) * 100, 1) if (mq["stopped"] + mq["late"]) else 0,
        }

    # Formatted text
    lines = ["=== MONOS DECISION QUALITY REPORT ===", ""]
    lines.append(f"Closed Trades: {total}")
    lines.append(f"Win Rate: {win_rate}%")
    lines.append(f"Good Exit Rate: {good_rate}% (optimal + correct stops)")
    lines.append(f"Exit Precision: {exit_precision}% (optimal / all winners)")
    lines.append(f"Exit Discipline: {exit_discipline}% (stops / all losers)")
    lines.append(f"Beat Rate: {beat_rate}% (actual > expected)")
    lines.append(f"Avg Gap: {avg_gap}%" if avg_gap is not None else "Avg Gap: —")
    lines.append("")
    lines.append("Quality Distribution:")
    for q, cnt in sorted(q_dist.items()):
        pct = round((cnt / total) * 100, 1)
        lines.append(f"  {q}: {cnt} ({pct}%)")
    lines.append("")
    lines.append("Mode Quality:")
    for m, ms in mode_summary.items():
        lines.append(f"  {m}: {ms['trades']}t WR:{ms['win_rate']}% Opt:{ms['optimal']} Early:{ms['early']} Late:{ms['late']} Stop:{ms['stopped']} Prec:{ms['precision']}% Disc:{ms['discipline']}%")
    lines.append("")
    lines.append("Trades:")
    for t in trades:
        g = f"{t['return_gap']:+.1f}%" if t["return_gap"] is not None else "—"
        lines.append(f"  #{t['id']} {t['ticker']:4s} {t['mode']:16s} Exp:{t['expected_return']}% Act:{t['actual_return']}% Gap:{g} Q:{t['exit_quality'] or '—'} {'W' if t['win'] else 'L'}")
    lines.append("")
    lines.append("Generated by MONOS Conviction Engine")

    return jsonify({
        "ok": True,
        "total": total,
        "trades": trades,
        "quality_distribution": q_dist,
        "accuracy": accuracy,
        "mode_quality": mode_summary,
        "formatted": "\n".join(lines),
    })


# ── open PnL monitor ────────────────────────────────────────────────

from datetime import datetime as _dt


@app.route("/api/ledger/open-pnl", methods=["GET"])
def api_ledger_open_pnl():
    """Live PnL for all open trades.  Fetches current bid/ask per contract."""
    try:
        from monos_engine.mode.mode_engine import get_mode_config as _get_mode_cfg
    except Exception:
        _get_mode_cfg = None

    try:
        open_trades = [e for e in _ledger if e.get("status") == "OPEN"]

        rows = []
        for e in open_trades:
            entry_price = e.get("actual_entry_price") or e.get("entry_price") or 0
            contract_sym = e.get("contract_symbol", "")
            mode = e.get("mode", "TACTICAL")
            contracts = e.get("contracts", 1) or 1

            # Fetch live quote
            current_bid, current_ask, current_mid = 0.0, 0.0, 0.0
            if contract_sym:
                try:
                    q = get_quote(contract_sym)
                    if q:
                        current_bid = float(q.get("bid", 0) or 0)
                        current_ask = float(q.get("ask", 0) or 0)
                        current_mid = float(q.get("mid", 0) or 0)
                except Exception:
                    pass

            # Fallback: no live data
            if current_mid == 0 and not contract_sym:
                current_mid = float(entry_price) if entry_price else 0.0

            entry_price = float(entry_price) if entry_price else 0.0

            # Unrealized PnL
            unrealized_pnl = round((current_mid - entry_price) * contracts * 100, 2) if entry_price else 0.0
            unrealized_return_pct = (
                round(((current_mid - entry_price) / entry_price) * 100, 4)
                if entry_price > 0 else 0.0
            )

            # Days held
            days_held = 0
            date_open = e.get("date_open", "")
            if date_open:
                try:
                    opened = _dt.strptime(str(date_open), "%Y-%m-%d").date()
                    days_held = (_dt.now().date() - opened).days
                except Exception:
                    pass

            # Mode config for TP/SL thresholds
            tp_target, sl_target, hold_target = 1.5, 1.0, 2
            if _get_mode_cfg:
                try:
                    cfg = _get_mode_cfg(mode)
                    tp_target = cfg.get("take_profit", 1.5)
                    sl_target = cfg.get("stop_loss", 1.0)
                    hold_target = cfg.get("hold_days", 2)
                except Exception:
                    pass

            distance_to_tp = round(tp_target - unrealized_return_pct, 4)
            distance_to_sl = round(unrealized_return_pct + sl_target, 4)

            # ── EXIT ENGINE ──────────────────────────────────────
            ret = unrealized_return_pct

            if mode == "TACTICAL":
                if ret >= 0.8 * tp_target:
                    exit_state = "TAKE_PROFIT"
                elif ret <= -(0.8 * sl_target):
                    exit_state = "CUT_LOSS"
                elif days_held >= hold_target:
                    exit_state = "TIME_EXIT"
                else:
                    exit_state = "HOLD"
            elif mode == "CONVEX":
                if ret >= 2 * tp_target:
                    exit_state = "SCALE_OUT"
                elif ret >= 0.8 * tp_target:
                    exit_state = "TAKE_PROFIT"
                elif ret <= -sl_target:
                    exit_state = "CUT_LOSS"
                else:
                    exit_state = "HOLD"
            elif mode == "MEAN_REVERSION":
                if ret > 0:
                    exit_state = "TAKE_PROFIT"
                elif days_held > 2:
                    exit_state = "TIME_EXIT"
                else:
                    exit_state = "HOLD"
            elif mode == "HYBRID":
                if ret >= 0.8 * tp_target:
                    exit_state = "TAKE_PROFIT"
                elif ret <= -(0.8 * sl_target):
                    exit_state = "CUT_LOSS"
                elif days_held >= hold_target:
                    exit_state = "TIME_EXIT"
                else:
                    exit_state = "HOLD"
            else:
                exit_state = "HOLD"

            # Health derived from exit_state
            if exit_state == "HOLD":
                health = "GREEN"
            elif exit_state in ("TAKE_PROFIT", "SCALE_OUT"):
                health = "YELLOW"
            else:
                health = "RED"

            # Suggested action text
            if exit_state == "TAKE_PROFIT":
                exit_action = "Close now — near take profit target"
            elif exit_state == "SCALE_OUT":
                exit_action = "Scale out 50% — 2x TP reached, let remainder run"
            elif exit_state == "CUT_LOSS":
                exit_action = "Close now — approaching stop loss"
            elif exit_state == "TIME_EXIT":
                exit_action = f"Close — held {days_held}d, exceeds {hold_target}d target"
            else:
                exit_action = "Hold position — within parameters"

            rows.append({
                "id": e["id"],
                "ticker": e.get("ticker", ""),
                "direction": e.get("direction", ""),
                "mode": mode,
                "structure": e.get("structure", ""),
                "contract_symbol": contract_sym,
                "strike": e.get("strike", ""),
                "expiration": e.get("expiration", ""),
                "entry_price": entry_price,
                "contracts": contracts,
                "current_bid": current_bid,
                "current_ask": current_ask,
                "current_mid": current_mid,
                "unrealized_pnl": unrealized_pnl,
                "unrealized_return_pct": unrealized_return_pct,
                "days_held": days_held,
                "hold_target": hold_target,
                "tp_target": tp_target,
                "sl_target": sl_target,
                "distance_to_tp": distance_to_tp,
                "distance_to_sl": distance_to_sl,
                "exit_state": exit_state,
                "exit_action": exit_action,
                "health": health,
                "confidence": e.get("confidence"),
                "expected_return": e.get("expected_return"),
            })

        # Aggregate
        total_unrealized = round(sum(r["unrealized_pnl"] for r in rows), 2)
        avg_return = round(
            sum(r["unrealized_return_pct"] for r in rows) / len(rows), 4
        ) if rows else 0

        # Run exit engine on each trade
        for row in rows:
            try:
                rec = get_exit_recommendation(row)
                row["exit_state"] = rec["exit_state"]
                row["exit_action"] = rec["action_text"]
                row["exit_urgency"] = rec["urgency"]
                row["exit_explanation"] = rec["explanation"]
                row["exit_rule_tags"] = rec["rule_tags"]
            except Exception:
                row.setdefault("exit_state", "HOLD")
                row.setdefault("exit_action", "Hold — engine error")
                row.setdefault("exit_urgency", "LOW")
                row.setdefault("exit_explanation", "")
                row.setdefault("exit_rule_tags", [])

        return jsonify({
            "ok": True,
            "trades": rows,
            "total_open": len(rows),
            "total_unrealized_pnl": total_unrealized,
            "avg_unrealized_return": avg_return,
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "trace": traceback.format_exc()}), 500


@app.route("/api/exit-recommendations", methods=["GET"])
def api_exit_recommendations():
    """Full exit recommendation report for all open trades.

    Calls the open-pnl logic internally, then runs the exit engine
    over every position.  Returns recommendations + summary + formatted text.
    """
    try:
        # Reuse the open-pnl data-gathering
        with app.test_request_context():
            pnl_resp = api_ledger_open_pnl()
            pnl_data = pnl_resp.get_json()

        if not pnl_data.get("ok"):
            return jsonify({"ok": False, "error": "Failed to load open PnL data"}), 500

        result = get_all_exit_recommendations(pnl_data.get("trades", []))
        return jsonify({"ok": True, **result})

    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "trace": traceback.format_exc()}), 500


# ── entry point ─────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n  MONOS Backtest Dashboard")
    print("  http://127.0.0.1:5000\n")
    app.run(debug=True, port=5000)
