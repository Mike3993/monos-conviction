"""
MONOS Decision vs Outcome Report
----------------------------------
Reads decision_log.csv and measures translation layer impact.

Output metrics:
  1. Overall: total trades, win rate, avg opt return
  2. By structure: original vs translated win rate + avg return
  3. Translation impact: changed vs unchanged structure
  4. By rule_id: win rate + avg return per matched rule
  5. By mode: Tactical vs Convex vs Mean Reversion

Pure measurement. No strategy modification. No scoring changes.
"""

from __future__ import annotations

import json
import os
from typing import Any

from monos_engine.translation.decision_logger import load_decision_log


def _wr(rows: list[dict]) -> float:
    if not rows:
        return 0.0
    wins = sum(1 for r in rows if r.get("win_flag") is True)
    return round((wins / len(rows)) * 100, 2)


def _avg(values: list) -> float | None:
    clean = [v for v in values if v is not None]
    return round(sum(clean) / len(clean), 4) if clean else None


# ── SECTION 1: Overall ──────────────────────────────────────────────

def section_overall(log: list[dict]) -> dict:
    total = len(log)
    wr = _wr(log)
    avg_ret = _avg([r.get("opt_return") for r in log])
    return {"total_trades": total, "win_rate": wr, "avg_opt_return": avg_ret}


# ── SECTION 2: By structure ─────────────────────────────────────────

def section_by_structure(log: list[dict]) -> dict:
    by_orig: dict[str, list] = {}
    by_trans: dict[str, list] = {}
    for r in log:
        o = r.get("original_structure", "") or ""
        t = r.get("translated_structure", "") or o
        if o:
            by_orig.setdefault(o, []).append(r)
        if t:
            by_trans.setdefault(t, []).append(r)

    result = {}
    for s in sorted(set(list(by_orig.keys()) + list(by_trans.keys()))):
        o_rows = by_orig.get(s, [])
        t_rows = by_trans.get(s, [])
        result[s] = {
            "original_count": len(o_rows),
            "original_wr": _wr(o_rows),
            "original_avg": _avg([r["opt_return"] for r in o_rows]),
            "translated_count": len(t_rows),
            "translated_wr": _wr(t_rows),
            "translated_avg": _avg([r["opt_return"] for r in t_rows]),
        }
    return result


# ── SECTION 3: Translation impact ──────────────────────────────────

def section_translation_impact(log: list[dict]) -> dict:
    changed = [r for r in log if r.get("structure_changed")]
    unchanged = [r for r in log if not r.get("structure_changed")]
    boosted = [r for r in log if (r.get("total_boost") or 0) > 0]
    unboosted = [r for r in log if (r.get("total_boost") or 0) == 0]

    return {
        "structure_changed_count": len(changed),
        "structure_changed_wr": _wr(changed),
        "structure_changed_avg": _avg([r["opt_return"] for r in changed]),
        "structure_unchanged_count": len(unchanged),
        "structure_unchanged_wr": _wr(unchanged),
        "structure_unchanged_avg": _avg([r["opt_return"] for r in unchanged]),
        "boosted_count": len(boosted),
        "boosted_wr": _wr(boosted),
        "boosted_avg": _avg([r["opt_return"] for r in boosted]),
        "unboosted_count": len(unboosted),
        "unboosted_wr": _wr(unboosted),
        "unboosted_avg": _avg([r["opt_return"] for r in unboosted]),
    }


# ── SECTION 4: By rule_id ──────────────────────────────────────────

def section_by_rule(log: list[dict]) -> dict:
    by_rule: dict[str, list] = {}
    for r in log:
        ids = (r.get("matched_rule_ids") or "").split("|")
        for rid in ids:
            rid = rid.strip()
            if rid:
                by_rule.setdefault(rid, []).append(r)
    result = {}
    for rid in sorted(by_rule.keys()):
        rows = by_rule[rid]
        result[rid] = {
            "trades": len(rows),
            "win_rate": _wr(rows),
            "avg_return": _avg([r["opt_return"] for r in rows]),
        }
    return result


# ── SECTION 5: By mode ─────────────────────────────────────────────

def section_by_mode(log: list[dict]) -> dict:
    by_mode: dict[str, list] = {}
    for r in log:
        m = r.get("mode", "UNKNOWN")
        by_mode.setdefault(m, []).append(r)
    result = {}
    for mode in sorted(by_mode.keys()):
        rows = by_mode[mode]
        result[mode] = {
            "trades": len(rows),
            "win_rate": _wr(rows),
            "avg_return": _avg([r["opt_return"] for r in rows]),
        }
    return result


# ── FORMAT ──────────────────────────────────────────────────────────

def _fmt(v, d=2):
    return f"{v:.{d}f}" if v is not None else "—"


def format_report(overall, by_structure, impact, by_rule, by_mode) -> str:
    lines = []
    w = 60

    lines.append("=" * w)
    lines.append("  MONOS DECISION vs OUTCOME REPORT")
    lines.append("=" * w)

    # 1. Overall
    lines.append("")
    lines.append("  1. OVERALL")
    lines.append(f"  {'Total Trades':<25} {overall['total_trades']}")
    lines.append(f"  {'Win Rate':<25} {overall['win_rate']}%")
    lines.append(f"  {'Avg Opt Return':<25} {_fmt(overall['avg_opt_return'])}%")

    # 2. By structure
    lines.append("")
    lines.append("  2. BY STRUCTURE")
    lines.append(f"  {'STRUCTURE':<16} {'ORIG#':>6} {'ORIG WR':>8} {'ORIG AVG':>9} {'TRANS#':>7} {'TRANS WR':>9} {'TRANS AVG':>10}")
    lines.append(f"  {'-'*66}")
    for s, d in by_structure.items():
        lines.append(
            f"  {s:<16} {d['original_count']:>6} {d['original_wr']:>7.1f}% {_fmt(d['original_avg']):>8}% "
            f"{d['translated_count']:>7} {d['translated_wr']:>8.1f}% {_fmt(d['translated_avg']):>9}%"
        )

    # 3. Translation impact
    lines.append("")
    lines.append("  3. TRANSLATION IMPACT")
    lines.append(f"  {'Struct Changed':<25} {impact['structure_changed_count']} trades  WR={impact['structure_changed_wr']}%  Avg={_fmt(impact['structure_changed_avg'])}%")
    lines.append(f"  {'Struct Unchanged':<25} {impact['structure_unchanged_count']} trades  WR={impact['structure_unchanged_wr']}%  Avg={_fmt(impact['structure_unchanged_avg'])}%")
    lines.append(f"  {'Score Boosted':<25} {impact['boosted_count']} trades  WR={impact['boosted_wr']}%  Avg={_fmt(impact['boosted_avg'])}%")
    lines.append(f"  {'Not Boosted':<25} {impact['unboosted_count']} trades  WR={impact['unboosted_wr']}%  Avg={_fmt(impact['unboosted_avg'])}%")

    # Delta
    if impact['boosted_avg'] is not None and impact['unboosted_avg'] is not None:
        delta = round(impact['boosted_avg'] - impact['unboosted_avg'], 4)
        lines.append(f"  {'DELTA (boost vs no)':<25} {delta:+.4f}%")

    # 4. By rule
    lines.append("")
    lines.append("  4. BY RULE")
    lines.append(f"  {'RULE_ID':<12} {'TRADES':>7} {'WIN RATE':>9} {'AVG RET':>9}")
    lines.append(f"  {'-'*40}")
    for rid, d in by_rule.items():
        lines.append(f"  {rid:<12} {d['trades']:>7} {d['win_rate']:>8.1f}% {_fmt(d['avg_return']):>8}%")

    # 5. By mode
    lines.append("")
    lines.append("  5. BY MODE")
    lines.append(f"  {'MODE':<20} {'TRADES':>7} {'WIN RATE':>9} {'AVG RET':>9}")
    lines.append(f"  {'-'*48}")
    for mode, d in by_mode.items():
        lines.append(f"  {mode:<20} {d['trades']:>7} {d['win_rate']:>8.1f}% {_fmt(d['avg_return']):>8}%")

    lines.append("")
    lines.append("=" * w)
    return "\n".join(lines)


# ── MAIN ────────────────────────────────────────────────────────────

def generate_full_report() -> dict[str, Any]:
    log = load_decision_log()
    if not log:
        return {
            "total": 0,
            "overall": {"total_trades": 0, "win_rate": 0, "avg_opt_return": None},
            "by_structure": {},
            "translation_impact": {},
            "by_rule": {},
            "by_mode": {},
            "formatted": "No decisions logged yet.",
        }

    overall = section_overall(log)
    by_structure = section_by_structure(log)
    impact = section_translation_impact(log)
    by_rule = section_by_rule(log)
    by_mode = section_by_mode(log)
    formatted = format_report(overall, by_structure, impact, by_rule, by_mode)

    return {
        "total": len(log),
        "overall": overall,
        "by_structure": by_structure,
        "translation_impact": impact,
        "by_rule": by_rule,
        "by_mode": by_mode,
        "formatted": formatted,
    }


def save_report_json(output_path: str | None = None) -> str:
    """Save report to JSON file. Returns the path."""
    report = generate_full_report()
    if output_path is None:
        output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "decision_report_summary.json")
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)
    return output_path


if __name__ == "__main__":
    report = generate_full_report()
    print(report["formatted"])
    path = save_report_json()
    print(f"\nSaved to: {path}")
