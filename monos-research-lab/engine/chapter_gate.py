"""
MONOS Research Lab — Chapter Gating Logic
-------------------------------------------
Controls the progression through Canon chapters.

Rules:
  - CH01 can be opened and tested immediately
  - CH02 opens only when CH01 is crystallized AND Tyler pack results received
  - CH03+ remain blocked by prior dependencies
  - Type C rules require Tyler validation — they cannot be crystallized
    from backtest data alone

This is governance enforcement. No shortcuts.
"""

from __future__ import annotations

import json
import os
from typing import Any


_CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config")


def load_chapters(path: str | None = None) -> list[dict[str, Any]]:
    """Load chapter definitions from config/chapters.json."""
    if path is None:
        path = os.path.join(_CONFIG_DIR, "chapters.json")
    with open(path, "r") as f:
        data = json.load(f)
    return data.get("chapters", [])


def can_open_chapter(
    chapter_id: str,
    chapters_state: dict[str, str],
    tyler_results_state: dict[str, bool] | None = None,
) -> dict[str, Any]:
    """Determine whether a chapter can be opened for testing.

    Parameters
    ----------
    chapter_id : str
        e.g. "CH02"
    chapters_state : dict[str, str]
        Current status of each chapter.
        e.g. {"CH01": "crystallized", "CH02": "blocked", ...}
    tyler_results_state : dict[str, bool] | None
        Whether Tyler pack results have been received per chapter.
        e.g. {"CH01": True, "CH02": False}

    Returns
    -------
    dict
        can_open: bool
        reason: str
        unmet_dependencies: list[str]
    """
    if tyler_results_state is None:
        tyler_results_state = {}

    chapters = load_chapters()
    chapter = None
    for ch in chapters:
        if ch["id"] == chapter_id:
            chapter = ch
            break

    if chapter is None:
        return {
            "can_open": False,
            "reason": f"Chapter {chapter_id} not found in config.",
            "unmet_dependencies": [],
        }

    # Check all dependencies are crystallized
    depends = chapter.get("depends_on", [])
    unmet = []
    for dep in depends:
        dep_status = chapters_state.get(dep, "blocked")
        if dep_status != "crystallized":
            unmet.append(f"{dep} (status: {dep_status})")

    # Check Tyler results for dependencies that require them
    tyler_unmet = []
    for dep in depends:
        dep_ch = next((c for c in chapters if c["id"] == dep), None)
        if dep_ch and dep_ch.get("tyler_required"):
            if not tyler_results_state.get(dep, False):
                tyler_unmet.append(f"{dep} (Tyler results pending)")

    all_unmet = unmet + tyler_unmet

    if all_unmet:
        return {
            "can_open": False,
            "reason": f"Chapter {chapter_id} blocked by: {', '.join(all_unmet)}",
            "unmet_dependencies": all_unmet,
        }

    return {
        "can_open": True,
        "reason": f"Chapter {chapter_id} dependencies satisfied. Ready for testing.",
        "unmet_dependencies": [],
    }


def get_chapter_status_report(
    chapters_state: dict[str, str],
    tyler_results_state: dict[str, bool] | None = None,
) -> list[dict[str, Any]]:
    """Generate a full status report for all chapters.

    Returns a list of chapter status dicts with can_open for each.
    """
    chapters = load_chapters()
    report = []
    for ch in chapters:
        cid = ch["id"]
        status = chapters_state.get(cid, ch.get("status", "blocked"))
        gate = can_open_chapter(cid, chapters_state, tyler_results_state)
        report.append({
            "id": cid,
            "name": ch["name"],
            "status": status,
            "can_open": gate["can_open"],
            "reason": gate["reason"],
            "depends_on": ch.get("depends_on", []),
            "tyler_required": ch.get("tyler_required", False),
            "rules": ch.get("rules", []),
        })
    return report
