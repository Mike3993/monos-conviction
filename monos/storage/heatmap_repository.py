"""
heatmap_repository.py

Persistence for scanner.heatmap_runs and scanner.heatmap_cells.
"""

import logging

from .supabase_client import get_client, write_agent_log

logger = logging.getLogger(__name__)

AGENT = "heatmap_repository"
T_RUNS  = "scanner_heatmap_runs"
T_CELLS = "scanner_heatmap_cells"


def write_heatmap_run(run_row: dict) -> str:
    """Insert a heatmap_runs row. Returns the heatmap_run_id."""
    sb = get_client()
    resp = sb.table(T_RUNS).insert(run_row).execute()
    run_id = resp.data[0]["heatmap_run_id"]
    logger.info("Heatmap run created: %s", run_id)
    write_agent_log(AGENT, "write_heatmap_run", "success",
                    {"heatmap_run_id": run_id})
    return run_id


def write_heatmap_cells(cells: list[dict]) -> int:
    """Bulk-insert heatmap_cells rows."""
    if not cells:
        return 0
    sb = get_client()
    sb.table(T_CELLS).insert(cells).execute()
    logger.info("Wrote %d heatmap cells", len(cells))
    write_agent_log(AGENT, "write_heatmap_cells", "success",
                    {"count": len(cells)})
    return len(cells)


def read_latest_heatmap() -> tuple[dict | None, list[dict]]:
    """Return the latest heatmap run and its cells."""
    sb = get_client()
    runs = (sb.table(T_RUNS).select("*")
            .order("created_at", desc=True).limit(1).execute().data)
    if not runs:
        return None, []
    run = runs[0]
    cells = (sb.table(T_CELLS).select("*")
             .eq("heatmap_run_id", run["heatmap_run_id"])
             .order("heat_score", desc=True).execute().data)
    return run, cells
