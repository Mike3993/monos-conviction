"""
gamma_exposure_engine.py

Computes Dealer Gamma Exposure (GEX) by joining position_legs with
greeks_snapshots, normalising null quantities and missing option types,
then aggregating net dealer gamma per strike.

Dealer gamma convention:
    - Customer long calls  → dealer is short gamma  (negative GEX)
    - Customer long puts   → dealer is long gamma   (positive GEX)
    - Customer short calls → dealer is long gamma   (positive GEX)
    - Customer short puts  → dealer is short gamma  (negative GEX)

Results are persisted to simulation_runs and logged to agent_logs.
"""

import logging
import os
import sys
from datetime import date, datetime

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.supabase_helpers import get_supabase_client, write_agent_log

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)

AGENT_NAME = "gamma_exposure_engine"


# ──────────────────────────────────────────────────────────────────────
# Data loaders
# ──────────────────────────────────────────────────────────────────────

def fetch_position_legs(sb) -> pd.DataFrame:
    resp = (sb.table("position_legs")
            .select("id, strike, ticker, option_type, quantity, leg_type")
            .execute())
    logger.info("Fetched %d position_legs rows", len(resp.data))
    return pd.DataFrame(resp.data)


def fetch_latest_greeks(sb) -> pd.DataFrame:
    resp = (sb.table("greeks_snapshots")
            .select("position_id, gamma, created_at")
            .order("created_at", desc=True)
            .limit(500)
            .execute())
    logger.info("Fetched %d greeks_snapshots rows", len(resp.data))
    return pd.DataFrame(resp.data)


# ──────────────────────────────────────────────────────────────────────
# Normalisation helpers
# ──────────────────────────────────────────────────────────────────────

def _resolve_option_type(row) -> str:
    """
    Return 'CALL' or 'PUT'.

    Checks option_type first; falls back to leg_type; defaults to CALL.
    """
    ot = row.get("option_type")
    if ot and isinstance(ot, str):
        upper = ot.upper()
        if "PUT" in upper:
            return "PUT"
        if "CALL" in upper:
            return "CALL"

    lt = row.get("leg_type")
    if lt and isinstance(lt, str):
        upper = lt.upper()
        if "PUT" in upper:
            return "PUT"
        if "CALL" in upper:
            return "CALL"

    return "CALL"  # safe default


def _is_short(row) -> bool:
    """Return True if the customer position is short (dealer is opposite side)."""
    lt = row.get("leg_type")
    if lt and isinstance(lt, str) and "SHORT" in lt.upper():
        return True
    return False


# ──────────────────────────────────────────────────────────────────────
# Core GEX computation
# ──────────────────────────────────────────────────────────────────────

def compute_gex(sb) -> tuple[pd.DataFrame | None, int, int]:
    """
    Compute dealer gamma exposure per strike.

    Returns
    -------
    (gex_df, processed, skipped)
    """
    legs = fetch_position_legs(sb)
    greeks = fetch_latest_greeks(sb)

    if legs.empty or greeks.empty:
        logger.warning("No data available for GEX computation")
        return None, 0, 0

    # Filter to greeks that have a valid position_id link
    greeks_linked = greeks[greeks["position_id"].notna()]
    logger.info("greeks_snapshots with valid position_id: %d / %d",
                len(greeks_linked), len(greeks))

    if greeks_linked.empty:
        logger.warning("No linked greeks_snapshots — position_id FK may be null")
        return None, 0, 0

    # Join: greeks_snapshots.position_id = position_legs.id
    df = greeks_linked.merge(
        legs,
        left_on="position_id",
        right_on="id",
        how="inner",
    )
    logger.info("Merged rows (inner join): %d", len(df))

    total_rows = len(df)
    skipped = 0

    # ── Defensive checks: skip rows missing strike or gamma ──────────
    before = len(df)
    df = df[df["strike"].notna() & df["gamma"].notna()]
    skipped += before - len(df)
    if skipped:
        logger.info("Skipped %d rows missing strike or gamma", skipped)

    if df.empty:
        return None, total_rows, skipped

    # ── Normalise quantity: None → 1, then cast to float ─────────────
    df["quantity"] = df["quantity"].apply(
        lambda q: float(q) if q is not None and q == q else 1.0  # q == q filters NaN
    )

    # ── Resolve option type and direction ────────────────────────────
    df["resolved_type"] = df.apply(_resolve_option_type, axis=1)
    df["is_short"] = df.apply(_is_short, axis=1)

    # ── Compute dealer gamma per row ─────────────────────────────────
    def dealer_gamma(row):
        g = float(row["gamma"] or 0) * float(row["quantity"] or 1)

        is_call = row["resolved_type"] == "CALL"
        short = row["is_short"]

        # Customer long call  → dealer short gamma → negative
        # Customer long put   → dealer long gamma  → positive
        # Customer short call → dealer long gamma  → positive
        # Customer short put  → dealer short gamma → negative
        if is_call:
            return g if short else -g
        else:
            return -g if short else g

    df["dealer_gamma"] = df.apply(dealer_gamma, axis=1)

    processed = len(df)

    # ── Aggregate by strike ──────────────────────────────────────────
    gex = (df.groupby("strike", as_index=False)["dealer_gamma"]
           .sum()
           .sort_values("dealer_gamma", ascending=True)
           .reset_index(drop=True))

    gex["strike"] = gex["strike"].astype(float)
    gex["dealer_gamma"] = gex["dealer_gamma"].round(6)

    return gex, processed, skipped


# ──────────────────────────────────────────────────────────────────────
# Persistence
# ──────────────────────────────────────────────────────────────────────

def store_gex(sb, gex: pd.DataFrame) -> None:
    payload = {
        "engine": "dealer_gamma_exposure",
        "parameters": {
            "run_date": date.today().isoformat(),
            "strikes": len(gex),
        },
        "result": gex.to_dict(orient="records"),
    }
    sb.table("simulation_runs").insert(payload).execute()
    logger.info("simulation_runs record written (dealer_gamma_exposure)")


# ──────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────

def main():
    sb = get_supabase_client()

    logger.info("=== Dealer Gamma Exposure Engine started ===")

    gex, processed, skipped = compute_gex(sb)

    if gex is None or gex.empty:
        logger.warning("No GEX results produced")
        write_agent_log(sb, AGENT_NAME, "compute_gex", "empty",
                        {"processed": processed, "skipped": skipped})
        return

    # ── Print report ─────────────────────────────────────────────────
    print()
    print("Dealer Gamma Exposure")
    print(f"  Legs processed: {processed}")
    print(f"  Legs skipped:   {skipped}")
    print()
    print(f"  {'Strike':>10}   {'Dealer Gamma':>14}")
    print(f"  {'-'*10}   {'-'*14}")
    for _, row in gex.iterrows():
        print(f"  {row['strike']:>10.0f}   {row['dealer_gamma']:>+14.6f}")

    # ── Persist ──────────────────────────────────────────────────────
    store_gex(sb, gex)

    write_agent_log(sb, AGENT_NAME, "compute_gex", "success", {
        "processed": processed,
        "skipped": skipped,
        "strikes": len(gex),
        "top_strike": gex.iloc[0].to_dict() if len(gex) > 0 else None,
    })

    logger.info("=== Dealer Gamma Exposure Engine complete ===")


if __name__ == "__main__":
    main()
