"""
market_service.py

Fetches and normalizes market data required by the engines.
Reads unique tickers from the positions table, pulls latest prices
via yfinance, and inserts records into market_snapshots.
"""

import logging
import os
import sys
from datetime import datetime

import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.supabase_helpers import get_supabase_client, write_agent_log

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

AGENT_NAME = "market_service"


class MarketService:
    """
    Unified interface for retrieving spot prices, implied vol surfaces,
    rates data, and macro indicators used across all engines.
    """

    def __init__(self, supabase_client=None):
        self.sb = supabase_client or get_supabase_client()

    # ----------------------------------------------------------------- core

    def get_unique_tickers(self) -> list[str]:
        """Read distinct tickers from the positions table."""
        resp = self.sb.table("positions").select("ticker").execute()
        tickers = list({row["ticker"] for row in resp.data if row.get("ticker")})
        logger.info("Unique tickers from positions: %s", tickers)
        return tickers

    def get_spot_price(self, ticker: str) -> dict:
        """
        Fetch current price data for a single ticker via yfinance.
        Returns dict with price, iv (if available), and volume.
        """
        try:
            t = yf.Ticker(ticker)
            info = t.fast_info
            price = float(info.get("lastPrice", 0) or info.get("previousClose", 0))
            volume = float(info.get("lastVolume", 0))

            result = {
                "ticker": ticker,
                "price": round(price, 4),
                "iv": None,  # yfinance doesn't provide aggregate IV directly
                "volume": round(volume, 2),
            }
            logger.info("Fetched %s: price=%.2f volume=%.0f", ticker, price, volume)
            return result
        except Exception as exc:
            logger.error("Failed to fetch price for %s: %s", ticker, exc)
            return {
                "ticker": ticker,
                "price": 0.0,
                "iv": None,
                "volume": 0.0,
            }

    def get_vol_surface(self, ticker: str) -> dict:
        """Fetch implied volatility surface (placeholder)."""
        logger.warning("Vol surface not yet implemented for %s", ticker)
        return {"ticker": ticker, "surface": {}}

    def get_macro_indicators(self) -> dict:
        """Fetch macro data points for regime engine (placeholder)."""
        logger.warning("Macro indicators not yet implemented")
        return {}

    # ---------------------------------------------------------- persistence

    def fetch_and_store(self) -> list[dict]:
        """
        End-to-end: read tickers → fetch prices → write to market_snapshots.
        Returns the list of inserted snapshot rows.
        """
        logger.info("=== Market snapshot fetch started ===")
        tickers = self.get_unique_tickers()

        if not tickers:
            logger.warning("No tickers found in positions table")
            write_agent_log(self.sb, AGENT_NAME, "fetch_and_store",
                            "skipped", {"reason": "no tickers"})
            return []

        snapshots = []
        for ticker in tickers:
            data = self.get_spot_price(ticker)
            if data["price"] > 0:
                snapshots.append(data)

        if snapshots:
            logger.info("Writing %d market_snapshots rows...", len(snapshots))
            self.sb.table("market_snapshots").insert(snapshots).execute()
            logger.info("market_snapshots written successfully")
        else:
            logger.warning("No valid price data fetched")

        write_agent_log(self.sb, AGENT_NAME, "fetch_and_store",
                        "success", {
                            "tickers": tickers,
                            "snapshots_written": len(snapshots),
                        })

        logger.info("=== Market snapshot fetch complete ===")
        return snapshots


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    svc = MarketService()
    results = svc.fetch_and_store()
    for r in results:
        print(r)
