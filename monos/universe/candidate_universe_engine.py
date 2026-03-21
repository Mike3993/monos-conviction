"""
candidate_universe_engine.py

Generates the ticker universe for each scanner run.

Sources
-------
1. Portfolio tickers  — from Supabase positions table
2. Sector ETFs        — curated list
3. Index ETFs         — curated list
4. Watchlists         — user-defined overrides
"""

import logging
from dataclasses import dataclass, field

from monos.storage.supabase_client import get_client, write_agent_log

logger = logging.getLogger(__name__)

AGENT = "candidate_universe_engine"

# ── Static universe components ────────────────────────────────────────

SECTOR_ETFS = [
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLP", "XLU", "XLB", "XLRE",
]

INDEX_ETFS = [
    "SPY", "QQQ", "IWM", "DIA",
]

METALS = [
    "GLD", "SLV", "GDX", "SIL", "GDXJ",
]

ENERGY = [
    "XLE", "USO", "XOP",
]

CRYPTO = [
    "BITO", "MSTR",
]

WATCHLIST = [
    "NVDA", "AAPL", "MSFT", "AMZN", "TSLA", "META", "GOOG",
]


@dataclass
class Universe:
    """Container for a resolved scanner universe."""
    tickers: list[str] = field(default_factory=list)
    sources: dict[str, list[str]] = field(default_factory=dict)
    name: str = "MONOS_DEFAULT"


class CandidateUniverseEngine:
    """
    Assembles the union of all ticker sources, de-duplicated and sorted.
    """

    def __init__(self):
        self.sb = get_client()

    # ── Portfolio tickers from Supabase ────────────────────────────

    def _load_portfolio_tickers(self) -> list[str]:
        """Pull distinct tickers from the existing positions table."""
        try:
            resp = self.sb.table("positions").select("ticker").execute()
            tickers = sorted({r["ticker"] for r in resp.data if r.get("ticker")})
            logger.info("Portfolio tickers: %s", tickers)
            return tickers
        except Exception:
            logger.warning("Could not load portfolio tickers — skipping")
            return []

    # ── Build universe ─────────────────────────────────────────────

    def build(self, include_watchlist: bool = True) -> Universe:
        """
        Assemble and return the full scanner universe.
        """
        logger.info("=== Building candidate universe ===")

        portfolio = self._load_portfolio_tickers()

        sources = {
            "portfolio":  portfolio,
            "sector_etf": SECTOR_ETFS,
            "index_etf":  INDEX_ETFS,
            "metals":     METALS,
            "energy":     ENERGY,
            "crypto":     CRYPTO,
        }
        if include_watchlist:
            sources["watchlist"] = WATCHLIST

        # Deduplicate
        all_tickers = sorted({t for group in sources.values() for t in group})

        universe = Universe(
            tickers=all_tickers,
            sources=sources,
            name="MONOS_DEFAULT",
        )

        logger.info("Universe assembled: %d unique tickers from %d sources",
                     len(all_tickers), len(sources))

        write_agent_log(AGENT, "build", "success", {
            "ticker_count": len(all_tickers),
            "sources": list(sources.keys()),
        })

        return universe


# ── CLI ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
    engine = CandidateUniverseEngine()
    u = engine.build()
    print(f"\nUniverse '{u.name}' — {len(u.tickers)} tickers:")
    for t in u.tickers:
        print(f"  {t}")
