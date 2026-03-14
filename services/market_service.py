"""
market_service.py

Fetches and normalizes market data required by the engines.
Abstracts data source connections (broker APIs, data vendors, vol surfaces)
so engines remain source-agnostic.
"""


class MarketService:
    """
    Unified interface for retrieving spot prices, implied vol surfaces,
    rates data, and macro indicators used across all engines.
    """

    def __init__(self):
        # TODO: inject data source clients (e.g. broker API, Bloomberg, FRED)
        pass

    def get_spot_price(self, ticker: str) -> float:
        # TODO: fetch current spot price for a given instrument
        raise NotImplementedError

    def get_vol_surface(self, ticker: str) -> dict:
        # TODO: fetch implied volatility surface
        raise NotImplementedError

    def get_macro_indicators(self) -> dict:
        # TODO: fetch macro data points for regime engine
        raise NotImplementedError
