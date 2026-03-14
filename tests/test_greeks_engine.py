"""
test_greeks_engine.py

Unit tests for the GreeksEngine.
Covers per-position Greek calculations and portfolio-level aggregation.
"""

import pytest
import sys
import os

# Ensure project root is on sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from engines.greeks_engine import GreeksEngine, bs_greeks


# Shared fixtures -------------------------------------------------------

SAMPLE_LONG_CALL = {
    "ticker": "SPY",
    "leg_type": "LONG_CALL",
    "strike": 560,
    "expiration": "2026-12-18",
    "spot": 564.0,
    "iv": 0.20,
}

SAMPLE_LONG_PUT = {
    "ticker": "SPY",
    "leg_type": "LONG_PUT",
    "strike": 560,
    "expiration": "2026-12-18",
    "spot": 564.0,
    "iv": 0.20,
}

SAMPLE_SHORT_CALL = {
    "ticker": "SPY",
    "leg_type": "SHORT_CALL",
    "strike": 600,
    "expiration": "2026-12-18",
    "spot": 564.0,
    "iv": 0.20,
}


class TestGreeksEngine:
    """Tests for GreeksEngine calculations and aggregation."""

    def setup_method(self):
        # Create engine without Supabase (pass dummy; we won't call Supabase)
        self.engine = GreeksEngine.__new__(GreeksEngine)
        self.engine.sb = None  # no Supabase calls in unit tests

    def test_compute_greeks_returns_expected_keys(self):
        result = self.engine.compute_greeks(SAMPLE_LONG_CALL)
        expected_keys = {"delta", "gamma", "vega", "theta", "rho", "iv"}
        assert expected_keys == set(result.keys())

    def test_delta_sign_matches_direction(self):
        call_greeks = self.engine.compute_greeks(SAMPLE_LONG_CALL)
        put_greeks = self.engine.compute_greeks(SAMPLE_LONG_PUT)

        assert call_greeks["delta"] > 0, "Long call should have positive delta"
        assert put_greeks["delta"] < 0, "Long put should have negative delta"

    def test_short_leg_flips_sign(self):
        long_greeks = self.engine.compute_greeks(SAMPLE_LONG_CALL)
        short_greeks = self.engine.compute_greeks(SAMPLE_SHORT_CALL)

        # Short call delta should be negative
        assert short_greeks["delta"] < 0

    def test_aggregate_portfolio_greeks_sums_correctly(self):
        legs = [SAMPLE_LONG_CALL, SAMPLE_LONG_PUT]
        agg = self.engine.aggregate_portfolio_greeks(legs)

        # Individually compute
        g1 = self.engine.compute_greeks(SAMPLE_LONG_CALL)
        g2 = self.engine.compute_greeks(SAMPLE_LONG_PUT)

        assert abs(agg["delta"] - (g1["delta"] + g2["delta"])) < 1e-5
        assert abs(agg["gamma"] - (g1["gamma"] + g2["gamma"])) < 1e-5
        assert abs(agg["vega"] - (g1["vega"] + g2["vega"])) < 1e-5

    def test_zero_positions_returns_zero_greeks(self):
        agg = self.engine.aggregate_portfolio_greeks([])
        for key in ("delta", "gamma", "vega", "theta", "rho"):
            assert agg[key] == 0.0


class TestBSGreeks:
    """Direct tests on the bs_greeks helper."""

    def test_call_delta_between_0_and_1(self):
        g = bs_greeks(S=100, K=100, T=0.5, r=0.05, sigma=0.20, option_type="call")
        assert 0 < g["delta"] < 1

    def test_put_delta_between_neg1_and_0(self):
        g = bs_greeks(S=100, K=100, T=0.5, r=0.05, sigma=0.20, option_type="put")
        assert -1 < g["delta"] < 0

    def test_gamma_always_positive(self):
        g = bs_greeks(S=100, K=100, T=0.5, r=0.05, sigma=0.20, option_type="call")
        assert g["gamma"] > 0

    def test_vega_always_positive(self):
        g = bs_greeks(S=100, K=100, T=0.5, r=0.05, sigma=0.20, option_type="call")
        assert g["vega"] > 0
