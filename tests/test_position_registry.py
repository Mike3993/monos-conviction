"""
test_position_registry.py

Unit tests for the PortfolioService position registry.
Covers data loading and validation against the sample JSON.
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "sample_positions.json")


class TestPositionRegistry:
    """Tests for portfolio data integrity and structure."""

    def setup_method(self):
        with open(DATA_PATH) as f:
            self.data = json.load(f)

    def test_json_has_required_keys(self):
        assert "ladders" in self.data
        assert "positions" in self.data
        assert "position_legs" in self.data

    def test_get_all_positions_returns_list(self):
        assert isinstance(self.data["positions"], list)
        assert len(self.data["positions"]) > 0

    def test_positions_have_required_fields(self):
        for pos in self.data["positions"]:
            assert "ticker" in pos
            assert "asset_class" in pos
            assert "state" in pos

    def test_legs_have_required_fields(self):
        for leg in self.data["position_legs"]:
            assert "ticker" in leg
            assert "leg_type" in leg
            assert "strike" in leg
            assert "expiration" in leg

    def test_ladders_have_required_fields(self):
        for ladder in self.data["ladders"]:
            assert "ticker" in ladder
            assert "name" in ladder
            assert "notional" in ladder
