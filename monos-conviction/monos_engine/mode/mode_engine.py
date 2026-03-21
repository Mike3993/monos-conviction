"""
monos_engine.mode.mode_engine

Mode Engine — canonical entry point for asset mode classification
and mode-specific trading configuration.

Re-exports classify_mode and get_mode_config from structure_engine
so the backtest and other consumers can import from a single,
dedicated module.
"""

from monos_engine.convexity.structure_engine import (
    classify_mode,
    get_mode_config,
    get_asset_hold_override,
)

__all__ = ["classify_mode", "get_mode_config", "get_asset_hold_override"]
