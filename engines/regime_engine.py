"""
regime_engine.py

Detects the current macro regime based on market indicators, vol surfaces,
rates positioning, and cross-asset signals. Regime classification informs
conviction weights and position sizing logic.
"""


class RegimeEngine:
    """
    Classifies the macro environment into a discrete regime state.
    Regime labels drive downstream conviction adjustments in the conviction map.
    """

    REGIMES = [
        "risk_on",
        "risk_off",
        "stagflation",
        "reflation",
        "deflation_scare",
        "vol_expansion",
        "vol_compression",
    ]

    def __init__(self):
        # TODO: inject market service and signal sources
        pass

    def detect_regime(self) -> str:
        # TODO: implement regime classification logic
        raise NotImplementedError
