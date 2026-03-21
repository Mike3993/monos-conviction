"""
dte_selector.py

Selects optimal days-to-expiration based on GEX phase and vol regime.

Rules:
    NEGATIVE gamma + ELEVATED vol  →  shorter DTE (30-45) — fast convexity
    NEGATIVE gamma + COMPRESSED    →  medium DTE (45-60)  — cheap gamma
    POSITIVE gamma + COMPRESSED    →  longer DTE (60-90)  — time value
    POSITIVE gamma + ELEVATED      →  medium DTE (45-60)  — balanced

Default: 60 days.
"""

import logging

logger = logging.getLogger(__name__)

DTE_RULES = {
    ("NEGATIVE", "EXTREME"):    30,
    ("NEGATIVE", "ELEVATED"):   35,
    ("NEGATIVE", "NORMAL"):     45,
    ("NEGATIVE", "COMPRESSED"): 50,
    ("POSITIVE", "EXTREME"):    45,
    ("POSITIVE", "ELEVATED"):   50,
    ("POSITIVE", "NORMAL"):     60,
    ("POSITIVE", "COMPRESSED"): 75,
}

DEFAULT_DTE = 60


def select_dte(gamma_regime: str, vol_regime: str) -> int:
    """
    Return optimal DTE based on gamma × vol regime.

    Parameters
    ----------
    gamma_regime : 'POSITIVE' or 'NEGATIVE'
    vol_regime   : 'COMPRESSED', 'NORMAL', 'ELEVATED', 'EXTREME'

    Returns
    -------
    int : recommended days to expiration
    """
    key = (gamma_regime.upper(), vol_regime.upper())
    dte = DTE_RULES.get(key, DEFAULT_DTE)
    logger.info("DTE selection: %s × %s → %d days", gamma_regime, vol_regime, dte)
    return dte
