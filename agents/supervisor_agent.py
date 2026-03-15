"""
supervisor_agent.py

Orchestrates the full MONOS Conviction Engine pipeline.
Coordinates between regime detection, Greeks monitoring, conviction scoring,
and briefing generation. Acts as the top-level entry point for nightly runs.
"""


class SupervisorAgent:
    """
    Top-level agent responsible for sequencing engine calls,
    aggregating outputs, and dispatching the nightly conviction briefing.
    """

    def __init__(self):
        # TODO: inject engine and service dependencies
        pass

    def run(self):
        # TODO: orchestrate regime, greeks, conviction, and briefing pipeline
        raise NotImplementedError
