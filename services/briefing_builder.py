"""
briefing_builder.py

Assembles the nightly conviction briefing from scored positions,
regime state, Greeks summary, and portfolio-level risk metrics.
Outputs a structured briefing document suitable for review or delivery.
"""


class BriefingBuilder:
    """
    Composes the nightly briefing payload from engine outputs.
    Supports multiple output formats (Markdown, JSON, Slack message).
    """

    def __init__(self):
        # TODO: inject portfolio service and engine result dependencies
        pass

    def build(self, regime: str, conviction_scores: list, greeks_summary: dict) -> dict:
        # TODO: assemble full briefing structure
        raise NotImplementedError

    def render_markdown(self, briefing: dict) -> str:
        # TODO: format briefing as a readable Markdown report
        raise NotImplementedError
