# MONOS Conviction Engine — System Overview

## Purpose

MONOS is a conviction-driven macro trading system built around convex positioning.
It exists to track, score, and brief on a portfolio of options and macro exposures
through a consistent, repeatable nightly pipeline.

---

## Core Concepts

### Convexity
Positions in MONOS are selected for asymmetric payoff profiles. The system
prioritizes instruments where the upside significantly exceeds the downside,
particularly during regime transitions or tail events.

### Laddering
Rather than sizing into a full position at once, MONOS manages staged entries
and exits via ladders — discrete rungs at target prices with defined size
allocations per rung.

### Macro Regime
The system continuously classifies the macro environment (e.g. risk-on,
stagflation, vol expansion) and adjusts conviction weights accordingly.
Regime context is the primary top-down filter.

### Conviction Scoring
Each position receives a normalized conviction score combining:
- Regime alignment
- Greeks profile (convexity, vega exposure)
- Proximity to ladder rungs
- Time decay pressure (theta)

### Nightly Briefing
The output of each pipeline run is a structured briefing summarizing:
- Current regime label
- Top conviction positions
- Aggregate Greeks
- Ladder status and next rungs
- Risk flags

---

## Technology Stack

| Component       | Technology              |
|-----------------|-------------------------|
| State layer     | Supabase (PostgreSQL)   |
| Scheduler       | Python (cron / APScheduler) |
| Engines/Agents  | Python                  |
| Data sources    | TBD (broker API, vendor)|
| Output delivery | TBD (Slack, email, file)|

---

## Key Design Principles

1. **Source-agnostic engines** — Engines receive normalized data objects. Market
   data sources are swappable without touching engine logic.

2. **Stateless engines, stateful services** — Engines compute; services manage state.
   All persistence goes through the service layer to Supabase.

3. **Regime-first** — Conviction scores are always conditioned on the current
   regime. No position is evaluated in isolation from the macro context.

4. **Incremental schema migration** — Database changes are versioned SQL files
   under `infra/migrations/` for reproducibility and auditability.

5. **Testability** — All engines and services are injected with dependencies
   to enable unit testing with mock data sources.
