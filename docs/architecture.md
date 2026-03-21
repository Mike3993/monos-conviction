# MONOS Conviction Engine — Architecture

## Overview

The MONOS Conviction Engine is a convex macro trading system designed for
institutional-grade portfolio monitoring, nightly conviction scoring, and
structured briefing generation.

---

## System Layers

```
┌─────────────────────────────────────────────┐
│               Scheduler (nightly)            │
└────────────────────┬────────────────────────┘
                     │
┌────────────────────▼────────────────────────┐
│             Supervisor Agent                 │
│  (orchestrates full pipeline per run)        │
└──┬──────────────┬──────────────┬────────────┘
   │              │              │
   ▼              ▼              ▼
Regime       Greeks         Conviction
Engine       Engine         Map Engine
   │              │              │
   └──────────────▼──────────────┘
              Services Layer
        ┌─────────┬────────────┐
        │Portfolio│  Market    │
        │Service  │  Service   │
        └────┬────┴────────────┘
             │
     ┌───────▼────────┐
     │  Supabase DB   │
     │  (state layer) │
     └────────────────┘
```

---

## Component Responsibilities

### Scheduler
- Triggers the nightly pipeline on a configurable cron schedule.
- Handles retries, logging, and failure alerting.

### Supervisor Agent
- Top-level orchestrator for each pipeline run.
- Sequences engine calls and aggregates results into a briefing.

### Regime Engine
- Classifies the macro environment into a discrete regime label.
- Feeds regime context into conviction scoring.

### Greeks Engine
- Computes options Greeks per position.
- Aggregates portfolio-level convexity exposure.

### Conviction Map Engine
- Scores each position combining regime, Greeks, and macro signals.
- Produces a ranked conviction map for the briefing.

### Portfolio Service
- CRUD interface to the Supabase positions and ladders tables.
- Primary data access layer for all position state.

### Market Service
- Abstracts all external market data retrieval.
- Supplies spot prices, vol surfaces, and macro indicators to engines.

### Briefing Builder
- Assembles nightly briefing from engine outputs.
- Renders structured reports in Markdown or JSON.

---

## State Layer (Supabase)

| Table              | Purpose                                      |
|--------------------|----------------------------------------------|
| `positions`        | Active and historical position records       |
| `ladders`          | Staged entry/exit rungs per position         |
| `regime_snapshots` | Daily macro regime classifications           |
| `briefings`        | Nightly briefing outputs and rendered text   |

---

## Data Flow

1. Scheduler fires nightly job.
2. Supervisor Agent starts pipeline.
3. Regime Engine detects current macro regime.
4. Greeks Engine computes exposure across all positions.
5. Conviction Map Engine scores positions using regime + Greeks.
6. Briefing Builder assembles and renders the nightly report.
7. Briefing is stored in Supabase and optionally dispatched.
