# MONOS Conviction Engine

A convex macro trading system architecture for institutional-grade
portfolio monitoring, conviction scoring, and nightly briefing generation.

---

## System Capabilities

- **Ladder tracking** — Staged entry and exit management across positions,
  with per-rung target prices and size allocations.

- **Greeks monitoring** — Per-position and portfolio-level options Greeks
  (delta, gamma, vega, theta, rho) with convexity exposure surfacing.

- **Macro regime engine** — Dynamic classification of the macro environment
  into discrete regime labels (risk-on, stagflation, vol expansion, etc.)
  that condition all downstream conviction scoring.

- **Nightly scheduler** — Automated pipeline execution with job logging,
  retries, and structured briefing output per run.

- **Supabase state layer** — PostgreSQL-backed persistence for positions,
  ladders, regime snapshots, and briefing history via Supabase.

---

## Repository Structure

```
monos-conviction/
├── agents/               # Supervisor agent — top-level pipeline orchestration
├── engines/              # Regime, Greeks, and conviction scoring engines
├── services/             # Portfolio, market data, and briefing services
├── scheduler/            # Nightly job runner and schedule configuration
├── infra/
│   └── migrations/       # Versioned SQL schema migrations for Supabase
├── data/                 # Sample data and fixtures for development
├── tests/                # Unit and integration tests
└── docs/                 # Architecture diagrams and system documentation
```

---

## Documentation

- [Architecture](docs/architecture.md) — Component diagram and data flow
- [System Overview](docs/system_overview.md) — Design principles and concepts

---

## Status

> System skeleton initialized. Full engine and service implementations are in progress.
