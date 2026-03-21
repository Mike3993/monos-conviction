# MONOS Research Lab

Governance-first research validation system for the MONOS Canon.

## CH01 — MSA Regime Classification (Real Data)

CH01 is now tested with **real historical price data** and the **actual MSA regime engine** — not placeholders.

### How It Works

`engine/msa_scanner.py` implements the production MSA classifier:
- Downloads real price history via yfinance
- Computes 50-day SMA + 20-day slope per bar
- Classifies each trading day as MSA_BULLISH / MSA_BEARISH / MSA_NEUTRAL
- Generates regime snapshots across the full universe

`engine/backtest_runner.py` runs the experiment:
1. **Baseline**: all momentum signals traded without MSA gating
2. **Candidate**: signals filtered by MSA regime (LONG blocked in BEARISH, SHORT blocked in BULLISH)
3. Runs both in-sample (2024-01 to 2025-06) and out-of-sample (2025-07 to 2026-03)
4. Scores via 5-metric scorecard (Sharpe, WR, MDD, theta efficiency, convexity retention)
5. Validates each rule through the 7-gate verdict system
6. Logs to `reports/evidence_table.csv` and `reports/experiment_log.csv`

### CH01 Results

| Metric | In-Sample | Out-of-Sample |
|---|---|---|
| Scorecard | **4/5 PASS** | **4/5 PASS** |
| Sharpe Delta | +0.12 | +0.55 |
| Win Rate Delta | -0.18pp | +3.7pp |
| Max Drawdown | Improved +15% | Worsened -10% |
| Cross-Instrument | 3/6 improved | **5/6 improved** |
| Trade Count | 1116 baseline / 735 filtered | 492 baseline / 359 filtered |

### Verdict Taxonomy

| Verdict | Meaning |
|---|---|
| `EARNS_PLACE` | All gates pass including Tyler. Canon-ready. |
| `INTERNAL_PASS` | All empirical gates pass. Tyler not yet received. Internal winner. |
| `PROVISIONAL` | Strong evidence, minor gaps. No Tyler required. |
| `CONDITIONAL_PENDING_TYLER` | Near-pass empirically, Type C Tyler required. |
| `CONDITIONAL` | Partial pass, needs more evidence. |
| `REDUNDANT` | No incremental value. |
| `HURTS` | Degrades performance. |
| `INSUFFICIENT_SAMPLE` | Not enough data. |

### CH01 Rule Verdicts

| Rule | Type | Verdict | Gates | Status |
|---|---|---|---|---|
| C-MSA-01 | C | **INTERNAL_PASS** | 6/6 empirical | Tyler pending |
| C-MSA-02 | C | **INTERNAL_PASS** | 6/6 empirical | Tyler pending |
| C-MSA-03 | C | **INTERNAL_PASS** | 6/6 empirical | Tyler pending |
| C-MSA-04 | C | **INTERNAL_PASS** | 6/6 empirical | Tyler pending |
| C-MSA-06 | C | **INTERNAL_PASS** | 6/6 empirical | Tyler pending |

All 5 CH01 rules are **internal winners**. They cannot be promoted to Canon without Tyler live-infrastructure validation (Type C governance constraint).

### Run Instructions

```powershell
cd C:\Users\mcala\Documents\convexity_engine\monos-research-lab
$env:PYTHONPATH = "."
python engine/backtest_runner.py
```

### Governance Constraints

- Geometric/log return discipline preserved throughout
- No writes to the live MONOS app
- No auto-promotion of rules
- Type C rules remain Tyler-gated
- Research lab is isolated from the main MONOS dashboard repo
- Internal winners are reportable but not Canon until Tyler validates
