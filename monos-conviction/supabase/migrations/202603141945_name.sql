-- MONOS Conviction Engine
-- Schema Version 1
-- Canonical State Layer
-- Created: 2026

------------------------------------------
-- POSITIONS (top level portfolio object)
------------------------------------------

CREATE TABLE positions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticker TEXT NOT NULL,
    asset_class TEXT,
    state TEXT DEFAULT 'TRIGGERED',
    created_at TIMESTAMP DEFAULT now()
);

------------------------------------------
-- POSITION LEGS (individual options)
------------------------------------------

CREATE TABLE position_legs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    position_id UUID REFERENCES positions(id),
    leg_type TEXT,
    option_type TEXT,
    expiration DATE,
    strike NUMERIC,
    quantity INTEGER,
    multiplier INTEGER DEFAULT 100,
    entry_price NUMERIC,
    is_hedge BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT now()
);

------------------------------------------
-- POSITION CHANGE LOG (append only)
------------------------------------------

CREATE TABLE position_changes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    position_id UUID,
    change_type TEXT,
    description TEXT,
    created_at TIMESTAMP DEFAULT now()
);

------------------------------------------
-- LADDERS (strategy grouping)
------------------------------------------

CREATE TABLE ladders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ladder_name TEXT,
    underlying TEXT,
    tag TEXT,
    notional NUMERIC,
    hedge_coverage_pct NUMERIC,
    created_at TIMESTAMP DEFAULT now()
);

------------------------------------------
-- TRADE BLOTTER
------------------------------------------

CREATE TABLE trade_blotter (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticker TEXT,
    trade_type TEXT,
    quantity INTEGER,
    price NUMERIC,
    rationale TEXT,
    created_at TIMESTAMP DEFAULT now()
);

------------------------------------------
-- EXECUTION RECORDS
------------------------------------------

CREATE TABLE execution_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trade_id UUID,
    broker TEXT,
    fill_price NUMERIC,
    fill_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT now()
);

------------------------------------------
-- DAILY GREEKS SNAPSHOT
------------------------------------------

CREATE TABLE greeks_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    position_id UUID,
    delta NUMERIC,
    gamma NUMERIC,
    theta NUMERIC,
    vega NUMERIC,
    iv NUMERIC,
    created_at TIMESTAMP DEFAULT now()
);

------------------------------------------
-- REGIME SNAPSHOTS
------------------------------------------

CREATE TABLE regime_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    regime TEXT,
    confidence NUMERIC,
    spx_momentum NUMERIC,
    vix_stress NUMERIC,
    dollar_trend NUMERIC,
    metals_breadth NUMERIC,
    created_at TIMESTAMP DEFAULT now()
);

------------------------------------------
-- MARKET SNAPSHOTS
------------------------------------------

CREATE TABLE market_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticker TEXT,
    price NUMERIC,
    iv NUMERIC,
    volume NUMERIC,
    created_at TIMESTAMP DEFAULT now()
);

------------------------------------------
-- SIMULATION RUNS
------------------------------------------

CREATE TABLE simulation_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    engine TEXT,
    parameters JSONB,
    result JSONB,
    created_at TIMESTAMP DEFAULT now()
);

------------------------------------------
-- BRIEFING REPORTS
------------------------------------------

CREATE TABLE briefing_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    report_date DATE,
    content JSONB,
    created_at TIMESTAMP DEFAULT now()
);

------------------------------------------
-- TASK RUNS
------------------------------------------

CREATE TABLE task_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_name TEXT,
    status TEXT,
    details JSONB,
    created_at TIMESTAMP DEFAULT now()
);

------------------------------------------
-- AGENT LOGS
------------------------------------------

CREATE TABLE agent_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent TEXT,
    action TEXT,
    result TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT now()
);

------------------------------------------
-- USER CONFIG
------------------------------------------

CREATE TABLE user_config (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    config_key TEXT,
    config_value JSONB,
    created_at TIMESTAMP DEFAULT now()
);

------------------------------------------
-- EVENT RISK
------------------------------------------

CREATE TABLE event_risk (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_name TEXT,
    severity TEXT,
    description TEXT,
    created_at TIMESTAMP DEFAULT now()
);

------------------------------------------
-- NOTION TASK MIRROR
------------------------------------------

CREATE TABLE notion_tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    notion_id TEXT,
    title TEXT,
    status TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT now()
);
