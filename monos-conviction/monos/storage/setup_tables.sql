-- ============================================================
-- MONOS Scanner Tables — Run in Supabase SQL Editor
-- Dashboard → SQL Editor → New Query → Paste & Run
-- ============================================================

CREATE TABLE IF NOT EXISTS public.scanner_candidates (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ticker TEXT NOT NULL,
    scan_date DATE DEFAULT CURRENT_DATE,
    opportunity_score INT,
    recommended_structure TEXT,
    gamma_state TEXT,
    vol_regime TEXT,
    iv_rank NUMERIC,
    thesis_health TEXT,
    complexity_index INT,
    risk_overlay JSONB DEFAULT '{}'::jsonb,
    score_breakdown JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.scanner_structure_library (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ticker TEXT NOT NULL,
    structure_type TEXT NOT NULL,
    legs JSONB DEFAULT '[]'::jsonb,
    convexity_score INT,
    risk_profile JSONB DEFAULT '{}'::jsonb,
    tier_allocation JSONB DEFAULT '{}'::jsonb,
    governor_status TEXT DEFAULT 'PENDING',
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.scanner_scenarios (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    structure_id UUID,
    ticker TEXT NOT NULL,
    price_scenario_pct NUMERIC,
    vol_scenario_pct NUMERIC,
    dte_remaining INT,
    expected_pnl NUMERIC,
    expected_pnl_pct NUMERIC,
    greeks_at_scenario JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.scanner_heatmap_runs (
    heatmap_run_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    scanner_run_id UUID,
    as_of_ts TIMESTAMPTZ DEFAULT now(),
    universe_name TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.scanner_heatmap_cells (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    heatmap_run_id UUID,
    ticker TEXT NOT NULL,
    heat_score INT,
    deployable_convexity NUMERIC,
    recommended_structure TEXT,
    governor_status TEXT,
    badges JSONB DEFAULT '[]'::jsonb,
    cluster_key TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.dealer_positioning (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ticker TEXT NOT NULL,
    gamma_flip NUMERIC,
    call_wall NUMERIC,
    put_wall NUMERIC,
    gamma_regime TEXT,
    timestamp TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.flow_snapshots (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ticker TEXT NOT NULL,
    call_volume NUMERIC,
    put_volume NUMERIC,
    call_put_ratio NUMERIC,
    largest_trade JSONB DEFAULT '{}'::jsonb,
    flow_signal TEXT,
    timestamp TIMESTAMPTZ DEFAULT now()
);
