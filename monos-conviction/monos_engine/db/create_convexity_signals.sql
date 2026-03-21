-- Run this in Supabase SQL Editor to create the convexity_signals table
-- Dashboard → SQL Editor → New Query → Paste → Run

CREATE TABLE IF NOT EXISTS public.convexity_signals (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ticker TEXT NOT NULL,
    signal_strength NUMERIC NOT NULL,
    convexity_score NUMERIC NOT NULL,
    implied_vol NUMERIC,
    realized_vol NUMERIC,
    skew NUMERIC,
    structure_type TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    timestamp TIMESTAMPTZ DEFAULT now(),
    created_at TIMESTAMPTZ DEFAULT now()
);
