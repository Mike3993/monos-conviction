-- ============================================================
-- MONOS MSA / PRE-MSA / ROTATION TABLES + COLUMN ADDITIONS
-- Run this in Supabase SQL Editor (single execution)
-- ============================================================

-- 1. PRE_MSA_SCORES TABLE
CREATE TABLE IF NOT EXISTS pre_msa_scores (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ticker text NOT NULL,
    as_of timestamptz NOT NULL DEFAULT now(),
    pre_msa_bull_score numeric,
    pre_msa_bear_score numeric,
    pre_msa_label_bull text,
    pre_msa_label_bear text,
    components jsonb
);

CREATE INDEX IF NOT EXISTS idx_pre_msa_scores_ticker ON pre_msa_scores(ticker);
CREATE INDEX IF NOT EXISTS idx_pre_msa_scores_as_of ON pre_msa_scores(as_of);

ALTER TABLE pre_msa_scores ENABLE ROW LEVEL SECURITY;
DO $$ BEGIN
    CREATE POLICY pre_msa_all ON pre_msa_scores FOR ALL USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- 2. ROTATION_STATES TABLE
CREATE TABLE IF NOT EXISTS rotation_states (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ticker text NOT NULL,
    as_of timestamptz NOT NULL DEFAULT now(),
    rotation_score numeric,
    rotation_state text,
    components jsonb
);

CREATE INDEX IF NOT EXISTS idx_rotation_states_ticker ON rotation_states(ticker);
CREATE INDEX IF NOT EXISTS idx_rotation_states_as_of ON rotation_states(as_of);

ALTER TABLE rotation_states ENABLE ROW LEVEL SECURITY;
DO $$ BEGIN
    CREATE POLICY rotation_states_all ON rotation_states FOR ALL USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- 3. NEW COLUMNS ON MEGABRAIN_STATES
-- (msa_state, pre_msa_bull, pre_msa_bear, rotation_state already may exist)
DO $$ BEGIN
    ALTER TABLE megabrain_states ADD COLUMN msa_state text;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE megabrain_states ADD COLUMN pre_msa_bull numeric;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE megabrain_states ADD COLUMN pre_msa_bear numeric;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE megabrain_states ADD COLUMN rotation_state text;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

-- 4. NEW COLUMNS ON SCANNER_CANDIDATES
DO $$ BEGIN
    ALTER TABLE scanner_candidates ADD COLUMN msa_state text;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE scanner_candidates ADD COLUMN rotation_state text;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE scanner_candidates ADD COLUMN pre_msa_bull_score numeric;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE scanner_candidates ADD COLUMN pre_msa_bear_score numeric;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

-- Done
SELECT 'MSA/Pre-MSA/Rotation schema setup complete' AS status;
