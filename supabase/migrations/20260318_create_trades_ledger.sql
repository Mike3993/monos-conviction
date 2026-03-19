-- MONOS Trades Ledger — Supabase migration
-- Run this in the Supabase SQL Editor at:
-- https://supabase.com/dashboard/project/yivhadyeunfukqvufixg/sql

CREATE TABLE IF NOT EXISTS trades_ledger (
    id              SERIAL PRIMARY KEY,
    date_open       DATE,
    date_close      DATE,
    ticker          VARCHAR(10) NOT NULL,
    direction       VARCHAR(10),
    trade_mode      VARCHAR(20),
    structure       VARCHAR(20),
    contract_symbol VARCHAR(40),
    expiration      DATE,
    strike          VARCHAR(20),
    strike_delta    NUMERIC(6,4),
    moneyness_pct   NUMERIC(8,4),
    contracts       INTEGER DEFAULT 1,
    hold_days       INTEGER,
    confidence      NUMERIC(6,2),
    msa_state       VARCHAR(20),
    expected_return NUMERIC(10,4),
    -- Open pricing
    quoted_bid_open       NUMERIC(10,4),
    quoted_ask_open       NUMERIC(10,4),
    quoted_mid_open       NUMERIC(10,4),
    suggested_entry_price NUMERIC(10,4),
    actual_entry_price    NUMERIC(10,4),
    -- Close pricing
    quoted_bid_close      NUMERIC(10,4),
    quoted_ask_close      NUMERIC(10,4),
    quoted_mid_close      NUMERIC(10,4),
    suggested_exit_price  NUMERIC(10,4),
    actual_exit_price     NUMERIC(10,4),
    -- Results
    realized_pnl          NUMERIC(12,2),
    realized_return_pct   NUMERIC(10,4),
    slippage_open         NUMERIC(10,4),
    slippage_close        NUMERIC(10,4),
    win                   BOOLEAN,
    status                VARCHAR(10) DEFAULT 'OPEN',
    notes                 TEXT,
    close_notes           TEXT,
    -- Future-ready: multiple strike candidates per trade
    strike_candidates     JSONB,
    -- Timestamps
    created_at            TIMESTAMPTZ DEFAULT NOW(),
    updated_at            TIMESTAMPTZ DEFAULT NOW()
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_ledger_ticker ON trades_ledger(ticker);
CREATE INDEX IF NOT EXISTS idx_ledger_status ON trades_ledger(status);
CREATE INDEX IF NOT EXISTS idx_ledger_mode ON trades_ledger(trade_mode);

-- Enable RLS (Row Level Security) but allow service role full access
ALTER TABLE trades_ledger ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Service role full access" ON trades_ledger
    FOR ALL USING (true) WITH CHECK (true);
