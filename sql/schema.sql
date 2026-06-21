-- Run this once against your new database to set everything up.
-- Uses the timescaledb extension for hypertables IF your Postgres provider
-- has it installed. Many managed Postgres instances (including most current
-- Supabase projects) don't ship the extension binary at all, so we check
-- for it first instead of hard-requiring it — everything below works fine
-- on plain Postgres too, just without Timescale's automatic chunking.

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb') THEN
    CREATE EXTENSION IF NOT EXISTS timescaledb;
  ELSE
    RAISE NOTICE 'timescaledb extension not available on this Postgres instance — skipping, plain tables will be used instead.';
  END IF;
END $$;

-- 1. Reference table: one row per stock
CREATE TABLE IF NOT EXISTS stocks (
  symbol      TEXT PRIMARY KEY,        -- e.g. 'RELIANCE'
  name        TEXT,
  sector      TEXT,
  is_active   BOOLEAN DEFAULT TRUE
);

-- 2. Price data: one row per stock per day
CREATE TABLE IF NOT EXISTS ohlcv (
  time        TIMESTAMPTZ NOT NULL,
  symbol      TEXT NOT NULL REFERENCES stocks(symbol),
  open        NUMERIC,
  high        NUMERIC,
  low         NUMERIC,
  close       NUMERIC,
  volume      BIGINT,
  PRIMARY KEY (symbol, time)
);

-- 3. Turn it into a hypertable (Timescale magic — only runs if the
-- extension actually got created above; plain Postgres table otherwise)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    PERFORM create_hypertable('ohlcv', 'time',
      chunk_time_interval => INTERVAL '1 month',
      if_not_exists => TRUE);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_time ON ohlcv (symbol, time DESC);

-- 4. Where your scanner writes its daily matches
CREATE TABLE IF NOT EXISTS scan_results (
  id          SERIAL PRIMARY KEY,
  scan_date   DATE NOT NULL,
  scanner_name TEXT NOT NULL,   -- e.g. '52_week_high'
  symbol      TEXT NOT NULL REFERENCES stocks(symbol),
  details     JSONB             -- flexible: rsi, volume_ratio, etc.
);

CREATE INDEX IF NOT EXISTS idx_scan_results_date_scanner ON scan_results (scan_date, scanner_name);

-- 5. Reference table: one row per tracked index (benchmarks + sectoral)
CREATE TABLE IF NOT EXISTS indices (
  symbol      TEXT PRIMARY KEY,        -- e.g. '^NSEI', 'NIFTY_FIN_SERVICE.NS'
  name        TEXT,
  category    TEXT,                    -- 'Broad Market' or 'Sectoral'
  is_active   BOOLEAN DEFAULT TRUE
);

-- 6. Price data: one row per index per day
CREATE TABLE IF NOT EXISTS index_ohlcv (
  time        TIMESTAMPTZ NOT NULL,
  symbol      TEXT NOT NULL REFERENCES indices(symbol),
  open        NUMERIC,
  high        NUMERIC,
  low         NUMERIC,
  close       NUMERIC,
  volume      BIGINT,
  PRIMARY KEY (symbol, time)
);

-- 7. Hypertable for index_ohlcv, same pattern as ohlcv above
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    PERFORM create_hypertable('index_ohlcv', 'time',
      chunk_time_interval => INTERVAL '1 month',
      if_not_exists => TRUE);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_index_ohlcv_symbol_time ON index_ohlcv (symbol, time DESC);
