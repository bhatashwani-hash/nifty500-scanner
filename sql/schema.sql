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
  market_cap  NUMERIC,                 -- INR, from yfinance (ingestion/build_nse_universe.py)
  is_fno      BOOLEAN DEFAULT FALSE,   -- in NSE Futures & Options segment (NSE fo_mktlots.csv)
  is_active   BOOLEAN DEFAULT TRUE
);
-- For existing databases:
ALTER TABLE stocks ADD COLUMN IF NOT EXISTS market_cap NUMERIC;
ALTER TABLE stocks ADD COLUMN IF NOT EXISTS is_fno BOOLEAN DEFAULT FALSE;

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

-- ============================================================================
-- 8. Daily scanner output tables
--    All scanners run over the 500-stock universe: `stocks` (universe) + `ohlcv`
--    (prices). Symbol columns reference stocks(symbol).
-- ============================================================================

-- Episodic Pivot — rolling 6-month leaderboard, ranked by return since pivot.
CREATE TABLE IF NOT EXISTS scanner_ep (
  run_date            DATE NOT NULL,                       -- the pivot date
  symbol              TEXT NOT NULL REFERENCES stocks(symbol),
  ep_type             TEXT,                                -- 'BULLISH' | 'BEARISH'
  open                NUMERIC,
  close               NUMERIC,                             -- pivot-day close
  prev_close          NUMERIC,
  gap_pct             NUMERIC,
  move_pct            NUMERIC,
  vol_ratio           NUMERIC,
  volume              BIGINT,
  vol_avg_50d         NUMERIC,
  last_date           DATE,                                -- latest bar used for return
  last_close          NUMERIC,                             -- latest close
  return_since_pivot  NUMERIC,                             -- (last_close/close - 1)*100
  rnk                 INTEGER,                             -- 1 = best return since pivot
  PRIMARY KEY (run_date, symbol)
);
CREATE INDEX IF NOT EXISTS idx_scanner_ep_rnk ON scanner_ep (rnk);

-- N-period closing highs / lows.
CREATE TABLE IF NOT EXISTS scanner_breakouts (
  run_date      DATE NOT NULL,
  symbol        TEXT NOT NULL REFERENCES stocks(symbol),
  close         NUMERIC,
  change_pct    NUMERIC,
  volume        BIGINT,
  breakout_type TEXT NOT NULL,                             -- e.g. '1M_HIGH', '1Y_LOW'
  PRIMARY KEY (run_date, symbol, breakout_type)
);

-- Volatility Contraction Pattern setups.
CREATE TABLE IF NOT EXISTS scanner_vcp (
  run_date      DATE NOT NULL,
  symbol        TEXT NOT NULL REFERENCES stocks(symbol),
  close         NUMERIC,
  return_3m     NUMERIC,
  high_15d      NUMERIC,
  low_15d       NUMERIC,
  range_15d_pct NUMERIC,
  PRIMARY KEY (run_date, symbol)
);

-- Sector performance (day / week / month).
CREATE TABLE IF NOT EXISTS scanner_sectors (
  run_date    DATE NOT NULL,
  period      TEXT NOT NULL,                               -- 'day' | 'week' | 'month'
  sector      TEXT NOT NULL,
  avg_return  NUMERIC,
  med_return  NUMERIC,
  up_count    INTEGER,
  down_count  INTEGER,
  stock_count INTEGER,
  PRIMARY KEY (run_date, period, sector)
);

-- "Manas Scan" — two-layer momentum-pullback scanner.
--   Layer 1 (hard filter): close>30, within 25% of 52w high, close>SMA50>SMA200,
--   SMA200 rising 3m, close>=1.5x 52w low, 52w high made in last 6 months.
--   Layer 2 (flags + score 0..5): prior up-leg >=50%, 0-5% above 21 EMA,
--   volatility contraction (tight 5d range + falling volume), inside bar,
--   fast mover (>5% day on >1M vol in last 40 bars). Written by scan_manas.py.
CREATE TABLE IF NOT EXISTS scanner_manas (
  run_date        DATE NOT NULL,
  symbol          TEXT NOT NULL REFERENCES stocks(symbol),
  close           NUMERIC,
  pct_from_high   NUMERIC,
  sma50           NUMERIC,
  sma200          NUMERIC,
  ema21           NUMERIC,
  pct_above_ema   NUMERIC,
  prior_move_pct  NUMERIC,
  range_5d_pct    NUMERIC,
  vol_ratio       NUMERIC,
  inside_bar      BOOLEAN DEFAULT FALSE,
  fast_mover      BOOLEAN DEFAULT FALSE,
  setup_score     INTEGER,
  setup_ready     BOOLEAN DEFAULT FALSE,
  last_date       DATE,
  PRIMARY KEY (run_date, symbol)
);
CREATE INDEX IF NOT EXISTS idx_scanner_manas_score ON scanner_manas (setup_score DESC);

-- "Linda Scan" — Linda Raschke short-term setups, one row per fired signal.
--   holy_grail    : ADX(14)>30 trend pullback to the 20 EMA (BUY uptrend / SELL downtrend)
--   turtle_soup   : failed 20-day breakout/breakdown (BUY/SELL)
--   eighty_twenty : 80/20 reversal bar, faded next day (BUY/SELL)
--   persistency   : 7/7 closes on one side of the 5-MA (BUY/SELL trend flag)
-- Written by scan_linda.py.
CREATE TABLE IF NOT EXISTS scanner_linda (
  run_date   DATE NOT NULL,
  symbol     TEXT NOT NULL REFERENCES stocks(symbol),
  pattern    TEXT NOT NULL,
  side       TEXT NOT NULL,
  close      NUMERIC,
  adx14      NUMERIC,
  ema20      NUMERIC,
  ref_level  NUMERIC,
  note       TEXT,
  last_date  DATE,
  PRIMARY KEY (run_date, symbol, pattern, side)
);
CREATE INDEX IF NOT EXISTS idx_scanner_linda_pat ON scanner_linda (pattern, side);

-- Live 15-minute intraday snapshot (F&O stocks + indices), one row per symbol.
-- Populated by ingestion/fetch_live.py via .github/workflows/live_15m.yml (market hours).
CREATE TABLE IF NOT EXISTS live_15m (
  symbol      TEXT PRIMARY KEY,
  name        TEXT,
  is_index    BOOLEAN DEFAULT FALSE,
  last        NUMERIC,
  prev_close  NUMERIC,
  chg_pct     NUMERIC,
  intraday    JSONB,
  bar_ts      TIMESTAMPTZ,
  updated_at  TIMESTAMPTZ DEFAULT now()
);

-- Live Zerodha (Kite) ticks (F&O stocks + indices), one row per symbol.
-- Populated continuously by ingestion/kite_ticks.py (Kite WebSocket) during market
-- hours; the dashboard "Ticks" tab polls it every ~3s. Needs a Kite Connect app.
CREATE TABLE IF NOT EXISTS ticks (
  symbol      TEXT PRIMARY KEY,
  is_index    BOOLEAN DEFAULT FALSE,
  ltp         NUMERIC,
  prev_close  NUMERIC,
  chg_pct     NUMERIC,
  vol         BIGINT,
  ts          TIMESTAMPTZ,
  updated_at  TIMESTAMPTZ DEFAULT now()
);

-- Market breadth (Worden T2107/T2108-style), one row per trading day.
CREATE TABLE IF NOT EXISTS scanner_breadth (
  date            DATE PRIMARY KEY,
  up_4pct         INTEGER,
  down_4pct       INTEGER,
  ratio_5d        NUMERIC,
  ratio_10d       NUMERIC,
  up_25pct_3m     INTEGER,
  down_25pct_3m   INTEGER,
  up_25pct_1m     INTEGER,
  down_25pct_1m   INTEGER,
  up_50pct_1m     INTEGER,
  down_50pct_1m   INTEGER,
  up_13pct_34d    INTEGER,
  down_13pct_34d  INTEGER,
  universe        INTEGER,
  pct_above_200ma NUMERIC,
  nifty50_close   NUMERIC
);
