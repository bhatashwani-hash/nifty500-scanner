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

-- Hourly OHLCV bars (Yahoo 1h interval, ~9:15–15:30 IST session), all active
-- stocks. Populated by ingestion/fetch_hourly.py via hourly_ingest.yml
-- (hourly 9:30–15:30 IST on trading days). Partial current bar is refreshed
-- on each run and finalizes by the 15:30 run.
CREATE TABLE IF NOT EXISTS ohlcv_hourly (
  symbol  TEXT NOT NULL REFERENCES stocks(symbol),
  ts      TIMESTAMPTZ NOT NULL,
  open    NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC,
  volume  BIGINT,
  PRIMARY KEY (symbol, ts)
);
ALTER TABLE ohlcv_hourly ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS anon_read_ohlcv_hourly ON ohlcv_hourly;
CREATE POLICY anon_read_ohlcv_hourly ON ohlcv_hourly FOR SELECT TO anon USING (true);

-- Intraday snapshot, one row per symbol: day change + time-adjusted RVOL
-- (today's cumulative volume ÷ (20d avg daily volume × session fraction
-- elapsed)). Written by fetch_hourly.py after each hourly ingest. The
-- dashboard Hourly F&O tab reads it and highlights |rvol| >= 1.5 movers.
CREATE TABLE IF NOT EXISTS hourly_feed (
  symbol       TEXT PRIMARY KEY REFERENCES stocks(symbol),
  trade_date   DATE,
  last_ts      TIMESTAMPTZ,
  last_close   NUMERIC,
  prev_close   NUMERIC,
  chg_pct      NUMERIC,
  cum_vol      BIGINT,
  avg_vol_20d  BIGINT,
  session_frac NUMERIC,
  rvol         NUMERIC,
  updated_at   TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE hourly_feed ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS anon_read_hourly_feed ON hourly_feed;
CREATE POLICY anon_read_hourly_feed ON hourly_feed FOR SELECT TO anon USING (true);

-- Daily RVOL movers (all stocks): EOD change + volume vs the 20-day average.
-- Written by ingestion/scan_rvol.py (via scan_all.py).
CREATE TABLE IF NOT EXISTS scanner_rvol (
  run_date     DATE NOT NULL,
  symbol       TEXT NOT NULL REFERENCES stocks(symbol),
  close        NUMERIC,
  chg_pct      NUMERIC,
  volume       BIGINT,
  avg_vol_20d  BIGINT,
  rvol         NUMERIC,
  PRIMARY KEY (run_date, symbol)
);
ALTER TABLE scanner_rvol ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS anon_read_scanner_rvol ON scanner_rvol;
CREATE POLICY anon_read_scanner_rvol ON scanner_rvol FOR SELECT TO anon USING (true);

-- JFS multi-screen scanner: per-stock metrics (RS 1-99 percentile, ADR%, RVOL,
-- ATR extension, returns, turnover) + 10 screen flags (focus, rs_leaders,
-- hot_adr, movers, vcp, pullback, rvol, breakout, ipo, parabolic).
-- Only rows passing >= 1 screen are stored; full refresh by scan_screens.py.
-- Read live by the standalone dashboard_india.html (JFS-style UI).
CREATE TABLE IF NOT EXISTS scanner_screens (
  run_date     DATE NOT NULL,
  symbol       TEXT NOT NULL REFERENCES stocks(symbol),
  close        NUMERIC,
  rs_rating    NUMERIC,
  adr20        NUMERIC,
  rvol         NUMERIC,
  ext_atr      NUMERIC,
  off_high_pct NUMERIC,
  ret_1d NUMERIC, ret_1w NUMERIC, ret_1m NUMERIC, ret_3m NUMERIC, ret_6m NUMERIC,
  turnover_cr  NUMERIC,
  composite    NUMERIC,
  s_focus BOOLEAN DEFAULT FALSE, s_rs_leaders BOOLEAN DEFAULT FALSE,
  s_hot_adr BOOLEAN DEFAULT FALSE, s_movers BOOLEAN DEFAULT FALSE,
  s_vcp BOOLEAN DEFAULT FALSE, s_pullback BOOLEAN DEFAULT FALSE,
  s_rvol BOOLEAN DEFAULT FALSE, s_breakout BOOLEAN DEFAULT FALSE,
  s_ipo BOOLEAN DEFAULT FALSE, s_parabolic BOOLEAN DEFAULT FALSE,
  PRIMARY KEY (run_date, symbol)
);
ALTER TABLE scanner_screens ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS anon_read_scanner_screens ON scanner_screens;
CREATE POLICY anon_read_scanner_screens ON scanner_screens FOR SELECT TO anon USING (true);

-- AAII Investor Sentiment Survey — weekly US individual-investor poll, one row
-- per week. Populated by ingestion/fetch_aaii.py. Contrarian risk gauge.
CREATE TABLE IF NOT EXISTS aaii_sentiment (
  week_ending      DATE PRIMARY KEY,
  bullish          NUMERIC,
  neutral          NUMERIC,
  bearish          NUMERIC,
  bull_bear_spread NUMERIC,
  updated_at       TIMESTAMPTZ DEFAULT now()
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

-- VCP Pro (Minervini-style): trend template + shrinking contractions + volume
-- dry-up + breakout state machine. One row per symbol (most recent base in the
-- trailing 3 months). Full-refresh by ingestion/scan_vcp_pro.py.
-- status: ACTIVE (armed, waiting for breakout) / FIRED (buy signal, return_pct
-- tracks signal close -> latest close) / FAILED (fell below 50-DMA).
CREATE TABLE IF NOT EXISTS scanner_vcp_pro (
  run_date          DATE NOT NULL,
  symbol            TEXT NOT NULL REFERENCES stocks(symbol),
  status            TEXT NOT NULL,
  setup_date        DATE,
  signal_date       DATE,
  pivot             NUMERIC,
  stop_loss         NUMERIC,
  num_contractions  INTEGER,
  depths            TEXT,
  first_depth_pct   NUMERIC,
  last_depth_pct    NUMERIC,
  signal_close      NUMERIC,
  signal_vol_ratio  NUMERIC,
  last_date         DATE,
  last_close        NUMERIC,
  return_pct        NUMERIC,
  pct_to_pivot      NUMERIC,
  created_at        TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (run_date, symbol)
);
ALTER TABLE scanner_vcp_pro ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS anon_read_scanner_vcp_pro ON scanner_vcp_pro;
CREATE POLICY anon_read_scanner_vcp_pro ON scanner_vcp_pro
  FOR SELECT TO anon USING (true);

-- 10%+ move alerts (calibrated "ignition" scanner): stocks primed for a 10%+
-- upside move within ~10 sessions. Score 0-100 built from typical-ATR energy,
-- RVOL surge on an up day, trend, levels and accumulation — weights calibrated
-- against the measured 1y hit rate (see scanner_move_validation). Tiers:
-- READY >= 70, SETUP 55-69, WATCH 45-54. Written by scan_move_alerts.py.
CREATE TABLE IF NOT EXISTS scanner_move_alerts (
  run_date           DATE NOT NULL,
  symbol             TEXT NOT NULL REFERENCES stocks(symbol),
  close              NUMERIC,
  chg_pct            NUMERIC,
  score              INTEGER,
  tier               TEXT,                                 -- 'READY'|'SETUP'|'WATCH'
  rvol               NUMERIC,
  atr_typ_pct        NUMERIC,                              -- typical (median) ATR%
  squeeze_pctile     NUMERIC,                              -- BB-width 1y percentile
  dist_20d_high_pct  NUMERIC,                              -- <=0 means broken out
  dist_52w_high_pct  NUMERIC,
  updown_vol         NUMERIC,                              -- 20d up/down volume ratio
  trigger_price      NUMERIC,                              -- prior 20-day high
  target_price       NUMERIC,                              -- +10%
  stop_price         NUMERIC,                              -- -4%
  reasons            JSONB,                                -- list of reason strings
  PRIMARY KEY (run_date, symbol)
);
CREATE INDEX IF NOT EXISTS idx_scanner_move_alerts_score
  ON scanner_move_alerts (run_date, score DESC);
ALTER TABLE scanner_move_alerts ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS anon_read_scanner_move_alerts ON scanner_move_alerts;
CREATE POLICY anon_read_scanner_move_alerts ON scanner_move_alerts
  FOR SELECT TO anon USING (true);

-- Self-validation for the move-alert scanner: hit rate of each candidate
-- signal over the trailing year (+10% touch within fwd_window sessions) vs
-- the matched base rate. One row per run_date; variants is a JSONB list of
-- {name, days, hits, hit_rate, lift}. Written by scan_move_alerts.py.
CREATE TABLE IF NOT EXISTS scanner_move_validation (
  run_date       DATE PRIMARY KEY,
  fwd_window     INTEGER,
  target_move    NUMERIC,
  base_days      INTEGER,
  base_hit_rate  NUMERIC,
  variants       JSONB
);
ALTER TABLE scanner_move_validation ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS anon_read_scanner_move_validation ON scanner_move_validation;
CREATE POLICY anon_read_scanner_move_validation ON scanner_move_validation
  FOR SELECT TO anon USING (true);

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
