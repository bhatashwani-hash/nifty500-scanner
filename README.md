# Nifty 500 EOD Scanner

## Setup

1. **Database**: Create a free Supabase project at supabase.com.
   - Go to Settings -> Database -> Connection string -> copy the URI (Transaction pooling mode)
   - Go to the SQL Editor, paste the contents of `sql/schema.sql`, and run it

2. **Local environment**:
   ```
   pip install -r requirements.txt
   cp .env.example .env
   # edit .env and paste your Supabase connection string into DATABASE_URL
   ```

3. **Backfill 5 years of history** (all 500 Nifty stocks + 16 tracked indices):
   ```
   python ingestion/fetch_data.py --mode backfill --years 5
   ```
   Use `--universe stocks` or `--universe indices` to run just one half (e.g.
   if stocks are already loaded and you only need to backfill indices):
   ```
   python ingestion/fetch_data.py --mode backfill --years 5 --universe indices
   ```

4. **Daily run** (what GitHub Actions will eventually run automatically):
   ```
   python ingestion/fetch_data.py --mode daily
   ```
   Also supports `--universe stocks|indices|both` (default `both`).

## Swapping in the real Nifty 500 list
Replace `data/nifty500_test.csv` with the real NSE file. It must keep the
same three columns: `Symbol`, `Company Name`, `Industry`. Nothing else
needs to change.

## Indices
`data/indices.csv` lists 16 benchmark + sectoral indices (NIFTY 50, NIFTY 500,
NIFTY BANK, SENSEX, NIFTY IT/AUTO/PHARMA/FMCG/METAL/ENERGY/REALTY/FIN SERVICE/
PSU BANK, NIFTY NEXT 50, NIFTY MIDCAP 150, NIFTY SMALLCAP 250) with their
Yahoo Finance ticker symbols. Unlike stocks, these symbols are used exactly
as listed — some are `^`-prefixed (e.g. `^NSEI`), some are `.NS`-suffixed
(e.g. `NIFTYMIDCAP150.NS`); no suffix is added automatically. Index data is
stored separately from stock data, in an `indices` reference table and an
`index_ohlcv` price table (mirrors the `stocks`/`ohlcv` structure). Add or
remove rows in `data/indices.csv` to change what's tracked — no code changes
needed as long as the `Symbol,Name,Category` columns stay the same.

## Project structure
```
ingestion/   -- fetch_data.py: pulls OHLCV data for stocks + indices, upserts into Postgres
sql/         -- schema.sql: run once to set up tables (safe to re-run, all statements are idempotent)
scanners/    -- (next step) scanner logic
api/         -- (next step) backend API to serve scan results
data/        -- nifty500_test.csv (stock list), indices.csv (index list)
```
