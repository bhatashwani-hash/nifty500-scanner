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

## Scanners
All five scanners run over the **500-stock universe** — the `stocks` table (universe)
joined to `ohlcv` (prices). They do **not** read the legacy `zerodha_*` tables.

Run them all for the latest trading day:
```
python ingestion/scan_all.py
# options: --date YYYY-MM-DD   --skip breakouts,sectors
```
Or run market breadth on its own (back-fills missing dates by default):
```
python ingestion/scan_breadth.py            # missing dates only
python ingestion/scan_breadth.py --full     # recompute all history
```

| Scanner | Output table | What it flags |
|---|---|---|
| `scan_breadth`   | `scanner_breadth`   | Daily breadth (Worden T2107/T2108-style): up/down 4%, 5d/10d ratios, % above 200-DMA, NIFTY 50 close |
| `scan_breakouts` | `scanner_breakouts` | New closing highs/lows over 1M/3M/6M/1Y/2Y |
| `scan_ep`        | `scanner_ep`        | Episodic Pivots (gap ≥1%, move ≥7%, vol ≥3×). Keeps every pivot in the **last 6 months**, ranked by return from pivot close to latest close |
| `scan_vcp`       | `scanner_vcp`       | Volatility Contraction Pattern (3-month return ≥25%, 15-day range <15%) |
| `scan_sectors`   | `scanner_sectors`   | Sector performance over day/week/month |

## Dashboard
`dashboard/index.html` is a single static file (Bootstrap 5 + DataTables + Supabase
JS, all from CDN) — open it directly in a browser. Tabs: Breadth, Breakouts,
Episodic Pivot, VCP, Sectors. The Breadth tab shows the last 6 months of daily
breadth; the Episodic Pivot tab is a 6-month leaderboard ranked by return since
the pivot.

## Project structure
```
ingestion/   -- fetch_data.py (OHLCV ingest) + scan_*.py (the 5 scanners) + scan_all.py (orchestrator)
sql/         -- schema.sql: run once to set up tables, incl. the 5 scanner_* output tables (idempotent)
dashboard/   -- index.html: static results dashboard, no server needed
data/        -- nifty500_test.csv (stock list), indices.csv (index list)
```
