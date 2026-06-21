"""
Backfill zerodha_ohlcv with 2 years of daily OHLCV from Yahoo Finance.

Reads all symbols from zerodha_stocks, maps each to SYMBOL.NS (NSE), downloads
via yfinance in batches of 200, and upserts into zerodha_ohlcv.

Run from the project root (where .env lives):
    python ingestion/yf_zerodha_backfill.py

Requirements:
    pip install yfinance psycopg2-binary python-dotenv
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import yfinance as yf
from dotenv import load_dotenv

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
load_dotenv(SCRIPT_DIR.parent / ".env")

DB_URL = os.environ["DATABASE_URL"]

END_DATE   = datetime.now(timezone.utc).date()
START_DATE = END_DATE - timedelta(days=2 * 365)   # ~2 years

BATCH_SIZE   = 100    # tickers per yfinance.download() call
PAUSE_SECS   = 2      # pause between batches to respect rate limits
LOG_EVERY    = 10     # log progress every N symbols

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(SCRIPT_DIR / "yf_zerodha_backfill.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

UPSERT_SQL = """
    INSERT INTO zerodha_ohlcv (symbol, time, open, high, low, close, volume)
    VALUES %s
    ON CONFLICT (symbol, time) DO UPDATE SET
        open   = EXCLUDED.open,
        high   = EXCLUDED.high,
        low    = EXCLUDED.low,
        close  = EXCLUDED.close,
        volume = EXCLUDED.volume
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_symbols(conn):
    """Return all symbols from zerodha_stocks, ordered."""
    with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM zerodha_stocks ORDER BY symbol")
        return [row[0] for row in cur.fetchall()]


def download_batch(symbols_ns: list[str], start: str, end: str) -> dict:
    """
    Download daily OHLCV for a list of SYMBOL.NS tickers.
    Returns dict: { 'SYMBOL.NS': DataFrame }
    Empty or failed tickers silently return empty DataFrame.
    """
    if not symbols_ns:
        return {}

    try:
        raw = yf.download(
            tickers=symbols_ns,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=True,
            progress=False,
            threads=True,
            group_by="ticker",
        )
    except Exception as e:
        log.warning("yfinance batch download error: %s", e)
        return {}

    # If single ticker, yfinance returns flat DataFrame — wrap it
    if len(symbols_ns) == 1:
        return {symbols_ns[0]: raw}

    result = {}
    for ticker in symbols_ns:
        try:
            df = raw[ticker].dropna(how="all")
            result[ticker] = df
        except (KeyError, TypeError):
            result[ticker] = None
    return result


def rows_from_df(symbol: str, df) -> list[tuple]:
    """Convert a yfinance DataFrame into upsert-ready tuples."""
    if df is None or df.empty:
        return []

    tuples = []
    for ts, row in df.iterrows():
        # ts is a pandas Timestamp (tz-aware or naive)
        try:
            t = ts.to_pydatetime()
        except Exception:
            continue

        try:
            open_  = float(row["Open"])
            high   = float(row["High"])
            low    = float(row["Low"])
            close  = float(row["Close"])
            volume = int(row["Volume"])
        except (KeyError, ValueError, TypeError):
            continue

        if any(v != v for v in (open_, high, low, close)):  # NaN check
            continue

        tuples.append((symbol, t, open_, high, low, close, volume))

    return tuples


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    start_str = str(START_DATE)
    end_str   = str(END_DATE)
    log.info("Backfill from %s to %s", start_str, end_str)

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False

    try:
        symbols = get_symbols(conn)
        log.info("Loaded %d symbols from zerodha_stocks", len(symbols))

        total_rows   = 0
        total_ok     = 0
        total_empty  = 0
        failed       = []

        batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
        log.info("%d batches of up to %d symbols each", len(batches), BATCH_SIZE)

        for batch_idx, batch_syms in enumerate(batches, 1):
            tickers_ns = [f"{s}.NS" for s in batch_syms]
            log.info("Batch %d/%d — downloading %d tickers …",
                     batch_idx, len(batches), len(tickers_ns))

            data = download_batch(tickers_ns, start_str, end_str)

            upsert_rows = []
            for sym, ticker in zip(batch_syms, tickers_ns):
                df = data.get(ticker)
                rows = rows_from_df(sym, df)
                if rows:
                    upsert_rows.extend(rows)
                    total_ok += 1
                else:
                    total_empty += 1
                    log.debug("Empty: %s (%s)", sym, ticker)

            if upsert_rows:
                try:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_values(
                            cur, UPSERT_SQL, upsert_rows, page_size=1000
                        )
                    conn.commit()
                    total_rows += len(upsert_rows)
                    log.info(
                        "  ↳ Upserted %d rows | cumulative %d rows | "
                        "%d symbols ok / %d empty so far",
                        len(upsert_rows), total_rows, total_ok, total_empty,
                    )
                except Exception as e:
                    conn.rollback()
                    log.error("DB upsert failed for batch %d: %s", batch_idx, e)
                    failed.extend(batch_syms)

            if batch_idx < len(batches):
                time.sleep(PAUSE_SECS)

    finally:
        conn.close()

    log.info("Done. Total rows: %d | symbols with data: %d | empty/missing: %d",
             total_rows, total_ok, total_empty)
    if failed:
        log.warning("Batches with DB errors (%d symbols): %s", len(failed), failed)


if __name__ == "__main__":
    main()
