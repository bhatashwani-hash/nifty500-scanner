"""
enrich_zerodha_stocks.py

Populates three new columns in zerodha_stocks:
  - sector    (Yahoo Finance, e.g. "Financial Services")
  - industry  (Yahoo Finance, e.g. "Banks - Regional")
  - is_fno    (True if stock trades in NSE F&O segment)

Run LOCALLY — requires internet access (Yahoo Finance + Kite instruments CSV).

    pip install yfinance psycopg2-binary python-dotenv requests tqdm
    python ingestion/enrich_zerodha_stocks.py

Re-running is safe: already-populated rows are skipped unless --force is passed.
Progress is saved to enrich_progress.json so you can resume after interruptions.
"""

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
import yfinance as yf
from dotenv import load_dotenv

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
load_dotenv(SCRIPT_DIR.parent / ".env")
DB_URL = os.environ["DATABASE_URL"]

PROGRESS_FILE = SCRIPT_DIR / "enrich_progress.json"
LOG_FILE      = SCRIPT_DIR / "enrich_zerodha_stocks.log"

# Kite public instruments CSV — no API key required
KITE_NFO_URL = "https://api.kite.trade/instruments/NFO"

# yfinance concurrency — keep low to avoid rate-limiting
YF_WORKERS   = 4
YF_PAUSE     = 0.3   # seconds between batches

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ── FNO detection ─────────────────────────────────────────────────────────────

def fetch_fno_symbols() -> set[str]:
    """
    Download the Kite NFO instruments CSV and return the set of unique
    underlying stock symbols that have Stock Futures (FUTSTK) contracts.
    Falls back to a local file if the download fails.
    """
    log.info("Fetching NFO instruments from %s …", KITE_NFO_URL)
    try:
        resp = requests.get(KITE_NFO_URL, timeout=30)
        resp.raise_for_status()
        csv_text = resp.text
    except Exception as e:
        # Check if user dropped the file manually
        fallback = SCRIPT_DIR.parent / "data" / "kite_nfo_instruments.csv"
        if fallback.exists():
            log.warning("Network error (%s). Using local fallback: %s", e, fallback)
            csv_text = fallback.read_text()
        else:
            log.error(
                "Could not fetch NFO instruments: %s\n"
                "You can download it manually:\n"
                "  curl -o data/kite_nfo_instruments.csv %s\n"
                "then re-run this script.",
                e, KITE_NFO_URL,
            )
            sys.exit(1)

    import csv
    fno_symbols: set[str] = set()
    reader = csv.DictReader(StringIO(csv_text))
    for row in reader:
        if row.get("instrument_type", "").strip() == "FUTSTK":
            name = row.get("name", "").strip()
            if name:
                fno_symbols.add(name)

    log.info("Found %d unique F&O underlying stocks", len(fno_symbols))
    return fno_symbols


# ── Sector / industry via Yahoo Finance ───────────────────────────────────────

def _fetch_one(symbol: str) -> tuple[str, str | None, str | None]:
    """Fetch sector + industry for a single NSE symbol. Returns (symbol, sector, industry)."""
    try:
        info = yf.Ticker(f"{symbol}.NS").info
        sector   = info.get("sector")   or None
        industry = info.get("industry") or None
        return symbol, sector, industry
    except Exception as e:
        log.debug("yfinance error for %s: %s", symbol, e)
        return symbol, None, None


def fetch_sectors(symbols: list[str], progress: dict) -> dict[str, tuple]:
    """
    Fetch sector/industry for all symbols in parallel.
    Skips symbols already in `progress`.
    Returns dict: symbol → (sector, industry)
    """
    todo = [s for s in symbols if s not in progress]
    log.info("%d symbols to enrich (%d already done)", len(todo), len(symbols) - len(todo))

    results: dict[str, tuple] = dict(progress)  # carry forward cached results

    if not todo:
        return results

    bar = tqdm(total=len(todo), desc="Yahoo Finance") if HAS_TQDM else None

    with ThreadPoolExecutor(max_workers=YF_WORKERS) as pool:
        futures = {pool.submit(_fetch_one, sym): sym for sym in todo}
        batch_count = 0
        for fut in as_completed(futures):
            sym, sector, industry = fut.result()
            results[sym] = (sector, industry)
            batch_count += 1

            if bar:
                bar.update(1)
            elif batch_count % 50 == 0:
                log.info("  %d / %d symbols enriched …", batch_count, len(todo))

            # Save progress checkpoint every 100 symbols
            if batch_count % 100 == 0:
                _save_progress(results)

            # Throttle
            time.sleep(YF_PAUSE / YF_WORKERS)

    if bar:
        bar.close()

    _save_progress(results)
    return results


def _save_progress(results: dict):
    with open(PROGRESS_FILE, "w") as f:
        # Serialize tuples as lists for JSON
        json.dump({k: list(v) for k, v in results.items()}, f)


def _load_progress() -> dict:
    if PROGRESS_FILE.exists():
        raw = json.loads(PROGRESS_FILE.read_text())
        return {k: tuple(v) for k, v in raw.items()}
    return {}


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_all_symbols(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM zerodha_stocks ORDER BY symbol")
        return [r[0] for r in cur.fetchall()]


def update_fno(conn, fno_symbols: set[str]):
    log.info("Updating is_fno …")
    with conn.cursor() as cur:
        # Reset all to false first (in case of re-run)
        cur.execute("UPDATE zerodha_stocks SET is_fno = false")
        if fno_symbols:
            psycopg2.extras.execute_values(
                cur,
                "UPDATE zerodha_stocks SET is_fno = true WHERE symbol = data.sym "
                "FROM (VALUES %s) AS data(sym) WHERE zerodha_stocks.symbol = data.sym",
                [(s,) for s in fno_symbols],
            )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM zerodha_stocks WHERE is_fno = true")
        count = cur.fetchone()[0]
    log.info("Marked %d stocks as FNO", count)


def update_sectors(conn, sector_data: dict[str, tuple]):
    log.info("Writing sector/industry to DB …")
    rows = [
        (sector, industry, symbol)
        for symbol, (sector, industry) in sector_data.items()
        if sector or industry
    ]
    if not rows:
        log.warning("No sector data to write")
        return

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            UPDATE zerodha_stocks
            SET sector = data.sector, industry = data.industry
            FROM (VALUES %s) AS data(sector, industry, symbol)
            WHERE zerodha_stocks.symbol = data.symbol
            """,
            rows,
            template="(%s, %s, %s)",
        )
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM zerodha_stocks WHERE sector IS NOT NULL")
        count = cur.fetchone()[0]
    log.info("Sector populated for %d stocks", count)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Enrich zerodha_stocks with sector + FNO flag")
    parser.add_argument("--force", action="store_true", help="Re-fetch all symbols even if cached")
    parser.add_argument("--skip-sector", action="store_true", help="Skip Yahoo Finance sector fetch")
    parser.add_argument("--skip-fno",    action="store_true", help="Skip FNO flag update")
    args = parser.parse_args()

    conn = psycopg2.connect(DB_URL)
    try:
        symbols = get_all_symbols(conn)
        log.info("Loaded %d symbols from zerodha_stocks", len(symbols))

        # ── Step 1: FNO flag ──────────────────────────────────────────────────
        if not args.skip_fno:
            fno_symbols = fetch_fno_symbols()
            update_fno(conn, fno_symbols)
        else:
            log.info("Skipping FNO update (--skip-fno)")

        # ── Step 2: Sector / industry via Yahoo Finance ───────────────────────
        if not args.skip_sector:
            progress = {} if args.force else _load_progress()
            if args.force and PROGRESS_FILE.exists():
                PROGRESS_FILE.unlink()

            sector_data = fetch_sectors(symbols, progress)
            update_sectors(conn, sector_data)

            # Clean up progress file on success
            if PROGRESS_FILE.exists():
                PROGRESS_FILE.unlink()
        else:
            log.info("Skipping sector update (--skip-sector)")

    finally:
        conn.close()

    log.info("Done.")


if __name__ == "__main__":
    main()
