"""
scan_all.py  —  Orchestrator: runs all daily scanners in sequence.

Loads a single DB connection and passes it to each scanner module.
All scanners default to the most recent trading date in zerodha_ohlcv.

Usage:
    python ingestion/scan_all.py              # run all scanners
    python ingestion/scan_all.py --date 2026-06-18  # specific date
    python ingestion/scan_all.py --skip breadth,sectors  # skip some
"""

import argparse
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path(__file__).parent / "scan_all.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run all NSE daily scanners")
    parser.add_argument("--date",  type=str, default=None, help="Override run date YYYY-MM-DD")
    parser.add_argument("--skip",  type=str, default="",   help="Comma-separated scanner names to skip")
    args = parser.parse_args()

    skip_set = {s.strip().lower() for s in args.skip.split(",") if s.strip()}
    run_date = date.fromisoformat(args.date) if args.date else None

    # Import here so each module picks up logging config above
    from scan_breadth    import run as run_breadth
    from scan_breakouts  import run as run_breakouts
    from scan_ep         import run as run_ep
    from scan_sectors    import run as run_sectors
    from scan_vcp        import run as run_vcp

    scanners = [
        ("breakouts", run_breakouts),
        ("ep",        run_ep),
        ("vcp",       run_vcp),
        ("sectors",   run_sectors),
        ("breadth",   run_breadth),
    ]

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    total_start = time.time()
    results = {}

    try:
        for name, fn in scanners:
            if name in skip_set:
                log.info("Skipping scanner: %s", name)
                results[name] = "skipped"
                continue

            t0 = time.time()
            try:
                if name == "breadth":
                    # breadth scanner has different signature
                    import pandas as pd
                    nifty50 = _load_nifty50(conn)
                    ohlcv   = pd.read_sql(
                        "SELECT symbol, time::date AS date, close FROM zerodha_ohlcv ORDER BY date",
                        conn, parse_dates=["date"],
                    )
                    from scan_breadth import compute_breadth, write_breadth, load_existing_dates
                    result_df = compute_breadth(ohlcv, nifty50)
                    existing  = load_existing_dates(conn)
                    new_dates = {d for d in result_df.index if d not in existing}
                    write_breadth(conn, result_df, dates_filter=new_dates or None)
                    count = len(new_dates)
                else:
                    count = fn(conn, run_date=run_date)
                elapsed = time.time() - t0
                results[name] = f"{count} rows in {elapsed:.1f}s"
                log.info("✓ %s: %s", name, results[name])
            except Exception as e:
                results[name] = f"FAILED: {e}"
                log.error("✗ %s: %s", name, e, exc_info=True)
    finally:
        conn.close()

    total = time.time() - total_start
    log.info("=" * 55)
    log.info("Scan complete in %.1fs", total)
    for name, status in results.items():
        log.info("  %-12s %s", name, status)
    log.info("=" * 55)


def _load_nifty50(conn):
    import pandas as pd
    try:
        df = pd.read_sql(
            "SELECT time::date AS date, close FROM zerodha_index_ohlcv WHERE symbol='NIFTY 50' ORDER BY date",
            conn, parse_dates=["date"],
        )
        return df.set_index("date")["close"]
    except Exception:
        return pd.Series(dtype=float)


if __name__ == "__main__":
    main()
