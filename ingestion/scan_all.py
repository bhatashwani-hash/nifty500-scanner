"""
scan_all.py  —  Orchestrator: runs all daily scanners in sequence.

Loads a single DB connection and passes it to each scanner module.
All scanners default to the most recent trading date in ohlcv.

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
    from scan_breakouts   import run as run_breakouts
    from scan_ep          import run as run_ep
    from scan_manas       import run as run_manas
    from scan_move_alerts import run as run_move_alerts
    from scan_rvol        import run as run_rvol
    from scan_screens     import run as run_screens
    from scan_sectors     import run as run_sectors
    from scan_snapback    import run as run_snapback
    from scan_vcp         import run as run_vcp
    from scan_vcp_bear    import run as run_vcp_bear
    from scan_vcp_pro     import run as run_vcp_pro

    scanners = [
        ("breakouts", run_breakouts),   # fresh 6M/1Y/2Y highs & lows only
        ("ep",        run_ep),
        ("vcp",       run_vcp),
        ("vcp_pro",   run_vcp_pro),
        ("manas",     run_manas),
        ("move",      run_move_alerts), # 10%+ move alerts (calibrated ignition)
        ("rvol",      run_rvol),        # daily RVOL movers (all stocks)
        ("snapback",  run_snapback),    # undercut & rally snapbacks (F&O, last 5 days)
        ("vcp_bear",  run_vcp_bear),    # inverse VCP short setups (fall + contraction + falling 20DMA)
        ("screens",   run_screens),     # JFS multi-screen scanner (10 screens)
        ("sectors",   run_sectors),
        ("breadth",   None),   # handled separately below via compute_breadth/write_breadth
    ]

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    total_start = time.time()
    results = {}

    # Refresh the liquidity gate before scanning: median 20d turnover >= 5 Cr,
    # traded >= 90% of sessions, < 3 circuit-locked (high = low) days.
    # Signal scanners filter on stocks.is_liquid; breadth/sectors stay full-universe.
    LIQUIDITY_SQL = """
        WITH win AS (
          SELECT COUNT(DISTINCT time::date) AS tot_days
          FROM ohlcv WHERE time >= CURRENT_DATE - INTERVAL '35 days'
        ),
        m AS (
          SELECT symbol,
                 PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY close * volume) AS med_turnover,
                 COUNT(*) AS days,
                 SUM(CASE WHEN high = low THEN 1 ELSE 0 END) AS locked
          FROM ohlcv
          WHERE time >= CURRENT_DATE - INTERVAL '35 days'
          GROUP BY symbol
        )
        UPDATE stocks s
        SET is_liquid = COALESCE(
              m.med_turnover >= 5e7
              AND m.days >= (SELECT tot_days FROM win) * 0.9
              AND m.locked < 3, false)
        FROM stocks base
        LEFT JOIN m ON m.symbol = base.symbol
        WHERE s.symbol = base.symbol
    """
    try:
        with conn.cursor() as cur:
            cur.execute(LIQUIDITY_SQL)
            cur.execute("SELECT COUNT(*) FROM stocks WHERE is_liquid")
            n_liquid = cur.fetchone()[0]
        conn.commit()
        log.info("Liquidity gate refreshed: %d liquid symbols", n_liquid)
    except Exception as e:
        conn.rollback()
        log.warning("Liquidity refresh failed (scanners use previous flags): %s", e)

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
                        """SELECT o.symbol, o.time::date AS date, o.close
                           FROM ohlcv o
                           JOIN stocks s ON o.symbol = s.symbol
                           WHERE s.is_active = true
                           ORDER BY date""",
                        conn, parse_dates=["date"],
                    )
                    from scan_breadth import compute_breadth, write_breadth, load_existing_dates
                    result_df = compute_breadth(ohlcv, nifty50)
                    existing  = load_existing_dates(conn)
                    # Also rewrite the trailing week: rows once written from a
                    # partial ingest (low universe, missing NIFTY close) self-heal
                    # on the next run instead of being frozen forever.
                    recent    = set(sorted(result_df.index)[-7:])
                    new_dates = {d for d in result_df.index if d not in existing} | recent
                    write_breadth(conn, result_df, dates_filter=new_dates)
                    count = len(new_dates)
                else:
                    count = fn(conn, run_date=run_date)
                elapsed = time.time() - t0
                results[name] = f"{count} rows in {elapsed:.1f}s"
                log.info("OK %s: %s", name, results[name])
            except Exception as e:
                results[name] = f"FAILED: {e}"
                log.error("FAIL %s: %s", name, e, exc_info=True)
                try:
                    conn.rollback()
                except Exception:
                    pass
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
            "SELECT time::date AS date, close FROM index_ohlcv WHERE symbol='^NSEI' ORDER BY date",
            conn, parse_dates=["date"],
        )
        return df.set_index("date")["close"]
    except Exception:
        return pd.Series(dtype=float)


if __name__ == "__main__":
    main()

