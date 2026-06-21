"""
scan_breadth.py

Computes daily market breadth indicators for the full NSE universe and writes
them to the scanner_breadth table in Supabase. Mirrors the Worden-style
breadth table but uses the `ohlcv` table (500-stock universe) as the data source.

Indicators computed per trading day:

  PRIMARY
  -------
  up_4pct / down_4pct   — stocks with daily return ≥ +4% / ≤ -4%
  ratio_5d              — sum(up_4pct last 5d) / sum(down_4pct last 5d)
  ratio_10d             — same, 10-day window

  SECONDARY
  ---------
  up/down_25pct_3m      — return vs close 63 trading days ago ≥ ±25%
  up/down_25pct_1m      — return vs close 21 trading days ago ≥ ±25%
  up/down_50pct_1m      — return vs close 21 trading days ago ≥ ±50%
  up/down_13pct_34d     — return vs close 34 trading days ago ≥ ±13%
  universe              — count of stocks with a close on that date
  pct_above_200ma       — T2108 equivalent: % of stocks above 200-day SMA
  nifty50_close         — NIFTY 50 close (^NSEI from index_ohlcv if available)

Run LOCALLY after yf_zerodha_backfill.py:
    python ingestion/scan_breadth.py

By default only fills missing dates. Use --full to recalculate all history.
Use --date YYYY-MM-DD to update a single day.
"""

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).parent
load_dotenv(SCRIPT_DIR.parent / ".env")
DB_URL = os.environ["DATABASE_URL"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(SCRIPT_DIR / "scan_breadth.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ── Data loading ──────────────────────────────────────────────────────────────

def load_ohlcv(conn) -> pd.DataFrame:
    """Load the 500-stock universe from ohlcv into a wide pivot: index=date, columns=symbol."""
    log.info("Loading ohlcv (500-stock universe) …")
    df = pd.read_sql(
        """SELECT o.symbol, o.time::date AS date, o.close
           FROM ohlcv o
           JOIN stocks s ON o.symbol = s.symbol
           WHERE s.is_active = true
           ORDER BY date""",
        conn,
        parse_dates=["date"],
    )
    log.info("  %d rows loaded, %d symbols, %d dates",
             len(df), df["symbol"].nunique(), df["date"].nunique())
    return df


def load_nifty50(conn) -> pd.Series:
    """
    Load NIFTY 50 (^NSEI) daily close from index_ohlcv (if populated).
    Returns a Series indexed by date. Empty Series if table is missing.
    """
    try:
        df = pd.read_sql(
            """
            SELECT time::date AS date, close
            FROM index_ohlcv
            WHERE symbol = '^NSEI'
            ORDER BY date
            """,
            conn,
            parse_dates=["date"],
        )
        return df.set_index("date")["close"]
    except Exception as e:
        log.warning("Could not load NIFTY 50 from index table: %s", e)
        return pd.Series(dtype=float)


def load_existing_dates(conn) -> set:
    with conn.cursor() as cur:
        cur.execute("SELECT date FROM scanner_breadth")
        return {r[0] for r in cur.fetchall()}

# ── Computation ───────────────────────────────────────────────────────────────

def compute_breadth(df_raw: pd.DataFrame, nifty50: pd.Series) -> pd.DataFrame:
    """
    Given long-form OHLCV dataframe, compute breadth for every trading date.
    Returns a DataFrame with one row per date and all breadth columns.
    """
    # Pivot to wide: rows=date, cols=symbol, values=close
    close = df_raw.pivot(index="date", columns="symbol", values="close").sort_index()

    log.info("Computing daily returns …")
    ret_1d  = close.pct_change(1)   # vs yesterday

    # Lookback returns — use trading-day offsets (not calendar days)
    log.info("Computing lookback returns (21d / 34d / 63d) …")
    ret_21d = close.pct_change(21)
    ret_34d = close.pct_change(34)
    ret_63d = close.pct_change(63)

    # 200-day SMA per symbol
    log.info("Computing 200-day SMA …")
    sma200  = close.rolling(200, min_periods=150).mean()
    above_200 = (close > sma200)

    # Build results row by row
    log.info("Building daily breadth rows …")
    rows = []

    all_dates = close.index.tolist()

    for i, dt in enumerate(all_dates):
        universe = int(close.loc[dt].notna().sum())
        if universe == 0:
            continue

        r1  = ret_1d.loc[dt]
        r21 = ret_21d.loc[dt]
        r34 = ret_34d.loc[dt]
        r63 = ret_63d.loc[dt]
        ab200 = above_200.loc[dt]

        row = {
            "date":           dt.date() if hasattr(dt, "date") else dt,
            "up_4pct":        int((r1  >=  0.04).sum()),
            "down_4pct":      int((r1  <= -0.04).sum()),
            "up_25pct_3m":    int((r63 >=  0.25).sum()),
            "down_25pct_3m":  int((r63 <= -0.25).sum()),
            "up_25pct_1m":    int((r21 >=  0.25).sum()),
            "down_25pct_1m":  int((r21 <= -0.25).sum()),
            "up_50pct_1m":    int((r21 >=  0.50).sum()),
            "down_50pct_1m":  int((r21 <= -0.50).sum()),
            "up_13pct_34d":   int((r34 >=  0.13).sum()),
            "down_13pct_34d": int((r34 <= -0.13).sum()),
            "universe":       universe,
            "pct_above_200ma": round(float(ab200.sum()) / max(ab200.notna().sum(), 1) * 100, 2),
            "nifty50_close":  float(nifty50.get(dt, np.nan)) if not nifty50.empty else None,
        }
        rows.append(row)

        if (i + 1) % 50 == 0:
            log.info("  %d / %d dates processed …", i + 1, len(all_dates))

    result = pd.DataFrame(rows).set_index("date").sort_index()

    # Rolling ratios — computed on the result DataFrame
    log.info("Computing 5d / 10d rolling ratios …")
    up_s   = result["up_4pct"].astype(float)
    down_s = result["down_4pct"].astype(float).replace(0, np.nan)  # avoid div/0

    result["ratio_5d"]  = (up_s.rolling(5).sum()  / down_s.rolling(5).sum()).round(4)
    result["ratio_10d"] = (up_s.rolling(10).sum() / down_s.rolling(10).sum()).round(4)

    return result


# ── DB write ──────────────────────────────────────────────────────────────────

UPSERT_SQL = """
    INSERT INTO scanner_breadth (
        date, up_4pct, down_4pct, ratio_5d, ratio_10d,
        up_25pct_3m, down_25pct_3m,
        up_25pct_1m, down_25pct_1m,
        up_50pct_1m, down_50pct_1m,
        up_13pct_34d, down_13pct_34d,
        universe, pct_above_200ma, nifty50_close
    ) VALUES %s
    ON CONFLICT (date) DO UPDATE SET
        up_4pct          = EXCLUDED.up_4pct,
        down_4pct        = EXCLUDED.down_4pct,
        ratio_5d         = EXCLUDED.ratio_5d,
        ratio_10d        = EXCLUDED.ratio_10d,
        up_25pct_3m      = EXCLUDED.up_25pct_3m,
        down_25pct_3m    = EXCLUDED.down_25pct_3m,
        up_25pct_1m      = EXCLUDED.up_25pct_1m,
        down_25pct_1m    = EXCLUDED.down_25pct_1m,
        up_50pct_1m      = EXCLUDED.up_50pct_1m,
        down_50pct_1m    = EXCLUDED.down_50pct_1m,
        up_13pct_34d     = EXCLUDED.up_13pct_34d,
        down_13pct_34d   = EXCLUDED.down_13pct_34d,
        universe         = EXCLUDED.universe,
        pct_above_200ma  = EXCLUDED.pct_above_200ma,
        nifty50_close    = EXCLUDED.nifty50_close
"""

COLS = [
    "date", "up_4pct", "down_4pct", "ratio_5d", "ratio_10d",
    "up_25pct_3m", "down_25pct_3m",
    "up_25pct_1m", "down_25pct_1m",
    "up_50pct_1m", "down_50pct_1m",
    "up_13pct_34d", "down_13pct_34d",
    "universe", "pct_above_200ma", "nifty50_close",
]


def write_breadth(conn, result: pd.DataFrame, dates_filter: set | None = None):
    df = result.reset_index()
    if dates_filter:
        df = df[df["date"].isin(dates_filter)]

    if df.empty:
        log.info("Nothing to write.")
        return

    def _safe(v):
        if v is None:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        return v

    tuples = [
        tuple(_safe(row[c]) for c in COLS)
        for _, row in df.iterrows()
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, UPSERT_SQL, tuples, page_size=500)
    conn.commit()
    log.info("Upserted %d breadth rows", len(tuples))


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Compute daily market breadth for NSE universe")
    parser.add_argument("--full",  action="store_true", help="Recalculate all history (not just missing dates)")
    parser.add_argument("--date",  type=str, default=None, help="Recompute a specific date YYYY-MM-DD")
    args = parser.parse_args()

    conn = psycopg2.connect(DB_URL)
    try:
        df_raw  = load_ohlcv(conn)
        nifty50 = load_nifty50(conn)

        result = compute_breadth(df_raw, nifty50)
        log.info("Computed breadth for %d trading days", len(result))

        if args.date:
            target = {date.fromisoformat(args.date)}
            write_breadth(conn, result, dates_filter=target)
        elif args.full:
            write_breadth(conn, result)
        else:
            # Only write dates not already in the table
            existing = load_existing_dates(conn)
            new_dates = {d for d in result.index if d not in existing}
            log.info("%d new dates to write (skipping %d existing)", len(new_dates), len(existing))
            write_breadth(conn, result, dates_filter=new_dates)

    finally:
        conn.close()

    log.info("Done.")


if __name__ == "__main__":
    main()
