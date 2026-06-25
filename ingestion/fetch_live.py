"""
fetch_live.py — delayed 15-minute intraday quotes for F&O stocks + indices.

Pulls today's 15-minute bars from Yahoo Finance (free, ~15-min delayed) for the
F&O universe (stocks.is_fno) plus the tracked indices, computes the intraday %
move vs the previous close, and upserts a single live row per symbol into the
`live_15m` table. The dashboard's F&O tab polls that table.

Run by .github/workflows/live_15m.yml every 15 min during market hours, or
locally:  python ingestion/fetch_live.py
"""

import os
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
DB_URL = os.environ["DATABASE_URL"]

UPSERT = """
INSERT INTO live_15m (symbol, name, is_index, last, prev_close, chg_pct, intraday, bar_ts, updated_at)
VALUES %s
ON CONFLICT (symbol) DO UPDATE SET
  name=EXCLUDED.name, is_index=EXCLUDED.is_index, last=EXCLUDED.last,
  prev_close=EXCLUDED.prev_close, chg_pct=EXCLUDED.chg_pct, intraday=EXCLUDED.intraday,
  bar_ts=EXCLUDED.bar_ts, updated_at=now()
"""


def load_universe(conn):
    """Return [(db_symbol, name, is_index, yf_symbol)] for F&O stocks + indices."""
    rows = []
    with conn.cursor() as cur:
        cur.execute("SELECT symbol, name FROM stocks WHERE is_active AND is_fno ORDER BY symbol")
        rows += [(s, n, False, f"{s}.NS") for s, n in cur.fetchall()]
        cur.execute("SELECT symbol, name FROM indices WHERE is_active ORDER BY symbol")
        rows += [(s, n, True, s) for s, n in cur.fetchall()]  # index symbols are used as-is
    return rows


def _col(df, ticker, field):
    """Pull a column from a yf.download frame whether single- or multi-ticker."""
    try:
        if isinstance(df.columns, pd.MultiIndex):
            return df[ticker][field].dropna()
        return df[field].dropna()
    except Exception:
        return pd.Series(dtype=float)


def main():
    conn = psycopg2.connect(DB_URL)
    uni = load_universe(conn)
    yf_syms = [u[3] for u in uni]
    print(f"Live fetch: {len(yf_syms)} symbols ({sum(1 for u in uni if u[2])} indices)")

    # Today's 15-minute bars + last 2 daily bars (for previous close), in batched downloads.
    intraday = yf.download(yf_syms, period="1d", interval="15m", group_by="ticker",
                           auto_adjust=False, threads=True, progress=False)
    daily = yf.download(yf_syms, period="5d", interval="1d", group_by="ticker",
                        auto_adjust=False, threads=True, progress=False)

    rows = []
    now = datetime.now(timezone.utc)
    for db_sym, name, is_idx, yf_sym in uni:
        closes = _col(intraday, yf_sym, "Close")
        if closes.empty:
            continue
        last = float(closes.iloc[-1])
        if math.isnan(last):
            continue
        bar_ts = closes.index[-1].to_pydatetime()

        dcloses = _col(daily, yf_sym, "Close")
        prev_close = None
        if len(dcloses) >= 2:
            # the last daily row may be today (forming); use the prior settled close
            prev_close = float(dcloses.iloc[-2])
        elif len(dcloses) == 1:
            prev_close = float(dcloses.iloc[-1])

        chg = round((last / prev_close - 1) * 100, 2) if prev_close else None
        series = [[ts.strftime("%H:%M"), round(float(c), 2)] for ts, c in closes.items() if not math.isnan(c)]

        rows.append((
            db_sym, name, is_idx, round(last, 2),
            round(prev_close, 2) if prev_close else None,
            chg, json.dumps(series), bar_ts, now,
        ))

    if rows:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT, rows, page_size=300)
        conn.commit()
    print(f"Upserted {len(rows)} live rows at {now.isoformat()}")
    conn.close()


if __name__ == "__main__":
    main()
