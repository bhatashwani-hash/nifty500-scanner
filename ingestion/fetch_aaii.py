"""
fetch_aaii.py  —  AAII Investor Sentiment Survey → aaii_sentiment table.

The AAII weekly survey asks members whether they are bullish / neutral /
bearish on the next six months for US stocks. It's a widely-watched
contrarian risk-sentiment gauge.

AAII publishes the full weekly history as a downloadable spreadsheet. This
script downloads that file, parses the weekly rows, and upserts them into the
`aaii_sentiment` Supabase/Postgres table. Run it locally / via CI — the Cowork
sandbox has no general internet (same constraint as the yfinance jobs).

Usage:
    python ingestion/fetch_aaii.py                 # download from AAII_XLS_URL
    python ingestion/fetch_aaii.py --file sent.xls # parse a local file you saved
    python ingestion/fetch_aaii.py --weeks 260      # only keep the last N weeks

Notes:
  * The AAII historical file is an .xls — needs `xlrd`; newer .xlsx needs
    `openpyxl`. Both are in requirements.txt.
  * If AAII puts the file behind a member login, download it manually in your
    browser and pass it with --file.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
log = logging.getLogger(__name__)

# AAII's public historical sentiment file (override with env if it moves).
DEFAULT_URL = os.environ.get(
    "AAII_XLS_URL", "https://www.aaii.com/files/surveys/sentiment.xls"
)

UPSERT_SQL = """
    INSERT INTO aaii_sentiment (week_ending, bullish, neutral, bearish, bull_bear_spread)
    VALUES %s
    ON CONFLICT (week_ending) DO UPDATE SET
        bullish          = EXCLUDED.bullish,
        neutral          = EXCLUDED.neutral,
        bearish          = EXCLUDED.bearish,
        bull_bear_spread = EXCLUDED.bull_bear_spread,
        updated_at       = now()
"""


def _pct(v):
    """AAII stores fractions (0.366); normalise to a 0-100 percentage."""
    if pd.isna(v):
        return None
    v = float(v)
    return round(v * 100, 1) if abs(v) <= 1.5 else round(v, 1)


def load_frame(source) -> pd.DataFrame:
    """Read the AAII sheet and return tidy rows: week_ending, bull, neutral, bear."""
    raw = pd.read_excel(source, header=None)

    # Find the header row (the one containing 'Bullish').
    hdr = None
    for i in range(min(12, len(raw))):
        cells = [str(c).strip().lower() for c in raw.iloc[i].tolist()]
        if any("bullish" in c for c in cells) and any("bearish" in c for c in cells):
            hdr = i
            break
    if hdr is None:
        raise ValueError("Could not locate the AAII header row (no 'Bullish'/'Bearish').")

    df = pd.read_excel(source, header=hdr)
    df.columns = [str(c).strip().lower() for c in df.columns]

    def col(*names):
        for n in names:
            for c in df.columns:
                if c == n or c.startswith(n):
                    return c
        return None

    c_date = col("date", "reported date", "week ending")
    c_bull = col("bullish")
    c_neut = col("neutral")
    c_bear = col("bearish")
    if not all([c_date, c_bull, c_neut, c_bear]):
        raise ValueError(f"Missing expected columns. Found: {list(df.columns)}")

    out = pd.DataFrame({
        "week_ending": pd.to_datetime(df[c_date], errors="coerce"),
        "bullish": df[c_bull].map(_pct),
        "neutral": df[c_neut].map(_pct),
        "bearish": df[c_bear].map(_pct),
    }).dropna(subset=["week_ending", "bullish"])
    out["bull_bear_spread"] = (out["bullish"] - out["bearish"]).round(1)
    return out.sort_values("week_ending")


def main():
    ap = argparse.ArgumentParser(description="Ingest AAII sentiment survey")
    ap.add_argument("--file", help="Local .xls/.xlsx to parse instead of downloading")
    ap.add_argument("--weeks", type=int, default=0, help="Keep only the last N weeks (0 = all)")
    args = ap.parse_args()

    source = args.file or DEFAULT_URL
    log.info("fetch_aaii: reading %s", source)
    df = load_frame(source)
    if args.weeks:
        df = df.tail(args.weeks)

    rows = [(r.week_ending.date(), float(r.bullish), float(r.neutral),
             float(r.bearish), float(r.bull_bear_spread)) for r in df.itertuples()]
    if not rows:
        log.warning("fetch_aaii: no rows parsed")
        return

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=500)
        conn.commit()
        log.info("fetch_aaii: upserted %d weekly rows (%s … %s)",
                 len(rows), rows[0][0], rows[-1][0])
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        stream=sys.stdout)
    main()
