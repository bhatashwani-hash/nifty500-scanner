"""
Shared upsert helper for the Zerodha/Kite backfill pipeline.

Reads a JSON file containing OHLCV rows fetched from Kite and upserts them
directly into the zerodha_ohlcv table via psycopg2 (DATABASE_URL from .env),
bypassing the Supabase MCP for the actual write (per project decision to
script the bulk write step given the volume of data involved).

Input JSON format (a flat list of rows):
[
  ["SYMBOL", "2024-06-21T00:00:00+05:30", 57.82, 57.82, 57.82, 57.82, 2696],
  ...
]
Columns: symbol, time (ISO8601 string, any tz offset ok), open, high, low, close, volume

Usage:
    python kite_zerodha_upsert.py /path/to/rows.json

Prints per-symbol row counts upserted and a final total. Exits non-zero with
the exact exception on failure -- no silent retries or fallback behavior.
"""

import json
import os
import sys
from collections import Counter

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, "..", ".env"))

DB_URL = os.environ.get("DATABASE_URL")

UPSERT_SQL = """
    INSERT INTO zerodha_ohlcv (symbol, time, open, high, low, close, volume)
    VALUES %s
    ON CONFLICT (symbol, time) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume
"""


def main():
    if len(sys.argv) != 2:
        print("Usage: python kite_zerodha_upsert.py /path/to/rows.json", file=sys.stderr)
        sys.exit(1)

    input_path = sys.argv[1]
    if not DB_URL:
        print("ERROR: DATABASE_URL not found in .env", file=sys.stderr)
        sys.exit(1)

    with open(input_path, "r", encoding="utf-8") as f:
        rows = json.load(f)

    if not isinstance(rows, list) or len(rows) == 0:
        print(f"ERROR: input file {input_path} did not contain a non-empty list", file=sys.stderr)
        sys.exit(1)

    # Validate row shape early -- fail loudly rather than letting psycopg2
    # throw a confusing error deep into a partial batch.
    for i, r in enumerate(rows):
        if len(r) != 7:
            print(f"ERROR: row {i} does not have 7 fields: {r}", file=sys.stderr)
            sys.exit(1)

    tuples = [tuple(r) for r in rows]
    counts = Counter(r[0] for r in rows)

    conn = psycopg2.connect(DB_URL)
    try:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, tuples, page_size=500)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"ERROR during upsert: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

    for sym, cnt in counts.items():
        print(f"{sym}: {cnt} rows upserted")
    print(f"TOTAL: {len(tuples)} rows upserted across {len(counts)} symbols")


if __name__ == "__main__":
    main()
