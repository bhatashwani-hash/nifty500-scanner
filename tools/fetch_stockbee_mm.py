"""
fetch_stockbee_mm.py — download EVERY year tab of the Stockbee Market Monitor
breadth spreadsheet and build one formatted Excel workbook.

The Stockbee "Market Monitor" page (https://stockbee.blogspot.com/p/mm.html)
embeds a Google Sheet whose tabs are one-per-year (2026, 2025, 2024, …) plus
"Chart YYYY" tabs. Each year tab is downloadable as CSV via the gviz endpoint.

This script fetches each year tab, normalises the columns, de-duplicates the
December overlaps between adjacent year tabs, and writes:
  * one worksheet per year, plus
  * a combined "All Years" worksheet (oldest → newest)
to an .xlsx in your Downloads folder.

Run locally (needs internet):
    pip install requests pandas openpyxl
    python tools/fetch_stockbee_mm.py
    python tools/fetch_stockbee_mm.py --start 2007 --end 2026 --out "C:/Users/you/Downloads/Stockbee_MM.xlsx"
"""

import argparse
import io
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

SHEET_ID = "1O6OhS7ciA8zwfycBfGPbP2fWJnR0pn2UUvFZVDP9jpE"
CSV_URL = ("https://docs.google.com/spreadsheets/d/{sid}/gviz/tq"
           "?tqx=out:csv&sheet={year}")

# 16 canonical columns, by position (header text on the sheet is verbose).
COLS = ["Date", "Up 4%+ (today)", "Down 4%+ (today)", "5-Day Ratio", "10-Day Ratio",
        "Up 25%+ (qtr)", "Down 25%+ (qtr)", "Up 25%+ (mo)", "Down 25%+ (mo)",
        "Up 50%+ (mo)", "Down 50%+ (mo)", "Up 13%+ (34d)", "Down 13%+ (34d)",
        "Worden Universe", "T2108", "S&P 500"]


def fetch_year(year: int, session: requests.Session) -> pd.DataFrame | None:
    url = CSV_URL.format(sid=SHEET_ID, year=year)
    for attempt in range(3):
        try:
            r = session.get(url, timeout=60)
            if r.status_code != 200 or not r.text.strip():
                return None
            df = pd.read_csv(io.StringIO(r.text), header=0)
            if df.shape[1] < 16:
                return None
            df = df.iloc[:, :16]
            df.columns = COLS
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"])
            # numeric cleanup (S&P has thousands commas)
            for c in COLS[1:]:
                df[c] = (df[c].astype(str).str.replace(",", "", regex=False)
                         .str.strip().replace({"": None}))
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df = df[df[COLS[1:]].notna().any(axis=1)]
            return df.sort_values("Date")
        except Exception as e:
            print(f"  {year}: attempt {attempt+1} failed ({e})", file=sys.stderr)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, default=2007)
    ap.add_argument("--end", type=int, default=datetime.now().year)
    ap.add_argument("--out", default=str(Path.home() / "Downloads" /
                                         "Stockbee_Market_Monitor_AllYears.xlsx"))
    args = ap.parse_args()

    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"

    per_year: dict[int, pd.DataFrame] = {}
    for year in range(args.end, args.start - 1, -1):
        print(f"Fetching {year} …")
        df = fetch_year(year, s)
        if df is None or df.empty:
            print(f"  {year}: no tab / empty — skipping")
            continue
        per_year[year] = df
        print(f"  {year}: {len(df)} rows ({df['Date'].min().date()} → {df['Date'].max().date()})")

    if not per_year:
        sys.exit("No year tabs could be downloaded.")

    # Combined, de-duplicated by date (keep the row from the tab whose own year
    # matches the date's year; otherwise first seen).
    combined = (pd.concat(per_year.values(), ignore_index=True)
                .drop_duplicates(subset=["Date"], keep="first")
                .sort_values("Date").reset_index(drop=True))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl", datetime_format="mm/dd/yyyy") as xw:
        combined.to_excel(xw, sheet_name="All Years", index=False)
        for year in sorted(per_year, reverse=True):
            per_year[year].to_excel(xw, sheet_name=str(year), index=False)

    print(f"\nSaved {len(combined)} unique rows "
          f"({combined['Date'].min().date()} → {combined['Date'].max().date()}) to:\n{out}")


if __name__ == "__main__":
    main()
