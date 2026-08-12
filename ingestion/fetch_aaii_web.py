"""
fetch_aaii_web.py — scrape the AAII sentiment survey page → aaii_sentiment table.

The bulk .xls download (fetch_aaii.py) is behind Imperva bot protection, which
blocks scripted HTTP clients. This variant drives a real Chromium via Playwright,
loads https://www.aaii.com/sentimentsurvey, and parses the on-page table of the
4 most recent weekly readings.

Date convention: the site lists Wednesday close dates; the table stores the
Thursday publication date, so 1 day is added to each site date.

Run weekly (Saturday) via Task Scheduler:
    python ingestion/fetch_aaii_web.py
"""

import logging
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv(Path(__file__).parent.parent / ".env")
log = logging.getLogger("aaii_web")

URL = "https://www.aaii.com/sentimentsurvey"
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


def scrape() -> str:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        page = browser.new_page(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"))
        page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        # Imperva may interpose a challenge; give the page time to settle, then
        # wait for a date-looking cell to appear in the rendered text.
        for _ in range(6):
            page.wait_for_timeout(5000)
            text = page.inner_text("body")
            if re.search(r"\d{1,2}/\d{1,2}/\d{4}", text) and "Bullish" in text:
                browser.close()
                return text
        browser.close()
        raise RuntimeError("AAII page never showed the sentiment table (bot challenge not cleared?)")


def parse(text: str):
    """Return rows (week_ending+1d, bull, neutral, bear, spread) from the page text."""
    rows = []
    # The table renders as: date line, then three percentage lines.
    pat = re.compile(
        r"(\d{1,2}/\d{1,2}/\d{4})\s*\n\s*([\d.]+)%\s*\n\s*([\d.]+)%\s*\n\s*([\d.]+)%")
    for m in pat.finditer(text):
        d = datetime.strptime(m.group(1), "%m/%d/%Y").date() + timedelta(days=1)
        bull, neut, bear = (round(float(m.group(i)), 1) for i in (2, 3, 4))
        if bull + neut + bear < 90 or bull + neut + bear > 110:
            continue                      # not a sentiment row (e.g. unrelated stats)
        rows.append((d, bull, neut, bear, round(bull - bear, 1)))
    return rows


def main():
    text = scrape()
    rows = parse(text)
    if not rows:
        raise RuntimeError("Parsed 0 sentiment rows from the AAII page")
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=50)
        conn.commit()
        log.info("upserted %d weeks: %s", len(rows), ", ".join(str(r[0]) for r in rows))
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        stream=sys.stdout)
    main()
