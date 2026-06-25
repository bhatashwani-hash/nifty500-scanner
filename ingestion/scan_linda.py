"""
scan_linda.py  —  "Linda Scan": Linda Raschke's classic short-term setups.

Four patterns, each emitting a BUY and/or SELL signal (one row per fired
signal). All measured on the latest completed daily bar.

------------------------------------------------------------------------------
  holy_grail    Trend-pullback (Raschke "Holy Grail").
                BUY : ADX(14) > 30 AND uptrend (close > 20 EMA) AND the bar's
                      low touches / is near the 20 EMA (pullback into trend).
                SELL: ADX(14) > 30 AND downtrend (close < 20 EMA) AND the bar's
                      high rallies back to / near the 20 EMA.

  turtle_soup   False breakout fade ("Turtle Soup").
                BUY : today's low < lowest low of the prior 20 days AND close
                      back ABOVE that prior low (failed breakdown).
                SELL: today's high > highest high of the prior 20 days AND close
                      back BELOW that prior high (failed breakout).

  eighty_twenty 80/20 reversal bar (Connors/Raschke "Street Smarts").
                The bar's direction is faded the next day:
                BUY : open in the TOP 20% of range AND close in the BOTTOM 20%
                      (strong down bar → fade up).
                SELL: open in the BOTTOM 20% of range AND close in the TOP 20%
                      (strong up bar → fade down).

  persistency   Strong-trend flag: all of the last 7 closes on the same side of
                the 5-period MA.
                BUY : 7 of last 7 closes ABOVE the 5-MA (persistent uptrend).
                SELL: 7 of last 7 closes BELOW the 5-MA (persistent downtrend).

Results written to scanner_linda (one row per symbol/pattern/side).
"""

import logging
import os
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
log = logging.getLogger(__name__)

UPSERT_SQL = """
    INSERT INTO scanner_linda
        (run_date, symbol, pattern, side, close, adx14, ema20, ref_level, note, last_date)
    VALUES %s
    ON CONFLICT (run_date, symbol, pattern, side) DO UPDATE SET
        close     = EXCLUDED.close,
        adx14     = EXCLUDED.adx14,
        ema20     = EXCLUDED.ema20,
        ref_level = EXCLUDED.ref_level,
        note      = EXCLUDED.note,
        last_date = EXCLUDED.last_date
"""


def _r(v, d=2):
    if v is None:
        return None
    v = float(v)
    return None if np.isnan(v) else round(v, d)


def _wilder(s: pd.Series, n: int) -> pd.Series:
    """Wilder's RMA smoothing (== ewm with alpha=1/n, adjust=False)."""
    return s.ewm(alpha=1.0 / n, adjust=False).mean()


def _adx(high, low, close, n=14):
    up = high.diff()
    down = -low.diff()
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    tr = pd.concat([(high - low).abs(),
                    (high - close.shift()).abs(),
                    (low - close.shift()).abs()], axis=1).max(axis=1)
    atr = _wilder(tr, n)
    plus_di = 100 * _wilder(pd.Series(plus_dm, index=high.index), n) / atr.replace(0, np.nan)
    minus_di = 100 * _wilder(pd.Series(minus_dm, index=high.index), n) / atr.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return _wilder(dx, n)


def run(conn, run_date: date | None = None):
    log.info("scan_linda: loading data …")

    df = pd.read_sql(
        """SELECT o.symbol, o.time::date AS date,
                  o.open, o.high, o.low, o.close, o.volume
           FROM ohlcv o
           JOIN stocks s ON o.symbol = s.symbol
           WHERE s.is_active = true
           ORDER BY o.symbol, o.time""",
        conn, parse_dates=["date"],
    )
    if df.empty:
        log.warning("scan_linda: no data")
        return 0

    rows = []
    eff_run_date = run_date

    for sym, g in df.groupby("symbol", sort=False):
        g = g.sort_values("date")
        if len(g) < 40:                         # need >=20 prior + ADX warmup
            continue

        o = g["open"].astype(float)
        h = g["high"].astype(float)
        l = g["low"].astype(float)
        c = g["close"].astype(float)

        last_date = g["date"].iloc[-1].date()
        if eff_run_date is None:
            eff_run_date = last_date

        ema20 = c.ewm(span=20, adjust=False).mean()
        adx = _adx(h, l, c, 14)
        ma5 = c.rolling(5).mean()

        O, H, L, C = o.iloc[-1], h.iloc[-1], l.iloc[-1], c.iloc[-1]
        e = ema20.iloc[-1]
        a = adx.iloc[-1]
        rng = H - L
        prior20low = l.iloc[-21:-1].min()
        prior20high = h.iloc[-21:-1].max()

        def add(pattern, side, ref=None, note=""):
            rows.append((eff_run_date, sym, pattern, side, _r(C), _r(a), _r(e),
                         _r(ref), note, last_date))

        # --- Holy Grail (trend pullback) ---
        if not np.isnan(a) and a > 30:
            if C > e and L <= e * 1.01:
                add("holy_grail", "BUY", e, f"ADX {a:.0f}, pullback to 20EMA")
            elif C < e and H >= e * 0.99:
                add("holy_grail", "SELL", e, f"ADX {a:.0f}, rally to 20EMA")

        # --- Turtle Soup (false breakout fade) ---
        if not np.isnan(prior20low) and L < prior20low and C > prior20low:
            add("turtle_soup", "BUY", prior20low, "failed 20d breakdown")
        if not np.isnan(prior20high) and H > prior20high and C < prior20high:
            add("turtle_soup", "SELL", prior20high, "failed 20d breakout")

        # --- 80/20 reversal bar (fade the bar) ---
        if rng > 0:
            if O >= H - 0.2 * rng and C <= L + 0.2 * rng:
                add("eighty_twenty", "BUY", None, "down 80/20 bar → fade up")
            elif O <= L + 0.2 * rng and C >= H - 0.2 * rng:
                add("eighty_twenty", "SELL", None, "up 80/20 bar → fade down")

        # --- Persistency (7/7 closes on one side of 5-MA) ---
        last7 = pd.concat([c.iloc[-7:], ma5.iloc[-7:]], axis=1)
        last7.columns = ["c", "m"]
        if last7["m"].notna().all():
            if (last7["c"] > last7["m"]).all():
                add("persistency", "BUY", None, "7/7 closes > 5MA")
            elif (last7["c"] < last7["m"]).all():
                add("persistency", "SELL", None, "7/7 closes < 5MA")

    if not rows:
        log.info("scan_linda: no signals for %s", eff_run_date)
        return 0

    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_linda WHERE run_date = %s", (eff_run_date,))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=300)
    conn.commit()
    buys = sum(1 for r in rows if r[3] == "BUY")
    log.info("scan_linda: %d signals (%d buy / %d sell) for %s",
             len(rows), buys, len(rows) - buys, eff_run_date)
    return len(rows)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        stream=sys.stdout)
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        run(conn)
    finally:
        conn.close()
