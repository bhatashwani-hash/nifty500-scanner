"""
scan_screens.py  —  JFS multi-screen scanner (all stocks, EOD).

Computes per-stock metrics and 10 screen flags, mirroring the JFS US-tech
dashboard screens for the NSE universe:

  focus       RS>=80, above rising 50-MA, ExtATR<=4, contracting range (ADR5 < 75% ADR20)
  rs_leaders  RS>=80, close > 50-MA > 200-MA, within 25% of 52-wk high
  hot_adr     ADR%>=4, RS>=70, above 20/50-MA, +10% or more in a month
  movers      top 200 by momentum composite (0.4*3M + 0.3*6M + 0.2*1M + 0.1*1W)
  vcp         range contraction near highs: ADR5 < 75% ADR20, ExtATR<=4, within 25% of high
  pullback    uptrend pullback to rising 20/50-MA on quiet volume (RVOL<1), RS>=70
  rvol        RVOL >= 1.4 (vs 50d avg) and up > 2% today
  breakout    new 3M/6M/12M closing high on RVOL >= 1.2
  ipo         listed < 12 months, close>=20, turnover >= 5 Cr, ADR% >= 3
  parabolic   SHORT candidates: >= 7x ATR above 50-MA and +25% in a month

Metrics: rs_rating (1-99 composite percentile), adr20, rvol, ext_atr
(ATR20 multiples above 50-MA), off_high_pct (vs 52-wk closing high),
ret_1d/1w/1m/3m/6m, turnover_cr (close*volume / 1e7).

Full refresh of scanner_screens; only rows passing >= 1 screen are stored.
"""

import logging
import os
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
log = logging.getLogger(__name__)

INSERT_SQL = """
    INSERT INTO scanner_screens
      (run_date, symbol, close, rs_rating, adr20, rvol, ext_atr, off_high_pct,
       ret_1d, ret_1w, ret_1m, ret_3m, ret_6m, turnover_cr, composite,
       s_focus, s_rs_leaders, s_hot_adr, s_movers, s_vcp, s_pullback, s_rvol,
       s_breakout, s_ipo, s_parabolic)
    VALUES %s
"""


def _r(v, d=2):
    return None if v is None or (isinstance(v, float) and np.isnan(v)) else round(float(v), d)


def run(conn, run_date: date | None = None):
    log.info("scan_screens: loading data …")
    df = pd.read_sql(
        """SELECT o.symbol, o.time::date AS date, o.close, o.high, o.low, o.volume
           FROM ohlcv o JOIN stocks s ON o.symbol = s.symbol
           WHERE s.is_active AND o.time >= now() - interval '480 days'
           ORDER BY o.symbol, date""",
        conn, parse_dates=["date"],
    )
    first_bars = pd.read_sql(
        "SELECT symbol, min(time::date) AS first_bar FROM ohlcv GROUP BY symbol",
        conn, parse_dates=["first_bar"],
    ).set_index("symbol")["first_bar"]
    if df.empty:
        return 0

    latest = df["date"].max()
    run_date = run_date or latest.date()
    rows_out = []
    metrics = {}

    for sym, g in df.groupby("symbol", sort=False):
        if g["date"].iloc[-1] != latest or len(g) < 25:
            continue
        c = g["close"].to_numpy(float); h = g["high"].to_numpy(float)
        l = g["low"].to_numpy(float);  v = g["volume"].to_numpy(float)
        n = len(c)

        ret = lambda k: (c[-1] / c[-1 - k] - 1) * 100 if n > k and c[-1 - k] > 0 else None
        r1d, r1w, r1m, r3m, r6m = ret(1), ret(5), ret(21), ret(63), ret(126)

        rng = h / np.where(l > 0, l, np.nan) - 1
        adr20 = np.nanmean(rng[-20:]) * 100
        adr5  = np.nanmean(rng[-5:]) * 100
        tr = np.maximum(h[1:] - l[1:],
             np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
        atr20 = tr[-20:].mean() if len(tr) >= 20 else None
        v50 = v[-50:].mean() if n >= 2 else None
        rvol = v[-1] / v50 if v50 and v50 > 0 else None
        sma20 = c[-20:].mean(); sma50 = c[-50:].mean() if n >= 50 else c.mean()
        sma200 = c[-200:].mean() if n >= 200 else None
        sma20_p5  = c[-25:-5].mean() if n >= 25 else None
        sma50_p10 = c[-60:-10].mean() if n >= 60 else None
        hi52 = c[-252:].max()
        ext_atr = (c[-1] - sma50) / atr20 if atr20 and atr20 > 0 else None
        off_high = (c[-1] / hi52 - 1) * 100 if hi52 > 0 else None
        turnover = c[-1] * v[-1] / 1e7
        comp = (0.4 * (r3m or 0) + 0.3 * (r6m if r6m is not None else (r3m or 0))
                + 0.2 * (r1m or 0) + 0.1 * (r1w or 0))
        new_hi = (n > 63 and (
            c[-1] > c[max(0, n - 64):-1].max()
            or c[-1] > c[max(0, n - 127):-1].max()
            or c[-1] > c[max(0, n - 253):-1].max()))
        fb = first_bars.get(sym)
        is_recent = fb is not None and (latest - fb).days < 365

        metrics[sym] = dict(close=c[-1], r1d=r1d, r1w=r1w, r1m=r1m, r3m=r3m, r6m=r6m,
                            adr20=adr20, adr5=adr5, rvol=rvol, ext_atr=ext_atr,
                            off_high=off_high, turnover=turnover, comp=comp,
                            sma20=sma20, sma50=sma50, sma200=sma200,
                            sma20_p5=sma20_p5, sma50_p10=sma50_p10,
                            new_hi=new_hi, is_recent=is_recent)

    # RS rating: 1-99 percentile of composite across the universe
    comps = pd.Series({s: m["comp"] for s, m in metrics.items()})
    rs = (comps.rank(pct=True) * 98).apply(np.ceil).clip(1, 99)
    comp_rank = comps.rank(ascending=False, method="first")

    for sym, m in metrics.items():
        r = float(rs[sym]); crank = comp_rank[sym]
        cl, e = m["close"], m["ext_atr"]
        contracting = m["adr5"] < 0.75 * m["adr20"] if not np.isnan(m["adr5"]) else False
        s50_rising = m["sma50_p10"] is not None and m["sma50"] > m["sma50_p10"]
        s20_rising = m["sma20_p5"] is not None and m["sma20"] > m["sma20_p5"]

        f = dict(
            s_focus=(r >= 80 and cl > m["sma50"] and s50_rising
                     and e is not None and e <= 4 and contracting),
            s_rs_leaders=(r >= 80 and m["sma200"] is not None and cl > m["sma50"]
                          and m["sma50"] > m["sma200"]
                          and m["off_high"] is not None and m["off_high"] >= -25),
            s_hot_adr=(m["adr20"] >= 4 and r >= 70 and cl > m["sma20"] and cl > m["sma50"]
                       and m["r1m"] is not None and m["r1m"] >= 10),
            s_movers=(crank <= 200 and m["comp"] > 0),
            s_vcp=(contracting and e is not None and e <= 4
                   and m["off_high"] is not None and m["off_high"] >= -25 and cl > m["sma50"]),
            s_pullback=(m["sma200"] is not None and cl > m["sma200"] and s20_rising
                        and s50_rising and r >= 70 and cl >= 0.98 * m["sma50"]
                        and cl <= 1.02 * m["sma20"]
                        and m["rvol"] is not None and m["rvol"] < 1.0),
            s_rvol=(m["rvol"] is not None and m["rvol"] >= 1.4
                    and m["r1d"] is not None and m["r1d"] > 2),
            s_breakout=(m["rvol"] is not None and m["rvol"] >= 1.2 and m["new_hi"]),
            s_ipo=(m["is_recent"] and cl >= 20 and m["turnover"] >= 5 and m["adr20"] >= 3),
            s_parabolic=(e is not None and e >= 7
                         and m["r1m"] is not None and m["r1m"] >= 25),
        )
        if not any(f.values()):
            continue
        rows_out.append((
            run_date, sym, _r(cl), r, _r(m["adr20"]), _r(m["rvol"]),
            _r(m["ext_atr"], 1), _r(m["off_high"], 1),
            _r(m["r1d"], 1), _r(m["r1w"], 1), _r(m["r1m"], 1), _r(m["r3m"], 1), _r(m["r6m"], 1),
            _r(m["turnover"], 1), _r(m["comp"]),
            f["s_focus"], f["s_rs_leaders"], f["s_hot_adr"], f["s_movers"], f["s_vcp"],
            f["s_pullback"], f["s_rvol"], f["s_breakout"], f["s_ipo"], f["s_parabolic"],
        ))

    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_screens")
        if rows_out:
            psycopg2.extras.execute_values(cur, INSERT_SQL, rows_out, page_size=500)
    conn.commit()
    log.info("scan_screens: %d rows for %s", len(rows_out), run_date)
    return len(rows_out)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        stream=sys.stdout)
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        run(conn)
    finally:
        conn.close()
