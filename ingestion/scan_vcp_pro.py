"""
scan_vcp_pro.py  —  Minervini-style VCP scanner (trend template + contraction
sequence + volume dry-up + breakout state machine).

For every stock, every day (state machine over the trailing ~6 months, results
kept for bases whose setup or signal fell inside the last 3 months / 63 bars):

STEP 1 — Stage-2 trend template (all must hold):
    close > SMA50, SMA50 > SMA150, SMA150 > SMA200,
    SMA200 rising vs 1 month (21 bars) ago,
    close >= 75% of 52-week high, close >= 130% of 52-week low.

STEP 2 — Contractions:
    Swing high = bar whose high exceeds the 5 bars on each side; swing low the
    opposite. Each swing-high -> swing-low pullback depth = (SH-SL)/SH*100.
    Keep the last 6. The suffix run of "shrinking" pullbacks (each <= 75% the
    depth of the one before) must be >= 2 long, first depth <= 35%, final <= 10%.

STEP 3 — Volume dry-up:  5-day avg volume < 80% of 50-day avg volume.

STEP 4 — Base is live:   last swing low within 15 bars, close still below the
    last swing high (= PIVOT).  All pass -> setup ARMED (one per base).

STEP 5 — Breakout while armed:
    close > pivot  AND  volume >= 1.5x 50-day avg  AND  trend template intact
        -> FIRED (buy signal, one per base; return tracked from signal close).
    close < SMA50 -> FAILED (disarm quietly).

Stop reference = low of the final contraction at arming time.

Full-refresh table: scanner_vcp_pro (one row per symbol, most recent base).
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

SCAN_WINDOW   = 63    # "last 3 months" of trading days
MACHINE_START = 126   # run the state machine over the trailing 6 months
SWING_SPAN    = 5     # bars each side for swing highs/lows
SHRINK        = 0.75  # each contraction <= 75% of the previous
MAX_FIRST     = 35.0  # deepest contraction cap (%)
MAX_LAST      = 10.0  # final contraction cap (%)
MIN_RUN       = 2     # >= 2 shrinking contractions in a row
LIVE_BARS     = 15    # last swing low within N bars
VOL_DRY       = 0.80  # vol5 < 80% of vol50
VOL_BREAK     = 1.50  # breakout volume >= 1.5x vol50

INSERT_SQL = """
    INSERT INTO scanner_vcp_pro
        (run_date, symbol, status, setup_date, signal_date, pivot, stop_loss,
         num_contractions, depths, first_depth_pct, last_depth_pct,
         signal_close, signal_vol_ratio, last_date, last_close,
         return_pct, pct_to_pivot)
    VALUES %s
"""


def _sma(a, n):
    out = np.full(len(a), np.nan)
    if len(a) >= n:
        c = np.cumsum(np.insert(a, 0, 0.0))
        out[n - 1:] = (c[n:] - c[:-n]) / n
    return out


def _swings(high, low, span=SWING_SPAN):
    """Return (swing_high_idx[], swing_low_idx[]) — strict 5-bar pivots."""
    n = len(high)
    sh, sl = [], []
    for j in range(span, n - span):
        w = slice(j - span, j + span + 1)
        if high[j] == high[w].max() and (high[w] == high[j]).sum() == 1:
            sh.append(j)
        if low[j] == low[w].min() and (low[w] == low[j]).sum() == 1:
            sl.append(j)
    return sh, sl


def _scan_symbol(dates, high, low, close, vol):
    """Run the daily state machine for one symbol. Returns latest base dict or None."""
    n = len(close)
    if n < 260:
        return None

    sma50, sma150, sma200 = _sma(close, 50), _sma(close, 150), _sma(close, 200)
    vol50, vol5 = _sma(vol, 50), _sma(vol, 5)

    def trend_ok(i):
        if i < 251 or np.isnan(sma200[i]) or np.isnan(sma200[i - 21]):
            return False
        hi52 = high[i - 251:i + 1].max()
        lo52 = low[i - 251:i + 1].min()
        return (close[i] > sma50[i] and sma50[i] > sma150[i]
                and sma150[i] > sma200[i] and sma200[i] > sma200[i - 21]
                and close[i] >= 0.75 * hi52 and close[i] >= 1.30 * lo52)

    sh_idx, sl_idx = _swings(high, low)
    sh_set, sl_set = set(sh_idx), set(sl_idx)

    # --- walk forward, maintaining confirmed swings + pullback pairs ----------
    pairs = []            # [{sh, shv, sl, slv, depth}]
    last_sh = None        # (idx, val) most recent confirmed swing high
    last_sl = None
    armed = None          # dict while a base is armed
    done_pivots = set()   # pivot bar idx already fired/failed (one signal per base)
    latest_base = None    # most recent base dict (armed episode)

    start = max(252, n - MACHINE_START)
    for i in range(start, n):
        # confirm swings that became visible today (bar j = i - SWING_SPAN)
        j = i - SWING_SPAN
        if j in sh_set:
            last_sh = (j, high[j])
        if j in sl_set:
            last_sl = (j, low[j])
            if last_sh and (not pairs or last_sh[0] > pairs[-1]["sh"]):
                d = (last_sh[1] - low[j]) / last_sh[1] * 100.0
                pairs.append({"sh": last_sh[0], "shv": last_sh[1],
                              "sl": j, "slv": low[j], "depth": d})
            elif pairs and low[j] < pairs[-1]["slv"]:
                # same leg made a lower low -> deepen the last contraction
                pairs[-1].update(sl=j, slv=low[j],
                                 depth=(pairs[-1]["shv"] - low[j]) / pairs[-1]["shv"] * 100.0)
            if len(pairs) > 6:
                del pairs[:-6]

        if armed:
            if (close[i] > armed["pivot"] and vol50[i] > 0
                    and vol[i] >= VOL_BREAK * vol50[i] and trend_ok(i)):
                armed.update(status="FIRED", signal_i=i,
                             signal_close=close[i],
                             signal_vol_ratio=vol[i] / vol50[i])
                done_pivots.add(armed["pivot_i"])
                latest_base = armed
                armed = None
            elif close[i] < sma50[i]:
                armed.update(status="FAILED")
                done_pivots.add(armed["pivot_i"])
                latest_base = armed
                armed = None
            continue

        # --- not armed: evaluate steps 1-4 -----------------------------------
        if not (last_sh and last_sl and pairs):
            continue
        if last_sh[0] in done_pivots:
            continue
        if not trend_ok(i):
            continue

        depths = [p["depth"] for p in pairs]
        run = 1
        for k in range(len(depths) - 1, 0, -1):
            if depths[k] <= SHRINK * depths[k - 1]:
                run += 1
            else:
                break
        if run < MIN_RUN:
            continue
        run_d = depths[-run:]
        if run_d[0] > MAX_FIRST or run_d[-1] > MAX_LAST:
            continue
        if i - last_sl[0] > LIVE_BARS:
            continue
        if not (close[i] < last_sh[1]):
            continue
        if not (vol50[i] > 0 and vol5[i] < VOL_DRY * vol50[i]):
            continue

        armed = {"status": "ACTIVE", "setup_i": i,
                 "pivot": last_sh[1], "pivot_i": last_sh[0],
                 "stop": pairs[-1]["slv"],
                 "num_contractions": run,
                 "depths": " → ".join(f"{d:.1f}" for d in run_d),
                 "first_depth": run_d[0], "last_depth": run_d[-1],
                 "signal_i": None, "signal_close": None, "signal_vol_ratio": None}
        latest_base = armed

    if latest_base is None:
        return None
    # keep only bases whose setup or signal is inside the 3-month window
    cutoff = n - SCAN_WINDOW
    sig_i = latest_base.get("signal_i")
    if latest_base["setup_i"] < cutoff and (sig_i is None or sig_i < cutoff):
        return None

    b = latest_base
    last_close = float(close[-1])
    row = {
        "status": b["status"],
        "setup_date": dates[b["setup_i"]],
        "signal_date": dates[sig_i] if sig_i is not None else None,
        "pivot": round(float(b["pivot"]), 2),
        "stop_loss": round(float(b["stop"]), 2),
        "num_contractions": int(b["num_contractions"]),
        "depths": b["depths"],
        "first_depth_pct": round(float(b["first_depth"]), 2),
        "last_depth_pct": round(float(b["last_depth"]), 2),
        "signal_close": round(float(b["signal_close"]), 2) if b["signal_close"] else None,
        "signal_vol_ratio": round(float(b["signal_vol_ratio"]), 2) if b["signal_vol_ratio"] else None,
        "last_date": dates[-1],
        "last_close": round(last_close, 2),
        "return_pct": (round((last_close / float(b["signal_close"]) - 1) * 100, 2)
                       if b["status"] == "FIRED" and b["signal_close"] else None),
        "pct_to_pivot": (round((last_close / float(b["pivot"]) - 1) * 100, 2)
                         if b["status"] == "ACTIVE" else None),
    }
    return row


def run(conn, run_date: date | None = None):
    log.info("scan_vcp_pro: loading data …")
    df = pd.read_sql(
        """SELECT o.symbol, o.time::date AS date, o.high, o.low, o.close, o.volume
           FROM ohlcv o
           JOIN stocks s ON o.symbol = s.symbol
           WHERE s.is_active = true
           ORDER BY o.symbol, date""",
        conn, parse_dates=["date"],
    )
    if df.empty:
        log.warning("scan_vcp_pro: no data")
        return 0

    run_date = run_date or df["date"].max().date()
    rows = []
    for sym, g in df.groupby("symbol", sort=False):
        r = _scan_symbol(
            [d.date() for d in g["date"]],
            g["high"].to_numpy(float), g["low"].to_numpy(float),
            g["close"].to_numpy(float), g["volume"].to_numpy(float),
        )
        if r:
            rows.append((
                run_date, sym, r["status"], r["setup_date"], r["signal_date"],
                r["pivot"], r["stop_loss"], r["num_contractions"], r["depths"],
                r["first_depth_pct"], r["last_depth_pct"], r["signal_close"],
                r["signal_vol_ratio"], r["last_date"], r["last_close"],
                r["return_pct"], r["pct_to_pivot"],
            ))

    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_vcp_pro")   # full refresh (leaderboard style)
        if rows:
            psycopg2.extras.execute_values(cur, INSERT_SQL, rows, page_size=200)
    conn.commit()
    log.info("scan_vcp_pro: %d bases (%s) for %s",
             len(rows),
             ", ".join(f"{s}={sum(1 for r in rows if r[2]==s)}"
                       for s in ("ACTIVE", "FIRED", "FAILED")),
             run_date)
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
