"""
scan_manas.py  —  "Manas Scan": a two-layer momentum-pullback scanner.

The methodology has two layers (a broad trend universe, then per-stock
setup-ready conditions). We compute Layer 1 as a hard filter and Layer 2 as a
set of boolean flags + a setup score, so the dashboard can show the full
Layer-1 candidate list ranked by how setup-ready each name is right now.

------------------------------------------------------------------------------
LAYER 1 — Universe / trend filter (a stock must pass ALL of these)
------------------------------------------------------------------------------
  1. Close > 30                              (liquidity / penny filter)
  2. Close within 25% of the 52-week high    (close >= 0.75 * high_252)
  3. Close > SMA50 > SMA200                  (proper trend stacking)
  4. SMA200 rising for >= 3 months           (sma200 today > sma200 63 bars ago)
  5. Close >= 50% above the 52-week low       (close >= 1.5 * low_252)
  6. A 52-week high was made in the last 6 months
                                             (argmax of high_252 within last 126 bars)

------------------------------------------------------------------------------
LAYER 2 — Setup-ready conditions (boolean flags + score, applied per stock)
------------------------------------------------------------------------------
  A. prior_move   : a meaningful prior up-leg (>= ~50%) over the last ~6 months
                    measured as max(high / running-min-low) over the window.
  B. above_ema    : price consolidating roughly 0–5% above the 21 EMA
                    (close >= ema21 and close <= 1.05 * ema21; a low piercing
                    the EMA is fine, a close below disqualifies).
  C. contraction  : recent daily range tight (<3% avg over 5d) AND volume
                    declining into the pullback (vol5 < vol_prior_20).
  D. inside_bar   : the last bar (or the prior bar) is an inside bar — housed
                    entirely within the previous candle.
  E. fast_mover   : at least one prior day in the last 40 bars with a >5% move
                    on >1M shares of volume (the "purple dot" / institutional
                    footprint).

  setup_score = A + B + C + D + E   (0..5)
  setup_ready = above_ema AND contraction AND setup_score >= 4

Results written to scanner_manas.
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
    INSERT INTO scanner_manas
        (run_date, symbol, close, pct_from_high, sma50, sma200, ema21,
         pct_above_ema, prior_move_pct, range_5d_pct, vol_ratio,
         inside_bar, fast_mover, setup_score, setup_ready, last_date)
    VALUES %s
    ON CONFLICT (run_date, symbol) DO UPDATE SET
        close          = EXCLUDED.close,
        pct_from_high  = EXCLUDED.pct_from_high,
        sma50          = EXCLUDED.sma50,
        sma200         = EXCLUDED.sma200,
        ema21          = EXCLUDED.ema21,
        pct_above_ema  = EXCLUDED.pct_above_ema,
        prior_move_pct = EXCLUDED.prior_move_pct,
        range_5d_pct   = EXCLUDED.range_5d_pct,
        vol_ratio      = EXCLUDED.vol_ratio,
        inside_bar     = EXCLUDED.inside_bar,
        fast_mover     = EXCLUDED.fast_mover,
        setup_score    = EXCLUDED.setup_score,
        setup_ready    = EXCLUDED.setup_ready,
        last_date      = EXCLUDED.last_date
"""


def _f(v):
    """Safe float cast (handles numpy scalars), None for NaN."""
    if v is None:
        return None
    v = float(v)
    return None if np.isnan(v) else round(v, 2)


def run(conn, run_date: date | None = None):
    log.info("scan_manas: loading data …")

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
        log.warning("scan_manas: no data")
        return 0

    rows = []
    eff_run_date = run_date

    for sym, g in df.groupby("symbol", sort=False):
        g = g.sort_values("date")
        if len(g) < 210:                       # need ~200d SMA + a little
            continue

        close = g["close"].to_numpy(dtype=float)
        high  = g["high"].to_numpy(dtype=float)
        low   = g["low"].to_numpy(dtype=float)
        vol   = g["volume"].to_numpy(dtype=float)
        last_date = g["date"].iloc[-1].date()
        if eff_run_date is None:
            eff_run_date = last_date

        c = close[-1]

        # --- moving averages ---
        sma50  = close[-50:].mean()
        sma200 = close[-200:].mean()
        sma20  = close[-20:].mean()
        ema21  = pd.Series(close).ewm(span=21, adjust=False).mean().iloc[-1]

        # sma200 ~3 months ago (63 trading bars)
        sma200_63 = close[-263:-63].mean() if len(close) >= 263 else None

        # --- 52-week (252-bar) high / low + recency ---
        win = min(252, len(g))
        high_252 = high[-win:].max()
        low_252  = low[-win:].min()
        idx_high = int(np.argmax(high[-win:]))           # 0-based within window
        bars_since_high = (win - 1) - idx_high           # how many bars ago

        # ---------------- LAYER 1 ----------------
        l1 = (
            c > 30
            and high_252 > 0 and c >= 0.75 * high_252
            and c > sma50 > sma200
            and (sma200_63 is not None and sma200 > sma200_63)
            and low_252 > 0 and c >= 1.5 * low_252
            and bars_since_high <= 126
        )
        if not l1:
            continue

        # ---------------- LAYER 2 ----------------
        # A. prior up-leg over last ~126 bars: max(high / running-min-low) - 1
        w = min(126, len(g))
        lw = low[-w:]
        hw = high[-w:]
        run_min = np.minimum.accumulate(lw)
        upleg = np.max(hw / np.where(run_min == 0, np.nan, run_min)) - 1.0
        prior_move_pct = float(upleg * 100)
        a_prior = prior_move_pct >= 50.0

        # B. consolidating 0–5% above the 21 EMA
        pct_above_ema = (c / ema21 - 1.0) * 100 if ema21 > 0 else None
        b_ema = pct_above_ema is not None and 0.0 <= pct_above_ema <= 5.0

        # C. volatility contraction: tight 5d range + declining volume
        rng5 = float(np.mean((high[-5:] - low[-5:]) / np.where(close[-5:] == 0, np.nan, close[-5:])))
        vol5 = float(np.mean(vol[-5:]))
        vol_prior = float(np.mean(vol[-25:-5])) if len(vol) >= 25 else np.nan
        vol_ratio = (vol5 / vol_prior) if vol_prior and not np.isnan(vol_prior) and vol_prior > 0 else None
        c_contract = (rng5 < 0.03) and (vol_ratio is not None and vol_ratio < 1.0)

        # D. inside bar (last bar inside prev, or prev inside the one before)
        def _inside(i):
            return high[i] <= high[i - 1] and low[i] >= low[i - 1]
        d_inside = _inside(-1) or (len(g) >= 3 and _inside(-2))

        # E. fast mover ("purple dot"): >5% move on >1M vol in last 40 bars
        n = min(40, len(g) - 1)
        e_fast = False
        for i in range(len(g) - n, len(g)):
            if close[i - 1] > 0:
                mv = abs(close[i] / close[i - 1] - 1.0)
                if mv > 0.05 and vol[i] > 1_000_000:
                    e_fast = True
                    break

        setup_score = int(a_prior) + int(b_ema) + int(c_contract) + int(d_inside) + int(e_fast)
        setup_ready = bool(b_ema and c_contract and setup_score >= 4)

        rows.append((
            eff_run_date, sym,
            _f(c),
            _f((c / high_252 - 1) * 100),       # pct_from_high (<= 0)
            _f(sma50), _f(sma200), _f(ema21),
            _f(pct_above_ema),
            _f(prior_move_pct),
            _f(rng5 * 100),
            _f(vol_ratio),
            bool(d_inside), bool(e_fast),
            int(setup_score), bool(setup_ready),
            last_date,
        ))

    if not rows:
        log.info("scan_manas: no Layer-1 candidates for %s", eff_run_date)
        return 0

    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_manas WHERE run_date = %s", (eff_run_date,))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=200)
    conn.commit()
    log.info("scan_manas: %d Layer-1 candidates (%d setup-ready) for %s",
             len(rows), sum(1 for r in rows if r[14]), eff_run_date)
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
