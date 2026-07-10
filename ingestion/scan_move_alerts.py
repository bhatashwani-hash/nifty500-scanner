"""
scan_move_alerts.py  —  10%+ move alerts (calibrated "ignition" scanner).

Flags stocks primed for a 10%+ upside move within the next ~10 sessions.
The scoring is CALIBRATED, not assumed: every candidate signal was replayed
over the trailing year of this universe and measured against the matched
base rate of a +10% touch in 10 sessions. What the data showed (the same
validation grid re-runs on every scan and is stored alongside the alerts):

  WORKS      * high-energy names — typical ATR >= 3%/day        (~1.5x lift)
             * volume-surge ignition — RVOL >= 2.5x on an up day (~1.2x)
             * both together                                     (~1.6x)
             * up-day surge inside an uptrend                    (~1.8x)
  DOESN'T    * the classic "coiled spring" (BB squeeze + tight base under
               the 20-day high) UNDERPERFORMED the base rate at this horizon
               (0.66-0.86x) — it stays in the research grid, not the score.

Score components: ENERGY (typical ATR), IGNITION (RVOL surge on an up day),
TREND (50/200 DMA), LEVELS (20d-high break, 52w-high proximity),
ACCUMULATION (20d up/down volume). Tiers: READY >= 70, SETUP 55-69,
WATCH 45-54; rows below 45 are not stored.

Results written to scanner_move_alerts (delete+insert per run_date) and the
validation grid to scanner_move_validation (one row per run_date).
"""

import json
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

FWD_WINDOW = 10      # sessions ahead in which the 10% move must happen
TARGET_MOVE = 0.10   # +10%
MIN_ATR_PCT = 1.2    # typical ATR% below which a 10% burst is unrealistic
MIN_PRICE = 30.0     # skip micro-priced names where 10% is noise
MIN_SCORE = 45       # WATCH floor — rows below this are not stored

INSERT_SQL = """
    INSERT INTO scanner_move_alerts
        (run_date, symbol, close, chg_pct, score, tier, rvol, atr_typ_pct,
         squeeze_pctile, dist_20d_high_pct, dist_52w_high_pct, updown_vol,
         trigger_price, target_price, stop_price, reasons)
    VALUES %s
    ON CONFLICT (run_date, symbol) DO UPDATE SET
        close = EXCLUDED.close, chg_pct = EXCLUDED.chg_pct,
        score = EXCLUDED.score, tier = EXCLUDED.tier,
        rvol = EXCLUDED.rvol, atr_typ_pct = EXCLUDED.atr_typ_pct,
        squeeze_pctile = EXCLUDED.squeeze_pctile,
        dist_20d_high_pct = EXCLUDED.dist_20d_high_pct,
        dist_52w_high_pct = EXCLUDED.dist_52w_high_pct,
        updown_vol = EXCLUDED.updown_vol,
        trigger_price = EXCLUDED.trigger_price,
        target_price = EXCLUDED.target_price,
        stop_price = EXCLUDED.stop_price, reasons = EXCLUDED.reasons
"""


def feature_frame(hist: pd.DataFrame) -> pd.DataFrame:
    """Per-day feature series for one stock (today's score + validation replay)."""
    c, h, l, v = hist["close"], hist["high"], hist["low"], hist["volume"]
    f = pd.DataFrame(index=hist.index)
    f["close"], f["high"], f["volume"] = c, h, v

    prev_c = c.shift(1)
    tr = pd.concat([h - l, (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    f["atr_pct"] = tr.rolling(14).mean() / c * 100
    # typical volatility for the 10%-feasibility gate — current ATR is
    # deliberately LOW in a squeeze, so gate on the name's normal energy level
    f["atr_typ"] = f["atr_pct"].expanding(min_periods=30).median()

    sma20 = c.rolling(20).mean()
    f["bbw"] = (4 * c.rolling(20).std()) / sma20 * 100
    f["bbw_rank"] = f["bbw"].rank(pct=True)
    f["tight5"] = (c.rolling(5).max() - c.rolling(5).min()) / c * 100

    prior_20d_high = h.shift(1).rolling(20).max()
    f["dist_20d_high"] = (prior_20d_high - c) / c * 100          # <=0 means broken out
    f["dist_52w_high"] = (h.shift(1).expanding().max() - c) / c * 100
    f["prior_20d_high"] = prior_20d_high

    f["up_day"] = c > prev_c
    up = (c > prev_c).astype(float)
    f["updown_vol"] = (v * up).rolling(20).sum() / (v * (1 - up)).rolling(20).sum().clip(lower=1)
    f["rvol"] = v / v.shift(1).rolling(20).mean()

    sma50, sma200 = c.rolling(50).mean(), c.rolling(200).mean()
    f["uptrend"] = (c > sma50) & (sma50 > sma50.shift(5))
    f["strong_trend"] = f["uptrend"] & (sma50 > sma200)
    return f


def coil_mask(f: pd.DataFrame) -> pd.Series:
    """Classic 'coiled spring' — kept in the research grid as a reference."""
    return (
        (f["bbw_rank"] <= 0.25)
        & (f["dist_20d_high"].between(-1.0, 4.0))
        & (f["updown_vol"] >= 1.2)
        & f["uptrend"]
        & (f["atr_typ"] >= MIN_ATR_PCT)
        & (f["close"] >= MIN_PRICE)
    )


VARIANTS = {
    "ignition (hi-ATR + rvol>=2.5 up)": lambda f: (
        (f["atr_typ"] >= 3.0) & (f["rvol"] >= 2.5) & f["up_day"]
    ),
    "ignition + uptrend": lambda f: (
        (f["atr_typ"] >= 3.0) & (f["rvol"] >= 2.5) & f["up_day"] & f["uptrend"]
    ),
    "coil": coil_mask,
    "coil+breakout": lambda f: coil_mask(f) & (f["close"] > f["prior_20d_high"]),
    "rvol>=2.5 up day": lambda f: (f["rvol"] >= 2.5) & f["up_day"],
    "rvol>=2.5 up + uptrend": lambda f: (f["rvol"] >= 2.5) & f["up_day"] & f["uptrend"],
    "rvol>=2 breakout": lambda f: (f["rvol"] >= 2.0) & (f["close"] > f["prior_20d_high"]),
    "near 52w-high + rvol>=2": lambda f: (f["dist_52w_high"] <= 5) & (f["rvol"] >= 2.0),
    "accumulation>=1.6": lambda f: f["updown_vol"] >= 1.6,
    "squeeze p<=10 alone": lambda f: f["bbw_rank"] <= 0.10,
    "high-ATR name (>=3%)": lambda f: f["atr_typ"] >= 3.0,
}


def validate(features_by_symbol) -> dict:
    """Replay each candidate signal over the trailing year vs the matched base
    rate (same price/ATR gates), counting +10% touches within FWD_WINDOW."""
    counts = {k: [0, 0] for k in VARIANTS}
    base_days = base_hits = 0
    for f in features_by_symbol.values():
        fwd_high = f["high"][::-1].rolling(FWD_WINDOW).max()[::-1].shift(-1)
        fwd_ret = fwd_high / f["close"] - 1
        valid = (
            fwd_ret.notna()
            & f["bbw_rank"].notna()
            & (f["close"] >= MIN_PRICE)
            & (f["atr_typ"] >= MIN_ATR_PCT)
        )
        hit = fwd_ret >= TARGET_MOVE
        base_days += int(valid.sum())
        base_hits += int((valid & hit).sum())
        for name, mask_fn in VARIANTS.items():
            m = mask_fn(f) & valid
            counts[name][0] += int(m.sum())
            counts[name][1] += int((m & hit).sum())
    base_rate = base_hits / base_days if base_days else 0.0
    variants = []
    for name, (days, hits) in counts.items():
        rate = hits / days if days else 0.0
        variants.append({
            "name": name, "days": days, "hits": hits,
            "hit_rate": round(rate, 4),
            "lift": round(rate / base_rate, 2) if base_rate else None,
        })
    return {
        "base_days": base_days,
        "base_hit_rate": round(base_rate, 4),
        "variants": variants,
    }


def score_today(f: pd.DataFrame):
    """Score the most recent bar. Returns (score, reasons, stats) or None."""
    r = f.iloc[-1]
    if not np.isfinite(r["bbw_rank"]) or r["close"] < MIN_PRICE:
        return None
    if not np.isfinite(r["atr_typ"]) or r["atr_typ"] < MIN_ATR_PCT:
        return None  # this name can't plausibly travel 10% in ~2 weeks
    rvol = float(r["rvol"]) if np.isfinite(r["rvol"]) else 0.0

    score, reasons = 0.0, []

    # ENERGY — the single strongest predictor (1.5x lift on its own)
    if r["atr_typ"] >= 4.0:
        score += 30; reasons.append(f"very high-energy name (typical ATR {r['atr_typ']:.1f}%/day)")
    elif r["atr_typ"] >= 3.0:
        score += 24; reasons.append(f"high-energy name (typical ATR {r['atr_typ']:.1f}%/day)")
    elif r["atr_typ"] >= 2.5:
        score += 16; reasons.append(f"energetic name (typical ATR {r['atr_typ']:.1f}%/day)")
    elif r["atr_typ"] >= 2.0:
        score += 10
    elif r["atr_typ"] >= 1.6:
        score += 5

    # IGNITION — volume surge on an UP day (surging down-volume is a red flag)
    up_day = bool(r["up_day"])
    if up_day and rvol >= 4.0:
        score += 28; reasons.append(f"ignition: volume {rvol:.1f}x average on an up day")
    elif up_day and rvol >= 2.5:
        score += 24; reasons.append(f"ignition: volume {rvol:.1f}x average on an up day")
    elif up_day and rvol >= 2.0:
        score += 16; reasons.append(f"volume {rvol:.1f}x average on an up day")
    elif up_day and rvol >= 1.5:
        score += 8

    # TREND
    if r["strong_trend"]:
        score += 12; reasons.append("uptrend: price > rising 50-DMA > 200-DMA")
    elif r["uptrend"]:
        score += 8; reasons.append("price above a rising 50-DMA")

    # LEVELS — through or at the 20-day high; near the 52-week high
    d20 = r["dist_20d_high"]
    if d20 <= 0:
        score += 12; reasons.append("breaking the 20-day high NOW")
    elif d20 <= 2.0:
        score += 7; reasons.append(f"{d20:.1f}% under the 20-day high")
    elif d20 <= 4.0:
        score += 4

    d52 = r["dist_52w_high"]
    if d52 <= 5.0:
        score += 6; reasons.append("within 5% of 52-week high (no overhead supply)")
    elif d52 <= 12.0:
        score += 3

    # ACCUMULATION
    if r["updown_vol"] >= 1.6:
        score += 6; reasons.append(f"accumulation (up/down volume {r['updown_vol']:.1f})")
    elif r["updown_vol"] >= 1.2:
        score += 3

    stats = {
        "rvol": rvol,
        "atr_typ": float(r["atr_typ"]),
        "squeeze_pctile": float(r["bbw_rank"]) * 100,
        "dist_20d_high": float(d20),
        "dist_52w_high": float(d52),
        "updown_vol": float(r["updown_vol"]),
        "trigger": float(r["prior_20d_high"]) if np.isfinite(r["prior_20d_high"]) else None,
        "target": float(r["close"]) * (1 + TARGET_MOVE),
        "stop": float(r["close"]) * (1 - 0.04),
    }
    return min(round(score), 100), reasons, stats


def tier_of(score: int) -> str:
    if score >= 70:
        return "READY"
    if score >= 55:
        return "SETUP"
    return "WATCH"


def run(conn, run_date: date | None = None):
    log.info("scan_move_alerts: loading 1y of data …")
    df = pd.read_sql(
        """SELECT o.symbol, o.time::date AS date,
                  o.high, o.low, o.close, o.volume
           FROM ohlcv o
           JOIN stocks s ON o.symbol = s.symbol
           WHERE s.is_active = true AND o.time >= now() - interval '420 days'
           ORDER BY o.symbol, date""",
        conn, parse_dates=["date"],
    )
    if df.empty:
        log.warning("scan_move_alerts: no data")
        return 0

    latest = df["date"].max()
    run_date = run_date or latest.date()

    rows, features_by_symbol = [], {}
    for symbol, hist in df.groupby("symbol"):
        hist = hist.set_index("date")[["high", "low", "close", "volume"]].astype(float)
        if len(hist) < 60 or hist.index[-1] != latest:
            continue
        try:
            f = feature_frame(hist)
            features_by_symbol[symbol] = f
            scored = score_today(f)
            if scored is None:
                continue
            score, reasons, s = scored
            if score < MIN_SCORE:
                continue
            closes = hist["close"]
            chg = (closes.iloc[-1] / closes.iloc[-2] - 1) * 100 if len(closes) > 1 else 0.0
            rows.append((
                run_date, symbol,
                round(float(closes.iloc[-1]), 2), round(float(chg), 2),
                score, tier_of(score),
                round(s["rvol"], 2), round(s["atr_typ"], 2),
                round(s["squeeze_pctile"], 1),
                round(s["dist_20d_high"], 2), round(s["dist_52w_high"], 2),
                round(s["updown_vol"], 2),
                round(s["trigger"], 2) if s["trigger"] is not None else None,
                round(s["target"], 2), round(s["stop"], 2),
                json.dumps(reasons),
            ))
        except Exception as e:
            log.warning("scan_move_alerts: %s failed: %s", symbol, e)

    validation = validate(features_by_symbol)

    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_move_alerts WHERE run_date = %s", (run_date,))
        if rows:
            psycopg2.extras.execute_values(cur, INSERT_SQL, rows, page_size=500)
        cur.execute(
            """INSERT INTO scanner_move_validation
                   (run_date, fwd_window, target_move, base_days, base_hit_rate, variants)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (run_date) DO UPDATE SET
                   fwd_window = EXCLUDED.fwd_window,
                   target_move = EXCLUDED.target_move,
                   base_days = EXCLUDED.base_days,
                   base_hit_rate = EXCLUDED.base_hit_rate,
                   variants = EXCLUDED.variants""",
            (run_date, FWD_WINDOW, TARGET_MOVE,
             validation["base_days"], validation["base_hit_rate"],
             json.dumps(validation["variants"])),
        )
    conn.commit()
    log.info("scan_move_alerts: %d rows for %s (base hit rate %.1f%%)",
             len(rows), run_date, validation["base_hit_rate"] * 100)
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
