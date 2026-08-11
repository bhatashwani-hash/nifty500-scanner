"""
scan_us_all.py  —  US EOD scanner suite (orchestrator + all scanners in one file).

Scanners over the us_stocks / us_ohlcv universe (1-yr history):

  breadth    -> scanner_us_breadth    daily up/down 4%, 5d/10d ratios, %>200-DMA, S&P 500 close
  breakouts  -> scanner_us_breakouts  FRESH new 3M/6M/1Y closing highs/lows (first day only,
                                      longest window wins; 1Y activates as history accrues)
  rvol       -> scanner_us_rvol       day change + volume vs 20d avg (all stocks)
  ep         -> scanner_us_ep         Episodic Pivots (gap>=1%, move>=7%, vol>=3x 50d;
                                      or mega-cap variant move>=12% on vol>=2x),
                                      3-month leaderboard ranked by return since pivot;
                                      pivots on an earnings release get earnings_date set
  vcp        -> scanner_us_vcp        3-month return >= 25% and 15-day range < 15%
  groups     -> scanner_us_groups     sector + granular-theme performance day/week/month,
                                      each with its tracked ETF's return + top-3 leaders
  snapback   -> scanner_us_snapback   undercut & rally / spring: undercut of the prior
                                      1M/3M/6M/1Y low, then a SAME_DAY reversal bar or a
                                      D1/D2 close back above the broken level; scored 0-5

The universe is the TOP 1000 US stocks by market cap (us_stocks.is_active).

Usage:
    python us/scan_us_all.py
    python us/scan_us_all.py --skip breadth,groups
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    stream=sys.stdout)
log = logging.getLogger("scan_us")


def _load(conn, cols="close"):
    df = pd.read_sql(
        f"""SELECT o.symbol, o.time::date AS date, {cols}
            FROM us_ohlcv o JOIN us_stocks s ON s.symbol = o.symbol
            WHERE s.is_active ORDER BY date""",
        conn, parse_dates=["date"])
    return df


def _pivot(df, col):
    return df.pivot(index="date", columns="symbol", values=col).sort_index()


# ── breadth ──────────────────────────────────────────────────────────────────
def scan_breadth(conn):
    df = _load(conn)
    c = _pivot(df, "close")
    ret = c.pct_change()
    up4, dn4 = (ret >= 0.04).sum(axis=1), (ret <= -0.04).sum(axis=1)
    r5 = (up4.rolling(5).sum() / dn4.rolling(5).sum().replace(0, np.nan)).round(2)
    r10 = (up4.rolling(10).sum() / dn4.rolling(10).sum().replace(0, np.nan)).round(2)
    ma200 = c.rolling(200, min_periods=200).mean()
    above = ((c > ma200).sum(axis=1) / ma200.notna().sum(axis=1).replace(0, np.nan) * 100).round(1)
    universe = c.notna().sum(axis=1)
    spx = pd.read_sql("SELECT time::date AS date, close FROM us_index_ohlcv WHERE symbol='^GSPC' ORDER BY 1",
                      conn, parse_dates=["date"]).set_index("date")["close"]
    rows = []
    for d in c.index[1:]:
        dd = d.date()
        rows.append((dd, int(up4[d]), int(dn4[d]),
                     None if pd.isna(r5[d]) else float(r5[d]),
                     None if pd.isna(r10[d]) else float(r10[d]),
                     None if pd.isna(above[d]) else float(above[d]),
                     int(universe[d]),
                     float(spx[d]) if d in spx.index and pd.notna(spx[d]) else None))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO scanner_us_breadth
              (date, up_4pct, down_4pct, ratio_5d, ratio_10d, pct_above_200dma, universe, spx_close)
            VALUES %s
            ON CONFLICT (date) DO UPDATE SET
              up_4pct=EXCLUDED.up_4pct, down_4pct=EXCLUDED.down_4pct,
              ratio_5d=EXCLUDED.ratio_5d, ratio_10d=EXCLUDED.ratio_10d,
              pct_above_200dma=EXCLUDED.pct_above_200dma, universe=EXCLUDED.universe,
              spx_close=EXCLUDED.spx_close
        """, rows, page_size=500)
    conn.commit()
    return len(rows)


# ── fresh breakouts (3M/6M/1Y) ───────────────────────────────────────────────
def scan_breakouts(conn):
    df = _load(conn, "close, volume")
    c, v = _pivot(df, "close"), _pivot(df, "volume")
    if len(c) < 66:
        return 0
    run_date = c.index[-1].date()
    c0, c1, v0 = c.iloc[-1], c.iloc[-2], v.iloc[-1]
    chg = ((c0 - c1) / c1 * 100).round(2)
    best = {}
    for rank, (label, n) in enumerate({"3M": 63, "6M": 126, "1Y": 252}.items()):
        if len(c) < n + 2:
            continue
        pm, pn = c.shift(1).rolling(n).max(), c.shift(1).rolling(n).min()
        hi = (c0 > pm.iloc[-1]) & ~(c1 > pm.iloc[-2]) & pm.iloc[-1].notna() & pm.iloc[-2].notna()
        lo = (c0 < pn.iloc[-1]) & ~(c1 < pn.iloc[-2]) & pn.iloc[-1].notna() & pn.iloc[-2].notna()
        for s in c.columns[hi.fillna(False)]:
            best[(s, "HIGH")] = f"{label}_HIGH"
        for s in c.columns[lo.fillna(False)]:
            best[(s, "LOW")] = f"{label}_LOW"
    rows = []
    for (s, _), t in best.items():
        cl = c0.get(s)
        if cl is None or np.isnan(cl):
            continue
        vol = v0.get(s)
        rows.append((run_date, s, round(float(cl), 2),
                     None if pd.isna(chg.get(s)) else float(chg[s]),
                     int(vol) if pd.notna(vol) else None, t))
    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_us_breakouts WHERE run_date=%s", (run_date,))
        if rows:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO scanner_us_breakouts (run_date, symbol, close, change_pct, volume, breakout_type)
                VALUES %s ON CONFLICT DO NOTHING""", rows, page_size=500)
    conn.commit()
    return len(rows)


# ── daily RVOL movers ────────────────────────────────────────────────────────
def scan_rvol(conn):
    df = _load(conn, "close, volume")
    c, v = _pivot(df, "close"), _pivot(df, "volume")
    if len(c) < 22:
        return 0
    run_date = c.index[-1].date()
    c0, c1, v0 = c.iloc[-1], c.iloc[-2], v.iloc[-1]
    av20 = v.iloc[-21:-1].mean()
    rows = []
    for s in c.columns:
        cl, p, vv, a = c0.get(s), c1.get(s), v0.get(s), av20.get(s)
        if any(pd.isna(x) for x in (cl, p, vv, a)) or p <= 0 or a <= 0:
            continue
        rows.append((run_date, s, round(float(cl), 2), round(float(cl / p - 1) * 100, 2),
                     int(vv), int(a), round(float(vv) / float(a), 2)))
    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_us_rvol WHERE run_date=%s", (run_date,))
        if rows:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO scanner_us_rvol (run_date, symbol, close, chg_pct, volume, avg_vol_20d, rvol)
                VALUES %s ON CONFLICT DO NOTHING""", rows, page_size=500)
    conn.commit()
    return len(rows)


# ── episodic pivots (3-month leaderboard) ────────────────────────────────────
_CAL_URL = "https://api.nasdaq.com/api/calendar/earnings"
_CAL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json",
}


def _earnings_calendar(days, tdays):
    """(symbol, affected trading day) -> report date, from the NASDAQ earnings
    calendar (one request per calendar day; the endpoint that already works
    from GH runners, unlike per-symbol Yahoo lookups which get rate-limited).
    Pre-market reports hit the same session, after-hours the next one;
    unknown timing counts for both."""
    import requests

    def td_on_or_after(d):
        pos = tdays.searchsorted(pd.Timestamp(d))
        return tdays[pos].date() if pos < len(tdays) else None

    def td_after(d):
        pos = tdays.searchsorted(pd.Timestamp(d), side="right")
        return tdays[pos].date() if pos < len(tdays) else None

    out, failed = {}, 0
    for day in days:
        try:
            r = requests.get(_CAL_URL, params={"date": day.isoformat()},
                             headers=_CAL_HEADERS, timeout=30)
            r.raise_for_status()
            rows = (r.json().get("data") or {}).get("rows") or []
        except Exception as exc:
            failed += 1
            log.warning("earnings calendar %s failed: %s", day, exc)
            time.sleep(1)
            continue
        for x in rows:
            sym = (x.get("symbol") or "").strip()
            if not sym:
                continue
            t = x.get("time") or ""
            if "pre-market" in t:
                affected = {td_on_or_after(day)}
            elif "after-hours" in t:
                affected = {td_after(day)}
            else:
                affected = {td_on_or_after(day), td_after(day)}
            for a in affected:
                if a is not None:
                    out.setdefault((sym, a), day)
        time.sleep(0.15)
    log.info("ep: earnings calendar %d days fetched, %d failed, %d (symbol, day) pairs",
             len(days) - failed, failed, len(out))
    return out


def scan_ep(conn):
    df = pd.read_sql(
        """SELECT o.symbol, o.time::date AS date, o.open, o.close, o.volume
           FROM us_ohlcv o JOIN us_stocks s ON s.symbol=o.symbol
           WHERE s.is_active ORDER BY date""", conn, parse_dates=["date"])
    o, c, v = _pivot(df, "open"), _pivot(df, "close"), _pivot(df, "volume")
    if len(c) < 60:
        return 0
    prev = c.shift(1)
    gap, move = o / prev - 1, c / prev - 1
    vavg = v.rolling(50, min_periods=20).mean().shift(1)
    vr = v / vavg
    # standard EP: gap >=1%, move >=7%, vol >=3x 50d
    bull = (gap >= 0.01) & (move >= 0.07) & (vr >= 3.0)
    bear = (gap <= -0.01) & (move <= -0.07) & (vr >= 3.0)
    # mega-cap EP: huge-volume multiples don't happen on the biggest names —
    # accept a >=12% move on >=2x volume (catches AMZN/MSFT-style earnings pops)
    bull |= (gap >= 0.01) & (move >= 0.12) & (vr >= 2.0)
    bear |= (gap <= -0.01) & (move <= -0.12) & (vr >= 2.0)
    last_d = c.index[-1]
    cutoff = last_d - pd.DateOffset(months=3)
    lc = c.iloc[-1]
    recs = []
    for mask, typ in ((bull, "BULLISH"), (bear, "BEARISH")):
        hits = mask[mask.index >= cutoff].stack()
        for (d, s), hit in hits.items():
            if not hit:
                continue
            pc = c.at[d, s]
            if pd.isna(pc) or pd.isna(lc.get(s)):
                continue
            recs.append(dict(d=d.date(), s=s, t=typ, close=float(pc),
                             gap=round(float(gap.at[d, s]) * 100, 2),
                             mv=round(float(move.at[d, s]) * 100, 2),
                             vr=round(float(vr.at[d, s]), 1),
                             last=float(lc[s]),
                             ret=round((float(lc[s]) / float(pc) - 1) * 100, 2)))
    # keep latest pivot per symbol, rank by return
    recs.sort(key=lambda r: r["d"], reverse=True)
    seen, keep = set(), []
    for r in recs:
        if r["s"] in seen:
            continue
        seen.add(r["s"]); keep.append(r)
    keep.sort(key=lambda r: r["ret"], reverse=True)

    # flag pivots that landed on an earnings release
    weekdays = [d.date() for d in pd.bdate_range(cutoff, last_d)]
    cal = _earnings_calendar(weekdays, c.index)

    rows = [(r["d"], r["s"], r["t"], round(r["close"], 2), r["gap"], r["mv"], r["vr"],
             last_d.date(), round(r["last"], 2), r["ret"], i + 1, cal.get((r["s"], r["d"])))
            for i, r in enumerate(keep)]
    log.info("ep: %d of %d pivots on an earnings release", sum(1 for r in rows if r[-1]), len(rows))
    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_us_ep")
        if rows:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO scanner_us_ep (run_date, symbol, ep_type, close, gap_pct, move_pct,
                  vol_ratio, last_date, last_close, return_since_pivot, rnk, earnings_date)
                VALUES %s ON CONFLICT DO NOTHING""", rows, page_size=500)
    conn.commit()
    return len(rows)


# ── VCP ──────────────────────────────────────────────────────────────────────
def scan_vcp(conn):
    df = _load(conn, "close, high, low")
    c, h, l = _pivot(df, "close"), _pivot(df, "high"), _pivot(df, "low")
    if len(c) < 64:
        return 0
    run_date = c.index[-1].date()
    c0, c63 = c.iloc[-1], c.iloc[-64]
    h15, l15 = h.iloc[-15:].max(), l.iloc[-15:].min()
    rng = (h15 - l15) / l15.replace(0, np.nan)
    rows = []
    for s in c.columns:
        cl, cb, hh, ll, r = c0.get(s), c63.get(s), h15.get(s), l15.get(s), rng.get(s)
        if any(pd.isna(x) for x in (cl, cb, hh, ll, r)) or cb <= 0:
            continue
        ret3 = (cl - cb) / cb
        if ret3 >= 0.25 and r < 0.15:
            rows.append((run_date, s, round(float(cl), 2), round(float(ret3) * 100, 2),
                         round(float(hh), 2), round(float(ll), 2), round(float(r) * 100, 2)))
    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_us_vcp WHERE run_date=%s", (run_date,))
        if rows:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO scanner_us_vcp (run_date, symbol, close, return_3m, high_15d, low_15d, range_15d_pct)
                VALUES %s ON CONFLICT DO NOTHING""", rows, page_size=500)
    conn.commit()
    return len(rows)


# ── sectors + granular themes ────────────────────────────────────────────────
SECTOR_ETFS = {
    "Technology": "XLK", "Finance": "XLF", "Health Care": "XLV", "Energy": "XLE",
    "Consumer Discretionary": "XLY", "Consumer Staples": "XLP", "Industrials": "XLI",
    "Basic Materials": "XLB", "Real Estate": "XLRE", "Utilities": "XLU",
    "Telecommunications": "XLC",
}

# (theme, etf, NASDAQ industry tags — matched after strip())
THEMES = [
    ("Semiconductors", "SMH", ["Semiconductors"]),
    ("Software & Cloud", "IGV", [
        "Computer Software: Prepackaged Software",
        "Computer Software: Programming Data Processing", "EDP Services",
        "Retail: Computer Software & Peripheral Equipment"]),
    ("Hardware & Networking", "XLK", [
        "Computer Manufacturing", "Computer peripheral equipment", "Electronic Components",
        "Computer Communications Equipment",
        "Radio And Television Broadcasting And Communications Equipment"]),
    ("Banks", "KBE", ["Major Banks"]),
    ("Regional Banks", "KRE", ["Commercial Banks", "Savings Institutions"]),
    ("Capital Markets", "IAI", [
        "Investment Bankers/Brokers/Service", "Investment Managers",
        "Finance/Investors Services", "Diversified Financial Services"]),
    ("Insurance", "IAK", [
        "Property-Casualty Insurers", "Life Insurance", "Specialty Insurers",
        "Accident &Health Insurance"]),
    ("Payments & Fin Services", "IPAY", ["Business Services", "Finance: Consumer Services"]),
    ("Biotech", "XBI", [
        "Biotechnology: Biological Products (No Diagnostic Substances)",
        "Biotechnology: Commercial Physical & Biological Resarch",
        "Biotechnology: In Vitro & In Vivo Diagnostic Substances"]),
    ("Pharma", "XPH", [
        "Biotechnology: Pharmaceutical Preparations", "Other Pharmaceuticals",
        "Medicinal Chemicals and Botanical Products"]),
    ("Medical Devices", "IHI", [
        "Medical/Dental Instruments", "Medical Specialities", "Medical Electronics",
        "Ophthalmic Goods", "Biotechnology: Electromedical & Electrotherapeutic Apparatus",
        "Biotechnology: Laboratory Analytical Instruments", "Precision Instruments"]),
    ("Healthcare Services", "IHF", [
        "Hospital/Nursing Management", "Medical/Nursing Services",
        "Misc Health and Biotechnology Services"]),
    ("Oil & Gas", "XOP", ["Oil & Gas Production", "Integrated oil Companies", "Coal Mining"]),
    ("Oil Services", "OIH", ["Oilfield Services/Equipment", "Oil and Gas Field Machinery"]),
    ("Midstream & Gas", "AMLP", ["Oil/Gas Transmission", "Natural Gas Distribution"]),
    ("Aerospace & Defense", "ITA", [
        "Aerospace", "Military/Government/Technical", "Ordnance And Accessories"]),
    ("Homebuilders & Building", "XHB", [
        "Homebuilding", "Building Products", "Building Materials", "RETAIL: Building Materials"]),
    ("Retail", "XRT", [
        "Department/Specialty Retail Stores", "Clothing/Shoe/Accessory Stores",
        "Other Specialty Stores", "Catalog/Specialty Distribution",
        "Consumer Electronics/Video Chains", "Auto & Home Supply Stores",
        "Retail-Auto Dealers and Gas Stations", "Food Chains",
        "Retail-Drug Stores and Proprietary Stores", "Home Furnishings"]),
    ("Autos & EV", "CARZ", [
        "Auto Manufacturing", "Motor Vehicles", "Auto Parts:O.E.M.", "Automotive Aftermarket"]),
    ("Transports & Logistics", "IYT", [
        "Railroads", "Trucking Freight/Courier Services", "Air Freight/Delivery Services",
        "Marine Transportation", "Transportation Services", "Integrated Freight & Logistics"]),
    ("Gold & Miners", "GDX", ["Precious Metals", "Metal Mining", "Other Metals and Minerals"]),
    ("Steel & Mining", "SLX", [
        "Steel/Iron Ore", "Aluminum", "Mining & Quarrying of Nonmetallic Minerals (No Fuels)"]),
    ("Chemicals", "XLB", [
        "Major Chemicals", "Specialty Chemicals", "Agricultural Chemicals", "Paints/Coatings"]),
    ("Travel & Leisure", "PEJ", [
        "Restaurants", "Hotels/Resorts", "Services-Misc. Amusement & Recreation",
        "Recreational Games/Products/Toys"]),
    ("Media & Telecom", "XLC", [
        "Broadcasting", "Cable & Other Pay Television Services", "Newspapers/Magazines",
        "Publishing", "Advertising", "Telecommunications Equipment"]),
    ("Industrial Machinery", "XLI", [
        "Industrial Machinery/Components", "Metal Fabrications", "Fluid Controls",
        "Construction/Ag Equipment/Trucks", "Electrical Products", "Industrial Specialties"]),
    ("Infrastructure & E&C", "PAVE", [
        "Engineering & Construction", "Water Sewer Pipeline Comm & Power Line Construction",
        "Pollution Control Equipment", "Environmental Services"]),
    ("Power Generation", "XLU", ["Power Generation", "Electric Utilities: Central"]),
    ("Food & Household", "PBJ", [
        "Beverages (Production/Distribution)", "Packaged Foods", "Meat/Poultry/Fish",
        "Specialty Foods", "Farming/Seeds/Milling", "Food Distributors",
        "Package Goods/Cosmetics"]),
]


def scan_groups(conn):
    df = _load(conn)
    c = _pivot(df, "close")
    if len(c) < 23:
        return 0
    run_date = c.index[-1].date()
    meta = pd.read_sql(
        "SELECT symbol, sector, trim(industry) AS industry FROM us_stocks WHERE is_active",
        conn).set_index("symbol")
    ec = pd.read_sql(
        "SELECT symbol, time::date AS date, close FROM us_index_ohlcv "
        "WHERE left(symbol, 1) <> '^' ORDER BY date",
        conn, parse_dates=["date"]).pivot(index="date", columns="symbol", values="close").sort_index()

    def etf_ret(sym, k):
        if not sym or sym not in ec.columns:
            return None
        s = ec[sym].dropna()
        return round(float(s.iloc[-1] / s.iloc[-1 - k] - 1) * 100, 2) if len(s) > k else None

    ind2theme = {i: name for name, _, inds in THEMES for i in inds}
    theme_etf = {name: etf for name, etf, _ in THEMES}

    rows = []
    for period, k in (("day", 1), ("week", 5), ("month", 21)):
        if len(c) <= k:
            continue
        ret = (c.iloc[-1] / c.iloc[-1 - k] - 1) * 100
        g = pd.DataFrame({"ret": ret}).join(meta).dropna(subset=["ret"])
        g["theme"] = g["industry"].map(ind2theme)
        for kind, col, etf_of in (("sector", "sector", SECTOR_ETFS.get),
                                  ("theme", "theme", theme_etf.get)):
            for gname, grp in g.dropna(subset=[col]).groupby(col):
                lead = grp["ret"].sort_values(ascending=False).head(3)
                rows.append((run_date, period, kind, gname, etf_of(gname),
                             etf_ret(etf_of(gname), k),
                             round(float(grp["ret"].mean()), 2),
                             int((grp["ret"] > 0).sum()), int((grp["ret"] < 0).sum()),
                             len(grp),
                             ", ".join(f"{s} {v:+.1f}" for s, v in lead.items())))
    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_us_groups WHERE run_date=%s", (run_date,))
        if rows:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO scanner_us_groups (run_date, period, kind, name, etf, etf_return,
                  avg_return, up_count, down_count, stock_count, leaders)
                VALUES %s ON CONFLICT DO NOTHING""", rows, page_size=500)
    conn.commit()
    return len(rows)


# ── snapback (undercut & rally / spring) ─────────────────────────────────────
SNAP_WINDOWS = [("1Y", 252), ("6M", 126), ("3M", 63), ("1M", 21)]  # deepest first
SNAP_HISTORY = 60       # trailing sessions re-scanned each run (rolling history)
SNAP_CLOSEPOS = 0.65    # SAME_DAY: close in the top 35% of the day's range


def scan_snapback(conn):
    """Capitulation snapback: day D undercuts the lowest low of the prior N
    sessions (deepest of 1M/3M/6M/1Y wins), then either closes that same day
    in the top 35% of its range (SAME_DAY spring bar) or closes back above
    the broken level within 1-2 sessions (D1/D2 reclaim). Score 0-5: window
    >=3M, SAME_DAY bar, flush volume >=1.5x, snap volume >=1.5x, >200-DMA.
    The trailing SNAP_HISTORY sessions are fully refreshed every run so the
    table keeps a rolling signal history."""
    df = _load(conn, "close, high, low, volume")
    c, h, l, v = _pivot(df, "close"), _pivot(df, "high"), _pivot(df, "low"), _pivot(df, "volume")
    if len(c) < 25:
        return 0
    av20 = v.shift(1).rolling(20, min_periods=10).mean()
    ma200 = c.rolling(200, min_periods=180).mean()
    # gap-tolerant prior-low windows: one missing bar must not NaN-out a month
    pm = {lab: l.shift(1).rolling(n, min_periods=max(5, int(n * 0.9))).min()
          for lab, n in SNAP_WINDOWS}

    fcache = {}

    def flushes(q):
        """Symbols whose bar q undercut a prior N-day low -> {sym: (label, broken_level)};
        the deepest broken window wins."""
        if q in fcache:
            return fcache[q]
        out, lq = {}, l.iloc[q]
        for lab, _ in SNAP_WINDOWS:
            lvl = pm[lab].iloc[q]
            hit = (lq < lvl) & lvl.notna() & lq.notna()
            for s in l.columns[hit.values]:
                out.setdefault(s, (lab, float(lvl[s])))
        fcache[q] = out
        return out

    def mk(p, q, s, lab, broken, typ):
        d, dq = c.index[p], c.index[q]
        cl, xl = float(c.at[d, s]), float(l.at[dq, s])
        hi, lo = h.at[d, s], l.at[d, s]
        cp = (cl - float(lo)) / (float(hi) - float(lo)) \
            if pd.notna(hi) and pd.notna(lo) and float(hi) > float(lo) else None
        fv, fa = v.at[dq, s], av20.at[dq, s]
        sv, sa = v.at[d, s], av20.at[d, s]
        fr = round(float(fv) / float(fa), 2) if pd.notna(fv) and pd.notna(fa) and fa > 0 else None
        sr = round(float(sv) / float(sa), 2) if pd.notna(sv) and pd.notna(sa) and sa > 0 else None
        m2 = ma200.at[d, s]
        ab = bool(cl >= float(m2)) if pd.notna(m2) else None
        score = int(sum([lab in ("3M", "6M", "1Y"), typ == "SAME_DAY",
                         (fr or 0) >= 1.5, (sr or 0) >= 1.5, bool(ab)]))
        return (d.date(), s, lab, typ, dq.date(), round(xl, 2), round(broken, 2),
                round(cl, 2), round((cl / xl - 1) * 100, 2),
                None if cp is None else round(cp * 100, 1), fr, sr, ab, score)

    start = max(len(c) - SNAP_HISTORY, 3)
    rows = {}
    for p in range(start, len(c)):
        d, dt = c.index[p], c.index[p].date()
        # SAME_DAY: undercut + close in the top of the range
        for s, (lab, broken) in flushes(p).items():
            hi, lo, cl = h.at[d, s], l.at[d, s], c.at[d, s]
            if any(pd.isna(x) for x in (hi, lo, cl)) or float(hi) <= float(lo):
                continue
            if (float(cl) - float(lo)) / (float(hi) - float(lo)) >= SNAP_CLOSEPOS:
                rows[(dt, s)] = mk(p, p, s, lab, broken, "SAME_DAY")
        # D1/D2: first close back above the level broken 1-2 sessions ago
        for back, typ in ((1, "D1"), (2, "D2")):
            q = p - back
            if q < 0:
                continue
            for s, (lab, broken) in flushes(q).items():
                if (dt, s) in rows:
                    continue
                cl = c.at[d, s]
                if pd.isna(cl) or float(cl) <= broken:
                    continue
                interim = [c.at[c.index[q + k], s] for k in range(back)]
                if any(pd.notna(x) and float(x) > broken for x in interim):
                    continue                     # already reclaimed earlier
                rows[(dt, s)] = mk(p, q, s, lab, broken, typ)

    out = list(rows.values())
    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_us_snapback WHERE run_date >= %s",
                    (c.index[start].date(),))
        if out:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO scanner_us_snapback (run_date, symbol, window_label, snap_type,
                  low_date, extreme_low, broken_level, close, pct_off_low, close_pos_pct,
                  flush_vol_ratio, snap_vol_ratio, above_200dma, score)
                VALUES %s ON CONFLICT DO NOTHING""", out, page_size=500)
    conn.commit()
    return len(out)


SCANNERS = [("breadth", scan_breadth), ("breakouts", scan_breakouts),
            ("rvol", scan_rvol), ("ep", scan_ep), ("vcp", scan_vcp),
            ("groups", scan_groups), ("snapback", scan_snapback)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip", type=str, default="")
    args = ap.parse_args()
    skip = {s.strip() for s in args.skip.split(",") if s.strip()}

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    results = {}
    try:
        for name, fn in SCANNERS:
            if name in skip:
                results[name] = "skipped"; continue
            t0 = time.time()
            try:
                n = fn(conn)
                results[name] = f"{n} rows in {time.time()-t0:.1f}s"
                log.info("OK %s: %s", name, results[name])
            except Exception as e:
                results[name] = f"FAILED: {e}"
                log.error("FAIL %s: %s", name, e, exc_info=True)
                try:
                    conn.rollback()
                except Exception:
                    pass
    finally:
        conn.close()
    log.info("=" * 50)
    for k, s in results.items():
        log.info("  %-10s %s", k, s)


if __name__ == "__main__":
    main()
