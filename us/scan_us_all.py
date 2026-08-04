"""
scan_us_all.py  —  US EOD scanner suite (orchestrator + all scanners in one file).

Scanners over the us_stocks / us_ohlcv universe (1-yr history):

  breadth    -> scanner_us_breadth    daily up/down 4%, 5d/10d ratios, %>200-DMA, S&P 500 close
  breakouts  -> scanner_us_breakouts  FRESH new 3M/6M/1Y closing highs/lows (first day only,
                                      longest window wins; 1Y activates as history accrues)
  rvol       -> scanner_us_rvol       day change + volume vs 20d avg (all stocks)
  ep         -> scanner_us_ep         Episodic Pivots (gap>=1%, move>=7%, vol>=3x 50d),
                                      6-month leaderboard ranked by return since pivot;
                                      pivots on an earnings release get earnings_date set
  vcp        -> scanner_us_vcp        3-month return >= 25% and 15-day range < 15%
  groups     -> scanner_us_groups     sector + granular-theme performance day/week/month,
                                      each with its tracked ETF's return + top-3 leaders
  earnings   -> us_earnings           earnings calendar (last ~7 sessions + next 7 days),
                                      beat/miss vs consensus + day reaction
  news       -> us_news               headlines for today's biggest movers + SPY/QQQ

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


# ── episodic pivots (6-month leaderboard) ────────────────────────────────────
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
    bull = (gap >= 0.01) & (move >= 0.07) & (vr >= 3.0)
    bear = (gap <= -0.01) & (move <= -0.07) & (vr >= 3.0)
    last_d = c.index[-1]
    cutoff = last_d - pd.DateOffset(months=6)
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


# ── earnings calendar + beat/miss ────────────────────────────────────────────
def _num(v):
    """Parse NASDAQ number strings: '$1.98', '(0.26)' = negative, 'N/A' = None."""
    if v is None or isinstance(v, (int, float)):
        return None if v is None else float(v)
    s = str(v).replace("$", "").replace(",", "").strip()
    if not s or s.upper() in ("N/A", "NA", "--", ""):
        return None
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()")
    try:
        return -float(s) if neg else float(s)
    except ValueError:
        return None


def scan_earnings(conn):
    """Earnings for the top-1000 universe: last ~7 sessions (with beat/miss
    verdicts from the NASDAQ earnings-surprise endpoint + day reaction) and
    the next 7 calendar days (PENDING)."""
    import requests

    uni = set(pd.read_sql("SELECT symbol FROM us_stocks WHERE is_active", conn)["symbol"])
    c = _pivot(_load(conn), "close")
    today = pd.Timestamp.today().normalize()

    days = [d for d in pd.date_range(today - pd.Timedelta(days=10), today + pd.Timedelta(days=7))
            if d.weekday() < 5]
    recs = {}
    for day in days:
        try:
            r = requests.get(_CAL_URL, params={"date": day.date().isoformat()},
                             headers=_CAL_HEADERS, timeout=30)
            r.raise_for_status()
            rows = (r.json().get("data") or {}).get("rows") or []
        except Exception as exc:
            log.warning("earnings calendar %s failed: %s", day.date(), exc)
            time.sleep(1)
            continue
        for x in rows:
            sym = (x.get("symbol") or "").strip()
            if sym not in uni:
                continue
            t = (x.get("time") or "").replace("time-", "")
            recs[(day.date(), sym)] = {
                "time": t if t in ("pre-market", "after-hours") else "unknown",
                "est": _num(x.get("epsForecast")),
            }
        time.sleep(0.15)
    log.info("earnings: %d universe reports in window", len(recs))

    # actuals for reporters whose date has passed
    past = sorted({s for (d, s) in recs if d <= today.date()})
    actuals = {}
    for sym in past:
        try:
            r = requests.get(f"https://api.nasdaq.com/api/company/{sym}/earnings-surprise",
                             headers=_CAL_HEADERS, timeout=30)
            rows = (((r.json().get("data") or {}).get("earningsSurpriseTable") or {})
                    .get("rows") or [])
        except Exception as exc:
            log.warning("earnings surprise %s failed: %s", sym, exc)
            rows = []
        for x in rows:
            dt = pd.to_datetime(x.get("dateReported"), errors="coerce")
            if pd.notna(dt):
                actuals[(sym, dt.date())] = (_num(x.get("eps")),
                                             _num(x.get("consensusForecast")),
                                             _num(x.get("percentageSurprise")))
        time.sleep(0.2)

    def reaction(sym, day, when):
        """Close-over-close move on the session the report hits."""
        if sym not in c.columns:
            return None
        s = c[sym].dropna()
        pos = s.index.searchsorted(pd.Timestamp(day), side="left" if when != "after-hours" else "right")
        if pos >= len(s) or pos < 1:
            return None
        if s.index[pos].date() > today.date():
            return None
        return round(float(s.iloc[pos] / s.iloc[pos - 1] - 1) * 100, 2)

    out = []
    for (day, sym), rec in recs.items():
        eps_a = eps_e = spct = None
        # surprise rows are keyed by their own dateReported; match within 4 days
        for (s2, d2), (a, e, p) in actuals.items():
            if s2 == sym and abs((d2 - day).days) <= 4:
                eps_a, eps_e, spct = a, (e if e is not None else rec["est"]), p
                break
        if eps_e is None:
            eps_e = rec["est"]
        if day > today.date() or (eps_a is None and day == today.date()):
            verdict = "PENDING"
        elif eps_a is None:
            verdict = "PENDING"
        elif eps_e is None:
            verdict = "REPORTED"
        elif eps_a > eps_e:
            verdict = "BEAT"
        elif eps_a < eps_e:
            verdict = "MISS"
        else:
            verdict = "INLINE"
        out.append((day, sym, rec["time"], eps_e, eps_a, spct, verdict,
                    reaction(sym, day, rec["time"]) if day <= today.date() else None))

    with conn.cursor() as cur:
        cur.execute("DELETE FROM us_earnings WHERE report_date >= %s", (days[0].date(),))
        cur.execute("DELETE FROM us_earnings WHERE report_date < %s",
                    ((today - pd.Timedelta(days=30)).date(),))
        if out:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO us_earnings (report_date, symbol, report_time, eps_est, eps_actual,
                  surprise_pct, verdict, reaction_pct)
                VALUES %s
                ON CONFLICT (report_date, symbol) DO UPDATE SET
                  report_time=EXCLUDED.report_time, eps_est=EXCLUDED.eps_est,
                  eps_actual=EXCLUDED.eps_actual, surprise_pct=EXCLUDED.surprise_pct,
                  verdict=EXCLUDED.verdict, reaction_pct=EXCLUDED.reaction_pct,
                  updated_at=now()""", out, page_size=500)
    conn.commit()
    return len(out)


# ── breaking news (top movers + market) ──────────────────────────────────────
def scan_news(conn):
    """Yahoo headlines for today's biggest movers (from scanner_us_rvol) plus
    market-level news via SPY/QQQ. Deduped on Yahoo's article id, pruned to 7d."""
    import yfinance as yf

    movers = pd.read_sql("""
        SELECT r.symbol FROM scanner_us_rvol r
        WHERE r.run_date = (SELECT max(run_date) FROM scanner_us_rvol)
          AND (abs(r.chg_pct) >= 3 OR r.rvol >= 2)
        ORDER BY abs(r.chg_pct) DESC LIMIT 25""", conn)["symbol"].tolist()
    syms = [("SPY", None), ("QQQ", None)] + [(s, s) for s in movers]

    items = {}
    for ysym, tag in syms:
        try:
            news = yf.Ticker(ysym.replace(".", "-")).news or []
        except Exception as exc:
            log.warning("news %s failed: %s", ysym, exc)
            news = []
        for it in news[:8]:
            content = it.get("content") or it
            nid = str(it.get("id") or it.get("uuid") or
                      (content.get("canonicalUrl") or {}).get("url") or "")
            title = (content.get("title") or "").strip()
            if not nid or not title:
                continue
            ts = content.get("pubDate") or content.get("providerPublishTime")
            ts = (pd.to_datetime(ts, unit="s", utc=True) if isinstance(ts, (int, float))
                  else pd.to_datetime(ts, utc=True, errors="coerce"))
            row = (nid, tag, title[:300],
                   (content.get("provider") or {}).get("displayName") or content.get("publisher"),
                   (content.get("canonicalUrl") or {}).get("url") or content.get("link"),
                   None if pd.isna(ts) else ts.to_pydatetime())
            # prefer the stock-tagged copy of a story over the market-level one
            if nid not in items or (tag and not items[nid][1]):
                items[nid] = row
        time.sleep(0.4)

    with conn.cursor() as cur:
        if items:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO us_news (id, symbol, headline, publisher, url, published_at)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                  symbol=COALESCE(us_news.symbol, EXCLUDED.symbol),
                  headline=EXCLUDED.headline, fetched_at=now()""",
                list(items.values()), page_size=200)
        cur.execute("DELETE FROM us_news WHERE COALESCE(published_at, fetched_at) < now() - interval '7 days'")
    conn.commit()
    return len(items)


SCANNERS = [("breadth", scan_breadth), ("breakouts", scan_breakouts),
            ("rvol", scan_rvol), ("ep", scan_ep), ("vcp", scan_vcp),
            ("groups", scan_groups), ("earnings", scan_earnings),
            ("news", scan_news)]


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
