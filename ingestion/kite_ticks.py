"""
kite_ticks.py — stream live Zerodha (Kite) ticks for F&O stocks + indices into
the `ticks` table. The dashboard's "Ticks" tab polls that table and refreshes.

This is a CONTINUOUS process (Kite WebSocket) — run it on an always-on machine
during market hours (your PC or a small cloud VM). It is NOT a cron job.

Prerequisites (.env):
    DATABASE_URL=...                 # Supabase connection string
    KITE_API_KEY=...                 # from your Kite Connect app
    KITE_ACCESS_TOKEN=...            # daily token (see ingestion/kite_login.py)

Run:
    python ingestion/kite_ticks.py
"""

import os
import threading
import time
from datetime import datetime, time as dtime, timedelta, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from kiteconnect import KiteConnect, KiteTicker

load_dotenv(Path(__file__).parent.parent / ".env")
DB_URL = os.environ["DATABASE_URL"]
API_KEY = os.environ["KITE_API_KEY"]
ACCESS_TOKEN = os.environ["KITE_ACCESS_TOKEN"]
FLUSH_SECS = 1.5

# DB index symbol -> Kite NSE index tradingsymbol
INDEX_MAP = {
    "^NSEI": "NIFTY 50", "^NSEBANK": "NIFTY BANK", "NIFTY_FIN_SERVICE.NS": "NIFTY FIN SERVICE",
    "^CNXIT": "NIFTY IT", "^CNXAUTO": "NIFTY AUTO", "^CNXPHARMA": "NIFTY PHARMA",
    "^CNXFMCG": "NIFTY FMCG", "^CNXMETAL": "NIFTY METAL", "^CNXENERGY": "NIFTY ENERGY",
    "^CNXREALTY": "NIFTY REALTY", "^CNXPSUBANK": "NIFTY PSU BANK", "^CRSLDX": "NIFTY 500",
    "^NSMIDCP": "NIFTY NEXT 50",
}

UPSERT = """
INSERT INTO ticks (symbol, is_index, ltp, prev_close, chg_pct, vol, ts, updated_at)
VALUES %s
ON CONFLICT (symbol) DO UPDATE SET
  ltp=EXCLUDED.ltp, prev_close=COALESCE(EXCLUDED.prev_close, ticks.prev_close),
  chg_pct=EXCLUDED.chg_pct, vol=EXCLUDED.vol, ts=EXCLUDED.ts, updated_at=now()
"""

UPSERT_FUT = """
INSERT INTO ticks_fut (symbol, tradingsymbol, expiry, ltp, prev_close, chg_pct, vol, oi, ts, updated_at)
VALUES %s
ON CONFLICT (symbol) DO UPDATE SET
  tradingsymbol=EXCLUDED.tradingsymbol, expiry=EXCLUDED.expiry, ltp=EXCLUDED.ltp,
  prev_close=COALESCE(EXCLUDED.prev_close, ticks_fut.prev_close),
  chg_pct=EXCLUDED.chg_pct, vol=EXCLUDED.vol, oi=EXCLUDED.oi, ts=EXCLUDED.ts, updated_at=now()
"""

# intraday sector series: one row per sector/index per minute (feeds the Sector Trend tab)
HIST_SECS = 60
HIST_UPSERT = """
INSERT INTO sector_history (name, is_index, ts, chg_pct, ltp)
VALUES %s
ON CONFLICT (name, ts) DO UPDATE SET chg_pct=EXCLUDED.chg_pct, ltp=EXCLUDED.ltp
"""
IST = timezone(timedelta(hours=5, minutes=30))
EOD_IST = dtime(15, 35)   # after close: wipe the day's 1-min bars and stop


def clear_history(conn, before=None):
    """Delete sector_history rows — all of them, or only those before `before`."""
    with conn.cursor() as cur:
        if before is None:
            cur.execute("DELETE FROM sector_history")
        else:
            cur.execute("DELETE FROM sector_history WHERE ts < %s", (before,))
        n = cur.rowcount
    conn.commit()
    if n:
        print(f"Cleared {n} sector_history rows.")


def build_token_maps(kite, conn):
    """Return token->(symbol, is_index) and the prev_close per symbol."""
    with conn.cursor() as cur:
        cur.execute("SELECT symbol, sector FROM stocks WHERE is_active AND is_fno")
        sector_of = {r[0]: (r[1] or "Other") for r in cur.fetchall()}
        fno = set(sector_of)

    nse = kite.instruments("NSE")          # all NSE instruments (equity + indices)
    by_tsym = {i["tradingsymbol"]: i for i in nse}

    token_map = {}                          # instrument_token -> (db_symbol, is_index)
    kite_keys = []                          # "NSE:tradingsymbol" for the prev-close quote
    keymap = {}                             # kite_key -> db_symbol
    for sym in fno:
        inst = by_tsym.get(sym)
        if inst:
            token_map[inst["instrument_token"]] = (sym, False)
            kite_keys.append(f"NSE:{sym}"); keymap[f"NSE:{sym}"] = sym
    for db_sym, ktsym in INDEX_MAP.items():
        inst = by_tsym.get(ktsym)
        if inst:
            token_map[inst["instrument_token"]] = (db_sym, True)
            kite_keys.append(f"NSE:{ktsym}"); keymap[f"NSE:{ktsym}"] = db_sym

    prev_close = {}
    for i in range(0, len(kite_keys), 250):
        q = kite.quote(kite_keys[i:i + 250])
        for k, v in q.items():
            sym = keymap.get(k)
            if sym and v.get("ohlc"):
                prev_close[sym] = v["ohlc"].get("close")
        time.sleep(0.2)

    # current-month (front) futures contract per F&O underlying
    today = datetime.now(IST).date()
    best = {}
    for i in kite.instruments("NFO"):
        if i.get("instrument_type") != "FUT" or i.get("name") not in fno:
            continue
        exp = i.get("expiry")
        exp = exp.date() if hasattr(exp, "date") else exp
        if exp is None or exp < today:
            continue
        cur = best.get(i["name"])
        if cur is None or exp < cur[0]:
            best[i["name"]] = (exp, i)
    fut_map = {i["instrument_token"]: (name, i["tradingsymbol"], exp)
               for name, (exp, i) in best.items()}

    fut_prev = {}
    fut_keys = [f"NFO:{tsym}" for (_n, tsym, _e) in fut_map.values()]
    fut_keymap = {f"NFO:{tsym}": name for (name, tsym, _e) in fut_map.values()}
    for i in range(0, len(fut_keys), 250):
        q = kite.quote(fut_keys[i:i + 250])
        for k, v in q.items():
            sym = fut_keymap.get(k)
            if sym and v.get("ohlc"):
                fut_prev[sym] = v["ohlc"].get("close")
        time.sleep(0.2)
    return token_map, prev_close, sector_of, fut_map, fut_prev


def main():
    conn = psycopg2.connect(DB_URL)
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)

    # morning safety net: drop any bars left over from a previous session
    session_start = datetime.now(IST).replace(hour=9, minute=15, second=0, microsecond=0)
    clear_history(conn, before=session_start.astimezone(timezone.utc))

    token_map, prev_close, sector_of, fut_map, fut_prev = build_token_maps(kite, conn)
    tokens = list(token_map.keys()) + list(fut_map.keys())
    print(f"Subscribing to {len(tokens)} instruments "
          f"({sum(1 for v in token_map.values() if v[1])} indices, "
          f"{len(fut_map)} current-month futures)")

    latest = {}            # token -> (ltp, vol, ts, oi)
    lock = threading.Lock()
    last_hist = 0.0        # last sector_history append (epoch secs)
    state = {"last_rx": time.time()}   # last websocket tick receipt (watchdog)

    def on_ticks(ws, ticks):
        state["last_rx"] = time.time()
        with lock:
            for t in ticks:
                latest[t["instrument_token"]] = (
                    t.get("last_price"), t.get("volume_traded"),
                    t.get("exchange_timestamp") or datetime.now(timezone.utc),
                    t.get("oi"),
                )

    def on_connect(ws, response):
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)

    def on_close(ws, code, reason):
        print(f"[ws closed] {code} {reason}")

    def on_error(ws, code, reason):
        print(f"[ws error] {code} {reason}")

    def on_reconnect(ws, attempts):
        print(f"[ws reconnecting] attempt {attempts}")

    def make_ws():
        w = KiteTicker(API_KEY, ACCESS_TOKEN)
        w.on_ticks = on_ticks
        w.on_connect = on_connect
        w.on_close = on_close
        w.on_error = on_error
        w.on_reconnect = on_reconnect
        w.connect(threaded=True)
        return w

    kws = make_ws()
    print("Ticker running. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(FLUSH_SECS)
            if datetime.now(IST).time() >= EOD_IST:
                print("Market closed — removing the day's sector_history bars and stopping.")
                clear_history(conn)
                break
            # watchdog: KiteTicker's auto-reconnect sometimes never fires after an unclean
            # drop (code 1006) — if the feed goes silent, tear the socket down and rebuild it
            if time.time() - state["last_rx"] > 60:
                print("[watchdog] no ticks for 60s — rebuilding websocket")
                try:
                    kws.close()
                except Exception:
                    pass
                state["last_rx"] = time.time()
                kws = make_ws()
            with lock:
                snap = dict(latest)
            rows, fut_rows = [], []
            now = datetime.now(timezone.utc)
            for tok, (ltp, vol, ts, oi) in snap.items():
                if ltp is None:
                    continue
                if tok in token_map:
                    sym, is_idx = token_map[tok]
                    pc = prev_close.get(sym)
                    chg = round((ltp / pc - 1) * 100, 2) if pc else None
                    rows.append((sym, is_idx, round(float(ltp), 2),
                                 round(float(pc), 2) if pc else None, chg,
                                 int(vol) if vol else None, ts, now))
                else:
                    sym, tsym, exp = fut_map[tok]
                    pc = fut_prev.get(sym)
                    chg = round((ltp / pc - 1) * 100, 2) if pc else None
                    fut_rows.append((sym, tsym, exp, round(float(ltp), 2),
                                     round(float(pc), 2) if pc else None, chg,
                                     int(vol) if vol else None,
                                     int(oi) if oi else None, ts, now))
            try:
                if rows:
                    with conn.cursor() as cur:
                        execute_values(cur, UPSERT, rows, page_size=400)
                    conn.commit()
                if fut_rows:
                    with conn.cursor() as cur:
                        execute_values(cur, UPSERT_FUT, fut_rows, page_size=400)
                    conn.commit()
                if rows and time.time() - last_hist >= HIST_SECS:
                    last_hist = time.time()
                    ts_min = now.replace(second=0, microsecond=0)
                    by_sec, hist = {}, []
                    for (sym, is_idx, ltp, pc, chg, vol, ts, _n) in rows:
                        if is_idx:
                            hist.append((sym, True, ts_min, chg, ltp))
                        elif chg is not None:
                            by_sec.setdefault(sector_of.get(sym, "Other"), []).append(chg)
                    for sec, vals in by_sec.items():
                        hist.append((sec, False, ts_min, round(sum(vals) / len(vals), 3), None))
                    with conn.cursor() as cur:
                        execute_values(cur, HIST_UPSERT, hist, page_size=200)
                    conn.commit()
            except psycopg2.Error as e:
                # Supabase dropped the connection (network blip) — reconnect and carry on
                print(f"[db] write failed ({e.__class__.__name__}) — reconnecting")
                try:
                    conn.close()
                except Exception:
                    pass
                time.sleep(2)
                try:
                    conn = psycopg2.connect(DB_URL)
                except Exception as e2:
                    print(f"[db] reconnect failed: {e2} — will retry next flush")
    except KeyboardInterrupt:
        print("Stopping.")
    finally:
        try:
            kws.close()
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    # supervisor: restart on any crash (network loss etc.); a normal return (EOD / Ctrl+C) exits
    while True:
        try:
            main()
            break
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"[supervisor] crashed: {e.__class__.__name__}: {e} — restarting in 15s")
            time.sleep(15)
