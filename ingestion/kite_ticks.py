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
from datetime import datetime, timezone
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


def build_token_maps(kite, conn):
    """Return token->(symbol, is_index) and the prev_close per symbol."""
    with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM stocks WHERE is_active AND is_fno")
        fno = {r[0] for r in cur.fetchall()}

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
    return token_map, prev_close


def main():
    conn = psycopg2.connect(DB_URL)
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)

    token_map, prev_close = build_token_maps(kite, conn)
    tokens = list(token_map.keys())
    print(f"Subscribing to {len(tokens)} instruments "
          f"({sum(1 for v in token_map.values() if v[1])} indices)")

    latest = {}            # token -> (ltp, vol, ts)
    lock = threading.Lock()

    kws = KiteTicker(API_KEY, ACCESS_TOKEN)

    def on_ticks(ws, ticks):
        with lock:
            for t in ticks:
                latest[t["instrument_token"]] = (
                    t.get("last_price"), t.get("volume_traded"),
                    t.get("exchange_timestamp") or datetime.now(timezone.utc),
                )

    def on_connect(ws, response):
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.connect(threaded=True)

    print("Ticker running. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(FLUSH_SECS)
            with lock:
                snap = dict(latest)
            rows = []
            now = datetime.now(timezone.utc)
            for tok, (ltp, vol, ts) in snap.items():
                if ltp is None:
                    continue
                sym, is_idx = token_map[tok]
                pc = prev_close.get(sym)
                chg = round((ltp / pc - 1) * 100, 2) if pc else None
                rows.append((sym, is_idx, round(float(ltp), 2),
                             round(float(pc), 2) if pc else None, chg,
                             int(vol) if vol else None, ts, now))
            if rows:
                with conn.cursor() as cur:
                    execute_values(cur, UPSERT, rows, page_size=400)
                conn.commit()
    except KeyboardInterrupt:
        print("Stopping.")
    finally:
        try:
            kws.close()
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    main()
