"""
kite_trader.py  —  Automated intraday execution for the Trend Day scanner.

    python trading/kite_trader.py                 # DRY RUN (default, no orders)
    python trading/kite_trader.py --live          # refuses unless armed, see below

=============================================================================
BACKTEST VERDICT — READ BEFORE ARMING
=============================================================================
The default entry pool (stage == CONFIRMED at 11:15) LOST MONEY on the only
sample available: 78 trades over 30 sessions, net -Rs 9,713, expectancy
-Rs 125/trade after Zerodha intraday charges.

Cause is structural, not a tuning problem: `CONFIRMED` means the stock is
ALREADY up >= 6%.  Median entry sits +7.42% up on the day, i.e. after the move
the scanner exists to find.  64 of 78 trades reached neither target and were
squared off at the close.

  pool              trades   net P&L    expectancy   win rate
  CONFIRMED             78   -9,713      -125/trade    48.7%
  SETUP                 39   +7,036      +180/trade    59.0%
  CONFIRMED+SETUP       87  -10,785      -124/trade    47.1%

ENTRY_STAGES = ("SETUP",) was the only non-losing variant, and 39 trades over
22 sessions is nowhere near significant.  Both figures come from 33 sessions of
one market regime, on hourly bars that cannot resolve intrabar sequencing.
Treat every number here as indicative, not as evidence of an edge.

=============================================================================
ARMING (deliberately two independent switches, both set by you)
=============================================================================
    1.  pass --live on the command line
    2.  export KITE_TRADING_ARMED=yes   in the same shell

Missing either one => dry run.  Create the file trading/KILL to force flat and
block all new entries; it is checked every cycle.

=============================================================================
RISK RULES
=============================================================================
  * Day loss cap Rs 10,000 realised — no NEW positions past it (open positions
    keep their stops; the cap is not a liquidation trigger).
  * Per-position risk Rs 3,000, sized as qty = 3000 / (entry - stop).
  * Max 3 concurrent positions.
  * Stop = low of day AS OF ENTRY, frozen. It never trails down — a moving
    stop would make the Rs 3,000 cap meaningless.
  * MIN_STOP_PCT floors the stop distance at 1.5%. Not optional: the intraday
    low can sit 0.1% under the entry, which makes 3000/(entry-stop) explode
    (a real case demanded 29,970 shares = Rs 6.6 crore of stock).
  * MAX_NOTIONAL caps per-position exposure independently of the risk maths.
  * Targets: 50% off at +1%, remainder at +3%.
  * Everything squares off at SQUAREOFF_IST; MIS would be auto-closed by the
    broker anyway, but on our own terms and at a known time.

State lives in trading/state_<date>.json so a restart mid-session resumes
instead of re-entering positions it already holds.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")

IST = timezone(timedelta(hours=5, minutes=30))
STATE_DIR = Path(__file__).parent
KILL_FILE = STATE_DIR / "KILL"

# ----------------------------------------------------------------- config --
DAY_LOSS_CAP   = -10_000.0    # stop opening new positions past this (realised)
RISK_PER_TRADE = 3_000.0
MAX_POSITIONS  = 3
MIN_STOP_PCT   = 0.015        # floor on stop distance — see docstring
MAX_NOTIONAL   = 200_000.0    # per-position exposure cap
T1_PCT, T2_PCT = 0.01, 0.03
T1_FRACTION    = 0.50

ENTRY_STAGES   = ("CONFIRMED",)   # as specified. ("SETUP",) was the only
                                  # non-losing variant in the backtest above.
MIN_SCORE      = 20.0
MAX_CUM_PCT    = 10.0   # don't chase past the +6-10% band this targets.
                        # `CONFIRMED` has no upper bound on its own — without
                        # this the book buys names already +15% on the day.
ENTRY_WINDOW   = (dtime(11, 15), dtime(11, 45))   # act on the 11:15 scan
SQUAREOFF_IST  = dtime(15, 10)
POLL_SECONDS   = 30

EXCHANGE = "NSE"
PRODUCT  = "MIS"

log = logging.getLogger("kite_trader")


# ------------------------------------------------------------------ state --
@dataclass
class Position:
    symbol: str
    qty: int
    entry: float
    stop: float
    t1: float
    t2: float
    entry_order_id: str | None = None
    sl_order_id: str | None = None
    t1_done: bool = False
    closed: bool = False
    realised: float = 0.0
    opened_at: str = ""
    notes: list = field(default_factory=list)


class Book:
    """Positions + realised P&L, persisted per session."""

    def __init__(self, day: date):
        self.day = day
        self.path = STATE_DIR / f"state_{day:%Y%m%d}.json"
        self.positions: dict[str, Position] = {}
        self.realised = 0.0
        self.load()

    def load(self):
        if not self.path.exists():
            return
        raw = json.loads(self.path.read_text())
        self.realised = raw.get("realised", 0.0)
        self.positions = {k: Position(**v) for k, v in raw.get("positions", {}).items()}
        log.info("resumed state: %d positions, realised Rs %.0f",
                 len(self.open_positions()), self.realised)

    def save(self):
        self.path.write_text(json.dumps(
            {"realised": self.realised,
             "positions": {k: asdict(v) for k, v in self.positions.items()}},
            indent=2, default=str))

    def open_positions(self):
        return [p for p in self.positions.values() if not p.closed]

    def can_open(self):
        if self.realised <= DAY_LOSS_CAP:
            return False, f"day loss cap hit (realised Rs {self.realised:,.0f})"
        if len(self.open_positions()) >= MAX_POSITIONS:
            return False, f"already holding {MAX_POSITIONS} positions"
        return True, ""


# ---------------------------------------------------------------- broker ---
class Broker:
    """Thin Kite wrapper. In dry-run nothing leaves the process."""

    def __init__(self, live: bool):
        self.live = live
        self.kite = None
        if live:
            from kiteconnect import KiteConnect
            self.kite = KiteConnect(api_key=os.environ["KITE_API_KEY"])
            self.kite.set_access_token(os.environ["KITE_ACCESS_TOKEN"])
            log.info("broker: LIVE as %s", self.kite.profile()["user_id"])
        else:
            log.info("broker: DRY RUN — no orders will be sent")

    def ltp(self, symbols):
        if not self.live:
            return {}
        keys = [f"{EXCHANGE}:{s}" for s in symbols]
        return {k.split(":")[1]: v["last_price"]
                for k, v in self.kite.ltp(keys).items()}

    def _order(self, **kw):
        if not self.live:
            log.info("[DRYRUN] order %s", kw)
            return f"DRY-{kw['tradingsymbol']}-{kw['transaction_type']}-{int(time.time())}"
        oid = self.kite.place_order(variety=self.kite.VARIETY_REGULAR, **kw)
        log.info("order placed %s: %s", oid, kw)
        return oid

    def buy_market(self, symbol, qty):
        return self._order(exchange=EXCHANGE, tradingsymbol=symbol,
                           transaction_type="BUY", quantity=qty,
                           product=PRODUCT, order_type="MARKET")

    def sell_market(self, symbol, qty):
        return self._order(exchange=EXCHANGE, tradingsymbol=symbol,
                           transaction_type="SELL", quantity=qty,
                           product=PRODUCT, order_type="MARKET")

    def sell_slm(self, symbol, qty, trigger):
        return self._order(exchange=EXCHANGE, tradingsymbol=symbol,
                           transaction_type="SELL", quantity=qty,
                           product=PRODUCT, order_type="SL-M",
                           trigger_price=round(trigger, 1))

    def cancel(self, order_id):
        if not self.live:
            log.info("[DRYRUN] cancel %s", order_id)
            return
        try:
            self.kite.cancel_order(variety=self.kite.VARIETY_REGULAR, order_id=order_id)
        except Exception as e:
            log.warning("cancel %s failed: %s", order_id, e)


# -------------------------------------------------------------- selection --
def fetch_candidates(conn):
    """Scanner rows eligible for entry, best relative volume first."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """SELECT symbol, stage, score, h1_rvol, last_close, day_low,
                      cum_pct, slot_no, trade_date
               FROM scanner_trendday
               WHERE stage = ANY(%s) AND score >= %s AND slot_no >= 2
               ORDER BY h1_rvol DESC NULLS LAST""",
            (list(ENTRY_STAGES), MIN_SCORE))
        return cur.fetchall()


def size_position(entry: float, day_low: float):
    """qty, stop. Returns (0, stop) when the trade cannot be sized safely."""
    stop = float(day_low)
    if entry <= 0 or stop <= 0 or stop >= entry:
        return 0, stop        # impossible data — refuse rather than guess a stop
    floor = entry * MIN_STOP_PCT
    if entry - stop < floor:
        stop = entry - floor          # LOD too close to size off
    risk_ps = entry - stop
    if risk_ps <= 0:
        return 0, stop
    qty = int(RISK_PER_TRADE // risk_ps)
    qty = min(qty, int(MAX_NOTIONAL // entry))
    return max(qty, 0), stop


# ----------------------------------------------------------------- engine --
def try_entries(conn, broker: Broker, book: Book):
    ok, why = book.can_open()
    if not ok:
        log.info("no new entries: %s", why)
        return
    for row in fetch_candidates(conn):
        ok, why = book.can_open()
        if not ok:
            log.info("stopping entries: %s", why)
            return
        sym = row["symbol"]
        if sym in book.positions:
            continue
        if row["cum_pct"] is not None and float(row["cum_pct"]) > MAX_CUM_PCT:
            log.info("skip %s: already +%.2f%% — past the +6-%.0f%% band",
                     sym, row["cum_pct"], MAX_CUM_PCT)
            continue
        entry = float(row["last_close"])
        qty, stop = size_position(entry, float(row["day_low"]))
        if qty < 1:
            log.info("skip %s: cannot size (entry %.2f, low %.2f)", sym, entry, row["day_low"])
            continue
        risk = qty * (entry - stop)
        log.info("ENTER %s qty=%d entry~%.2f stop=%.2f risk=Rs %.0f notional=Rs %.0f "
                 "(rvol %.1fx, score %.0f%%, already %+.2f%%)",
                 sym, qty, entry, stop, risk, qty * entry,
                 row["h1_rvol"] or 0, row["score"] or 0, row["cum_pct"] or 0)
        eid = broker.buy_market(sym, qty)
        sid = broker.sell_slm(sym, qty, stop)
        book.positions[sym] = Position(
            symbol=sym, qty=qty, entry=entry, stop=stop,
            t1=entry * (1 + T1_PCT), t2=entry * (1 + T2_PCT),
            entry_order_id=eid, sl_order_id=sid,
            opened_at=datetime.now(IST).isoformat())
        book.save()


def manage(broker: Broker, book: Book):
    live = book.open_positions()
    if not live:
        return
    prices = broker.ltp([p.symbol for p in live])
    for p in live:
        px = prices.get(p.symbol)
        if px is None:
            continue
        if not p.t1_done and px >= p.t1:
            half = max(1, int(p.qty * T1_FRACTION))
            broker.sell_market(p.symbol, half)
            p.realised += half * (px - p.entry)
            p.qty -= half
            p.t1_done = True
            # resize the protective stop to what is still open
            if p.sl_order_id:
                broker.cancel(p.sl_order_id)
            p.sl_order_id = broker.sell_slm(p.symbol, p.qty, p.stop) if p.qty else None
            p.notes.append(f"T1 {half}@{px:.2f}")
            log.info("T1 %s: sold %d @ %.2f, %d left", p.symbol, half, px, p.qty)
            book.save()
        if p.t1_done and p.qty > 0 and px >= p.t2:
            broker.sell_market(p.symbol, p.qty)
            p.realised += p.qty * (px - p.entry)
            if p.sl_order_id:
                broker.cancel(p.sl_order_id)
            p.notes.append(f"T2 {p.qty}@{px:.2f}")
            log.info("T2 %s: sold %d @ %.2f — closed", p.symbol, p.qty, px)
            p.qty, p.closed = 0, True
            book.realised += p.realised
            book.save()
        if not p.closed and px <= p.stop:
            # the SL-M should already have fired; reconcile our view of the book
            p.realised += p.qty * (p.stop - p.entry)
            p.notes.append(f"STOP {p.qty}@{p.stop:.2f}")
            log.warning("STOP %s at %.2f (%d qty)", p.symbol, p.stop, p.qty)
            p.qty, p.closed = 0, True
            book.realised += p.realised
            book.save()


def squareoff(broker: Broker, book: Book, reason="EOD"):
    for p in book.open_positions():
        if p.qty > 0:
            broker.sell_market(p.symbol, p.qty)
            if p.sl_order_id:
                broker.cancel(p.sl_order_id)
            p.notes.append(f"{reason} {p.qty}")
            log.info("%s square-off %s qty %d", reason, p.symbol, p.qty)
        p.closed = True
        book.realised += p.realised
    book.save()


def main():
    ap = argparse.ArgumentParser(description="Trend Day automated execution")
    ap.add_argument("--live", action="store_true",
                    help="send real orders (also needs KITE_TRADING_ARMED=yes)")
    ap.add_argument("--once", action="store_true", help="single cycle then exit")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout),
                                  logging.FileHandler(STATE_DIR / "kite_trader.log")])

    armed = os.environ.get("KITE_TRADING_ARMED", "").lower() == "yes"
    live = args.live and armed
    if args.live and not armed:
        log.error("--live passed but KITE_TRADING_ARMED is not 'yes' — staying in DRY RUN")
    if live:
        log.warning("=" * 62)
        log.warning("LIVE TRADING ARMED. Backtest expectancy for ENTRY_STAGES=%s "
                    "was NEGATIVE (-Rs 125/trade). See module docstring.", ENTRY_STAGES)
        log.warning("=" * 62)

    broker = Broker(live)
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    book = Book(datetime.now(IST).date())

    try:
        while True:
            now = datetime.now(IST).time()
            if KILL_FILE.exists():
                log.error("KILL file present — squaring off and exiting")
                squareoff(broker, book, "KILL")
                break
            if now >= SQUAREOFF_IST:
                squareoff(broker, book)
                log.info("session done. realised Rs %.0f", book.realised)
                break
            manage(broker, book)
            if ENTRY_WINDOW[0] <= now <= ENTRY_WINDOW[1]:
                try_entries(conn, broker, book)
            if args.once:
                break
            time.sleep(POLL_SECONDS)
    finally:
        conn.close()
        log.info("realised P&L Rs %.0f across %d positions",
                 book.realised, len(book.positions))


if __name__ == "__main__":
    main()
