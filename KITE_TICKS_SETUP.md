# Live Ticks — Zerodha KiteTicker Streamer (setup & run)

Streams **real-time** Kite WebSocket ticks for F&O stocks + indices into the
Supabase `ticks` table. The dashboard's **⚡ Ticks** tab polls that table every ~3s,
so you get a truly live board with **no per-fetch permission popups** — the popup
issue only exists in the Cowork sandbox; this streamer runs locally on your machine.

Files involved (already in the repo):
- `ingestion/kite_login.py` — get the daily Kite access token
- `ingestion/kite_ticks.py` — the continuous WebSocket streamer
- `sql/schema.sql` — defines the `ticks` table (already applied in your DB)

---

## One-time setup

### 1. Create a Kite Connect app
- Go to <https://developers.kite.trade/apps> and create an app (Kite Connect costs ₹2,000/month for the API; a personal app is fine).
- Set the **Redirect URL** to anything you control, e.g. `https://127.0.0.1/` (you only need to copy the `request_token` from the redirected URL — the page itself doesn't have to load).
- Note the **API key** and **API secret**.

### 2. Fill in `.env`
Copy `.env.example` to `.env` and set:
```
DATABASE_URL=postgresql://postgres.<project-ref>:<PASSWORD>@aws-1-<region>.pooler.supabase.com:6543/postgres
KITE_API_KEY=your_kite_api_key
KITE_API_SECRET=your_kite_api_secret
KITE_ACCESS_TOKEN=            # filled by kite_login.py (step 4)
```

### 3. Install dependencies
```
pip install -r requirements.txt          # includes kiteconnect
# (or minimally: pip install kiteconnect psycopg2-binary python-dotenv)
```

---

## Daily run (Kite tokens expire every morning ~6 AM IST)

### 4. Get today's access token
```
python ingestion/kite_login.py
```
- Open the printed URL, log in to Zerodha (+ 2FA).
- After it redirects, copy the `request_token=...` value from the browser URL and paste it into the prompt.
- It prints the `access_token` and writes `KITE_ACCESS_TOKEN` into `.env`.

### 5. Start the streamer (keep it running during market hours)
```
python ingestion/kite_ticks.py
```
You'll see `Subscribing to N instruments (…​ indices)` then `Ticker running.`
Leave it running 9:15 AM–3:30 PM IST. `Ctrl+C` to stop.

### 6. Watch it live
Open `dashboard/index.html` → **⚡ Ticks** tab. It refreshes every 3s from the `ticks` table.

---

## Keeping it always-on (optional)

**Windows (Task Scheduler / a terminal):** just leave the `python ingestion/kite_ticks.py` window open, or create a Task Scheduler task that runs it at 9:10 AM on weekdays. (Run `kite_login.py` first each day — that step needs your manual login.)

**Linux / small cloud VM (systemd):**
```ini
# /etc/systemd/system/kite-ticks.service
[Unit]
Description=Kite tick streamer
After=network-online.target
[Service]
WorkingDirectory=/path/to/nifty500-scanner
ExecStart=/usr/bin/python3 ingestion/kite_ticks.py
Restart=on-failure
[Install]
WantedBy=multi-user.target
```
```
sudo systemctl enable --now kite-ticks
```
Or quick-and-dirty: `nohup python ingestion/kite_ticks.py > ticks.log 2>&1 &`.

> The token still has to be refreshed daily (step 4). You can't fully automate the login — Zerodha requires an interactive 2FA each morning.

---

## How it works (so you can tweak it)
- On start it loads your **F&O universe** (`stocks` where `is_active AND is_fno`) + a fixed **index list** (`INDEX_MAP` in `kite_ticks.py`), maps each to its Kite `instrument_token`, and fetches each symbol's **previous close** once (for `chg_pct`).
- It subscribes in `MODE_FULL`, buffers the latest tick per token, and every ~1.5s **upserts** one row per symbol into `ticks` (`ON CONFLICT (symbol) DO UPDATE`). So the table always holds the *current* snapshot, one row per name — light for the dashboard to poll.
- `chg_pct = (ltp / prev_close - 1) × 100`. Indices carry `is_index = true`; the dashboard splits them into the index strip via `IDX_NAMES`.
- Reconnect/error callbacks log drops; KiteTicker auto-reconnects.

**To change what's tracked:** edit `INDEX_MAP` (indices) or the `is_fno` flags in `stocks` (equities). No dashboard change needed — the Ticks tab renders whatever is in `ticks`.

---

## Security note (important)
Your `.env.example` previously contained a **real Supabase database password** — it has been replaced with a placeholder here, but that credential was exposed (also flagged in `CLAUDE.md`). **Rotate it now:** Supabase Dashboard → Settings → Database → Reset database password, then update `.env` and the `DATABASE_URL` GitHub Actions secret. Never commit `.env`.
