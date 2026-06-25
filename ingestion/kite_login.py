"""
kite_login.py — get a daily Kite access token.

Kite access tokens expire every morning (~6 AM IST), so run this once each day
before starting kite_ticks.py.

  1. Set KITE_API_KEY and KITE_API_SECRET in .env (from your Kite Connect app).
  2. Run:  python ingestion/kite_login.py
  3. Open the printed URL, log in to Zerodha. After redirect, copy the
     `request_token=...` value from the URL and paste it here.
  4. It prints the access_token and (if writable) updates KITE_ACCESS_TOKEN in .env.
"""

import os
import re
from pathlib import Path

from dotenv import load_dotenv
from kiteconnect import KiteConnect

ENV = Path(__file__).parent.parent / ".env"
load_dotenv(ENV)
API_KEY = os.environ["KITE_API_KEY"]
API_SECRET = os.environ["KITE_API_SECRET"]


def main():
    kite = KiteConnect(api_key=API_KEY)
    print("\n1) Open this URL and log in:\n   " + kite.login_url() + "\n")
    req = input("2) Paste the request_token from the redirect URL: ").strip()
    data = kite.generate_session(req, api_secret=API_SECRET)
    token = data["access_token"]
    print("\naccess_token:", token)

    # best-effort: update KITE_ACCESS_TOKEN in .env
    try:
        text = ENV.read_text() if ENV.exists() else ""
        if "KITE_ACCESS_TOKEN=" in text:
            text = re.sub(r"KITE_ACCESS_TOKEN=.*", f"KITE_ACCESS_TOKEN={token}", text)
        else:
            text += f"\nKITE_ACCESS_TOKEN={token}\n"
        ENV.write_text(text)
        print("Updated KITE_ACCESS_TOKEN in .env")
    except Exception as e:
        print("Could not write .env (set KITE_ACCESS_TOKEN manually):", e)


if __name__ == "__main__":
    main()
