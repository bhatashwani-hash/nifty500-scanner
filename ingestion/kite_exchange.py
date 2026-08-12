"""
kite_exchange.py — non-interactive version of kite_login.py.

Usage:  python ingestion/kite_exchange.py <request_token>

Exchanges the request_token (from the redirect URL after the daily Zerodha
login) for an access_token and writes KITE_ACCESS_TOKEN into .env.
"""

import re
import sys
from pathlib import Path

from dotenv import dotenv_values
from kiteconnect import KiteConnect

ENV = Path(__file__).parent.parent / ".env"


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: python ingestion/kite_exchange.py <request_token>")
    req = sys.argv[1].strip()
    cfg = dotenv_values(ENV)
    kite = KiteConnect(api_key=cfg["KITE_API_KEY"])
    data = kite.generate_session(req, api_secret=cfg["KITE_API_SECRET"])
    token = data["access_token"]

    text = ENV.read_text()
    if "KITE_ACCESS_TOKEN=" in text:
        text = re.sub(r"KITE_ACCESS_TOKEN=.*", f"KITE_ACCESS_TOKEN={token}", text)
    else:
        text += f"\nKITE_ACCESS_TOKEN={token}\n"
    ENV.write_text(text)
    print("OK — KITE_ACCESS_TOKEN updated in .env (user:", data.get("user_id"), ")")


if __name__ == "__main__":
    main()
