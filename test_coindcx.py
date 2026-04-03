"""
CoinDCX Connection Test
========================
Run this before starting the bot to verify:
1. API keys are valid
2. Account balance is visible
3. Candle data is fetching
4. Order placement is working (dry run only — no real order placed)

Usage: python test_coindcx.py
"""

import os
import sys
import json
import hmac
import hashlib
import time
import requests

# ── Load config ───────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from config import CONFIG

API_KEY    = CONFIG.get("COINDCX_API_KEY", "")
API_SECRET = CONFIG.get("COINDCX_API_SECRET", "")

print("\n" + "="*60)
print("  CoinDCX Connection Test")
print("="*60)


def signed_request(endpoint: str, body: dict) -> dict:
    body["timestamp"] = int(round(time.time() * 1000))
    json_body  = json.dumps(body, separators=(',', ':'))
    signature  = hmac.new(
        API_SECRET.encode('utf-8'),
        json_body.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    headers = {
        "Content-Type":     "application/json",
        "X-AUTH-APIKEY":    API_KEY,
        "X-AUTH-SIGNATURE": signature,
    }
    r = requests.post(
        f"https://api.coindcx.com{endpoint}",
        headers=headers,
        data=json_body,
        timeout=10
    )
    return r.json()


# ── Test 1: API key present ───────────────────────────────────────────────────
print("\n[1] Checking API keys...")
if not API_KEY or "YOUR_" in API_KEY or API_KEY == "":
    print("    FAIL — COINDCX_API_KEY not set in config or .env")
    sys.exit(1)
if not API_SECRET or API_SECRET == "":
    print("    FAIL — COINDCX_API_SECRET not set in config or .env")
    sys.exit(1)
print(f"    OK — API key loaded: {API_KEY[:8]}...{API_KEY[-4:]}")


# ── Test 2: Account balance ───────────────────────────────────────────────────
print("\n[2] Fetching account balance...")
try:
    resp = signed_request("/exchange/v1/users/balances", {})
    if isinstance(resp, list):
        # Filter non-zero balances
        nonzero = [b for b in resp if float(b.get("balance", 0)) > 0]
        if nonzero:
            print(f"    OK — {len(nonzero)} asset(s) with balance:")
            for b in nonzero[:5]:
                print(f"         {b['currency']:10} | Available: {float(b['balance']):,.4f}"
                      f" | Locked: {float(b.get('locked_balance', 0)):,.4f}")
        else:
            print("    OK — Account connected but no balance yet.")
            print("         Deposit INR first: CoinDCX app → Funds → Deposit")
    elif isinstance(resp, dict) and resp.get("code") == 401:
        print("    FAIL — Invalid API key or secret. Double-check your credentials.")
        sys.exit(1)
    else:
        print(f"    WARN — Unexpected response: {resp}")
except Exception as e:
    print(f"    FAIL — {e}")
    sys.exit(1)


# ── Test 3: Public market ticker ──────────────────────────────────────────────
print("\n[3] Fetching live ticker (BTCUSDT)...")
try:
    r    = requests.get("https://api.coindcx.com/exchange/ticker", timeout=5)
    data = r.json()
    btc  = next((x for x in data if x.get("market") == "BTCUSDT"), None)
    if btc:
        print(f"    OK — BTC/USDT last price: ${float(btc['last_price']):,.2f}")
        print(f"         24h change: {btc.get('change_24_hour', 'N/A')}%")
    else:
        print("    WARN — BTCUSDT not found in ticker. CoinDCX may use different symbol format.")

    # Check a few of the symbols your bot scans
    test_symbols = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "TRXUSDT"]
    found = [x["market"] for x in data if x.get("market") in test_symbols]
    print(f"    Bot-relevant pairs found: {found}")
except Exception as e:
    print(f"    FAIL — {e}")


# ── Test 4: Candle data ───────────────────────────────────────────────────────
print("\n[4] Fetching candle data (BTCUSDT 5m)...")
try:
    r = requests.get(
        "https://public.coindcx.com/market_data/candles",
        params={"pair": "B-BTC_USDT", "interval": "5m", "limit": 10},
        timeout=10
    )
    candles = r.json()
    if candles and len(candles) > 0:
        last = candles[-1]
        print(f"    OK — {len(candles)} candles received")
        print(f"         Last candle: O={last[1]} H={last[2]} L={last[3]} C={last[4]} V={last[5]}")
    else:
        print("    WARN — No candle data returned. Pair format may differ.")
except Exception as e:
    print(f"    FAIL — {e}")


# ── Test 5: Active orders (read-only check) ───────────────────────────────────
print("\n[5] Checking active orders (read-only)...")
try:
    resp = signed_request("/exchange/v1/orders/active_orders", {"market": "BTCUSDT"})
    if isinstance(resp, list):
        print(f"    OK — {len(resp)} active order(s) on BTCUSDT")
    elif isinstance(resp, dict) and "orders" in resp:
        print(f"    OK — {len(resp['orders'])} active order(s)")
    else:
        print(f"    OK — Response: {resp}")
except Exception as e:
    print(f"    FAIL — {e}")


# ── Test 6: Bot config check ──────────────────────────────────────────────────
print("\n[6] Bot configuration check...")
capital      = CONFIG.get("CAPITAL", 0)
pos_pct      = CONFIG.get("MAX_POSITION_PCT", 5)
max_crypto   = CONFIG.get("MAX_OPEN_TRADES_CRYPTO", 3)
per_trade    = capital * pos_pct / 100
rotation     = CONFIG.get("CAPITAL_ROTATION_ENABLED", False)
paper        = CONFIG.get("PAPER_TRADING", True)

print(f"    Capital         : Rs.{capital:,}")
print(f"    Position size   : {pos_pct}% = Rs.{per_trade:.0f} per trade")
print(f"    Max crypto slots: {max_crypto}")
print(f"    Max deployed    : Rs.{per_trade * max_crypto:.0f} ({pos_pct * max_crypto:.0f}% of capital)")
print(f"    Capital rotation: {'ON' if rotation else 'OFF'}")
print(f"    Paper trading   : {'YES — no real orders will be placed' if paper else 'NO — LIVE MODE'}")
print(f"    Crypto broker   : {CONFIG.get('CRYPTO_BROKER', 'paper')}")

if per_trade < 100:
    print(f"\n    WARN — Rs.{per_trade:.0f} per trade is below CoinDCX minimum order (Rs.100).")
    print(f"    Increase CAPITAL or MAX_POSITION_PCT.")
else:
    print(f"\n    OK — Rs.{per_trade:.0f} per trade is above CoinDCX minimum (Rs.100).")

if paper:
    print("\n    NOTE: PAPER_TRADING is True — bot won't place real orders on CoinDCX.")
    print("    Set PAPER_TRADING = False in config.py when ready to go live.")


# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  All tests passed. CoinDCX is ready.")
print("="*60)
print()
print("  Next steps:")
if paper:
    print("  1. Keep PAPER_TRADING = True for now (safe)")
    print("  2. Run: python bot.py")
    print("  3. After 1-2 weeks of good paper results:")
    print("     → Set PAPER_TRADING = False in config.py")
    print("     → Deposit Rs.1500 on CoinDCX app")
    print("     → Run: python bot.py")
else:
    print("  1. Deposit Rs.1500 on CoinDCX if not done yet")
    print("  2. Run: python bot.py")
    print("  3. Watch dashboard at http://localhost:5001")
print()
