"""
Pre-Live Price Validation
==========================
Run this BEFORE switching PAPER_TRADING = False.
Compares CoinDCX prices vs yfinance to confirm they match.

Usage: python validate_prices.py
"""

import sys
import time
import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from config import CONFIG

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "XRPUSDT", "BNBUSDT",
    "SOLUSDT", "TRXUSDT", "XLMUSDT", "HBARUSDT",
    "DOGEUSDT", "ADAUSDT",
]

print("\n" + "="*65)
print("  Price Validation: CoinDCX vs yfinance")
print("="*65)


def get_coindcx_price(symbol):
    """Try multiple CoinDCX endpoints to get price."""
    base = symbol.replace("USDT", "")

    # Method 1: candles endpoint
    try:
        r = requests.get(
            "https://public.coindcx.com/market_data/candles",
            params={"pair": f"B-{base}_USDT", "interval": "1m", "limit": 3},
            timeout=8)
        data = r.json()
        if data and isinstance(data, list) and len(data) > 0:
            last = data[-1]
            # columns: [time, open, high, low, close, volume]
            close = float(last[4]) if len(last) > 4 else None
            if close and close > 0:
                return close, "candle"
    except Exception as e:
        pass

    # Method 2: ticker endpoint
    try:
        r = requests.get("https://api.coindcx.com/exchange/ticker", timeout=8)
        data = r.json()
        for item in data:
            if item.get("market") == symbol:
                price = float(item["last_price"])
                if price > 0:
                    return price, "ticker"
    except Exception as e:
        pass

    # Method 3: CoinDCX market data v2
    try:
        r = requests.get(
            f"https://api.coindcx.com/exchange/v1/markets_details",
            timeout=8)
        data = r.json()
        for item in data:
            if item.get("symbol") == symbol or item.get("pair") == symbol:
                price = float(item.get("last_traded_price", 0))
                if price > 0:
                    return price, "markets"
    except Exception as e:
        pass

    return None, "failed"


def get_yfinance_price(symbol):
    try:
        import yfinance as yf
        yf_sym = symbol.replace("USDT", "") + "-USD"
        ticker = yf.Ticker(yf_sym)
        ltp    = ticker.fast_info.get("lastPrice")
        if ltp and float(ltp) > 0:
            return float(ltp)
        hist = ticker.history(period="1d", interval="5m")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
        return None
    except Exception:
        return None


# ── Test connectivity first ────────────────────────────────────────────────
print("\n[1] Testing CoinDCX connectivity...")
try:
    r = requests.get("https://api.coindcx.com/exchange/ticker", timeout=8)
    sample = r.json()
    btc = next((x for x in sample if x.get("market") == "BTCUSDT"), None)
    if btc:
        print(f"    ✅ Ticker OK — BTCUSDT: ${float(btc['last_price']):,.2f}")
    else:
        markets = [x["market"] for x in sample[:5]]
        print(f"    ⚠️  Ticker connected but BTCUSDT not found. Sample markets: {markets}")
        # Try to find BTC
        btc_markets = [x for x in sample if "BTC" in x.get("market","")][:3]
        print(f"    BTC-related markets: {[x['market'] for x in btc_markets]}")
except Exception as e:
    print(f"    ❌ Ticker failed: {e}")
    print("    Check your internet connection.")
    sys.exit(1)

print("\n[2] Testing candle endpoint...")
try:
    r = requests.get(
        "https://public.coindcx.com/market_data/candles",
        params={"pair": "B-BTC_USDT", "interval": "1m", "limit": 2},
        timeout=8)
    data = r.json()
    if data and isinstance(data, list):
        last = data[-1]
        print(f"    ✅ Candles OK — BTC last candle: {last}")
    else:
        print(f"    ⚠️  Candles returned: {str(data)[:150]}")
except Exception as e:
    print(f"    ❌ Candles failed: {e}")

# ── Price comparison ───────────────────────────────────────────────────────
print(f"\n[3] Comparing prices ({len(SYMBOLS)} symbols)...\n")
print(f"  {'Symbol':<14} {'CoinDCX':>12} {'yfinance':>12} {'Diff%':>8}  Source  Status")
print("  " + "-"*68)

results  = []
all_ok   = True

for sym in SYMBOLS:
    cdx_price, source = get_coindcx_price(sym)
    time.sleep(0.2)
    yf_price = get_yfinance_price(sym)

    if cdx_price is None or yf_price is None:
        status = "SKIP"
        diff   = None
    else:
        diff = abs(cdx_price - yf_price) / yf_price * 100
        if diff <= 2.0:
            status = "OK"
        elif diff <= 10.0:
            status = "WARN"
        else:
            status = "FAIL"
            all_ok = False

    results.append((sym, cdx_price, yf_price, diff, status, source))
    cdx_str  = f"${cdx_price:,.4f}" if cdx_price else "N/A"
    yf_str   = f"${yf_price:,.4f}"  if yf_price  else "N/A"
    diff_str = f"{diff:.2f}%"       if diff is not None else "N/A"
    icon     = {"OK":"✅","WARN":"⚠️ ","FAIL":"❌","SKIP":"⏭️ "}[status]
    print(f"  {icon} {sym:<12} {cdx_str:>12} {yf_str:>12} {diff_str:>8}  {source}")

print("  " + "-"*68)

ok_count   = sum(1 for r in results if r[4] == "OK")
warn_count = sum(1 for r in results if r[4] == "WARN")
fail_count = sum(1 for r in results if r[4] == "FAIL")
skip_count = sum(1 for r in results if r[4] == "SKIP")

print(f"\n  Results: {ok_count} OK  |  {warn_count} WARN  |  {fail_count} FAIL  |  {skip_count} SKIP\n")

if fail_count > 0:
    print("  ❌ PRICES DO NOT MATCH — DO NOT GO LIVE YET")
elif skip_count == len(SYMBOLS):
    print("  ❌ ALL SKIPPED — CoinDCX candle API may be down or format changed")
    print("  The bot can still use ticker prices as fallback.")
    print("  Check if bot.py ran successfully with real trades today.")
elif ok_count + warn_count >= 5:
    print("  ✅ PRICES MATCH — Safe to go live")
    print()
    print("  Steps to go live:")
    print("  1. Set PAPER_TRADING = False in config.py")
    print("  2. Deposit Rs.1500 on CoinDCX app")
    print("  3. Run: python bot.py")
    print("  4. Watch http://localhost:5001")
else:
    print("  ⚠️  Partial results — run again in a few minutes")

print("\n" + "="*65 + "\n")