"""
AlgoBot Live Dashboard
======================
Run this in a SEPARATE terminal while bot.py is running.
Shows all open positions with live PnL, SL/TP status.

Usage: python dashboard.py
"""

import json
import os
import time
import warnings
warnings.filterwarnings("ignore")

try:
    import yfinance as yf
    _cache_dir = os.path.join(os.path.expanduser("~"), "yfinance_cache")
    os.makedirs(_cache_dir, exist_ok=True)
    yf.set_tz_cache_location(_cache_dir)
except Exception:
    pass

POSITIONS_FILE = "logs/open_positions.json"


def get_live_price(symbol, market):
    try:
        from curl_cffi import requests as curl_requests
        session = curl_requests.Session(impersonate="chrome")
        import yfinance as yf

        if market == "stocks":
            yf_sym = f"{symbol}.NS"
        else:
            yf_sym = symbol.replace("USDT", "") + "-USD"

        ticker = yf.Ticker(yf_sym, session=session)
        ltp = ticker.fast_info.get("lastPrice")
        if ltp and ltp > 0:
            return float(ltp)
        hist = ticker.history(period="1d", interval="5m")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
        return None
    except Exception:
        return None


def show_dashboard():
    if not os.path.exists(POSITIONS_FILE):
        print("No open_positions.json found. Is bot.py running?")
        return

    with open(POSITIONS_FILE) as f:
        data = json.load(f)

    all_positions = []
    for market, positions in data.items():
        for p in positions:
            p["market"] = market
            all_positions.append(p)

    if not all_positions:
        print("No open positions right now.")
        return

    print("\n" + "=" * 80)
    print(f"  ALGOBOT LIVE DASHBOARD  |  {time.strftime('%Y-%m-%d %H:%M:%S')}  |  {len(all_positions)} open positions")
    print("=" * 80)
    print(f"  {'SYMBOL':<14} {'MKT':<7} {'SIDE':<5} {'ENTRY':>10} {'CURRENT':>10} {'PnL':>10} {'PnL%':>7} {'SL':>10} {'TP':>10} {'STATUS'}")
    print("-" * 80)

    total_pnl    = 0.0
    winning      = 0
    losing       = 0
    fetch_errors = 0

    for pos in all_positions:
        symbol = pos["symbol"]
        market = pos["market"]
        action = pos["action"]
        entry  = pos["entry_price"]
        sl     = pos["stop_loss"]
        tp     = pos["take_profit"]
        qty    = pos["qty"]
        strategy = pos.get("strategy", "-")

        ltp = get_live_price(symbol, market)

        if ltp is None:
            fetch_errors += 1
            print(f"  {symbol:<14} {market:<7} {action:<5} {entry:>10.2f} {'N/A':>10} {'---':>10} {'---':>7} {sl:>10.2f} {tp:>10.2f}  [price fetch failed]")
            continue

        # Calculate PnL
        if action == "BUY":
            pnl     = (ltp - entry) * qty
            pnl_pct = (ltp - entry) / entry * 100
            sl_dist = (ltp - sl) / entry * 100
            tp_dist = (tp - ltp) / entry * 100
        else:
            pnl     = (entry - ltp) * qty
            pnl_pct = (entry - ltp) / entry * 100
            sl_dist = (sl - ltp) / entry * 100
            tp_dist = (ltp - tp) / entry * 100

        total_pnl += pnl

        # Status
        sl_pct_away = abs((ltp - sl) / entry * 100)
        tp_pct_away = abs((tp - ltp) / entry * 100)

        if pnl > 0:
            winning += 1
            if tp_pct_away < 0.5:
                status = ">>> NEAR TP!"
            else:
                status = f"(+) {tp_pct_away:.1f}% to TP"
        else:
            losing += 1
            if sl_pct_away < 0.5:
                status = "!!! NEAR SL!"
            else:
                status = f"(-) {sl_pct_away:.1f}% to SL"

        pnl_str = f"{pnl:+.2f}"
        pnl_pct_str = f"{pnl_pct:+.1f}%"

        print(f"  {symbol:<14} {market:<7} {action:<5} {entry:>10.2f} {ltp:>10.2f} "
              f"{pnl_str:>10} {pnl_pct_str:>7} {sl:>10.2f} {tp:>10.2f}  {status}")

    print("-" * 80)
    print(f"  {'TOTAL':.<14} {'':7} {'':5} {'':10} {'':10} {total_pnl:>+10.2f}  "
          f"|  Winning: {winning}  Losing: {losing}  "
          f"{'Fetch errors: ' + str(fetch_errors) if fetch_errors else ''}")
    print("=" * 80)

    # Summary box
    print(f"\n  Capital     : Rs.{100000:,}")
    print(f"  Open trades : {len(all_positions)}")
    print(f"  Unrealised  : Rs.{total_pnl:+,.2f}  ({total_pnl/100000*100:+.2f}% of capital)")
    print(f"  Winning     : {winning} / {len(all_positions) - fetch_errors}")
    if len(all_positions) - fetch_errors > 0:
        win_rate = winning / (len(all_positions) - fetch_errors) * 100
        print(f"  Win rate    : {win_rate:.0f}%")
    print()


if __name__ == "__main__":
    import sys

    # Run once or loop
    loop = "--watch" in sys.argv

    if loop:
        print("Watching live positions (refreshes every 30s). Press Ctrl+C to stop.")
        while True:
            os.system("cls" if os.name == "nt" else "clear")
            show_dashboard()
            time.sleep(30)
    else:
        show_dashboard()
        print("  Tip: Run 'python dashboard.py --watch' to auto-refresh every 30 seconds.\n")
