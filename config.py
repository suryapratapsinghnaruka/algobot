"""
AlgoBot Configuration
=====================
Edit this file before running the bot.
Start with PAPER_TRADING = True to test safely.

Security: API keys and tokens are read from environment variables.
Set them in your shell, Railway env vars, or a .env file.
Never hardcode secrets here.
"""

import os
import logging
log = logging.getLogger("Config")


def get_all_nse_symbols():
    """Fetch all ~1800 NSE-listed equity symbols dynamically."""
    try:
        import requests
        import io
        import pandas as pd
        headers = {"User-Agent": "Mozilla/5.0"}
        url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
        r = requests.get(url, headers=headers, timeout=15)
        df = pd.read_csv(io.StringIO(r.text))
        symbols = df["SYMBOL"].dropna().tolist()
        log.info(f"Loaded {len(symbols)} NSE symbols.")
        return symbols
    except Exception as e:
        log.warning(f"Could not fetch NSE symbols, using fallback list: {e}")
        return [
            "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK",
            "SBIN", "WIPRO", "AXISBANK", "LT", "BAJFINANCE",
            "HCLTECH", "ASIANPAINT", "MARUTI", "SUNPHARMA", "TITAN",
            "ULTRACEMCO", "NESTLEIND", "POWERGRID", "NTPC", "ONGC",
            "TATAMOTORS", "TATASTEEL", "JSWSTEEL", "HINDALCO", "COALINDIA",
            "ADANIPORTS", "ADANIENT", "GRASIM", "CIPLA", "DRREDDY",
            "DIVISLAB", "EICHERMOT", "HEROMOTOCO", "BAJAJ-AUTO", "INDUSINDBK",
            "BRITANNIA", "PIDILITIND", "SIEMENS", "HAVELLS", "MCDOWELL-N",
            "APOLLOHOSP", "DMART", "TRENT", "NAUKRI", "ZOMATO",
            "PAYTM", "IRCTC", "HAL", "BEL", "BHEL"
        ]


def get_all_binance_symbols():
    """Fetch all active USDT trading pairs from Binance (~300+ symbols)."""
    try:
        import requests
        r = requests.get("https://api.binance.com/api/v3/exchangeInfo", timeout=15)
        data = r.json()
        symbols = [
            s["symbol"] for s in data["symbols"]
            if s["quoteAsset"] == "USDT"
            and s["status"] == "TRADING"
            and s["isSpotTradingAllowed"]
        ]
        log.info(f"Loaded {len(symbols)} Binance USDT pairs.")
        return symbols
    except Exception as e:
        log.warning(f"Could not fetch Binance symbols, using fallback list: {e}")
        return [
            "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT",
            "XRPUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "MATICUSDT",
            "LINKUSDT", "UNIUSDT", "LTCUSDT", "ATOMUSDT", "ETCUSDT",
            "XLMUSDT", "ALGOUSDT", "VETUSDT", "FILUSDT", "TRXUSDT",
            "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "SUIUSDT"
        ]


CONFIG = {

    # ── MODE ─────────────────────────────────────────────────────────────────
    "PAPER_TRADING": False,

    # ── CAPITAL ──────────────────────────────────────────────────────────────
    # For crypto (CoinDCX), CAPITAL is treated as USD (USDT).
    # You have 15 USDT on CoinDCX — set CAPITAL to 15.
    # For NSE stocks, CAPITAL is in INR (add separate INR funds when ready).
    "CAPITAL": 15,              # USDT for crypto / INR for stocks

    # ── CANDLE SETTINGS ──────────────────────────────────────────────────────
    "CANDLE_TIMEFRAME":        "5m",
    "CANDLE_INTERVAL_SECONDS": 300,

    # ── RISK MANAGEMENT ──────────────────────────────────────────────────────
    "STOP_LOSS_PCT":       2.0,
    "TAKE_PROFIT_PCT":     4.0,
    "MAX_DAILY_LOSS_PCT":  5.0,
    "MAX_DRAWDOWN_PCT":    10.0,
    "MAX_POSITION_PCT":    80.0,  # 80% per trade = $12 USDT per position (1 position at a time)
    "MAX_OPEN_TRADES":        1,   # kept for backward compat
    "MAX_OPEN_TRADES_STOCKS":  3,   # max simultaneous NSE positions
    "MAX_OPEN_TRADES_CRYPTO":  1,   # only 1 crypto trade at a time with $15 balance

    # ── CAPITAL ROTATION ─────────────────────────────────────────────────────
    # When True: stocks get full capital during NSE hours (9:15–3:30 IST),
    # crypto gets full capital after market close.
    # When False: both markets share capital simultaneously.
    "CAPITAL_ROTATION_ENABLED": True,

    # ── TRAILING STOP ─────────────────────────────────────────────────────────
    # Once trade reaches TRAILING_ACTIVATE_PCT profit, SL is moved to breakeven + buffer.
    "TRAILING_STOP_ENABLED":   True,
    "TRAILING_ACTIVATE_PCT":   1.5,   # activate after +1.5% profit
    "TRAILING_BUFFER_PCT":     0.3,   # lock in this much above entry

    # ── ATR-BASED SL/TP ──────────────────────────────────────────────────────
    # When True, ignores STOP_LOSS_PCT / TAKE_PROFIT_PCT and uses ATR multipliers.
    "ATR_BASED_EXITS":    True,
    "ATR_SL_MULTIPLIER":  1.5,   # SL = entry ± 1.5 * ATR
    "ATR_TP_MULTIPLIER":  3.0,   # TP = entry ± 3.0 * ATR  (2:1 R:R)

    # ── SIGNAL QUALITY ───────────────────────────────────────────────────────
    "MIN_SIGNAL_CONFIDENCE": 0.65,

    # ── LIQUIDITY FILTER ─────────────────────────────────────────────────────
    "MIN_AVG_VOLUME_STOCKS": 500000,  # Higher filter — only liquid stocks affordable at ₹1500
    "MIN_AVG_VOLUME_CRYPTO": 100000,

    # ── PARALLEL PROCESSING ──────────────────────────────────────────────────
    "THREAD_WORKERS": 5,

    # ── WATCHLIST RANKER ─────────────────────────────────────────────────────
    # After scanning, rank all signals by score and only trade top N per cycle.
    "WATCHLIST_RANKER_ENABLED": True,
    "WATCHLIST_TOP_N":          5,   # trade only the top 5 signals per cycle

    # ── COINDCX PAIR BLACKLIST ────────────────────────────────────────────────
    # Pairs that consistently get rejected (not listed, lot-size issues, etc.)
    # The bot will auto-add pairs at runtime too — add here to make permanent.
    "COINDCX_BLACKLIST": {
        "VICUSDT",        # not listed on CoinDCX
        "RVNUSDT",        # lot size too small for $15 capital
        "BANANAS31USDT",  # lot size / precision issue
        "GUNUSDT",        # lot size / precision issue
        "MAGICUSDT",      # auto-blacklisted: Invalid request
        "RSRUSDT",        # auto-blacklisted: Invalid request (micro-price)
        "PEPEUSDT",       # auto-blacklisted: Invalid request (micro-price, 3.6M qty)
    },

    # ── PRICE FLOOR ───────────────────────────────────────────────────────────
    # Skip coins below this USD price. At $15 capital, sub-cent coins need
    # millions of units which hits CoinDCX lot-size and precision limits.
    "MIN_COIN_PRICE_USD": 0.005,   # skip anything below half a cent

    # ── AI FEATURES ──────────────────────────────────────────────────────────
    # Claude API used as a second-opinion filter before placing trades.
    # Set ANTHROPIC_API_KEY in your environment variables.
    # NOTE: auto-disables itself when ANTHROPIC_API_KEY is missing/expired
    # so the bot keeps trading on algo signals alone.
    "AI_FILTER_ENABLED":  False,  # disabled — algo signals are sufficient at this capital level
    "AI_MIN_CONFIDENCE":  0.60,        # minimum Claude confidence to proceed
    "AI_MODEL":           "claude-haiku-4-5-20251001",  # fast + cheap

    # ── SENTIMENT ANALYSIS ────────────────────────────────────────────────────
    # Fetches news headlines and scores them via Claude before trading.
    # Requires NEWS_API_KEY env var (free tier at newsapi.org).
    # Auto-disables when ANTHROPIC_API_KEY is missing.
    "SENTIMENT_ENABLED":         False,
    "SENTIMENT_VETO_THRESHOLD":  -0.5,   # veto trade if sentiment < -0.5 (bearish)
    "NEWS_API_KEY":              os.environ.get("NEWS_API_KEY", ""),

    # ── AI STRATEGY ADVISOR ───────────────────────────────────────────────────
    # Weekly analysis: feeds closed trades to Claude → generates new strategy ideas.
    # Auto-disables when ANTHROPIC_API_KEY is missing.
    "AI_STRATEGY_ADVISOR_ENABLED": False,

    # ── LIVE DASHBOARD ────────────────────────────────────────────────────────
    "DASHBOARD_ENABLED": True,
    "DASHBOARD_PORT":    5001,
    "DASHBOARD_HOST":    "0.0.0.0",

    # ── STRATEGIES ───────────────────────────────────────────────────────────
    "STRATEGIES": {
        "macd_crossover":            True,
        "ma_crossover":              True,
        "ema_ribbon":                True,
        "supertrend":                True,
        "adx_trend":                 True,
        "parabolic_sar":             True,
        "rsi_mean_reversion":        True,
        "bollinger_bands":           True,
        "stochastic_reversal":       True,
        "rsi_divergence":            True,
        "mean_reversion_zscore":     True,
        "donchian_breakout":         True,
        "range_breakout":            True,
        "volatility_breakout":       True,
        "resistance_breakout":       True,
        "vwap_reversion":            True,
        "obv_trend":                 True,
        "volume_price_trend":        True,
        "accumulation_distribution": True,
        "hammer_pattern":            True,
        "engulfing_pattern":         True,
        "morning_evening_star":      True,
        "three_soldiers_crows":      True,
        "doji_reversal":             True,
        "custom_trend_rsi":          True,
        "custom_momentum_volume":    True,
        "custom_squeeze_breakout":   True,
        "custom_multi_timeframe":    True,
        "custom_smart_scalp":        True,
        "statistical_arb":           False,
    },

    # ── STRATEGY PARAMETERS ──────────────────────────────────────────────────
    "RSI_PERIOD":           14,
    "RSI_OVERSOLD":         30,
    "RSI_OVERBOUGHT":       70,
    "MACD_FAST":            12,
    "MACD_SLOW":            26,
    "MACD_SIGNAL":          9,
    "BB_PERIOD":            20,
    "BB_STD":               2,
    "MA_FAST":              50,
    "MA_SLOW":              200,
    "CUSTOM_RSI_THRESHOLD": 40,
    "CUSTOM_TREND_EMA":     21,
    "VWAP_PERIOD":          20,      # rolling VWAP period (replaces cumulative VWAP)

    # ── SYMBOLS (auto-fetched at startup) ─────────────────────────────────────
    "STOCK_SYMBOLS":  [],
    "CRYPTO_SYMBOLS": [],

    # ── BROKER SELECTION ─────────────────────────────────────────────────────
    # Stock broker: "angelone" (default, free) or "zerodha"
    # Crypto broker: "coindcx" (Indian) or "binance" or "paper" (no real crypto)
    "STOCK_BROKER":  "angelone",
    "CRYPTO_BROKER": "coindcx",      # Change to "coindcx" or "binance" when ready

    # ── ANGEL ONE API (Indian Stocks) ─────────────────────────────────────────
    # Get from: myapi.angelbroking.com
    # Enable TOTP on your Angel One account first (Google Authenticator)
    "ANGELONE_API_KEY":     os.environ.get("ANGELONE_API_KEY", ""),
    "ANGELONE_CLIENT_ID":   os.environ.get("ANGELONE_CLIENT_ID", ""),      # your login ID
    "ANGELONE_PASSWORD":    os.environ.get("ANGELONE_PASSWORD", ""),       # your login password
    "ANGELONE_TOTP_SECRET": os.environ.get("ANGELONE_TOTP_SECRET", ""),   # from Authenticator app

    # ── COINDCX API (Crypto — Indian exchange) ────────────────────────────────
    # Get from: coindcx.com → Settings → API Keys (free)
    "COINDCX_API_KEY":    os.environ.get("COINDCX_API_KEY", ""),
    "COINDCX_API_SECRET": os.environ.get("COINDCX_API_SECRET", ""),

    # ── BINANCE API (Crypto — if you have an account) ─────────────────────────
    "BINANCE_API_KEY":    os.environ.get("BINANCE_API_KEY", ""),
    "BINANCE_API_SECRET": os.environ.get("BINANCE_API_SECRET", ""),

    # ── ZERODHA API (kept for compatibility) ──────────────────────────────────
    "ZERODHA_API_KEY":      os.environ.get("ZERODHA_API_KEY", ""),
    "ZERODHA_API_SECRET":   os.environ.get("ZERODHA_API_SECRET", ""),
    "ZERODHA_ACCESS_TOKEN": os.environ.get("ZERODHA_ACCESS_TOKEN", ""),

    # ── ANTHROPIC API ─────────────────────────────────────────────────────────
    "ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY", ""),

    # ── TELEGRAM ─────────────────────────────────────────────────────────────
    "TELEGRAM_TOKEN":   os.environ.get("TELEGRAM_TOKEN", ""),
    "TELEGRAM_CHAT_ID": os.environ.get("TELEGRAM_CHAT_ID", ""),
    "NOTIFICATIONS_ON": True,
}
