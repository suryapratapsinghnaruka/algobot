"""
AlgoBot - Automated Trading Bot (v2)
=====================================
New in v2:
- Watchlist Ranker: scan all symbols, trade only the top N setups
- AI Filter: Claude second-opinion before every trade
- Sentiment Analysis: news sentiment check via Claude
- Trade Journal: CSV log + per-strategy win rate tracking
- ATR-based dynamic SL/TP
- Trailing stop
- Live web dashboard (Flask)
- AI Strategy Advisor (weekly)
- Fixed: daily reset race condition
- Fixed: instrument map cached at startup
- Fixed: rolling VWAP
- Fixed: thread-safe yfinance sessions
- Fixed: stat arb memory leak
"""

import time
import logging
import json
import os
import sys
import warnings
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Load .env FIRST — before config.py reads os.environ ───────────────────────
# Ensures ANTHROPIC_API_KEY, TELEGRAM_TOKEN, broker keys, etc. are populated
# when CONFIG dict is built, even when running locally without shell env vars.
try:
    from dotenv import load_dotenv
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(_env_path):
        load_dotenv(_env_path, override=False)   # override=False: real shell vars win
        print(f"[Startup] Loaded environment from {_env_path}")
    else:
        print("[Startup] No .env file found — relying on shell environment variables.")
except ImportError:
    print("[Startup] python-dotenv not installed. Run: pip install -r requirements.txt")

try:
    import yfinance as yf
    _cache_dir = os.path.join(os.path.expanduser("~"), "yfinance_cache")
    os.makedirs(_cache_dir, exist_ok=True)
    yf.set_tz_cache_location(_cache_dir)
except Exception:
    pass

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message=".*TzCache.*")

# Silence noisy third-party loggers
for _noisy in ["yfinance", "peewee", "urllib3", "requests", "asyncio"]:
    logging.getLogger(_noisy).setLevel(logging.CRITICAL)

from config import CONFIG, get_all_nse_symbols, get_all_binance_symbols
from strategies import StrategyEngine
from risk_manager import RiskManager
from broker import AngelOneBroker, ZerodhaBroker, BinanceBroker, CoinDCXBroker
from paper_trader import PaperTrader
from notifier import Notifier
from ai_filter import AIFilter
from watchlist_ranker import WatchlistRanker
from trade_journal import TradeJournal

# ── Logging ───────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

_fmt            = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
_file_handler   = logging.FileHandler("logs/bot.log", encoding="utf-8")
_stream_handler = logging.StreamHandler(sys.stdout)
_file_handler.setFormatter(_fmt)
_stream_handler.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _stream_handler])
log = logging.getLogger("AlgoBot")

POSITIONS_FILE = "logs/open_positions.json"


class AlgoBot:
    def __init__(self):
        log.info("=" * 65)
        log.info("AlgoBot v2 starting up...")
        log.info(f"Mode: {'PAPER TRADING' if CONFIG['PAPER_TRADING'] else '*** LIVE TRADING ***'}")
        log.info("=" * 65)

        log.info("Fetching full symbol lists from exchanges...")
        CONFIG["STOCK_SYMBOLS"] = get_all_nse_symbols()

        _crypto_broker = CONFIG.get("CRYPTO_BROKER", "paper").lower()
        if _crypto_broker == "delta":
            try:
                import requests as _req
                r = _req.get("https://api.india.delta.exchange/v2/products",
                             params={"contract_types": "perpetual_futures",
                                     "page_size": "200"}, timeout=15)
                data = r.json()
                delta_syms = [p["symbol"] for p in data.get("result", [])
                              if p.get("trading_status") == "operational"]
                CONFIG["CRYPTO_SYMBOLS"] = delta_syms
                log.info(f"Universe: {len(CONFIG['STOCK_SYMBOLS'])} NSE stocks + "
                         f"{len(CONFIG['CRYPTO_SYMBOLS'])} Delta perpetuals")
            except Exception as e:
                log.warning(f"Delta symbol fetch failed ({e}) — using Binance fallback")
                CONFIG["CRYPTO_SYMBOLS"] = get_all_binance_symbols()
                log.info(f"Universe: {len(CONFIG['STOCK_SYMBOLS'])} NSE stocks + "
                         f"{len(CONFIG['CRYPTO_SYMBOLS'])} Binance pairs (fallback)")
        else:
            CONFIG["CRYPTO_SYMBOLS"] = get_all_binance_symbols()
            log.info(f"Universe: {len(CONFIG['STOCK_SYMBOLS'])} NSE stocks + "
                     f"{len(CONFIG['CRYPTO_SYMBOLS'])} Binance pairs")

        self.risk    = RiskManager(CONFIG)
        self.engine  = StrategyEngine(CONFIG)
        self.notify  = Notifier(CONFIG)
        self.journal = TradeJournal()
        self.ai      = AIFilter(CONFIG)

        self.daily_pnl    = {"stocks": 0.0, "crypto": 0.0}
        self._last_reset  = datetime.now().date()
        self._ai_confidence_map: dict = {}
        self._sentiment_map: dict     = {}

        # Capital rotation state
        self._rotation_mode   = "none"   # "stocks" | "crypto" | "none"
        self._rotation_capital = CONFIG.get("CAPITAL", 1500)
        self._crypto_opened_after_close = False   # track if we opened crypto this session

        # Weekly advisor tracker
        self._last_advisor_run = datetime.now() - timedelta(days=8)

        if CONFIG["PAPER_TRADING"]:
            self.brokers = {
                "stocks": PaperTrader("stocks", CONFIG),
                "crypto": PaperTrader("crypto", CONFIG),
            }
        else:
            # Select stock broker
            stock_broker_name = CONFIG.get("STOCK_BROKER", "angelone").lower()
            if stock_broker_name == "zerodha":
                stock_broker = ZerodhaBroker(CONFIG)
            else:
                try:
                    stock_broker = AngelOneBroker(CONFIG)
                    # If Angel One is unavailable (maintenance), fall back to paper
                    if not stock_broker._available:
                        log.warning(
                            "Angel One unavailable at startup — using paper trader for stocks. "
                            "Will auto-reconnect after 6 AM IST."
                        )
                        stock_broker = PaperTrader("stocks", CONFIG)
                except Exception as e:
                    log.warning(f"Angel One init failed ({e}) — using paper trader for stocks.")
                    stock_broker = PaperTrader("stocks", CONFIG)

            # Select crypto broker (or paper trade if no crypto API configured)
            crypto_broker_name = CONFIG.get("CRYPTO_BROKER", "paper").lower()
            if crypto_broker_name == "binance":
                crypto_broker = BinanceBroker(CONFIG)
            elif crypto_broker_name == "coindcx":
                crypto_broker = CoinDCXBroker(CONFIG)
            elif crypto_broker_name == "delta":
                try:
                    from broker import DeltaBroker
                    crypto_broker = DeltaBroker(CONFIG)
                except Exception as e:
                    log.warning(f"Delta Exchange init failed ({e}) — using paper trader for crypto.")
                    crypto_broker = PaperTrader("crypto", CONFIG)
            else:
                # No crypto API — paper trade crypto with real prices
                log.info("No crypto broker configured — using paper trader for crypto.")
                crypto_broker = PaperTrader("crypto", CONFIG)

            self.brokers = {
                "stocks": stock_broker,
                "crypto": crypto_broker,
            }

        self.ranker = WatchlistRanker(
            CONFIG, self.engine,
            self.brokers["stocks"], self.brokers["crypto"]
        )

        self._restore_positions()

        # Start dashboard in background thread
        if CONFIG.get("DASHBOARD_ENABLED"):
            self._start_dashboard()

    # ── MAIN LOOP ─────────────────────────────────────────────────────────────

    def run(self):
        log.info("Bot is live. Scanning entire market every candle...")
        self.notify.send(
            f"AlgoBot v2 started\n"
            f"Stocks: {len(CONFIG['STOCK_SYMBOLS'])} symbols\n"
            f"Crypto: {len(CONFIG['CRYPTO_SYMBOLS'])} symbols\n"
            f"AI filter: {'ON' if CONFIG.get('AI_FILTER_ENABLED') else 'OFF'}\n"
            f"Sentiment: {'ON' if CONFIG.get('SENTIMENT_ENABLED') else 'OFF'}"
        )

        while True:
            try:
                now = datetime.now()

                # Daily reset
                if now.date() > self._last_reset:
                    self._reset_daily_stats()

                # Weekly AI strategy advisor
                if (CONFIG.get("AI_STRATEGY_ADVISOR_ENABLED") and
                        (now - self._last_advisor_run).days >= 7):
                    self._run_strategy_advisor()

                self._run_rotation_cycle()

                time.sleep(CONFIG["CANDLE_INTERVAL_SECONDS"])

            except KeyboardInterrupt:
                log.info("Bot stopped by user.")
                self.notify.send("AlgoBot stopped.")
                self._print_summary()
                break
            except Exception as e:
                log.error(f"Main loop error: {e}", exc_info=True)
                self.notify.send(f"Bot error: {e}")
                time.sleep(30)


    # ── CAPITAL ROTATION ─────────────────────────────────────────────────────

    def _run_rotation_cycle(self):
        """
        Capital rotation logic:
        - NSE hours (9:15–3:30 IST):  trade STOCKS only, pause crypto new entries
        - After 3:15 PM IST:          square off stocks, rotate capital to crypto
        - After 3:30 PM IST:          trade CRYPTO only with full capital
        - Weekend:                    trade CRYPTO only (NSE closed)

        This means the same ₹1,500 works in stocks during the day
        and in crypto overnight — no idle capital.
        """
        from datetime import timezone, timedelta as td
        IST     = timezone(td(hours=5, minutes=30))
        now_ist = datetime.now(IST)
        rotation = CONFIG.get("CAPITAL_ROTATION_ENABLED", False)

        market_open    = self._is_market_open_stocks()
        squareoff_time = self._is_squareoff_time()
        is_weekday     = now_ist.weekday() < 5

        if not rotation:
            # Original behaviour — both markets active simultaneously
            if market_open:
                self._run_cycle("stocks")
            elif squareoff_time:
                self._squareoff_all_stocks()
            else:
                n = len(self.brokers["stocks"].get_open_positions())
                if n:
                    log.info(f"[STOCKS] Market closed {now_ist.strftime('%H:%M')} IST | "
                             f"{n} positions held.")
            self._run_cycle("crypto")
            return

        # ── ROTATION MODE ─────────────────────────────────────────────────────

        if market_open:
            # PHASE 1: NSE is open — trade stocks only
            if self._rotation_mode != "stocks":
                log.info(f"[ROTATION] Switching to STOCKS mode — "
                         f"capital: ₹{self._rotation_capital:,.0f} | "
                         f"time: {now_ist.strftime('%H:%M')} IST")
                self._rotation_mode = "stocks"
                self._crypto_opened_after_close = False
                # Close any open crypto positions before switching to stocks
                self._close_all_crypto_for_rotation()

            self._run_cycle("stocks")
            # Monitor crypto positions that were entered before rotation
            # (should be none, but safety net)
            self._monitor_positions("crypto")

        elif squareoff_time:
            # PHASE 2: 3:15 PM — square off all stocks, prepare for crypto rotation
            if self._rotation_mode == "stocks":
                log.info("[ROTATION] 3:15 PM — squaring off stocks, rotating to CRYPTO...")
                self._squareoff_all_stocks()
                self._rotation_mode = "rotating"

        elif is_weekday and not market_open:
            # PHASE 3: After market close on weekdays — crypto only
            if self._rotation_mode in ("stocks", "rotating"):
                freed = self._get_available_capital("stocks")
                log.info(f"[ROTATION] Market closed. Capital rotated to CRYPTO. "
                         f"Available: ₹{freed:,.0f} | time: {now_ist.strftime('%H:%M')} IST")
                self._rotation_mode = "crypto"
                self.notify.send(
                    "[ROTATION] NSE closed. "
                    f"Rotating Rs.{self._rotation_capital:,.0f} to crypto until 9:15 AM IST."
                )

            if self._rotation_mode in ("crypto", "none", "rotating"):
                self._rotation_mode = "crypto"
                self._run_cycle("crypto")

        else:
            # Weekend — crypto all day
            if self._rotation_mode != "crypto":
                log.info(f"[ROTATION] Weekend — CRYPTO mode all day.")
                self._rotation_mode = "crypto"
            self._run_cycle("crypto")

    def _close_all_crypto_for_rotation(self):
        """Close all open crypto positions before rotating capital to stocks."""
        broker    = self.brokers["crypto"]
        positions = broker.get_open_positions()
        if not positions:
            return
        log.info(f"[ROTATION] Closing {len(positions)} crypto positions before market open...")
        for pos in list(positions):
            try:
                symbol = pos["symbol"]
                ltp    = broker._get_current_price(symbol)
                if ltp is None:
                    ltp = pos["entry_price"]
                entry  = pos["entry_price"]
                qty    = pos["qty"]
                action = pos["action"]
                pnl    = (ltp - entry) * qty if action == "BUY" else (entry - ltp) * qty
                broker.total_pnl += pnl
                broker.closed_trades.append({**pos, "exit_price": ltp,
                                              "pnl": pnl, "reason": "rotation-to-stocks"})
                broker.open_positions.pop(symbol, None)
                self.daily_pnl["crypto"] += pnl
                result = {"symbol": symbol, "pnl": pnl,
                          "reason": "rotation-to-stocks", "exit_price": ltp}
                self.journal.log_trade(pos, result,
                                       self._ai_confidence_map.get(symbol, 1.0),
                                       self._sentiment_map.get(symbol, 0.0))
                log.info(f"[ROTATION] Closed crypto {symbol} | PnL: {pnl:+.2f}")
            except Exception as e:
                log.error(f"Rotation close error for {pos.get('symbol','?')}: {e}")
        self._save_positions()

    def _get_available_capital(self, market: str) -> float:
        """Estimate capital freed up from closed positions."""
        broker    = self.brokers[market]
        positions = broker.get_open_positions()
        deployed  = sum(
            p.get("entry_price", 0) * p.get("qty", 0)
            for p in positions
        )
        return max(0, self._rotation_capital - deployed)

    # ── CYCLE ─────────────────────────────────────────────────────────────────

    def _run_cycle(self, market: str):
        """
        Use WatchlistRanker to find top N signals, then apply AI filter,
        sentiment check, risk check, and execute trades.
        """
        log.info(f"[{market.upper()}] Cycle start — "
                 f"{len(CONFIG['STOCK_SYMBOLS'] if market == 'stocks' else CONFIG['CRYPTO_SYMBOLS'])} symbols")

        broker = self.brokers[market]

        # ── Ranker: scan all symbols, return top N ────────────────────────────
        if CONFIG.get("WATCHLIST_RANKER_ENABLED"):
            candidates = self.ranker.rank(market)
        else:
            # Fallback: old full scan (no ranking)
            candidates = self._full_scan_fallback(market)

        trade_count = 0

        for candidate in candidates:
            symbol   = candidate["symbol"]
            action   = candidate["action"]
            signal   = candidate["signal"]
            candles  = candidate["candles"]
            candles_summary = candidate.get("candles_summary", {})

            # ── Risk check ────────────────────────────────────────────────────
            open_positions = broker.get_open_positions()
            if not self.risk.can_trade(symbol, action, self.daily_pnl[market], open_positions, market=market):
                log.debug(f"[{market}] {symbol} blocked by risk manager")
                continue

            # ── Sentiment check ───────────────────────────────────────────────
            sentiment_score = 0.0
            if CONFIG.get("SENTIMENT_ENABLED") and self.ai.enabled:
                sentiment_score, sentiment_summary = self.ai.get_sentiment(symbol, market)
                veto_threshold = CONFIG.get("SENTIMENT_VETO_THRESHOLD", -0.5)
                if sentiment_score < veto_threshold:
                    log.info(f"[Sentiment] VETO {symbol} — {sentiment_score:+.2f}: {sentiment_summary}")
                    continue

            # ── AI filter ─────────────────────────────────────────────────────
            ai_confidence = 1.0
            if CONFIG.get("AI_FILTER_ENABLED") and self.ai.enabled:
                proceed, ai_confidence, reasoning = self.ai.should_trade(
                    symbol, action, signal, candles_summary
                )
                if not proceed:
                    log.info(f"[AI] VETO {symbol} {action} — {reasoning}")
                    continue
                if ai_confidence < CONFIG.get("AI_MIN_CONFIDENCE", 0.60):
                    log.info(f"[AI] Low confidence veto {symbol}: {ai_confidence:.0%}")
                    continue

            # ── Execute trade ─────────────────────────────────────────────────
            price = float(candles["close"].iloc[-1])
            atr   = float(candles.get("atr", candles["close"]).iloc[-1]) \
                    if "atr" in candles.columns else None

            # For crypto on CoinDCX, candle prices come back in INR.
            # Position sizing must use the live USD ticker price instead.
            sizing_price = price
            if market == "crypto" and hasattr(broker, "_get_current_price"):
                usd_price = broker._get_current_price(symbol)
                if usd_price and usd_price > 0:
                    if atr and price > 0:
                        atr = atr * (usd_price / price)   # scale ATR INR→USD
                    sizing_price = usd_price

            qty = self.risk.position_size(sizing_price, CONFIG["CAPITAL"], atr)

            # CoinDCX minimum order is ~$11 USD — bump qty if needed
            if market == "crypto" and hasattr(broker, "_get_current_price") and sizing_price > 0:
                min_order_usd = 11.0
                if qty * sizing_price < min_order_usd:
                    qty = max(qty, int(min_order_usd / sizing_price) + 1)
                    log.debug(f"[{symbol}] qty bumped to {qty} to meet $11 min order value")

            order = broker.place_order(
                symbol          = symbol,
                action          = action,
                qty             = qty,
                price           = price,
                stop_loss_pct   = CONFIG["STOP_LOSS_PCT"],
                take_profit_pct = CONFIG["TAKE_PROFIT_PCT"],
                strategy        = signal["strategy"],
                atr             = atr,
                signal_confidence     = signal["confidence"],
                confluence_count      = signal.get("confluence_count", 1),
            )

            if order:
                trade_count += 1
                self._ai_confidence_map[symbol]  = ai_confidence
                self._sentiment_map[symbol]      = sentiment_score
                self._save_positions()

                strats_str = ", ".join(signal.get("strategies_agreed", [signal["strategy"]]))
                msg = (
                    f"{'[BUY]' if action == 'BUY' else '[SELL]'} {action} {qty} {symbol} "
                    f"@ {price:.2f}\n"
                    f"Strategies: {strats_str}\n"
                    f"Signal conf: {signal['confidence']:.0%} | "
                    f"AI conf: {ai_confidence:.0%} | "
                    f"Sentiment: {sentiment_score:+.2f}\n"
                    f"SL: {order['stop_loss']:.2f} | TP: {order['take_profit']:.2f}"
                )
                log.info(msg)
                self.notify.send(msg)

        log.info(f"[{market.upper()}] Cycle done | Trades executed: {trade_count}")
        self._monitor_positions(market)

    # ── POSITION MONITORING ───────────────────────────────────────────────────

    def _monitor_positions(self, market: str):
        broker    = self.brokers[market]
        positions = broker.get_open_positions()

        if not positions:
            return

        for pos in positions:
            try:
                result = broker.check_exit(pos)
                if result:
                    self.daily_pnl[market] += result["pnl"]
                    self._save_positions()

                    # Log to journal with AI/sentiment metadata
                    ai_conf   = self._ai_confidence_map.get(pos["symbol"], 1.0)
                    sentiment = self._sentiment_map.get(pos["symbol"], 0.0)
                    self.journal.log_trade(pos, result, ai_conf, sentiment)

                    # Update ranker strategy stats
                    if CONFIG.get("WATCHLIST_RANKER_ENABLED"):
                        self.ranker.update_strategy_stats(
                            pos.get("strategy", "unknown"),
                            won=(result["pnl"] > 0)
                        )

                    emoji  = "[+]" if result["pnl"] > 0 else "[-]"
                    reason = result["reason"]
                    msg = (
                        f"{emoji} CLOSED {pos['symbol']} [{market.upper()}]\n"
                        f"Exit: {result.get('exit_price', 0):.4f} | "
                        f"Entry: {pos.get('entry_price', 0):.4f}\n"
                        f"PnL: {result['pnl']:+.2f} | Reason: {reason}\n"
                        f"Strategy: {pos.get('strategy','?')} | "
                        f"Daily P&L ({market}): {self.daily_pnl[market]:+.2f}"
                    )
                    log.info(msg)
                    self.notify.send(msg)

                    if self.risk.is_drawdown_breached(self.daily_pnl[market]):
                        self.notify.send(f"[STOP] Max drawdown hit. Pausing until tomorrow.")
                        self._pause_until_tomorrow()

            except Exception as e:
                log.error(f"Monitor error for {pos.get('symbol', '?')}: {e}")

    # ── FULL SCAN FALLBACK (no ranker) ────────────────────────────────────────

    def _full_scan_fallback(self, market: str) -> list[dict]:
        """Original full-scan behaviour when ranker is disabled."""
        broker  = self.brokers[market]
        symbols = CONFIG["STOCK_SYMBOLS"] if market == "stocks" else CONFIG["CRYPTO_SYMBOLS"]
        workers = CONFIG.get("THREAD_WORKERS", 5)
        min_vol = (CONFIG["MIN_AVG_VOLUME_STOCKS"] if market == "stocks"
                   else CONFIG["MIN_AVG_VOLUME_CRYPTO"])

        candidates = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self._process_symbol_simple, s, broker, market, min_vol): s
                for s in symbols
            }
            for future in as_completed(futures):
                try:
                    results = future.result()
                    candidates.extend(results)
                except Exception:
                    pass

        return candidates

    def _process_symbol_simple(self, symbol, broker, market, min_vol) -> list:
        try:
            candles = broker.get_candles(symbol, CONFIG["CANDLE_TIMEFRAME"], limit=100)
            if candles is None or len(candles) < 50:
                return []
            if candles["volume"].mean() < min_vol:
                return []
            signals = self.engine.scan(symbol, candles, market)
            results = []
            for s in signals:
                if s and s["confidence"] >= CONFIG.get("MIN_SIGNAL_CONFIDENCE", 0.65):
                    results.append({
                        "symbol": symbol, "action": s["action"],
                        "strategy": s["strategy"], "confidence": s["confidence"],
                        "score": s["confidence"], "signal": s,
                        "candles": candles, "candles_summary": {}, "market": market,
                    })
            return results
        except Exception:
            return []

    # ── AI STRATEGY ADVISOR ───────────────────────────────────────────────────

    def _run_strategy_advisor(self):
        log.info("[AI Advisor] Running weekly strategy analysis...")
        closed_trades = self.journal.get_closed_trades()
        report = self.ai.suggest_strategies(closed_trades, {})

        report_path = f"logs/ai_advisor_{datetime.now().strftime('%Y%m%d')}.md"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(f"# AI Strategy Advisor Report\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
            f.write(report)

        log.info(f"[AI Advisor] Report saved to {report_path}")
        self.notify.send(f"[AI Advisor] Weekly report generated. Check {report_path}")
        self._last_advisor_run = datetime.now()

        # Also log strategy stats
        log.info(self.journal.strategy_summary())

    # ── DASHBOARD ─────────────────────────────────────────────────────────────

    def _start_dashboard(self):
        """Start Flask dashboard in a background daemon thread."""
        try:
            from web_dashboard import create_app
            app = create_app(self.brokers, self.journal, self.daily_pnl, CONFIG)

            def run_flask():
                app.run(
                    host=CONFIG.get("DASHBOARD_HOST", "0.0.0.0"),
                    port=CONFIG.get("DASHBOARD_PORT", 5001),
                    debug=False,
                    use_reloader=False,
                )

            t = threading.Thread(target=run_flask, daemon=True)
            t.start()
            log.info(f"Dashboard running at http://localhost:{CONFIG.get('DASHBOARD_PORT', 5001)}")
        except ImportError:
            log.warning("Flask not installed — dashboard disabled. Run: pip install flask")
        except Exception as e:
            log.warning(f"Dashboard start failed: {e}")

    # ── POSITION PERSISTENCE ──────────────────────────────────────────────────

    def _save_positions(self):
        try:
            data = {}
            for market, broker in self.brokers.items():
                data[market] = broker.get_open_positions()
            with open(POSITIONS_FILE, "w") as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            log.warning(f"Could not save positions: {e}")

    def _restore_positions(self):
        if not os.path.exists(POSITIONS_FILE):
            return
        try:
            with open(POSITIONS_FILE) as f:
                data = json.load(f)
            for market, positions in data.items():
                if market in self.brokers:
                    for pos in positions:
                        self.brokers[market].open_positions[pos["symbol"]] = pos
            total = sum(len(v) for v in data.values())
            if total:
                log.info(f"Restored {total} open positions from disk.")
        except Exception as e:
            log.warning(f"Could not restore positions: {e}")

    # ── HELPERS ───────────────────────────────────────────────────────────────

    def _squareoff_all_stocks(self):
        """
        Auto square-off all open stock positions at 3:15 PM IST.
        Mirrors Zerodha MIS auto-squareoff behaviour.
        In paper trading, closes at last available price.
        """
        broker    = self.brokers["stocks"]
        positions = broker.get_open_positions()
        if not positions:
            return
        log.info(f"[STOCKS] 3:15 PM square-off: closing {len(positions)} MIS positions...")
        self.notify.send(f"[SQUAREOFF] Closing {len(positions)} stock positions at 3:15 PM IST")
        for pos in list(positions):
            try:
                symbol = pos["symbol"]
                ltp    = broker._get_current_price(symbol)
                if ltp is None:
                    ltp = pos["entry_price"]   # fallback to entry if price unavailable
                entry  = pos["entry_price"]
                qty    = pos["qty"]
                action = pos["action"]
                pnl    = (ltp - entry) * qty if action == "BUY" else (entry - ltp) * qty
                broker.total_pnl += pnl
                broker.closed_trades.append({**pos, "exit_price": ltp,
                                              "pnl": pnl, "reason": "squareoff"})
                broker.open_positions.pop(symbol, None)
                self.daily_pnl["stocks"] += pnl
                result = {"symbol": symbol, "pnl": pnl,
                          "reason": "squareoff", "exit_price": ltp}
                ai_conf   = self._ai_confidence_map.get(symbol, 1.0)
                sentiment = self._sentiment_map.get(symbol, 0.0)
                self.journal.log_trade(pos, result, ai_conf, sentiment)
                if CONFIG.get("WATCHLIST_RANKER_ENABLED"):
                    self.ranker.update_strategy_stats(
                        pos.get("strategy", "unknown"), won=(pnl > 0))
                emoji = "[+]" if pnl > 0 else "[-]"
                log.info(f"{emoji} SQUAREOFF {symbol} @ {ltp:.2f} | PnL: {pnl:+.2f}")
            except Exception as e:
                log.error(f"Squareoff error for {pos.get('symbol', '?')}: {e}")
        self._save_positions()
        log.info(f"[STOCKS] Square-off done. Daily PnL: {self.daily_pnl['stocks']:+.2f}")

    def _is_market_open_stocks(self, now: datetime = None) -> bool:
        """
        NSE market hours: Mon-Fri, 9:15 AM - 3:30 PM IST.
        Uses IST (UTC+5:30) explicitly so the bot works correctly
        even when deployed on servers in other timezones (Railway, Oracle Cloud).
        """
        from datetime import timezone, timedelta as td
        IST = timezone(td(hours=5, minutes=30))
        now_ist = datetime.now(IST)
        if now_ist.weekday() >= 5:   # Saturday=5, Sunday=6
            return False
        market_open  = now_ist.replace(hour=9,  minute=15, second=0, microsecond=0)
        market_close = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
        return market_open <= now_ist <= market_close

    def _is_squareoff_time(self) -> bool:
        """3:15 PM IST — auto square-off all MIS stock positions (like Zerodha does)."""
        from datetime import timezone, timedelta as td
        IST = timezone(td(hours=5, minutes=30))
        now_ist = datetime.now(IST)
        return (now_ist.weekday() < 5 and
                now_ist.hour == 15 and now_ist.minute >= 15)

    def _reset_daily_stats(self):
        self.daily_pnl  = {"stocks": 0.0, "crypto": 0.0}
        self._last_reset = datetime.now().date()
        self.risk.reset_daily()
        log.info("Daily stats reset.")
        self.notify.send("[RESET] New trading day. Stats reset.")

    def _pause_until_tomorrow(self):
        now      = datetime.now()
        tomorrow = (now + timedelta(days=1)).replace(hour=9, minute=0, second=0)
        sleep_s  = (tomorrow - now).total_seconds()
        log.info(f"Sleeping until {tomorrow}...")
        time.sleep(sleep_s)

    def _print_summary(self):
        log.info("=" * 65)
        log.info("SESSION SUMMARY")
        for market, broker in self.brokers.items():
            if hasattr(broker, "summary"):
                log.info(broker.summary())
        log.info(f"Daily PnL — Stocks: {self.daily_pnl['stocks']:+.2f} | "
                 f"Crypto: {self.daily_pnl['crypto']:+.2f}")
        log.info(self.journal.strategy_summary())
        log.info("=" * 65)


if __name__ == "__main__":
    bot = AlgoBot()
    bot.run()
