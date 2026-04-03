"""
Paper Trader — fixed version
=============================
Fixes:
- Thread-safe sessions via threading.local()
- ATR-based SL/TP support
- Trailing stop logic in check_exit
- Candle summary method for AI filter
"""

import pandas as pd
import logging
import time
import random
import threading
from datetime import datetime

log = logging.getLogger("PaperTrader")

_local = threading.local()


def _get_session():
    """Return a thread-local curl_cffi session (thread-safe)."""
    if not hasattr(_local, "session"):
        try:
            from curl_cffi import requests as curl_requests
            _local.session = curl_requests.Session(impersonate="chrome")
        except ImportError:
            import requests as _req
            _local.session = _req.Session()
            _local.session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            })
    return _local.session


class PaperTrader:
    def __init__(self, market: str, config: dict):
        self.market         = market
        self.cfg            = config
        self.open_positions = {}
        self.closed_trades  = []
        self.total_pnl      = 0.0
        log.info(f"Paper trader initialized for {market}.")

    def get_candles(self, symbol: str, timeframe: str, limit: int = 100) -> pd.DataFrame | None:
        import yfinance as yf

        yf_symbol  = self._to_yf_symbol(symbol)
        tf_map     = {"1m": "1m",  "5m": "5m",  "15m": "15m",  "1h": "1h"}
        period_map = {"1m": "7d",  "5m": "60d", "15m": "60d",  "1h": "730d"}
        interval   = tf_map.get(timeframe, "5m")
        period     = period_map.get(timeframe, "60d")

        for attempt in range(3):
            try:
                time.sleep(random.uniform(0.1, 0.4))
                session = _get_session()
                ticker  = yf.Ticker(yf_symbol, session=session)
                df      = ticker.history(period=period, interval=interval, timeout=15)

                if df is None or df.empty:
                    return None

                df = df.rename(columns={
                    "Open": "open", "High": "high",
                    "Low":  "low",  "Close": "close", "Volume": "volume"
                })
                df = df[["open", "high", "low", "close", "volume"]].tail(limit).reset_index(drop=True)
                return df

            except Exception as e:
                err = str(e).lower()
                if ("timeout" in err or "timed out" in err or "connection" in err) and attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                log.debug(f"Candles error for {symbol}: {e}")
                return None

        return None

    def place_order(self, symbol, action, qty, price,
                    stop_loss_pct, take_profit_pct, strategy,
                    atr=None, signal_confidence=1.0,
                    confluence_count=1) -> dict:
        """Place a paper order. Uses ATR-based SL/TP if configured and ATR is available."""

        if self.cfg.get("ATR_BASED_EXITS") and atr and atr > 0:
            sl_dist = self.cfg.get("ATR_SL_MULTIPLIER", 1.5) * atr
            tp_dist = self.cfg.get("ATR_TP_MULTIPLIER", 3.0) * atr
            sl = price - sl_dist if action == "BUY" else price + sl_dist
            tp = price + tp_dist if action == "BUY" else price - tp_dist
        else:
            sl = price * (1 - stop_loss_pct / 100) if action == "BUY" else price * (1 + stop_loss_pct / 100)
            tp = price * (1 + take_profit_pct / 100) if action == "BUY" else price * (1 - take_profit_pct / 100)

        trade = {
            "order_id":          f"PAPER-{datetime.now().strftime('%H%M%S%f')}",
            "symbol":            symbol,
            "action":            action,
            "qty":               qty,
            "entry_price":       price,
            "stop_loss":         sl,
            "take_profit":       tp,
            "original_sl":       sl,    # kept for trailing stop reference
            "strategy":          strategy,
            "time":              datetime.now().isoformat(),
            "market":            self.market,
            "signal_confidence": signal_confidence,
            "confluence_count":  confluence_count,
            "trailing_activated":False,
        }
        self.open_positions[symbol] = trade
        log.info(f"[PAPER] {action} {qty} {symbol} @ {price:.4f} | SL:{sl:.4f} TP:{tp:.4f}")
        return trade

    def get_open_positions(self) -> list:
        return list(self.open_positions.values())

    def check_exit(self, position: dict) -> dict | None:
        """Check SL/TP and apply trailing stop logic."""
        ltp = self._get_current_price(position["symbol"])
        if ltp is None:
            return None

        action = position["action"]
        entry  = position["entry_price"]
        qty    = position["qty"]

        # ── Trailing stop ─────────────────────────────────────────────────────
        if self.cfg.get("TRAILING_STOP_ENABLED"):
            activate_pct = self.cfg.get("TRAILING_ACTIVATE_PCT", 1.5) / 100
            buffer_pct   = self.cfg.get("TRAILING_BUFFER_PCT", 0.3) / 100

            if action == "BUY":
                unrealised_pct = (ltp - entry) / entry
                if unrealised_pct >= activate_pct:
                    new_sl = entry * (1 + buffer_pct)
                    if new_sl > position["stop_loss"]:
                        position["stop_loss"] = new_sl
                        position["trailing_activated"] = True
                        self.open_positions[position["symbol"]] = position
            else:
                unrealised_pct = (entry - ltp) / entry
                if unrealised_pct >= activate_pct:
                    new_sl = entry * (1 - buffer_pct)
                    if new_sl < position["stop_loss"]:
                        position["stop_loss"] = new_sl
                        position["trailing_activated"] = True
                        self.open_positions[position["symbol"]] = position

        sl     = position["stop_loss"]
        tp     = position["take_profit"]
        hit_sl = (action == "BUY"  and ltp <= sl) or (action == "SELL" and ltp >= sl)
        hit_tp = (action == "BUY"  and ltp >= tp) or (action == "SELL" and ltp <= tp)

        if hit_sl or hit_tp:
            pnl    = (ltp - entry) * qty if action == "BUY" else (entry - ltp) * qty
            reason = "take-profit" if hit_tp else "stop-loss"
            if position.get("trailing_activated") and hit_sl:
                reason = "trailing-stop"
            self.total_pnl += pnl
            self.closed_trades.append({**position, "exit_price": ltp,
                                        "pnl": pnl, "reason": reason})
            self.open_positions.pop(position["symbol"], None)
            log.info(f"[PAPER] CLOSED {position['symbol']} | PnL: {pnl:+.2f} | {reason}")
            return {"symbol": position["symbol"], "pnl": pnl,
                    "reason": reason, "exit_price": ltp}
        return None

    def _get_current_price(self, symbol: str) -> float | None:
        """
        Fetch latest price with a robust 3-step fallback:
        1. fast_info.lastPrice  (real-time, works during market hours)
        2. 5m recent history    (works after hours, more accurate than daily)
        3. 1d history           (last resort)
        """
        try:
            import yfinance as yf
            session   = _get_session()
            yf_symbol = self._to_yf_symbol(symbol)
            if yf_symbol is None:
                return None
            ticker = yf.Ticker(yf_symbol, session=session)

            # Step 1: real-time price
            try:
                ltp = ticker.fast_info.get("lastPrice")
                if ltp and float(ltp) > 0:
                    return float(ltp)
            except Exception:
                pass

            # Step 2: last 5m candle close (much better than daily for crypto)
            try:
                hist = ticker.history(period="1d", interval="5m", timeout=10)
                if not hist.empty:
                    return float(hist["Close"].iloc[-1])
            except Exception:
                pass

            # Step 3: daily fallback
            try:
                hist = ticker.history(period="5d", interval="1d", timeout=10)
                if not hist.empty:
                    return float(hist["Close"].iloc[-1])
            except Exception:
                pass

            return None
        except Exception as e:
            log.debug(f"Price fetch error for {symbol}: {e}")
            return None

    def _close_position(self, position: dict):
        self.open_positions.pop(position["symbol"], None)

    def _to_yf_symbol(self, symbol: str) -> str:
        if self.market == "stocks":
            return f"{symbol}.NS"
        elif self.market == "crypto":
            if symbol.endswith("USDT"):
                base = symbol.replace("USDT", "")
                return f"{base}-USD"
        return symbol

    def summary(self) -> str:
        wins     = [t for t in self.closed_trades if t["pnl"] > 0]
        total    = len(self.closed_trades)
        win_rate = len(wins) / total * 100 if total > 0 else 0
        return (
            f"\n{'='*50}\n"
            f"PAPER TRADING SUMMARY ({self.market.upper()})\n"
            f"Total trades : {total}\n"
            f"Win rate     : {win_rate:.1f}%\n"
            f"Total P&L    : {self.total_pnl:+.2f}\n"
            f"Open         : {len(self.open_positions)}\n"
            f"{'='*50}"
        )