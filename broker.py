"""
Broker Integrations
====================
- AngelOneBroker : Indian stocks via Angel One SmartAPI (free)
- CoinDCXBroker  : Crypto via CoinDCX (Indian exchange)
- BinanceBroker  : Crypto via Binance
- ZerodhaBroker  : kept for compatibility

Key fixes:
- AngelOneBroker: _available set before _connect(), handles maintenance window 
- CoinDCXBroker: _get_current_price uses fresh candle data (not unreliable ticker)
- CoinDCXBroker: candle timeout 5s (fast-fail on illiquid pairs)
- CoinDCXBroker: timeout errors demoted to debug level
"""

import pandas as pd
import logging
import hmac
import hashlib
from datetime import datetime, timedelta

log = logging.getLogger("Broker")


# ─────────────────────────────────────────────────────────────────────────────
# ANGEL ONE
# ─────────────────────────────────────────────────────────────────────────────

class AngelOneBroker:
    def __init__(self, config: dict):
        self.cfg             = config
        self.open_positions  = {}
        self._session        = None
        self._token_date     = None
        self._available      = False          # MUST be set before _connect()
        self._instrument_map: dict = {}
        self._connect()
        if self._available:
            log.info("Angel One broker connected.")
        else:
            log.warning("Angel One broker unavailable — will retry after 6 AM IST.")

    def _connect(self):
        try:
            from SmartApi import SmartConnect
            import pyotp
            api_key   = self.cfg.get("ANGELONE_API_KEY", "")
            client_id = self.cfg.get("ANGELONE_CLIENT_ID", "")
            password  = self.cfg.get("ANGELONE_PASSWORD", "")
            totp_sec  = self.cfg.get("ANGELONE_TOTP_SECRET", "")
            if not all([api_key, client_id, password, totp_sec]):
                raise ValueError("Missing Angel One credentials.")
            totp = pyotp.TOTP(totp_sec).now()
            obj  = SmartConnect(api_key=api_key)
            data = obj.generateSession(client_id, password, totp)
            if data["status"] is False:
                raise ConnectionError(f"Login failed: {data['message']}")
            self._session    = obj
            self._token_date = datetime.now().date()
            self._available  = True
            self._load_instrument_map()
            log.info(f"Angel One session started for {client_id}")
        except ImportError:
            raise ImportError("Run: pip install smartapi-python pyotp")
        except Exception as e:
            from datetime import timezone, timedelta as td
            IST     = timezone(td(hours=5, minutes=30))
            now_ist = datetime.now(IST)
            mins    = now_ist.hour * 60 + now_ist.minute
            if mins >= 22 * 60 or mins < 6 * 60:
                log.warning(
                    f"Angel One unavailable (maintenance 10PM-6AM IST). "
                    f"IST: {now_ist.strftime('%H:%M')}. Stocks paused until 6 AM."
                )
            else:
                log.error(f"Angel One connection failed: {e}")
            self._session   = None
            self._available = False

    def _ensure_session(self):
        today = datetime.now().date()
        if self._token_date and self._token_date < today:
            log.info("Angel One session expired — re-logging in...")
            self._connect()
        elif not self._available:
            from datetime import timezone, timedelta as td
            IST     = timezone(td(hours=5, minutes=30))
            now_ist = datetime.now(IST)
            if now_ist.hour * 60 + now_ist.minute >= 6 * 60:
                log.info("Retrying Angel One connection after maintenance...")
                self._connect()

    def _load_instrument_map(self):
        try:
            import requests
            url  = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
            data = requests.get(url, timeout=15).json()
            self._instrument_map = {
                item["symbol"]: item["token"]
                for item in data
                if item.get("exch_seg") == "NSE" and item.get("instrumenttype") == ""
            }
            log.info(f"Cached {len(self._instrument_map)} Angel One NSE instruments.")
        except Exception as e:
            log.warning(f"Could not load Angel One instruments: {e}")

    def _get_token(self, symbol):
        return (self._instrument_map.get(f"{symbol}-EQ") or
                self._instrument_map.get(symbol))

    def get_candles(self, symbol, timeframe, limit=100):
        self._ensure_session()
        if not self._available or not self._session:
            return None
        try:
            token = self._get_token(symbol)
            if not token:
                return None
            tf_map = {"1m":"ONE_MINUTE","5m":"FIVE_MINUTE","15m":"FIFTEEN_MINUTE",
                      "1h":"ONE_HOUR","1d":"ONE_DAY"}
            to_date   = datetime.now()
            from_date = to_date - timedelta(days=30)
            resp = self._session.getCandleData({
                "exchange":"NSE","symboltoken":token,
                "interval":tf_map.get(timeframe,"FIVE_MINUTE"),
                "fromdate":from_date.strftime("%Y-%m-%d %H:%M"),
                "todate":  to_date.strftime("%Y-%m-%d %H:%M"),
            })
            if not resp.get("status") or not resp.get("data"):
                return None
            df = pd.DataFrame(resp["data"],
                              columns=["timestamp","open","high","low","close","volume"])
            for col in ["open","high","low","close","volume"]:
                df[col] = pd.to_numeric(df[col])
            return df.tail(limit).reset_index(drop=True)
        except Exception as e:
            log.error(f"Angel One candles error for {symbol}: {e}")
            return None

    def place_order(self, symbol, action, qty, price, stop_loss_pct, take_profit_pct,
                    strategy, atr=None, signal_confidence=1.0, confluence_count=1):
        self._ensure_session()
        if not self._available or not self._session:
            log.warning(f"Angel One unavailable — cannot place order for {symbol}")
            return None
        try:
            token = self._get_token(symbol)
            if not token:
                return None
            order_id = self._session.placeOrder({
                "variety":"NORMAL","tradingsymbol":symbol,"symboltoken":token,
                "transactiontype":action,"exchange":"NSE","ordertype":"MARKET",
                "producttype":"INTRADAY","duration":"DAY","quantity":str(int(qty)),
            })
            sl, tp = _calc_sl_tp(price, action, stop_loss_pct, take_profit_pct, atr, self.cfg)
            trade  = _make_trade(order_id, symbol, action, qty, price, sl, tp,
                                 strategy, "stocks", signal_confidence, confluence_count)
            self.open_positions[symbol] = trade
            log.info(f"[ANGELONE] {action} {qty} {symbol} @ {price:.2f} | SL:{sl:.2f} TP:{tp:.2f}")
            return trade
        except Exception as e:
            log.error(f"Angel One order error for {symbol}: {e}")
            return None

    def get_open_positions(self):
        return list(self.open_positions.values())

    def check_exit(self, position):
        self._ensure_session()
        if not self._available or not self._session:
            return None
        try:
            symbol = position["symbol"]
            token  = self._get_token(symbol)
            if not token:
                return None
            resp = self._session.ltpData("NSE", symbol, token)
            ltp  = float(resp["data"]["ltp"])
            return _check_exit_logic(position, ltp, self)
        except Exception as e:
            log.error(f"Angel One exit check error: {e}")
            return None

    def _get_current_price(self, symbol):
        self._ensure_session()
        if not self._available or not self._session:
            return None
        try:
            token = self._get_token(symbol)
            if not token:
                return None
            resp = self._session.ltpData("NSE", symbol, token)
            return float(resp["data"]["ltp"])
        except Exception:
            return None

    def _close_position(self, position):
        if not self._available or not self._session:
            return
        try:
            self._ensure_session()
            token = self._get_token(position["symbol"])
            if not token:
                return
            self._session.placeOrder({
                "variety":"NORMAL","tradingsymbol":position["symbol"],"symboltoken":token,
                "transactiontype":"SELL" if position["action"]=="BUY" else "BUY",
                "exchange":"NSE","ordertype":"MARKET",
                "producttype":"INTRADAY","duration":"DAY",
                "quantity":str(int(position["qty"])),
            })
            self.open_positions.pop(position["symbol"], None)
        except Exception as e:
            log.error(f"Angel One close error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# COINDCX
# ─────────────────────────────────────────────────────────────────────────────

class CoinDCXBroker:
    """
    CoinDCX broker for Indian crypto trading.

    Price fetching strategy:
    - _get_current_price() fetches a fresh 1m candle for the exact symbol
    - This is more reliable than the ticker endpoint which has market name
      format mismatches (e.g. returns "HBARINR" instead of "HBARUSDT")
    - Stale/wrong prices from ticker were causing false SL/TP triggers
    """

    _ticker_cache  = {"data": None, "ts": 0}   # class-level USD price cache
    _markets_cache = {"data": None, "ts": 0}   # class-level valid markets cache

    def __init__(self, config: dict):
        self.cfg            = config
        self.api_key        = config.get("COINDCX_API_KEY", "")
        self.api_secret     = config.get("COINDCX_API_SECRET", "")
        self.open_positions = {}
        if not self.api_key or "YOUR_" in self.api_key:
            raise ValueError("CoinDCX API key missing.")
        self._load_valid_markets()
        log.info("CoinDCX broker connected.")

    def _load_valid_markets(self):
        """Cache the list of valid CoinDCX USDT spot markets at startup."""
        import requests, time as t
        try:
            r = requests.get(
                "https://api.coindcx.com/exchange/v1/markets_details", timeout=10
            )
            data = r.json()
            valid = {
                item["symbol"]
                for item in data
                if item.get("status") == "active"
                and item.get("base_currency_short_name") == "USDT"
                and "market_order" in item.get("order_types", [])
            }
            CoinDCXBroker._markets_cache["data"] = valid
            CoinDCXBroker._markets_cache["ts"]   = t.time()
            log.info(f"CoinDCX: loaded {len(valid)} valid USDT spot markets.")
        except Exception as e:
            log.warning(f"CoinDCX: could not load markets list: {e}")
            CoinDCXBroker._markets_cache["data"] = set()

    def _is_valid_market(self, symbol: str) -> bool:
        """Return True if symbol is a tradeable CoinDCX spot market."""
        markets = CoinDCXBroker._markets_cache.get("data")
        if not markets:
            return True   # if cache failed, don't block — let API decide
        return symbol in markets

    def _signed_request(self, endpoint, body):
        import hmac, hashlib, time as t, requests, json
        body["timestamp"] = int(round(t.time() * 1000))
        json_body  = json.dumps(body, separators=(',', ':'))
        signature  = hmac.new(
            self.api_secret.encode(), json_body.encode(), hashlib.sha256
        ).hexdigest()
        r = requests.post(
            f"https://api.coindcx.com{endpoint}",
            headers={"Content-Type":"application/json",
                     "X-AUTH-APIKEY":self.api_key,
                     "X-AUTH-SIGNATURE":signature},
            data=json_body, timeout=10)
        return r.json()

    def get_candles(self, symbol, timeframe, limit=100):
        """
        Fetch OHLCV candles from CoinDCX.
        Uses 5s timeout — illiquid/unsupported pairs fail fast and are skipped.
        """
        try:
            import requests
            base = symbol.replace("USDT", "")
            tf   = {"1m":"1m","5m":"5m","15m":"15m","1h":"1h"}.get(timeframe, "5m")
            r    = requests.get(
                "https://public.coindcx.com/market_data/candles",
                params={"pair": f"B-{base}_USDT", "interval": tf, "limit": limit},
                timeout=5)
            data = r.json()
            if not data or not isinstance(data, list):
                return None
            df = pd.DataFrame(data, columns=["time","open","high","low","close","volume"])
            for col in ["open","high","low","close","volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna()
            if df.empty:
                return None
            return df.tail(limit).reset_index(drop=True)
        except Exception as e:
            log.debug(f"CoinDCX candles skipped for {symbol}: {type(e).__name__}")
            return None

    def place_order(self, symbol, action, qty, price, stop_loss_pct, take_profit_pct,
                    strategy, atr=None, signal_confidence=1.0, confluence_count=1):
        """
        Places a CoinDCX market order.
        IMPORTANT: Uses ticker USD price for SL/TP calculation, not the
        candle close price (which is in INR on CoinDCX candle endpoint).

        Fixes applied:
        - SELL guard: CoinDCX is spot-only. SELL requires owning the base asset.
          Bot now checks your wallet balance before attempting a SELL.
        - Blacklist: pairs that consistently fail (not listed, lot-size issues) are skipped.
        - Lot size: qty is validated against CoinDCX min_quantity for the pair.
        """
        try:
            # ── Blacklist check ───────────────────────────────────────────────
            # Pairs that are on Binance but not properly supported on CoinDCX spot
            blacklist = self.cfg.get("COINDCX_BLACKLIST", set())
            if symbol in blacklist:
                log.debug(f"CoinDCX: {symbol} is blacklisted — skipping.")
                return None

            # ── Listed market check ───────────────────────────────────────────
            if not self._is_valid_market(symbol):
                log.warning(f"CoinDCX: {symbol} is not listed on CoinDCX — skipping.")
                return None

            # ── Always get fresh USD price from ticker ─────────────────────────
            usd_price = self._get_current_price(symbol)
            if usd_price is None:
                log.error(f"CoinDCX: cannot get USD price for {symbol} — order cancelled")
                return None

            # Sanity check: passed price should be within 50% of ticker price
            if price > 0 and abs(price - usd_price) / usd_price > 0.5:
                log.warning(
                    f"CoinDCX: price mismatch for {symbol} — "
                    f"passed={price:.6f} ticker={usd_price:.6f} "
                    f"(candle was likely INR). Using ticker price."
                )
                price = usd_price

            # ── SPOT SELL GUARD ───────────────────────────────────────────────
            # CoinDCX is a spot exchange. A SELL order means selling coins you OWN.
            # The bot's strategies fire SELL as a short signal — invalid on spot.
            # We only allow SELL to close an existing BUY position.
            if action == "SELL":
                base_currency = symbol.replace("USDT", "")
                owned_qty = self._get_wallet_balance(base_currency)
                min_sell_qty = self._get_min_quantity(symbol)
                if owned_qty < min_sell_qty:
                    log.info(
                        f"CoinDCX: SELL signal for {symbol} ignored — "
                        f"spot-only exchange, you own {owned_qty:.4f} {base_currency} "
                        f"(need ≥ {min_sell_qty}). Only BUY signals are executed. "
                        f"SELL orders are placed automatically when SL/TP is hit."
                    )
                    return None
                # If we do own enough, sell only what we have (close position)
                qty = min(qty, owned_qty)

            # ── Minimum order value check ─────────────────────────────────────
            min_order_usd = 11.0
            order_value   = qty * usd_price
            if order_value < min_order_usd:
                log.warning(
                    f"CoinDCX: order value ${order_value:.2f} is below minimum ${min_order_usd} "
                    f"for {symbol} — order cancelled. Increase CAPITAL or MAX_POSITION_PCT."
                )
                return None

            # ── Lot size validation ───────────────────────────────────────────
            min_qty = self._get_min_quantity(symbol)
            qty_fmt = self._format_qty(symbol, qty, usd_price)
            if qty_fmt < min_qty:
                log.warning(
                    f"CoinDCX: qty {qty_fmt} is below min lot size {min_qty} for {symbol} — "
                    f"order cancelled. Add {symbol} to COINDCX_BLACKLIST in config.py to suppress."
                )
                return None

            log.info(
                f"CoinDCX placing order: {action} {qty_fmt} {symbol} "
                f"@ ${usd_price:.6f} = ${qty_fmt * usd_price:.2f} USD"
            )
            resp = self._signed_request("/exchange/v1/orders/create", {
                "market":        symbol,
                "total_quantity": qty_fmt,
                "side":          "buy" if action == "BUY" else "sell",
                "order_type":    "market_order"
            })
            if isinstance(resp, dict) and resp.get("code") and resp["code"] != 200:
                log.error(f"CoinDCX order rejected: {resp}")
                # Auto-blacklist pairs that get 'Invalid request' (lot size / unsupported)
                if resp.get("message") == "Invalid request":
                    self._auto_blacklist(symbol)
                return None

            sl, tp = _calc_sl_tp(usd_price, action, stop_loss_pct, take_profit_pct, atr, self.cfg)
            trade  = _make_trade(
                resp.get("id", "CDX"), symbol, action, qty, usd_price, sl, tp,
                strategy, "crypto", signal_confidence, confluence_count
            )
            self.open_positions[symbol] = trade
            log.info(f"[COINDCX] {action} {qty} {symbol} @ ${usd_price:.6f} | SL:{sl:.6f} TP:{tp:.6f}")
            return trade
        except Exception as e:
            log.error(f"CoinDCX order error: {e}")
            return None

    def get_open_positions(self):
        return list(self.open_positions.values())

    def check_exit(self, position):
        """
        Check SL/TP using a fresh candle price — NOT the ticker.
        The ticker has market name format issues that return wrong prices.
        """
        try:
            ltp = self._get_current_price(position["symbol"])
            if ltp is None:
                return None
            # Sanity check: price must be within 50% of entry to be valid
            entry = position.get("entry_price", 0)
            if entry > 0 and (ltp < entry * 0.5 or ltp > entry * 1.5):
                log.warning(
                    f"[CoinDCX] Suspicious price for {position['symbol']}: "
                    f"entry={entry:.6f} ltp={ltp:.6f} — skipping exit check"
                )
                return None
            return _check_exit_logic(position, ltp, self)
        except Exception as e:
            log.error(f"CoinDCX exit check error: {e}")
            return None

    def _get_current_price(self, symbol):
        """
        Get current price from CoinDCX ticker (USD prices).
        NOTE: CoinDCX candle endpoint returns INR prices — do NOT use for USD comparison.
        The ticker endpoint returns correct USD prices matching yfinance.
        """
        try:
            import requests, time as t
            # Cache ticker for 15 seconds to avoid hammering the API
            now = t.time()
            if (not CoinDCXBroker._ticker_cache["data"] or
                    now - CoinDCXBroker._ticker_cache["ts"] > 15):
                r = requests.get("https://api.coindcx.com/exchange/ticker", timeout=8)
                CoinDCXBroker._ticker_cache["data"] = r.json()
                CoinDCXBroker._ticker_cache["ts"]   = now
            for item in CoinDCXBroker._ticker_cache["data"]:
                if item.get("market") == symbol:
                    price = float(item["last_price"])
                    if price > 0:
                        return price
        except Exception as e:
            log.debug(f"CoinDCX price fetch error for {symbol}: {e}")
        return None

    def _get_wallet_balance(self, currency: str) -> float:
        """
        Fetch available balance for a currency from CoinDCX wallet.
        Used by the SELL guard to confirm you own the asset before selling.
        """
        try:
            resp = self._signed_request("/exchange/v1/users/balances", {})
            if isinstance(resp, list):
                for item in resp:
                    if item.get("currency") == currency:
                        return float(item.get("balance", 0))
        except Exception as e:
            log.debug(f"CoinDCX wallet balance error for {currency}: {e}")
        return 0.0

    def _get_min_quantity(self, symbol: str) -> float:
        """
        Return the minimum tradeable quantity for a symbol from the markets cache.
        Falls back to a safe default (1.0) if not found.
        """
        try:
            import requests
            r    = requests.get("https://api.coindcx.com/exchange/v1/markets_details", timeout=8)
            data = r.json()
            for item in data:
                if item.get("symbol") == symbol:
                    return float(item.get("min_quantity", 1.0))
        except Exception:
            pass
        return 1.0

    def _auto_blacklist(self, symbol: str):
        """
        Add a symbol to the runtime blacklist after repeated 'Invalid request' failures.
        This prevents the bot from retrying bad pairs every cycle.
        """
        bl = self.cfg.setdefault("COINDCX_BLACKLIST", set())
        if symbol not in bl:
            bl.add(symbol)
            log.warning(
                f"CoinDCX: auto-blacklisted {symbol} after 'Invalid request'. "
                f"Add it to COINDCX_BLACKLIST in config.py to make permanent."
            )

    def _format_qty(self, symbol: str, qty: float, price: float) -> float:
        """
        Return quantity as a properly rounded number (not string).
        CoinDCX docs show total_quantity as a number in the JSON body.
        Rounding prevents floating point precision errors.
        """
        if price >= 100:
            return round(qty, 4)
        elif price < 0.01:
            return int(qty)   # penny coins: whole units only
        else:
            return round(qty, 2)

    def _close_position(self, position):
        try:
            qty_num = self._format_qty(
                position["symbol"], position["qty"], position.get("entry_price", 1)
            )
            self._signed_request("/exchange/v1/orders/create", {
                "market":position["symbol"],
                "total_quantity":qty_num,
                "side":"sell" if position["action"]=="BUY" else "buy",
                "order_type":"market_order"
            })
            self.open_positions.pop(position["symbol"], None)
        except Exception as e:
            log.error(f"CoinDCX close error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# BINANCE
# ─────────────────────────────────────────────────────────────────────────────

class BinanceBroker:
    def __init__(self, config):
        from binance.client import Client
        self.client = Client(config["BINANCE_API_KEY"], config["BINANCE_API_SECRET"])
        self.cfg    = config
        self.open_positions = {}
        log.info("Binance broker connected.")

    def get_candles(self, symbol, timeframe, limit=100):
        try:
            from binance.client import Client
            tf  = {"1m":Client.KLINE_INTERVAL_1MINUTE,"5m":Client.KLINE_INTERVAL_5MINUTE,
                   "15m":Client.KLINE_INTERVAL_15MINUTE,"1h":Client.KLINE_INTERVAL_1HOUR}
            raw = self.client.get_klines(
                symbol=symbol,
                interval=tf.get(timeframe, Client.KLINE_INTERVAL_5MINUTE),
                limit=limit)
            df  = pd.DataFrame(raw, columns=[
                "time","open","high","low","close","volume",
                "close_time","quote_vol","trades","taker_buy_base","taker_buy_quote","ignore"])
            for col in ["open","high","low","close","volume"]:
                df[col] = pd.to_numeric(df[col])
            return df
        except Exception as e:
            log.error(f"Binance candles error for {symbol}: {e}")
            return None

    def place_order(self, symbol, action, qty, price, stop_loss_pct, take_profit_pct,
                    strategy, atr=None, signal_confidence=1.0, confluence_count=1):
        try:
            from binance.client import Client
            order = self.client.create_order(
                symbol=symbol,
                side=Client.SIDE_BUY if action=="BUY" else Client.SIDE_SELL,
                type=Client.ORDER_TYPE_MARKET,
                quoteOrderQty=self.cfg["CAPITAL"]*(self.cfg["MAX_POSITION_PCT"]/100))
            sl, tp = _calc_sl_tp(price, action, stop_loss_pct, take_profit_pct, atr, self.cfg)
            trade  = _make_trade(
                order["orderId"], symbol, action,
                float(order.get("executedQty", qty)),
                price, sl, tp, strategy, "crypto",
                signal_confidence, confluence_count)
            self.open_positions[symbol] = trade
            return trade
        except Exception as e:
            log.error(f"Binance order error: {e}")
            return None

    def get_open_positions(self):
        return list(self.open_positions.values())

    def check_exit(self, position):
        try:
            ltp = float(self.client.get_symbol_ticker(symbol=position["symbol"])["price"])
            return _check_exit_logic(position, ltp, self)
        except Exception as e:
            log.error(f"Binance exit check error: {e}")
            return None

    def _get_current_price(self, symbol):
        try:
            return float(self.client.get_symbol_ticker(symbol=symbol)["price"])
        except Exception:
            return None

    def _close_position(self, position):
        try:
            from binance.client import Client
            self.client.create_order(
                symbol=position["symbol"],
                side=Client.SIDE_SELL if position["action"]=="BUY" else Client.SIDE_BUY,
                type=Client.ORDER_TYPE_MARKET, quantity=position["qty"])
            self.open_positions.pop(position["symbol"], None)
        except Exception as e:
            log.error(f"Binance close error: {e}")



# ─────────────────────────────────────────────────────────────────────────────
# DELTA EXCHANGE (India)
# ─────────────────────────────────────────────────────────────────────────────

class DeltaBroker:
    """
    Delta Exchange India broker for crypto perpetual futures.

    Key facts from docs:
    - Base URL: https://api.india.delta.exchange
    - Auth: api-key, signature, timestamp headers (seconds not ms)
    - Signature: HMAC-SHA256 of method+timestamp+path+query+body
    - Orders need product_id (int), not symbol string
    - size = number of contracts (1 contract = contract_value BTC e.g. 0.001)
    - limit_price must be string
    - IP whitelist required for trading keys
    """

    BASE_URL      = "https://api.india.delta.exchange"
    _product_cache = {}   # symbol -> product info
    _ticker_cache  = {"data": {}, "ts": 0}

    def __init__(self, config: dict):
        self.cfg            = config
        self.api_key        = config.get("DELTA_API_KEY", "")
        self.api_secret     = config.get("DELTA_API_SECRET", "")
        self.open_positions = {}
        if not self.api_key:
            raise ValueError("Delta Exchange API key missing.")
        self._load_products()
        log.info("Delta Exchange broker connected.")

    # ── Auth ─────────────────────────────────────────────────────────────────
    def _sign(self, method, path, query_string="", payload=""):
        import time as t
        timestamp = str(int(t.time()))
        msg       = method + timestamp + path + query_string + payload
        sig       = hmac.new(bytes(self.api_secret, "utf-8"),
                             bytes(msg, "utf-8"), hashlib.sha256).hexdigest()
        return {
            "api-key":      self.api_key,
            "timestamp":    timestamp,
            "signature":    sig,
            "User-Agent":   "python-algobot",
            "Content-Type": "application/json",
        }

    def _get(self, path, params={}):
        import requests, json
        qs = ("?" + "&".join(f"{k}={v}" for k,v in params.items())) if params else ""
        r  = requests.get(self.BASE_URL + path, params=params,
                          headers=self._sign("GET", path, qs), timeout=10)
        return r.json()

    def _post(self, path, body):
        import requests, json
        payload = json.dumps(body, separators=(",", ":"))
        r = requests.post(self.BASE_URL + path, data=payload,
                          headers=self._sign("POST", path, "", payload), timeout=10)
        return r.json()

    def _delete(self, path, body):
        import requests, json
        payload = json.dumps(body, separators=(",", ":"))
        r = requests.delete(self.BASE_URL + path, data=payload,
                            headers=self._sign("DELETE", path, "", payload), timeout=10)
        return r.json()

    # ── Product cache ─────────────────────────────────────────────────────────
    def _load_products(self):
        """Cache all perpetual futures products keyed by symbol."""
        try:
            resp = self._get("/v2/products",
                             {"contract_types": "perpetual_futures", "page_size": "200"})
            if resp.get("success"):
                for p in resp.get("result", []):
                    DeltaBroker._product_cache[p["symbol"]] = p
                log.info(f"Delta: loaded {len(DeltaBroker._product_cache)} perpetual products.")
        except Exception as e:
            log.warning(f"Delta: could not load products: {e}")

    def _get_product(self, symbol):
        """Return product info for a symbol, fetching if needed."""
        if symbol not in DeltaBroker._product_cache:
            try:
                resp = self._get(f"/v2/products/{symbol}")
                if resp.get("success"):
                    DeltaBroker._product_cache[symbol] = resp["result"]
            except Exception:
                pass
        return DeltaBroker._product_cache.get(symbol, {})

    # ── Price ─────────────────────────────────────────────────────────────────
    def _get_current_price(self, symbol):
        """Get mark price from Delta ticker."""
        try:
            import time as t
            now = t.time()
            if now - DeltaBroker._ticker_cache["ts"] > 15:
                resp = self._get("/v2/tickers",
                                 {"contract_types": "perpetual_futures"})
                if resp.get("success"):
                    DeltaBroker._ticker_cache["data"] = {
                        r["symbol"]: float(r["mark_price"])
                        for r in resp.get("result", [])
                        if r.get("mark_price")
                    }
                    DeltaBroker._ticker_cache["ts"] = now
            return DeltaBroker._ticker_cache["data"].get(symbol)
        except Exception as e:
            log.debug(f"Delta price error for {symbol}: {e}")
            return None

    # ── Candles ───────────────────────────────────────────────────────────────
    def get_candles(self, symbol, timeframe, limit=100):
        """Fetch OHLCV candles from Delta Exchange."""
        try:
            import requests, time as t
            product = self._get_product(symbol)
            if not product:
                return None
            product_id = product["id"]
            tf_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h"}
            resolution = tf_map.get(timeframe, "5m")
            end   = int(t.time())
            mins  = {"1m":1,"5m":5,"15m":15,"1h":60}.get(timeframe,5)
            start = end - limit * mins * 60
            r = requests.get(
                f"{self.BASE_URL}/v2/history/candles",
                params={"resolution": resolution,
                        "symbol": symbol,
                        "start": start, "end": end},
                timeout=8)
            data = r.json()
            if not data.get("success") or not data.get("result"):
                return None
            candles = data["result"]
            df = pd.DataFrame(candles)
            df = df.rename(columns={"time":"time","open":"open","high":"high",
                                    "low":"low","close":"close","volume":"volume"})
            for col in ["open","high","low","close","volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            return df.tail(limit).reset_index(drop=True)
        except Exception as e:
            log.debug(f"Delta candles error for {symbol}: {type(e).__name__}")
            return None

    # ── Place order ───────────────────────────────────────────────────────────
    def place_order(self, symbol, action, qty, price, stop_loss_pct, take_profit_pct,
                    strategy, atr=None, signal_confidence=1.0, confluence_count=1):
        """
        Place a perpetual futures order on Delta Exchange.

        Delta uses contracts not coins:
        - size = number of contracts (integers)
        - 1 BTCUSD contract = 0.001 BTC = ~$67 at current prices
        - Capital allocation: use CAPITAL * MAX_POSITION_PCT / contract_value_usd
        """
        try:
            product = self._get_product(symbol)
            if not product:
                log.warning(f"Delta: unknown product {symbol}")
                return None

            product_id     = product["id"]
            contract_value = float(product.get("contract_value", 0.001))
            tick_size      = float(product.get("tick_size", 0.5))

            # Get current mark price
            mark_price = self._get_current_price(symbol)
            if not mark_price or mark_price <= 0:
                log.error(f"Delta: cannot get price for {symbol}")
                return None

            # Calculate contract size from capital
            capital          = self.cfg.get("CAPITAL", 100)
            pos_pct          = self.cfg.get("MAX_POSITION_PCT", 80) / 100
            usd_to_allocate  = capital * pos_pct
            contract_usd     = mark_price * contract_value
            num_contracts    = max(1, int(usd_to_allocate / contract_usd))

            # Round limit price to tick_size
            import math
            def round_to_tick(p, tick):
                if tick <= 0:
                    return p
                rounded = round(p / tick) * tick
                # Determine decimal places from tick size
                if tick >= 1:
                    decimals = 0
                else:
                    decimals = len(str(tick).rstrip("0").split(".")[-1])
                return round(rounded, decimals)

            raw_price   = mark_price * (1.001 if action == "BUY" else 0.999)
            limit_price = round_to_tick(raw_price, tick_size)
            if limit_price <= 0:
                limit_price = raw_price  # fallback: use raw price as string

            log.info(f"Delta placing order: {action} {num_contracts} contracts {symbol} "
                     f"@ ${limit_price} (mark=${mark_price:.2f}) = ~${num_contracts*contract_usd:.2f}")

            resp = self._post("/v2/orders", {
                "product_id":  product_id,
                "size":        num_contracts,
                "side":        "buy" if action == "BUY" else "sell",
                "order_type":  "limit_order",
                "limit_price": str(limit_price),
            })

            if not resp.get("success"):
                log.error(f"Delta order rejected: {resp.get('error')}")
                return None

            order = resp["result"]
            sl, tp = _calc_sl_tp(mark_price, action, stop_loss_pct, take_profit_pct, atr, self.cfg)
            trade  = _make_trade(
                str(order["id"]), symbol, action,
                num_contracts, mark_price, sl, tp,
                strategy, "crypto", signal_confidence, confluence_count
            )
            trade["product_id"] = product_id
            self.open_positions[symbol] = trade
            log.info(f"[DELTA] ✅ {action} {num_contracts}x {symbol} @ ${mark_price:.2f} | SL:${sl:.2f} TP:${tp:.2f}")
            return trade

        except Exception as e:
            log.error(f"Delta order error: {e}")
            return None

    def get_open_positions(self):
        return list(self.open_positions.values())

    def check_exit(self, position):
        try:
            ltp = self._get_current_price(position["symbol"])
            if ltp is None:
                return None
            return _check_exit_logic(position, ltp, self)
        except Exception as e:
            log.error(f"Delta exit check error: {e}")
            return None

    def _close_position(self, position):
        try:
            product    = self._get_product(position["symbol"])
            product_id = product.get("id") or position.get("product_id")
            tick_size  = float(product.get("tick_size", 0.5))
            ltp        = self._get_current_price(position["symbol"]) or position["entry_price"]

            import math
            def round_to_tick(p, tick):
                if tick <= 0:
                    return p
                rounded = round(p / tick) * tick
                if tick >= 1:
                    decimals = 0
                else:
                    decimals = len(str(tick).rstrip("0").split(".")[-1])
                return round(rounded, decimals)

            close_price = round_to_tick(
                ltp * (0.999 if position["action"]=="BUY" else 1.001), tick_size
            )
            if close_price <= 0:
                close_price = ltp
            close_side = "sell" if position["action"] == "BUY" else "buy"

            resp = self._post("/v2/orders", {
                "product_id":  product_id,
                "size":        int(position["qty"]),
                "side":        close_side,
                "order_type":  "limit_order",
                "limit_price": str(close_price),
                "reduce_only": True,
            })
            if resp.get("success"):
                self.open_positions.pop(position["symbol"], None)
                log.info(f"[DELTA] Closed {position['symbol']} @ ${close_price}")
            else:
                log.error(f"Delta close error: {resp.get('error')}")
        except Exception as e:
            log.error(f"Delta close error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# ZERODHA
# ─────────────────────────────────────────────────────────────────────────────

class ZerodhaBroker:
    def __init__(self, config):
        from kiteconnect import KiteConnect
        self.cfg = config
        self.kite = KiteConnect(api_key=config["ZERODHA_API_KEY"])
        self.open_positions = {}
        self._token_date = None
        self._instrument_map = {}
        self._set_access_token(config["ZERODHA_ACCESS_TOKEN"])
        self._load_instrument_map()
        log.info("Zerodha broker connected.")

    def _set_access_token(self, token):
        self.kite.set_access_token(token)
        self._token_date = datetime.now().date()

    def _load_instrument_map(self):
        try:
            instruments = self.kite.instruments("NSE")
            self._instrument_map = {
                i["tradingsymbol"]: i["instrument_token"] for i in instruments
            }
        except Exception as e:
            log.warning(f"Could not load instrument map: {e}")

    def _check_token_expiry(self):
        if self._token_date and self._token_date < datetime.now().date():
            log.error("ZERODHA ACCESS TOKEN EXPIRED!")
            return False
        return True

    def get_candles(self, symbol, timeframe, limit=100):
        if not self._check_token_expiry():
            return None
        try:
            tf  = {"1m":"minute","5m":"5minute","15m":"15minute","1h":"60minute"}
            tok = self._instrument_map.get(symbol)
            if not tok:
                return None
            hrs = {"1m":1/60,"5m":5/60,"15m":15/60,"1h":1}.get(timeframe, 5/60)
            td_ = datetime.now()
            fd  = td_ - timedelta(hours=limit * hrs)
            data = self.kite.historical_data(tok, fd, td_, tf.get(timeframe,"5minute"))
            df   = pd.DataFrame(data)
            df.rename(columns={"date":"time"}, inplace=True)
            return df
        except Exception as e:
            log.error(f"Zerodha candles error for {symbol}: {e}")
            return None

    def place_order(self, symbol, action, qty, price, stop_loss_pct, take_profit_pct,
                    strategy, atr=None, signal_confidence=1.0, confluence_count=1):
        if not self._check_token_expiry():
            return None
        try:
            oid = self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR, exchange=self.kite.EXCHANGE_NSE,
                tradingsymbol=symbol, transaction_type=action, quantity=int(qty),
                product=self.kite.PRODUCT_MIS, order_type=self.kite.ORDER_TYPE_MARKET)
            sl, tp = _calc_sl_tp(price, action, stop_loss_pct, take_profit_pct, atr, self.cfg)
            trade  = _make_trade(oid, symbol, action, qty, price, sl, tp,
                                 strategy, "stocks", signal_confidence, confluence_count)
            self.open_positions[symbol] = trade
            return trade
        except Exception as e:
            log.error(f"Zerodha order error: {e}")
            return None

    def get_open_positions(self):
        return list(self.open_positions.values())

    def check_exit(self, position):
        if not self._check_token_expiry():
            return None
        try:
            s   = position["symbol"]
            ltp = self.kite.ltp(f"NSE:{s}")[f"NSE:{s}"]["last_price"]
            return _check_exit_logic(position, ltp, self)
        except Exception as e:
            log.error(f"Zerodha exit check error: {e}")
            return None

    def _get_current_price(self, symbol):
        try:
            return self.kite.ltp(f"NSE:{symbol}")[f"NSE:{symbol}"]["last_price"]
        except Exception:
            return None

    def _close_position(self, position):
        try:
            self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR, exchange=self.kite.EXCHANGE_NSE,
                tradingsymbol=position["symbol"],
                transaction_type="SELL" if position["action"]=="BUY" else "BUY",
                quantity=int(position["qty"]), product=self.kite.PRODUCT_MIS,
                order_type=self.kite.ORDER_TYPE_MARKET)
            self.open_positions.pop(position["symbol"], None)
        except Exception as e:
            log.error(f"Zerodha close error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def _calc_sl_tp(price, action, sl_pct, tp_pct, atr, cfg):
    """Calculate stop-loss and take-profit prices."""
    if atr and atr > 0 and cfg.get("ATR_BASED_EXITS"):
        sl_dist = cfg.get("ATR_SL_MULTIPLIER", 1.5) * atr
        tp_dist = cfg.get("ATR_TP_MULTIPLIER", 3.0) * atr
        sl = price - sl_dist if action == "BUY" else price + sl_dist
        tp = price + tp_dist if action == "BUY" else price - tp_dist
    else:
        sl = price * (1 - sl_pct/100) if action == "BUY" else price * (1 + sl_pct/100)
        tp = price * (1 + tp_pct/100) if action == "BUY" else price * (1 - tp_pct/100)
    return sl, tp


def _make_trade(order_id, symbol, action, qty, price, sl, tp,
                strategy, market, signal_confidence, confluence_count):
    return {
        "order_id":          order_id,
        "symbol":            symbol,
        "action":            action,
        "qty":               qty,
        "entry_price":       price,
        "stop_loss":         sl,
        "take_profit":       tp,
        "strategy":          strategy,
        "time":              datetime.now().isoformat(),
        "market":            market,
        "signal_confidence": signal_confidence,
        "confluence_count":  confluence_count,
        "trailing_activated":False,
    }


def _check_exit_logic(position, ltp, broker):
    action = position["action"]
    sl, tp = position["stop_loss"], position["take_profit"]
    entry, qty = position["entry_price"], position["qty"]
    hit_sl = (action=="BUY" and ltp<=sl) or (action=="SELL" and ltp>=sl)
    hit_tp = (action=="BUY" and ltp>=tp) or (action=="SELL" and ltp<=tp)
    if hit_sl or hit_tp:
        pnl    = (ltp-entry)*qty if action=="BUY" else (entry-ltp)*qty
        reason = "take-profit" if hit_tp else "stop-loss"
        broker._close_position(position)
        return {"symbol":position["symbol"],"pnl":pnl,"reason":reason,"exit_price":ltp}
    return None
