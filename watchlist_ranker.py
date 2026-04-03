"""
Watchlist Ranker
================
Scans ALL symbols from NSE + Binance, scores each one, and returns the
top N candidates ranked by signal quality.

Scoring factors:
- Confluence count (how many strategies agreed)
- Signal confidence
- Volume ratio (institutional interest)
- RSI distance from neutral (momentum room)
- ATR-normalised move size (risk/reward potential)

This replaces the "trade anything that passes threshold" approach with
"only trade the best N setups per cycle".
"""

import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

log = logging.getLogger("WatchlistRanker")


class WatchlistRanker:
    def __init__(self, config: dict, strategy_engine, broker_stocks, broker_crypto):
        self.cfg      = config
        self.engine   = strategy_engine
        self.brokers  = {"stocks": broker_stocks, "crypto": broker_crypto}
        self.top_n    = config.get("WATCHLIST_TOP_N", 5)
        self.workers  = config.get("THREAD_WORKERS", 5)

        # Strategy performance tracker — updated externally after each closed trade
        self.strategy_stats: dict[str, dict] = {}   # strategy → {wins, losses}

    def rank(self, market: str) -> list[dict]:
        """
        Scan all symbols in a market and return top N ranked signals.

        Returns list of dicts:
        {symbol, action, strategy, confidence, score, candles_summary, signal}
        """
        broker  = self.brokers[market]
        symbols = (self.cfg["STOCK_SYMBOLS"] if market == "stocks"
                   else self.cfg["CRYPTO_SYMBOLS"])
        min_vol = (self.cfg["MIN_AVG_VOLUME_STOCKS"] if market == "stocks"
                   else self.cfg["MIN_AVG_VOLUME_CRYPTO"])

        log.info(f"[Ranker] Scanning {len(symbols)} {market} symbols...")

        candidates = []

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(self._scan_symbol, symbol, broker, market, min_vol): symbol
                for symbol in symbols
            }
            for future in as_completed(futures):
                try:
                    results = future.result()
                    if results:
                        candidates.extend(results)
                except Exception as e:
                    log.debug(f"Ranker thread error: {e}")

        if not candidates:
            log.info(f"[Ranker] No candidates found for {market} this cycle.")
            return []

        # For spot-only crypto brokers (CoinDCX), drop SELL signals entirely.
        # SELL = shorting, impossible on spot with no existing position.
        # Exits are handled automatically by check_exit() when SL/TP is hit.
        spot_only_brokers = {"coindcx"}
        broker_name = self.cfg.get("CRYPTO_BROKER", "paper").lower()
        if market == "crypto" and broker_name in spot_only_brokers:
            before = len(candidates)
            candidates = [c for c in candidates if c["action"] == "BUY"]
            dropped = before - len(candidates)
            if dropped:
                log.info(f"[Ranker] Dropped {dropped} SELL signals (spot-only broker: {broker_name})")
            if not candidates:
                log.info("[Ranker] No BUY signals this cycle — market in downtrend. Waiting for reversal...")
                return []

        # Sort by composite score descending
        candidates.sort(key=lambda x: x["score"], reverse=True)
        top = candidates[:self.top_n]

        log.info(f"[Ranker] {market}: {len(candidates)} signals found, "
                 f"returning top {len(top)}")
        for c in top:
            log.info(f"  #{candidates.index(c)+1} {c['symbol']:15} "
                     f"{c['action']:4} score={c['score']:.3f} "
                     f"conf={c['confidence']:.0%} "
                     f"strategies={c['signal'].get('confluence_count', 1)}")

        return top

    def _scan_symbol(self, symbol: str, broker, market: str,
                     min_vol: float) -> list[dict]:
        """Fetch candles + run strategies for one symbol. Returns scored candidates."""
        try:
            import time, random
            time.sleep(random.uniform(0.05, 0.2))   # gentle rate limiting

            candles = broker.get_candles(
                symbol, self.cfg["CANDLE_TIMEFRAME"], limit=100
            )
            if candles is None or len(candles) < 50:
                return []

            # Liquidity filter
            avg_vol = candles["volume"].mean()
            if avg_vol < min_vol:
                return []

            # Price floor filter — skip micro-price coins that cause CoinDCX lot-size rejections.
            # With $15 capital, coins below $0.01 require qty > 1500 units which hits precision limits.
            # Coins below $0.001 are essentially untradeable at this capital level.
            last_price = float(candles["close"].iloc[-1])
            min_price = self.cfg.get("MIN_COIN_PRICE_USD", 0.005)
            if market == "crypto" and last_price < min_price:
                return []

            signals = self.engine.scan(symbol, candles, market)
            if not signals:
                return []

            results = []
            for signal in signals:
                if signal is None:
                    continue
                if signal["confidence"] < self.cfg.get("MIN_SIGNAL_CONFIDENCE", 0.65):
                    continue

                score = self._score_signal(signal, candles)
                candles_summary = self._summarise_candles(candles)

                results.append({
                    "symbol":          symbol,
                    "action":          signal["action"],
                    "strategy":        signal["strategy"],
                    "confidence":      signal["confidence"],
                    "score":           score,
                    "signal":          signal,
                    "candles":         candles,
                    "candles_summary": candles_summary,
                    "market":          market,
                })

            return results

        except Exception as e:
            log.debug(f"Ranker scan error for {symbol}: {e}")
            return []

    def _score_signal(self, signal: dict, df: pd.DataFrame) -> float:
        """
        Composite score: higher is better.
        Weights are intentionally simple — tune after paper trading.
        """
        score = 0.0

        # Base: signal confidence (0–0.95)
        score += signal["confidence"] * 0.4

        # Confluence bonus (each extra strategy adds 0.05, max 0.20)
        confluence = signal.get("confluence_count", 1)
        score += min((confluence - 1) * 0.05, 0.20)

        c = df.iloc[-1]

        # Volume surge (high volume = institutional interest)
        vol_ratio = c.get("vol_ratio", 1.0)
        if not pd.isna(vol_ratio):
            score += min(vol_ratio / 10.0, 0.15)

        # RSI room to move (neutral RSI = more room)
        rsi = c.get("rsi", 50)
        if not pd.isna(rsi):
            action = signal["action"]
            if action == "BUY":
                # Buy signal is better when RSI isn't already overbought
                rsi_room = max(0, (70 - rsi) / 70)
            else:
                rsi_room = max(0, (rsi - 30) / 70)
            score += rsi_room * 0.10

        # Strategy historical win rate bonus (if we have data)
        strat = signal["strategy"]
        stats = self.strategy_stats.get(strat)
        if stats and (stats["wins"] + stats["losses"]) >= 5:
            win_rate = stats["wins"] / (stats["wins"] + stats["losses"])
            score += win_rate * 0.15

        return round(score, 4)

    def _summarise_candles(self, df: pd.DataFrame) -> dict:
        """Extract key indicator values for the AI filter prompt."""
        c   = df.iloc[-1]
        p5  = df.iloc[-6] if len(df) >= 6 else df.iloc[0]

        price_change_5 = ((c["close"] - p5["close"]) / p5["close"] * 100
                          if p5["close"] > 0 else 0)

        macd_trend = "rising" if c.get("macd_hist", 0) > df.iloc[-2].get("macd_hist", 0) \
                     else "falling"

        vs_ema50 = ((c["close"] - c.get("ema50", c["close"])) /
                    c.get("ema50", c["close"]) * 100
                    if c.get("ema50") else 0)

        return {
            "current_price": round(float(c["close"]), 4),
            "price_change_5": round(price_change_5, 2),
            "rsi":            round(float(c.get("rsi", 50)), 1),
            "macd_trend":     macd_trend,
            "vol_ratio":      round(float(c.get("vol_ratio", 1.0)), 2),
            "vs_ema50":       round(vs_ema50, 2),
            "atr":            round(float(c.get("atr", 0)), 4),
            "bb_pct":         round(float(c.get("bb_pct", 0.5)), 3),
        }

    def update_strategy_stats(self, strategy: str, won: bool):
        """Call this after each closed trade to track strategy win rates."""
        if strategy not in self.strategy_stats:
            self.strategy_stats[strategy] = {"wins": 0, "losses": 0}
        if won:
            self.strategy_stats[strategy]["wins"] += 1
        else:
            self.strategy_stats[strategy]["losses"] += 1
