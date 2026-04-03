"""
AlgoBot — Expanded Strategy Engine
====================================
25+ strategies across all major categories:

TREND FOLLOWING       → Ride the momentum
MEAN REVERSION        → Buy dips, sell peaks
BREAKOUT              → Catch explosive moves
VOLATILITY            → Trade on volatility expansion/contraction
VOLUME-BASED          → Follow smart money
CANDLESTICK PATTERNS  → Classic chart patterns
CUSTOM ALGOS          → Proprietary edge
ARBITRAGE             → Correlation-based spread capture

Fix: statistical_arb signature corrected to match scan() interface.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from collections import defaultdict

log = logging.getLogger("Strategies")


class StrategyEngine:
    def __init__(self, config: dict):
        self.cfg     = config
        self.enabled = config.get("STRATEGIES", {})
        # Store last candles per symbol for stat arb pair comparison
        # Bounded to prevent memory growth across 2100+ symbols
        from collections import OrderedDict
        self._candle_cache: OrderedDict = OrderedDict()

    def scan(self, symbol: str, candles: pd.DataFrame, market: str) -> list:
        if len(candles) < 50:
            return []

        df = self._add_indicators(candles.copy())

        # Cache candles for stat arb (bounded — evict oldest if over 200 symbols)
        self._candle_cache[symbol] = df
        if len(self._candle_cache) > 200:
            self._candle_cache.popitem(last=False)

        strategy_map = {
            "macd_crossover":            self.macd_crossover,
            "ma_crossover":              self.ma_crossover,
            "ema_ribbon":                self.ema_ribbon,
            "supertrend":                self.supertrend,
            "adx_trend":                 self.adx_trend,
            "parabolic_sar":             self.parabolic_sar,
            "rsi_mean_reversion":        self.rsi_mean_reversion,
            "bollinger_bands":           self.bollinger_bands,
            "stochastic_reversal":       self.stochastic_reversal,
            "rsi_divergence":            self.rsi_divergence,
            "mean_reversion_zscore":     self.mean_reversion_zscore,
            "donchian_breakout":         self.donchian_breakout,
            "range_breakout":            self.range_breakout,
            "volatility_breakout":       self.volatility_breakout,
            "resistance_breakout":       self.resistance_breakout,
            "vwap_reversion":            self.vwap_reversion,
            "obv_trend":                 self.obv_trend,
            "volume_price_trend":        self.volume_price_trend,
            "accumulation_distribution": self.accumulation_distribution,
            "hammer_pattern":            self.hammer_pattern,
            "engulfing_pattern":         self.engulfing_pattern,
            "morning_evening_star":      self.morning_evening_star,
            "three_soldiers_crows":      self.three_soldiers_crows,
            "doji_reversal":             self.doji_reversal,
            "custom_trend_rsi":          self.custom_trend_rsi,
            "custom_momentum_volume":    self.custom_momentum_volume,
            "custom_squeeze_breakout":   self.custom_squeeze_breakout,
            "custom_multi_timeframe":    self.custom_multi_timeframe,
            "custom_smart_scalp":        self.custom_smart_scalp,
        }

        signals = []
        for name, func in strategy_map.items():
            if not self.enabled.get(name, False):
                continue
            try:
                s = func(symbol, df)
                if s:
                    signals.append(s)
            except Exception as e:
                log.debug(f"{name} error on {symbol}: {e}")

        # Statistical arb — handled separately (needs 2 symbols)
        if self.enabled.get("statistical_arb", False):
            arb_signals = self._run_stat_arb(symbol, df)
            signals.extend(arb_signals)

        return self._merge_signals(signals)

    # ── INDICATORS ────────────────────────────────────────────────────────────

    def _add_indicators(self, df):
        c = self.cfg
        close, high, low, vol = df["close"], df["high"], df["low"], df["volume"]

        # RSI
        d    = close.diff()
        gain = d.clip(lower=0).rolling(c.get("RSI_PERIOD", 14)).mean()
        loss = (-d.clip(upper=0)).rolling(c.get("RSI_PERIOD", 14)).mean()
        df["rsi"] = 100 - (100 / (1 + gain / loss.replace(0, np.nan)))

        # MACD
        ef = close.ewm(span=c.get("MACD_FAST", 12), adjust=False).mean()
        es = close.ewm(span=c.get("MACD_SLOW", 26), adjust=False).mean()
        df["macd"]        = ef - es
        df["macd_signal"] = df["macd"].ewm(span=c.get("MACD_SIGNAL", 9), adjust=False).mean()
        df["macd_hist"]   = df["macd"] - df["macd_signal"]

        # Bollinger Bands
        bm = close.rolling(c.get("BB_PERIOD", 20)).mean()
        bs = close.rolling(c.get("BB_PERIOD", 20)).std()
        df["bb_upper"] = bm + c.get("BB_STD", 2) * bs
        df["bb_lower"] = bm - c.get("BB_STD", 2) * bs
        df["bb_mid"]   = bm
        df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / bm
        df["bb_pct"]   = (close - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"])

        # EMAs and SMAs
        for p in [9, 20, 21, 50, 100, 200]:
            df[f"ema{p}"] = close.ewm(span=p, adjust=False).mean()
            df[f"sma{p}"] = close.rolling(p).mean()

        # Stochastic
        lo14 = low.rolling(14).min()
        hi14 = high.rolling(14).max()
        df["stoch_k"] = 100 * (close - lo14) / (hi14 - lo14 + 1e-9)
        df["stoch_d"] = df["stoch_k"].rolling(3).mean()

        # ATR
        tr = pd.concat([(high - low),
                         (high - close.shift()).abs(),
                         (low  - close.shift()).abs()], axis=1).max(axis=1)
        df["atr"] = tr.rolling(14).mean()

        # ADX
        df["adx"] = self._calc_adx(df)

        # Supertrend
        df["supertrend"], df["supertrend_dir"] = self._calc_supertrend(df)

        # Parabolic SAR
        df["psar"] = self._calc_psar(df)

        # Donchian
        df["dc_upper"] = high.rolling(20).max()
        df["dc_lower"] = low.rolling(20).min()

        # VWAP — rolling window (fixes the cumulative VWAP intraday anchoring bug)
        # Cumulative VWAP only makes sense when data starts at market open;
        # with 60d / 5m data the tail candles would show a stale wrong value.
        vwap_period = c.get("VWAP_PERIOD", 20)
        tp = (high + low + close) / 3
        df["vwap"] = (tp * vol).rolling(vwap_period).sum() / vol.rolling(vwap_period).sum()

        # OBV
        obv = [0]
        for i in range(1, len(df)):
            if close.iloc[i] > close.iloc[i-1]:
                obv.append(obv[-1] + vol.iloc[i])
            elif close.iloc[i] < close.iloc[i-1]:
                obv.append(obv[-1] - vol.iloc[i])
            else:
                obv.append(obv[-1])
        df["obv"]     = obv
        df["obv_ema"] = pd.Series(obv).ewm(span=20, adjust=False).mean().values

        # Volume ratio
        df["vol_sma"]   = vol.rolling(20).mean()
        df["vol_ratio"] = vol / df["vol_sma"]

        # Z-score
        df["zscore"] = (close - close.rolling(30).mean()) / close.rolling(30).std()

        # Keltner + Squeeze
        kc_mid   = close.ewm(span=20, adjust=False).mean()
        kc_upper = kc_mid + 1.5 * df["atr"]
        kc_lower = kc_mid - 1.5 * df["atr"]
        df["squeeze"] = (df["bb_upper"] < kc_upper) & (df["bb_lower"] > kc_lower)

        # Accumulation/Distribution
        clv       = ((close - low) - (high - close)) / (high - low + 1e-9)
        df["ad"]      = (clv * vol).cumsum()
        df["ad_ema"]  = df["ad"].ewm(span=10, adjust=False).mean()

        return df

    # ── TREND FOLLOWING ───────────────────────────────────────────────────────

    def macd_crossover(self, symbol, df):
        """MACD line crosses signal line + histogram momentum filter."""
        p, c = df.iloc[-2], df.iloc[-1]
        if p["macd"] < p["macd_signal"] and c["macd"] > c["macd_signal"] and c["macd_hist"] > 0:
            return self._sig(symbol, "BUY",  "macd_crossover", 0.70)
        if p["macd"] > p["macd_signal"] and c["macd"] < c["macd_signal"] and c["macd_hist"] < 0:
            return self._sig(symbol, "SELL", "macd_crossover", 0.70)

    def ma_crossover(self, symbol, df):
        """Golden cross (50 over 200) / Death cross. Rare but powerful."""
        if df["sma200"].isna().any():
            return None
        p, c = df.iloc[-2], df.iloc[-1]
        if p["sma50"] < p["sma200"] and c["sma50"] > c["sma200"]:
            return self._sig(symbol, "BUY",  "ma_crossover", 0.82)
        if p["sma50"] > p["sma200"] and c["sma50"] < c["sma200"]:
            return self._sig(symbol, "SELL", "ma_crossover", 0.82)

    def ema_ribbon(self, symbol, df):
        """All 5 EMAs (9,20,50,100,200) perfectly stacked = ultra-strong trend."""
        c, p = df.iloc[-1], df.iloc[-2]
        bull      = c["ema9"] > c["ema20"] > c["ema50"] > c["ema100"] > c["ema200"]
        bear      = c["ema9"] < c["ema20"] < c["ema50"] < c["ema100"] < c["ema200"]
        prev_bull = p["ema9"] > p["ema20"] > p["ema50"]
        prev_bear = p["ema9"] < p["ema20"] < p["ema50"]
        if bull and not prev_bull:
            return self._sig(symbol, "BUY",  "ema_ribbon", 0.85)
        if bear and not prev_bear:
            return self._sig(symbol, "SELL", "ema_ribbon", 0.85)

    def supertrend(self, symbol, df):
        """Supertrend direction flip — ATR-based dynamic support/resistance."""
        p, c = df.iloc[-2], df.iloc[-1]
        if pd.isna(c["supertrend_dir"]) or pd.isna(p["supertrend_dir"]):
            return None
        if p["supertrend_dir"] == -1 and c["supertrend_dir"] == 1:
            return self._sig(symbol, "BUY",  "supertrend", 0.78)
        if p["supertrend_dir"] == 1  and c["supertrend_dir"] == -1:
            return self._sig(symbol, "SELL", "supertrend", 0.78)

    def adx_trend(self, symbol, df):
        """ADX > 25 = strong trend. EMA50 gives direction."""
        c, p = df.iloc[-1], df.iloc[-2]
        if pd.isna(c["adx"]) or c["adx"] < 25:
            return None
        if c["adx"] > p["adx"] and c["close"] > c["ema50"]:
            return self._sig(symbol, "BUY",  "adx_trend", 0.72)
        if c["adx"] > p["adx"] and c["close"] < c["ema50"]:
            return self._sig(symbol, "SELL", "adx_trend", 0.72)

    def parabolic_sar(self, symbol, df):
        """Price crosses above/below SAR dots = trend reversal."""
        p, c = df.iloc[-2], df.iloc[-1]
        if pd.isna(c["psar"]):
            return None
        if p["close"] < p["psar"] and c["close"] > c["psar"]:
            return self._sig(symbol, "BUY",  "parabolic_sar", 0.68)
        if p["close"] > p["psar"] and c["close"] < c["psar"]:
            return self._sig(symbol, "SELL", "parabolic_sar", 0.68)

    # ── MEAN REVERSION ────────────────────────────────────────────────────────

    def rsi_mean_reversion(self, symbol, df):
        """RSI crosses back from oversold (30) or overbought (70)."""
        p, c = df.iloc[-2], df.iloc[-1]
        if p["rsi"] < 30 and c["rsi"] >= 30:
            return self._sig(symbol, "BUY",  "rsi_mean_reversion", 0.67)
        if p["rsi"] > 70 and c["rsi"] <= 70:
            return self._sig(symbol, "SELL", "rsi_mean_reversion", 0.67)

    def bollinger_bands(self, symbol, df):
        """Price bounces from Bollinger Band extremes."""
        p, c = df.iloc[-2], df.iloc[-1]
        if p["bb_pct"] <= 0.05 and c["close"] > p["close"]:
            return self._sig(symbol, "BUY",  "bollinger_bands", 0.62)
        if p["bb_pct"] >= 0.95 and c["close"] < p["close"]:
            return self._sig(symbol, "SELL", "bollinger_bands", 0.62)

    def stochastic_reversal(self, symbol, df):
        """Stochastic %K crosses %D in oversold/overbought zone."""
        p, c = df.iloc[-2], df.iloc[-1]
        if c["stoch_k"] < 20 and p["stoch_k"] < p["stoch_d"] and c["stoch_k"] > c["stoch_d"]:
            return self._sig(symbol, "BUY",  "stochastic_reversal", 0.70)
        if c["stoch_k"] > 80 and p["stoch_k"] > p["stoch_d"] and c["stoch_k"] < c["stoch_d"]:
            return self._sig(symbol, "SELL", "stochastic_reversal", 0.70)

    def rsi_divergence(self, symbol, df):
        """
        Bullish divergence: price lower low + RSI higher low.
        Bearish divergence: price higher high + RSI lower high.
        """
        if len(df) < 20:
            return None
        prices = df["close"].iloc[-15:].values
        rsis   = df["rsi"].iloc[-15:].values
        if prices[-1] < min(prices[:-5]) and rsis[-1] > min(rsis[:-5]) and rsis[-1] < 40:
            return self._sig(symbol, "BUY",  "rsi_divergence", 0.80)
        if prices[-1] > max(prices[:-5]) and rsis[-1] < max(rsis[:-5]) and rsis[-1] > 60:
            return self._sig(symbol, "SELL", "rsi_divergence", 0.80)

    def mean_reversion_zscore(self, symbol, df):
        """Z-score > 2.5 = too far from mean. Reversion expected."""
        c = df.iloc[-1]
        if pd.isna(c["zscore"]):
            return None
        if c["zscore"] < -2.5:
            return self._sig(symbol, "BUY",  "mean_reversion_zscore", 0.73)
        if c["zscore"] > 2.5:
            return self._sig(symbol, "SELL", "mean_reversion_zscore", 0.73)

    # ── BREAKOUT ──────────────────────────────────────────────────────────────

    def donchian_breakout(self, symbol, df):
        """Price breaks 20-period Donchian Channel high/low with volume."""
        c, p = df.iloc[-1], df.iloc[-2]
        hi = df["high"].iloc[-21:-1].max()
        lo = df["low"].iloc[-21:-1].min()
        if p["close"] < hi and c["close"] > hi and c["vol_ratio"] > 1.2:
            return self._sig(symbol, "BUY",  "donchian_breakout", 0.75)
        if p["close"] > lo and c["close"] < lo and c["vol_ratio"] > 1.2:
            return self._sig(symbol, "SELL", "donchian_breakout", 0.75)

    def range_breakout(self, symbol, df):
        """Compression (low ATR) followed by explosive breakout."""
        c = df.iloc[-1]
        if df["atr"].iloc[-10:].mean() >= df["atr"].iloc[-50:].mean() * 0.7:
            return None
        highest = df["high"].iloc[-15:].max()
        lowest  = df["low"].iloc[-15:].min()
        if c["close"] > highest * 0.998 and c["vol_ratio"] > 1.5:
            return self._sig(symbol, "BUY",  "range_breakout", 0.77)
        if c["close"] < lowest  * 1.002 and c["vol_ratio"] > 1.5:
            return self._sig(symbol, "SELL", "range_breakout", 0.77)

    def volatility_breakout(self, symbol, df):
        """Single candle > 1.5x ATR with heavy volume = volatility burst."""
        c = df.iloc[-1]
        if pd.isna(c["atr"]):
            return None
        if abs(c["close"] - c["open"]) > 1.5 * c["atr"] and c["vol_ratio"] > 2.0:
            if c["close"] > c["open"]:
                return self._sig(symbol, "BUY",  "volatility_breakout", 0.68)
            else:
                return self._sig(symbol, "SELL", "volatility_breakout", 0.68)

    def resistance_breakout(self, symbol, df):
        """Break above swing high resistance or below swing low support with volume."""
        if len(df) < 30:
            return None
        c          = df.iloc[-1]
        swing_high = df["high"].iloc[-30:-3].nlargest(3).mean()
        swing_low  = df["low"].iloc[-30:-3].nsmallest(3).mean()
        if c["close"] > swing_high * 1.005 and c["vol_ratio"] > 1.4:
            return self._sig(symbol, "BUY",  "resistance_breakout", 0.74)
        if c["close"] < swing_low  * 0.995 and c["vol_ratio"] > 1.4:
            return self._sig(symbol, "SELL", "resistance_breakout", 0.74)

    # ── VOLUME-BASED ──────────────────────────────────────────────────────────

    def vwap_reversion(self, symbol, df):
        """Price deviating > 1.5% from VWAP = reversion trade."""
        c = df.iloc[-1]
        if pd.isna(c["vwap"]):
            return None
        dev = (c["close"] - c["vwap"]) / c["vwap"] * 100
        if dev < -1.5 and c["rsi"] < 45:
            return self._sig(symbol, "BUY",  "vwap_reversion", 0.71)
        if dev >  1.5 and c["rsi"] > 55:
            return self._sig(symbol, "SELL", "vwap_reversion", 0.71)

    def obv_trend(self, symbol, df):
        """OBV crosses its EMA = smart money direction change."""
        c, p = df.iloc[-1], df.iloc[-2]
        if c["obv"] > c["obv_ema"] and p["obv"] < p["obv_ema"] and c["close"] > c["ema21"]:
            return self._sig(symbol, "BUY",  "obv_trend", 0.69)
        if c["obv"] < c["obv_ema"] and p["obv"] > p["obv_ema"] and c["close"] < c["ema21"]:
            return self._sig(symbol, "SELL", "obv_trend", 0.69)

    def volume_price_trend(self, symbol, df):
        """3x average volume candle in trend direction = institutional move."""
        c = df.iloc[-1]
        if c["vol_ratio"] < 3.0:
            return None
        if c["close"] > c["ema20"] and c["close"] > c["open"]:
            return self._sig(symbol, "BUY",  "volume_price_trend", 0.76)
        if c["close"] < c["ema20"] and c["close"] < c["open"]:
            return self._sig(symbol, "SELL", "volume_price_trend", 0.76)

    def accumulation_distribution(self, symbol, df):
        """A/D Line crosses EMA = accumulation or distribution."""
        c, p = df.iloc[-1], df.iloc[-2]
        if c["ad"] > c["ad_ema"] and p["ad"] < p["ad_ema"]:
            return self._sig(symbol, "BUY",  "accumulation_distribution", 0.66)
        if c["ad"] < c["ad_ema"] and p["ad"] > p["ad_ema"]:
            return self._sig(symbol, "SELL", "accumulation_distribution", 0.66)

    # ── CANDLESTICK PATTERNS ──────────────────────────────────────────────────

    def hammer_pattern(self, symbol, df):
        """Hammer / Shooting star — long wick = price rejection."""
        c    = df.iloc[-1]
        body = abs(c["close"] - c["open"])
        if body == 0:
            return None
        lo_wick = c["open"] - c["low"]  if c["close"] > c["open"] else c["close"] - c["low"]
        hi_wick = c["high"] - c["close"] if c["close"] > c["open"] else c["high"] - c["open"]
        if lo_wick >= 2 * body and hi_wick < body * 0.5 and c["rsi"] < 45:
            return self._sig(symbol, "BUY",  "hammer_pattern", 0.72)
        if hi_wick >= 2 * body and lo_wick < body * 0.5 and c["rsi"] > 55:
            return self._sig(symbol, "SELL", "hammer_pattern", 0.72)

    def engulfing_pattern(self, symbol, df):
        """Bullish/Bearish engulfing — one candle swallows the previous."""
        c, p = df.iloc[-1], df.iloc[-2]
        if (p["close"] < p["open"] and c["close"] > c["open"]
                and c["open"] < p["close"] and c["close"] > p["open"]):
            return self._sig(symbol, "BUY",  "engulfing_pattern", 0.76)
        if (p["close"] > p["open"] and c["close"] < c["open"]
                and c["open"] > p["close"] and c["close"] < p["open"]):
            return self._sig(symbol, "SELL", "engulfing_pattern", 0.76)

    def morning_evening_star(self, symbol, df):
        """3-candle reversal: big candle → small doji → confirming candle."""
        if len(df) < 3:
            return None
        c, p, pp = df.iloc[-1], df.iloc[-2], df.iloc[-3]
        b_pp = abs(pp["close"] - pp["open"])
        b_p  = abs(p["close"]  - p["open"])
        if (pp["close"] < pp["open"] and b_pp > 0.01 * pp["close"]
                and b_p < b_pp * 0.3 and c["close"] > c["open"]
                and c["close"] > pp["open"]):
            return self._sig(symbol, "BUY",  "morning_evening_star", 0.79)
        if (pp["close"] > pp["open"] and b_pp > 0.01 * pp["close"]
                and b_p < b_pp * 0.3 and c["close"] < c["open"]
                and c["close"] < pp["open"]):
            return self._sig(symbol, "SELL", "morning_evening_star", 0.79)

    def three_soldiers_crows(self, symbol, df):
        """3 consecutive large trending candles = strong momentum."""
        if len(df) < 3:
            return None
        c, p, pp  = df.iloc[-1], df.iloc[-2], df.iloc[-3]
        avg_body  = df["close"].iloc[-20:].std() * 0.5
        soldiers  = all(x["close"] > x["open"] and
                        abs(x["close"] - x["open"]) > avg_body for x in [pp, p, c])
        crows     = all(x["close"] < x["open"] and
                        abs(x["close"] - x["open"]) > avg_body for x in [pp, p, c])
        if soldiers:
            return self._sig(symbol, "BUY",  "three_soldiers_crows", 0.74)
        if crows:
            return self._sig(symbol, "SELL", "three_soldiers_crows", 0.74)

    def doji_reversal(self, symbol, df):
        """Doji (indecision candle) at RSI extreme = reversal signal."""
        c   = df.iloc[-1]
        rng = c["high"] - c["low"]
        if rng == 0:
            return None
        if abs(c["close"] - c["open"]) / rng < 0.1:
            if c["rsi"] < 35 and c["close"] < c["ema20"]:
                return self._sig(symbol, "BUY",  "doji_reversal", 0.65)
            if c["rsi"] > 65 and c["close"] > c["ema20"]:
                return self._sig(symbol, "SELL", "doji_reversal", 0.65)

    # ── CUSTOM ALGOS ─────────────────────────────────────────────────────────

    def custom_trend_rsi(self, symbol, df):
        """
        Custom Algo 1 — Trend + RSI + Volume.
        EMA trend + volume spike + 2 aligned candles + RSI in healthy zone.
        """
        c, p = df.iloc[-1], df.iloc[-2]
        if (c["close"] > c["ema21"] and c["vol_ratio"] > 1.3
                and c["close"] > c["open"] and p["close"] > p["open"]
                and 40 < c["rsi"] < 70):
            return self._sig(symbol, "BUY",  "custom_trend_rsi", 0.80)
        if (c["close"] < c["ema21"] and c["vol_ratio"] > 1.3
                and c["close"] < c["open"] and p["close"] < p["open"]
                and 30 < c["rsi"] < 60):
            return self._sig(symbol, "SELL", "custom_trend_rsi", 0.80)

    def custom_momentum_volume(self, symbol, df):
        """
        Custom Algo 2 — 4-way confluence: MACD + RSI + OBV + VWAP.
        All 4 must agree. Fewer signals, very high accuracy.
        """
        c, p    = df.iloc[-1], df.iloc[-2]
        macd_bull = c["macd"] > c["macd_signal"] and c["macd_hist"] > p["macd_hist"]
        macd_bear = c["macd"] < c["macd_signal"] and c["macd_hist"] < p["macd_hist"]
        if macd_bull and 50 < c["rsi"] < 68 and c["obv"] > c["obv_ema"] and c["close"] > c["vwap"]:
            return self._sig(symbol, "BUY",  "custom_momentum_volume", 0.84)
        if macd_bear and 32 < c["rsi"] < 50 and c["obv"] < c["obv_ema"] and c["close"] < c["vwap"]:
            return self._sig(symbol, "SELL", "custom_momentum_volume", 0.84)

    def custom_squeeze_breakout(self, symbol, df):
        """
        Custom Algo 3 — Bollinger Squeeze.
        BB inside Keltner = compressed energy. Release = explosive move.
        Direction confirmed by MACD + EMA.
        """
        c, p = df.iloc[-1], df.iloc[-2]
        if not (p["squeeze"] and not c["squeeze"]):
            return None
        if c["macd_hist"] > p["macd_hist"] and c["close"] > c["ema20"]:
            return self._sig(symbol, "BUY",  "custom_squeeze_breakout", 0.88)
        if c["macd_hist"] < p["macd_hist"] and c["close"] < c["ema20"]:
            return self._sig(symbol, "SELL", "custom_squeeze_breakout", 0.88)

    def custom_multi_timeframe(self, symbol, df):
        """
        Custom Algo 4 — 8-signal vote system.
        Trade only when 6+ of 8 signals agree. Highest quality, low frequency.
        """
        c, p = df.iloc[-1], df.iloc[-2]
        scores = {
            "bull": sum([
                c["close"] > c["ema50"],
                c["macd"]  > c["macd_signal"],
                c["rsi"]   > 50,
                c["close"] > c["vwap"],
                c["stoch_k"] > c["stoch_d"] and c["stoch_k"] < 80,
                c["obv"]   > c["obv_ema"],
                c["vol_ratio"] > 1.2 and c["close"] > c["open"],
                c["adx"]   > 20 and c["close"] > c["ema20"],
            ]),
            "bear": sum([
                c["close"] < c["ema50"],
                c["macd"]  < c["macd_signal"],
                c["rsi"]   < 50,
                c["close"] < c["vwap"],
                c["stoch_k"] < c["stoch_d"] and c["stoch_k"] > 20,
                c["obv"]   < c["obv_ema"],
                c["vol_ratio"] > 1.2 and c["close"] < c["open"],
                c["adx"]   > 20 and c["close"] < c["ema20"],
            ])
        }
        for side, action in [("bull", "BUY"), ("bear", "SELL")]:
            if scores[side] >= 6:
                conf = min(0.75 + (scores[side] - 6) * 0.05, 0.92)
                return self._sig(symbol, action, "custom_multi_timeframe", conf)

    def custom_smart_scalp(self, symbol, df):
        """
        Custom Algo 5 — Smart Scalp for 1m/5m crypto.
        BB squeeze + Stochastic flip in extreme zone + volume burst.
        """
        c, p     = df.iloc[-1], df.iloc[-2]
        bb_tight = c["bb_width"] < df["bb_width"].rolling(50).mean().iloc[-1] * 0.7
        if not bb_tight:
            return None
        if p["stoch_k"] < 20 and c["stoch_k"] > c["stoch_d"] and c["vol_ratio"] > 1.4:
            return self._sig(symbol, "BUY",  "custom_smart_scalp", 0.73)
        if p["stoch_k"] > 80 and c["stoch_k"] < c["stoch_d"] and c["vol_ratio"] > 1.4:
            return self._sig(symbol, "SELL", "custom_smart_scalp", 0.73)

    # ── INDICATOR CALCULATORS ─────────────────────────────────────────────────

    def _calc_adx(self, df, period=14):
        """Average Directional Index — measures trend strength."""
        hi, lo, cl = df["high"], df["low"], df["close"]
        pdm = hi.diff().clip(lower=0)
        ndm = (-lo.diff()).clip(lower=0)
        pdm[pdm < ndm] = 0
        ndm[ndm < pdm] = 0
        tr  = pd.concat([hi - lo,
                         (hi - cl.shift()).abs(),
                         (lo - cl.shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        pdi = 100 * pdm.rolling(period).mean() / atr.replace(0, np.nan)
        ndi = 100 * ndm.rolling(period).mean() / atr.replace(0, np.nan)
        dx  = 100 * (pdi - ndi).abs() / (pdi + ndi).replace(0, np.nan)
        return dx.rolling(period).mean()

    def _calc_supertrend(self, df, period=10, mult=3.0):
        """Supertrend indicator — dynamic support/resistance based on ATR."""
        hl2   = (df["high"] + df["low"]) / 2
        upper = hl2 + mult * df["atr"]
        lower = hl2 - mult * df["atr"]
        st        = pd.Series(np.nan, index=df.index)
        direction = pd.Series(np.nan, index=df.index)
        for i in range(1, len(df)):
            if df["close"].iloc[i] > upper.iloc[i - 1]:
                st.iloc[i]        = lower.iloc[i]
                direction.iloc[i] = 1
            elif df["close"].iloc[i] < lower.iloc[i - 1]:
                st.iloc[i]        = upper.iloc[i]
                direction.iloc[i] = -1
            else:
                st.iloc[i]        = st.iloc[i - 1] if not pd.isna(st.iloc[i - 1]) else lower.iloc[i]
                direction.iloc[i] = direction.iloc[i - 1] if not pd.isna(direction.iloc[i - 1]) else 1
        return st, direction

    def _calc_psar(self, df, af_start=0.02, af_max=0.2):
        """Parabolic SAR — trailing stop that flips on trend reversal."""
        close, high, low = df["close"].values, df["high"].values, df["low"].values
        psar = close.copy().astype(float)
        bull, af  = True, af_start
        hp, lp    = high[0], low[0]
        for i in range(2, len(df)):
            prev = psar[i - 1]
            if bull:
                psar[i] = prev + af * (hp - prev)
                psar[i] = min(psar[i], low[i - 1], low[i - 2])
                if low[i] < psar[i]:
                    bull, af, lp = False, af_start, low[i]
                    psar[i] = hp
                elif high[i] > hp:
                    hp = high[i]
                    af = min(af + af_start, af_max)
            else:
                psar[i] = prev - af * (prev - lp)
                psar[i] = max(psar[i], high[i - 1], high[i - 2])
                if high[i] > psar[i]:
                    bull, af, hp = True, af_start, high[i]
                    psar[i] = lp
                elif low[i] < lp:
                    lp = low[i]
                    af = min(af + af_start, af_max)
        return pd.Series(psar, index=df.index)

    # ── STATISTICAL ARBITRAGE ─────────────────────────────────────────────────

    def _run_stat_arb(self, symbol_a: str, df_a) -> list:
        """
        Compare symbol_a against all cached symbols.
        If z-score of spread diverges > 2.0 → mean reversion trade.
        Fixed: now uses candle cache instead of requiring 2 explicit args.
        """
        signals = []
        for symbol_b, df_b in self._candle_cache.items():
            if symbol_b == symbol_a:
                continue
            try:
                min_len = min(len(df_a), len(df_b))
                if min_len < 50:
                    continue
                spread = df_a["close"].iloc[-min_len:].values - df_b["close"].iloc[-min_len:].values
                spread = pd.Series(spread)
                mean   = spread.rolling(30).mean().iloc[-1]
                std    = spread.rolling(30).std().iloc[-1]
                if std == 0:
                    continue
                z = (spread.iloc[-1] - mean) / std
                if z < -2.0:
                    signals.append(self._sig(symbol_a, "BUY",  "statistical_arb", 0.75))
                    break   # one arb signal per symbol per cycle
                elif z > 2.0:
                    signals.append(self._sig(symbol_a, "SELL", "statistical_arb", 0.75))
                    break
            except Exception:
                continue
        return signals

    # ── SIGNAL MERGING ────────────────────────────────────────────────────────

    def _merge_signals(self, signals: list) -> list:
        """
        Merge signals for same symbol+action.
        Multiple strategies agreeing = higher confidence (max 0.95).
        """
        if not signals:
            return []
        groups = defaultdict(list)
        for s in signals:
            groups[(s["symbol"], s["action"])].append(s)
        merged = []
        for (sym, action), group in groups.items():
            best = max(group, key=lambda x: x["confidence"])
            if len(group) > 1:
                best = best.copy()
                best["confidence"]       = min(best["confidence"] + 0.05 * (len(group) - 1), 0.95)
                best["confluence_count"] = len(group)
                best["strategies_agreed"]= [g["strategy"] for g in group]
            merged.append(best)
        return merged

    def _sig(self, symbol, action, strategy, confidence):
        return {
            "symbol":     symbol,
            "action":     action,
            "strategy":   strategy,
            "confidence": confidence,
            "time":       datetime.now().isoformat(),
        }
