"""
AI Filter - Claude-powered trade intelligence layer 
====================================================
Three capabilities:

1. trade_filter()      - Claude reviews signal + candle context before trade
2. sentiment_check()   - Fetches news + Claude scores sentiment (bullish/bearish)
3. strategy_advisor()  - Analyzes closed trades, suggests new algos

All calls use claude-haiku (fast + cheap). Falls back gracefully if API is down.
"""

import logging
import json
import os
import requests
from datetime import datetime, timedelta

log = logging.getLogger("AIFilter")

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"


class AIFilter:
    def __init__(self, config: dict):
        self.cfg     = config
        self.api_key = config.get("ANTHROPIC_API_KEY", "")
        self.model   = config.get("AI_MODEL", "claude-haiku-4-5-20251001")
        self.enabled = config.get("AI_FILTER_ENABLED", False) and bool(self.api_key)

        if self.enabled:
            log.info(f"AI Filter enabled - model: {self.model}")
        else:
            log.info("AI Filter disabled (no ANTHROPIC_API_KEY or AI_FILTER_ENABLED=False)")

        self._sentiment_cache: dict = {}
        self._sentiment_ttl = 1800

    # -------------------------------------------------------------------------
    # TRADE FILTER
    # -------------------------------------------------------------------------

    def should_trade(self, symbol: str, action: str, signal: dict,
                     candles_summary: dict) -> tuple:
        if not self.enabled:
            return True, 1.0, "AI filter disabled"

        try:
            prompt   = self._build_trade_prompt(symbol, action, signal, candles_summary)
            response = self._call_claude(prompt, max_tokens=300)
            data       = self._parse_json_response(response)
            proceed    = data.get("proceed", True)
            confidence = float(data.get("confidence", 0.7))
            reasoning  = data.get("reasoning", "No reasoning provided")
            log.info(
                f"[AI] {symbol} {action} -> {'PROCEED' if proceed else 'VETO'} "
                f"| confidence: {confidence:.0%} | {reasoning[:80]}"
            )
            return proceed, confidence, reasoning
        except Exception as e:
            err_str = str(e)
            # 401 = invalid/expired API key — disable AI filter for this session
            # to stop spamming the log every cycle
            if "401" in err_str:
                log.warning(
                    f"AI filter: Anthropic API key invalid or credits exhausted (401). "
                    f"Disabling AI filter for this session. Top up at console.anthropic.com."
                )
                self.enabled = False
            else:
                log.warning(f"AI filter error for {symbol}: {e} - defaulting to proceed")
            return True, 1.0, f"AI error: {e}"

    def _build_trade_prompt(self, symbol, action, signal, candles):
        return (
            "You are a trading signal validator. Analyze this trade setup and decide if it should proceed.\n\n"
            f"Symbol: {symbol}\n"
            f"Action: {action}\n"
            f"Strategy: {signal.get('strategy', 'unknown')}\n"
            f"Algo confidence: {signal.get('confidence', 0):.0%}\n"
            f"Confluence (strategies agreed): {signal.get('confluence_count', 1)}\n\n"
            "Recent price context:\n"
            f"- Current price: {candles.get('current_price', 'N/A')}\n"
            f"- Price change (last 5 candles): {candles.get('price_change_5', 'N/A')}%\n"
            f"- RSI: {candles.get('rsi', 'N/A')}\n"
            f"- MACD histogram trend: {candles.get('macd_trend', 'N/A')}\n"
            f"- Volume vs average: {candles.get('vol_ratio', 'N/A')}x\n"
            f"- Price vs EMA50: {candles.get('vs_ema50', 'N/A')}%\n"
            f"- ATR (volatility): {candles.get('atr', 'N/A')}\n"
            f"- Bollinger Band position: {candles.get('bb_pct', 'N/A')} (0=lower band, 1=upper band)\n\n"
            "Respond ONLY with valid JSON, no other text:\n"
            "{\n"
            '  "proceed": true or false,\n'
            '  "confidence": 0.0 to 1.0,\n'
            '  "reasoning": "one sentence explanation"\n'
            "}\n\n"
            "Criteria for VETO (proceed=false):\n"
            "- RSI extremely overbought for BUY (>80) or oversold for SELL (<20)\n"
            "- Conflicting signals (e.g. BUY but price far below all EMAs with no momentum)\n"
            "- Volume below average on a breakout signal\n"
            "- Setup looks like a bull/bear trap based on candle context\n"
        )

    # -------------------------------------------------------------------------
    # SENTIMENT ANALYSIS
    # -------------------------------------------------------------------------

    def get_sentiment(self, symbol: str, market: str) -> tuple:
        if not self.enabled or not self.cfg.get("SENTIMENT_ENABLED", False):
            return 0.0, "Sentiment disabled"

        cached = self._sentiment_cache.get(symbol)
        if cached:
            score, ts, summary = cached
            if (datetime.now() - ts).seconds < self._sentiment_ttl:
                return score, summary

        try:
            headlines = self._fetch_news(symbol, market)
            if not headlines:
                return 0.0, "No news found"
            score, summary = self._score_sentiment(symbol, headlines)
            self._sentiment_cache[symbol] = (score, datetime.now(), summary)
            log.info(f"[Sentiment] {symbol}: {score:+.2f} | {summary[:60]}")
            return score, summary
        except Exception as e:
            log.warning(f"Sentiment error for {symbol}: {e}")
            return 0.0, f"Sentiment error: {e}"

    def _fetch_news(self, symbol: str, market: str) -> list:
        """Fetch headlines from NewsAPI + free RSS feeds."""
        headlines = []

        api_key = self.cfg.get("NEWS_API_KEY", "")
        if api_key:
            headlines += self._fetch_newsapi(symbol, market, api_key)

        if market == "stocks" and len(headlines) < 5:
            headlines += self._fetch_rss_india(symbol)

        return headlines[:10]

    def _fetch_newsapi(self, symbol: str, market: str, api_key: str) -> list:
        """NewsAPI.org - 100 requests/day free at newsapi.org"""
        if market == "stocks":
            query = f"{symbol} NSE India stock"
        else:
            base  = symbol.replace("USDT", "")
            query = f"{base} cryptocurrency crypto"

        try:
            url    = "https://newsapi.org/v2/everything"
            params = {
                "q":        query,
                "apiKey":   api_key,
                "language": "en",
                "sortBy":   "publishedAt",
                "pageSize": 10,
                "from":     (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S"),
            }
            r        = requests.get(url, params=params, timeout=10)
            data     = r.json()
            articles = data.get("articles", [])
            return [
                f"{a['title']} - {a.get('description', '')[:100]}"
                for a in articles if a.get("title")
            ][:8]
        except Exception as e:
            log.debug(f"NewsAPI fetch error for {symbol}: {e}")
            return []

    def _fetch_rss_india(self, symbol: str) -> list:
        """
        Free Indian financial RSS feeds - no API key needed.
        Add to requirements.txt: feedparser
        """
        try:
            import feedparser
        except ImportError:
            log.debug("feedparser not installed - run: pip install feedparser")
            return []

        feeds = [
            "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
            "https://www.moneycontrol.com/rss/marketsindia.xml",
        ]
        headlines = []
        for url in feeds:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:15]:
                    title = entry.get("title", "")
                    if symbol.upper() in title.upper():
                        headlines.append(title)
            except Exception as e:
                log.debug(f"RSS fetch error ({url}): {e}")
        return headlines[:5]

    def _score_sentiment(self, symbol: str, headlines: list) -> tuple:
        headlines_text = "\n".join(f"- {h}" for h in headlines)
        prompt = (
            f"Analyze these recent news headlines about {symbol} and rate the trading sentiment.\n\n"
            f"Headlines:\n{headlines_text}\n\n"
            "Respond ONLY with valid JSON, no other text:\n"
            "{\n"
            '  "score": -1.0 to 1.0,\n'
            '  "summary": "one sentence summary of sentiment"\n'
            "}\n\n"
            "Score guide: -1.0=very bearish, -0.5=bearish, 0.0=neutral, +0.5=bullish, +1.0=very bullish\n"
            "Focus on: earnings, regulatory news, product launches, scandals, market moves, analyst ratings."
        )
        response = self._call_claude(prompt, max_tokens=150)
        data     = self._parse_json_response(response)
        score    = float(data.get("score", 0.0))
        summary  = data.get("summary", "Neutral sentiment")
        return max(-1.0, min(1.0, score)), summary

    # -------------------------------------------------------------------------
    # STRATEGY ADVISOR
    # -------------------------------------------------------------------------

    def suggest_strategies(self, closed_trades: list, candle_examples: dict) -> str:
        """
        Weekly analysis: feed closed trades to Claude, get strategy suggestions back.
        Saves report to logs/ai_advisor_YYYYMMDD.md
        """
        if not self.enabled or not self.cfg.get("AI_STRATEGY_ADVISOR_ENABLED", False):
            return "AI Strategy Advisor disabled."

        if len(closed_trades) < 10:
            return "Not enough trades for analysis (need 10+)."

        try:
            wins  = [t for t in closed_trades if float(t.get("pnl", 0)) > 0]
            loses = [t for t in closed_trades if float(t.get("pnl", 0)) <= 0]

            win_strategies  = {}
            lose_strategies = {}
            for t in wins:
                s = t.get("strategy", "unknown")
                win_strategies[s] = win_strategies.get(s, 0) + 1
            for t in loses:
                s = t.get("strategy", "unknown")
                lose_strategies[s] = lose_strategies.get(s, 0) + 1

            prompt = (
                "You are an expert algorithmic trading strategy designer for Indian NSE stocks and crypto.\n\n"
                "Analyze this trading performance data and suggest improvements:\n\n"
                "PERFORMANCE SUMMARY:\n"
                f"- Total trades: {len(closed_trades)}\n"
                f"- Win rate: {len(wins)/len(closed_trades)*100:.1f}%\n"
                f"- Total PnL: {sum(float(t.get('pnl', 0)) for t in closed_trades):+.2f}\n\n"
                f"WINNING STRATEGIES:\n{json.dumps(win_strategies, indent=2)}\n\n"
                f"LOSING STRATEGIES:\n{json.dumps(lose_strategies, indent=2)}\n\n"
                "BEST TRADES (top 3):\n"
                f"{json.dumps(sorted(wins,  key=lambda x: float(x.get('pnl', 0)), reverse=True)[:3], indent=2, default=str)}\n\n"
                "WORST TRADES (bottom 3):\n"
                f"{json.dumps(sorted(loses, key=lambda x: float(x.get('pnl', 0)))[:3], indent=2, default=str)}\n\n"
                "Please provide:\n"
                "1. Which strategies are working well and why\n"
                "2. Which strategies to disable or tune\n"
                "3. ONE new custom strategy idea (Python pseudocode)\n"
                "4. Specific parameter tuning suggestions\n\n"
                "Format as markdown."
            )

            response = self._call_claude(prompt, max_tokens=1000)
            log.info("[AI Advisor] Strategy analysis complete.")
            return response

        except Exception as e:
            log.error(f"Strategy advisor error: {e}")
            return f"Strategy advisor error: {e}"

    # -------------------------------------------------------------------------
    # INTERNAL HELPERS
    # -------------------------------------------------------------------------

    def _call_claude(self, prompt: str, max_tokens: int = 300) -> str:
        headers = {
            "x-api-key":         self.api_key,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json",
        }
        body = {
            "model":      self.model,
            "max_tokens": max_tokens,
            "messages":   [{"role": "user", "content": prompt}],
        }
        r = requests.post(ANTHROPIC_API_URL, headers=headers, json=body, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data["content"][0]["text"]

    def _parse_json_response(self, text: str) -> dict:
        clean = text.strip()
        if clean.startswith("```"):
            parts = clean.split("```")
            clean = parts[1] if len(parts) > 1 else clean
            if clean.startswith("json"):
                clean = clean[4:]
        clean = clean.strip()
        return json.loads(clean)
