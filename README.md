# AlgoBot v2 — AI-Powered Automated Trading Bot

Scans **entire NSE (~1800 stocks)** + **entire Binance (~300 crypto pairs)** in parallel.
Ranks signals by quality. Vetted by Claude AI before each trade.

---

## What's New in v2

| Feature | Description |
|---|---|
| **Watchlist Ranker** | Scores all signals, trades only the top N per cycle |
| **AI Filter** | Claude reviews every signal before placing — vetoes bad setups |
| **News Sentiment** | Fetches headlines via NewsAPI, Claude scores sentiment |
| **AI Strategy Advisor** | Weekly analysis: Claude reviews your trades and suggests new algos |
| **Live Web Dashboard** | Candlestick charts with EMA/BB/VWAP/RSI/MACD overlaid on live positions |
| **Trade Journal** | CSV log of every trade with AI confidence + sentiment metadata |
| **ATR-based SL/TP** | Dynamic stop-loss and take-profit sized to each symbol's volatility |
| **Trailing Stop** | Moves SL to breakeven once trade reaches +1.5% profit |
| **Strategy Stats** | Per-strategy win rate tracking — see which algos actually work |

---

## Quick Start

### 1. Install
```bash
pip install -r requirements.txt
```

### 2. Set environment variables
```bash
cp .env.example .env
# Edit .env with your API keys
```

Or on Railway: paste each key into the **Variables** tab.

**Required for AI features:**
- `ANTHROPIC_API_KEY` — get at console.anthropic.com
- `NEWS_API_KEY` — free tier at newsapi.org (100 req/day)

**Required for Telegram alerts:**
- `TELEGRAM_TOKEN` + `TELEGRAM_CHAT_ID`

**Required for live trading only:**
- Zerodha and/or Binance keys

### 3. Run
```bash
python bot.py
```

Dashboard auto-starts at **http://localhost:5001**

Or run dashboard standalone (while bot is running in another terminal):
```bash
python web_dashboard.py
```

---

## File Structure

```
algobot/
├── bot.py               ← Main bot — run this
├── config.py            ← All settings (reads from env vars)
├── strategies.py        ← 25+ trading strategies (fixed rolling VWAP)
├── risk_manager.py      ← SL/TP/drawdown/trailing stop
├── broker.py            ← Zerodha + Binance live brokers (cached instruments)
├── paper_trader.py      ← Simulated trading via yfinance (thread-safe)
├── notifier.py          ← Telegram alerts
├── ai_filter.py         ← Claude AI trade filter + sentiment + strategy advisor
├── watchlist_ranker.py  ← Scans all symbols, ranks by signal quality
├── trade_journal.py     ← CSV trade log + strategy performance tracker
├── web_dashboard.py     ← Flask live dashboard with candlestick charts
├── diagnose.py          ← Self-test tool
├── requirements.txt
├── .env.example         ← Copy to .env and fill in keys
├── Procfile             ← Railway/Render deploy
└── logs/
    ├── bot.log                    ← All bot activity
    ├── open_positions.json        ← Persists across restarts
    ├── trades.csv                 ← Full trade journal
    ├── strategy_stats.json        ← Per-strategy win rates
    └── ai_advisor_YYYYMMDD.md     ← Weekly AI strategy reports
```

---

## AI Features Deep-Dive

### AI Trade Filter
Before every trade, Claude reviews:
- Signal confidence + confluence count
- RSI, MACD, volume ratio, BB position
- Whether the setup looks like a trap

Claude returns `proceed: true/false` + a confidence score. If confidence is below `AI_MIN_CONFIDENCE` (default 0.60), the trade is skipped.

**Cost:** ~$0.0001 per trade check using claude-haiku. Negligible.

### News Sentiment
For each trade candidate, the bot:
1. Fetches the last 24h of headlines via NewsAPI (free: 100 req/day)
2. Claude scores sentiment from -1.0 (very bearish) to +1.0 (very bullish)
3. If score < `SENTIMENT_VETO_THRESHOLD` (default -0.5), the trade is vetoed

Sentiment scores are cached 30 minutes to stay within NewsAPI limits.

### AI Strategy Advisor
Every 7 days, the bot:
1. Pulls all closed trades from `trades.csv`
2. Sends win/loss breakdown per strategy to Claude
3. Claude identifies what's working and suggests a new custom strategy
4. Report saved to `logs/ai_advisor_YYYYMMDD.md`

### Watchlist Ranker
Every cycle, instead of trading anything that passes the threshold:
1. Scans all 2000+ symbols in parallel
2. Scores each signal: confidence + confluence + volume surge + RSI room + strategy win rate
3. Returns only the top `WATCHLIST_TOP_N` (default 5) signals
4. Only those go through AI filter → risk check → execution

This means you're always trading the **best** setups, not just the first ones to cross the bar.

---

## Dashboard

Visit **http://localhost:5001** while the bot is running.

- **Candlestick chart** — EMA20, Bollinger Bands, rolling VWAP overlaid
- **RSI subchart** — overbought/oversold levels marked
- **MACD subchart** — histogram + signal line
- **Open positions table** — live PnL, Near SL/TP warnings, click to load chart
- **Strategy performance** — win rate and total PnL per strategy
- **Recent trades** — last 15 closed trades with reason

Auto-refreshes every 30 seconds.

---

## Risk Settings

| Setting | Default | Meaning |
|---|---|---|
| STOP_LOSS_PCT | 2.0% | Fixed fallback SL (used when ATR_BASED_EXITS=False) |
| TAKE_PROFIT_PCT | 4.0% | Fixed fallback TP |
| ATR_BASED_EXITS | True | Dynamic SL/TP based on ATR (recommended) |
| ATR_SL_MULTIPLIER | 1.5 | SL = entry ± 1.5 × ATR |
| ATR_TP_MULTIPLIER | 3.0 | TP = entry ± 3.0 × ATR (2:1 R:R) |
| TRAILING_STOP_ENABLED | True | Move SL to breakeven once trade is profitable |
| TRAILING_ACTIVATE_PCT | 1.5% | Activate trailing after +1.5% gain |
| TRAILING_BUFFER_PCT | 0.3% | Lock in this much above entry |
| MAX_DAILY_LOSS_PCT | 5.0% | Bot stops if daily loss hits -5% of capital |
| MAX_DRAWDOWN_PCT | 10.0% | Bot pauses if drawdown hits -10% |
| MAX_OPEN_TRADES | 10 | Max simultaneous positions |
| WATCHLIST_TOP_N | 5 | Max new trades per scan cycle |
| MIN_SIGNAL_CONFIDENCE | 0.65 | Minimum algo confidence |
| AI_MIN_CONFIDENCE | 0.60 | Minimum Claude confidence |
| SENTIMENT_VETO_THRESHOLD | -0.5 | Veto trade if sentiment score below this |

---

## Go Live (after paper testing)

Same as before — set `PAPER_TRADING: False`, add Zerodha/Binance keys.
Start with ₹10,000–₹20,000. Raise `MIN_SIGNAL_CONFIDENCE` to 0.75–0.80 for live.

---

## Disclaimer
For educational purposes. Trading involves financial risk.
Only trade with money you can afford to lose.
