"""
Trade Journal
=============
Logs every closed trade to CSV for post-analysis.
Tracks per-strategy win rates to power the ranker and AI advisor.
"""

import csv
import json
import logging
import os
from datetime import datetime

log = logging.getLogger("TradeJournal")

JOURNAL_FILE   = "logs/trades.csv"
STATS_FILE     = "logs/strategy_stats.json"
FIELDNAMES     = [
    "timestamp", "symbol", "market", "action", "strategy",
    "entry_price", "exit_price", "qty", "pnl", "pnl_pct",
    "hold_minutes", "reason", "confluence_count", "signal_confidence",
    "ai_confidence", "sentiment_score",
]


class TradeJournal:
    def __init__(self):
        os.makedirs("logs", exist_ok=True)
        self._ensure_csv()
        self.strategy_stats = self._load_stats()

    def _ensure_csv(self):
        if not os.path.exists(JOURNAL_FILE):
            with open(JOURNAL_FILE, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                writer.writeheader()

    def log_trade(self, position: dict, exit_result: dict,
                  ai_confidence: float = 1.0,
                  sentiment_score: float = 0.0):
        """Log a closed trade to CSV and update strategy stats."""
        try:
            entry  = position.get("entry_price", 0)
            exit_p = exit_result.get("exit_price", 0)
            qty    = position.get("qty", 0)
            pnl    = exit_result.get("pnl", 0)
            pnl_pct = (pnl / (entry * qty) * 100) if (entry * qty) > 0 else 0

            # Calculate hold time
            try:
                opened = datetime.fromisoformat(position.get("time", datetime.now().isoformat()))
                hold_minutes = int((datetime.now() - opened).total_seconds() / 60)
            except Exception:
                hold_minutes = 0

            # Resolve market — position dict has it, but fallback to exit_result if missing
            market_val = (position.get("market") or
                          exit_result.get("market") or "unknown")

            row = {
                "timestamp":        datetime.now().isoformat(),
                "symbol":           position.get("symbol", ""),
                "market":           market_val,
                "action":           position.get("action", ""),
                "strategy":         position.get("strategy", "unknown"),
                "entry_price":      round(entry, 4),
                "exit_price":       round(exit_p, 4),
                "qty":              qty,
                "pnl":              round(pnl, 2),
                "pnl_pct":          round(pnl_pct, 2),
                "hold_minutes":     hold_minutes,
                "reason":           exit_result.get("reason", ""),
                "confluence_count": position.get("confluence_count", 1),
                "signal_confidence":round(position.get("signal_confidence", 0), 3),
                "ai_confidence":    round(ai_confidence, 3),
                "sentiment_score":  round(sentiment_score, 3),
            }

            with open(JOURNAL_FILE, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                writer.writerow(row)

            # Update strategy stats
            strat = row["strategy"]
            if strat not in self.strategy_stats:
                self.strategy_stats[strat] = {"wins": 0, "losses": 0, "total_pnl": 0.0}
            if pnl > 0:
                self.strategy_stats[strat]["wins"] += 1
            else:
                self.strategy_stats[strat]["losses"] += 1
            self.strategy_stats[strat]["total_pnl"] = round(
                self.strategy_stats[strat]["total_pnl"] + pnl, 2
            )
            self._save_stats()

            log.info(f"[Journal] Logged {row['symbol']} | PnL: {pnl:+.2f} | "
                     f"Strategy: {strat} | Hold: {hold_minutes}m")

        except Exception as e:
            log.warning(f"Trade journal error: {e}")

    def get_closed_trades(self) -> list[dict]:
        """Read all logged trades from CSV."""
        trades = []
        if not os.path.exists(JOURNAL_FILE):
            return trades
        try:
            with open(JOURNAL_FILE, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Cast numeric fields
                    for field in ["entry_price", "exit_price", "pnl", "pnl_pct",
                                  "ai_confidence", "sentiment_score"]:
                        try:
                            row[field] = float(row[field])
                        except Exception:
                            pass
                    trades.append(row)
        except Exception as e:
            log.warning(f"Journal read error: {e}")
        return trades

    def strategy_summary(self) -> str:
        """Returns a human-readable strategy performance table."""
        if not self.strategy_stats:
            return "No strategy data yet."

        lines = [f"\n{'Strategy':<35} {'W':>5} {'L':>5} {'WR%':>6} {'PnL':>10}"]
        lines.append("-" * 65)

        for strat, stats in sorted(
            self.strategy_stats.items(),
            key=lambda x: x[1]["total_pnl"],
            reverse=True
        ):
            w    = stats["wins"]
            l    = stats["losses"]
            wr   = w / (w + l) * 100 if (w + l) > 0 else 0
            pnl  = stats["total_pnl"]
            lines.append(f"{strat:<35} {w:>5} {l:>5} {wr:>5.0f}% {pnl:>+10.2f}")

        return "\n".join(lines)

    def _load_stats(self) -> dict:
        if os.path.exists(STATS_FILE):
            try:
                with open(STATS_FILE) as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save_stats(self):
        try:
            with open(STATS_FILE, "w") as f:
                json.dump(self.strategy_stats, f, indent=2)
        except Exception as e:
            log.warning(f"Stats save error: {e}")