"""
Risk Manager — fixed version
==============================
Fixes:
- Daily reset uses date comparison (no race condition)
- can_trade() receives live open_positions as before
"""

import logging
from datetime import datetime

log = logging.getLogger("RiskManager")


class RiskManager:
    def __init__(self, config: dict):
        self.cfg           = config
        self.daily_loss    = 0.0
        self.blocked_today = False
        self._last_reset   = datetime.now().date()

    def can_trade(self, symbol: str, action: str, daily_pnl: float,
                  open_positions: list, market: str = "crypto") -> bool:
        # Auto-reset check (fixes midnight race condition)
        today = datetime.now().date()
        if today > self._last_reset:
            self.reset_daily()

        if self.blocked_today:
            log.debug("Trade blocked: bot paused for today.")
            return False

        # Per-market position limits
        if market == "stocks":
            limit = self.cfg.get("MAX_OPEN_TRADES_STOCKS", self.cfg.get("MAX_OPEN_TRADES", 10))
        else:
            limit = self.cfg.get("MAX_OPEN_TRADES_CRYPTO", self.cfg.get("MAX_OPEN_TRADES", 10))

        if len(open_positions) >= limit:
            log.warning(f"Max {market} trades reached ({len(open_positions)}/{limit}).")
            return False

        capital        = self.cfg["CAPITAL"]
        max_daily_loss = capital * (self.cfg["MAX_DAILY_LOSS_PCT"] / 100)
        if daily_pnl < -max_daily_loss:
            log.warning(f"Daily loss limit hit: {daily_pnl:.2f} (limit: -{max_daily_loss:.2f})")
            self.blocked_today = True
            return False

        open_symbols = {t["symbol"] for t in open_positions}
        if symbol in open_symbols:
            log.debug(f"Already have open position for {symbol}. Skipping.")
            return False

        return True

    def position_size(self, price: float, capital: float, atr: float = None) -> int:
        """
        Position sizing. If ATR is provided, uses ATR-based sizing
        (risk 1% of capital per trade, sized by ATR stop distance).
        """
        if atr and atr > 0 and self.cfg.get("ATR_BASED_EXITS"):
            sl_dist = self.cfg.get("ATR_SL_MULTIPLIER", 1.5) * atr
            risk_amount = capital * 0.01   # risk 1% of capital per trade
            qty = risk_amount / sl_dist
        else:
            max_trade_value = capital * (self.cfg["MAX_POSITION_PCT"] / 100)
            qty = max_trade_value / price

        return max(1, int(qty))

    def is_drawdown_breached(self, daily_pnl: float) -> bool:
        capital = self.cfg["CAPITAL"]
        max_dd  = capital * (self.cfg["MAX_DRAWDOWN_PCT"] / 100)
        if daily_pnl < -max_dd:
            log.warning(f"Max drawdown breached: {daily_pnl:.2f} (limit: -{max_dd:.2f})")
            self.blocked_today = True
            return True
        return False

    def reset_daily(self):
        self.daily_loss    = 0.0
        self.blocked_today = False
        self._last_reset   = datetime.now().date()
        log.info("Risk manager daily stats reset.")