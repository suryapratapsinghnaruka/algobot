"""
Notifier — sends trade alerts to Telegram.
Every trade, every stop-loss hit, every error → message on your phone.
"""

import logging
import requests

log = logging.getLogger("Notifier")


class Notifier:
    def __init__(self, config: dict):
        self.token = config.get("TELEGRAM_TOKEN", "")
        self.chat_id = config.get("TELEGRAM_CHAT_ID", "")
        self.enabled = config.get("NOTIFICATIONS_ON", False)

    def send(self, message: str):
        if not self.enabled or not self.token or "YOUR_" in self.token:
            return
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            requests.post(url, data={"chat_id": self.chat_id, "text": message}, timeout=5)
        except KeyboardInterrupt:
            pass   # ignore Ctrl+C during shutdown Telegram send
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")