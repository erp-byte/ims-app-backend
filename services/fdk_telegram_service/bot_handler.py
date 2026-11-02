"""Telegram bot for sending messages."""
import asyncio
import requests
from shared.fdk_logger import get_logger
from shared.fdk_config_loader import TelegramChannel

log = get_logger("telegram")


class TelegramSender:
    """Send messages to a specific Telegram channel (bot_token + chat_id pair)."""

    def __init__(self, channel: TelegramChannel):
        self.bot_token = channel.bot_token
        self.chat_id = channel.chat_id
        self.base_url = f"https://api.telegram.org/bot{channel.bot_token}"

    def send(self, text: str, parse_mode: str = "HTML") -> bool:
        """Send a message to this channel."""
        try:
            resp = requests.post(
                f"{self.base_url}/sendMessage",
                data={"chat_id": self.chat_id, "text": text,
                      "parse_mode": parse_mode},
                timeout=10
            )
            if resp.status_code == 200:
                return True
            # Telegram explains every refusal in the body — revoked token,
            # wrong or migrated chat_id, bot removed from the group, HTML
            # parse error. Without this a rejected send is indistinguishable
            # from having had no data to send.
            log.error(f"Telegram refused send to {self.chat_id}: "
                      f"HTTP {resp.status_code} {resp.text}")
            return False
        except Exception as e:
            log.error(f"Send failed to {self.chat_id}: {e}")
            return False

    async def send_async(self, text: str, parse_mode: str = "HTML") -> bool:
        """Async wrapper (runs sync send in thread)."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.send, text, parse_mode)
