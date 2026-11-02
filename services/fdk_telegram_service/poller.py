"""Telegram long polling on the login bot.

Used when the host has no public HTTPS URL (FDK_BASE_URL empty), e.g. the EC2 box
whose static IP is whitelisted at Shoonya: Telegram will only deliver webhooks to
HTTPS, but getUpdates works from anywhere. Polling and webhooks are mutually
exclusive on a bot, so the webhook is removed first. Only the login bot is polled;
commands sent to the other four bot chats are not seen in this mode.
"""
import asyncio
from typing import Awaitable, Callable

import requests

from shared.fdk_logger import get_logger
from .command_parser import redact_code

log = get_logger("telegram_poller")


class TelegramPoller:
    def __init__(self, bot_token: str, handler: Callable[[str], Awaitable], *,
                 chat_id: str | int | None = None,
                 timeout: int = 25, idle_pause: float = 0.5, error_backoff: float = 5.0,
                 api_base: str = "https://api.telegram.org"):
        self.token = bot_token
        self.handler = handler
        self.chat_id = str(chat_id) if chat_id else None   # only this chat is listened to
        self.timeout = timeout            # server-side long-poll wait (seconds)
        self.idle_pause = idle_pause
        self.error_backoff = error_backoff
        self.api_base = api_base

    async def run(self) -> None:
        """Poll forever; cancelled by the lifespan on shutdown."""
        try:
            await asyncio.to_thread(self._delete_webhook)
        except Exception as exc:
            log.warning(f"deleteWebhook failed ({exc}); Telegram may reject polling until it is removed")
        log.info("Telegram polling started on the login bot")
        offset = None
        while True:
            try:
                updates = await asyncio.to_thread(self._get_updates, offset)
            except Exception as exc:
                log.warning(f"Telegram getUpdates failed: {exc}; retrying in {self.error_backoff}s")
                await asyncio.sleep(self.error_backoff)
                continue
            for update in updates:
                offset = update["update_id"] + 1
                message = update.get("message") or {}
                text = message.get("text")
                if not text:
                    continue
                chat = str((message.get("chat") or {}).get("id"))
                if self.chat_id and chat != self.chat_id:
                    log.warning(f"Ignoring a message from chat {chat} (not the login chat)")
                    continue
                try:
                    await self.handler(text)
                except Exception as exc:
                    log.error(f"Telegram command handler failed for {redact_code(text)!r}: {exc}")
            if not updates:
                await asyncio.sleep(self.idle_pause)

    # -- blocking HTTP, run in worker threads ----------------------------------
    def _delete_webhook(self) -> None:
        requests.post(f"{self.api_base}/bot{self.token}/deleteWebhook", timeout=10)

    def _get_updates(self, offset) -> list:
        params = {"timeout": self.timeout, "allowed_updates": '["message"]'}
        if offset is not None:
            params["offset"] = offset
        resp = requests.get(f"{self.api_base}/bot{self.token}/getUpdates",
                            params=params, timeout=self.timeout + 10)
        data = resp.json()
        if not data.get("ok"):
            raise RuntimeError(f"getUpdates not ok: {data}")
        return data.get("result", [])
