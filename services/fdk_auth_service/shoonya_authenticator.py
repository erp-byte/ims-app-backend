"""Shoonya authentication — the same surface the orchestrator drives for Fyers.

Ladder: cached session (time-valid AND accepted by the broker) -> fresh headless
login with bounded, cancellable retries -> report on Telegram and cool down.
There is no manual/browser fallback: Shoonya has no OAuth step.
Every blocking call (Supabase, broker HTTP) runs in a worker thread because the
event loop is shared with the backend.
"""
import asyncio
from datetime import datetime

import pyotp

from shared.fdk_logger import get_logger
from shared.fdk_constants import (
    AUTO_LOGIN_MAX_ATTEMPTS, AUTO_LOGIN_RETRY_DELAY, LOGIN_URL_RETRY_INTERVAL, IST,
)
from services.fdk_telegram_service import TelegramSender
from services.fdk_telegram_service.message_template import (
    auth_success_message, auth_failure_message,
)
from .token_manager import TokenManager
from . import shoonya_login

log = get_logger("shoonya_authenticator")


class ShoonyaAuthenticator:
    broker = "shoonya"

    def __init__(self, config, token_manager: TokenManager, login_sender: TelegramSender):
        self.cfg = config.shoonya
        # Fail at fdk start-up (visible in /fdk/health) rather than at market open.
        secret = (self.cfg.totp_secret or "").strip()
        if not secret:
            raise ValueError("FDK_SHOONYA_TOTP_SECRET is empty")
        try:
            pyotp.TOTP(secret).now()
        except Exception as exc:
            raise ValueError(
                f"FDK_SHOONYA_TOTP_SECRET is not a valid base32 TOTP secret: {exc}") from exc
        self.token_manager = token_manager
        self.login_sender = login_sender
        self.access_token: str | None = None
        self.account_id: str | None = None
        self.is_authenticated = False
        self.last_login_at: datetime | None = None
        self.last_error: str | None = None
        # Seconds to wait after an exhausted login cycle before the supervisor
        # loop may try again (keeps a wrong password from hammering the broker).
        self.cooldown = LOGIN_URL_RETRY_INTERVAL
        self._cancel_event = asyncio.Event()

    # -- control (the surface the orchestrator and the webhook drive) --------
    def cancel_auth(self):
        """Cancel any in-progress login wait (used by /hld and /rst)."""
        self._cancel_event.set()

    def reset_cancel(self):
        self._cancel_event.clear()

    def reset(self):
        """Clear the in-memory session so the next authenticate() starts fresh."""
        self.is_authenticated = False
        self.access_token = None
        self.account_id = None

    # -- the ladder ---------------------------------------------------------
    async def authenticate(self) -> bool:
        # Strategy 1: cached session (time-valid AND live-verified)
        valid, _ = await asyncio.to_thread(self.token_manager.is_token_valid_by_time)
        if valid:
            token, _, _, _ = await asyncio.to_thread(self.token_manager.load_token)
            ok, detail = await asyncio.to_thread(
                shoonya_login.validate_session, self.cfg.username, token)
            if ok:
                self._adopt(token, self.cfg.username)
                log.info("Using existing valid Shoonya session from Supabase")
                await self.login_sender.send_async(
                    auth_success_message(broker="Shoonya", detail="Session active."))
                return True
            log.warning(f"Stored Shoonya session rejected: {detail}")
        if self._cancel_event.is_set():
            return False

        # Strategy 2: fresh headless login (bounded, cancellable)
        log.info("Starting Shoonya login...")
        result = await asyncio.to_thread(
            shoonya_login.login_with_retry,
            self._creds(), AUTO_LOGIN_MAX_ATTEMPTS, AUTO_LOGIN_RETRY_DELAY,
            self._cancel_event.is_set,
        )
        if result.get("access_token"):
            await asyncio.to_thread(self.token_manager.save_token, result["access_token"])
            self._adopt(result["access_token"], result.get("account_id") or self.cfg.username)
            log.info(f"Shoonya login successful ({result.get('user_name', '')})")
            await self.login_sender.send_async(
                auth_success_message(broker="Shoonya", detail="Session active."))
            return True
        if self._cancel_event.is_set():
            return False

        # Strategy 3: report, then cool down before the supervisor loop retries
        self.last_error = result.get("error", "unknown error")
        log.error(f"Shoonya login failed: {self.last_error}")
        await self.login_sender.send_async(
            auth_failure_message(self.last_error, broker="Shoonya"))
        await self._cancellable_sleep(self.cooldown)
        return False

    # -- helpers ------------------------------------------------------------
    def _creds(self) -> dict:
        return {
            "username": self.cfg.username,
            "password": self.cfg.password,
            "totp_secret": self.cfg.totp_secret,
            "vendor_code": self.cfg.vendor_code,
            "api_key": self.cfg.api_key,
            "imei": self.cfg.imei,
        }

    def _adopt(self, token: str, account_id: str) -> None:
        self.access_token = token
        self.account_id = account_id
        self.is_authenticated = True
        self.last_login_at = datetime.now(IST)
        self.last_error = None

    async def _cancellable_sleep(self, seconds: float) -> None:
        for _ in range(int(seconds)):
            if self._cancel_event.is_set():
                return
            await asyncio.sleep(1)
