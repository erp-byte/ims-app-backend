"""Shoonya authentication — the surface the orchestrator drives.

Ladder (Shoonya's post-April-2026 OAuth API):
  1. cached session   — time-valid AND accepted by the broker (UserDetails)
  2. automated login  — headless browser on Shoonya's login page captures the
                        OAuth code (optional: FDK_SHOONYA_AUTO_LOGIN, needs Chrome)
  3. manual link      — the login bot sends the OAuth URL; the operator logs in
                        and pastes the URL they land on (it carries code=...);
                        the link is resent every few minutes until then
Either code is exchanged for an access token from THIS host, which must be the
static IP whitelisted on the Shoonya account.
Every blocking call (Supabase, broker HTTP, browser) runs in a worker thread
because the event loop is shared with the backend.
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
    auth_success_message, auth_failure_message, shoonya_login_link_message,
    shoonya_login_paused_message,
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
        self.login_method: str | None = None     # cached / auto / manual
        self.awaiting_manual_login = False
        # The orchestrator points this at its market-hours check so the manual
        # wait yields (no link spam, 17:00 reset keeps running) when a login
        # would not be used anyway.
        self.keep_waiting = lambda: True
        # Tunables (tests shorten them). The browser recipe already resubmits
        # once internally, so two attempts are plenty before the Telegram link.
        self.auto_attempts = AUTO_LOGIN_MAX_ATTEMPTS
        self.auto_retry_delay = AUTO_LOGIN_RETRY_DELAY
        self.resend_interval = LOGIN_URL_RETRY_INTERVAL
        self._cancel_event = asyncio.Event()
        self._code_event = asyncio.Event()
        self._pending_code: str | None = None

    # -- control (the surface the orchestrator and the webhook drive) --------
    def cancel_auth(self):
        """Cancel any in-progress login wait (used by /hld, /rst and shutdown)."""
        self._cancel_event.set()
        self._code_event.set()          # wake the manual wait so authenticate() can exit

    def reset_cancel(self):
        self._cancel_event.clear()

    def reset(self):
        """Clear the in-memory session so the next authenticate() starts fresh."""
        self.is_authenticated = False
        self.access_token = None
        self.account_id = None

    def submit_code(self, text: str) -> bool:
        """Hand a pasted redirect URL (or anything containing code=...) to the manual wait."""
        code = shoonya_login.extract_code(text)
        if not code or not self.awaiting_manual_login:
            return False
        self._pending_code = code
        self._code_event.set()
        return True

    # -- the ladder ---------------------------------------------------------
    async def authenticate(self) -> bool:
        # 1. cached session (time-valid AND live-verified)
        valid, _ = await asyncio.to_thread(self.token_manager.is_token_valid_by_time)
        if valid:
            token, _, _, _ = await asyncio.to_thread(self.token_manager.load_token)
            ok, detail = await asyncio.to_thread(
                shoonya_login.validate_session, self.cfg.username, token)
            if ok:
                self._adopt(token, self.cfg.username, "cached")
                log.info("Using existing valid Shoonya session from Supabase")
                await self.login_sender.send_async(
                    auth_success_message(broker="Shoonya", detail="Session active."))
                return True
            log.warning(f"Stored Shoonya session rejected: {detail}")
        if self._cancel_event.is_set():
            return False

        # 2. automated browser login
        if self.cfg.auto_login and shoonya_login.auto_login_available():
            for attempt in range(1, max(1, self.auto_attempts) + 1):
                if self._cancel_event.is_set():
                    return False
                log.info(f"Starting Shoonya browser login (attempt {attempt}/{self.auto_attempts})...")
                result = await asyncio.to_thread(
                    shoonya_login.auto_login_code,
                    self.cfg.client_id, self.cfg.username, self.cfg.password, self.cfg.totp_secret,
                    cancelled=self._cancel_event.is_set,
                    redirect_hint=self.cfg.redirect_hint,
                )
                if result.get("code"):
                    if await self._exchange_and_adopt(result["code"], "auto"):
                        return True
                    break                               # the code was fine, the exchange was not: config problem -> manual
                log.warning(f"Shoonya browser login failed: {result.get('error')}")
                self.last_error = result.get("error")
                if result.get("fatal"):
                    break                               # no browser to drive: retrying cannot help
                if attempt < self.auto_attempts:
                    await self._cancellable_sleep(self.auto_retry_delay)
        elif self.cfg.auto_login:
            log.warning("FDK_SHOONYA_AUTO_LOGIN is on but selenium is not installed — manual link only")
        if self._cancel_event.is_set():
            return False

        # 3. manual link via Telegram
        return await self._manual_login()

    async def _manual_login(self) -> bool:
        url = shoonya_login.oauth_login_url(self.cfg.client_id)
        self.awaiting_manual_login = True
        self._code_event.clear()
        self._pending_code = None
        try:
            while True:
                await self.login_sender.send_async(shoonya_login_link_message(url))
                try:
                    await asyncio.wait_for(self._code_event.wait(), timeout=self.resend_interval)
                except asyncio.TimeoutError:
                    if self._cancel_event.is_set():
                        return False
                    if not self.keep_waiting():
                        # e.g. market closed: hand control back to the supervisor loop
                        # (17:00 reset, market-hours gate); it re-enters at the next open.
                        log.info("Shoonya manual login paused — a login would not be used now")
                        await self.login_sender.send_async(shoonya_login_paused_message())
                        return False
                    log.info("Shoonya login link resent (still waiting for the pasted URL)")
                    continue
                if self._cancel_event.is_set():
                    log.info("Shoonya manual login cancelled")
                    return False
                code, self._pending_code = self._pending_code, None
                self._code_event.clear()
                if code and await self._exchange_and_adopt(code, "manual"):
                    return True
                # bad paste / rejected code: the failure was reported, keep waiting
        finally:
            self.awaiting_manual_login = False

    # -- helpers ------------------------------------------------------------
    async def _exchange_and_adopt(self, code: str, method: str) -> bool:
        result = await asyncio.to_thread(
            shoonya_login.exchange_code,
            self.cfg.username, self.cfg.client_id, self.cfg.secret_code, code,
        )
        if not result.get("access_token"):
            self.last_error = result.get("error", "unknown error")
            log.error(f"Shoonya token exchange failed: {self.last_error}")
            await self.login_sender.send_async(
                auth_failure_message(self.last_error, broker="Shoonya"))
            return False
        await asyncio.to_thread(
            self.token_manager.save_token, result["access_token"],
            refresh_token=result.get("refresh_token"))
        self._adopt(result["access_token"], result.get("account_id") or self.cfg.username, method)
        log.info(f"Shoonya login successful ({method})")
        await self.login_sender.send_async(
            auth_success_message(broker="Shoonya", detail="Session active."))
        return True

    def _adopt(self, token: str, account_id: str, method: str) -> None:
        self.access_token = token
        self.account_id = account_id
        self.is_authenticated = True
        self.last_login_at = datetime.now(IST)
        self.last_error = None
        self.login_method = method

    async def _cancellable_sleep(self, seconds: float) -> None:
        for _ in range(int(seconds)):
            if self._cancel_event.is_set():
                return
            await asyncio.sleep(1)
