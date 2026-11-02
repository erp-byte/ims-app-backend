"""Orchestrates both detectors with market-hour scheduling."""
import asyncio
import time
from datetime import datetime
from shared.fdk_logger import get_logger
from shared.fdk_config_loader import AppConfig
from shared.fdk_constants import IST
from services.fdk_auth_service.shoonya_authenticator import ShoonyaAuthenticator
from services.fdk_auth_service.token_manager import TokenManager
from services.fdk_telegram_service import TelegramSender
from services.fdk_telegram_service.message_template import (
    monitoring_started_message, websocket_rebuild_message,
)
from services.fdk_summary_service import SummaryScheduler
from services.fdk_sector_service import SymbolManager, init_sector_mapping
from services.fdk_detector_service.websocket_manager import TickDispatcher
from services.fdk_fyers_service import FyersService, FyersSummaryService
from services.fdk_penny_service import PennyService, PennySummaryService
from .schedular import (
    is_market_hours, should_reset_tokens, should_rebuild_socket,
)

log = get_logger("orchestrator")

# Flipped by step 2 (the Shoonya tick feed). While False the orchestrator logs in
# and idles: the data socket below is the dormant Fyers one, which cannot run on a
# Shoonya session.
TICK_FEED_WIRED = False

# Seconds between retries of the start-up Supabase loads. Inside the backend there
# is no process restart to recover from a transient DB error, so run() must.
SUPERVISOR_RETRY_DELAY = 30


class Orchestrator:
    def __init__(self, config: AppConfig):
        self.config = config

        # Create 5 TelegramSender instances (one per bot+chat pair)
        self.login_sender = TelegramSender(config.telegram.login)
        self.fyers_trade_sender = TelegramSender(config.telegram.fyers_trade)
        self.fyers_summary_sender = TelegramSender(config.telegram.fyers_summary)
        self.penny_trade_sender = TelegramSender(config.telegram.penny_trade)
        self.penny_summary_sender = TelegramSender(config.telegram.penny_summary)

        # Shoonya is the only login (the Fyers webhook/TOTP login was removed).
        self.broker = ShoonyaAuthenticator.broker
        self.token_manager = TokenManager(config, table=f"{self.broker}_tokens")
        self.authenticator = ShoonyaAuthenticator(
            config, self.token_manager, self.login_sender,
        )

        # Isolated services — each manages its own detector + controller
        self.fyers = FyersService(
            config.fyers_client_id, config.google.fyers_sheet_id,
            config.google.credentials,
            self.fyers_trade_sender, self.fyers_summary_sender,
        )
        self.penny = PennyService(
            config.fyers_client_id, config.google.penny_sheet_id,
            config.google.credentials,
            self.penny_trade_sender, self.penny_summary_sender,
        )

        self.on_hold = False
        self.restart_requested = False
        self._dispatcher: TickDispatcher | None = None
        self._summary_task: asyncio.Task | None = None
        self._last_ws_rebuild: float | None = None
        self._services_built = False
        self._last_reset_date: str | None = None
        self._feed_notice_sent = False

        # Isolated summary services
        self.fyers_summary = FyersSummaryService(
            config.google.credentials,
            config.google.fyers_sheet_id, self.fyers_summary_sender,
        )
        self.penny_summary = PennySummaryService(
            config.google.credentials,
            config.google.penny_sheet_id, self.penny_summary_sender,
        )

        # Symbol/sector manager (Supabase, auto-seeds from JSON on first run)
        self.symbol_manager = SymbolManager(config.supabase.dsn)

    def hold(self):
        """Stop all detectors and enter hold mode."""
        self.on_hold = True
        self.fyers.stop()
        self.penny.stop()
        self._close_dispatcher()
        self.authenticator.cancel_auth()
        log.info("Detectors on hold")

    async def hold_async(self):
        """hold() for callers on the event loop (the Telegram webhook): the thread
        joins and the socket close run in a worker thread so the backend stays responsive."""
        self.on_hold = True
        self.authenticator.cancel_auth()
        await asyncio.to_thread(self.fyers.stop)
        await asyncio.to_thread(self.penny.stop)
        await self._close_dispatcher_async()
        log.info("Detectors on hold")

    def login_state(self) -> str:
        """One word for operators: ok / failed / on_hold / waiting_for_market_hours / in_progress."""
        if self.on_hold:
            return "on_hold"
        auth = self.authenticator
        if auth.is_authenticated:
            return "ok"
        if getattr(auth, "awaiting_manual_login", False):
            return "awaiting_manual_login"       # link sent on Telegram, waiting for the pasted URL
        if getattr(auth, "last_error", None):
            return "failed"
        if self.config.scheduling_enabled and not is_market_hours():
            return "waiting_for_market_hours"
        return "in_progress"

    def request_restart(self):
        """Signal the orchestrator loop to restart and re-authenticate."""
        self.restart_requested = True
        self.on_hold = False
        self.authenticator.cancel_auth()
        log.info("Restart requested")

    def _any_token_expired(self) -> bool:
        return self.fyers.token_expired or self.penny.token_expired

    def _close_dispatcher(self):
        if self._dispatcher:
            self._dispatcher.close()
            self._dispatcher = None

    async def _close_dispatcher_async(self):
        """_close_dispatcher() with the (slow) socket close off the shared event loop."""
        dispatcher, self._dispatcher = self._dispatcher, None
        if dispatcher:
            await asyncio.to_thread(dispatcher.close)

    def _connect_dispatcher(self):
        """Create and connect a shared TickDispatcher for both detectors."""
        detectors = [d for d in (self.fyers.detector, self.penny.detector) if d]
        token = self.authenticator.access_token
        self._dispatcher = TickDispatcher(
            self.config.fyers_client_id, token, detectors
        )
        self._dispatcher.connect()

    async def _daily_token_reset(self, today: str):
        """17:00 IST wipe — clear tokens + auth state so 09:13 is a fresh login.

        Stops detectors (already stopped after 16:00 close), deletes stored
        tokens, resets in-memory auth state, and forces services to rebuild with
        the new token at the next open (via _services_built=False).
        """
        log.info("Daily 17:00 token reset — clearing credentials for a fresh login")
        self.fyers.stop()
        self.penny.stop()
        await self._close_dispatcher_async()
        await asyncio.to_thread(self.token_manager.clear_tokens)
        self.authenticator.reset()
        self._services_built = False
        self._last_reset_date = today
        await self.login_sender.send_async(
            "<b>Daily token reset</b>\nCredentials cleared at 17:00 IST. "
            "A fresh login will run automatically at 09:13 tomorrow."
        )

    async def _re_authenticate(self):
        """Stop detectors, get fresh token, update both services."""
        log.info(f"Token rejected by {self.broker} — re-authenticating...")
        self.fyers.stop()
        self.penny.stop()
        await self._close_dispatcher_async()

        success = await self._authenticate_with_checks()
        if not success:
            return  # cancelled by /hld or /rst

        token = self.authenticator.access_token
        self.fyers.update_token(token)
        self.penny.update_token(token)

    async def _authenticate_with_checks(self) -> bool:
        """Run auth flow, respecting cancellation from /hld and /rst."""
        self.authenticator.reset_cancel()
        result = await self.authenticator.authenticate()
        return result is not False

    async def run(self):
        """Main supervisor loop."""
        # Start summary scheduler (runs independently at 16:30 IST)
        generators = [self.fyers_summary.generator, self.penny_summary.generator]
        summary_scheduler = SummaryScheduler(generators)
        # Keep a strong reference: the event loop only holds a weak one, so an
        # un-stored task can be garbage-collected mid-flight, silently killing
        # the 16:30 summaries for the rest of the process's life.
        self._summary_task = asyncio.create_task(summary_scheduler.run())

        # Load symbols and sectors from Supabase
        # Supabase + Sheets calls run in worker threads: the event loop is shared with the backend.
        # Retried: a transient DB error here must not end the supervisor for good.
        while True:
            try:
                fyers_symbols = await asyncio.to_thread(self.symbol_manager.load_symbols, "fyers")
                penny_symbols = await asyncio.to_thread(self.symbol_manager.load_symbols, "penny")
                sectors = await asyncio.to_thread(self.symbol_manager.load_sector_mapping)
                break
            except Exception as e:
                log.error(f"Supabase symbol load failed, retrying in {SUPERVISOR_RETRY_DELAY}s: {e}")
                await asyncio.sleep(SUPERVISOR_RETRY_DELAY)
        init_sector_mapping(sectors)

        while True:
            try:
                # Daily 17:00 IST token wipe -> guarantees a fresh 09:13 login.
                # Runs regardless of hold/market state so the invariant always holds.
                now = datetime.now(IST)
                if should_reset_tokens(
                    now.strftime("%H:%M"), self._last_reset_date,
                    now.strftime("%d-%m-%Y"),
                ):
                    await self._daily_token_reset(now.strftime("%d-%m-%Y"))

                if self.restart_requested:
                    self.restart_requested = False
                    self.fyers.stop()
                    self.penny.stop()
                    await self._close_dispatcher_async()
                    await self._re_authenticate()
                    continue

                if self.on_hold:
                    await asyncio.sleep(5)
                    continue

                if self.config.scheduling_enabled and not is_market_hours():
                    if self.fyers.is_running or self.penny.is_running:
                        log.info("Outside market hours, stopping detectors")
                        self.fyers.stop()
                        self.penny.stop()
                        await self._close_dispatcher_async()
                    await asyncio.sleep(60)
                    continue

                # Authenticate if we don't have a token yet
                if not self.authenticator.is_authenticated:
                    success = await self._authenticate_with_checks()
                    if not success:
                        # Auth was cancelled by /hld or /rst — loop back
                        continue

                # Stay logged in — so /fdk/health, the webhook and the MCP tools
                # report a real session — and idle until the Shoonya feed is wired.
                if not TICK_FEED_WIRED:
                    if not self._feed_notice_sent:
                        log.warning(f"{self.broker}: authenticated, but the tick feed for "
                                    "this broker is not wired yet — detectors stay idle")
                        self._feed_notice_sent = True
                    await asyncio.sleep(30)
                    continue

                # Build services once after first successful auth (rebuilt after
                # each daily reset so they pick up the fresh token cleanly).
                if not self._services_built:
                    token = self.authenticator.access_token
                    await asyncio.to_thread(self.fyers.build, token, fyers_symbols, sectors)
                    await asyncio.to_thread(self.penny.build, token, penny_symbols, sectors)
                    self._services_built = True

                if self._any_token_expired():
                    await self._re_authenticate()
                    continue

                # Nothing in the SDK reports a socket that has died or given
                # up reconnecting, so poll for it. Without this a drop stays
                # invisible and detection silently stops for the rest of the
                # day while /health still reports healthy.
                if self._dispatcher and should_rebuild_socket(
                    self._dispatcher.needs_rebuild(),
                    self._last_ws_rebuild, time.monotonic(),
                ):
                    log.error("WebSocket unhealthy — rebuilding dispatcher")
                    await self._close_dispatcher_async()
                    self._last_ws_rebuild = time.monotonic()
                    await self.login_sender.send_async(
                        websocket_rebuild_message()
                    )

                # /hld or /rst may have arrived while the awaits above were in flight —
                # re-check before opening the socket or starting the detectors.
                if self.on_hold or self.restart_requested:
                    await self._close_dispatcher_async()
                    continue

                if not self._dispatcher:
                    # Blocking: waits for the socket to open, then subscribes.
                    # Kept off the event loop so the webhook, health check and
                    # summary scheduler stay responsive meanwhile.
                    await asyncio.to_thread(self._connect_dispatcher)
                    await self.login_sender.send_async(
                        monitoring_started_message(
                            len(self.fyers.detector.config.symbols),
                            len(self.penny.detector.config.symbols),
                        )
                    )

                if self.on_hold or self.restart_requested:
                    await self._close_dispatcher_async()
                    continue

                self.fyers.start()
                self.penny.start()

                await asyncio.sleep(5)

            except Exception as e:
                log.error(f"Supervisor error: {e}")
                await asyncio.sleep(10)
