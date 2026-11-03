"""Orchestrates both detectors with market-hour scheduling."""
import asyncio
import time
from datetime import datetime
from shared.fdk_logger import get_logger
from shared.fdk_config_loader import AppConfig
from shared.fdk_constants import (
    FEED_SILENT_ALERT_INTERVAL, INSTRUMENT_REFRESH_ATTEMPTS, INSTRUMENT_REFRESH_RETRY, IST,
)
from services.fdk_auth_service.shoonya_authenticator import ShoonyaAuthenticator
from services.fdk_auth_service.token_manager import TokenManager
from services.fdk_telegram_service import TelegramSender
from services.fdk_telegram_service.message_template import (
    feed_coverage_warning_message, feed_not_up_message, feed_rejected_message,
    feed_still_silent_message, instrument_refresh_failed_message, instrument_refresh_message,
    monitoring_started_message, websocket_rebuild_message,
)
from services.fdk_summary_service import SummaryScheduler
from services.fdk_sector_service import SymbolManager, init_sector_mapping
from services.fdk_sector_service.instrument_refresh import InstrumentRefresher
from services.fdk_detector_service.websocket_manager import TickDispatcher
from services.fdk_shoonya_service import ShoonyaService, ShoonyaSummaryService
from services.fdk_penny_service import PennyService, PennySummaryService
from .schedular import (
    feed_should_be_live, is_market_hours, should_refresh_instruments, should_reset_tokens,
    should_rebuild_socket,
)

log = get_logger("orchestrator")

# Seconds between passes of the supervisor loop.
SUPERVISOR_POLL = 5

# A rejection after this many tries, the last on a fresh login, needs a person.
FEED_REJECTION_LIMIT = 2

# Seconds the supervisor waits after an unexpected error in its loop.
SUPERVISOR_ERROR_DELAY = 10

# Seconds between retries of the start-up Supabase loads. Inside the backend there
# is no process restart to recover from a transient DB error, so run() must.
SUPERVISOR_RETRY_DELAY = 30


class Orchestrator:
    def __init__(self, config: AppConfig):
        self.config = config

        # Create 5 TelegramSender instances (one per bot+chat pair)
        self.login_sender = TelegramSender(config.telegram.login)
        self.shoonya_trade_sender = TelegramSender(config.telegram.shoonya_trade)
        self.shoonya_summary_sender = TelegramSender(config.telegram.shoonya_summary)
        self.penny_trade_sender = TelegramSender(config.telegram.penny_trade)
        self.penny_summary_sender = TelegramSender(config.telegram.penny_summary)

        # Shoonya is the only broker: its login, and its feed (TickDispatcher) for the ticks.
        self.broker = ShoonyaAuthenticator.broker
        self.token_manager = TokenManager(config, table=f"{self.broker}_tokens")
        self.authenticator = ShoonyaAuthenticator(
            config, self.token_manager, self.login_sender,
        )
        self.authenticator.keep_waiting = self._login_wait_allowed

        # Isolated services — each manages its own detector + controller
        self.shoonya = ShoonyaService(
            config.google.shoonya_sheet_id,
            config.google.credentials,
            self.shoonya_trade_sender, self.shoonya_summary_sender,
        )
        self.penny = PennyService(
            config.google.penny_sheet_id,
            config.google.credentials,
            self.penny_trade_sender, self.penny_summary_sender,
        )

        self.on_hold = False
        self.restart_requested = False
        self._dispatcher: TickDispatcher | None = None
        self._summary_task: asyncio.Task | None = None
        self._last_ws_rebuild: float | None = None
        self._feed_announced = False        # "Monitoring Started" sent for this feed
        self._feed_down_noticed = False     # the operator knows it is down
        self._feed_rejections = 0
        self._feed_parked = False           # rejected repeatedly, waiting for /rst or tomorrow
        self._feed_silent_since: float | None = None   # outage start, while ticks are due
        self._silence_alerts = 0            # "still silent" reminders sent for this outage
        self._last_coverage_gap: list[str] | None = None
        self._services_built = False
        self._last_reset_date: str | None = None
        self._instrument_tokens: dict[str, str] = {}

        # Isolated summary services
        self.shoonya_summary = ShoonyaSummaryService(
            config.google.credentials,
            config.google.shoonya_sheet_id, self.shoonya_summary_sender,
        )
        self.penny_summary = PennySummaryService(
            config.google.credentials,
            config.google.penny_sheet_id, self.penny_summary_sender,
        )

        # Symbol/sector manager (Supabase, auto-seeds from JSON on first run)
        self.symbol_manager = SymbolManager(config.supabase.dsn)
        # Shoonya's symbol file is re-read every trading day: tokens and names can change.
        self.instrument_refresher = InstrumentRefresher(self.symbol_manager._get_conn)
        self._last_refresh_date: str | None = None
        self._refresh_failures = 0
        self._refresh_retry_at: float | None = None
        # The symbol universe, loaded from Supabase (again after a refresh renamed something)
        self._shoonya_symbols: list[str] = []
        self._penny_symbols: list[str] = []
        self._sectors: dict[str, str] = {}

    def hold(self):
        """Stop all detectors and enter hold mode."""
        self.on_hold = True
        self.shoonya.stop()
        self.penny.stop()
        self._close_dispatcher()
        self.authenticator.cancel_auth()
        log.info("Detectors on hold")

    async def hold_async(self):
        """hold() for callers on the event loop (the Telegram webhook): the thread
        joins and the socket close run in a worker thread so the backend stays responsive."""
        self.on_hold = True
        self.authenticator.cancel_auth()
        await asyncio.to_thread(self.shoonya.stop)
        await asyncio.to_thread(self.penny.stop)
        await self._close_dispatcher_async()
        log.info("Detectors on hold")

    async def _stop_detectors(self):
        """stop() can wait on a Sheets call in flight, and this loop is shared with the backend."""
        await asyncio.to_thread(self.shoonya.stop)
        await asyncio.to_thread(self.penny.stop)

    def _login_wait_allowed(self) -> bool:
        """Keep the Telegram login link alive only while a login would be used (market hours)."""
        return (not self.config.scheduling_enabled) or is_market_hours()

    def login_state(self) -> str:
        """One word for operators: ok / failed / on_hold / waiting_for_market_hours / in_progress."""
        if self.on_hold:
            return "on_hold"
        auth = self.authenticator
        if auth.is_authenticated:
            return "ok"
        if getattr(auth, "awaiting_manual_login", False):
            return "awaiting_manual_login"       # link sent on Telegram, waiting for the pasted URL
        if self.config.scheduling_enabled and not is_market_hours():
            # Parked (also after a paused manual wait): nothing is being attempted, so a
            # failure earlier in the day is history — it stays visible as last_auth_error.
            return "waiting_for_market_hours"
        if getattr(auth, "last_error", None):
            return "failed"
        return "in_progress"

    def request_restart(self):
        """Signal the orchestrator loop to restart and re-authenticate."""
        self.restart_requested = True
        self.on_hold = False
        self.authenticator.cancel_auth()
        log.info("Restart requested")

    def _any_token_expired(self) -> bool:
        return self.shoonya.token_expired or self.penny.token_expired

    def feed_status(self) -> dict:
        """For /fdk/health and the MCP status tool: a dead feed must not look healthy."""
        if self._dispatcher is None:
            return {"connected": False, "acknowledged": 0, "requested": 0}
        return self._dispatcher.status()

    def _forget_feed(self):
        """The operator-facing state of the feed that is going away."""
        self._feed_announced = self._feed_down_noticed = False
        self._feed_silent_since = None
        self._silence_alerts = 0

    def _close_dispatcher(self):
        self._forget_feed()
        if self._dispatcher:
            self._dispatcher.close()
            self._dispatcher = None

    async def _close_dispatcher_async(self):
        """_close_dispatcher() with the (slow) socket close off the shared event loop."""
        self._forget_feed()
        dispatcher, self._dispatcher = self._dispatcher, None
        if dispatcher:
            await asyncio.to_thread(dispatcher.close)

    def _connect_dispatcher(self):
        """Create and connect the shared Shoonya feed for both detectors."""
        detectors = [d for d in (self.shoonya.detector, self.penny.detector) if d]
        username = self.config.shoonya.username
        token = self.authenticator.access_token
        # Clears any earlier rejection: from here on the flag can only mean this socket.
        self.shoonya.update_token(token)
        self.penny.update_token(token)
        # Opened during continuous trading (a rebuild, /rst): ticks are due at once, so
        # "Monitoring Started" waits for the first one and silence counts from now.
        expect_ticks = feed_should_be_live()
        if expect_ticks and self._feed_silent_since is None:
            self._feed_silent_since = time.monotonic()
        # actid is the user id on every login path, which is what the vendor SDK sends.
        self._dispatcher = TickDispatcher(
            username, username, token, detectors, self._instrument_tokens,
            expect_ticks=expect_ticks,
        )
        self._dispatcher.connect()

    async def _report_feed_state(self):
        """Tell the operator once per change: up, down, or running with symbols missing."""
        dispatcher = self._dispatcher
        if dispatcher is None or self._any_token_expired():   # a rejection has its own message
            return
        if not dispatcher.expect_ticks and feed_should_be_live():
            # The day's first socket opened before trading (09:13). From here its silence
            # counts: the stall rule applies and reminders run until a tick arrives.
            dispatcher.expect_ticks_from_now()
            if not dispatcher.ticks_seen() and self._feed_silent_since is None:
                self._feed_silent_since = time.monotonic()
        if dispatcher.ready():                 # up — and ticking, when ticks are due
            self._feed_rejections = 0
            self._feed_down_noticed = False
            self._feed_silent_since = None
            self._silence_alerts = 0
            if not self._feed_announced:
                self._feed_announced = True
                await self.login_sender.send_async(
                    monitoring_started_message(
                        dispatcher.monitored(self.shoonya.detector),
                        dispatcher.monitored(self.penny.detector),
                    )
                )
        elif not dispatcher.is_up() and not self._feed_announced and not self._feed_down_noticed:
            self._feed_down_noticed = True
            log.error("Shoonya feed is not up")
            await self.login_sender.send_async(feed_not_up_message(len(dispatcher.no_token)))
        await self._remind_if_silent(dispatcher)
        report = dispatcher.take_coverage_report()
        if report and report["missing"] != self._last_coverage_gap:
            self._last_coverage_gap = report["missing"]
            await self.login_sender.send_async(feed_coverage_warning_message(report))

    async def _remind_if_silent(self, dispatcher):
        """While ticks are due and none arrive, say so every FEED_SILENT_ALERT_INTERVAL.

        A rebuild does not end the outage, only ticks do; the clock runs across rebuilds
        (see the rebuild branch of run()) and stops when the feed is announced again.
        """
        since = self._feed_silent_since
        if since is None or not dispatcher.expect_ticks:
            return
        if not feed_should_be_live():          # trading closed meanwhile: silence is normal
            return
        elapsed = time.monotonic() - since
        if elapsed >= (self._silence_alerts + 1) * FEED_SILENT_ALERT_INTERVAL:
            self._silence_alerts += 1
            minutes = int(elapsed // 60)
            log.error(f"Shoonya feed still silent after {minutes} minutes of trading")
            await self.login_sender.send_async(feed_still_silent_message(minutes))

    async def _on_feed_rejected(self):
        """The socket refused the session, which REST may still accept.

        So the stored session is discarded and the next login is a fresh one. If the socket
        refuses that too, logging in again cannot help: it stops and says so, rather
        than reporting a successful login every minute with no data flowing.
        """
        reason = self._dispatcher.rejection_reason or "no reason given"
        # If this write fails, the next pass starts over: a database blip is not a rejection.
        await asyncio.to_thread(self.token_manager.clear_tokens)
        self.authenticator.reset()
        self._feed_rejections += 1
        if self._feed_rejections >= FEED_REJECTION_LIMIT:
            await self._stop_detectors()
            await self._close_dispatcher_async()
            self._feed_parked = True
            self.authenticator.last_error = f"Shoonya feed rejected the session: {reason}"
            log.error(f"{self.authenticator.last_error} — not retrying until /rst")
            await self.login_sender.send_async(feed_rejected_message(reason, retrying=False))
            return
        await self.login_sender.send_async(feed_rejected_message(reason, retrying=True))
        await self._re_authenticate()

    async def _daily_token_reset(self, today: str):
        """17:00 IST wipe — clear tokens + auth state so 09:13 is a fresh login.

        Stops detectors (already stopped after 16:00 close), deletes stored
        tokens, resets in-memory auth state, and forces services to rebuild with
        the new token at the next open (via _services_built=False).
        """
        log.info("Daily 17:00 token reset — clearing credentials for a fresh login")
        await self._stop_detectors()
        await self._close_dispatcher_async()
        await asyncio.to_thread(self.token_manager.clear_tokens)
        self.authenticator.reset()
        self._services_built = False
        self._last_reset_date = today
        self._feed_parked = False
        self._feed_rejections = 0
        self._last_coverage_gap = None
        await self.login_sender.send_async(
            "<b>Daily token reset</b>\nCredentials cleared at 17:00 IST. "
            "A fresh login will run automatically at 09:13 tomorrow."
        )

    async def _re_authenticate(self):
        """Stop detectors, get fresh token, update both services."""
        log.info(f"Token rejected by {self.broker} — re-authenticating...")
        await self._stop_detectors()
        await self._close_dispatcher_async()
        if self.on_hold:                # /hld arrived during the close: no login now
            return

        success = await self._authenticate_with_checks()
        if not success:
            return  # cancelled by /hld or /rst

        token = self.authenticator.access_token
        self.shoonya.update_token(token)
        self.penny.update_token(token)

    async def _authenticate_with_checks(self) -> bool:
        """Run auth flow, respecting cancellation from /hld and /rst."""
        self.authenticator.reset_cancel()
        result = await self.authenticator.authenticate()
        return result is not False

    async def _load_universe(self):
        """Symbols, sectors and tokens from Supabase.

        Supabase calls run in worker threads: the event loop is shared with the backend.
        Retried: a transient DB error here must not end the supervisor for good.
        """
        while True:
            try:
                self._shoonya_symbols = await asyncio.to_thread(
                    self.symbol_manager.load_symbols, "shoonya")
                self._penny_symbols = await asyncio.to_thread(
                    self.symbol_manager.load_symbols, "penny")
                self._sectors = await asyncio.to_thread(self.symbol_manager.load_sector_mapping)
                self._instrument_tokens = await asyncio.to_thread(
                    self.symbol_manager.load_instrument_tokens)
                break
            except Exception as e:
                log.error(f"Supabase symbol load failed, retrying in {SUPERVISOR_RETRY_DELAY}s: {e}")
                await asyncio.sleep(SUPERVISOR_RETRY_DELAY)
        init_sector_mapping(self._sectors)

    async def _maybe_refresh_instruments(self) -> bool:
        """Once a day from 08:30 IST, re-read Shoonya's symbol file. True when names changed.

        A failed download is retried a few times, then yesterday's tokens stay in use
        for the day; the operator hears about the first failure and about every change.
        """
        now = datetime.now(IST)
        today = now.strftime("%d-%m-%Y")
        if not should_refresh_instruments(now.strftime("%H:%M"), self._last_refresh_date, today):
            return False
        if self._refresh_retry_at is not None and time.monotonic() < self._refresh_retry_at:
            return False
        try:
            plan = await asyncio.to_thread(self.instrument_refresher.run)
        except Exception as e:
            self._refresh_failures += 1
            log.error(f"Instrument refresh failed "
                      f"({self._refresh_failures}/{INSTRUMENT_REFRESH_ATTEMPTS}): {e}")
            if self._refresh_failures == 1:
                await self.login_sender.send_async(instrument_refresh_failed_message(str(e)))
            if self._refresh_failures >= INSTRUMENT_REFRESH_ATTEMPTS:
                self._last_refresh_date = today          # give up for today
                self._refresh_failures = 0
                self._refresh_retry_at = None
            else:
                self._refresh_retry_at = time.monotonic() + INSTRUMENT_REFRESH_RETRY
            return False
        self._last_refresh_date = today
        self._refresh_failures = 0
        self._refresh_retry_at = None
        if plan.aborted or plan.renames or plan.decisions or plan.missing:
            await self.login_sender.send_async(instrument_refresh_message(plan))
        return plan.changed

    async def run(self):
        """Main supervisor loop."""
        # Start summary scheduler (runs independently at 16:30 IST)
        generators = [self.shoonya_summary.generator, self.penny_summary.generator]
        summary_scheduler = SummaryScheduler(generators)
        # Keep a strong reference: the event loop only holds a weak one, so an
        # un-stored task can be garbage-collected mid-flight, silently killing
        # the 16:30 summaries for the rest of the process's life.
        self._summary_task = asyncio.create_task(summary_scheduler.run())

        # Today's symbol file first, so the names and tokens loaded below are current.
        await self._maybe_refresh_instruments()
        await self._load_universe()

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

                # Daily 08:30 IST symbol-file refresh. A rename means the detectors and
                # the feed are built on stale names: reload and rebuild them.
                if await self._maybe_refresh_instruments():
                    log.info("Instrument names changed — reloading symbols and rebuilding")
                    await self._stop_detectors()
                    await self._close_dispatcher_async()
                    await self._load_universe()
                    self._services_built = False

                if self.restart_requested:
                    self.restart_requested = False
                    self._feed_parked = False
                    self._feed_rejections = 0
                    await self._stop_detectors()
                    await self._close_dispatcher_async()
                    await self._re_authenticate()
                    continue

                if self.on_hold:
                    await asyncio.sleep(SUPERVISOR_POLL)
                    continue

                if self.config.scheduling_enabled and not is_market_hours():
                    if self.shoonya.is_running or self.penny.is_running or self._dispatcher:
                        log.info("Outside market hours, stopping detectors")
                        await self._stop_detectors()
                        await self._close_dispatcher_async()
                    await asyncio.sleep(60)
                    continue

                if self._feed_parked:
                    await asyncio.sleep(SUPERVISOR_POLL)
                    continue

                # Authenticate if we don't have a token yet
                if not self.authenticator.is_authenticated:
                    success = await self._authenticate_with_checks()
                    if not success:
                        # Auth was cancelled by /hld or /rst — loop back
                        continue

                # Build services once after first successful auth (rebuilt after
                # each daily reset so they pick up the fresh token cleanly).
                if not self._services_built:
                    token = self.authenticator.access_token
                    await asyncio.to_thread(
                        self.shoonya.build, token, self._shoonya_symbols, self._sectors)
                    await asyncio.to_thread(
                        self.penny.build, token, self._penny_symbols, self._sectors)
                    self._services_built = True

                if self._dispatcher and self._any_token_expired():
                    await self._on_feed_rejected()
                    continue

                # Nothing reports a socket that has died or given up
                # reconnecting, so poll for it. Without this a drop stays
                # invisible and detection silently stops for the rest of the
                # day while /health still reports healthy.
                if self._dispatcher and should_rebuild_socket(
                    self._dispatcher.needs_rebuild(),
                    self._last_ws_rebuild, time.monotonic(),
                ):
                    log.error("WebSocket unhealthy — rebuilding dispatcher")
                    was_up, noticed = self._feed_announced, self._feed_down_noticed
                    silent_since, reminders = self._feed_silent_since, self._silence_alerts
                    await self._close_dispatcher_async()
                    self._last_ws_rebuild = time.monotonic()
                    # One alert per outage; recovering sends Monitoring Started again.
                    self._feed_down_noticed = noticed or was_up
                    # The outage clock keeps running across rebuilds, until ticks are back.
                    self._feed_silent_since = silent_since or (time.monotonic() if was_up else None)
                    self._silence_alerts = reminders
                    if was_up:
                        await self.login_sender.send_async(websocket_rebuild_message())

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

                await self._report_feed_state()

                if self.on_hold or self.restart_requested:
                    await self._close_dispatcher_async()
                    continue

                self.shoonya.start()
                self.penny.start()

                await asyncio.sleep(SUPERVISOR_POLL)

            except Exception as e:
                log.error(f"Supervisor error: {e}")
                await asyncio.sleep(SUPERVISOR_ERROR_DELAY)
