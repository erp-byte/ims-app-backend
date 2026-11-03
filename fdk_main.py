"""fdk — Shoonya volume-spike detector, hosted inside the Candor backend.

Adapted from fyersdk/main.py (the standalone FastAPI entry point). The backend
touches fdk in exactly three marked places, all in main.py:

    from fdk_main import fdk_lifespan, fdk_mount
    async with fdk_lifespan():      # wraps the backend lifespan's `yield`
        yield
    fdk_mount(app)                  # after the other include_router calls

Routes (all under FDK_ROUTE_PREFIX = "/fdk"):
    GET  /fdk/health            detector + login status
    POST /fdk/webhook/telegram  Telegram commands /hld /rst /snd /sdt (+ pasted login URL)
    *    /fdk/mcp               MCP endpoint (or /fdk/mcp-<FDK_MCP_PATH_SECRET>)

Telegram inbound: with FDK_BASE_URL set, webhooks are registered on all 5 bots;
without it (no public HTTPS, e.g. the EC2 box) the login bot is long-polled instead.
Only messages from the configured chats are handled.

Nothing starts unless FDK_ENABLED is truthy. A failed fdk start-up (missing
FDK_* env, Supabase down, dependency missing) is logged and the backend carries
on without the detector. Every fdk path is listed in FDK_FILES.md.
"""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import APIRouter, FastAPI
from starlette.responses import JSONResponse

from shared.fdk_config_loader import AppConfig, fdk_enabled, load_config
from shared.fdk_constants import (
    BASE_URL, FDK_ROUTE_PREFIX, FDK_SELF_PING, FDK_STARTUP_TIMEOUT, IST,
)
from shared.fdk_logger import get_logger

log = get_logger("main")


class FdkState:
    """Everything fdk owns at runtime, kept in one place so removal is one module drop."""

    def __init__(self) -> None:
        self.enabled: bool = False
        self.config: AppConfig | None = None
        self.orchestrator: Any = None
        self.mcp: Any = None
        self.mcp_app: Any = None          # ASGI app, set once the lifespan has built it
        self.task: asyncio.Task | None = None
        self.poller_task: asyncio.Task | None = None   # Telegram long polling (no FDK_BASE_URL)
        self.telegram_mode: str = "none"               # webhook / webhook_failed / polling / none
        self.error: str | None = None


fdk_state = FdkState()

fdk_router = APIRouter(prefix=FDK_ROUTE_PREFIX, tags=["fdk"])


def supervisor_failure(task: "asyncio.Task | None") -> str | None:
    """Why the supervisor task is no longer running, or None while it runs (or never ran)."""
    if task is None or not task.done():
        return None
    if task.cancelled():
        return "cancelled"
    exc = task.exception()
    return repr(exc) if exc else "exited"


@fdk_router.get("/health")
async def fdk_health_check():
    now = datetime.now(IST).isoformat()
    if not fdk_state.enabled:
        return {"status": "disabled", "timestamp": now}
    orchestrator = fdk_state.orchestrator
    if orchestrator is None:
        return {"status": "unavailable", "timestamp": now, "error": fdk_state.error}
    failure = supervisor_failure(fdk_state.task)
    if failure is not None:
        # The supervisor loop died (or was cancelled): nothing will log in or
        # detect until the backend restarts, so say so instead of "healthy".
        return {"status": "stopped", "timestamp": now, "broker": orchestrator.broker,
                "error": failure}
    from services.fdk_supervisor_service.schedular import is_market_hours

    auth = orchestrator.authenticator
    last_login = getattr(auth, "last_login_at", None)
    return {
        "status": "on_hold" if orchestrator.on_hold else "healthy",
        "timestamp": now,
        "broker": orchestrator.broker,
        # ok / failed / on_hold / waiting_for_market_hours / in_progress
        "login_state": orchestrator.login_state(),
        "authenticated": auth.is_authenticated,
        "login_method": getattr(auth, "login_method", None),     # cached / auto / manual
        "last_login_at": last_login.isoformat() if last_login else None,
        "last_auth_error": getattr(auth, "last_error", None),
        "telegram_mode": fdk_state.telegram_mode,
        "market_hours": is_market_hours(),
        "scheduling_enabled": orchestrator.config.scheduling_enabled,
        "shoonya_running": orchestrator.shoonya.is_running,
        "penny_running": orchestrator.penny.is_running,
        "feed": orchestrator.feed_status(),
    }


async def _fdk_mcp_asgi(scope, receive, send):
    """Forwards to the MCP app once the lifespan has built it; 503 before/without it."""
    mcp_app = fdk_state.mcp_app
    if mcp_app is None:
        if scope["type"] == "http":
            response = JSONResponse(
                {"detail": "fdk MCP endpoint is not available"}, status_code=503
            )
            await response(scope, receive, send)
        return
    await mcp_app(scope, receive, send)


def fdk_mount(app: FastAPI) -> None:
    """Attach the fdk routes and the MCP mount to the backend app."""
    app.include_router(fdk_router)
    try:
        # Importing the auth package pulls in pyotp/psycopg2; keep the backend
        # bootable even if the fdk dependencies are not installed.
        from services.fdk_auth_service.server import router as fdk_auth_router
    except ImportError as exc:
        fdk_state.error = f"fdk dependencies missing: {exc}"
        log.error("fdk auth/webhook routes not mounted — %s "
                  "(pip install -r requirements.txt)", exc)
    else:
        app.include_router(fdk_auth_router, prefix=FDK_ROUTE_PREFIX, tags=["fdk"])
    # Mounted last so it never shadows the routes above (as in the original main.py).
    app.mount(FDK_ROUTE_PREFIX, _fdk_mcp_asgi, name="fdk_mcp")


def _build_detector():
    """Synchronous construction; mirrors the module-level section of fyersdk/main.py."""
    from services.fdk_supervisor_service.ochestrator import Orchestrator

    config = load_config()
    orchestrator = Orchestrator(config)
    return config, orchestrator


def _register_telegram(config: AppConfig) -> list[str]:
    """Webhook mode: register the webhook on all 5 bots. Polling mode (no public
    HTTPS URL): drop the login bot's webhook so getUpdates is allowed. Both: the
    command menu on the login bot. Returns the chat ids whose webhook could not be
    set — one bot's failure must not leave the others unregistered."""
    from services.fdk_auth_service.tools import (
        delete_telegram_webhook, register_bot_commands, register_telegram_webhook,
    )

    failed: list[str] = []
    if BASE_URL:
        for channel in [
            config.telegram.login,
            config.telegram.shoonya_trade,
            config.telegram.shoonya_summary,
            config.telegram.penny_trade,
            config.telegram.penny_summary,
        ]:
            try:
                data = register_telegram_webhook(
                    channel.bot_token, secret_token=config.telegram.webhook_secret or None)
            except Exception as exc:
                log.error("setWebhook failed for chat %s: %s", channel.chat_id, exc)
                failed.append(channel.chat_id)
            else:
                if not data.get("ok"):
                    failed.append(channel.chat_id)
    else:
        log.info("FDK_BASE_URL is not set — long-polling the login bot for commands "
                 "and the pasted Shoonya login URL (no webhooks)")
        delete_telegram_webhook(config.telegram.login.bot_token)
    register_bot_commands(config.telegram.login.bot_token)
    return failed


def _start_telegram_poller(config: AppConfig) -> asyncio.Task:
    from services.fdk_auth_service.server import handle_text
    from services.fdk_telegram_service.poller import TelegramPoller

    login = config.telegram.login
    # The poller only forwards messages from the login chat; say so to the
    # dispatcher, whose chat allowlist fails closed.
    return asyncio.create_task(
        TelegramPoller(login.bot_token, lambda text: handle_text(text, login.chat_id),
                       chat_id=login.chat_id).run())


def _log_supervisor_exit(task: asyncio.Task) -> None:
    """A supervisor that dies with an exception would otherwise vanish silently."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        log.error("fdk supervisor stopped: %r — /fdk/health reports 'stopped' until restart", exc)


def _build_mcp(orchestrator):
    from services.fdk_mcp_service import build_mcp

    mcp = build_mcp(orchestrator)
    return mcp, mcp.streamable_http_app()


@asynccontextmanager
async def fdk_lifespan():
    """Run the detector + MCP for the life of the backend; never raises into the host."""
    if not fdk_enabled():
        log.info("fdk disabled — set FDK_ENABLED=true to run the volume-spike detector")
        yield
        return

    fdk_state.enabled = True
    log.info("fdk starting up...")
    try:
        config, orchestrator = await asyncio.wait_for(
            asyncio.to_thread(_build_detector), timeout=FDK_STARTUP_TIMEOUT
        )
    except Exception as exc:   # missing env (KeyError), Supabase/psycopg2, TimeoutError, ImportError
        fdk_state.error = f"{type(exc).__name__}: {exc}"
        log.error("fdk start-up failed — backend continues without the detector: %s",
                  fdk_state.error)
        config = orchestrator = None

    fdk_state.config, fdk_state.orchestrator = config, orchestrator

    if orchestrator is not None:
        from services.fdk_auth_service.server import init_auth_router

        tg = config.telegram
        init_auth_router(
            orchestrator=orchestrator, login_sender=orchestrator.login_sender,
            allowed_chat_ids=[c.chat_id for c in (tg.login, tg.shoonya_trade, tg.shoonya_summary,
                                                  tg.penny_trade, tg.penny_summary)],
            webhook_secret=tg.webhook_secret,
            webhook_enabled=bool(BASE_URL),
        )
        fdk_state.telegram_mode = "webhook" if BASE_URL else "polling"
        try:
            failed = await asyncio.to_thread(_register_telegram, config)
        except Exception as exc:
            log.error("fdk Telegram registration failed: %s", exc)
            failed = ["(all)"]
        if failed and BASE_URL:
            # Telegram keeps delivering to the previous registration (maybe without
            # the secret), so commands may be answered 403 until a restart succeeds.
            fdk_state.telegram_mode = "webhook_failed"
            log.error("Telegram webhook not registered for chat(s) %s — /fdk/health reports "
                      "telegram_mode=webhook_failed; restart the backend to retry", ", ".join(failed))
        if fdk_state.telegram_mode == "polling":
            fdk_state.poller_task = _start_telegram_poller(config)
        if FDK_SELF_PING:
            if BASE_URL:
                from services.fdk_supervisor_service.health_monitor import start_self_ping
                start_self_ping()
            else:
                log.error("FDK_SELF_PING is set but FDK_BASE_URL is empty — self-ping NOT started")
        fdk_state.task = asyncio.create_task(orchestrator.run())
        fdk_state.task.add_done_callback(_log_supervisor_exit)

    try:
        # News tools work even without the detector; detector tools then report "unavailable".
        fdk_state.mcp, fdk_state.mcp_app = _build_mcp(orchestrator)
    except Exception as exc:
        log.error("fdk MCP endpoint not available: %s", exc)
        fdk_state.mcp = fdk_state.mcp_app = None

    try:
        if fdk_state.mcp is not None:
            async with fdk_state.mcp.session_manager.run():
                yield
        else:
            yield
    finally:
        log.info("fdk shutting down...")
        if orchestrator is not None:
            # A login may be sleeping between retries in a worker thread; without
            # this the interpreter waits for it (up to minutes) before exiting.
            orchestrator.authenticator.cancel_auth()
        # First, or the supervisor would see the closed feed below and open a new one.
        if fdk_state.task is not None:
            fdk_state.task.cancel()
        if orchestrator is not None:
            try:
                await asyncio.wait_for(orchestrator._stop_detectors(), timeout=25)
                await asyncio.wait_for(orchestrator._close_dispatcher_async(), timeout=10)
            except Exception as exc:
                log.error("Stopping the detectors or the Shoonya feed failed: %s", exc)
        if fdk_state.poller_task is not None:
            fdk_state.poller_task.cancel()
        fdk_state.mcp_app = None
