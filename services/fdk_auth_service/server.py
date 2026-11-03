"""Inbound Telegram messages: commands (/hld /rst /snd /sdt) and, while the Shoonya
login is waiting for one, the pasted OAuth redirect URL (carries code=...).

handle_text() is the single dispatcher; the webhook route and the long poller
(services/fdk_telegram_service/poller.py) both feed it. Messages are accepted
only from the configured bot chats (a message without a chat is a forgery:
Telegram always sends one). The webhook route is live only in webhook mode and
additionally requires Telegram's secret token header when
FDK_TELEGRAM_WEBHOOK_SECRET is set.
"""
import hmac

from fastapi import APIRouter, Request
from starlette.responses import JSONResponse

from shared.fdk_logger import get_logger
from services.fdk_telegram_service.command_parser import parse_command, parse_date, redact_code
from services.fdk_telegram_service.message_template import (
    hold_message, restart_message,
    summary_generating_message, summary_date_prompt_message,
    summary_date_invalid_message, summary_date_confirm_message,
)

log = get_logger("auth_webhook")

router = APIRouter()

_orchestrator = None
_login_sender = None
_allowed_chat_ids: set[str] = set()
_webhook_secret = ""
_webhook_enabled = True       # False in polling mode: the poller is then the only inbound path
_pending_summary_date = False

SECRET_HEADER = "X-Telegram-Bot-Api-Secret-Token"


def init_auth_router(orchestrator=None, login_sender=None, allowed_chat_ids=None,
                     webhook_secret: str = "", webhook_enabled: bool = True):
    global _orchestrator, _login_sender, _allowed_chat_ids, _webhook_secret, _webhook_enabled
    _orchestrator = orchestrator
    _login_sender = login_sender
    _allowed_chat_ids = {str(c) for c in (allowed_chat_ids or []) if c}
    _webhook_secret = webhook_secret or ""
    _webhook_enabled = bool(webhook_enabled)
    return router


@router.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    """Receive Telegram webhook updates containing commands."""
    if not _webhook_enabled:
        # Polling mode: no webhook is registered, so nothing legitimate arrives here.
        return JSONResponse({"detail": "not found"}, status_code=404)
    if _webhook_secret and not hmac.compare_digest(
            request.headers.get(SECRET_HEADER, "").encode("utf-8"), _webhook_secret.encode("utf-8")):
        log.warning("Telegram webhook call without the expected secret token — ignored")
        return JSONResponse({"detail": "forbidden"}, status_code=403)

    try:
        body = await request.json()
    except Exception:
        log.warning("Malformed request body, ignoring")
        return {"status": "ok"}

    message = body.get("message") or {}
    text = message.get("text", "")
    chat_id = (message.get("chat") or {}).get("id")
    log.info(f"Webhook received: {redact_code(text)!r}")
    return await handle_text(text, chat_id)


async def handle_text(text: str, chat_id=None) -> dict:
    """Dispatch one inbound Telegram message (from the webhook or the poller)."""
    global _pending_summary_date

    # Fail closed: once chats are configured, no chat (None) is as foreign as a wrong one.
    if _allowed_chat_ids and str(chat_id) not in _allowed_chat_ids:
        log.warning(f"Ignoring a Telegram message from chat {chat_id} (not a configured fdk chat)")
        return {"status": "ignored_chat"}

    # Check for commands first
    command, rest = parse_command(text)

    # Any command cancels pending /sdt state
    if command:
        _pending_summary_date = False

    if command == "/hld":
        return await _handle_hold(rest)

    if command == "/rst":
        return await _handle_restart()

    if command == "/snd":
        return await _handle_send_summary()

    if command == "/sdt":
        return await _handle_summary_date_prompt()

    # The operator pasted the Shoonya redirect URL (or tried to)
    if "code=" in text:
        return await _handle_pasted_code(text)

    # Check if we're waiting for a date (from /sdt)
    if _pending_summary_date and text.strip():
        return await _handle_summary_date_input(text)

    return {"status": "ok"}


async def _handle_pasted_code(text: str) -> dict:
    auth = getattr(_orchestrator, "authenticator", None)
    if auth is None or not getattr(auth, "awaiting_manual_login", False):
        if _login_sender:
            await _login_sender.send_async(
                "No login is waiting for a code right now. Send /rst to start a fresh login.")
        return {"status": "code_ignored"}
    if auth.submit_code(text):
        log.info("Shoonya authorization code received via Telegram")
        return {"status": "code_received"}
    if _login_sender:
        await _login_sender.send_async(
            "No authorization code found in that message — paste the full URL you landed on "
            "after logging in (it contains <code>?code=...</code>).")
    return {"status": "code_rejected"}


async def _handle_hold(reason: str):
    """Handle /hld command — stop all detectors."""
    if not _orchestrator:
        return {"status": "error", "message": "orchestrator not ready"}

    await _orchestrator.hold_async()
    log.info(f"Hold command received. Reason: {reason or 'none given'}")

    if _login_sender:
        await _login_sender.send_async(hold_message(reason))

    return {"status": "hold_activated"}


async def _handle_restart():
    """Handle /rst command — restart all services and re-authenticate."""
    if not _orchestrator:
        return {"status": "error", "message": "orchestrator not ready"}

    if _login_sender:
        await _login_sender.send_async(restart_message())

    _orchestrator.request_restart()
    log.info("Restart command received")

    return {"status": "restart_requested"}


async def _handle_send_summary():
    """Handle /snd command — generate and send today's summaries."""
    if not _orchestrator:
        return {"status": "error", "message": "orchestrator not ready"}

    if _login_sender:
        await _login_sender.send_async(summary_generating_message())

    log.info("Send summary command received")
    await _orchestrator.shoonya_summary.send_today()
    await _orchestrator.penny_summary.send_today()

    return {"status": "summary_sent"}


async def _handle_summary_date_prompt():
    """Handle /sdt command — prompt user for a date."""
    global _pending_summary_date
    _pending_summary_date = True

    if _login_sender:
        await _login_sender.send_async(summary_date_prompt_message())

    log.info("Summary date prompt sent, waiting for date input")
    return {"status": "awaiting_date"}


async def _handle_summary_date_input(text: str):
    """Handle date input after /sdt command."""
    global _pending_summary_date

    target_date = parse_date(text.strip())
    if not target_date:
        if _login_sender:
            await _login_sender.send_async(summary_date_invalid_message())
        log.warning(f"Invalid date input: {text}")
        # Stay in pending state so user can retry
        return {"status": "invalid_date"}

    _pending_summary_date = False
    date_str = target_date.strftime("%d-%m-%Y")
    day_name = target_date.strftime("%A")

    if _login_sender:
        await _login_sender.send_async(
            summary_date_confirm_message(date_str, day_name)
        )

    log.info(f"Generating summaries for {date_str} ({day_name})")
    await _orchestrator.shoonya_summary.send_for_date(target_date)
    await _orchestrator.penny_summary.send_for_date(target_date)

    return {"status": "summary_sent", "date": date_str}
