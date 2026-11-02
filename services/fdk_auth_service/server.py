"""Telegram webhook endpoint for receiving commands (/hld /rst /snd /sdt).

The Fyers browser-redirect login (/auth/callback and pasted auth codes) is gone;
the Shoonya login is headless, so this router only carries commands.
"""
from fastapi import APIRouter, Request
from shared.fdk_logger import get_logger
from services.fdk_telegram_service.command_parser import parse_command, parse_date
from services.fdk_telegram_service.message_template import (
    hold_message, restart_message,
    summary_generating_message, summary_date_prompt_message,
    summary_date_invalid_message, summary_date_confirm_message,
)

log = get_logger("auth_webhook")

router = APIRouter()

_orchestrator = None
_login_sender = None
_pending_summary_date = False


def init_auth_router(orchestrator=None, login_sender=None):
    global _orchestrator, _login_sender
    _orchestrator = orchestrator
    _login_sender = login_sender
    return router


@router.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    """Receive Telegram webhook updates containing commands."""
    global _pending_summary_date

    try:
        body = await request.json()
    except Exception:
        log.warning("Malformed request body, ignoring")
        return {"status": "ok"}

    message = body.get("message", {})
    text = message.get("text", "")
    log.info(f"Webhook received: {text!r}")

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

    # Check if we're waiting for a date (from /sdt)
    if _pending_summary_date and text.strip():
        return await _handle_summary_date_input(text)

    return {"status": "ok"}


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
    await _orchestrator.fyers_summary.send_today()
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
    await _orchestrator.fyers_summary.send_for_date(target_date)
    await _orchestrator.penny_summary.send_for_date(target_date)

    return {"status": "summary_sent", "date": date_str}
