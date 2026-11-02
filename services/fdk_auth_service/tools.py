"""Auth service utility tools."""
import requests
from shared.fdk_logger import get_logger
from shared.fdk_constants import BASE_URL, WEBHOOK_PATH

log = get_logger("auth_tools")


def register_telegram_webhook(bot_token: str):
    """Register the Telegram webhook URL with the Bot API."""
    webhook_url = f"{BASE_URL}{WEBHOOK_PATH}"
    resp = requests.post(
        f"https://api.telegram.org/bot{bot_token}/setWebhook",
        json={"url": webhook_url, "allowed_updates": ["message"]},
        timeout=10
    )
    data = resp.json()
    if data.get("ok"):
        log.info(f"Telegram webhook set: {webhook_url}")
    else:
        log.error(f"Failed to set webhook: {data}")
    return data


def register_bot_commands(bot_token: str):
    """Register bot commands so they show in Telegram's command menu."""
    commands = [
        {"command": "hld", "description": "Hold — stop all detectors (market closed/holiday)"},
        {"command": "rst", "description": "Restart — re-authenticate and restart all detectors"},
        {"command": "snd", "description": "Send today's summary now"},
        {"command": "sdt", "description": "Send summary for a specific date"},
    ]
    resp = requests.post(
        f"https://api.telegram.org/bot{bot_token}/setMyCommands",
        json={"commands": commands},
        timeout=10
    )
    data = resp.json()
    if data.get("ok"):
        log.info("Bot commands registered: /hld, /rst, /snd, /sdt")
    else:
        log.error(f"Failed to register commands: {data}")
    return data


def delete_telegram_webhook(bot_token: str):
    """Remove the Telegram webhook."""
    resp = requests.post(
        f"https://api.telegram.org/bot{bot_token}/deleteWebhook",
        timeout=10
    )
    return resp.json()
