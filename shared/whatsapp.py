import httpx
from shared.config_loader import settings
from shared.logger import get_logger

logger = get_logger("whatsapp")

WHATSAPP_API_URL = (
    f"https://graph.facebook.com/v21.0/{settings.WHATSAPP_PHONE_NUMBER_ID}/messages"
)


def send_whatsapp_message(to: str, text: str) -> bool:
    """Send a free-form text message via WhatsApp Business API."""
    if not settings.WHATSAPP_ACCESS_TOKEN or not settings.WHATSAPP_PHONE_NUMBER_ID:
        logger.warning("WhatsApp credentials not configured, skipping message")
        return False

    try:
        resp = httpx.post(
            WHATSAPP_API_URL,
            headers={
                "Authorization": f"Bearer {settings.WHATSAPP_ACCESS_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "messaging_product": "whatsapp",
                "to": to,
                "type": "text",
                "text": {"body": text},
            },
            timeout=15,
        )
        if resp.status_code in (200, 201):
            logger.info("WhatsApp message sent to %s", to)
            return True
        else:
            logger.error("WhatsApp API error %s: %s", resp.status_code, resp.text)
            return False
    except Exception as exc:
        logger.error("WhatsApp send failed: %s", exc)
        return False


def send_rtv_notification(
    rtv_id: str,
    customer: str,
    lines: list,
    remark: str | None = None,
) -> bool:
    """Send RTV details via WhatsApp to the configured recipient."""
    recipient = settings.WHATSAPP_RECIPIENT
    if not recipient:
        logger.warning("WHATSAPP_RECIPIENT not set, skipping RTV notification")
        return False

    # Build article + qty lines
    article_lines = []
    for line in lines:
        desc = line.get("item_description", "")
        qty = line.get("qty", "0")
        net_wt = line.get("net_weight", "0")
        article_lines.append(f"  - {desc} | Qty: {qty} | Net Wt: {net_wt} kg")

    articles_text = "\n".join(article_lines) if article_lines else "  (none)"
    reason_text = remark if remark else "N/A"

    message = (
        f"*New RTV Created*\n"
        f"RTV ID: {rtv_id}\n"
        f"Customer: {customer}\n"
        f"Reason: {reason_text}\n\n"
        f"*Articles:*\n{articles_text}"
    )

    return send_whatsapp_message(recipient, message)
