"""Telegram message templates."""
from shared.fdk_models import TradeAlert


def trade_alert_message(alert: TradeAlert) -> str:
    return (
        f"<b>LARGE TRADE DETECTED</b>\n\n"
        f"<b>Symbol:</b> {alert.symbol}\n"
        f"<b>Sector:</b> {alert.sector}\n"
        f"<b>LTP:</b> Rs{alert.ltp:,.2f}\n"
        f"<b>Volume:</b> {alert.volume_spike:,}\n"
        f"<b>Value:</b> Rs{alert.trade_value_cr:.2f} Cr\n"
        f"<b>Type:</b> {alert.spike_type}\n"
        f"<b>Time:</b> {alert.timestamp.strftime('%H:%M:%S')}"
    )


def shoonya_login_link_message(login_url: str) -> str:
    return (
        f"<b>Shoonya login needed</b>\n\n"
        f"Open this link and log in with your Shoonya user id, password and OTP/TOTP:\n"
        f"{login_url}\n\n"
        f"Then paste the full URL of the page you land on here "
        f"(it contains <code>code=...</code>).\n"
        f"<i>Link resent every 5 minutes until logged in.</i>"
    )


def shoonya_login_paused_message() -> str:
    return (
        "<b>Shoonya login paused</b>\n\n"
        "No login is needed outside market hours. The link will be sent again at the "
        "next open (09:13 IST) — or send /rst to get it now."
    )


def auth_success_message(broker: str = "Shoonya", detail: str = "Session active.") -> str:
    return f"<b>{broker} Authentication Successful!</b>\n{detail}"


def auth_failure_message(error: str, broker: str = "Shoonya") -> str:
    return f"<b>{broker} Authentication Failed</b>\n\nError: {error}"


def hold_message(reason: str = "") -> str:
    msg = "<b>Detectors ON HOLD</b>\n\nAll monitoring stopped."
    if reason:
        msg += f"\n<b>Reason:</b> {reason}"
    msg += "\n\nSend /rst to restart."
    return msg


def restart_message() -> str:
    return "<b>Restarting...</b>\n\nRe-authenticating and restarting all detectors."


def restart_complete_message() -> str:
    return "<b>Restart Complete</b>\n\nAll detectors are back online."


def monitoring_started_message(fyers_count: int, penny_count: int) -> str:
    return (
        f"<b>Monitoring Started</b>\n\n"
        f"Fyers: {fyers_count} symbols\n"
        f"Penny: {penny_count} symbols"
    )


def summary_generating_message() -> str:
    return "<b>Generating summaries...</b>\n\nPlease wait."


def summary_date_prompt_message() -> str:
    return "<b>Send the date</b> (DD-MM-YYYY):"


def summary_date_invalid_message() -> str:
    return "<b>Invalid date format.</b>\n\nUse DD-MM-YYYY (e.g. 28-02-2026)"


def summary_date_confirm_message(date_str: str, day_name: str) -> str:
    return f"<b>Generating summaries for {date_str} ({day_name})...</b>\n\nPlease wait."


def websocket_rebuild_message() -> str:
    return (
        "<b>WebSocket Rebuild</b>\n\n"
        "The Fyers feed stopped delivering ticks (dropped socket, exhausted "
        "reconnects, or a subscription that never landed).\n"
        "Tearing it down and reconnecting now."
    )
