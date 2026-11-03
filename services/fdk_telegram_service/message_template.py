"""Telegram message templates."""
import html
from shared.fdk_models import TradeAlert

COVERAGE_NAMES_SHOWN = 20


def trade_alert_message(alert: TradeAlert) -> str:
    # HTML parse mode: a bare & (M&M-EQ, "Oil & Gas") must go out as &amp;.
    return (
        f"<b>LARGE TRADE DETECTED</b>\n\n"
        f"<b>Symbol:</b> {html.escape(alert.symbol)}\n"
        f"<b>Sector:</b> {html.escape(alert.sector)}\n"
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


def monitoring_started_message(shoonya_count: int, penny_count: int) -> str:
    return (
        f"<b>Monitoring Started</b>\n\n"
        f"Shoonya: {shoonya_count} symbols\n"
        f"Penny: {penny_count} symbols"
    )


def feed_coverage_warning_message(report: dict) -> str:
    """report: from TickDispatcher.take_coverage_report; detectors are shoonya then penny."""
    shoonya_missing, penny_missing = report["missing_by_detector"]
    names = html.escape(", ".join(report["missing"][:COVERAGE_NAMES_SHOWN]))
    more = len(report["missing"]) - COVERAGE_NAMES_SHOWN
    return (
        f"<b>Feed coverage warning</b>\n\n"
        f"Shoonya acknowledged {report['acknowledged']} of {report['requested']} symbols.\n"
        f"Not monitored: Shoonya {shoonya_missing}, Penny {penny_missing}.\n"
        f"{names}{f' and {more} more (all named in the log)' if more > 0 else ''}"
    )


def feed_not_up_message(no_token: int) -> str:
    detail = f"\n{no_token} symbols have no Shoonya token." if no_token else ""
    return (
        f"<b>Feed warning</b>\n\n"
        f"The Shoonya feed is not up, so nothing is being detected. Still retrying.{detail}"
    )


def feed_rejected_message(reason: str, retrying: bool) -> str:
    next_step = ("Logging in again." if retrying else
                 "A fresh login was rejected too, so the feed stays off until this is "
                 "fixed and you send /rst.")
    return (
        f"<b>Feed rejected</b>\n\n"
        f"The Shoonya feed rejected the session: {html.escape(reason)}\n{next_step}"
    )


def summary_generating_message() -> str:
    return "<b>Generating summaries...</b>\n\nPlease wait."


def summary_date_prompt_message() -> str:
    return "<b>Send the date</b> (DD-MM-YYYY):"


def summary_date_invalid_message() -> str:
    return "<b>Invalid date format.</b>\n\nUse DD-MM-YYYY (e.g. 28-02-2026)"


def summary_date_confirm_message(date_str: str, day_name: str) -> str:
    return f"<b>Generating summaries for {date_str} ({day_name})...</b>\n\nPlease wait."


def instrument_refresh_message(plan) -> str:
    """plan: services.fdk_sector_service.instrument_refresh.RefreshPlan."""
    if plan.aborted:
        return (
            f"<b>Instrument refresh skipped</b>\n\n"
            f"{html.escape(plan.aborted)}.\nNothing was changed; yesterday's tokens stay in use."
        )
    lines = ["<b>Instrument refresh</b>", ""]
    if plan.renames:
        lines.append("Renamed (series change), feed and sheets follow the new name:")
        lines += [f"  {html.escape(old)} → {html.escape(new)}" for _, old, new, _ in plan.renames]
    if plan.decisions:
        lines.append("Not changed, needs your decision:")
        lines += [f"  {html.escape(old)} is {html.escape(new)} in Shoonya's file ({why})"
                  for _, old, new, why in plan.decisions]
    if plan.missing:
        lines.append("Missing from Shoonya's file (still on the old token, which may be dead — check the symbol):")
        lines += [f"  {html.escape(symbol)} (token {token})" for _, symbol, token in plan.missing]
    lines.append("")
    lines.append(f"<i>{len(plan.verified)} instruments verified.</i>")
    return "\n".join(lines)


def instrument_refresh_failed_message(error: str) -> str:
    return (
        f"<b>Instrument refresh failed</b>\n\n"
        f"{html.escape(error)}\n"
        f"Yesterday's tokens stay in use. Retrying a few times, then giving up for today."
    )


def feed_still_silent_message(minutes: int) -> str:
    return (
        f"<b>Feed still silent</b>\n\n"
        f"No ticks from Shoonya for {minutes} minutes during trading, rebuilds included. "
        f"Nothing is being detected. This reminder repeats until data flows again; "
        f"send /hld to silence it."
    )


def websocket_rebuild_message() -> str:
    return (
        "<b>WebSocket Rebuild</b>\n\n"
        "The Shoonya feed stopped delivering ticks (dropped socket, exhausted "
        "reconnects, or a subscription that never landed).\n"
        "Tearing it down and reconnecting now."
    )
