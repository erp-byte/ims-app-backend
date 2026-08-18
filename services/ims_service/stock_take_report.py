"""Stock take daily mail — built at 7:00 PM IST, same cut-off as the daily report.

SCHEDULE (registered in main.py)
    19:00 IST daily   the day's count. Sent every day, including days with no
                      count: a mail that only arrives when someone counted is
                      indistinguishable from a mail that failed to send, and the
                      quiet version carries the verification backlog and the
                      days-since-last-count that nobody otherwise looks at.
    10:30 IST daily   yesterday re-sent ONLY if it changed after the cut-off, or
                      if the evening send never happened.

THREADING
    One conversation per business day, anchored on a Message-ID derived from the
    date alone. A revision replies into that thread rather than opening a second
    inbox row, and the subject stays byte-identical because Gmail splits a
    conversation the moment the subject changes — "REVISED" lives in the body.
"""
from __future__ import annotations

import smtplib
from datetime import date, timedelta
from email.message import EmailMessage

from sqlalchemy.orm import Session

from shared.config_loader import settings
from shared.database import SessionLocal
from shared.logger import get_logger
from shared.timezone import now_ist
from shared.mail_identity import Module, SubjectPolicy, stamp
from services.ims_service.report_delivery import Ledger
from services.ims_service.stock_take import build
from services.ims_service.stock_take_html import (
    render_email, render_page, render_plain,
)

logger = get_logger("stock_take_report")

# Named by the business. Every address checked against the live roster:
# `stocktake_users` carries Yash, Sunil, Hrithik, Digamber, R M Patil and
# Satyendra; Rakesh Ratra is in `auth_user` as a business head.
REPORT_TO = [
    "yash@candorfoods.in",              # Yash Gawdi
    "sunil.jasoria@candorfoods.in",     # Sunil Jasoria
    "b.hrithik@candorfoods.in",         # B Hrithik
    "digamber.sawant@candorfoods.in",   # Digamber Sawant
    "rmpatil@candorfoods.in",           # R M Patil
    "satyendra@candorfoods.in",         # Satyendra Garg
    "rakesh@candorfoods.in",            # Rakesh Ratra
    "sachin.more@candorfoods.in",       # Sachin More
]
REPORT_CC: list[str] = []

EVENING_HOUR, EVENING_MIN = 19, 0
MORNING_HOUR, MORNING_MIN = 10, 30

LEDGER = Ledger("stock_take_report_log", REPORT_TO + REPORT_CC)


def view_url(day: date) -> str | None:
    base = (settings.BACKEND_URL or "").rstrip("/")
    return f"{base}/stock-take/view?day={day:%Y-%m-%d}" if base else None


def day_anchor(day: date) -> str:
    return f"stock-take-{day:%Y-%m-%d}@candorfoods.in"


def day_subject(day: date) -> str:
    """One subject per business day — identical for a report and its revision."""
    return f"Stock Take Report — {day:%a, %d %b %Y}"


def _send_mail(subject: str, html_body: str, plain_body: str,
               to: list[str], cc: list[str], *, day: date,
               revised: bool = False, message_id: str | None = None,
               in_reply_to: str | None = None) -> None:
    """Send synchronously and raise on failure — the caller records the outcome."""
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = settings.SMTP_EMAIL
    msg["To"] = ", ".join(to)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg.set_content(plain_body)
    msg.add_alternative(html_body, subtype="html")

    if message_id:
        msg["Message-ID"] = f"<{message_id}>"
    if in_reply_to:
        msg["In-Reply-To"] = f"<{in_reply_to}>"
        msg["References"] = f"<{in_reply_to}>"

    stamp(msg, module=Module.REPORTS, policy=SubjectPolicy.ANCHOR,
          entity_type="StockTakeReport", entity_id=f"{day:%Y-%m-%d}",
          event="STOCK_TAKE_REPORT_REVISED" if revised else "STOCK_TAKE_REPORT",
          status="updated" if revised else "report",
          sender=settings.SMTP_EMAIL)

    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=60) as server:
        server.starttls()
        server.login(settings.SMTP_EMAIL, settings.SMTP_APP_PASSWORD)
        server.send_message(msg, to_addrs=to + cc)


def send_report(day: date, *, kind: str, revised: bool = False,
                db: Session | None = None,
                to_override: list[str] | None = None,
                cc_override: list[str] | None = None) -> dict:
    """Build and email the stock take report for `day`. Never raises."""
    own_db = db is None
    db = db or SessionLocal()
    try:
        LEDGER.ensure(db)
        rep = build(db, day)
        fp = rep["fingerprint"]

        claim_id = None
        first_send = LEDGER.last_sent(db, day) is None
        if kind in ("evening", "revision", "catchup"):
            claim_id = LEDGER.claim(db, day, kind, fp)
            if claim_id is None:
                logger.info("Stock take report %s (%s): an identical send is "
                            "already recorded — skipping this duplicate", day, kind)
                return {"day": str(day), "status": "skipped_duplicate", "sent": False}

        generated = now_ist()
        html = render_email(day, rep, generated, revised=revised,
                            view_url=view_url(day))
        plain = render_plain(day, rep, revised)
        anchor = day_anchor(day)
        to_list = to_override if to_override is not None else REPORT_TO
        cc_list = cc_override if cc_override is not None else REPORT_CC

        try:
            _send_mail(day_subject(day), html, plain, to_list, cc_list,
                       day=day, revised=revised,
                       message_id=anchor if first_send else None,
                       in_reply_to=None if first_send else anchor)
        except Exception:
            # The claim was written as 'sent' before the mail left. Hand it back
            # so the day is still owed and the morning catch-up retries it.
            if claim_id is not None:
                LEDGER.release(db, claim_id, "SMTP send failed")
            raise

        if claim_id is None:
            LEDGER.log(db, day, kind, "sent", fp)

        h = rep["agg"]["head"]
        logger.info("Stock take report %s (%s) sent to %d recipients — "
                    "%d entries, %.2f kg, %d counted, %d did not",
                    day, kind, len(to_list) + len(cc_list), h["n"], h["kg"],
                    len(rep["roster"]["entered"]), len(rep["roster"]["missing"]))
        return {"day": str(day), "status": "sent", "sent": True, "kind": kind,
                "revised": revised, "fingerprint": fp,
                "entries": h["n"], "kg": round(h["kg"], 2),
                "counted_by": len(rep["roster"]["entered"]),
                "did_not_count": len(rep["roster"]["missing"]),
                "unverified": rep["outstanding"]["unverified"]}
    except Exception as exc:                                       # noqa: BLE001
        logger.error("Stock take report %s (%s) FAILED: %s", day, kind, exc)
        try:
            db.rollback()
            LEDGER.log(db, day, kind, "failed", error=str(exc)[:1000])
        except Exception:                                          # noqa: BLE001
            logger.error("Could not record the stock take failure for %s", day)
        return {"day": str(day), "status": "failed", "sent": False, "error": str(exc)}
    finally:
        if own_db:
            db.close()


def run_evening_report() -> dict:
    """19:00 IST. Sent every day — a quiet day is a result, not a non-event."""
    return send_report(now_ist().date(), kind="evening")


def run_morning_revision() -> dict:
    """10:30 IST. Re-send yesterday only if it changed, or was never sent."""
    day = now_ist().date() - timedelta(days=1)
    db = SessionLocal()
    try:
        LEDGER.ensure(db)
        prev = LEDGER.last_sent(db, day)
        fp = build(db, day)["fingerprint"]

        if prev is None:
            logger.warning("No stock take report was sent for %s — "
                           "sending catch-up now", day)
            return send_report(day, kind="catchup", db=db)
        if prev[0] != fp:
            logger.info("Stock take figures for %s changed after the 19:00 "
                        "cut-off — sending revision", day)
            return send_report(day, kind="revision", revised=True, db=db)

        LEDGER.log(db, day, "revision", "skipped_unchanged", fp)
        logger.info("No late stock take entries for %s — no revision sent", day)
        return {"day": str(day), "status": "skipped_unchanged", "sent": False}
    except Exception as exc:                                       # noqa: BLE001
        logger.error("Stock take revision check for %s failed: %s", day, exc)
        return {"day": str(day), "status": "failed", "sent": False, "error": str(exc)}
    finally:
        db.close()
