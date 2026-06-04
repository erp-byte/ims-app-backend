"""
IMAP-based listener that auto-approves RTVs when the assigned business head
replies "Approved" (or accepted variants) to the original creation email.

Optimization notes (do not bombard the server):
  - UID-incremental fetch: only UIDs > last processed UID per poll
  - Server-side IMAP filter (SUBJECT + SINCE) — Gmail returns only candidates
  - Headers-only first pass; full body fetched only on subject match
  - Single persistent IMAP connection, reused across polls with NOOP keepalive
  - Process-level lock so two polls never overlap
  - Active-hours throttling (07:00–23:00 IST)
  - Circuit breaker with exponential backoff after consecutive failures
  - Cap of MAX_PER_POLL messages per cycle
  - Gmail label marks processed messages server-side
  - DB session opened only when there is actual work
"""

from __future__ import annotations

import email
import imaplib
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from email.utils import parseaddr
from typing import Optional

from sqlalchemy import text

from shared.config_loader import settings
from shared.database import SessionLocal
from shared.email_notifier import BUSINESS_HEAD_EMAILS, notify_rtv_approved
from shared.logger import get_logger

logger = get_logger("email.reply_listener")

# ── Constants ────────────────────────────────────────────────────────

_RTV_ID_RE = re.compile(r"RTV-\d{14}")
_APPROVAL_RE = re.compile(
    r"^\s*(approved?|approve\s*it|ok(?:ay)?|yes)\s*[.!]?\s*$",
    re.IGNORECASE,
)
_QUOTE_HEADER_RE = re.compile(
    r"^(On .+ wrote:|From:|Sent:|To:|Subject:|-----Original Message-----)",
    re.IGNORECASE,
)

PROCESSED_LABEL = "RTV-Auto-Approved"
MAX_PER_POLL = 50
ACTIVE_START_HOUR = 7   # IST
ACTIVE_END_HOUR = 23    # IST
INITIAL_LOOKBACK_DAYS = 1
SEARCH_LOOKBACK_DAYS = 7
BACKOFF_MINUTES = (10, 30, 60)

# ── Module-level state ──────────────────────────────────────────────

_LOCK = threading.Lock()
_CONNECTION: Optional[imaplib.IMAP4_SSL] = None
_FAILURES = 0
_BACKOFF_UNTIL: Optional[float] = None


# ── Utility helpers ─────────────────────────────────────────────────


def _ist_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))


def _is_active_hours() -> bool:
    return ACTIVE_START_HOUR <= _ist_now().hour < ACTIVE_END_HOUR


def _imap_date(days_ago: int) -> str:
    """IMAP SEARCH date format: DD-Mon-YYYY (English month abbreviation)."""
    d = _ist_now() - timedelta(days=days_ago)
    return d.strftime("%d-%b-%Y")


# ── State persistence (last UID per mailbox) ────────────────────────


def _ensure_state_table(db) -> None:
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS email_poll_state (
            mailbox VARCHAR(100) PRIMARY KEY,
            last_uid BIGINT NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """))
    db.commit()


def _get_last_uid(db, mailbox: str) -> int:
    _ensure_state_table(db)
    row = db.execute(
        text("SELECT last_uid FROM email_poll_state WHERE mailbox = :m"),
        {"m": mailbox},
    ).fetchone()
    return int(row.last_uid) if row else 0


def _set_last_uid(db, mailbox: str, uid: int) -> None:
    db.execute(
        text("""
            INSERT INTO email_poll_state (mailbox, last_uid, updated_at)
            VALUES (:m, :u, NOW())
            ON CONFLICT (mailbox) DO UPDATE
                SET last_uid = EXCLUDED.last_uid, updated_at = NOW()
        """),
        {"m": mailbox, "u": uid},
    )
    db.commit()


# ── IMAP connection management ──────────────────────────────────────


def _connect() -> imaplib.IMAP4_SSL:
    """Return a live IMAP connection. Reuses the existing one if healthy."""
    global _CONNECTION
    if _CONNECTION is not None:
        try:
            status, _ = _CONNECTION.noop()
            if status == "OK":
                return _CONNECTION
        except Exception:
            pass
        _safe_logout()

    host = getattr(settings, "IMAP_HOST", "imap.gmail.com")
    port = getattr(settings, "IMAP_PORT", 993)
    conn = imaplib.IMAP4_SSL(host, port)
    conn.login(settings.SMTP_EMAIL, settings.SMTP_APP_PASSWORD)
    _CONNECTION = conn
    return conn


def _safe_logout() -> None:
    global _CONNECTION
    if _CONNECTION is not None:
        try:
            _CONNECTION.logout()
        except Exception:
            pass
        _CONNECTION = None


def _ensure_label(conn: imaplib.IMAP4_SSL, label: str) -> None:
    try:
        conn.create(label)
    except Exception:
        pass  # label likely exists already


# ── Body parsing ────────────────────────────────────────────────────


def _extract_body(msg) -> str:
    """Return the plain-text portion of an email (falls back to HTML stripped)."""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = str(part.get("Content-Disposition") or "")
            if ctype == "text/plain" and "attachment" not in disp:
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode(
                        part.get_content_charset() or "utf-8",
                        errors="replace",
                    )
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    html = payload.decode(
                        part.get_content_charset() or "utf-8",
                        errors="replace",
                    )
                    return re.sub(r"<[^>]+>", "", html)
        return ""

    payload = msg.get_payload(decode=True)
    if not payload:
        return ""
    return payload.decode(msg.get_content_charset() or "utf-8", errors="replace")


def _strip_quoted_reply(body: str) -> str:
    """Return only the top (non-quoted) reply text."""
    out = []
    for raw in body.splitlines():
        line = raw.strip()
        if line.startswith(">"):
            break
        if _QUOTE_HEADER_RE.match(line):
            break
        out.append(raw)
    # Drop trailing blanks
    while out and not out[-1].strip():
        out.pop()
    return "\n".join(out).strip()


def _is_approval(reply_text: str) -> bool:
    """First non-empty line in the cleaned reply must match the approval pattern."""
    for line in reply_text.splitlines():
        s = line.strip()
        if not s:
            continue
        return bool(_APPROVAL_RE.match(s))
    return False


# ── RTV lookup + approval dispatch ──────────────────────────────────


def _find_rtv(db, rtv_id_str: str) -> Optional[dict]:
    """Find an RTV by its rtv_id across both company tables."""
    for company, prefix in (("CFPL", "cfpl"), ("CDPL", "cdpl")):
        row = db.execute(
            text(
                f"SELECT id, status, business_head "
                f"FROM {prefix}_rtv_header WHERE rtv_id = :r"
            ),
            {"r": rtv_id_str},
        ).fetchone()
        if row:
            return {
                "id": row.id,
                "company": company,
                "status": row.status,
                "business_head": row.business_head,
            }
    return None


def _authorized_email_for(business_head: Optional[str]) -> Optional[str]:
    if not business_head:
        return None
    key = business_head.strip().lower()
    for name, addr in BUSINESS_HEAD_EMAILS.items():
        if name.lower() == key:
            return addr.lower()
    return None


def _extra_approvers() -> set[str]:
    raw = getattr(settings, "RTV_EMAIL_EXTRA_APPROVERS", "") or ""
    return {e.strip().lower() for e in raw.split(",") if e.strip()}


def _is_sender_authorized(rtv: dict, from_email: str) -> bool:
    if from_email in _extra_approvers():
        return True
    expected = _authorized_email_for(rtv["business_head"])
    return bool(expected) and from_email == expected


def _process_message(db, uid_bytes: bytes, msg) -> bool:
    """Validate and (if eligible) approve. Returns True if RTV was approved."""
    uid_str = uid_bytes.decode()
    subject = msg.get("Subject", "") or ""
    _, from_email = parseaddr(msg.get("From", "") or "")
    from_email = (from_email or "").lower().strip()

    m = _RTV_ID_RE.search(subject)
    if not m:
        return False
    rtv_id_str = m.group(0)

    rtv = _find_rtv(db, rtv_id_str)
    if not rtv:
        logger.info("UID %s: RTV %s not found in either company table", uid_str, rtv_id_str)
        return False

    if rtv["status"] == "Approved":
        logger.info("UID %s: RTV %s already approved, skipping", uid_str, rtv_id_str)
        return False

    if not _is_sender_authorized(rtv, from_email):
        expected = _authorized_email_for(rtv["business_head"])
        logger.info(
            "UID %s: sender %s not authorized for RTV %s (expected %s or extra approver)",
            uid_str, from_email or "<none>", rtv_id_str, expected or "<unmapped>",
        )
        return False

    cleaned = _strip_quoted_reply(_extract_body(msg))
    if not _is_approval(cleaned):
        logger.info("UID %s: reply body does not match approval pattern (%s)", uid_str, rtv_id_str)
        return False

    if getattr(settings, "RTV_EMAIL_APPROVAL_DRY_RUN", False):
        logger.info(
            "[DRY-RUN] Would auto-approve %s for %s (%s) — sender %s, reply: %r",
            rtv_id_str, rtv["company"], rtv["business_head"], from_email,
            cleaned.splitlines()[0] if cleaned else "",
        )
        return False

    # Local imports to avoid circular dependency with rtv_server -> email_notifier
    from services.ims_service.rtv_models import RTVApprovalRequest
    from services.ims_service.rtv_tools import approve_rtv, get_rtv

    try:
        approve_rtv(
            rtv["company"],
            rtv["id"],
            RTVApprovalRequest(approved_by=from_email),
            db,
        )
        detail = get_rtv(rtv["company"], rtv["id"], db)
        notify_rtv_approved(detail, from_email)
        logger.info("Auto-approved %s via email reply from %s", rtv_id_str, from_email)
        return True
    except Exception as exc:
        db.rollback()
        logger.error("Auto-approval failed for %s: %s", rtv_id_str, exc)
        return False


# ── Polling pass ────────────────────────────────────────────────────


def _trigger_backoff(reason: str) -> None:
    global _FAILURES, _BACKOFF_UNTIL
    _FAILURES += 1
    _safe_logout()
    minutes = BACKOFF_MINUTES[min(_FAILURES - 1, len(BACKOFF_MINUTES) - 1)]
    _BACKOFF_UNTIL = time.time() + minutes * 60
    logger.error("IMAP poll failed (#%d): %s. Backing off %d min.", _FAILURES, reason, minutes)


def poll_once() -> None:
    """Single polling pass. Safe to call from APScheduler."""
    global _FAILURES, _BACKOFF_UNTIL

    if _BACKOFF_UNTIL and time.time() < _BACKOFF_UNTIL:
        return

    if not _is_active_hours():
        return

    if not settings.SMTP_EMAIL or not settings.SMTP_APP_PASSWORD:
        return

    if not _LOCK.acquire(blocking=False):
        logger.debug("Previous poll still running, skipping")
        return

    try:
        conn = _connect()
        _ensure_label(conn, PROCESSED_LABEL)

        status, _ = conn.select("INBOX")
        if status != "OK":
            raise RuntimeError("IMAP SELECT INBOX failed")

        # Server-side prefilter: subject + recency. Gmail returns near-zero
        # when nothing is pending.
        since_arg = _imap_date(SEARCH_LOOKBACK_DAYS).encode()
        status, data = conn.uid(
            "SEARCH", None,
            "SUBJECT", "RTV",
            "SINCE", since_arg,
        )
        if status != "OK":
            raise RuntimeError(f"IMAP SEARCH failed: {status}")
        if not data or not data[0]:
            _FAILURES = 0
            return

        # Open DB only now that there is *some* candidate.
        db = SessionLocal()
        try:
            last_uid = _get_last_uid(db, "INBOX")

            all_uids = [int(u) for u in data[0].split()]
            new_uids = sorted(u for u in all_uids if u > last_uid)[:MAX_PER_POLL]

            if not new_uids:
                # Catch up the cursor so we don't re-evaluate old UIDs forever.
                if all_uids:
                    _set_last_uid(db, "INBOX", max(all_uids))
                _FAILURES = 0
                return

            logger.info("IMAP poll: %d candidate UID(s) since UID %d", len(new_uids), last_uid)

            max_uid = last_uid
            for uid_int in new_uids:
                uid_bytes = str(uid_int).encode()
                try:
                    # Cheap header-only fetch first
                    status, header_data = conn.uid(
                        "FETCH", uid_bytes,
                        "(BODY.PEEK[HEADER.FIELDS (SUBJECT FROM IN-REPLY-TO)])",
                    )
                    if status != "OK" or not header_data or not header_data[0]:
                        max_uid = max(max_uid, uid_int)
                        continue

                    header_bytes = header_data[0][1] if isinstance(header_data[0], tuple) else b""
                    headers = email.message_from_bytes(header_bytes or b"")
                    if not _RTV_ID_RE.search(headers.get("Subject", "") or ""):
                        max_uid = max(max_uid, uid_int)
                        continue

                    # Full fetch (only on subject match)
                    status, full_data = conn.uid("FETCH", uid_bytes, "(RFC822)")
                    if status != "OK" or not full_data or not full_data[0]:
                        max_uid = max(max_uid, uid_int)
                        continue

                    full_bytes = full_data[0][1] if isinstance(full_data[0], tuple) else b""
                    msg = email.message_from_bytes(full_bytes or b"")
                    approved = _process_message(db, uid_bytes, msg)

                    if approved:
                        try:
                            conn.uid("STORE", uid_bytes, "+X-GM-LABELS", f"({PROCESSED_LABEL})")
                        except Exception as exc:
                            logger.debug("Could not label UID %d: %s", uid_int, exc)

                    max_uid = max(max_uid, uid_int)
                except Exception as exc:
                    logger.warning("Failed processing UID %d: %s", uid_int, exc)
                    max_uid = max(max_uid, uid_int)  # don't get stuck on a bad message

            if max_uid > last_uid:
                _set_last_uid(db, "INBOX", max_uid)

            _FAILURES = 0
            _BACKOFF_UNTIL = None
        finally:
            db.close()

    except Exception as exc:
        _trigger_backoff(str(exc))
    finally:
        _LOCK.release()


def shutdown() -> None:
    _safe_logout()
