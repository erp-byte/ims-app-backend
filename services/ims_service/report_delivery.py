"""Send-once bookkeeping shared by the scheduled report mails.

WHY THIS IS NOT A COMMENT IN EACH REPORT
    `daily_report` learned this the expensive way and wrote it down: more than
    one instance serves the API, each starts its own BackgroundScheduler, and
    every cron therefore fires once PER INSTANCE. The daily report went out
    twice a day to eleven people for months — seconds apart, byte-identical,
    landing in the same thread, which is exactly what makes Gmail fold the
    second one away as quoted text so nobody reports it.

    APScheduler's `max_instances` / `coalesce` cannot help: they coordinate jobs
    inside one process and the duplicate is in another. The only thing every
    instance shares is the database, so the claim lives there.

THE CLAIM
    The row is written as 'sent' BEFORE the mail goes out, so claiming and
    recording are one atomic act and two processes cannot both pass. A send that
    then fails is flipped to 'failed', which frees the slot again — the partial
    unique index covers 'sent' only — so a genuine failure is still retried and
    still visible in the log.

    Scoped to automated kinds. A manual re-send is a person deliberately asking
    for the same report again and must never be swallowed.

Every new report mail gets its own table through `Ledger(...)` rather than
sharing one: the tables are tiny, and a per-report unique index means a bug in
one report's fingerprint can never block another report's send.
"""
from __future__ import annotations

import re
from datetime import date, datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger

logger = get_logger("report_delivery")

# Kinds that fire from a scheduler and must therefore be de-duplicated.
AUTOMATED_KINDS = ("evening", "revision", "catchup", "weekly")

_SAFE_TABLE = re.compile(r"^[a-z_][a-z0-9_]*$")


class Ledger:
    """The delivery log for one report mail.

    `table` is interpolated into DDL, which cannot take a bind parameter, so it
    is validated against a strict pattern rather than trusted. Every caller
    passes a literal today; the check is there so that stays true.
    """

    def __init__(self, table: str, recipients: list[str] | None = None):
        if not _SAFE_TABLE.match(table):
            raise ValueError(f"unsafe ledger table name: {table!r}")
        self.table = table
        self.recipients = recipients or []

    # ── schema ───────────────────────────────────────────────────────────
    def ensure(self, db: Session) -> None:
        t = self.table
        db.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {t} (
                id            SERIAL PRIMARY KEY,
                business_day  DATE        NOT NULL,
                kind          VARCHAR(20) NOT NULL,
                fingerprint   TEXT        NOT NULL DEFAULT '',
                recipients    TEXT        NOT NULL DEFAULT '',
                status        VARCHAR(20) NOT NULL,
                error         TEXT,
                sent_at       TIMESTAMP   NOT NULL DEFAULT NOW()
            )
        """))
        db.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{t}_day ON {t}(business_day)"))
        # Clear any duplicate pairs a pre-index deploy already recorded, so the
        # unique index below can actually be created.
        db.execute(text(f"""
            DELETE FROM {t} a USING {t} b
            WHERE a.status = 'sent' AND b.status = 'sent'
              AND a.kind IN ('evening', 'revision', 'catchup', 'weekly')
              AND b.kind = a.kind
              AND a.business_day = b.business_day
              AND a.fingerprint = b.fingerprint
              AND a.id > b.id
        """))
        db.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_{t}_sent_once
            ON {t} (business_day, kind, fingerprint)
            WHERE status = 'sent'
              AND kind IN ('evening', 'revision', 'catchup', 'weekly')
        """))
        db.commit()

    # ── claim / release ──────────────────────────────────────────────────
    def claim(self, db: Session, day: date, kind: str, fp: str) -> int | None:
        """Reserve this exact send, or None when another process already has it."""
        row = db.execute(text(f"""
            INSERT INTO {self.table}
                   (business_day, kind, fingerprint, recipients, status)
            VALUES (:d, :k, :f, :r, 'sent')
            ON CONFLICT DO NOTHING
            RETURNING id
        """), {"d": day, "k": kind, "f": fp,
               "r": ", ".join(self.recipients)}).fetchone()
        db.commit()
        return row.id if row else None

    def release(self, db: Session, claim_id: int, error: str) -> None:
        db.execute(text(f"UPDATE {self.table} SET status = 'failed', error = :e "
                        f"WHERE id = :i"), {"i": claim_id, "e": error[:1000]})
        db.commit()

    def log(self, db: Session, day: date, kind: str, status: str,
            fp: str = "", error: str | None = None) -> None:
        db.execute(text(f"""
            INSERT INTO {self.table}
                   (business_day, kind, fingerprint, recipients, status, error)
            VALUES (:d, :k, :f, :r, :s, :e)
        """), {"d": day, "k": kind, "f": fp, "r": ", ".join(self.recipients),
               "s": status, "e": error})
        db.commit()

    def last_sent(self, db: Session, day: date) -> tuple[str, datetime] | None:
        row = db.execute(text(f"""
            SELECT fingerprint, sent_at FROM {self.table}
            WHERE business_day = :d AND status = 'sent'
            ORDER BY sent_at DESC LIMIT 1
        """), {"d": day}).fetchone()
        return (row.fingerprint, row.sent_at) if row else None

    def recent(self, db: Session, since: date) -> list[dict]:
        rows = db.execute(text(f"""
            SELECT business_day, kind, status, error, sent_at FROM {self.table}
            WHERE business_day >= :since
            ORDER BY business_day DESC, sent_at DESC
        """), {"since": since}).fetchall()
        return [{"business_day": str(r.business_day), "kind": r.kind,
                 "status": r.status, "error": r.error,
                 "sent_at": r.sent_at.isoformat() if r.sent_at else None}
                for r in rows]
