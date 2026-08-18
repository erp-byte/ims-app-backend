"""Scheduled jobs, and the guard that keeps them running ONCE per firing.

WHY THE GUARD EXISTS
    More than one instance serves this API, and each one starts its own
    BackgroundScheduler at boot. Every cron therefore fires once PER INSTANCE:
    the daily report went out twice a day to eleven people for months, seconds
    apart and byte-identical, and the mail-reply poller processed the same inbox
    twice over. APScheduler's own `max_instances` / `coalesce` never helped —
    they coordinate jobs inside one process, and the duplicates are in another.

    A thread lock has the same blind spot: `email_reply_listener._LOCK` is a
    threading.Lock, so it serialises two threads here and nothing at all there.

    The only thing every instance shares is the database, so the claim lives
    there. `claim_scheduled_run` is race-safe by construction: a transaction-scoped
    advisory lock serialises the check-and-insert, so of two instances arriving
    together exactly one is told to proceed.

    `min_gap_seconds` is a WINDOW rather than an exact key on purpose. The
    instances do not fire at the same instant — observed skew ran from 40 ms to
    2.3 s, and a restart can push one much later — so a key like "this minute"
    would let a straddling pair through. The window only has to be shorter than
    the job's own period.
"""
from datetime import datetime, timezone, timedelta

from sqlalchemy import text, update, delete

from shared.database import SessionLocal
from shared.models import Attendance, RefreshToken
from shared.logger import get_logger

logger = get_logger("scheduler")

IST = timezone(timedelta(hours=5, minutes=30))


def _ensure_claim_table(db) -> None:
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS scheduler_run_claim (
            id         SERIAL PRIMARY KEY,
            job_id     TEXT      NOT NULL,
            claimed_at TIMESTAMP NOT NULL DEFAULT NOW(),
            instance   TEXT
        )
    """))
    db.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_scheduler_run_claim_job "
        "ON scheduler_run_claim (job_id, claimed_at DESC)"))
    db.commit()


def claim_scheduled_run(job_id: str, min_gap_seconds: int) -> bool:
    """True when THIS process should run `job_id` now; False when another already has.

    Fails OPEN: if the claim cannot be read or written, the job still runs. A
    duplicate mail is a nuisance; a report that silently stops going out because
    the bookkeeping broke is the failure this whole module exists to prevent.
    """
    db = SessionLocal()
    try:
        _ensure_claim_table(db)
        # Serialise the check and the insert against the other instance. Held to the
        # end of THIS transaction only — the job's real work happens after commit,
        # so a slow job never blocks the next firing.
        db.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"),
                   {"k": f"scheduler:{job_id}"})
        recent = db.execute(text("""
            SELECT claimed_at FROM scheduler_run_claim
            WHERE job_id = :j AND claimed_at > NOW() - make_interval(secs => :w)
            ORDER BY claimed_at DESC LIMIT 1
        """), {"j": job_id, "w": min_gap_seconds}).fetchone()
        if recent:
            db.rollback()
            logger.info("%s already claimed at %s by another instance — skipping",
                        job_id, recent.claimed_at)
            return False
        db.execute(text("INSERT INTO scheduler_run_claim (job_id) VALUES (:j)"),
                   {"j": job_id})
        # Keep the ledger small; it is a coordination aid, not an audit trail.
        db.execute(text("DELETE FROM scheduler_run_claim "
                        "WHERE claimed_at < NOW() - INTERVAL '30 days'"))
        db.commit()
        return True
    except Exception as exc:                                   # noqa: BLE001
        logger.error("Run claim for %s failed (%s) — running anyway", job_id, exc)
        return True
    finally:
        db.close()


def auto_punch_out_and_revoke():
    """Run at 11 PM IST daily — punch out all active sessions and revoke all refresh tokens."""
    if not claim_scheduled_run("auto_punch_out", 3600):
        return
    now_ist = datetime.now(IST)
    now_utc = now_ist.astimezone(timezone.utc).replace(tzinfo=None)

    logger.info(f"Running auto punch-out at {now_ist.strftime('%Y-%m-%d %H:%M:%S')} IST")

    db = SessionLocal()
    try:
        # Punch out all active attendance records (no punch_out_timestamp)
        result = db.execute(
            update(Attendance)
            .where(Attendance.punch_out_timestamp.is_(None))
            .values(
                punch_out_timestamp=now_utc,
                punch_out_store="Auto punch-out (11 PM)",
            )
        )
        punched_out = result.rowcount

        # Delete all active refresh tokens
        result = db.execute(
            delete(RefreshToken)
            .where(RefreshToken.is_revoked == False)
        )
        tokens_deleted = result.rowcount

        db.commit()

        logger.info(
            f"Auto punch-out complete: {punched_out} sessions closed, "
            f"{tokens_deleted} refresh tokens deleted"
        )

    except Exception as e:
        db.rollback()
        logger.error(f"Auto punch-out failed: {e}")
    finally:
        db.close()


def weekly_reports():
    """Run every Monday at 10 AM IST — the weekly roll-up of every report mail.

    Four streams (inward/transfer, stock take, customer returns, job work), each
    to the recipients of the daily mail it summarises. `run_weekly_reports`
    isolates the streams from one another, so this only has to guard the firing.

    REPLACES the Monday 09:00 `job_work_weekly_digest`. That job sent a narrower
    job-work summary to a narrower list, and running both would put two job-work
    weeklies in the same inbox an hour apart. `send_job_work_weekly_digest` is
    left in email_notifier — it is still callable by hand — but nothing
    schedules it any more.
    """
    if not claim_scheduled_run("weekly_reports", 3600):
        return
    logger.info("Running weekly report roll-ups...")
    try:
        from services.ims_service.weekly_report import run_weekly_reports
        results = run_weekly_reports()
        for r in results:
            logger.info("Weekly report %s: %s", r.get("stream"), r.get("status"))
    except Exception as e:
        logger.error(f"Weekly reports failed: {e}")


def stock_take_evening():
    """Run at 7 PM IST — email the day's stock take report.

    Sent every day, including days with no count: counting runs as a campaign,
    and a mail that only arrives when someone counted cannot be told apart from
    one that failed to send.
    """
    if not claim_scheduled_run("stock_take_evening", 3600):
        return
    logger.info("Running stock take report (evening)...")
    try:
        from services.ims_service.stock_take_report import run_evening_report
        result = run_evening_report()
        logger.info(f"Stock take report (evening): {result}")
    except Exception as e:
        logger.error(f"Stock take report (evening) failed: {e}")


def stock_take_morning_revision():
    """Run at 10:30 AM IST — re-send yesterday's stock take only if it changed."""
    if not claim_scheduled_run("stock_take_morning_revision", 3600):
        return
    logger.info("Running stock take report (morning revision check)...")
    try:
        from services.ims_service.stock_take_report import run_morning_revision
        result = run_morning_revision()
        logger.info(f"Stock take report (morning revision): {result}")
    except Exception as e:
        logger.error(f"Stock take report (morning revision) failed: {e}")


def rtv_email_poll_once():
    """Poll the approval mailbox — one instance per interval, not one per instance.

    Wrapping the listener rather than editing it: its own `_LOCK` is a
    threading.Lock and correctly stops two THREADS here overlapping, which is a
    different problem from two INSTANCES each opening the same inbox and acting on
    the same reply twice. The claim window is half the poll interval so a slow
    cycle can never lock the next one out.
    """
    from shared.config_loader import settings
    from shared.email_reply_listener import poll_once
    gap = max(30, (settings.RTV_EMAIL_POLL_MINUTES * 60) // 2)
    if not claim_scheduled_run("rtv_email_poll", gap):
        return
    poll_once()


def daily_report_evening():
    """Run at 7 PM IST — email the day's inward & transfer report.

    Sends every Mon-Sat; on Sunday only when the day actually had activity.
    """
    if not claim_scheduled_run("daily_report_evening", 3600):
        return
    logger.info("Running daily inward/transfer report (evening)...")
    try:
        from services.ims_service.daily_report import run_evening_report
        result = run_evening_report()
        logger.info(f"Daily report (evening): {result}")
    except Exception as e:
        logger.error(f"Daily report (evening) failed: {e}")


def daily_report_morning_revision():
    """Run at 10:30 AM IST — re-send yesterday's report only if it changed.

    Covers entries keyed after the 7 PM cut-off, and doubles as a catch-up when
    the evening send never happened (host idled the service through 7 PM).
    Sends nothing when yesterday's figures are unchanged.
    """
    if not claim_scheduled_run("daily_report_morning_revision", 3600):
        return
    logger.info("Running daily inward/transfer report (morning revision check)...")
    try:
        from services.ims_service.daily_report import run_morning_revision
        result = run_morning_revision()
        logger.info(f"Daily report (morning revision): {result}")
    except Exception as e:
        logger.error(f"Daily report (morning revision) failed: {e}")
