from datetime import datetime, timezone, timedelta

from sqlalchemy import update, delete

from shared.database import SessionLocal
from shared.models import Attendance, RefreshToken
from shared.logger import get_logger

logger = get_logger("scheduler")

IST = timezone(timedelta(hours=5, minutes=30))


def auto_punch_out_and_revoke():
    """Run at 11 PM IST daily — punch out all active sessions and revoke all refresh tokens."""
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


def job_work_weekly_digest():
    """Run every Monday at 9 AM IST — send weekly jobwork summary digest."""
    logger.info("Running weekly jobwork digest...")
    try:
        from shared.email_notifier import send_job_work_weekly_digest
        send_job_work_weekly_digest()
    except Exception as e:
        logger.error(f"Weekly jobwork digest failed: {e}")


def daily_report_evening():
    """Run at 7 PM IST — email the day's inward & transfer report.

    Sends every Mon-Sat; on Sunday only when the day actually had activity.
    """
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
    logger.info("Running daily inward/transfer report (morning revision check)...")
    try:
        from services.ims_service.daily_report import run_morning_revision
        result = run_morning_revision()
        logger.info(f"Daily report (morning revision): {result}")
    except Exception as e:
        logger.error(f"Daily report (morning revision) failed: {e}")
