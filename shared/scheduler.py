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
