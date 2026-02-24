import time
import threading
from queue import Queue

from sqlalchemy import update

from shared.database import SessionLocal
from shared.models import Attendance
from shared.logger import get_logger
from services.geocoding_service import reverse_geocode

logger = get_logger("geocoding.background")

_queue: Queue = Queue()
_worker_thread: threading.Thread | None = None
_STOP = object()


def _worker():
    """Drain the queue, resolve addresses, respect LocationIQ rate limit."""
    while True:
        task = _queue.get()
        if task is _STOP:
            _queue.task_done()
            break

        attendance_id, latitude, longitude, is_punch_out = task
        try:
            address = reverse_geocode(latitude, longitude)
            field = "punch_out_store" if is_punch_out else "punch_in_store"

            db = SessionLocal()
            try:
                db.execute(
                    update(Attendance)
                    .where(Attendance.id == attendance_id)
                    .values(**{field: address})
                )
                db.commit()
                logger.info(f"Updated attendance {attendance_id} {field} -> {address}")
            except Exception as e:
                db.rollback()
                logger.error(f"DB update failed for attendance {attendance_id}: {e}")
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Geocoding failed for attendance {attendance_id}: {e}")
        finally:
            _queue.task_done()
            time.sleep(0.5)  # Rate limit: 2 req/sec for LocationIQ


def _ensure_worker():
    """Start the worker thread if it isn't running."""
    global _worker_thread
    if _worker_thread is None or not _worker_thread.is_alive():
        _worker_thread = threading.Thread(target=_worker, daemon=True)
        _worker_thread.start()
        logger.info("Geocoding worker thread started")


def publish_geocoding_task(
    attendance_id: str, latitude: float, longitude: float, is_punch_out: bool = False,
) -> None:
    """Enqueue a geocoding task for background processing."""
    _ensure_worker()
    _queue.put((attendance_id, latitude, longitude, is_punch_out))
    logger.info(f"Queued geocoding task for attendance {attendance_id}")


def shutdown_executor():
    """Drain the queue and stop the worker thread."""
    _queue.put(_STOP)
    _queue.join()
    logger.info("Geocoding worker shut down")
