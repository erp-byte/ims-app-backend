from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from sqlalchemy import text
from shared.database import engine, SessionLocal
from shared.logger import get_logger
from shared.middleware import RouteObfuscationMiddleware
from shared.kafka_producer import shutdown_executor
from shared.scheduler import auto_punch_out_and_revoke
from services.auth_service.server import router as auth_router
from services.ims_service.server import router as ims_router
from services.ims_service.inward_server import router as inward_router
from services.ims_service.interunit_server import router as interunit_router
from services.ims_service.cold_storage_server import router as cold_storage_router
from services.cold_storage_service.server import router as cold_storage_service_router
from services.ims_service.rtv_server import router as rtv_router
from services.bulk_entry_service.server import router as bulk_entry_router
from services.qc_service.server import router as qc_router
from services.qc_service.ipqc.server import router as ipqc_router
from services.qc_service.ipqc.user_server import router as ipqc_user_router
from services.competitor_service.server import router as competitor_router


logger = get_logger("main")

KEEP_ALIVE_URLS = [
    "https://new-app-backend-and-ims.onrender.com/health",
    "https://desktop-backend-vhf0.onrender.com/health",
]


def keep_alive_ping():
    """Ping health endpoints every 7 minutes to keep Render services alive."""
    for url in KEEP_ALIVE_URLS:
        try:
            resp = httpx.get(url, timeout=10)
            logger.info("Keep-alive ping: %s %s", resp.status_code, url)
        except Exception as exc:
            logger.warning("Keep-alive ping failed (%s): %s", url, exc)


def _run_startup_migrations():
    """One-time schema migrations that run at server boot."""
    db = SessionLocal()
    try:
        db.execute(text("""
            ALTER TABLE interunit_transfer_in_boxes
            ADD COLUMN IF NOT EXISTS transfer_out_box_id INTEGER
            REFERENCES interunit_transfer_boxes(id)
        """))
        db.execute(text("""
            ALTER TABLE interunit_transfer_in_boxes
            ADD COLUMN IF NOT EXISTS line_index INTEGER
        """))
        db.commit()

        # Separate try/catch for cold storage table columns (table may not exist yet)
        try:
            db.execute(text("""
                ALTER TABLE interunit_transfer_in_cold_storage
                ADD COLUMN IF NOT EXISTS item_subgroup VARCHAR(100)
            """))
            db.execute(text("""
                ALTER TABLE interunit_transfer_in_cold_storage
                ADD COLUMN IF NOT EXISTS spl_remarks TEXT
            """))
            db.commit()
        except Exception:
            db.rollback()

        # Add approval columns to cold_storage_stocks
        try:
            db.execute(text("""
                ALTER TABLE cold_storage_stocks
                ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'pending'
            """))
            db.execute(text("""
                ALTER TABLE cold_storage_stocks
                ADD COLUMN IF NOT EXISTS approved_by VARCHAR(100)
            """))
            db.execute(text("""
                ALTER TABLE cold_storage_stocks
                ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP
            """))
            db.commit()
        except Exception:
            db.rollback()

        logger.info("Startup migrations completed")
    except Exception as exc:
        db.rollback()
        logger.warning("Startup migration skipped: %s", exc)
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Server starting up")
    _run_startup_migrations()

    # 11 PM IST = 17:30 UTC daily
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        auto_punch_out_and_revoke,
        CronTrigger(hour=17, minute=30, timezone="UTC"),
        id="auto_punch_out",
    )
    scheduler.add_job(
        keep_alive_ping,
        IntervalTrigger(minutes=7),
        id="keep_alive",
    )
    scheduler.start()
    logger.info("Scheduler started — auto punch-out at 11:00 PM IST daily")
    logger.info("Keep-alive ping scheduled every 7 minutes")

    yield

    scheduler.shutdown()
    shutdown_executor()
    engine.dispose()


app = FastAPI(
    title="Candor Retail Backend",
    version="1.1",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(RouteObfuscationMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(auth_router)
app.include_router(ims_router)
app.include_router(inward_router)
app.include_router(interunit_router)
app.include_router(cold_storage_router)
app.include_router(cold_storage_service_router)
app.include_router(rtv_router)
app.include_router(bulk_entry_router)
app.include_router(qc_router)
app.include_router(ipqc_user_router)
app.include_router(ipqc_router)
app.include_router(competitor_router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
