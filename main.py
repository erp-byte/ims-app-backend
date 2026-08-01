from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from sqlalchemy import text
from shared.config_loader import settings
from shared.database import engine, SessionLocal
from shared.logger import get_logger
from shared.middleware import RouteObfuscationMiddleware
from shared.kafka_producer import shutdown_executor
from shared.timezone import IST
from shared.scheduler import (
    auto_punch_out_and_revoke,
    daily_report_evening,
    daily_report_morning_revision,
)
from shared.email_reply_listener import poll_once as rtv_email_poll, shutdown as rtv_email_shutdown
from services.auth_service.server import router as auth_router
from services.ims_service.server import router as ims_router
from services.ims_service.inward_server import router as inward_router
from services.ims_service.daily_report_server import router as daily_report_router
from services.ims_service.interunit_server import router as interunit_router
from services.ims_service.cold_storage_server import router as cold_storage_router
from services.cold_storage_service.server import router as cold_storage_service_router
from services.ims_service.rtv_server import router as rtv_router
from services.bulk_entry_service.server import router as bulk_entry_router
from services.qc_service.server import router as qc_router
from services.qc_service.ipqc.server import router as ipqc_router
from services.qc_service.ipqc.user_server import router as ipqc_user_router
from services.competitor_service.server import router as competitor_router
from services.cold_storage_service.dashboard_server import router as dashboard_router
from services.ims_service.inward_dashboard_server import router as inward_dashboard_router
from services.ims_service.transfer_dashboard_server import router as transfer_dashboard_router
from services.ims_service.jobwork_dashboard_server import router as jobwork_dashboard_router
from services.ims_service.job_work_server import router as job_work_router
from services.lot_search_service.server import router as lot_search_router
from services.packing_service.server import router as packing_router


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
        db.execute(text("""
            ALTER TABLE interunit_transfers_lines
            ADD COLUMN IF NOT EXISTS vakkal VARCHAR(100)
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

        # Add approval columns to bulk entry transaction tables
        for _be_tbl in ("cfpl_bulk_entry_transactions", "cdpl_bulk_entry_transactions"):
            try:
                db.execute(text(f"ALTER TABLE {_be_tbl} ADD COLUMN IF NOT EXISTS approved_by VARCHAR(255)"))
                db.execute(text(f"ALTER TABLE {_be_tbl} ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP"))
                db.commit()
            except Exception:
                db.rollback()

        # Jobwork tables
        try:
            db.execute(text("""
                CREATE TABLE IF NOT EXISTS jobwork_orders (
                    id SERIAL PRIMARY KEY,
                    jwo_id VARCHAR(50) UNIQUE NOT NULL,
                    company VARCHAR(20) NOT NULL,
                    dispatch_date DATE NOT NULL,
                    vendor_name VARCHAR(200) NOT NULL,
                    item_name VARCHAR(200) NOT NULL,
                    item_description TEXT,
                    process_type VARCHAR(50) NOT NULL,
                    qty_dispatched FLOAT NOT NULL DEFAULT 0,
                    uom VARCHAR(20) DEFAULT 'Kgs',
                    jwo_status VARCHAR(30) NOT NULL DEFAULT 'Open',
                    expected_loss_pct FLOAT DEFAULT 0,
                    overdue_threshold_days INTEGER DEFAULT 30,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """))
            db.execute(text("""
                CREATE TABLE IF NOT EXISTS jobwork_inward_receipts (
                    id SERIAL PRIMARY KEY,
                    jwo_order_id INTEGER NOT NULL REFERENCES jobwork_orders(id),
                    ir_number VARCHAR(50) UNIQUE NOT NULL,
                    ir_date DATE NOT NULL,
                    receipt_type VARCHAR(20) NOT NULL DEFAULT 'Partial',
                    fg_qty_received FLOAT DEFAULT 0,
                    waste_qty_received FLOAT DEFAULT 0,
                    rejection_qty FLOAT DEFAULT 0,
                    actual_loss_pct FLOAT DEFAULT 0,
                    loss_status VARCHAR(30) DEFAULT 'Pending',
                    remarks TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """))
            db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_jwo_company ON jobwork_orders(company)
            """))
            db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_jwo_dispatch_date ON jobwork_orders(dispatch_date)
            """))
            db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_jwo_vendor ON jobwork_orders(vendor_name)
            """))
            db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_jwo_status ON jobwork_orders(jwo_status)
            """))
            db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_jir_jwo_id ON jobwork_inward_receipts(jwo_order_id)
            """))
            db.commit()
        except Exception:
            db.rollback()

        # RTV header columns (logistics fields + manual Sales POC email).
        # Mirrors migrations/2026-06-09_rtv_header_logistics_fields.sql and
        # migrations/2026-06-18_rtv_sales_poc_email.sql so the schema self-heals
        # at boot and deploys are order-independent (no manual migrate step).
        for _rtv_tbl in ("cfpl_rtv_header", "cdpl_rtv_header"):
            try:
                db.execute(text(f"ALTER TABLE {_rtv_tbl} ADD COLUMN IF NOT EXISTS vehicle_number   varchar"))
                db.execute(text(f"ALTER TABLE {_rtv_tbl} ADD COLUMN IF NOT EXISTS transporter_name varchar"))
                db.execute(text(f"ALTER TABLE {_rtv_tbl} ADD COLUMN IF NOT EXISTS driver_name      varchar"))
                db.execute(text(f"ALTER TABLE {_rtv_tbl} ADD COLUMN IF NOT EXISTS inward_manager   varchar"))
                db.execute(text(f"ALTER TABLE {_rtv_tbl} ADD COLUMN IF NOT EXISTS sales_poc_email  varchar"))
                db.commit()
            except Exception:
                db.rollback()

        # Packing Details (QR / encrypted batch tokens). Mirrors
        # migrations/2026-07-02_packing_details.sql so the schema self-heals at boot.
        # `details` is JSON (not JSONB) so the user's block/key order is preserved
        # on round-trip — JSONB canonicalises (reorders) object keys.
        try:
            db.execute(text("""
                CREATE TABLE IF NOT EXISTS packing_details (
                    id           SERIAL PRIMARY KEY,
                    batch_code   VARCHAR(255) NOT NULL,
                    article_name VARCHAR(255) NOT NULL,
                    details      JSON NOT NULL DEFAULT '{}'::json,
                    created_by   VARCHAR(255),
                    created_at   TIMESTAMP DEFAULT NOW(),
                    updated_at   TIMESTAMP DEFAULT NOW()
                )
            """))
            # Self-heal older deployments where `details` was created as JSONB:
            # convert to JSON in place so key order is preserved from here on
            # (idempotent — only rewrites while the column is still jsonb).
            db.execute(text("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'packing_details'
                          AND column_name = 'details'
                          AND data_type  = 'jsonb'
                    ) THEN
                        ALTER TABLE packing_details ALTER COLUMN details DROP DEFAULT;
                        ALTER TABLE packing_details ALTER COLUMN details TYPE JSON USING details::text::json;
                        ALTER TABLE packing_details ALTER COLUMN details SET DEFAULT '{}'::json;
                    END IF;
                END $$;
            """))
            db.execute(text("CREATE INDEX IF NOT EXISTS idx_packing_details_batch ON packing_details(batch_code)"))
            db.execute(text("CREATE INDEX IF NOT EXISTS idx_packing_details_article ON packing_details(article_name)"))
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
    # Daily inward & transfer report — 7:00 PM IST every day. The job itself
    # applies the Sunday rule (send only if the day had activity), so the
    # trigger stays simple and the decision lives with the data.
    scheduler.add_job(
        daily_report_evening,
        CronTrigger(hour=19, minute=0, timezone=IST),
        id="daily_report_evening",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,   # still fire if the worker was busy/asleep briefly
    )
    # 10:30 AM IST — re-send yesterday only if entries landed after the 7 PM
    # cut-off, or if yesterday's evening send was missed entirely.
    scheduler.add_job(
        daily_report_morning_revision,
        CronTrigger(hour=10, minute=30, timezone=IST),
        id="daily_report_morning_revision",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    if settings.RTV_EMAIL_APPROVAL_ENABLED:
        scheduler.add_job(
            rtv_email_poll,
            IntervalTrigger(minutes=settings.RTV_EMAIL_POLL_MINUTES),
            id="rtv_email_poll",
            max_instances=1,
            coalesce=True,
        )
        mode = "DRY-RUN" if settings.RTV_EMAIL_APPROVAL_DRY_RUN else "LIVE"
        logger.info(
            "RTV email reply listener scheduled every %d min — mode: %s (active 07:00–23:00 IST)",
            settings.RTV_EMAIL_POLL_MINUTES, mode,
        )
    scheduler.start()
    logger.info("Scheduler started — auto punch-out at 11:00 PM IST daily")
    logger.info("Keep-alive ping scheduled every 7 minutes")
    logger.info(
        "Daily inward/transfer report scheduled — 7:00 PM IST daily "
        "(Sunday only if there was activity), revision check 10:30 AM IST"
    )

    # A missed 7 PM send (host asleep / mid-deploy) would otherwise be lost
    # silently, so on boot we settle any recent business day that never went out.
    try:
        from services.ims_service.daily_report import run_startup_catchup
        run_startup_catchup()
    except Exception as exc:
        logger.warning("Daily report catch-up could not start: %s", exc)

    yield

    scheduler.shutdown()
    rtv_email_shutdown()
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


# Starlette's ServerErrorMiddleware sits OUTSIDE CORSMiddleware, so an unhandled 500 is
# answered with no Access-Control-Allow-Origin and the browser reports a backend crash as
# "blocked by CORS policy / Failed to fetch" — the real error never reaches the operator.
# Declared before the CORS add_middleware call so it nests *inside* it and its response
# gets the CORS headers.
@app.middleware("http")
async def cors_safe_errors(request, call_next):
    try:
        return await call_next(request)
    except Exception as exc:
        logger.exception("UNHANDLED %s %s", request.method, request.url.path)
        return JSONResponse(
            status_code=500, content={"detail": f"{type(exc).__name__}: {exc}"}
        )


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:4000",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:4000",
        "https://candorims.netlify.app",
        "https://8vp3hks5-8000.inc1.devtunnels.ms",
        # candorfoods.in (Wix) — for the public packing QR scan page.
        "https://www.candorfoods.in",
        "https://candorfoods.in",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(auth_router)
app.include_router(ims_router)
app.include_router(inward_router)
app.include_router(daily_report_router)
app.include_router(interunit_router)
app.include_router(cold_storage_router)
app.include_router(cold_storage_service_router)
app.include_router(rtv_router)
app.include_router(bulk_entry_router)
app.include_router(qc_router)
app.include_router(ipqc_user_router)
app.include_router(ipqc_router)
app.include_router(competitor_router)
app.include_router(dashboard_router)
app.include_router(inward_dashboard_router)
app.include_router(transfer_dashboard_router)
app.include_router(jobwork_dashboard_router)
app.include_router(job_work_router)
app.include_router(lot_search_router)
app.include_router(packing_router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
