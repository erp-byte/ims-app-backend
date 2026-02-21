from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from shared.database import engine
from shared.logger import get_logger
from shared.middleware import RouteObfuscationMiddleware
from shared.kafka_producer import shutdown_executor
from shared.scheduler import auto_punch_out_and_revoke
from services.auth_service.server import router as auth_router
from services.ims_service.server import router as ims_router
from services.ims_service.inward_server import router as inward_router
from services.ims_service.interunit_server import router as interunit_router
from services.ims_service.transfer_server import router as transfer_router

logger = get_logger("main")

HEALTH_URL = "https://new-app-backend-and-ims.onrender.com/health"


def keep_alive_ping():
    """Ping the health endpoint every 7 minutes to keep the Render server alive."""
    try:
        resp = httpx.get(HEALTH_URL, timeout=10)
        logger.info("Keep-alive ping: %s %s", resp.status_code, HEALTH_URL)
    except Exception as exc:
        logger.warning("Keep-alive ping failed: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Server starting up")

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
app.include_router(transfer_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
