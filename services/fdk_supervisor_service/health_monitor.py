"""Self-ping to keep Render alive."""
import time
import requests
import threading
from shared.fdk_constants import HEALTH_CHECK_URL, SELF_PING_INTERVAL
from shared.fdk_logger import get_logger

log = get_logger("health_monitor")


def start_self_ping():
    """Start background thread that pings /health every 7 min."""
    def _ping_loop():
        time.sleep(30)  # initial delay
        while True:
            try:
                time.sleep(SELF_PING_INTERVAL)
                requests.get(HEALTH_CHECK_URL, timeout=30)
                log.info("Self-ping OK")
            except Exception as e:
                log.warning(f"Self-ping failed: {e}")

    threading.Thread(target=_ping_loop, daemon=True).start()
