"""Application-wide constants."""
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Absolute path so .env is found regardless of the working directory (shared/ -> repo root).
load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def _flag(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


IST = ZoneInfo("Asia/Kolkata")

# Market schedule
MARKET_START_TIME = "09:13"
MARKET_END_TIME = "16:00"

# Daily token wipe — clear stored credentials each evening so the next
# 09:13 login is always a fresh full TOTP login (17:00 IST, after market close).
DAILY_TOKEN_RESET_TIME = "17:00"

# Minimum gap between WebSocket rebuilds, so a hard-down socket cannot
# spam reconnects and Telegram alerts every supervisor tick.
WS_REBUILD_COOLDOWN = 60

# Detection thresholds
FYERS_TRADE_THRESHOLD = 30_000_000    # Rs 3 crore
PENNY_TRADE_THRESHOLD = 5_000_000     # Rs 50 lakh (0.50 Cr)
MIN_VOLUME_SPIKE = 1000

# Timing
LOGIN_URL_RETRY_INTERVAL = 300        # cooldown after an exhausted login cycle (5 min)
TOKEN_VALIDITY_SECONDS = 28800        # 8 hours

# Headless login retry policy
AUTO_LOGIN_MAX_ATTEMPTS = 5           # attempts before reporting failure and cooling down
AUTO_LOGIN_RETRY_DELAY = 60           # seconds between attempts

# Integration into the Candor backend — every fdk route lives under this prefix
FDK_ROUTE_PREFIX = "/fdk"
# Public base URL of the backend that hosts fdk (Telegram webhook target, self-ping).
# Empty = webhooks are not registered at start-up (logged as an error).
BASE_URL = os.getenv("FDK_BASE_URL", "").strip().rstrip("/")
WEBHOOK_PATH = f"{FDK_ROUTE_PREFIX}/webhook/telegram"
HEALTH_CHECK_URL = f"{BASE_URL}{FDK_ROUTE_PREFIX}/health"
# Seconds the backend waits for fdk to build (Supabase tables + seed) before carrying on without it
FDK_STARTUP_TIMEOUT = 120
# Opt-in self-ping thread (the backend already runs its own keep-alive job)
FDK_SELF_PING = _flag("FDK_SELF_PING")
SELF_PING_INTERVAL = 420              # 7 min
SUMMARY_SEND_TIME = "16:30"           # 4:30 PM IST

# Google Sheets headers
SHEET_HEADERS = ['Date', 'Time', 'Symbol', 'LTP', 'Volume_Spike',
                 'Trd_Val_Cr', 'Spike_Type', 'Sector']
