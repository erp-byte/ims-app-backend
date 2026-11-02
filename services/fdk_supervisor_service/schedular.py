"""Market-hours scheduling."""
from datetime import datetime
from shared.fdk_constants import (
    MARKET_START_TIME, MARKET_END_TIME, DAILY_TOKEN_RESET_TIME,
    WS_REBUILD_COOLDOWN, IST,
)
from shared.fdk_logger import get_logger

log = get_logger("scheduler")


def is_market_hours() -> bool:
    """Check if current IST time is within configured market hours."""
    now = datetime.now(IST)
    current = now.strftime("%H:%M")
    return MARKET_START_TIME <= current < MARKET_END_TIME


def should_reset_tokens(current_hhmm: str, last_reset_date: str | None,
                        today: str) -> bool:
    """True when the daily token wipe is due — once per day at/after 17:00 IST.

    Pure and testable: compares zero-padded HH:MM strings (lexicographic order
    matches chronological order) with a once-per-day guard. Using >= (not ==)
    means a missed exact minute still fires later the same evening.
    """
    return current_hhmm >= DAILY_TOKEN_RESET_TIME and last_reset_date != today


def should_rebuild_socket(needs_rebuild: bool, last_rebuild_ts: float | None,
                          now_ts: float,
                          cooldown: float = WS_REBUILD_COOLDOWN) -> bool:
    """True when an unhealthy WebSocket is due to be torn down and rebuilt.

    Pure and testable. The cooldown stops a socket that cannot come back up
    (Fyers down, token dead) from rebuilding — and alerting — on every tick.
    """
    if not needs_rebuild:
        return False
    if last_rebuild_ts is None:
        return True
    return (now_ts - last_rebuild_ts) >= cooldown
