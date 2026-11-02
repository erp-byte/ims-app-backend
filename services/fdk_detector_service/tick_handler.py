"""Process raw WebSocket tick messages."""
from shared.fdk_models import TickData
from shared.fdk_logger import get_logger

log = get_logger("tick_handler")

SKIP_TYPES = {"cn", "ful", "sub"}


def parse_tick(message: dict) -> TickData | None:
    """Parse a raw WebSocket message into a TickData, or None if irrelevant."""
    if message.get("type") in SKIP_TYPES:
        return None
    symbol = message.get("symbol")
    ltp = message.get("ltp", 0)
    vol = message.get("vol_traded_today", 0)
    if not symbol or float(ltp) <= 0 or float(vol) <= 0:
        return None
    return TickData(symbol=symbol, ltp=float(ltp), vol_traded_today=float(vol))
