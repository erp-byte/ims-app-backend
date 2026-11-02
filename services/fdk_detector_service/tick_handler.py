"""Process Shoonya tick messages (tk/tf frames merged by TickDispatcher)."""
from shared.fdk_models import TickData
from shared.fdk_logger import get_logger

log = get_logger("tick_handler")

TICK_TYPES = {"tk", "tf"}


def parse_tick(message: dict) -> TickData | None:
    """Parse a merged tick message into a TickData, or None if irrelevant."""
    kind = message.get("t")
    if kind not in TICK_TYPES:
        return None
    symbol = message.get("symbol")
    try:
        ltp = float(message.get("lp", 0))
        vol = float(message.get("v", 0))
    except (TypeError, ValueError):
        return None
    if not symbol:
        return None
    # A snapshot stating that nothing has traded yet is a baseline of zero, or the day's
    # first trade would become the baseline and never be evaluated. A missing v is not
    # that: the docs say any field may be absent, so it tells us nothing.
    if kind == "tk" and vol == 0 and "v" in message:
        return TickData(symbol=symbol, ltp=ltp, vol_traded_today=0.0, type=kind)
    if ltp <= 0 or vol <= 0:
        return None
    return TickData(symbol=symbol, ltp=ltp, vol_traded_today=vol, type=kind)
