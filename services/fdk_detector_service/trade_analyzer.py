"""Analyze tick data for large trades."""
from shared.fdk_models import TradeAlert
from shared.fdk_constants import MIN_VOLUME_SPIKE
from services.fdk_sector_service import get_sector
from datetime import datetime
from shared.fdk_constants import IST


def analyze_trade(symbol: str, ltp: float, current_volume: float,
                  previous_volume: float, threshold: int
                  ) -> TradeAlert | None:
    """Return a TradeAlert if the tick represents a large trade, else None."""
    volume_spike = current_volume - previous_volume
    if volume_spike <= MIN_VOLUME_SPIKE:
        return None

    trade_value = ltp * volume_spike
    if trade_value < threshold:
        return None

    spike_pct = (volume_spike / previous_volume * 100) if previous_volume > 0 else 0
    if spike_pct > 50:
        spike_type = "Large Spike"
    elif spike_pct > 20:
        spike_type = "Medium Spike"
    else:
        spike_type = "Volume Increase"

    return TradeAlert(
        symbol=symbol,
        sector=get_sector(symbol),
        ltp=ltp,
        volume_spike=int(volume_spike),
        trade_value=trade_value,
        trade_value_cr=round(trade_value / 10_000_000, 2),
        spike_type=spike_type,
        timestamp=datetime.now(IST),
    )
