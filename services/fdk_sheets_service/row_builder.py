"""Build Google Sheets rows from TradeAlert objects."""
from shared.fdk_models import TradeAlert
from .models import SheetRow


def build_row(alert: TradeAlert) -> SheetRow:
    return SheetRow(
        date=alert.timestamp.strftime("%Y-%m-%d"),
        time=alert.timestamp.strftime("%H:%M:%S"),
        symbol=alert.symbol,
        ltp=round(alert.ltp, 2),
        volume_spike=alert.volume_spike,
        trade_value_cr=round(alert.trade_value / 10_000_000, 2),
        spike_type=alert.spike_type,
        sector=alert.sector,
    )


def row_to_list(row: SheetRow) -> list:
    return [row.date, row.time, row.symbol, row.ltp,
            row.volume_spike, row.trade_value_cr, row.spike_type, row.sector]
