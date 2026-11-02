"""Sheets service models."""
from pydantic import BaseModel


class SheetRow(BaseModel):
    date: str
    time: str
    symbol: str
    ltp: float
    volume_spike: int
    trade_value_cr: float
    spike_type: str
    sector: str
