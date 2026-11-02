"""Shared data models."""
from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class FyersToken(BaseModel):
    access_token: str
    timestamp: float
    created_at: str


class TickData(BaseModel):
    symbol: str
    ltp: float
    vol_traded_today: float
    type: Optional[str] = None


class TradeAlert(BaseModel):
    symbol: str
    sector: str
    ltp: float
    volume_spike: int
    trade_value: float       # raw rupees
    trade_value_cr: float    # in crores
    spike_type: str          # "Large Spike", "Medium Spike", "Volume Increase"
    timestamp: datetime


class DetectorConfig(BaseModel):
    """Per-detector configuration (fyers vs penny)."""
    name: str                        # "fyers" or "penny"
    threshold: int
    google_sheet_id: str
    symbols: list[str]
    sector_mapping: dict[str, str]
