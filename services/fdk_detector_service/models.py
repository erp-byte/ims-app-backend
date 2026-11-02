"""Detector service models."""
from pydantic import BaseModel
from typing import Optional


class TickState(BaseModel):
    previous_volume: float = 0.0
    previous_ltp: Optional[float] = None
    last_alert_time: float = 0.0
