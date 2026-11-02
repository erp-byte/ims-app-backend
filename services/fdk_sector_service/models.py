"""Sector service models."""
from pydantic import BaseModel


class SectorInfo(BaseModel):
    symbol: str
    sector: str
