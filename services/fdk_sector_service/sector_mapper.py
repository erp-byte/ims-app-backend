"""Sector classification for NSE stock symbols."""
from shared.fdk_logger import get_logger

log = get_logger("sector_mapper")

_SECTOR_MAPPING: dict[str, str] = {}


def init_sector_mapping(mapping: dict[str, str]):
    """Initialize the global sector mapping from Supabase data."""
    global _SECTOR_MAPPING
    _SECTOR_MAPPING = mapping
    log.info(f"Sector mapping initialized ({len(_SECTOR_MAPPING)} entries)")


def get_sector(symbol: str) -> str:
    """Look up sector for a symbol. Returns 'Others' if not found."""
    return _SECTOR_MAPPING.get(symbol, "Others")
