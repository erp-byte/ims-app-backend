"""Sector service utilities."""
from .sector_mapper import get_sector, _SECTOR_MAPPING


def get_all_sectors() -> set[str]:
    return set(_SECTOR_MAPPING.values())


def get_symbols_for_sector(sector: str) -> list[str]:
    return [s for s, sec in _SECTOR_MAPPING.items() if sec == sector]
