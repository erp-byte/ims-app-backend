"""Supabase CRUD for stock symbols and sector mappings."""
import json
import os
import psycopg2
from shared.fdk_logger import get_logger

log = get_logger("symbol_manager")


class SymbolManager:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._ensure_tables()
        self._seed_if_empty()

    def _get_conn(self):
        return psycopg2.connect(self.dsn)

    def _ensure_tables(self):
        """Create tables if they don't exist."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS stock_symbols (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        detector TEXT NOT NULL,
                        active BOOLEAN DEFAULT TRUE,
                        UNIQUE(symbol, detector)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS sector_mappings (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT UNIQUE NOT NULL,
                        sector TEXT NOT NULL
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_stock_symbols_detector
                    ON stock_symbols (detector, active)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_sector_mappings_symbol
                    ON sector_mappings (symbol)
                """)
            conn.commit()
        log.info("Supabase symbol/sector tables ready")

    def _seed_if_empty(self):
        """Seed from JSON files if tables are empty (first run)."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM stock_symbols")
                symbols_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM sector_mappings")
                sectors_count = cur.fetchone()[0]

        if symbols_count == 0 or sectors_count == 0:
            config_dir = os.path.join(
                os.path.dirname(__file__), "..", "..", "config"
            )
            if symbols_count == 0:
                self._seed_symbols_from_json(config_dir)
            if sectors_count == 0:
                self._seed_sectors_from_json(config_dir)

    def _seed_symbols_from_json(self, config_dir: str):
        symbols_path = os.path.join(config_dir, "fdk_symbols.json")
        try:
            with open(symbols_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for detector, symbols in data.items():
                self.seed_symbols(detector, symbols)
            log.info(f"Seeded symbols from {symbols_path}")
        except FileNotFoundError:
            log.warning("fdk_symbols.json not found, skipping seed")

    def _seed_sectors_from_json(self, config_dir: str):
        sectors_path = os.path.join(config_dir, "fdk_sectors.json")
        try:
            with open(sectors_path, "r", encoding="utf-8") as f:
                mapping = json.load(f)
            self.seed_sectors(mapping)
            log.info(f"Seeded {len(mapping)} sector mappings from {sectors_path}")
        except FileNotFoundError:
            log.warning("fdk_sectors.json not found, skipping seed")

    def load_symbols(self, detector: str) -> list[str]:
        """Load active symbols for a detector type."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT symbol FROM stock_symbols "
                    "WHERE detector = %s AND active = TRUE "
                    "ORDER BY symbol",
                    (detector,)
                )
                rows = cur.fetchall()
        symbols = [row[0] for row in rows]
        log.info(f"Loaded {len(symbols)} symbols for '{detector}'")
        return symbols

    def load_instrument_tokens(self) -> dict[str, str]:
        """Map each active symbol to its Shoonya token, which the feed subscribes by."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT s.symbol, i.shoonya_token "
                    "FROM stock_symbols s JOIN instruments i USING (isin) "
                    "WHERE s.active = TRUE"
                )
                rows = cur.fetchall()
        tokens = {row[0]: row[1] for row in rows}
        log.info(f"Loaded {len(tokens)} instrument tokens")
        return tokens

    def load_sector_mapping(self) -> dict[str, str]:
        """Load all sector mappings as a dict."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT symbol, sector FROM sector_mappings")
                rows = cur.fetchall()
        mapping = {row[0]: row[1] for row in rows}
        log.info(f"Loaded {len(mapping)} sector mappings")
        return mapping

    def seed_symbols(self, detector: str, symbols: list[str]):
        """Bulk insert symbols, skip duplicates."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                for symbol in symbols:
                    cur.execute(
                        "INSERT INTO stock_symbols (symbol, detector) "
                        "VALUES (%s, %s) ON CONFLICT (symbol, detector) DO NOTHING",
                        (symbol, detector)
                    )
            conn.commit()
        log.info(f"Seeded {len(symbols)} symbols for '{detector}'")

    def seed_sectors(self, mapping: dict[str, str]):
        """Bulk insert sector mappings, skip duplicates."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                for symbol, sector in mapping.items():
                    cur.execute(
                        "INSERT INTO sector_mappings (symbol, sector) "
                        "VALUES (%s, %s) ON CONFLICT (symbol) DO NOTHING",
                        (symbol, sector)
                    )
            conn.commit()
        log.info(f"Seeded {len(mapping)} sector mappings")
