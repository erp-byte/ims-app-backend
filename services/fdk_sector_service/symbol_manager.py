"""Supabase CRUD for instruments, stock symbols and sector mappings.

An empty database comes up ready to run the feed: the three tables are created
here, then seeded from config/fdk_instruments.json, fdk_symbols.json and
fdk_sectors.json (exported from a live database with scripts/fdk_export_seed.py).
After that Supabase is the source of truth; instrument_refresh.py keeps the tokens
and names current each trading day.
"""
import json
import os
import psycopg2
from shared.fdk_logger import get_logger

log = get_logger("symbol_manager")

DEFAULT_CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "config")


class SymbolManager:
    def __init__(self, dsn: str, config_dir: str | None = None):
        self.dsn = dsn
        self.config_dir = config_dir or DEFAULT_CONFIG_DIR
        self._ensure_tables()
        self._seed_if_empty()
        self._link_symbols_to_instruments()

    def _get_conn(self):
        return psycopg2.connect(self.dsn)

    def _ensure_tables(self):
        """Create tables if they don't exist. instruments first: stock_symbols refers to it."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS instruments (
                        isin           TEXT PRIMARY KEY,
                        company_name   TEXT NOT NULL,
                        series         TEXT NOT NULL,
                        verified_on    DATE NOT NULL,
                        shoonya_symbol TEXT NOT NULL UNIQUE,
                        shoonya_token  TEXT NOT NULL UNIQUE
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS stock_symbols (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        detector TEXT NOT NULL,
                        active BOOLEAN DEFAULT TRUE,
                        isin TEXT REFERENCES instruments (isin),
                        UNIQUE(symbol, detector)
                    )
                """)
                # A database from before the Shoonya switch has the table without the column.
                # No REFERENCES here: PostgreSQL skips only the column when it exists and
                # would add a duplicate foreign key on every start-up; the one from the
                # 2026-11-02 migration (or the CREATE TABLE above) already exists.
                cur.execute("""
                    ALTER TABLE stock_symbols
                    ADD COLUMN IF NOT EXISTS isin TEXT
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
                    CREATE INDEX IF NOT EXISTS idx_stock_symbols_isin
                    ON stock_symbols (isin)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_sector_mappings_symbol
                    ON sector_mappings (symbol)
                """)
            conn.commit()
        log.info("Supabase symbol/sector tables ready")

    def _seed_if_empty(self):
        """Seed from JSON files if tables are empty (first run). Instruments go first so
        the symbols can pick up their ISIN."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM instruments")
                instruments_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM stock_symbols")
                symbols_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM sector_mappings")
                sectors_count = cur.fetchone()[0]

        if instruments_count == 0:
            self._seed_instruments_from_json(self.config_dir)
        if symbols_count == 0:
            self._seed_symbols_from_json(self.config_dir)
        if sectors_count == 0:
            self._seed_sectors_from_json(self.config_dir)

    def _link_symbols_to_instruments(self):
        """Give every symbol without an ISIN the instrument of the same name (idempotent)."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE stock_symbols s SET isin = i.isin
                    FROM instruments i
                    WHERE s.isin IS NULL AND i.shoonya_symbol = s.symbol
                """)
            conn.commit()

    def _seed_instruments_from_json(self, config_dir: str):
        path = os.path.join(config_dir, "fdk_instruments.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                rows = json.load(f)
            self.seed_instruments(rows)
            log.info(f"Seeded instruments from {path}")
        except FileNotFoundError:
            log.warning("fdk_instruments.json not found, skipping seed — no symbol can be "
                        "subscribed until instruments is filled (scripts/fdk_export_seed.py)")

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

    def seed_instruments(self, rows: list[dict]):
        """Bulk insert instruments (one row per company), skip duplicates."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                for row in rows:
                    cur.execute(
                        "INSERT INTO instruments "
                        "(isin, company_name, series, verified_on, shoonya_symbol, shoonya_token) "
                        "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (isin) DO NOTHING",
                        (row["isin"], row["company_name"], row["series"], row["verified_on"],
                         row["shoonya_symbol"], str(row["shoonya_token"]))
                    )
            conn.commit()
        log.info(f"Seeded {len(rows)} instruments")

    def seed_symbols(self, detector: str, symbols: list[str]):
        """Bulk insert symbols with the ISIN of the instrument of the same name, skip duplicates."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                for symbol in symbols:
                    cur.execute(
                        "INSERT INTO stock_symbols (symbol, detector, isin) "
                        "VALUES (%s, %s, (SELECT isin FROM instruments WHERE shoonya_symbol = %s)) "
                        "ON CONFLICT (symbol, detector) DO NOTHING",
                        (symbol, detector, symbol)
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
