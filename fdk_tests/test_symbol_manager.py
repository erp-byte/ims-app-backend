"""An empty Supabase must come up able to run the feed: instruments, stock_symbols.isin
and the seed files are the code's job, not a hand-run migration's."""
import json

import psycopg2
import pytest

from fdk_fakes import FakeConn, FakeCursor
from services.fdk_sector_service.symbol_manager import SymbolManager


class _Cursor(FakeCursor):
    counts = {"instruments": 1, "stock_symbols": 1, "sector_mappings": 1}
    log: list = []

    def execute(self, sql, params=None):
        super().execute(sql, params)
        _Cursor.log.append((" ".join(sql.split()), params))

    def fetchone(self):
        sql = self.last_sql.upper()
        if "COUNT" in sql:
            for table, count in _Cursor.counts.items():
                if table.upper() in sql:
                    return (count,)
            return (1,)
        return None


class _Conn(FakeConn):
    def cursor(self):
        return _Cursor()


@pytest.fixture
def db(monkeypatch):
    _Cursor.log = []
    _Cursor.counts = {"instruments": 1, "stock_symbols": 1, "sector_mappings": 1}
    monkeypatch.setattr(psycopg2, "connect", lambda *a, **k: _Conn())
    return _Cursor


def _sql():
    return [sql for sql, _ in _Cursor.log]


def _index(fragment):
    return next(i for i, sql in enumerate(_sql()) if fragment in sql)


def test_tables_are_created_with_instruments_first_and_isin_on_stock_symbols(db):
    SymbolManager("dsn")

    assert _index("CREATE TABLE IF NOT EXISTS instruments") < _index("CREATE TABLE IF NOT EXISTS stock_symbols")
    instruments = next(s for s in _sql() if "CREATE TABLE IF NOT EXISTS instruments" in s)
    for column in ("isin TEXT PRIMARY KEY", "shoonya_symbol TEXT NOT NULL UNIQUE",
                   "shoonya_token TEXT NOT NULL UNIQUE", "series TEXT NOT NULL", "verified_on DATE NOT NULL"):
        assert column in instruments
    # an existing database gets the column added, a new one has it from the start. The ALTER
    # must not carry REFERENCES: PostgreSQL skips only the column when it exists and would add
    # a duplicate foreign key (stock_symbols_isin_fkey1, 2, ...) on every start-up.
    alter = next(s for s in _sql() if "ALTER TABLE stock_symbols ADD COLUMN IF NOT EXISTS isin" in s)
    assert "REFERENCES" not in alter
    assert any("idx_stock_symbols_isin" in s for s in _sql())


def test_symbols_with_no_isin_are_linked_to_instruments_by_name(db):
    SymbolManager("dsn")
    backfill = next(s for s in _sql() if "UPDATE stock_symbols" in s and "FROM instruments" in s)
    assert "isin IS NULL" in backfill and "shoonya_symbol = s.symbol" in backfill


def test_an_empty_database_is_seeded_instruments_first_with_isins(db, tmp_path):
    (tmp_path / "fdk_instruments.json").write_text(json.dumps([
        {"isin": "INE467B01029", "company_name": "Tata Consultancy Services", "series": "EQ",
         "verified_on": "2026-11-02", "shoonya_symbol": "TCS-EQ", "shoonya_token": "11536"},
    ]), encoding="utf-8")
    (tmp_path / "fdk_symbols.json").write_text(json.dumps({"shoonya": ["TCS-EQ"], "penny": []}), encoding="utf-8")
    (tmp_path / "fdk_sectors.json").write_text(json.dumps({"TCS-EQ": "Information Technology"}), encoding="utf-8")
    db.counts = {"instruments": 0, "stock_symbols": 0, "sector_mappings": 0}

    SymbolManager("dsn", config_dir=str(tmp_path))

    inst = _index("INSERT INTO instruments")
    sym = _index("INSERT INTO stock_symbols")
    sec = _index("INSERT INTO sector_mappings")
    assert inst < sym < sec
    inst_sql, inst_params = _Cursor.log[inst]
    assert "ON CONFLICT (isin) DO NOTHING" in inst_sql
    assert inst_params[0] == "INE467B01029" and "11536" in inst_params
    sym_sql, sym_params = _Cursor.log[sym]
    assert "(SELECT isin FROM instruments WHERE shoonya_symbol = %s)" in sym_sql
    assert sym_params == ("TCS-EQ", "shoonya", "TCS-EQ")
    assert _Cursor.log[sec][1] == ("TCS-EQ", "Information Technology")


def test_a_seeded_database_is_left_alone(db):
    SymbolManager("dsn")
    assert not any("INSERT INTO" in s for s in _sql())


def test_a_missing_instruments_file_is_a_warning_not_a_crash(db, tmp_path, caplog):
    db.counts = {"instruments": 0, "stock_symbols": 1, "sector_mappings": 1}
    import logging
    with caplog.at_level(logging.WARNING, logger="fdk.symbol_manager"):
        SymbolManager("dsn", config_dir=str(tmp_path))
    assert "fdk_instruments.json" in " ".join(r.getMessage() for r in caplog.records)


def test_the_shipped_seed_files_use_shoonya_names():
    import pathlib
    config = pathlib.Path(__file__).resolve().parents[1] / "config"
    symbols = json.loads((config / "fdk_symbols.json").read_text(encoding="utf-8"))
    sectors = json.loads((config / "fdk_sectors.json").read_text(encoding="utf-8"))
    assert set(symbols) == {"shoonya", "penny"}
    assert not any(s.startswith("NSE:") for lst in symbols.values() for s in lst)
    assert not any(s.startswith("NSE:") for s in sectors)


# ----------------------------------------------------------------- export script
def test_export_seed_writes_the_three_files_from_the_database(tmp_path):
    import importlib.util
    import pathlib
    path = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "fdk_export_seed.py"
    spec = importlib.util.spec_from_file_location("fdk_export_seed", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    class Cursor(FakeCursor):
        def fetchall(self):
            sql = self.last_sql.lower()
            if "from instruments" in sql:
                return [("INE467B01029", "Tata Consultancy Services", "EQ", "2026-11-02", "TCS-EQ", "11536")]
            if "from stock_symbols" in sql:
                return [("penny", "IDEA-EQ"), ("shoonya", "TCS-EQ")]
            return [("TCS-EQ", "Information Technology")]

    class Conn(FakeConn):
        def cursor(self):
            return Cursor()

    written = mod.export_seed(lambda: Conn(), str(tmp_path))

    assert {p.name for p in written} == {"fdk_instruments.json", "fdk_symbols.json", "fdk_sectors.json"}
    instruments = json.loads((tmp_path / "fdk_instruments.json").read_text(encoding="utf-8"))
    assert instruments == [{"isin": "INE467B01029", "company_name": "Tata Consultancy Services",
                            "series": "EQ", "verified_on": "2026-11-02",
                            "shoonya_symbol": "TCS-EQ", "shoonya_token": "11536"}]
    assert json.loads((tmp_path / "fdk_symbols.json").read_text(encoding="utf-8")) == \
        {"shoonya": ["TCS-EQ"], "penny": ["IDEA-EQ"]}
    assert json.loads((tmp_path / "fdk_sectors.json").read_text(encoding="utf-8")) == \
        {"TCS-EQ": "Information Technology"}
