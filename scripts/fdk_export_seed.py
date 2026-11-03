"""Export fdk's symbol universe from Supabase into the seed files an empty database starts from.

Writes config/fdk_instruments.json, config/fdk_symbols.json and config/fdk_sectors.json.
Run where FDK_SUPABASE_DSN is set, e.g. inside the backend container:

    docker exec ims-backend python scripts/fdk_export_seed.py
    docker cp ims-backend:/app/config/fdk_instruments.json config/
    docker cp ims-backend:/app/config/fdk_symbols.json config/
    docker cp ims-backend:/app/config/fdk_sectors.json config/

then commit the three files. A fresh database cannot get ISINs or company names from
Shoonya's symbol file, so this export is the only source for fdk_instruments.json.
"""
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def export_seed(connect, config_dir: str) -> list[Path]:
    """Query the three tables through `connect()` and write the JSON files; returns their paths."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT isin, company_name, series, verified_on, shoonya_symbol, shoonya_token "
                "FROM instruments ORDER BY shoonya_symbol")
            instruments = [
                {"isin": isin, "company_name": name, "series": series, "verified_on": str(verified),
                 "shoonya_symbol": symbol, "shoonya_token": str(token)}
                for isin, name, series, verified, symbol, token in cur.fetchall()
            ]
            cur.execute("SELECT detector, symbol FROM stock_symbols WHERE active = TRUE "
                        "ORDER BY detector, symbol")
            symbols: dict[str, list[str]] = {"shoonya": [], "penny": []}
            for detector, symbol in cur.fetchall():
                symbols.setdefault(detector, []).append(symbol)
            cur.execute("SELECT symbol, sector FROM sector_mappings ORDER BY symbol")
            sectors = {symbol: sector for symbol, sector in cur.fetchall()}

    out = Path(config_dir)
    out.mkdir(parents=True, exist_ok=True)
    files = {
        "fdk_instruments.json": instruments,
        "fdk_symbols.json": symbols,
        "fdk_sectors.json": sectors,
    }
    written = []
    for name, data in files.items():
        path = out / name
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        written.append(path)
    return written


def main(argv: list[str]) -> int:
    import psycopg2

    dsn = os.environ.get("FDK_SUPABASE_DSN", "").strip()
    if not dsn:
        print("FDK_SUPABASE_DSN is not set", file=sys.stderr)
        return 2
    config_dir = argv[1] if len(argv) > 1 else str(ROOT / "config")
    for path in export_seed(lambda: psycopg2.connect(dsn), config_dir):
        print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
