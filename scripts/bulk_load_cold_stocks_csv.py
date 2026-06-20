"""
Bulk-load cfpl_cold_stocks and cdpl_cold_stocks from CSV files using COPY.

CSVs:
  backend/cfpl_cold_stocks.csv  -> public.cfpl_cold_stocks
  backend/cdpl_cold_stocks.csv  -> public.cdpl_cold_stocks

Assumes both tables are already truncated. Uses psycopg COPY for speed.
After insert, resets the SERIAL sequence to max(id) + 1.
"""

from __future__ import annotations
import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
DSN = os.environ["DATABASE_URL"]  # from .env — never hardcode credentials

JOBS = [
    ("cfpl_cold_stocks", ROOT / "cfpl_cold_stocks.csv"),
    ("cdpl_cold_stocks", ROOT / "cdpl_cold_stocks.csv"),
]


def _parse_date(s):
    s = (s or "").strip()
    if not s or s.upper() == "NULL":
        return None
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unparseable date: {s!r}")


def _parse_ts(s):
    s = (s or "").strip()
    if not s or s.upper() == "NULL":
        return None
    for fmt in (
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d-%m-%Y",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"unparseable timestamp: {s!r}")


def _num(s):
    s = (s or "").strip()
    if not s or s.upper() == "NULL":
        return None
    return s  # COPY can take the numeric as text; PG will cast


def _txt(s):
    """Empty string -> None; 'NULL' literal -> None."""
    if s is None:
        return None
    s = s.strip()
    if not s or s.upper() == "NULL":
        return None
    return s


def _bool(s):
    s = (s or "").strip().upper()
    if not s or s == "NULL":
        return None
    if s in ("TRUE", "T", "1", "YES", "Y"):
        return True
    if s in ("FALSE", "F", "0", "NO", "N"):
        return False
    raise ValueError(f"unparseable boolean: {s!r}")


def _int(s):
    s = (s or "").strip()
    if not s or s.upper() == "NULL":
        return None
    return int(s)


# Per-column transformer functions. Keyed by column name.
TRANSFORMERS = {
    "id": _int,
    "inward_dt": _parse_date,
    "unit": _txt,
    "inward_no": _txt,
    "cold_item_mark": _txt,
    "vakkal": _txt,
    "lot_no": _txt,
    "no_of_cartons": _num,
    "weight_kg": _num,
    "total_inventory_kgs": _num,
    "group_name": _txt,
    "item_description": _txt,
    "storage_location": _txt,
    "exporter": _txt,
    "last_purchase_rate": _num,
    "value": _num,
    "created_at": _parse_ts,
    "updated_at": _parse_ts,
    "box_id": _txt,
    "transaction_no": _txt,
    "item_subgroup": _txt,
    "item_mark": _txt,
    "inward_transaction_no": _txt,
    "auto_created_from_inward": _bool,
    "spl_remarks": _txt,
    "canonical_warehouse": _txt,
    "canonical_group": _txt,
    "canonical_subgroup": _txt,
}


def load_table(conn, table: str, csv_path: Path, dry_run: bool = False):
    print(f"\n=== {table} <- {csv_path.name} ===")
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    with csv_path.open(encoding="utf-8-sig", newline="") as fh:
        rd = csv.reader(fh)
        header = next(rd)

        # Get DB column types
        cur = conn.cursor()
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s ORDER BY ordinal_position",
            (table,),
        )
        db_cols = [r[0] for r in cur.fetchall()]
        if header != db_cols:
            print(f"  CSV columns:  {header}")
            print(f"  DB columns:   {db_cols}")
            raise SystemExit(f"Column order mismatch for {table}")

        # Pre-compute transformer list in header order
        transformers = [TRANSFORMERS[c] for c in header]

        if dry_run:
            n = 0
            bad = 0
            for row in rd:
                n += 1
                try:
                    [t(v) for t, v in zip(transformers, row)]
                except Exception as e:
                    bad += 1
                    if bad <= 5:
                        print(f"  parse error row {n}: {e} | row={row}")
            print(f"  DRY: parsed {n:,} rows, errors: {bad}")
            return

        # Pre-check table is empty
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        cnt = cur.fetchone()[0]
        if cnt > 0:
            raise SystemExit(f"{table} is NOT empty ({cnt} rows) — refusing to bulk load")

        # COPY ... FROM STDIN
        col_list = ", ".join(header)
        copy_sql = f"COPY {table} ({col_list}) FROM STDIN"
        loaded = 0
        with cur.copy(copy_sql) as cp:
            for row in rd:
                converted = tuple(t(v) for t, v in zip(transformers, row))
                cp.write_row(converted)
                loaded += 1
                if loaded % 10000 == 0:
                    print(f"  ...{loaded:,} rows")
        print(f"  COPY done: {loaded:,} rows")

        # Sequence reset is done once after BOTH tables load (see main()),
        # because cfpl_cold_stocks and cdpl_cold_stocks share the same sequence
        # (cold_storage_stocks_id_seq). Resetting per-table would leave it at
        # the lower of the two max(id) values and cause collisions.


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()
    if not (args.dry_run or args.execute):
        print("specify --dry-run or --execute", file=sys.stderr)
        sys.exit(2)

    print(f"DB: {DSN.split('@')[1]}")
    conn = psycopg.connect(DSN, autocommit=False)
    try:
        for table, csv_path in JOBS:
            load_table(conn, table, csv_path, dry_run=args.dry_run)
        if args.execute:
            cur = conn.cursor()
            # Final counts (in-tx)
            for table, _ in JOBS:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                print(f"  {table} row count: {cur.fetchone()[0]:,}")
            # Reset the SHARED sequence to GREATEST(max(id)) across both tables.
            # Both cfpl_ and cdpl_cold_stocks default id to nextval of
            # cold_storage_stocks_id_seq, so per-table reset would underset it.
            cur.execute(
                """SELECT setval('cold_storage_stocks_id_seq',
                                  GREATEST(
                                    COALESCE((SELECT MAX(id) FROM cfpl_cold_stocks), 0),
                                    COALESCE((SELECT MAX(id) FROM cdpl_cold_stocks), 0),
                                    1
                                  ))"""
            )
            print(f"  shared sequence cold_storage_stocks_id_seq set to {cur.fetchone()[0]:,}")
            conn.commit()
            print("\nCOMMIT OK.")
    except Exception:
        conn.rollback()
        print("\nROLLBACK.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
