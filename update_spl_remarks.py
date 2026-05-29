"""
Update spl_remarks in cdpl/cfpl_cold_stocks from cold_boxes_v5_new2.xlsx.
Lookup key: lot_no.

Run with --dry to preview, then with --commit to actually apply.
"""
import argparse
import os
import sys
from collections import defaultdict

import openpyxl
import psycopg
from dotenv import load_dotenv


SHEET_CONFIG = {
    "CDPL": {"table": "cdpl_cold_stocks", "lot_col": "Lot No", "remark_col": "Spl. Remarks"},
    "CFPL": {"table": "cfpl_cold_stocks", "lot_col": "Lot No", "remark_col": "Special remarks"},
}

XLSX_PATH = os.path.join(os.path.dirname(__file__), "cold_boxes_v5_new2.xlsx")


def build_lot_remark_map(sheet_name: str) -> dict:
    """Return {lot_no -> remark} for a sheet, taking the first non-empty remark per lot."""
    cfg = SHEET_CONFIG[sheet_name]
    wb = openpyxl.load_workbook(XLSX_PATH, read_only=True, data_only=True)
    ws = wb[sheet_name]

    rows = ws.iter_rows(values_only=True)
    header = [str(c).strip() if c is not None else "" for c in next(rows)]
    try:
        lot_idx = header.index(cfg["lot_col"])
        rem_idx = header.index(cfg["remark_col"])
    except ValueError:
        raise SystemExit(f"[{sheet_name}] Missing column. Headers: {header}")

    seen = {}
    conflicts = defaultdict(set)
    for r in rows:
        if r is None or len(r) <= max(lot_idx, rem_idx):
            continue
        lot = r[lot_idx]
        rem = r[rem_idx]
        if lot is None:
            continue
        # DB stores lot_no as VARCHAR — coerce to string, drop trailing .0 from float-cast ints
        if isinstance(lot, float) and lot.is_integer():
            lot = str(int(lot))
        else:
            lot = str(lot).strip()
        if not lot:
            continue
        if rem is None:
            continue
        rem_str = str(rem).strip()
        if not rem_str or rem_str.lower() in ("null", "none", "nan"):
            continue
        if lot in seen:
            if seen[lot] != rem_str:
                conflicts[lot].add(rem_str)
        else:
            seen[lot] = rem_str

    if conflicts:
        print(f"[{sheet_name}] WARN {len(conflicts)} lots had multiple distinct remarks; kept first.")
        for lot, vals in list(conflicts.items())[:5]:
            print(f"   lot {lot}: {sorted(vals)} (kept '{seen[lot]}')")
    print(f"[{sheet_name}] {len(seen)} lots with non-empty remarks.")
    return seen


def apply_updates(conn, table: str, lot_map: dict, dry: bool) -> tuple[int, int]:
    """Update spl_remarks for each lot. Returns (rows_affected, lots_not_in_db)."""
    rows_affected = 0
    missing_lots = 0
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT lot_no FROM {table}")
        db_lots = {row[0] for row in cur.fetchall()}

    matching = {lot: rem for lot, rem in lot_map.items() if lot in db_lots}
    missing_lots = len(lot_map) - len(matching)
    print(f"  {len(matching)} lots match DB / {missing_lots} not in DB")

    if dry:
        sample = list(matching.items())[:3]
        for lot, rem in sample:
            print(f"  PREVIEW lot={lot}: spl_remarks <- {rem!r}")
        # Estimate rows by counting boxes for matching lots
        if matching:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE lot_no = ANY(%s)",
                    (list(matching.keys()),),
                )
                rows_affected = cur.fetchone()[0]
        return rows_affected, missing_lots

    # Real update — executemany per lot (psycopg3 has no mogrify)
    items = list(matching.items())
    sql = f"UPDATE {table} SET spl_remarks = %s WHERE lot_no = %s"
    with conn.cursor() as cur:
        cur.executemany(sql, [(rem, lot) for lot, rem in items])
        rows_affected = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    if rows_affected == 0:
        # executemany rowcount is unreliable in some psycopg3 versions — recount
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {table} WHERE lot_no = ANY(%s)",
                (list(matching.keys()),),
            )
            rows_affected = cur.fetchone()[0]
    return rows_affected, missing_lots


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true", help="Actually apply updates")
    parser.add_argument("--dry", action="store_true", help="Dry run (default)")
    args = parser.parse_args()

    if not args.commit and not args.dry:
        args.dry = True

    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
    db_url = os.environ["DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://")

    with psycopg.connect(db_url) as conn:
        for sheet, cfg in SHEET_CONFIG.items():
            print(f"\n=== {sheet} -> {cfg['table']} ===")
            lot_map = build_lot_remark_map(sheet)
            rows, missing = apply_updates(conn, cfg["table"], lot_map, dry=args.dry)
            print(f"  {'WOULD UPDATE' if args.dry else 'UPDATED'} {rows} rows")
        if args.commit:
            conn.commit()
            print("\nCOMMIT done.")
        else:
            conn.rollback()
            print("\nDRY RUN — no changes saved. Re-run with --commit to apply.")


if __name__ == "__main__":
    main()
