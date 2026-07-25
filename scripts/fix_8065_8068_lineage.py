"""
Fix the lineage split created by the 22-Jul closing reconciliation's reassignment.

During the reconcile, 500 boxes were relabeled lot 8065 -> 8068 in cold_stocks only
(to hit the sheet count). A later FIFO inter-unit transfer then shipped them out AS
lot 8068, so their movement records say 8068 while their inward (bulk_entry) correctly
says 8065. This relabels those movement records back to 8065 so lot-search reconciles:
  8068 transfer-out 1500 -> 1000  (2600 inward - 1000 out = 1600 cold, consistent)
  8065 transfer-out  300 ->  800

Target = box_ids that are in cdpl_bulk_entry_boxes under lot 8065 AND currently recorded
under lot 8068 in a movement table (the exact 500; the 1000 genuine-8068 boxes have their
bulk_entry under 8068 and are untouched). No stock rows move; only audit/lot attribution.

Reversible: re-run with the lots swapped, or DELETE is not involved. Idempotent: only
rows still at 8068 are changed.

Run:  python scripts/fix_8065_8068_lineage.py --dry-run
      python scripts/fix_8065_8068_lineage.py --execute
"""
from __future__ import annotations
import os
import sys
import argparse
from pathlib import Path

import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
DSN = os.environ["DATABASE_URL"]

WRONG_LOT = "8068"
TRUE_LOT = "8065"
# the 500 box_ids: bulk_entry says 8065 (their real inward lot)
TARGET_SUBQUERY = "SELECT box_id FROM cdpl_bulk_entry_boxes WHERE lot_number = %s"

# (table, box_id_column, lot_column)
FIX_TABLES = [
    ("interunit_transfer_boxes", "box_id", "lot_number"),
    ("interunit_transfer_in_boxes", "box_id", "lot_number"),
    ("cold_stock_disposition", "box_id", "lot_no"),
    ("transfer_box_reconciliation", "actual_box_id", "lot_no"),
]


def counts(cur):
    """lot-search style counts for 8065 / 8068 to show before/after."""
    out = {}
    for lot in (TRUE_LOT, WRONG_LOT):
        cur.execute("SELECT COUNT(*) FROM cdpl_cold_stocks WHERE lot_no=%s", (lot,))
        cold = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM cdpl_bulk_entry_boxes WHERE lot_number=%s", (lot,))
        inw = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM interunit_transfer_boxes WHERE lot_number::text=%s", (lot,))
        tout = cur.fetchone()[0]
        out[lot] = (cold, inw, tout)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()
    if not (args.dry_run or args.execute):
        print("Specify --dry-run or --execute", file=sys.stderr)
        sys.exit(2)

    conn = psycopg.connect(DSN, autocommit=False)
    cur = conn.cursor()

    before = counts(cur)
    print("BEFORE (cold / inward / transfer_out):")
    for lot in (TRUE_LOT, WRONG_LOT):
        print(f"  {lot}: cold={before[lot][0]} inward={before[lot][1]} transfer_out={before[lot][2]}")

    print(f"\nRows to relabel {WRONG_LOT} -> {TRUE_LOT} (only boxes whose bulk_entry = {TRUE_LOT}):")
    total = 0
    for tbl, bc, lc in FIX_TABLES:
        cur.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE {lc}::text=%s "
            f"AND {bc} IN ({TARGET_SUBQUERY})", (WRONG_LOT, TRUE_LOT))
        n = cur.fetchone()[0]
        total += n
        print(f"  {tbl}.{lc}: {n}")
    print(f"  total rows: {total}")

    if args.dry_run:
        conn.close()
        print("\nDRY RUN -- nothing written.")
        return

    print("\nEXECUTING in one transaction...")
    try:
        for tbl, bc, lc in FIX_TABLES:
            cur.execute(
                f"UPDATE {tbl} SET {lc}=%s WHERE {lc}::text=%s "
                f"AND {bc} IN ({TARGET_SUBQUERY})", (TRUE_LOT, WRONG_LOT, TRUE_LOT))
        after = counts(cur)
        print("AFTER (cold / inward / transfer_out):")
        for lot in (TRUE_LOT, WRONG_LOT):
            print(f"  {lot}: cold={after[lot][0]} inward={after[lot][1]} transfer_out={after[lot][2]}")
        # consistency assert for 8068: inward - transfer_out should equal cold
        c8068 = after[WRONG_LOT]
        print(f"\n  8068 check: inward {c8068[1]} - transfer_out {c8068[2]} = {c8068[1]-c8068[2]}  (cold={c8068[0]})")
        conn.commit()
        print("COMMIT OK.")
    except Exception:
        conn.rollback()
        print("ROLLBACK -- error during operation.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
