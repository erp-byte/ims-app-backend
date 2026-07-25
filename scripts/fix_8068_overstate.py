"""
Correct lot 8068: it was OVER-stated because the closing reconcile relabeled 500
boxes 8065->8068, and a later FIFO transfer then shipped THOSE (lowest-numbered)
boxes as 8068 instead of 500 real 8068 boxes -> 500 real boxes never got deducted.

User confirmed truth (2026-07-25): 8068 = 2,600 inward - 1,500 out = 1,100 cold.

Two parts, one transaction:
  A) UNDO the earlier lineage relabel: move the 500 transfer/disposition records for
     boxes 27179729-9901..10400 back from lot 8065 -> 8068, so transfer-out reads 1,500.
     (These 500 are cleanly separable: suffix 9901-10400, created 2026-07-24 07:22;
      8065's genuine 300 transfers are suffix 7801-8100, created 2026-07-14 — disjoint.)
  B) REMOVE the 500 over-counted boxes from cold_stocks 8068 (the lowest 500 by suffix,
     27179729-11401..11900), leaving 11901..13000 = 1,100. WITH a cold_stock_disposition
     row (disposition_type='manual_correction') per box, so there is a real audit trail.

Result: 8068 inward 2,600, transfer-out 1,500, cold 1,100 (reconciles).
8065 is intentionally NOT touched here (its cold count 1,150 already matches the sheet).

Run:  python scripts/fix_8068_overstate.py --dry-run
      python scripts/fix_8068_overstate.py --execute
"""
from __future__ import annotations
import os
import sys
import argparse
from datetime import datetime
from pathlib import Path

import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
DSN = os.environ["DATABASE_URL"]

LO, HI = 9901, 10400          # the 500 to un-relabel (8065 -> 8068)
REMOVE_LO, REMOVE_HI = 11401, 11900   # the 500 to delete from cold_stocks 8068
PREFIX = "27179729"
REF = "RECON22JUL-FIX"

# undo-relabel targets: (table, box_col, lot_col)
UNDO = [
    ("interunit_transfer_boxes", "box_id", "lot_number"),
    ("interunit_transfer_in_boxes", "box_id", "lot_number"),
    ("cold_stock_disposition", "box_id", "lot_no"),
    ("transfer_box_reconciliation", "actual_box_id", "lot_no"),
]


def snap(cur):
    cur.execute("SELECT COUNT(*) FROM cdpl_cold_stocks WHERE lot_no='8068'")
    cold = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM cdpl_bulk_entry_boxes WHERE lot_number='8068'")
    inw = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM interunit_transfer_boxes WHERE lot_number::text='8068'")
    tout = cur.fetchone()[0]
    return cold, inw, tout


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
    c, i, t = snap(cur)
    print(f"BEFORE 8068: cold={c} inward={i} transfer_out={t}  (inward-out={i-t})")

    # part A preview
    print(f"\nA) un-relabel 8065->8068 for boxes {PREFIX}-{LO}..{HI}:")
    a_total = 0
    for tbl, bc, lc in UNDO:
        cur.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE {lc}::text='8065' AND {bc} LIKE %s "
            f"AND CAST(split_part({bc},'-',2) AS INT) BETWEEN %s AND %s",
            (PREFIX + "-%", LO, HI))
        n = cur.fetchone()[0]
        a_total += n
        print(f"   {tbl}.{lc}: {n}")

    # part B preview
    cur.execute(
        "SELECT COUNT(*) FROM cdpl_cold_stocks WHERE lot_no='8068' AND box_id LIKE %s "
        "AND CAST(split_part(box_id,'-',2) AS INT) BETWEEN %s AND %s",
        (PREFIX + "-%", REMOVE_LO, REMOVE_HI))
    b_n = cur.fetchone()[0]
    print(f"\nB) remove {b_n} boxes from cold_stocks 8068 ({PREFIX}-{REMOVE_LO}..{REMOVE_HI}) "
          f"+ write {b_n} manual_correction disposition rows")

    if b_n != 500 or a_total != 2000:
        print(f"\n!! sanity: expected A=2000 rows (got {a_total}), B=500 boxes (got {b_n}). "
              f"Review before executing.")

    if args.dry_run:
        conn.close()
        print("\nDRY RUN -- nothing written.")
        return

    print("\nEXECUTING in one transaction...")
    try:
        now = datetime.utcnow()
        # A) un-relabel
        for tbl, bc, lc in UNDO:
            cur.execute(
                f"UPDATE {tbl} SET {lc}='8068' WHERE {lc}::text='8065' AND {bc} LIKE %s "
                f"AND CAST(split_part({bc},'-',2) AS INT) BETWEEN %s AND %s",
                (PREFIX + "-%", LO, HI))
        # B) snapshot the 500 to remove, write disposition, then delete
        cur.execute(
            "SELECT box_id, transaction_no, lot_no, item_description, unit, storage_location "
            "FROM cdpl_cold_stocks WHERE lot_no='8068' AND box_id LIKE %s "
            "AND CAST(split_part(box_id,'-',2) AS INT) BETWEEN %s AND %s",
            (PREFIX + "-%", REMOVE_LO, REMOVE_HI))
        victims = cur.fetchall()
        disp_rows = [(
            v[0], v[1], v[2], v[3], "CDPL", v[4], v[5], "cdpl_cold_stocks",
            "manual_correction", REF, now, "reconcile-fix", False,
            "8068 overstatement correction: box transferred out (as 9901-10400 via "
            "RECON22JUL reassignment) but a real 8068 box was never deducted", now,
        ) for v in victims]
        cur.executemany(
            "INSERT INTO cold_stock_disposition "
            "(box_id, transaction_no, lot_no, item_description, from_company, unit, from_site, "
            " source_table, disposition_type, disposition_ref_no, disposed_at, disposed_by, "
            " reverted, notes, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", disp_rows)
        cur.execute(
            "DELETE FROM cdpl_cold_stocks WHERE lot_no='8068' AND box_id LIKE %s "
            "AND CAST(split_part(box_id,'-',2) AS INT) BETWEEN %s AND %s",
            (PREFIX + "-%", REMOVE_LO, REMOVE_HI))
        deleted = cur.rowcount

        c2, i2, t2 = snap(cur)
        print(f"  disposition rows written: {len(disp_rows)} | cold rows deleted: {deleted}")
        print(f"AFTER 8068: cold={c2} inward={i2} transfer_out={t2}  (inward-out={i2-t2})")
        assert c2 == 1100 and t2 == 1500 and (i2 - t2) == c2, "post-state check failed!"
        conn.commit()
        print("COMMIT OK — 8068 reconciles: 2600 - 1500 = 1100.")
    except Exception:
        conn.rollback()
        print("ROLLBACK -- error during operation.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
