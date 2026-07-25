"""
Repair the duplicate box_id that blocks Cold-transfer for lot 125859.

PROBLEM
-------
cdpl_cold_stocks pile (item='Fresho Kimia Dates 500 Gm', lot=125859, inward='Rishi Cold')
has 67 rows but only 66 distinct box_id values. box_id '90671000-1' appears twice:
  - id 782623  txn TR-20260314174751  weight 5.000  (the lone 5 kg box)
  - id 782557  txn TR-20260314174752  weight 7.500  (first of 66 boxes 90671000-1..66)
The two rows have DIFFERENT transaction_no, so the cold_stocks UNIQUE(transaction_no, box_id)
constraint permits both — but /cold-storage/stocks/pick-boxes groups by (item, lot, inward_no)
only and returns all 67, so the frontend uniqueness guard aborts with
"Duplicate Box IDs From Source".

FIX
---
Relabel the lone TR-20260314174751 box (id 782623) from '90671000-1' to '90671000-67'
(the next free suffix in this pile — the pile currently uses -1..-66). This makes all 67
box_ids in the pile distinct while keeping the (transaction_no, box_id) pair unique.
No carton/weight/quantity change — pure identity relabel of ONE row.

USAGE
  Dry-run (default, NO writes):  python fix_lot125859_dupbox.py
  Apply (writes in a txn):       python fix_lot125859_dupbox.py --apply
"""
import argparse
import os
import sys
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

TABLE = "cdpl_cold_stocks"
ROW_ID = 782623
OLD_BOX_ID = "90671000-1"
NEW_BOX_ID = "90671000-67"
TXN = "TR-20260314174751"
ITEM = "Fresho Kimia Dates 500 Gm"
LOT = "125859"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Actually write (default: dry-run)")
    args = ap.parse_args()

    raw = os.environ["DATABASE_URL"]
    url = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
    engine = create_engine(url)

    with engine.begin() as c:
        # 1) Confirm the target row is exactly what we expect
        row = c.execute(text(f"""
            SELECT id, box_id, transaction_no, weight_kg, item_description, CAST(lot_no AS TEXT) AS lot
            FROM {TABLE} WHERE id = :id
        """), {"id": ROW_ID}).fetchone()
        if not row:
            print(f"ABORT: row id {ROW_ID} not found in {TABLE}.")
            return 1
        m = row._mapping
        print(f"Target row: id={m['id']} box_id={m['box_id']} txn={m['transaction_no']} "
              f"weight={m['weight_kg']} item={m['item_description']!r} lot={m['lot']}")
        if not (m["box_id"] == OLD_BOX_ID and m["transaction_no"] == TXN
                and m["item_description"] == ITEM and m["lot"] == LOT):
            print("ABORT: row does not match expected identity — data changed. Re-diagnose before applying.")
            return 1

        # 2) Ensure the NEW box_id is free within this pile
        clash = c.execute(text(f"""
            SELECT COUNT(*) FROM {TABLE}
            WHERE item_description = :item AND CAST(lot_no AS TEXT) = :lot
              AND COALESCE(inward_no,'') = (SELECT COALESCE(inward_no,'') FROM {TABLE} WHERE id = :id)
              AND box_id = :new
        """), {"item": ITEM, "lot": LOT, "id": ROW_ID, "new": NEW_BOX_ID}).scalar()
        if clash:
            print(f"ABORT: {NEW_BOX_ID} already used in this pile — pick another free suffix.")
            return 1

        # 3) Ensure (txn, new box_id) pair is free (UNIQUE constraint safety)
        pair_clash = c.execute(text(f"""
            SELECT COUNT(*) FROM {TABLE} WHERE transaction_no = :txn AND box_id = :new
        """), {"txn": TXN, "new": NEW_BOX_ID}).scalar()
        if pair_clash:
            print(f"ABORT: pair ({TXN}, {NEW_BOX_ID}) already exists — would violate UNIQUE. Pick another suffix.")
            return 1

        print(f"\nPlanned change: UPDATE {TABLE} SET box_id='{NEW_BOX_ID}' WHERE id={ROW_ID} "
              f"(was '{OLD_BOX_ID}', txn {TXN})")

        if args.apply:
            c.execute(text(f"UPDATE {TABLE} SET box_id = :new WHERE id = :id"),
                      {"new": NEW_BOX_ID, "id": ROW_ID})
            # 4) Post-check: pile now fully distinct
            chk = c.execute(text(f"""
                SELECT COUNT(*) AS rows, COUNT(DISTINCT box_id) AS distinct_ids
                FROM {TABLE}
                WHERE item_description = :item AND CAST(lot_no AS TEXT) = :lot
                  AND COALESCE(inward_no,'') = (SELECT COALESCE(inward_no,'') FROM {TABLE} WHERE id = :id)
            """), {"item": ITEM, "lot": LOT, "id": ROW_ID}).fetchone()
            print(f"Post-fix pile: rows={chk._mapping['rows']} distinct_box_ids={chk._mapping['distinct_ids']} "
                  f"({'OK' if chk._mapping['rows'] == chk._mapping['distinct_ids'] else 'STILL BROKEN'})")
            print("[OK] COMMITTED.")
        else:
            print("\n(dry-run — no changes written. Re-run with --apply to commit.)")
            # roll back the implicit txn by raising nothing; engine.begin commits on exit,
            # but we made no writes in dry-run, so nothing to undo.

    return 0


if __name__ == "__main__":
    sys.exit(main())
