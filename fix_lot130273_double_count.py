"""
Remove the 5 boxes of lot 130273 that are counted in cold stock AND in transit.

PROBLEM
-------
TRANS202605221134 (transfer_out 770, Cold Storage -> F53) dispatched 5 boxes of
'Indian Green Raisins' lot 130273. All 11 cfpl_cold_stocks rows of that pile carry
transaction_no = NULL (the pile was re-inserted during a disposition recovery), and the
dispatch looks the source row up with

    WHERE box_id = :bid AND transaction_no = :tno        -- :tno arrives as ''

`'' = NULL` is NULL in SQL, so no source row was found: the pending (In Transit) rows
were written but the cold rows were never deducted. The pile therefore shows 11 cartons
plus "+5 in transit" — 165 kg on a 90 kg pile.

The lookup itself is fixed in pending_stock_tools._find_in_cold_stocks (COALESCE on
both sides), so no new dispatch can do this. This script only clears the 5 rows the
old code left behind.

FIX
---
DELETE the 5 cfpl_cold_stocks rows whose box_id matches an In-Transit pending row of
transfer 770 (box_ids 90514000-1..5). The boxes are physically on the truck; the
pending rows are the authoritative record of them. 6 rows remain in cold = the pile's
real balance. Weights/cartons of the surviving rows are untouched.

If the transfer is later deleted, restore_to_source re-inserts these rows from
pending_transfer_stock.cold_storage_data, so the reversal path still works.

USAGE
  Dry-run (default, NO writes):  python fix_lot130273_double_count.py
  Apply (writes in a txn):       python fix_lot130273_double_count.py --apply
"""
import argparse
import json
import os
import sys
from datetime import datetime

from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

TABLE = "cfpl_cold_stocks"
TRANSFER_OUT_ID = 770
LOT = "130273"
ITEM = "Indian Green Raisins"
EXPECTED_BOXES = 5

SELECT_DOUBLES = f"""
    SELECT s.*
    FROM {TABLE} s
    JOIN pending_transfer_stock p
      ON p.box_id = s.box_id
     AND p.status = 'In Transit'
     AND p.transfer_out_id = :tout
    WHERE (s.transaction_no IS NULL OR TRIM(s.transaction_no) = '')
      AND CAST(s.lot_no AS TEXT) = :lot
    ORDER BY s.id
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Actually write (default: dry-run)")
    args = ap.parse_args()

    raw = os.environ["DATABASE_URL"]
    url = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
    engine = create_engine(url)

    with engine.begin() as c:
        rows = c.execute(text(SELECT_DOUBLES), {"tout": TRANSFER_OUT_ID, "lot": LOT}).fetchall()
        pile = c.execute(text(
            f"SELECT COUNT(*) FROM {TABLE} WHERE CAST(lot_no AS TEXT) = :lot"), {"lot": LOT}).scalar()
        print(f"Pile lot {LOT}: {pile} rows in {TABLE}; {len(rows)} of them are ALSO In Transit "
              f"under transfer {TRANSFER_OUT_ID}")
        if not rows:
            print("Nothing to do — no double-counted rows (already repaired?).")
            return 0
        for r in rows:
            m = r._mapping
            print(f"  id={m['id']} box_id={m['box_id']} txn={m['transaction_no']} "
                  f"item={m['item_description']!r} cartons={m['no_of_cartons']} wt={m['weight_kg']}")

        if len(rows) != EXPECTED_BOXES:
            print(f"ABORT: expected {EXPECTED_BOXES} double-counted rows, found {len(rows)} — "
                  f"re-diagnose with diag_cold_pile_selector.py before applying.")
            return 1
        if any((r._mapping["item_description"] or "") != ITEM for r in rows):
            print(f"ABORT: a row is not {ITEM!r} — data changed. Re-diagnose.")
            return 1

        backup = f"fix_lot130273_backup_{datetime.now():%Y%m%d_%H%M%S}.json"
        payload = [{k: (str(v) if not isinstance(v, (int, float, type(None), str)) else v)
                    for k, v in r._mapping.items()} for r in rows]

        if args.apply:
            with open(backup, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
            ids = [r._mapping["id"] for r in rows]
            n = c.execute(text(f"DELETE FROM {TABLE} WHERE id = ANY(:ids)"), {"ids": ids}).rowcount
            left = c.execute(text(
                f"SELECT COUNT(*) FROM {TABLE} WHERE CAST(lot_no AS TEXT) = :lot"), {"lot": LOT}).scalar()
            print(f"\nDeleted {n} rows (backup: {backup}). Pile lot {LOT} now {left} rows "
                  f"+ {EXPECTED_BOXES} in transit = {left + EXPECTED_BOXES} physical.")
            print("[OK] COMMITTED.")
        else:
            print(f"\nPlanned: DELETE {len(rows)} rows -> pile drops to {pile - len(rows)} "
                  f"+ {EXPECTED_BOXES} in transit = {pile - len(rows) + EXPECTED_BOXES} physical.")
            print("(dry-run — no changes written. Re-run with --apply to commit.)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
