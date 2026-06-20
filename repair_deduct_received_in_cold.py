"""User Action 1: deduct (delete) cold_stocks rows for boxes that were RECEIVED
(present in interunit_transfer_in_boxes) but are still sitting in their source
cold table — only for WAREHOUSE-destination transfers (where the cold row is
definitely the leftover SOURCE, never a cold→cold destination).

Removes the double-count (box counted in cold AND received at warehouse).
DRY-RUN by default; pass --apply to write. Single transaction.
"""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
APPLY = "--apply" in sys.argv
eng = create_engine(DB)

# Cold rows whose box was received at a WAREHOUSE destination (cold row = leftover
# source). STRICT match: (box_id, transaction_no) is NOT unique (TR- timestamps +
# box_ids collide across batches), so we ALSO require item_description AND lot to
# match — otherwise we'd delete a different, legitimate cold box that merely shares
# the key. This guard prevents wrongly wiping ~634 unrelated cold rows.
SELECT_IDS = """
    SELECT s.id, s.box_id, s.transaction_no, s.item_description, s.lot_no, s.weight_kg
    FROM {tbl} s
    WHERE EXISTS (
        SELECT 1 FROM interunit_transfer_in_boxes ib
        JOIN interunit_transfer_in_header ti ON ti.id=ib.header_id
        JOIN interunit_transfers_header toh ON toh.id=ti.transfer_out_id
        WHERE ib.box_id=s.box_id AND ib.transaction_no=s.transaction_no
          AND LOWER(TRIM(COALESCE(ib.article,'')))=LOWER(TRIM(COALESCE(s.item_description,'')))
          AND COALESCE(ib.lot_number,'')=COALESCE(s.lot_no,'')
          AND LOWER(COALESCE(toh.to_site,'')) NOT SIMILAR TO '%(cold|rishi|savla|supreme)%'
    )
"""

with eng.begin() as c:
    grand_boxes = 0
    grand_kg = 0.0
    for tbl in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
        rows = c.execute(text(SELECT_IDS.format(tbl=tbl))).fetchall()
        kg = sum(float(r._mapping["weight_kg"] or 0) for r in rows)
        grand_boxes += len(rows); grand_kg += kg
        print(f"\n== {tbl}: {len(rows)} rows, {round(kg,1)} kg to deduct ==")
        for r in rows[:8]:
            m = r._mapping
            print(f"   id={m['id']} {m['box_id']} / {m['transaction_no']} | {m['item_description']} lot {m['lot_no']} | {m['weight_kg']}kg")
        if len(rows) > 8:
            print(f"   ... (+{len(rows)-8} more)")
        if APPLY and rows:
            ids = [r._mapping["id"] for r in rows]
            deleted = c.execute(
                text(f"DELETE FROM {tbl} WHERE id = ANY(:ids)"), {"ids": ids}
            ).rowcount
            print(f"   DELETED {deleted} rows from {tbl}.")

    print(f"\nTOTAL: {grand_boxes} cold rows, {round(grand_kg,1)} kg.  APPLY={APPLY}")
    if not APPLY:
        print("DRY-RUN only — no writes. Re-run with --apply to commit.")
print("Done.")
