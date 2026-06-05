"""Correct the over-repark: remove In-Transit pending rows whose box is STILL in
its source table (cold_stocks / boxes_v2). Those boxes were never actually
deducted at dispatch, so they are available source stock — NOT in transit.

For each affected transfer, after removing phantom rows, recompute completion:
if no real box remains In Transit, set Transfer-Out + Transfer-In back to 'Received'.

NO physical stock is touched (the source rows stay). Only the bogus in-transit
tracking rows are removed + statuses recomputed.

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

IN_SOURCE = """
   (EXISTS(SELECT 1 FROM cfpl_cold_stocks s WHERE s.box_id=p.box_id AND s.transaction_no=p.transaction_no)
 OR EXISTS(SELECT 1 FROM cdpl_cold_stocks s WHERE s.box_id=p.box_id AND s.transaction_no=p.transaction_no)
 OR EXISTS(SELECT 1 FROM cfpl_boxes_v2   s WHERE s.box_id=p.box_id AND s.transaction_no=p.transaction_no)
 OR EXISTS(SELECT 1 FROM cdpl_boxes_v2   s WHERE s.box_id=p.box_id AND s.transaction_no=p.transaction_no))
"""

FIND = text(f"""
    SELECT p.transfer_out_id, p.transfer_out_challan_no, COUNT(*) phantom_boxes
    FROM pending_transfer_stock p
    WHERE p.status='In Transit' AND p.box_id NOT LIKE 'LINE-%' AND {IN_SOURCE}
    GROUP BY p.transfer_out_id, p.transfer_out_challan_no
    ORDER BY phantom_boxes DESC
""")

DELETE = text(f"""
    DELETE FROM pending_transfer_stock p
    WHERE p.status='In Transit' AND p.box_id NOT LIKE 'LINE-%' AND {IN_SOURCE}
""")

REMAINING = text("""
    SELECT COUNT(*) FROM pending_transfer_stock
    WHERE transfer_out_id=:tid AND status='In Transit' AND box_id NOT LIKE 'LINE-%'
""")

with eng.begin() as c:
    rows = c.execute(FIND).fetchall()
    total_phantoms = sum(r._mapping["phantom_boxes"] for r in rows)
    print(f"Phantom In-Transit rows (box still in source): {total_phantoms} across {len(rows)} transfers. APPLY={APPLY}\n")
    for r in rows:
        print(f"  #{r._mapping['transfer_out_id']} {r._mapping['transfer_out_challan_no']}: "
              f"remove {r._mapping['phantom_boxes']} phantom rows")

    if APPLY:
        deleted = c.execute(DELETE).rowcount
        print(f"\nDeleted {deleted} phantom pending rows.")
        # Recompute status per affected transfer
        flipped = 0
        for r in rows:
            tid = r._mapping["transfer_out_id"]
            remaining = c.execute(REMAINING, {"tid": tid}).scalar() or 0
            # Only mark complete if (a) no real box left In Transit AND (b) there is
            # an actual receipt (transfer-in with >=1 box). Never fabricate 'Received'
            # for a transfer that received nothing (its boxes are all back in source).
            has_receipt = c.execute(text("""
                SELECT EXISTS(
                    SELECT 1 FROM interunit_transfer_in_header ti
                    JOIN interunit_transfer_in_boxes ib ON ib.header_id=ti.id
                    WHERE ti.transfer_out_id=:id)
            """), {"id": tid}).scalar()
            if remaining == 0 and has_receipt:
                c.execute(text("UPDATE interunit_transfers_header SET status='Received' "
                               "WHERE id=:id AND status<>'Received'"), {"id": tid})
                c.execute(text("UPDATE interunit_transfer_in_header SET status='Received' "
                               "WHERE transfer_out_id=:id AND status='Pending'"), {"id": tid})
                flipped += 1
        print(f"Recomputed status: {flipped} transfers set back to 'Received' "
              f"(0 real boxes left In Transit + had a receipt).")
    else:
        print("\nDRY-RUN only — no writes. Re-run with --apply to commit.")
print("Done.")
