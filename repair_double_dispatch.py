"""Cleanup double-dispatched boxes.

A box's RIGHTFUL OWNER is the transfer that actually received it (transfer_in_boxes)
or currently holds it In-Transit (pending_transfer_stock). Any OTHER transfer that
lists the same (box_id, transaction_no) in interunit_transfer_boxes is a SPURIOUS
listing (it never really took the box — park skipped it). This removes those
spurious OUT box rows so the non-owning transfer shows only its real boxes and
stops 409-ing on receive. The owner keeps the box. No pending/source/cold rows
are touched.

DRY-RUN by default; pass --apply to delete the spurious OUT rows.
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

# Spurious OUT box rows: box is owned (received or in-transit) by a DIFFERENT transfer.
SPURIOUS = text("""
    SELECT ob.id, ob.header_id AS listed_in, ob.box_id, ob.transaction_no,
           ob.lot_number, ob.article, owner.owner_transfer
    FROM interunit_transfer_boxes ob
    JOIN LATERAL (
        SELECT COALESCE(
          (SELECT ti.transfer_out_id FROM interunit_transfer_in_boxes ib
             JOIN interunit_transfer_in_header ti ON ti.id = ib.header_id
             WHERE ib.box_id = ob.box_id AND ib.transaction_no = ob.transaction_no
               AND TRIM(COALESCE(ib.lot_number,'')) = TRIM(COALESCE(ob.lot_number,''))
             LIMIT 1),
          (SELECT p.transfer_out_id FROM pending_transfer_stock p
             WHERE p.box_id = ob.box_id AND p.transaction_no = ob.transaction_no
               AND TRIM(COALESCE(p.lot_no,'')) = TRIM(COALESCE(ob.lot_number,''))
               AND p.status = 'In Transit' LIMIT 1)
        ) AS owner_transfer
    ) owner ON TRUE
    WHERE ob.box_id IS NOT NULL AND ob.box_id <> ''
      AND owner.owner_transfer IS NOT NULL
      AND owner.owner_transfer <> ob.header_id
    ORDER BY ob.header_id, ob.box_id
""")

with eng.begin() as c:
    rows = c.execute(SPURIOUS).fetchall()
    print(f"Spurious OUT box rows (box owned by another transfer): {len(rows)}  APPLY={APPLY}\n")

    by_hdr = {}
    for r in rows:
        m = r._mapping
        by_hdr.setdefault(m["listed_in"], []).append(m)
    for hdr, items in sorted(by_hdr.items(), key=lambda kv: -len(kv[1])):
        owners = sorted({str(i["owner_transfer"]) for i in items})
        print(f"  transfer {hdr}: remove {len(items)} box(es) -> owned by transfer(s) {', '.join(owners)}")
        for i in items[:4]:
            print(f"       {i['box_id']} / {i['transaction_no']} ({i['article']}) -> owner {i['owner_transfer']}")
        if len(items) > 4:
            print(f"       ... +{len(items)-4} more")

    print(f"\nTOTAL spurious rows: {len(rows)} across {len(by_hdr)} transfers.")
    if APPLY and rows:
        ids = [r._mapping["id"] for r in rows]
        deleted = c.execute(
            text("DELETE FROM interunit_transfer_boxes WHERE id = ANY(:ids)"), {"ids": ids}
        ).rowcount
        print(f"DELETED {deleted} spurious OUT box rows.")
    elif not APPLY:
        print("DRY-RUN only — no writes. Re-run with --apply to commit.")
print("Done.")
