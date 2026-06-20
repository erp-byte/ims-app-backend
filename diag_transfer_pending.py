"""
READ-ONLY diagnostic: why did a Transfer-Out not land in pending_transfer_stock?

Usage: set DIAG_CHALLAN env var (default TRANS202605281739).
SELECT-only, runs inside a read-only transaction.
"""
import os
from sqlalchemy import create_engine, text

raw = os.environ["DATABASE_URL"]
DB_URL = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
CHALLAN = os.environ.get("DIAG_CHALLAN", "TRANS202605281739")
engine = create_engine(DB_URL)


def run(title, sql, params=None):
    print(f"\n===== {title} =====")
    with engine.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        rows = c.execute(text(sql), params or {}).fetchall()
        if not rows:
            print("  (no rows)")
            return []
        cols = list(rows[0]._mapping.keys())
        print("  " + " | ".join(cols))
        for r in rows:
            print("  " + " | ".join(str(r._mapping[k]) for k in cols))
        return rows


hdr = run("HEADER", """
    SELECT id, challan_no, status, from_site, to_site, created_by, created_ts, has_variance
    FROM interunit_transfers_header WHERE challan_no = :ch
""", {"ch": CHALLAN})

if not hdr:
    print("\n!! No header for that challan — nothing to debug.")
    raise SystemExit

hid = hdr[0]._mapping["id"]

run("LINES", """
    SELECT id, rm_pm_fg_type, item_desc_raw, lot_number, qty, pack_size, net_weight, total_weight
    FROM interunit_transfers_lines WHERE header_id = :hid ORDER BY id
""", {"hid": hid})

boxes = run("BOXES (interunit_transfer_boxes)", """
    SELECT id, box_number, box_id, transaction_no, article, lot_number, net_weight
    FROM interunit_transfer_boxes WHERE header_id = :hid ORDER BY id
""", {"hid": hid})

run("PENDING rows for this transfer", """
    SELECT id, box_id, transaction_no, status, no_of_cartons, weight_kg, from_company, from_site, to_site
    FROM pending_transfer_stock WHERE transfer_out_id = :hid ORDER BY id
""", {"hid": hid})

# Why park may have skipped each box: empty id/tno, DIRECT, or no source-table match.
run("BOX skip-reason analysis", """
    SELECT b.box_id, b.transaction_no,
           CASE
             WHEN COALESCE(b.box_id,'')='' OR COALESCE(b.transaction_no,'')='' THEN 'SKIP: empty box_id/transaction_no'
             WHEN b.transaction_no = 'DIRECT' THEN 'SKIP: transaction_no=DIRECT'
             ELSE 'parkable (needs source-table match)'
           END AS verdict
    FROM interunit_transfer_boxes b WHERE b.header_id = :hid ORDER BY b.id
""", {"hid": hid})

# Do the box_ids still exist in cold_stocks (if cold source)? If present => not yet deducted
for tbl in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
    run(f"box_ids still present in {tbl}", f"""
        SELECT cs.box_id, cs.transaction_no, cs.lot_no, cs.no_of_cartons, cs.weight_kg
        FROM {tbl} cs
        JOIN interunit_transfer_boxes b
          ON b.box_id = cs.box_id AND b.transaction_no = cs.transaction_no
        WHERE b.header_id = :hid
    """, {"hid": hid})

print(f"\nSUMMARY: header={hid} boxes_in_transfer={len(boxes)}  (0 boxes => park_in_pending never ran)")
print("Done (read-only).")
