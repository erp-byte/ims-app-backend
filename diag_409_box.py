import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)
BOX = "90589000-15"

def run(t, sql, p=None):
    print(f"\n== {t} ==")
    with e.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        rows = c.execute(text(sql), p or {}).fetchall()
        if not rows:
            print("  (none)"); return
        cols = rows[0]._mapping.keys()
        print("  " + " | ".join(cols))
        for r in rows:
            print("  " + " | ".join(str(r._mapping[k]) for k in cols))

# header 483 -> which transfer_out, and transfer 494 details
run("transfer-in header 483 + its transfer_out + transfer 494", """
    SELECT ti.id in_header, ti.transfer_out_id, ti.status in_status,
           h.challan_no, h.from_site, h.to_site, h.status out_status
    FROM interunit_transfer_in_header ti
    JOIN interunit_transfers_header h ON h.id = ti.transfer_out_id
    WHERE ti.id = 483
    UNION ALL
    SELECT NULL, 494, NULL, h.challan_no, h.from_site, h.to_site, h.status
    FROM interunit_transfers_header h WHERE h.id = 494
""")

# all pending rows for this box_id
run("pending_transfer_stock rows for box (all)", """
    SELECT transfer_out_id, transfer_out_challan_no, transaction_no, lot_no,
           item_description, status, from_site, to_site
    FROM pending_transfer_stock WHERE box_id = :b ORDER BY transfer_out_id
""", {"b": BOX})

# OUT box records for this box_id — which transfers/txns/lots
run("interunit_transfer_boxes (OUT) for box", """
    SELECT header_id, transaction_no, lot_number, article, COUNT(*) rows
    FROM interunit_transfer_boxes WHERE box_id = :b
    GROUP BY header_id, transaction_no, lot_number, article ORDER BY header_id
""", {"b": BOX})

# already received anywhere?
run("interunit_transfer_in_boxes for box", """
    SELECT header_id, transaction_no, lot_number, article, is_matched
    FROM interunit_transfer_in_boxes WHERE box_id = :b
""", {"b": BOX})

# in source tables?
for tbl in ("cfpl_cold_stocks","cdpl_cold_stocks","cfpl_boxes_v2","cdpl_boxes_v2"):
    lotcol = "lot_no" if tbl.endswith("cold_stocks") else "lot_number"
    itemcol = "item_description" if tbl.endswith("cold_stocks") else "article_description"
    run(f"{tbl} for box", f"""
        SELECT transaction_no, {lotcol} lot, {itemcol} item FROM {tbl} WHERE box_id = :b
    """, {"b": BOX})
print("\nDone.")
