import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)
BOX = "37503710-1"

def run(t, sql, p=None):
    print(f"\n== {t} ==")
    with e.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        rows = c.execute(text(sql), p or {}).fetchall()
        if not rows:
            print("  (none)"); return
        for r in rows:
            print("  " + " | ".join(f"{k}={r._mapping[k]}" for k in r._mapping.keys()))

run("header 483 + its transfer_out", """
    SELECT ti.id in_hdr, ti.transfer_out_id, ti.status in_status,
           h.challan_no, h.from_site, h.to_site, h.status out_status
    FROM interunit_transfer_in_header ti JOIN interunit_transfers_header h ON h.id=ti.transfer_out_id
    WHERE ti.id=483
""")
run("OUT box records for this box (interunit_transfer_boxes)", """
    SELECT header_id, transaction_no, lot_number, article, net_weight
    FROM interunit_transfer_boxes WHERE box_id=:b ORDER BY header_id
""", {"b": BOX})
run("box in cfpl_boxes_v2", """
    SELECT box_id, transaction_no, lot_number, article_description, net_weight
    FROM cfpl_boxes_v2 WHERE box_id=:b
""", {"b": BOX})
run("cfpl_transactions_v2 for that txn (where inwarded)", """
    SELECT transaction_no, warehouse, supplier_name, created_at
    FROM cfpl_transactions_v2 WHERE transaction_no='TR-20260415124824'
""")
run("box in cdpl_boxes_v2", """
    SELECT transaction_no, lot_number, article_description FROM cdpl_boxes_v2 WHERE box_id=:b
""", {"b": BOX})
run("box in cold tables", """
    SELECT 'cfpl' co, transaction_no, lot_no, item_description FROM cfpl_cold_stocks WHERE box_id=:b
    UNION ALL SELECT 'cdpl', transaction_no, lot_no, item_description FROM cdpl_cold_stocks WHERE box_id=:b
""", {"b": BOX})
run("box in pending", """
    SELECT transfer_out_id, transaction_no, lot_no, status, source_table, destination_table
    FROM pending_transfer_stock WHERE box_id=:b
""", {"b": BOX})
run("box in transfer_in_boxes (received)", """
    SELECT header_id, transaction_no, lot_number, article FROM interunit_transfer_in_boxes WHERE box_id=:b
""", {"b": BOX})
run("transfer 908 from/to + box count", """
    SELECT from_site, to_site, status,
           (SELECT COUNT(*) FROM interunit_transfer_boxes b WHERE b.header_id=908) out_boxes
    FROM interunit_transfers_header WHERE id=908
""")
print("\nDone.")
