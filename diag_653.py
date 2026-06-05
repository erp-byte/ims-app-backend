import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)

def run(t, sql, p=None):
    print(f"\n== {t} ==")
    with e.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        for r in c.execute(text(sql), p or {}).fetchall():
            print("  " + " | ".join(f"{k}={r._mapping[k]}" for k in r._mapping.keys()))

run("653 OUT boxes: total / distinct / null-ish", """
    SELECT COUNT(*) total,
           COUNT(DISTINCT (box_id, transaction_no)) distinct_keys,
           COUNT(*) FILTER (WHERE box_id IS NULL OR box_id='') null_box,
           COUNT(*) FILTER (WHERE transaction_no IS NULL OR transaction_no='') null_txn
    FROM interunit_transfer_boxes WHERE header_id=653
""")
run("653 OUT distinct transaction_no", """
    SELECT transaction_no, COUNT(*) n FROM interunit_transfer_boxes WHERE header_id=653
    GROUP BY transaction_no ORDER BY n DESC LIMIT 10
""")
run("653 OUT boxes: where are their (box_id,txn) now?", """
    WITH ob AS (SELECT DISTINCT box_id, transaction_no FROM interunit_transfer_boxes WHERE header_id=653)
    SELECT
      COUNT(*) out_keys,
      COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM interunit_transfer_in_boxes ib
              WHERE ib.box_id=ob.box_id AND ib.transaction_no=ob.transaction_no)) in_any_transferin,
      COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM pending_transfer_stock p
              WHERE p.box_id=ob.box_id AND p.transaction_no=ob.transaction_no AND p.status='In Transit')) in_pending,
      COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM cfpl_cold_stocks s WHERE s.box_id=ob.box_id AND s.transaction_no=ob.transaction_no)
                          OR EXISTS (SELECT 1 FROM cdpl_cold_stocks s WHERE s.box_id=ob.box_id AND s.transaction_no=ob.transaction_no)) still_in_cold
    FROM ob
""")
run("653 IN boxes (this transfer) — which header & how many", """
    SELECT ti.id in_header, ti.status, COUNT(b.id) boxes
    FROM interunit_transfer_in_header ti
    LEFT JOIN interunit_transfer_in_boxes b ON b.header_id=ti.id
    WHERE ti.transfer_out_id=653 GROUP BY ti.id, ti.status
""")
run("653 OUT box_ids found in transfer_in_boxes under WHICH headers", """
    WITH ob AS (SELECT DISTINCT box_id, transaction_no FROM interunit_transfer_boxes WHERE header_id=653)
    SELECT ib.header_id, ti.transfer_out_id, COUNT(*) n
    FROM interunit_transfer_in_boxes ib
    JOIN ob ON ob.box_id=ib.box_id AND ob.transaction_no=ib.transaction_no
    LEFT JOIN interunit_transfer_in_header ti ON ti.id=ib.header_id
    GROUP BY ib.header_id, ti.transfer_out_id ORDER BY n DESC LIMIT 10
""")
print("\nDone.")
