"""READ-ONLY: does transfer-in actually write to destination tables (cold_stocks / boxes_v2)?"""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)

def run(t, sql, p=None, limit=40):
    print(f"\n== {t} ==")
    with e.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        rows = c.execute(text(sql), p or {}).fetchall()
        if not rows:
            print("  (none)"); return
        cols = rows[0]._mapping.keys()
        print("  " + " | ".join(cols))
        for r in rows[:limit]:
            print("  " + " | ".join(str(r._mapping[k]) for k in cols))

# destination_table distribution among current in-transit rows
run("destination_table for In-Transit rows", """
    SELECT destination_table, to_storage_type, COUNT(*) n
    FROM pending_transfer_stock WHERE status='In Transit'
    GROUP BY destination_table, to_storage_type ORDER BY n DESC
""")

# Pick a RECEIVED cold->warehouse transfer and see if its boxes are in boxes_v2
run("a fully-received warehouse-dest transfer (header)", """
    SELECT h.id, h.challan_no, h.from_site, h.to_site, h.status
    FROM interunit_transfers_header h
    WHERE h.status='Received'
      AND LOWER(h.to_site) NOT SIMILAR TO '%(cold|rishi|savla|supreme)%'
    ORDER BY h.id DESC LIMIT 5
""")

# For received warehouse-dest transfers, are their IN box_ids present in boxes_v2?
run("received warehouse-dest IN boxes vs boxes_v2 presence", """
    WITH wh AS (
      SELECT h.id FROM interunit_transfers_header h
      WHERE h.status='Received'
        AND LOWER(h.to_site) NOT SIMILAR TO '%(cold|rishi|savla|supreme)%'
      ORDER BY h.id DESC LIMIT 50)
    SELECT
      COUNT(*) AS in_boxes,
      COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM cfpl_boxes_v2 v WHERE v.box_id=ib.box_id AND v.transaction_no=ib.transaction_no)
                          OR EXISTS (SELECT 1 FROM cdpl_boxes_v2 v WHERE v.box_id=ib.box_id AND v.transaction_no=ib.transaction_no)) AS in_boxes_v2,
      COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM cfpl_cold_stocks s WHERE s.box_id=ib.box_id AND s.transaction_no=ib.transaction_no)
                          OR EXISTS (SELECT 1 FROM cdpl_cold_stocks s WHERE s.box_id=ib.box_id AND s.transaction_no=ib.transaction_no)) AS in_cold
    FROM interunit_transfer_in_boxes ib
    JOIN interunit_transfer_in_header ti ON ti.id=ib.header_id
    WHERE ti.transfer_out_id IN (SELECT id FROM wh)
""")

# For received cold->cold transfers, are their IN box_ids present in cold_stocks?
run("received cold-dest IN boxes vs cold_stocks presence", """
    WITH cd AS (
      SELECT h.id FROM interunit_transfers_header h
      WHERE h.status='Received'
        AND LOWER(h.to_site) SIMILAR TO '%(cold|rishi|savla|supreme)%'
      ORDER BY h.id DESC LIMIT 50)
    SELECT
      COUNT(*) AS in_boxes,
      COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM cfpl_cold_stocks s WHERE s.box_id=ib.box_id AND s.transaction_no=ib.transaction_no)
                          OR EXISTS (SELECT 1 FROM cdpl_cold_stocks s WHERE s.box_id=ib.box_id AND s.transaction_no=ib.transaction_no)) AS in_cold
    FROM interunit_transfer_in_boxes ib
    JOIN interunit_transfer_in_header ti ON ti.id=ib.header_id
    WHERE ti.transfer_out_id IN (SELECT id FROM cd)
""")
print("\nDone.")
