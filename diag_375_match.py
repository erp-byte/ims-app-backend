import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)

def run(t, sql, p=None, limit=120):
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
        if len(rows) > limit:
            print(f"  ... (+{len(rows)-limit} more)")

# Columns actually present on interunit_transfer_in_boxes
run("transfer_in_boxes columns", """
    SELECT column_name FROM information_schema.columns
    WHERE table_name='interunit_transfer_in_boxes' ORDER BY ordinal_position
""")

# IN header(s) for transfer_out 375
run("IN headers for transfer_out 375", """
    SELECT id, grn_number, status, received_at FROM interunit_transfer_in_header WHERE transfer_out_id=375
""")

# IN boxes for header 152: do they carry line_index / transfer_out_box_id / box_number?
run("IN boxes (header 152) key matching fields — sample", """
    SELECT box_id, transaction_no, is_matched, transfer_out_box_id, line_index
    FROM interunit_transfer_in_boxes WHERE header_id=152
    ORDER BY box_id LIMIT 8
""")
run("IN boxes (header 152) null-field summary", """
    SELECT COUNT(*) total,
           COUNT(transfer_out_box_id) have_out_box_id,
           COUNT(line_index) have_line_index,
           COUNT(*) FILTER (WHERE is_matched) matched
    FROM interunit_transfer_in_boxes WHERE header_id=152
""")

# OUT boxes: box_number vs box_id suffix alignment, per txn
run("OUT boxes (375) box_number range per txn", """
    SELECT transaction_no, MIN(box_number) minbn, MAX(box_number) maxbn,
           COUNT(*) rows, COUNT(DISTINCT box_id) distinct_boxes
    FROM interunit_transfer_boxes WHERE header_id=375 GROUP BY transaction_no
""")

# Which OUT box_ids are NOT received (per own IN)
run("OUT boxes of 375 NOT in its IN (the genuinely unreceived)", """
    SELECT ob.transaction_no, ob.box_id, ob.box_number
    FROM (SELECT DISTINCT box_id, transaction_no, box_number FROM interunit_transfer_boxes WHERE header_id=375) ob
    WHERE NOT EXISTS (
      SELECT 1 FROM interunit_transfer_in_boxes ib
      WHERE ib.header_id=152 AND ib.box_id=ob.box_id AND ib.transaction_no=ob.transaction_no)
    ORDER BY ob.transaction_no, ob.box_number
""")
print("\nDone.")
