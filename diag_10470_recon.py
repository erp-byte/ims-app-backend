"""READ-ONLY: transfer-out 924 / TRANS202606041324 — does it enumerate MORE than 27
Medjoul boxes? Inspect transfer_box_reconciliation (164 rows), disposition-by-ref (109),
and the IN-side boxes for the linked header."""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text, inspect

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)
insp = inspect(e)

def run(t, sql, p=None, lim=80):
    print(f"\n== {t} ==")
    try:
        with e.connect() as c:
            c.execute(text("SET TRANSACTION READ ONLY"))
            rows = c.execute(text(sql), p or {}).fetchall()
    except Exception as ex:
        print(f"  ERR: {str(ex).splitlines()[0]}"); return
    if not rows:
        print("  (none)"); return
    k = rows[0]._mapping.keys()
    print("  " + " | ".join(k))
    for r in rows[:lim]:
        print("  " + " | ".join(str(r._mapping[x]) for x in k))
    if len(rows) > lim: print(f"  ... (+{len(rows)-lim} more)")

print("== transfer_box_reconciliation columns ==")
print("  ", [c["name"] for c in insp.get_columns("transfer_box_reconciliation")])

# 1. reconciliation census by lot/article for transfer_out_id=924
run("1. transfer_box_reconciliation tid=924 — census (by every text-ish col combo)", """
    SELECT * FROM transfer_box_reconciliation WHERE transfer_out_id = 924 LIMIT 3
""")

# 2. disposition for the ref — census by lot (how many Medjoul total under this OUT)
run("2. cold_stock_disposition ref=TRANS202606041324 — census by lot", """
    SELECT lot_no, item_description, COUNT(*) boxes, MIN(box_id) min_b, MAX(box_id) max_b,
           bool_and(reverted) all_reverted
    FROM cold_stock_disposition WHERE disposition_ref_no='TRANS202606041324'
    GROUP BY lot_no, item_description ORDER BY boxes DESC
""")

# 3. IN-side: header linked to transfer_out_id=924, then its boxes
run("3. interunit_transfer_in_header for tid=924", """
    SELECT id, transfer_out_no, grn_number, status, inward_transaction_no, receiving_warehouse
    FROM interunit_transfer_in_header WHERE transfer_out_id=924
""")
run("3b. interunit_transfer_in_boxes for that header — Medjoul census", """
    SELECT article, lot_number, COUNT(*) boxes, MIN(box_id) min_b, MAX(box_id) max_b
    FROM interunit_transfer_in_boxes
    WHERE header_id IN (SELECT id FROM interunit_transfer_in_header WHERE transfer_out_id=924)
    GROUP BY article, lot_number ORDER BY boxes DESC
""")
print("\nDone.")
