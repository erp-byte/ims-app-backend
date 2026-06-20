"""READ-ONLY: trace lot 10470 (Medjoul, txn TR-20260314174556) — we have 27 in
cdpl_cold_stocks (box 21..47), user expects 47. Find the other 20 (box 1..20)."""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)

TXN = "TR-20260314174556"
P = {"t": TXN}

def run(t, sql, p=None):
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
    for r in rows:
        print("  " + " | ".join(str(r._mapping[x]) for x in k))

# A. The 27 we have now — what box numbers exactly?
run("A. cdpl_cold_stocks lot 10470 — count + box_id range", """
    SELECT COUNT(*) boxes, MIN(box_id) min_box, MAX(box_id) max_box,
           ROUND(SUM(weight_kg)::numeric,2) kg
    FROM cdpl_cold_stocks WHERE TRIM(lot_no)='10470'
""")
run("A2. cdpl_cold_stocks lot 10470 — list box_ids", """
    SELECT box_id, transaction_no, no_of_cartons, weight_kg
    FROM cdpl_cold_stocks WHERE TRIM(lot_no)='10470' ORDER BY box_id
""")

# B. Lot 10470 anywhere ELSE (other company, other txn)?
run("B. cfpl_cold_stocks lot 10470?", """
    SELECT transaction_no, COUNT(*) boxes, MIN(box_id), MAX(box_id)
    FROM cfpl_cold_stocks WHERE TRIM(lot_no)='10470' GROUP BY transaction_no
""")

# C. Disposition history for lot 10470 (real exits vs phantom-reverted)
run("C. cold_stock_disposition lot 10470 — by reverted flag", """
    SELECT reverted, COUNT(*) rows, MIN(box_id) min_box, MAX(box_id) max_box,
           MIN(id) min_id, MAX(id) max_id
    FROM cold_stock_disposition WHERE TRIM(lot_no)='10470'
    GROUP BY reverted ORDER BY reverted
""")
run("C2. disposition lot 10470 — distinct disposition_type / status", """
    SELECT * FROM (
      SELECT box_id, transaction_no, lot_no, reverted, id
      FROM cold_stock_disposition WHERE TRIM(lot_no)='10470' ORDER BY id
    ) q
""")

# D. WHERE ARE BOXES 1..20 of base 90556000? search every plausible table.
BX = {"bx": [f"90556000-{i}" for i in range(1, 21)]}
print(f"\n[searching for box_ids: 90556000-1 .. 90556000-20]")

run("D1. cdpl_cold_stocks — boxes 1..20 (any lot)", """
    SELECT box_id, lot_no, item_description, weight_kg, storage_location
    FROM cdpl_cold_stocks WHERE box_id = ANY(:bx) ORDER BY box_id
""", BX)
run("D2. cfpl_cold_stocks — boxes 1..20 (any lot)", """
    SELECT box_id, lot_no, item_description, weight_kg
    FROM cfpl_cold_stocks WHERE box_id = ANY(:bx) ORDER BY box_id
""", BX)
run("D3. cold_stock_disposition — boxes 1..20", """
    SELECT id, box_id, transaction_no, lot_no, reverted
    FROM cold_stock_disposition WHERE box_id = ANY(:bx) ORDER BY box_id
""", BX)
run("D4. pending_transfer_stock — boxes 1..20", """
    SELECT box_id, transaction_no, lot_no FROM pending_transfer_stock
    WHERE box_id = ANY(:bx) ORDER BY box_id
""", BX)
run("D5. interunit_transfer_in_boxes — boxes 1..20", """
    SELECT box_id, transaction_no, lot_no FROM interunit_transfer_in_boxes
    WHERE box_id = ANY(:bx) ORDER BY box_id
""", BX)
run("D6. cold_transfer_inboxes — boxes 1..20", """
    SELECT box_id, transaction_no, lot_no FROM cold_transfer_inboxes
    WHERE box_id = ANY(:bx) ORDER BY box_id
""", BX)
run("D7. cdpl_boxes_v2 — boxes 1..20", """
    SELECT box_id, lot_number, article, current_status FROM cdpl_boxes_v2
    WHERE box_id = ANY(:bx) ORDER BY box_id
""", BX)
run("D8. cfpl_boxes_v2 — boxes 1..20", """
    SELECT box_id, lot_number, article, current_status FROM cfpl_boxes_v2
    WHERE box_id = ANY(:bx) ORDER BY box_id
""", BX)

# E. Same txn — full box_id census across the base, all lots (what shares 90556000)
run("E. txn TR-...174556 in cdpl_cold_stocks — box census by lot", """
    SELECT lot_no, item_description, COUNT(*) boxes, MIN(box_id) min_box, MAX(box_id) max_box
    FROM cdpl_cold_stocks WHERE transaction_no=:t GROUP BY lot_no, item_description ORDER BY min_box
""", P)
print("\nDone.")
