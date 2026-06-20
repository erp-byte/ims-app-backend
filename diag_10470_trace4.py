"""READ-ONLY: map base 90556000 across sibling txns ...556 / ...557; full census of
...557; rates/values to compare Medjoul vs Safavi; disposition ids around 4498-4524."""
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
    for r in rows[:80]:
        print("  " + " | ".join(str(r._mapping[x]) for x in k))

# 1. base 90556000 boxes 1..99 across BOTH txns — number -> (txn, lot)
run("1. base 90556000 boxes 1..99 — txn + lot per number (both sibling txns)", """
    SELECT (split_part(box_id,'-',2))::int n, transaction_no, lot_no, item_description,
           weight_kg, last_purchase_rate, value
    FROM cdpl_cold_stocks
    WHERE box_id LIKE '90556000-%' AND (split_part(box_id,'-',2)) ~ '^[0-9]+$'
      AND (split_part(box_id,'-',2))::int BETWEEN 1 AND 99
    ORDER BY n, transaction_no
""")

# 2. full census of txn ...557
run("2. txn TR-...557 — census by lot", """
    SELECT lot_no, item_description, COUNT(*) boxes, MIN(box_id) min_b, MAX(box_id) max_b,
           ROUND(SUM(weight_kg)::numeric,2) kg
    FROM cdpl_cold_stocks WHERE transaction_no='TR-20260314174557'
    GROUP BY lot_no, item_description ORDER BY boxes DESC
""")

# 3. census of txn ...556
run("3. txn TR-...556 — census by lot", """
    SELECT lot_no, item_description, COUNT(*) boxes, MIN(box_id) min_b, MAX(box_id) max_b,
           ROUND(SUM(weight_kg)::numeric,2) kg
    FROM cdpl_cold_stocks WHERE transaction_no='TR-20260314174556'
    GROUP BY lot_no, item_description ORDER BY boxes DESC
""")

# 4. rate/value: Medjoul (10470) vs the Safavi boxes 1..20 of ...557 — do they differ?
run("4. rate/value compare — Medjoul 10470 vs Safavi 12324 (1..20 of ...557)", """
    SELECT 'medjoul 10470' grp, COUNT(*) n, MIN(last_purchase_rate) rate, AVG(value)::numeric(12,2) avg_val,
           MIN(item_mark) mark, MIN(exporter) exp
    FROM cdpl_cold_stocks WHERE TRIM(lot_no)='10470'
    UNION ALL
    SELECT 'safavi 1..20 (557)', COUNT(*), MIN(last_purchase_rate), AVG(value)::numeric(12,2),
           MIN(item_mark), MIN(exporter)
    FROM cdpl_cold_stocks WHERE transaction_no='TR-20260314174557'
      AND box_id = ANY(:bx)
""", {"bx": [f"90556000-{i}" for i in range(1, 21)]})

# 5. disposition ids around the phantom block (4471..4540) — any Medjoul-ish neighbors?
run("5. disposition ids 4471..4540 — neighbors of the 10470 phantom block", """
    SELECT id, box_id, transaction_no, lot_no, item_description, reverted
    FROM cold_stock_disposition WHERE id BETWEEN 4471 AND 4540 ORDER BY id
""")
print("\nDone.")
