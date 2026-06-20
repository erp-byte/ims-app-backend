"""READ-ONLY DECISIVE: find every box from original receipt GR6226 via disposition
snapshot_data->>'inward_no', regardless of current lot label. Also profile look-alike
lot 10570 'Medjoul-Large HM' (same mark/exporter) as a physical-miscount candidate."""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)

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

# 1. EVERY box ever recorded from receipt GR6226 (by snapshot inward_no) — current lot label too
run("1. all disposition boxes from inward GR6226 — by current lot_no", """
    SELECT lot_no, item_description, snapshot_data->>'item_mark' mark,
           COUNT(*) boxes, MIN(box_id) min_b, MAX(box_id) max_b
    FROM cold_stock_disposition
    WHERE snapshot_data->>'inward_no' = 'GR6226'
    GROUP BY lot_no, item_description, snapshot_data->>'item_mark' ORDER BY boxes DESC
""")
run("1b. raw box list from GR6226 (so we can see exact ids)", """
    SELECT box_id, transaction_no, lot_no, reverted, snapshot_data->>'item_mark' mark
    FROM cold_stock_disposition WHERE snapshot_data->>'inward_no'='GR6226' ORDER BY box_id
""")

# 2. any GR6226 anywhere in cold_stocks? (cold_stocks has no inward_no col, but check direct-out / bulk)
run("2. cdpl/cfpl bulk_entry transactions referencing GR6226", """
    SELECT 'cdpl' src, * FROM cdpl_bulk_entry_transactions WHERE CAST(grn_number AS TEXT) ILIKE '%6226%'
    UNION ALL SELECT 'cfpl', * FROM cfpl_bulk_entry_transactions WHERE CAST(grn_number AS TEXT) ILIKE '%6226%'
""")

# 3. look-alike lot 10570 Medjoul-Large HM — full footprint (could be the same physical stack)
run("3. lot 10570 'Medjoul-Large HM' footprint (cfpl+cdpl)", """
    SELECT 'cfpl' tbl, transaction_no, lot_no, item_mark, COUNT(*) boxes,
           MIN(box_id) min_b, MAX(box_id) max_b, ROUND(SUM(weight_kg)::numeric,2) kg
    FROM cfpl_cold_stocks WHERE TRIM(lot_no)='10570' GROUP BY transaction_no, lot_no, item_mark
    UNION ALL
    SELECT 'cdpl', transaction_no, lot_no, item_mark, COUNT(*),
           MIN(box_id), MAX(box_id), ROUND(SUM(weight_kg)::numeric,2)
    FROM cdpl_cold_stocks WHERE TRIM(lot_no)='10570' GROUP BY transaction_no, lot_no, item_mark
    ORDER BY boxes DESC
""")

# 4. does 10570 trace to GR6226 too? (same receipt -> same physical import)
run("4. 10570 disposition snapshot inward_no (same receipt as 10470?)", """
    SELECT DISTINCT snapshot_data->>'inward_no' inward_no, snapshot_data->>'inward_dt' dt,
           snapshot_data->>'last_purchase_rate' rate, COUNT(*) boxes
    FROM cold_stock_disposition WHERE TRIM(lot_no)='10570'
    GROUP BY 1,2,3
""")
print("\nDone.")
