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

# 1. The 27 target rows as they are NOW (mislabeled 12324) — sample + the columns I'd change
run("TARGET cdpl_cold_stocks rows (box 21-47) — current values (sample 4)", """
    SELECT box_id, lot_no, item_description, item_mark, group_name, item_subgroup,
           exporter, vakkal, no_of_cartons, weight_kg, last_purchase_rate, value, unit, storage_location
    FROM cdpl_cold_stocks
    WHERE transaction_no = :t AND box_id BETWEEN '90556000-21' AND '90556000-47'
    ORDER BY box_id LIMIT 4
""", P)

run("TARGET count + weight (box 21-47, currently lot 12324)", """
    SELECT lot_no, item_description, COUNT(*) boxes, ROUND(SUM(weight_kg)::numeric,2) kg,
           ROUND(SUM(value)::numeric,2) value
    FROM cdpl_cold_stocks
    WHERE transaction_no = :t AND box_id BETWEEN '90556000-21' AND '90556000-47'
    GROUP BY lot_no, item_description
""", P)

# 2. What the snapshot says they SHOULD be (Medjoul lot 10470) — the restore values
run("disposition snapshot — Medjoul values to restore (sample)", """
    SELECT box_id, lot_no, item_description,
           snapshot_data->>'item_mark' item_mark,
           snapshot_data->>'group_name' group_name,
           snapshot_data->>'item_subgroup' item_subgroup,
           snapshot_data->>'exporter' exporter,
           snapshot_data->>'weight_kg' weight_kg,
           snapshot_data->>'last_purchase_rate' rate,
           snapshot_data->>'value' value
    FROM cold_stock_disposition
    WHERE transaction_no = :t AND TRIM(lot_no)='10470'
    ORDER BY box_id LIMIT 4
""", P)

# 3. The disposition rows to mark reverted (the phantom deduction)
run("disposition rows for lot 10470 (to mark reverted) — id range + count", """
    SELECT MIN(id) min_id, MAX(id) max_id, COUNT(*) rows, bool_or(reverted) already_reverted
    FROM cold_stock_disposition WHERE transaction_no = :t AND TRIM(lot_no)='10470'
""", P)

# 4. Safety: confirm lot 12324's "real" boxes are the 100+ range, and 21-47 are the only strays
run("lot 12324 box-range split (confirm 21-47 are the strays)", """
    SELECT CASE WHEN box_id BETWEEN '90556000-21' AND '90556000-47' THEN 'strays_21_47'
                ELSE 'rest_of_12324' END AS bucket,
           COUNT(*) boxes, MIN(box_id) min_box, MAX(box_id) max_box
    FROM cdpl_cold_stocks WHERE transaction_no = :t AND TRIM(lot_no)='12324'
    GROUP BY 1
""", P)

# 5. Safety: make sure NONE of box 21-47 are currently in any transfer/pending (i.e. truly idle in cold)
run("box 21-47 referenced anywhere else? (pending / transfer-in)", """
    SELECT 'pending' src, COUNT(*) n FROM pending_transfer_stock
      WHERE transaction_no = :t AND box_id BETWEEN '90556000-21' AND '90556000-47'
    UNION ALL
    SELECT 'interunit_in_boxes', COUNT(*) FROM interunit_transfer_in_boxes
      WHERE transaction_no = :t AND box_id BETWEEN '90556000-21' AND '90556000-47'
    UNION ALL
    SELECT 'cold_transfer_inboxes', COUNT(*) FROM cold_transfer_inboxes
      WHERE transaction_no = :t AND box_id BETWEEN '90556000-21' AND '90556000-47'
""", P)
print("\nDone.")
