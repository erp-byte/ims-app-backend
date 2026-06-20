"""READ-ONLY: establish ground truth for lot 10470 box count. Is the Medjoul lot
really 47 boxes (1..47)? Trace base 90556000 census + source transfer + any snapshot."""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text, inspect

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)
insp = inspect(e)
TXN = "TR-20260314174556"

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

# 1. Full census of base-90556000 boxes 1..99 in this txn — which lot owns each number?
run("1. base 90556000 boxes 1..99 (this txn) — lot per box-number band", """
    WITH b AS (
      SELECT box_id, lot_no, item_description,
             (split_part(box_id,'-',2))::int AS n
      FROM cdpl_cold_stocks
      WHERE transaction_no = :t AND box_id LIKE '90556000-%'
        AND (split_part(box_id,'-',2)) ~ '^[0-9]+$'
    )
    SELECT lot_no, item_description, COUNT(*) boxes, MIN(n) min_n, MAX(n) max_n
    FROM b WHERE n BETWEEN 1 AND 99 GROUP BY lot_no, item_description ORDER BY min_n
""", {"t": TXN})

# 2. Which box-numbers 1..47 are present and what lot (gap check)
run("2. boxes 1..47 — number + lot (expect contiguous Medjoul if 47-box lot)", """
    SELECT (split_part(box_id,'-',2))::int AS n, lot_no, item_description
    FROM cdpl_cold_stocks
    WHERE transaction_no = :t AND box_id LIKE '90556000-%'
      AND (split_part(box_id,'-',2)) ~ '^[0-9]+$'
      AND (split_part(box_id,'-',2))::int BETWEEN 1 AND 47
    ORDER BY n
""", {"t": TXN})

# 3. snapshot_data for the already-repaired 21..47 — does it carry original box count / source?
run("3. disposition snapshot (box 21..47) sample — original source fields", """
    SELECT box_id,
           snapshot_data->>'transaction_no' src_txn,
           snapshot_data->>'lot_no' snap_lot,
           snapshot_data->>'item_description' snap_item,
           snapshot_data->>'no_of_cartons' cartons,
           snapshot_data->>'storage_location' loc
    FROM cold_stock_disposition
    WHERE TRIM(lot_no)='10470' ORDER BY box_id LIMIT 3
""")

# 4. Find the source transfer-out for lot 10470 / this txn — how many boxes shipped?
for tbl in ["transfer_out_boxes", "interunit_transfer_out_boxes", "cold_transfer_out_boxes",
            "transfer_out_articles", "interunit_transfer_out_articles"]:
    if insp.has_table(tbl):
        cols = [c["name"] for c in insp.get_columns(tbl)]
        lotcol = next((x for x in ("lot_no","lot_number","lot") if x in cols), None)
        txncol = next((x for x in ("transaction_no","transfer_out_id","txn","transaction_id") if x in cols), None)
        print(f"\n== 4. {tbl} cols ==\n  {cols}")
        if lotcol:
            run(f"4b. {tbl} where {lotcol}=10470", f"""
                SELECT * FROM {tbl} WHERE TRIM({lotcol})='10470' LIMIT 10
            """)

# 5. Source-document trace: any header/article rows for lot 10470 (count, weight)
for tbl in ["interunit_transfer_in_header", "cold_transfer_in_headers",
            "interunit_transfer_in_boxes", "transfer_out_header", "interunit_transfer_out_header"]:
    if insp.has_table(tbl):
        cols = [c["name"] for c in insp.get_columns(tbl)]
        print(f"\n== 5. {tbl} cols ==\n  {cols}")

print("\nDone.")
