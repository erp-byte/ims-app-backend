"""READ-ONLY: full dump of the cold_stock_disposition rows for lot 10470 (ids 4498-4524)
to learn HOW the system knew these 27 were Medjoul, and whether the origin (a transfer-out,
a reference txn, a batch) points at 20 more Medjoul boxes."""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text, inspect

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)
insp = inspect(e)

print("== cold_stock_disposition columns ==")
print("  ", [c["name"] for c in insp.get_columns("cold_stock_disposition")])

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
    for r in rows:
        print("  " + "  ".join(f"{k}={r._mapping[k]}" for k in r._mapping.keys()))

# 1. one full disposition row for lot 10470 (all columns)
run("1. full disposition row id=4498 (the first Medjoul phantom)", """
    SELECT * FROM cold_stock_disposition WHERE id = 4498
""")

# 2. distinct origin/type/reference values across the 27
run("2. distinct disposition_type / reason / reference across ids 4498..4524", """
    SELECT DISTINCT disposition_type, disposition_reference, disposition_reason, created_at::date
    FROM cold_stock_disposition WHERE id BETWEEN 4498 AND 4524
""")

# 3. ANY other disposition rows sharing that same reference / created_at (would reveal sibling Medjoul boxes)
run("3. ALL dispositions sharing the same disposition_reference as the 10470 block", """
    SELECT box_id, transaction_no, lot_no, item_description, reverted, id
    FROM cold_stock_disposition
    WHERE disposition_reference IN (
        SELECT DISTINCT disposition_reference FROM cold_stock_disposition WHERE id BETWEEN 4498 AND 4524
    )
    ORDER BY id
""")

# 4. dispositions created in the same instant as the block (origin event)
run("4. dispositions with same created_at as id=4498", """
    SELECT box_id, transaction_no, lot_no, item_description, reverted, id, disposition_type
    FROM cold_stock_disposition
    WHERE created_at = (SELECT created_at FROM cold_stock_disposition WHERE id=4498)
    ORDER BY id
""")
print("\nDone.")
