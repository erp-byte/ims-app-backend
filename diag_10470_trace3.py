"""READ-ONLY: (a) are 90556000-1..20 a different-txn collision? (b) find the SOURCE
transfer-out for lot 10470 / txn TR-...174556 — how many Medjoul boxes shipped? (c)
search every table for lot 10470 to find a 47-box origin or a partial-receipt leak."""
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
        print(f"  ERR: {str(ex).splitlines()[0]}"); return []
    if not rows:
        print("  (none)"); return []
    k = rows[0]._mapping.keys()
    print("  " + " | ".join(k))
    for r in rows[:60]:
        print("  " + " | ".join(str(r._mapping[x]) for x in k))
    if len(rows) > 60: print(f"  ... (+{len(rows)-60} more)")
    return rows

# (a) collision check: what txn(s) own 90556000-1..20 and 48..73 ?
run("a1. 90556000-1..20 — which txn/lot (collision check)", """
    SELECT transaction_no, lot_no, item_description, COUNT(*) boxes,
           MIN(box_id) min_b, MAX(box_id) max_b
    FROM cdpl_cold_stocks
    WHERE box_id = ANY(:bx)
    GROUP BY transaction_no, lot_no, item_description ORDER BY boxes DESC
""", {"bx": [f"90556000-{i}" for i in range(1, 21)]})

run("a2. 90556000-48..73 anywhere — which txn/lot", """
    SELECT transaction_no, lot_no, item_description, COUNT(*) boxes,
           MIN(box_id) min_b, MAX(box_id) max_b
    FROM cdpl_cold_stocks
    WHERE box_id = ANY(:bx)
    GROUP BY transaction_no, lot_no, item_description ORDER BY boxes DESC
""", {"bx": [f"90556000-{i}" for i in range(48, 74)]})

# (b) list candidate transfer / out / inward tables
print("\n== b. candidate tables (transfer/out/in/stock/grn) ==")
all_tables = insp.get_table_names()
cand = [t for t in all_tables if any(k in t.lower() for k in
        ("transfer","stock_out","grn","inward","bulk","interunit","cold"))]
for t in sorted(cand):
    print("  ", t)

# (c) search EVERY table that has a lot-ish column for value '10470'
print("\n== c. lot 10470 across all tables (lot-ish columns) ==")
for t in sorted(all_tables):
    try:
        cols = [col["name"] for col in insp.get_columns(t)]
    except Exception:
        continue
    lotcols = [c for c in cols if c.lower() in ("lot_no","lot_number","lot","batch_number")]
    if not lotcols:
        continue
    for lc in lotcols:
        try:
            with e.connect() as c:
                c.execute(text("SET TRANSACTION READ ONLY"))
                n = c.execute(text(f"SELECT COUNT(*) FROM {t} WHERE TRIM(CAST({lc} AS TEXT))='10470'")).scalar()
            if n:
                print(f"  {t}.{lc}: {n} rows")
        except Exception as ex:
            pass
print("\nDone.")
