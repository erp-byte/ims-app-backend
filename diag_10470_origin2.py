"""READ-ONLY: chase the two origin leads for lot 10470 Medjoul:
  (1) original inward GR6226 (2025-12-04) — how many Medjoul boxes received?
  (2) transfer-out 924 / TRANS202606041324 — did it list 47 Medjoul box_ids?
Goal: identify the other ~20 Medjoul box_ids from data (vs a blind physical recount)."""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text, inspect

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)
insp = inspect(e)
all_tables = insp.get_table_names()

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

# ---- 1. GR6226 anywhere (inward / grn / bulk-entry / cold) ----
print("### LEAD 1: inward GR6226 ###")
for t in sorted(all_tables):
    try: cols = [c["name"] for c in insp.get_columns(t)]
    except Exception: continue
    refcols = [c for c in cols if any(s in c.lower() for s in ("inward_no","grn_number","grn_no","inward_transaction","reference","ref_no","gr_no"))]
    for rc in refcols:
        try:
            with e.connect() as c:
                c.execute(text("SET TRANSACTION READ ONLY"))
                n = c.execute(text(f"SELECT COUNT(*) FROM {t} WHERE CAST({rc} AS TEXT) ILIKE '%GR6226%'")).scalar()
            if n: print(f"  HIT {t}.{rc}: {n}")
        except Exception: pass

# Medjoul-marked boxes from inward GR6226 in either cold table (search snapshot too)
run("1b. cdpl_cold_stocks Medjoul-marked / GR6226 (by item_mark)", """
    SELECT lot_no, item_mark, COUNT(*) boxes, MIN(box_id), MAX(box_id), transaction_no
    FROM cdpl_cold_stocks WHERE item_mark ILIKE '%Medjoul%' OR item_description ILIKE '%Medjoul%'
    GROUP BY lot_no, item_mark, transaction_no ORDER BY boxes DESC
""")
run("1c. cfpl_cold_stocks Medjoul-marked (origin was cfpl!)", """
    SELECT lot_no, item_mark, COUNT(*) boxes, MIN(box_id), MAX(box_id), transaction_no
    FROM cfpl_cold_stocks WHERE item_mark ILIKE '%Medjoul%' OR item_description ILIKE '%Medjoul%'
    GROUP BY lot_no, item_mark, transaction_no ORDER BY boxes DESC
""")

# ---- 2. transfer-out 924 / TRANS202606041324 ----
print("\n### LEAD 2: transfer-out 924 / TRANS202606041324 ###")
for t in sorted(all_tables):
    try: cols = [c["name"] for c in insp.get_columns(t)]
    except Exception: continue
    for rc in cols:
        if any(s in rc.lower() for s in ("transfer_out_id","reference","ref_no","transaction_no","transfer_no","out_id","pending")):
            for val, lbl in [("TRANS202606041324","ref"), ("924","id924")]:
                try:
                    with e.connect() as c:
                        c.execute(text("SET TRANSACTION READ ONLY"))
                        q = f"SELECT COUNT(*) FROM {t} WHERE CAST({rc} AS TEXT)=:v"
                        n = c.execute(text(q), {"v": val}).scalar()
                    if n: print(f"  HIT {t}.{rc} = {val}: {n}")
                except Exception: pass

# pending_transfer_stock for the ref — lists box_ids the OUT enumerated
run("2b. pending_transfer_stock for TRANS202606041324 — Medjoul lines", """
    SELECT box_id, lot_no, item_description, transaction_no, COUNT(*) OVER () total
    FROM pending_transfer_stock
    WHERE transaction_no = 'TRANS202606041324' AND (lot_no='10470' OR item_description ILIKE '%Medjoul%')
    ORDER BY box_id
""")
run("2c. pending_transfer_stock TRANS202606041324 — census by lot", """
    SELECT lot_no, item_description, COUNT(*) boxes, MIN(box_id), MAX(box_id)
    FROM pending_transfer_stock WHERE transaction_no='TRANS202606041324'
    GROUP BY lot_no, item_description ORDER BY boxes DESC
""")
print("\nDone.")
