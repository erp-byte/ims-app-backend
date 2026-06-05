"""READ-ONLY: quantify (box_id, transaction_no) key collisions that cause the
matching corruption. A 'collision' = one key mapping to MULTIPLE distinct
(item_description, lot) — i.e. different physical boxes sharing the same key.
"""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)

def run(t, sql, limit=15):
    print(f"\n===== {t} =====")
    with e.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        rows = c.execute(text(sql)).fetchall()
        if not rows:
            print("  (none)"); return
        cols = rows[0]._mapping.keys()
        print("  " + " | ".join(cols))
        for r in rows[:limit]:
            print("  " + " | ".join(str(r._mapping[k]) for k in cols))
        if len(rows) > limit:
            print(f"  ... (+{len(rows)-limit} more)")

COLD = "(SELECT box_id, transaction_no, item_description AS item, lot_no AS lot FROM cfpl_cold_stocks " \
       "UNION ALL SELECT box_id, transaction_no, item_description, lot_no FROM cdpl_cold_stocks)"
V2 = "(SELECT box_id, transaction_no, article_description AS item, lot_number AS lot FROM cfpl_boxes_v2 " \
     "UNION ALL SELECT box_id, transaction_no, article_description, lot_number FROM cdpl_boxes_v2)"

for label, src in (("COLD_STOCKS", COLD), ("BOXES_V2", V2)):
    run(f"[{label}] (box_id, txn) keys mapping to >1 distinct (item,lot) — COLLISIONS", f"""
        SELECT COUNT(*) AS colliding_keys,
               SUM(variants) AS total_variant_rows
        FROM (
          SELECT box_id, transaction_no, COUNT(DISTINCT COALESCE(item,'')||'#'||COALESCE(lot,'')) AS variants
          FROM {src} t
          GROUP BY box_id, transaction_no
          HAVING COUNT(DISTINCT COALESCE(item,'')||'#'||COALESCE(lot,'')) > 1
        ) x
    """)
    run(f"[{label}] sample colliding keys (same box+txn, different items)", f"""
        SELECT box_id, transaction_no,
               COUNT(DISTINCT COALESCE(item,'')||'#'||COALESCE(lot,'')) variants,
               STRING_AGG(DISTINCT COALESCE(item,'?')||' (lot '||COALESCE(lot,'?')||')', '  ||  ') items
        FROM {src} t
        GROUP BY box_id, transaction_no
        HAVING COUNT(DISTINCT COALESCE(item,'')||'#'||COALESCE(lot,'')) > 1
        ORDER BY variants DESC
        LIMIT 12
    """)

# transaction_no that spans multiple distinct items (timestamp collision across batches)
run("[COLD] transaction_no spanning multiple distinct items (TR- timestamp collisions)", f"""
    SELECT transaction_no, COUNT(DISTINCT COALESCE(item,'')) distinct_items, COUNT(*) rows
    FROM {COLD} t
    GROUP BY transaction_no
    HAVING COUNT(DISTINCT COALESCE(item,'')) > 1
    ORDER BY distinct_items DESC
    LIMIT 15
""")

# How unique would (box_id, txn, item, lot) be vs (box_id, txn)?
run("[COLD] uniqueness comparison", f"""
    SELECT
      COUNT(*) total_rows,
      COUNT(DISTINCT (box_id, transaction_no)) distinct_box_txn,
      COUNT(DISTINCT (box_id, transaction_no, COALESCE(item,''), COALESCE(lot,''))) distinct_box_txn_item_lot
    FROM {COLD} t
""")
print("\nDone (read-only).")
