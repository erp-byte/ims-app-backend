import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)

COLD = ("(SELECT box_id,transaction_no,lot_no lot,item_description item FROM cfpl_cold_stocks "
        "UNION ALL SELECT box_id,transaction_no,lot_no,item_description FROM cdpl_cold_stocks)")
V2 = ("(SELECT box_id,transaction_no,lot_number lot,article_description item FROM cfpl_boxes_v2 "
      "UNION ALL SELECT box_id,transaction_no,lot_number,article_description FROM cdpl_boxes_v2)")

with e.connect() as c:
    c.execute(text("SET TRANSACTION READ ONLY"))
    for label, src in (("COLD_STOCKS", COLD), ("BOXES_V2", V2)):
        r = c.execute(text(f"""
            SELECT COUNT(*) dup_keys, COALESCE(SUM(rows),0) total_rows,
                   COALESCE(SUM(CASE WHEN distinct_items>1 THEN 1 ELSE 0 END),0) keys_diff_item
            FROM (
              SELECT box_id, transaction_no, lot, COUNT(*) rows,
                     COUNT(DISTINCT COALESCE(item,'')) distinct_items
              FROM {src} t
              WHERE box_id IS NOT NULL AND box_id<>'' AND transaction_no IS NOT NULL
              GROUP BY box_id, transaction_no, lot
              HAVING COUNT(*)>1
            ) x
        """)).fetchone()
        m = r._mapping
        print(f"[{label}] (box_id,txn,lot) DUPLICATE keys: {m['dup_keys']} keys / {m['total_rows']} rows "
              f"| of which span DIFFERENT item: {m['keys_diff_item']}")

    # samples of triple-key dups that ALSO differ in item (the dangerous remainder)
    print("\nSample (box_id,txn,lot) dups that span different items (COLD):")
    rows = c.execute(text(f"""
        SELECT box_id, transaction_no, lot,
               COUNT(*) rows, COUNT(DISTINCT COALESCE(item,'')) items,
               STRING_AGG(DISTINCT COALESCE(item,'?'), ' | ') item_list
        FROM {COLD} t
        WHERE box_id IS NOT NULL AND box_id<>'' AND transaction_no IS NOT NULL
        GROUP BY box_id, transaction_no, lot
        HAVING COUNT(*)>1 AND COUNT(DISTINCT COALESCE(item,''))>1
        ORDER BY rows DESC LIMIT 12
    """)).fetchall()
    if not rows:
        print("  (none) ✓ — every (box_id,txn,lot) dup is the SAME item")
    for r in rows:
        m = r._mapping
        print(f"  {m['box_id']} / {m['transaction_no']} / lot {m['lot']}: {m['rows']} rows, {m['items']} items -> {m['item_list'][:120]}")
print("\nDone (read-only).")
