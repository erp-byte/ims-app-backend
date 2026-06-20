"""READ-ONLY: list every cold lot that overlaps boxes_v2 / bulk_entry_boxes (by lot),
with the distinct transaction_no in each source, to gauge the txn-mismatch tracing job."""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB, pool_pre_ping=True)

for comp in ["cfpl", "cdpl"]:
    cs, v2, bk = f"{comp}_cold_stocks", f"{comp}_boxes_v2", f"{comp}_bulk_entry_boxes"
    with engine.connect() as c:
        q = text(f"""
          WITH cg AS (
            SELECT TRIM(lot_no) lot,
              string_agg(DISTINCT item_description, ' | ') cold_desc,
              COUNT(*) cold_boxes,
              string_agg(DISTINCT transaction_no, ',') cold_txn
            FROM {cs} WHERE box_id IS NOT NULL GROUP BY TRIM(lot_no)
          ),
          vg AS (SELECT TRIM(lot_number) lot, string_agg(DISTINCT transaction_no, ',') v2_txn, COUNT(*) v2_boxes FROM {v2} GROUP BY TRIM(lot_number)),
          bg AS (SELECT TRIM(lot_number) lot, string_agg(DISTINCT transaction_no, ',') bk_txn, COUNT(*) bk_boxes FROM {bk} GROUP BY TRIM(lot_number))
          SELECT cg.lot, cg.cold_desc, cg.cold_boxes, cg.cold_txn,
                 vg.v2_txn, COALESCE(vg.v2_boxes,0) v2_boxes,
                 bg.bk_txn, COALESCE(bg.bk_boxes,0) bk_boxes
          FROM cg LEFT JOIN vg ON vg.lot=cg.lot LEFT JOIN bg ON bg.lot=cg.lot
          WHERE vg.lot IS NOT NULL OR bg.lot IS NOT NULL
          ORDER BY cg.cold_boxes DESC
        """)
        rows = c.execute(q).mappings().all()
        print(f"\n========== [{comp}]  {len(rows)} cold lots overlap v2/bulk ==========")
        for r in rows:
            cold_set = set((r["cold_txn"] or "").split(","))
            v2_set = set((r["v2_txn"] or "").split(",")) if r["v2_txn"] else set()
            bk_set = set((r["bk_txn"] or "").split(",")) if r["bk_txn"] else set()
            src = v2_set | bk_set
            mismatch = "TXN-MISMATCH" if not (cold_set & src) else ("PARTIAL" if (cold_set - src) else "match")
            print(f"  lot {r['lot']:>8} | cold={r['cold_boxes']:>4}box v2={r['v2_boxes']:>4} bulk={r['bk_boxes']:>4} | {mismatch}")
            print(f"      cold_desc : {r['cold_desc']!r}")
            print(f"      cold_txn  : {sorted(cold_set)}")
            print(f"      v2_txn    : {sorted(v2_set)}")
            print(f"      bulk_txn  : {sorted(bk_set)}")
