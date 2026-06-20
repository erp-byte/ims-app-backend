"""Read-only scan: find cold-mirror gaps (cold_stocks short vs bulk_entry_boxes) across both companies.

Classifies each cold-mirrored (txn, lot):
  - CLEAN_GAP : cold box_ids all exist in bulk (cold subset of bulk) AND some bulk boxes missing
                -> safe to auto-add (like lot 8762).
  - WRONG_IDS : cold has box_ids NOT in bulk -> needs manual review (don't auto-fix).
  - OK        : counts match.
Also lists articles where box_count != quantity_units (the mirror's root-cause field).
"""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)

def run(t, sql, p=None, limit_print=40):
    print(f"\n== {t} ==")
    with e.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        rows = c.execute(text(sql), p or {}).fetchall()
    if not rows:
        print("  (none)"); return []
    k = rows[0]._mapping.keys()
    print("  " + " | ".join(k))
    for r in rows[:limit_print]:
        print("  " + " | ".join(str(r._mapping[x]) for x in k))
    if len(rows) > limit_print:
        print(f"  ... (+{len(rows)-limit_print} more)")
    return rows

for co, cold_t, bulk_t, art_t in (
    ("cfpl", "cfpl_cold_stocks", "cfpl_bulk_entry_boxes", "cfpl_bulk_entry_articles"),
    ("cdpl", "cdpl_cold_stocks", "cdpl_bulk_entry_boxes", "cdpl_bulk_entry_articles"),
):
    sql = f"""
    WITH u AS (
      SELECT transaction_no, lot_no AS lot, box_id, 'cold' src FROM {cold_t} WHERE transaction_no IS NOT NULL AND box_id IS NOT NULL
      UNION ALL
      SELECT transaction_no, lot_number AS lot, box_id, 'bulk' src FROM {bulk_t} WHERE transaction_no IS NOT NULL AND box_id IS NOT NULL
    ),
    dedup AS (
      SELECT transaction_no, lot, box_id,
             bool_or(src='cold') in_cold, bool_or(src='bulk') in_bulk
      FROM u GROUP BY transaction_no, lot, box_id
    ),
    agg AS (
      SELECT transaction_no, lot,
        COUNT(*) FILTER (WHERE in_cold) cold_cnt,
        COUNT(*) FILTER (WHERE in_bulk) bulk_cnt,
        COUNT(*) FILTER (WHERE in_cold AND NOT in_bulk) cold_not_in_bulk,
        COUNT(*) FILTER (WHERE in_bulk AND NOT in_cold) bulk_not_in_cold
      FROM dedup GROUP BY transaction_no, lot
    )
    SELECT * FROM agg WHERE cold_cnt > 0 AND (bulk_not_in_cold > 0 OR cold_not_in_bulk > 0)
    ORDER BY bulk_not_in_cold DESC, cold_not_in_bulk DESC
    """
    rows = run(f"[{co}] cold-mirror discrepancies (cold-present lots with any mismatch)", sql, limit_print=60)
    clean = [r for r in rows if r._mapping["cold_not_in_bulk"] == 0 and r._mapping["bulk_not_in_cold"] > 0]
    wrong = [r for r in rows if r._mapping["cold_not_in_bulk"] > 0]
    add_total = sum(r._mapping["bulk_not_in_cold"] for r in clean)
    print(f"\n  [{co}] CLEAN_GAP lots (safe auto-add): {len(clean)}  (total boxes to add: {add_total})")
    print(f"  [{co}] WRONG_IDS lots (manual review): {len(wrong)}  "
          f"(cold box_ids not in bulk: {sum(r._mapping['cold_not_in_bulk'] for r in wrong)})")

    run(f"[{co}] articles with box_count != quantity_units (root-cause field)", f"""
        SELECT COUNT(*) articles,
               SUM(CASE WHEN quantity_units > box_count THEN 1 ELSE 0 END) qty_gt_boxcount
        FROM {art_t} WHERE box_count IS DISTINCT FROM quantity_units
    """, limit_print=5)
print("\nDone.")
