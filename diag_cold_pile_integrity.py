"""
Cold-stock pile integrity checker (READ-ONLY).

Scans cfpl_cold_stocks + cdpl_cold_stocks for any pile that would trip the
Cold-transfer "Duplicate Box IDs From Source" guard, i.e. a pile
(item_description, lot_no, COALESCE(inward_no,'')) whose /cold-storage/stocks/pick-boxes
result is not all-distinct-box_id. Reports both failure modes:
  MODE A  cross-transaction box_id collision (same box_id string on >1 physical box)
  MODE B  NULL / empty box_id rows

Run this on a schedule (or before big transfer days) so box-ID data errors are caught
proactively instead of surfacing as a blocked transfer at the counter.

Exit code 0 = clean, 1 = broken piles found (usable in CI / cron alerts).

USAGE:  python diag_cold_pile_integrity.py
"""
import os
import sys
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

_raw = os.environ["DATABASE_URL"]
DB_URL = _raw.replace("postgresql://", "postgresql+psycopg://", 1) if _raw.startswith("postgresql://") else _raw
engine = create_engine(DB_URL)

SCAN = """
    WITH pile AS (
        SELECT item_description,
               CAST(lot_no AS TEXT) AS lot,
               COALESCE(inward_no,'') AS inward,
               COUNT(*) AS rows,
               COUNT(DISTINCT box_id) AS distinct_ids,
               COUNT(DISTINCT transaction_no) AS txns,
               COUNT(*) FILTER (WHERE box_id IS NULL OR TRIM(box_id)='') AS null_ids
        FROM {t}
        GROUP BY item_description, CAST(lot_no AS TEXT), COALESCE(inward_no,'')
    )
    SELECT item_description, lot, inward, rows, distinct_ids, txns, null_ids,
           CASE WHEN null_ids > 0 THEN 'B: null box_id'
                ELSE 'A: cross-txn collision' END AS mode,
           (rows - distinct_ids) AS collisions
    FROM pile
    WHERE rows <> distinct_ids
    ORDER BY (rows - distinct_ids) DESC, rows DESC
"""


def main() -> int:
    broken_total = 0
    with engine.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        for tbl in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
            rows = c.execute(text(SCAN.replace("{t}", tbl))).fetchall()
            print(f"\n===== {tbl}: {len(rows)} broken pile(s) =====")
            if not rows:
                print("  (clean)")
                continue
            broken_total += len(rows)
            print(f"  {'mode':22s} {'item':34s} {'lot':10s} {'inward':16s} {'rows':>5s} {'dist':>5s} {'txns':>5s} {'nulls':>5s}")
            for r in rows:
                m = r._mapping
                print(f"  {m['mode']:22s} {str(m['item_description'])[:34]:34s} {str(m['lot']):10s} "
                      f"{str(m['inward'])[:16]:16s} {m['rows']:5d} {m['distinct_ids']:5d} {m['txns']:5d} {m['null_ids']:5d}")
    print(f"\nTOTAL broken piles: {broken_total}")
    return 1 if broken_total else 0


if __name__ == "__main__":
    sys.exit(main())
