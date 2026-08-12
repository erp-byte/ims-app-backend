"""READ-ONLY: index signatures + the exact nature of the repair collisions.

Two things must be known before any UPDATE is written:
  1. what the existing *_txn_line_uq / *_txn_line_box_uq indexes are actually ON -- their
     names imply line_number, which would change what a repair is allowed to do;
  2. whether the 1,842 colliding rows are genuine duplicate boxes (same box_id / same
     weights, i.e. the double-write this bug causes) or distinct boxes that merely share
     a box_number -- the first can be merged, the second must be renumbered.

NO writes.
"""
import os
from sqlalchemy import create_engine, text

if not os.environ.get("DATABASE_URL"):
    for line in open(os.path.join(os.path.dirname(__file__), ".env"), encoding="utf-8"):
        line = line.strip()
        if line.startswith("DATABASE_URL") and "=" in line:
            os.environ["DATABASE_URL"] = line.split("=", 1)[1].strip().strip('"').strip("'")
            break

DB = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
e = create_engine(DB)

with e.connect() as c:
    print("=" * 78)
    print("ACTUAL INDEX DEFINITIONS")
    print("=" * 78)
    for row in c.execute(text("""
        SELECT tablename, indexname, indexdef
        FROM pg_indexes
        WHERE tablename IN ('cfpl_articles_v2','cdpl_articles_v2',
                            'cfpl_boxes_v2','cdpl_boxes_v2')
        ORDER BY tablename, indexname
    """)).fetchall():
        print(f"\n  {row.tablename}.{row.indexname}")
        print(f"    {row.indexdef}")

    print("\n" + "=" * 78)
    print("NATURE OF THE CFPL COLLISIONS")
    print("=" * 78)
    summary = c.execute(text("""
        SELECT
          COUNT(*)                                                        AS total,
          COUNT(*) FILTER (WHERE b.box_id IS NOT DISTINCT FROM b2.box_id) AS same_box_id,
          COUNT(*) FILTER (WHERE b.box_id IS NULL)                        AS unset_has_no_boxid,
          COUNT(*) FILTER (WHERE b.net_weight IS NOT DISTINCT FROM b2.net_weight
                            AND b.gross_weight IS NOT DISTINCT FROM b2.gross_weight
                            AND b.lot_number IS NOT DISTINCT FROM b2.lot_number)
                                                                          AS identical_payload
        FROM cfpl_boxes_v2 b
        JOIN cfpl_articles_v2 a
          ON a.transaction_no   = b.transaction_no
         AND a.item_description = b.article_description
         AND a.line_number IS NOT NULL AND a.line_number <> 0
        JOIN cfpl_boxes_v2 b2
          ON b2.transaction_no = b.transaction_no
         AND b2.line_number    = a.line_number
         AND b2.box_number     = b.box_number
        WHERE (b.line_number IS NULL OR b.line_number = 0)
    """)).fetchone()
    print(f"  {summary.total:>6} colliding unset-line rows")
    print(f"  {summary.same_box_id:>6} share the same box_id as the row they'd collide with")
    print(f"  {summary.unset_has_no_boxid:>6} of the unset rows have NO box_id (never printed)")
    print(f"  {summary.identical_payload:>6} have identical net/gross/lot "
          f"(true duplicate content)")

    print("\n  sample (unset row  vs  the row it would collide with):")
    for s in c.execute(text("""
        SELECT b.transaction_no, b.box_number, a.line_number AS target_line,
               b.box_id AS unset_boxid, b.net_weight AS unset_net, b.lot_number AS unset_lot,
               b2.box_id AS keep_boxid, b2.net_weight AS keep_net, b2.lot_number AS keep_lot
        FROM cfpl_boxes_v2 b
        JOIN cfpl_articles_v2 a
          ON a.transaction_no   = b.transaction_no
         AND a.item_description = b.article_description
         AND a.line_number IS NOT NULL AND a.line_number <> 0
        JOIN cfpl_boxes_v2 b2
          ON b2.transaction_no = b.transaction_no
         AND b2.line_number    = a.line_number
         AND b2.box_number     = b.box_number
        WHERE (b.line_number IS NULL OR b.line_number = 0)
        ORDER BY b.transaction_no, b.box_number
        LIMIT 8
    """)).fetchall():
        print(f"    {s.transaction_no} box#{s.box_number} -> line {s.target_line}")
        print(f"       unset: box_id={s.unset_boxid} net={s.unset_net} lot={s.unset_lot}")
        print(f"       keep : box_id={s.keep_boxid} net={s.keep_net} lot={s.keep_lot}")

    # How many distinct transactions are affected by collisions?
    txns = c.execute(text("""
        SELECT COUNT(DISTINCT b.transaction_no)
        FROM cfpl_boxes_v2 b
        JOIN cfpl_articles_v2 a
          ON a.transaction_no   = b.transaction_no
         AND a.item_description = b.article_description
         AND a.line_number IS NOT NULL AND a.line_number <> 0
        JOIN cfpl_boxes_v2 b2
          ON b2.transaction_no = b.transaction_no
         AND b2.line_number    = a.line_number
         AND b2.box_number     = b.box_number
        WHERE (b.line_number IS NULL OR b.line_number = 0)
    """)).scalar()
    print(f"\n  collisions span {txns} transaction(s)")

print("\nDone. READ-ONLY - nothing was written.")
