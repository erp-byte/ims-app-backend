"""READ-ONLY: precise state of the line_number re-key across articles AND boxes.

Supersedes the classification in diag_box_line_number_gaps.py, which counted
COUNT(a.line_number) and so mis-reported "article matched but its own line_number is
NULL" as "orphan box". Splits the boxes three ways instead:

  ready        - description matches an article that already has a real (1-based) line
  needs_article- description matches an article whose OWN line_number is unset
  orphan       - description matches no article row at all

Also reports whether the 2026-07-31 migration actually landed: the column exists (we can
query it), but its backfill and unique indexes may not have run.

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

UNSET_B = "(b.line_number IS NULL OR b.line_number = 0)"
UNSET_A = "(a.line_number IS NULL OR a.line_number = 0)"

with e.connect() as c:
    print("=" * 78)
    print("DID THE 2026-07-31 MIGRATION LAND?")
    print("=" * 78)
    for idx in ("uq_cfpl_articles_v2_txn_line", "uq_cdpl_articles_v2_txn_line",
                "uq_cfpl_boxes_v2_txn_line_box", "uq_cdpl_boxes_v2_txn_line_box",
                "uq_cfpl_boxes_v2_txn_desc_box_nolinenull",
                "uq_cdpl_boxes_v2_txn_desc_box_nolinenull"):
        got = c.execute(text(
            "SELECT 1 FROM pg_class WHERE relkind='i' AND relname=:n"), {"n": idx}).fetchone()
        print(f"  {'PRESENT' if got else 'MISSING':<8} {idx}")

    # Old name-based unique keys the migration was meant to drop.
    print("\n  old name-based unique indexes still present (migration step 4):")
    old = c.execute(text("""
        SELECT t.relname AS tbl, i.relname AS idx
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_class t ON t.oid = ix.indrelid
        WHERE ix.indisunique
          AND t.relname IN ('cfpl_articles_v2','cdpl_articles_v2',
                            'cfpl_boxes_v2','cdpl_boxes_v2')
        ORDER BY t.relname, i.relname
    """)).fetchall()
    for o in old:
        print(f"    {o.tbl:<20} {o.idx}")

    for prefix in ("cfpl", "cdpl"):
        art, box = f"{prefix}_articles_v2", f"{prefix}_boxes_v2"
        print("\n" + "=" * 78)
        print(f"{prefix.upper()}")
        print("=" * 78)

        a_tot = c.execute(text(f"SELECT COUNT(*) FROM {art}")).scalar()
        a_bad = c.execute(text(
            f"SELECT COUNT(*) FROM {art} a WHERE {UNSET_A}")).scalar()
        b_tot = c.execute(text(f"SELECT COUNT(*) FROM {box}")).scalar()
        b_bad = c.execute(text(
            f"SELECT COUNT(*) FROM {box} b WHERE {UNSET_B}")).scalar()
        print(f"  articles : {a_bad:>8,} / {a_tot:>8,} with unset line_number")
        print(f"  boxes    : {b_bad:>8,} / {b_tot:>8,} with unset line_number")

        cls = c.execute(text(f"""
            SELECT
              COUNT(*) FILTER (WHERE a.item_description IS NOT NULL
                               AND a.line_number IS NOT NULL
                               AND a.line_number <> 0)                  AS ready,
              COUNT(*) FILTER (WHERE a.item_description IS NOT NULL
                               AND (a.line_number IS NULL
                                    OR a.line_number = 0))              AS needs_article,
              COUNT(*) FILTER (WHERE a.item_description IS NULL)        AS orphan
            FROM {box} b
            LEFT JOIN {art} a
                   ON a.transaction_no   = b.transaction_no
                  AND a.item_description = b.article_description
            WHERE {UNSET_B}
        """)).fetchone()
        print(f"\n  of the {b_bad:,} unset boxes:")
        print(f"    {cls.ready:>8,}  ready         (article already has a real line)")
        print(f"    {cls.needs_article:>8,}  needs_article (article's own line unset too)")
        print(f"    {cls.orphan:>8,}  orphan        (no article row matches the name)")

        # Ambiguity check: would a description->line map be one-to-one?
        dup = c.execute(text(f"""
            SELECT COUNT(*) FROM (
                SELECT transaction_no, item_description
                FROM {art}
                GROUP BY transaction_no, item_description
                HAVING COUNT(*) > 1
            ) x
        """)).scalar()
        print(f"\n  {dup} (transaction, item_description) pairs have >1 article row")
        print("    (>0 means the name is ambiguous there - repair by name would guess)")

        # Collision: after repair, would (txn, line, box_number) already be taken?
        coll = c.execute(text(f"""
            SELECT COUNT(*) FROM {box} b
            JOIN {art} a
              ON a.transaction_no   = b.transaction_no
             AND a.item_description = b.article_description
             AND a.line_number IS NOT NULL AND a.line_number <> 0
            JOIN {box} b2
              ON b2.transaction_no = b.transaction_no
             AND b2.line_number    = a.line_number
             AND b2.box_number     = b.box_number
            WHERE {UNSET_B}
        """)).scalar()
        print(f"  {coll} box(es) would collide on (transaction_no, line_number, box_number)")

print("\nDone. READ-ONLY - nothing was written.")
