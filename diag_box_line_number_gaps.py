"""READ-ONLY diagnostic: box rows whose line_number can't reach their article.

Boxes join to their article by line_number. A box row that carries NULL — or the 0 the
UI normalises a missing line to and posts back through upsert_box — never matches its
1-based article, so the inward edit and review screens (which group by line_number)
render nothing, while the view screen still shows every box because it renders a flat
list. The article's quantity_units / net_weight also stop tracking its boxes, because
the compute-on-read overlay keys off line_number too.

Prints, per company:
  1. how many box rows have an unset (NULL or 0) line_number,
  2. the affected transactions, split by what each box needs: repairable (its article
     already has a real line), needs_art (the article's own line is unset too), or
     orphan (no article row matches the name at all),
  3. for each, the article-level drift that the screens are showing.

NO writes. Run:  python diag_box_line_number_gaps.py [TRANSACTION_NO]
"""
import os
import sys
from sqlalchemy import create_engine, text

if not os.environ.get("DATABASE_URL"):
    for line in open(os.path.join(os.path.dirname(__file__), ".env"), encoding="utf-8"):
        line = line.strip()
        if line.startswith("DATABASE_URL") and "=" in line:
            os.environ["DATABASE_URL"] = line.split("=", 1)[1].strip().strip('"').strip("'")
            break

DB = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
e = create_engine(DB)

ONLY_TXN = sys.argv[1] if len(sys.argv) > 1 else None

# "unset" == NULL or 0. Real lines are 1-based (_assign_line_numbers starts at 1).
UNSET = "(b.line_number IS NULL OR b.line_number = 0)"


def main() -> None:
    with e.connect() as c:
        for prefix in ("cfpl", "cdpl"):
            box, art = f"{prefix}_boxes_v2", f"{prefix}_articles_v2"
            print(f"\n{'=' * 78}\n{box}\n{'=' * 78}")

            total = c.execute(text(f"SELECT COUNT(*) FROM {box}")).scalar()
            nulls = c.execute(text(
                f"SELECT COUNT(*) FROM {box} b WHERE b.line_number IS NULL")).scalar()
            zeros = c.execute(text(
                f"SELECT COUNT(*) FROM {box} b WHERE b.line_number = 0")).scalar()
            print(f"  {total:>8,} box rows total")
            print(f"  {nulls:>8,} with line_number IS NULL")
            print(f"  {zeros:>8,} with line_number = 0   <- written back by the UI")
            if not (nulls or zeros):
                print("  -> clean, nothing to repair here")
                continue

            txn_filter = " AND b.transaction_no = :txn" if ONLY_TXN else ""
            params = {"txn": ONLY_TXN} if ONLY_TXN else {}

            # NB: classify on the JOIN having matched, not on COUNT(a.line_number) --
            # an article can match by name and still carry an unset line_number of its
            # own (all 50 cdpl articles did), which COUNT() would silently score as
            # "orphan". Those need their article numbered first, not treating as orphans.
            rows = c.execute(text(f"""
                SELECT b.transaction_no,
                       COUNT(*)                                    AS bad_boxes,
                       COUNT(*) FILTER (WHERE a.item_description IS NOT NULL
                                          AND a.line_number IS NOT NULL
                                          AND a.line_number <> 0)  AS repairable,
                       COUNT(*) FILTER (WHERE a.item_description IS NOT NULL
                                          AND (a.line_number IS NULL
                                               OR a.line_number = 0)) AS needs_art,
                       COUNT(*) FILTER (WHERE a.item_description IS NULL) AS orphaned
                FROM {box} b
                LEFT JOIN {art} a
                       ON a.transaction_no  = b.transaction_no
                      AND a.item_description = b.article_description
                WHERE {UNSET}{txn_filter}
                GROUP BY b.transaction_no
                ORDER BY COUNT(*) DESC
                LIMIT 40
            """), params).fetchall()

            print(f"\n  affected transactions (top {len(rows)}):")
            print(f"    {'transaction_no':<28} {'bad':>6} {'repairable':>11} "
                  f"{'needs_art':>10} {'orphan':>7}")
            for r in rows:
                print(f"    {r.transaction_no:<28} {r.bad_boxes:>6} "
                      f"{r.repairable:>11} {r.needs_art:>10} {r.orphaned:>7}")

            # Article-level drift: what the screens currently show vs. what the boxes say.
            print("\n  article drift on those transactions "
                  "(stored value -> what the boxes actually total):")
            drift = c.execute(text(f"""
                SELECT a.transaction_no, a.line_number, a.item_description,
                       a.quantity_units                       AS stored_qty,
                       COUNT(b.*)                             AS real_boxes,
                       COALESCE(a.net_weight, 0)              AS stored_net,
                       COALESCE(SUM(b.net_weight), 0)         AS real_net
                FROM {art} a
                JOIN {box} b
                  ON b.transaction_no   = a.transaction_no
                 AND b.article_description = a.item_description
                WHERE {UNSET}{txn_filter}
                GROUP BY a.transaction_no, a.line_number, a.item_description,
                         a.quantity_units, a.net_weight
                HAVING a.quantity_units IS DISTINCT FROM COUNT(b.*)
                ORDER BY COUNT(b.*) DESC
                LIMIT 25
            """), params).fetchall()
            if not drift:
                print("    (none — aggregates already agree with the boxes)")
            for d in drift:
                print(f"    {d.transaction_no:<24} line={d.line_number} "
                      f"qty {d.stored_qty} -> {d.real_boxes:<6} "
                      f"net {float(d.stored_net):.2f} -> {float(d.real_net):.2f}"
                      f"   {str(d.item_description)[:38]}")

            # The duplicate-box hazard: a (0/NULL, box_number) row whose (real_line,
            # box_number) twin already exists. Printing either one inserts a duplicate.
            dupes = c.execute(text(f"""
                SELECT COUNT(*) FROM {box} b
                JOIN {art} a
                  ON a.transaction_no    = b.transaction_no
                 AND a.item_description  = b.article_description
                JOIN {box} b2
                  ON b2.transaction_no = b.transaction_no
                 AND b2.line_number    = a.line_number
                 AND b2.box_number     = b.box_number
                WHERE {UNSET}{txn_filter}
            """), params).scalar()
            print(f"\n  {dupes} unset-line box(es) already have a same-numbered row on the "
                  f"real line\n    (these would collide if repaired blindly — "
                  f"merge, don't reassign)")


if __name__ == "__main__":
    main()
    print("\nDone. READ-ONLY — nothing was written.")
