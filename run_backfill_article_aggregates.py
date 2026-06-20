"""One-shot backfill (Item 1A): repair article aggregates that desynced from their boxes.

The runtime recalc (recalc_article_aggregates, wired into upsert_box / update_inward) keeps NEW
and edited entries in sync, but rows that drifted before this fix shipped — e.g. an article stuck
at 1000 while it now has 1720 boxes — need a one-time repair. For every article that has box rows
this resets, from the boxes:

    quantity_units = COUNT(boxes)
    net_weight     = SUM(box.net_weight)
    total_weight   = SUM(box.gross_weight)

Articles with no boxes are left untouched (they were never the drift). The repair logic is
covered by test_backfill_article_aggregates.py; this script is the live-DB wrapper.

SAFE BY DEFAULT: previews (dry-run) unless you pass --confirm. Runs per company (cfpl_/cdpl_) and
covers both the main _v2 tables and the _bulk_entry_* tables. Run from the ims-app-backend dir:

    .venv\\Scripts\\python.exe run_backfill_article_aggregates.py            # preview all
    .venv\\Scripts\\python.exe run_backfill_article_aggregates.py --confirm  # APPLY
"""
import sys

from shared.database import SessionLocal
from services.ims_service.inward_tools import backfill_article_aggregates, table_names


def _table_sets():
    """The four table sets to repair: main + bulk-entry, for each company."""
    sets = []
    for company in ("CFPL", "CDPL"):
        t = table_names(company)
        prefix = "cfpl" if company == "CFPL" else "cdpl"
        sets.append((f"{company} main", {"art": t["art"], "box": t["box"]}))
        sets.append((f"{company} bulk-entry",
                     {"art": f"{prefix}_bulk_entry_articles", "box": f"{prefix}_bulk_entry_boxes"}))
    return sets


def main():
    apply = "--confirm" in sys.argv
    mode = "APPLY" if apply else "DRY-RUN (pass --confirm to apply)"
    print(f"=== Backfill article aggregates — {mode} ===\n")

    db = SessionLocal()
    grand = 0
    try:
        for label, tables in _table_sets():
            try:
                summary = backfill_article_aggregates(db, tables, apply=apply)
            except Exception as e:  # a missing/legacy table must not abort the other companies
                db.rollback()
                print(f"  {label:18s}: SKIPPED ({type(e).__name__}: {e})")
                continue
            n = summary["articles_recomputed"] if apply else summary["articles_with_boxes"]
            grand += n
            verb = "recomputed" if apply else "would recompute"
            print(f"  {label:18s}: {verb} {n} article(s) "
                  f"(articles with boxes: {summary['articles_with_boxes']})")

        print(f"\nTotal {'recomputed' if apply else 'to recompute'}: {grand}")
        if not apply:
            print("Nothing was written. Re-run with --confirm to apply.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
