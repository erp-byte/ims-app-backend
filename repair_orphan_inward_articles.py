"""Repair inward transactions whose boxes lost their article row (the "cashew 300" bug).

WHY THIS EXISTS
---------------
Before the line_number identity fix, two articles that shared a name — or, as seen in
TR-20260731122918, a second article ("cashew 300") added under a first ("cashew 320") —
could have their article row silently dropped by the old
    ON CONFLICT (transaction_no, item_description) DO NOTHING
while their BOXES were still saved. The result: the boxes exist in *_boxes_v2 but there is
no matching row in *_articles_v2, so:
  * the article is missing from the hover card, the view page's Articles card, and the
    edit/approve screens (all of which iterate article rows), and
  * the whole-transaction totals silently include those boxes, so the numbers disagree
    across pages.

get_inward now *surfaces* such orphan boxes on read, so the display is already consistent.
This script makes the fix permanent in the DB: it writes the missing article row (with
box-derived quantity/weights, its lot from the boxes, and a best-effort SKU lookup) and
recomputes every article's aggregates from its boxes so no stale value (e.g. an article
stuck at net 760 while its boxes sum to 1460) remains.

Metadata that was never persisted (grade, unit rate, category if the SKU can't be matched)
cannot be recovered from boxes alone — those come back NULL and can be edited afterwards.
Nothing here invents a weight or a lot; every number is read from the boxes.

Idempotent: re-running finds nothing to do. Safe-by-default: dry-run unless --apply.

USAGE
  Dry-run (default):  python repair_orphan_inward_articles.py
  Apply:              python repair_orphan_inward_articles.py --apply
  One transaction:    python repair_orphan_inward_articles.py --txn TR-20260731122918 --apply
  One company:        python repair_orphan_inward_articles.py --company CFPL --apply
"""
import argparse

from sqlalchemy import text

from services.ims_service.inward_tools import (
    table_names,
    _surface_orphan_box_articles,
    recalc_article_aggregates,
    lookup_sku,
)
from shared.database import SessionLocal

COMPANIES = [("CFPL", "cfpl"), ("CDPL", "cdpl")]


def _transactions_with_orphans(db, tables, only_txn=None):
    """Transaction numbers that have at least one box with no matching article row."""
    where_txn = "AND b.transaction_no = :txn" if only_txn else ""
    params = {"txn": only_txn} if only_txn else {}
    rows = db.execute(
        text(f"""
            SELECT DISTINCT b.transaction_no
            FROM {tables['box']} b
            WHERE (
                    (b.line_number IS NOT NULL AND NOT EXISTS (
                        SELECT 1 FROM {tables['art']} a
                        WHERE a.transaction_no = b.transaction_no AND a.line_number = b.line_number))
                 OR (b.line_number IS NULL AND NOT EXISTS (
                        SELECT 1 FROM {tables['art']} a
                        WHERE a.transaction_no = b.transaction_no AND a.item_description = b.article_description))
                  )
                  {where_txn}
        """),
        params,
    ).fetchall()
    return [r[0] for r in rows]


def _load(db, tables, txn):
    arts = db.execute(
        text(f"SELECT * FROM {tables['art']} WHERE transaction_no = :t"), {"t": txn}
    ).fetchall()
    boxes = db.execute(
        text(f"SELECT * FROM {tables['box']} WHERE transaction_no = :t"), {"t": txn}
    ).fetchall()
    return [dict(r._mapping) for r in arts], [dict(r._mapping) for r in boxes]


def repair_company(db, company, prefix, only_txn, apply):
    tables = table_names(company)
    txns = _transactions_with_orphans(db, tables, only_txn)
    summary = {"company": company, "transactions": [], "articles_created": 0}

    for txn in txns:
        arts, boxes = _load(db, tables, txn)
        orphans = _surface_orphan_box_articles(txn, arts, boxes)
        if not orphans:
            continue

        created_here = []
        for o in orphans:
            # Best-effort enrich from the SKU master by name (grade/rate stay NULL).
            sku = None
            try:
                sku = lookup_sku(o["item_description"], company, db)
            except Exception:
                sku = None
            # lot from the boxes of this group (first non-empty)
            grp_boxes = [
                b for b in boxes
                if (o["line_number"] is not None and b.get("line_number") == o["line_number"])
                or (o["line_number"] is None and b.get("article_description") == o["item_description"])
            ]
            lot = next((b.get("lot_number") for b in grp_boxes if b.get("lot_number")), None)
            row = {
                "transaction_no": txn,
                "line_number": o["line_number"],
                "sku_id": (sku or {}).get("sku_id"),
                "item_description": o["item_description"],
                "item_category": (sku or {}).get("item_category"),
                "sub_category": (sku or {}).get("sub_category"),
                "material_type": (sku or {}).get("material_type"),
                "uom": "BOX",
                "quantity_units": o["quantity_units"],
                "net_weight": o["net_weight"],
                "total_weight": o["total_weight"],
                "lot_number": lot,
            }
            created_here.append(row)
            if apply:
                db.execute(
                    text(f"""
                        INSERT INTO {tables['art']}
                            (transaction_no, line_number, sku_id, item_description, item_category,
                             sub_category, material_type, uom, quantity_units, net_weight,
                             total_weight, lot_number)
                        VALUES
                            (:transaction_no, :line_number, :sku_id, :item_description, :item_category,
                             :sub_category, :material_type, :uom, :quantity_units, :net_weight,
                             :total_weight, :lot_number)
                        ON CONFLICT (transaction_no, line_number) DO NOTHING
                    """),
                    row,
                )

        summary["articles_created"] += len(created_here)
        summary["transactions"].append({"transaction_no": txn, "created": created_here})

        # Recompute EVERY article's aggregates in this transaction from its boxes, so the
        # newly-created rows AND any stale existing rows (e.g. 760 -> 1460) become correct.
        if apply:
            # by line_number (v2) — one recompute per distinct line that has boxes
            lines = {b.get("line_number") for b in boxes if b.get("line_number") is not None}
            for ln in lines:
                desc = next((b["article_description"] for b in boxes if b.get("line_number") == ln), None)
                recalc_article_aggregates(db, tables, txn, desc, line_number=ln)
            # any boxes still without a line_number → recompute by description
            for desc in {b["article_description"] for b in boxes if b.get("line_number") is None}:
                recalc_article_aggregates(db, tables, txn, desc)

    if apply:
        db.commit()
    return summary


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--company", choices=["CFPL", "CDPL"], help="limit to one company")
    ap.add_argument("--txn", help="limit to one transaction_no")
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    args = ap.parse_args()

    companies = [(c, p) for c, p in COMPANIES if not args.company or c == args.company]
    db = SessionLocal()
    try:
        grand = 0
        for company, prefix in companies:
            s = repair_company(db, company, prefix, args.txn, args.apply)
            grand += s["articles_created"]
            print(f"\n=== {company} ===")
            if not s["transactions"]:
                print("  no orphan articles found")
            for t in s["transactions"]:
                print(f"  {t['transaction_no']}: +{len(t['created'])} article(s)")
                for c in t["created"]:
                    print(f"      line {c['line_number']}  {c['item_description']}  "
                          f"qty={c['quantity_units']} net={c['net_weight']} gross={c['total_weight']} "
                          f"lot={c['lot_number']} sku_id={c['sku_id']}")
        mode = "APPLIED" if args.apply else "DRY-RUN (re-run with --apply to write)"
        print(f"\n{mode}: {grand} orphan article row(s) across {sum(1 for _ in companies)} company set(s).")
    finally:
        db.close()


if __name__ == "__main__":
    main()
