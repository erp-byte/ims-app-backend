"""
Give a cold pile its missing lot number — at the source, so it sticks.

WHY NOT JUST UPDATE cold_stocks
-------------------------------
The lot-less piles are `auto_created_from_inward = true`: they are a MIRROR of an inward.
`sync_cold_stocks_from_inward` (inward_tools.py:2815) DELETEs and re-INSERTs those rows on
every approve/edit of that inward, taking

    lot_no = COALESCE(box.lot_number, article.lot_number)

so a lot typed straight into cold_stocks survives until the next edit and then silently
reverts. This writes the lot on the INWARD's boxes + article and then runs the same sync,
which is also what makes the lot show up in Lot Search / inward reports, not just cold stock.

The lot VALUE is never invented here — you pass it in. It has to come from the physical
paperwork (GRN / packing list) or the cold store's own closing sheet, because for these
piles it was never captured anywhere in the database (verified: no cold_stock_disposition
record, and the inward's own boxes carry lot_number = NULL).

USAGE
  Dry-run:  python set_inward_lot.py --txn TR-20260719123650 --lot 13766
  Apply:    python set_inward_lot.py --txn TR-20260719123650 --lot 13766 --apply
  Optional: --item-mark BROWN   (fills the blank item_mark on the same inward)
"""
import argparse
import os
import sys

from sqlalchemy import text

from services.ims_service.inward_tools import sync_cold_stocks_from_inward
from shared.database import SessionLocal


def resolve(db, txn):
    """Find which company + table family owns this inward transaction."""
    for company, prefix in (("CFPL", "cfpl"), ("CDPL", "cdpl")):
        for fam in ("bulk_entry_transactions", "transactions_v2"):
            tbl = f"{prefix}_{fam}"
            if not db.execute(text("SELECT to_regclass(:t)"), {"t": f"public.{tbl}"}).scalar():
                continue
            row = db.execute(text(f"SELECT * FROM {tbl} WHERE transaction_no = :t"), {"t": txn}).fetchone()
            if row is not None:
                tables = ({"tx": f"{prefix}_bulk_entry_transactions",
                           "art": f"{prefix}_bulk_entry_articles",
                           "box": f"{prefix}_bulk_entry_boxes"}
                          if fam == "bulk_entry_transactions" else
                          {"tx": f"{prefix}_transactions_v2",
                           "art": f"{prefix}_articles_v2",
                           "box": f"{prefix}_boxes_v2"})
                return company, prefix, tables, row
    return None, None, None, None


def has_col(db, table, col):
    return bool(db.execute(text("""
        SELECT 1 FROM information_schema.columns WHERE table_name = :t AND column_name = :c
    """), {"t": table, "c": col}).scalar())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--txn", required=True, help="Inward transaction_no (e.g. TR-20260719123650)")
    ap.add_argument("--lot", required=True, help="Lot number to set on every box of that inward")
    ap.add_argument("--item-mark", help="Optional item_mark to set on the inward's article")
    ap.add_argument("--apply", action="store_true", help="Write (default: dry-run)")
    args = ap.parse_args()

    db = SessionLocal()
    try:
        company, prefix, tables, tx = resolve(db, args.txn)
        if not tx:
            print(f"ABORT: no inward transaction {args.txn} in cfpl/cdpl.")
            return 1
        wh = tx._mapping.get("warehouse")
        print(f"Inward {args.txn}: company={company} tables={tables['box']} "
              f"warehouse={wh!r} status={tx._mapping.get('status')!r}")

        boxes = db.execute(text(f"""
            SELECT COUNT(*) n,
                   COUNT(*) FILTER (WHERE COALESCE(TRIM(lot_number),'') <> '') with_lot,
                   COUNT(DISTINCT lot_number) lots
            FROM {tables['box']} WHERE transaction_no = :t"""), {"t": args.txn}).fetchone()
        cold_tbl = f"{prefix}_cold_stocks"
        cold = db.execute(text(f"""
            SELECT COUNT(*) n, COUNT(*) FILTER (WHERE COALESCE(TRIM(CAST(lot_no AS TEXT)),'') <> '') with_lot
            FROM {cold_tbl}
            WHERE inward_transaction_no = :t AND auto_created_from_inward = true"""),
            {"t": args.txn}).fetchone()
        print(f"  inward boxes: {boxes.n} ({boxes.with_lot} already carry a lot)")
        print(f"  mirrored cold rows: {cold.n} ({cold.with_lot} already carry a lot)")

        if boxes.n == 0:
            print("ABORT: that inward has no box rows.")
            return 1
        if boxes.with_lot:
            print(f"ABORT: {boxes.with_lot} box(es) already carry a lot number — refusing to "
                  f"overwrite. Clear them first if the relabel is intended.")
            return 1

        print(f"\nPlanned: set lot_number = {args.lot!r} on {boxes.n} inward box(es)"
              + (f" and item_mark = {args.item_mark!r} on the article" if args.item_mark else "")
              + f", then re-run sync_cold_stocks_from_inward -> {cold_tbl}")

        if not args.apply:
            print("\n(dry-run — no changes written. Re-run with --apply to commit.)")
            return 0

        db.execute(text(f"UPDATE {tables['box']} SET lot_number = :lot WHERE transaction_no = :t"),
                   {"lot": args.lot, "t": args.txn})
        if has_col(db, tables["art"], "lot_number"):
            db.execute(text(f"UPDATE {tables['art']} SET lot_number = :lot WHERE transaction_no = :t"),
                       {"lot": args.lot, "t": args.txn})
        if args.item_mark and has_col(db, tables["art"], "item_mark"):
            db.execute(text(f"UPDATE {tables['art']} SET item_mark = :m WHERE transaction_no = :t"),
                       {"m": args.item_mark, "t": args.txn})

        inserted = sync_cold_stocks_from_inward(company, args.txn, tables, db)
        db.commit()

        after = db.execute(text(f"""
            SELECT COUNT(*) n, COUNT(DISTINCT CAST(lot_no AS TEXT)) lots, MIN(CAST(lot_no AS TEXT)) lot
            FROM {cold_tbl} WHERE inward_transaction_no = :t"""), {"t": args.txn}).fetchone()
        print(f"[OK] COMMITTED. cold rows re-synced: {inserted} inserted; "
              f"pile now {after.n} rows, lot={after.lot!r}")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
