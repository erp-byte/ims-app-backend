"""
One-time backfill: give box-less in-transit transfers their line-level pending rows.

Targets headers with status Dispatch/Partial that have NO pending_transfer_stock
rows. Uses the SAME runtime function (park_lines_in_pending) — tracking-only rows,
no source/destination inventory change. Idempotent: re-checks for existing pending
rows per header and skips if present.

DRY-RUN by default (inserts then ROLLS BACK, prints what it would do).
Set APPLY=1 to commit.
"""
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from services.ims_service.pending_stock_tools import park_lines_in_pending

raw = os.environ["DATABASE_URL"]
URL = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
APPLY = os.environ.get("APPLY") == "1"

engine = create_engine(URL)


def main():
    db = Session(bind=engine)
    try:
        targets = db.execute(text("""
            SELECT h.id, h.challan_no, h.from_site, h.to_site, h.status,
                   COALESCE(h.created_by, 'backfill') AS created_by
            FROM interunit_transfers_header h
            WHERE COALESCE(h.status,'') IN ('Dispatch','Partial')
              AND NOT EXISTS (
                  SELECT 1 FROM pending_transfer_stock p
                  WHERE p.transfer_out_id = h.id AND p.status = 'In Transit'
              )
            ORDER BY h.created_ts
        """)).fetchall()

        print(f"Found {len(targets)} box-less in-transit transfer(s) to backfill "
              f"({'APPLY' if APPLY else 'DRY-RUN'}).\n")

        total_rows = 0
        done = 0
        for h in targets:
            hid = h._mapping["id"]
            # idempotency guard (race / re-run safety)
            already = db.execute(text(
                "SELECT COUNT(*) FROM pending_transfer_stock "
                "WHERE transfer_out_id=:id AND status='In Transit'"), {"id": hid}).scalar()
            if already:
                print(f"  skip header {hid} {h._mapping['challan_no']}: already has {already} pending rows")
                continue

            lines = db.execute(text(
                "SELECT * FROM interunit_transfers_lines WHERE header_id=:id ORDER BY id"),
                {"id": hid}).fetchall()

            n = park_lines_in_pending(
                transfer_out_id=hid,
                challan_no=h._mapping["challan_no"],
                from_site=h._mapping["from_site"],
                to_site=h._mapping["to_site"],
                lines=lines,
                dispatched_by=h._mapping["created_by"],
                db=db,
            )
            total_rows += n
            done += 1
            print(f"  header {hid:5} {h._mapping['challan_no']:20} "
                  f"{h._mapping['from_site']}->{h._mapping['to_site']:14} "
                  f"lines={len(lines):3} -> parked {n} unit-row(s)")

        print(f"\nBackfilled {done} transfer(s), {total_rows} pending unit-row(s) total.")
        if APPLY:
            db.commit()
            print("COMMITTED.")
        else:
            db.rollback()
            print("DRY-RUN — rolled back. Set APPLY=1 to commit.")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
