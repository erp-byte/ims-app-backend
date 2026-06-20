"""Cancel / close a stale transfer-OUT on the LIVE database.

SAFE BY DEFAULT: previews unless you pass --confirm. Run from ims-app-backend.

  .venv\\Scripts\\python.exe cancel_transfer.py 403            # preview (read-only)
  .venv\\Scripts\\python.exe cancel_transfer.py 403 --confirm  # CANCEL tid 403

What --confirm does (via delete_transfer):
  1. reverses any Transfer-IN (delete destination boxes, restore to pending),
  2. restore_to_source: puts every remaining In-Transit box back into its source table,
  3. deletes the transfer-out boxes, lines, and header.
The transfer then disappears from the Pending modal. The transfer RECORD is deleted
(not merely flagged) -- this is the system's only 'close' mechanism for a dispatch.
"""
import sys
from shared.database import SessionLocal
from sqlalchemy import text
from services.ims_service.interunit_tools import delete_transfer


def main():
    confirm = "--confirm" in sys.argv
    tids = [int(a) for a in sys.argv[1:] if not a.startswith("--")]
    if not tids:
        print("Usage: cancel_transfer.py <transfer_out_id> [--confirm]")
        return
    db = SessionLocal()
    try:
        for tid in tids:
            h = db.execute(
                text("SELECT challan_no, from_site, to_site, status FROM interunit_transfers_header WHERE id=:t"),
                {"t": tid},
            ).fetchone()
            if not h:
                print(f"tid {tid}: NOT FOUND — skipping")
                continue
            pend = db.execute(
                text("SELECT COUNT(*) FROM pending_transfer_stock WHERE transfer_out_id=:t AND status='In Transit'"),
                {"t": tid},
            ).scalar()
            grn = db.execute(
                text("SELECT COUNT(*) FROM interunit_transfer_in_header WHERE transfer_out_id=:t"),
                {"t": tid},
            ).scalar()
            print(f"tid {tid}: {h.challan_no} [{h.status}] {h.from_site} -> {h.to_site} | "
                  f"in_transit={pend} | GRNs={grn}")
            if confirm:
                res = delete_transfer(tid, db)
                db.commit()
                print(f"  CANCELLED: {res}")
            else:
                print(f"  (preview) would restore {pend} in-transit box(es) to source, "
                      f"reverse {grn} GRN(s), and DELETE the transfer.")
        if not confirm:
            db.rollback()
            print("\n(DRY-RUN — nothing written. Re-run with --confirm to cancel.)")
    finally:
        db.close()


if __name__ == "__main__":
    main()
