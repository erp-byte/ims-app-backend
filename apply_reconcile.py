"""Apply the corrective reconcile to the LIVE database.

SAFE BY DEFAULT: previews (dry-run) unless you pass --confirm.
Run from the ims-app-backend directory.

  # preview a single transfer (read-only)
  .venv\\Scripts\\python.exe apply_reconcile.py 468

  # APPLY the corruption fix for tid 468 (restore wrong lot 183027/183033, pull ordered lot)
  .venv\\Scripts\\python.exe apply_reconcile.py 468 --confirm

  # preview / apply ALL in-transit transfers (468 fix + shortage flags on the rest)
  .venv\\Scripts\\python.exe apply_reconcile.py --all
  .venv\\Scripts\\python.exe apply_reconcile.py --all --confirm

Reconcile is flag-only for warehouse and corrective for cold (restore wrong-lot/excess to
source, pull the ORDERED lot FIFO, flag genuine shortage). It is receiving-aware (skips
transfers already being received) and reversible via the disposition ledger / restore_to_source.
"""
import sys
from shared.database import SessionLocal
from sqlalchemy import text
from services.ims_service.pending_stock_tools import (
    reconcile_transfer_to_order, backfill_pending_from_existing_transfers,
)


def _pending_by_lot(db, tid):
    rows = db.execute(
        text("SELECT lot_no, COUNT(*) n FROM pending_transfer_stock "
             "WHERE transfer_out_id=:t AND status='In Transit' GROUP BY lot_no ORDER BY n DESC"),
        {"t": tid},
    ).fetchall()
    return [(r.lot_no, r.n) for r in rows]


def main():
    confirm = "--confirm" in sys.argv
    do_all = "--all" in sys.argv
    tids = [int(a) for a in sys.argv[1:] if not a.startswith("--")]
    db = SessionLocal()
    try:
        if do_all:
            rep = backfill_pending_from_existing_transfers(db, dry_run=not confirm)
            print("SUMMARY:", {k: v for k, v in rep.items() if k != "reconciled"})
            changed = [r for r in rep.get("reconciled", [])
                       if r.get("restored_wrong_lot") or r.get("pulled_ordered")
                       or r.get("trimmed_excess") or r.get("unallocated")]
            for r in changed:
                print("  tid", r["transfer_out_id"], "restore_wrong", r.get("restored_wrong_lot", 0),
                      "pull", r.get("pulled_ordered", 0), "trim", r.get("trimmed_excess", 0),
                      "SHORTAGE", r.get("unallocated", 0))
            if confirm:
                print("APPLIED (committed by backfill).")
        else:
            if not tids:
                print("Give a transfer_out_id, e.g.  apply_reconcile.py 468 [--confirm]")
                return
            for tid in tids:
                print(f"\ntid {tid} BEFORE:", _pending_by_lot(db, tid))
                rep = reconcile_transfer_to_order(tid, db, dry_run=not confirm)
                print(f"tid {tid} {'APPLIED' if confirm else 'PLAN'}:",
                      {k: v for k, v in rep.items() if k != "groups"})
                if confirm:
                    db.commit()
                    print(f"tid {tid} AFTER :", _pending_by_lot(db, tid))
        if not confirm:
            db.rollback()
            print("\n(DRY-RUN — nothing written. Re-run with --confirm to apply.)")
    finally:
        db.close()


if __name__ == "__main__":
    main()
