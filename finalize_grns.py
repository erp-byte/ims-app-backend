"""Finalize the acknowledged-but-not-finalized transfer-IN backlog on the LIVE database.

SAFE BY DEFAULT: previews unless you pass --confirm. Run from ims-app-backend.

  .venv\\Scripts\\python.exe finalize_grns.py            # dry-run (read-only preview)
  .venv\\Scripts\\python.exe finalize_grns.py --confirm   # finalize the complete GRNs

Background:
  A transfer-IN (GRN) used to be created and its boxes acknowledged, but `finalize` was
  never called -- so the GRN header stayed 'Pending', the transfer-OUT never became
  'Received', pending_transfer_stock rows were never picked, and the dispatch lingered in
  the Pending modal as "Partial (GRN raised)". This sweep finalizes every Pending GRN whose
  acknowledged boxes already cover its in-transit set (acked >= in_transit > 0). It uses the
  same finalize_transfer_in path as the UI, so picking + status transitions are identical.
  GRNs that are NOT yet complete are listed and left untouched.
"""
import sys
from shared.database import SessionLocal
from services.ims_service.interunit_tools import finalize_complete_pending_grns


def main():
    confirm = "--confirm" in sys.argv
    db = SessionLocal()
    try:
        summary = finalize_complete_pending_grns(db, dry_run=not confirm)
        print(f"Pending GRNs scanned: {summary['pending_grns_scanned']}")
        print(f"\nComplete (acked >= in_transit) -> "
              f"{'FINALIZED' if confirm else 'WOULD FINALIZE'}: {len(summary['finalized'])}")
        for r in summary["finalized"]:
            print(f"  GRN {r['grn_id']} | {r['grn_number']} | transfer_out={r['transfer_out_id']} "
                  f"| acked={r['acked']} in_transit={r['in_transit']}")
        print(f"\nIncomplete (left as Pending): {len(summary['skipped'])}")
        for r in summary["skipped"]:
            print(f"  GRN {r['grn_id']} | {r['grn_number']} | transfer_out={r['transfer_out_id']} "
                  f"| acked={r['acked']} in_transit={r['in_transit']}")
        if not confirm:
            db.rollback()
            print("\n(DRY-RUN -- nothing written. Re-run with --confirm to finalize.)")
        else:
            # finalize_complete_pending_grns commits per GRN; nothing left to commit.
            print("\nDone. Finalized GRNs are now 'Received'; their dispatches leave the Pending modal.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
