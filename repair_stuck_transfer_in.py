"""Repair transfer-INs stuck at 'Pending' (transfer-OUT stuck 'Dispatch') even though every
box was acknowledged — the TRANS202606111352 class, where the acknowledged boxes couldn't
reconcile against the parked pending_transfer_stock rows (different ids / null lot).

With the Phase-3 article-count backstop now in pending_stock_tools.pick_from_pending, calling
finalize_transfer_in on these GRNs reconciles by article and flips them to 'Received'.

SCAN (read-only, default):     python repair_stuck_transfer_in.py
REPAIR all stuck non-cold:     python repair_stuck_transfer_in.py --apply
REPAIR one header only:        python repair_stuck_transfer_in.py --apply --only 559

Cold-destination Pending headers (orphans from the cold dual-write) are NEVER touched here —
they belong to the cold module and finalize_transfer_in rejects them by design.
"""
import os
import sys

if not os.environ.get("DATABASE_URL"):
    envp = os.path.join(os.path.dirname(__file__), ".env")
    for line in open(envp, encoding="utf-8"):
        line = line.strip()
        if line.startswith("DATABASE_URL") and "=" in line:
            os.environ["DATABASE_URL"] = line.split("=", 1)[1].strip().strip('"').strip("'")
            break

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from services.ims_service.interunit_tools import finalize_transfer_in, FinalizeTransferIn

APPLY = "--apply" in sys.argv
ONLY = None
if "--only" in sys.argv:
    ONLY = int(sys.argv[sys.argv.index("--only") + 1])

COLD = {"savla d-39", "savla d-514", "rishi", "supreme", "supreme cold", "cold storage"}
def is_cold(w):
    return (w or "").strip().lower() in COLD

DB = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
engine = create_engine(DB)
Session = sessionmaker(bind=engine)
db = Session()

rows = db.execute(text("""
    SELECT tih.id AS grn_id, tih.transfer_out_id, tih.transfer_out_no, tih.grn_number,
           tih.receiving_warehouse,
           (SELECT COUNT(*) FROM interunit_transfer_in_boxes b WHERE b.header_id = tih.id) AS acked,
           (SELECT COUNT(*) FROM pending_transfer_stock p
              WHERE p.transfer_out_id = tih.transfer_out_id AND p.status = 'In Transit'
                AND COALESCE(p.box_id,'') NOT LIKE 'LINE-%') AS in_transit
    FROM interunit_transfer_in_header tih
    WHERE tih.status = 'Pending'
    ORDER BY tih.id
""")).fetchall()

stuck = []
for r in rows:
    m = r._mapping
    cold = is_cold(m["receiving_warehouse"])
    complete = m["acked"] > 0 and m["in_transit"] > 0 and m["acked"] >= m["in_transit"]
    tag = "COLD-skip" if cold else ("STUCK-COMPLETE" if complete else "partial/other")
    if not cold and complete:
        stuck.append(m)
    print(f"  GRN {m['grn_id']:>5} out={m['transfer_out_no']:<20} dest={m['receiving_warehouse']:<14} "
          f"acked={m['acked']:>3} in_transit={m['in_transit']:>3}  [{tag}]")

print(f"\nPending interunit transfer-INs scanned: {len(rows)}")
print(f"Stuck-but-complete (non-cold) to repair: {len(stuck)} -> {[m['grn_id'] for m in stuck]}")

targets = [m for m in stuck if (ONLY is None or m["grn_id"] == ONLY)]
if ONLY is not None:
    print(f"--only {ONLY}: targeting {[m['grn_id'] for m in targets]}")

if not APPLY:
    print("\n(DRY-RUN — nothing written. Re-run with --apply to finalize the above.)")
    sys.exit(0)

print(f"\n=== APPLYING finalize to {len(targets)} GRN(s) ===")
ok, fail = 0, 0
for m in targets:
    try:
        res = finalize_transfer_in(m["grn_id"], FinalizeTransferIn(), db)
        db.commit()
        print(f"  GRN {m['grn_id']} -> status={res.get('status')} remaining_in_transit={res.get('remaining_in_transit')}")
        ok += 1
    except Exception as ex:
        db.rollback()
        print(f"  GRN {m['grn_id']} FAILED: {type(ex).__name__}: {str(ex)[:120]}")
        fail += 1
print(f"\nDone. finalized={ok} failed={fail}")
