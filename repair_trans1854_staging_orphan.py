"""Repair: remove the orphaned interunit transfer-IN staging header left behind by
the cold finalize for TRANS202606161854 (ANJEER 1*2, W202 -> Savla D-39).

The cold receipt is the system of record (cold_transfer_in_headers id=605, 33 boxes,
Received). The interunit_transfer_in_header id=605 (Pending, 99 scratch boxes) is the
orphaned scan/staging header that finalize_cold_transfer_in failed to purge.

DEFAULT = DRY RUN (read-only): evaluates all guards and prints exactly what WOULD be
deleted. NO writes. Pass --apply to actually delete (transactional, auto-rollback on any
failed guard/post-verify, idempotent). NO stock is touched either way — cold stock was
created by the cold finalize from the cold boxes; the interunit staging boxes are pure
scratch (their pending rows were already consumed).

    python repair_trans1854_staging_orphan.py            # dry run, read-only
    python repair_trans1854_staging_orphan.py --apply    # perform the delete
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
CHALLAN = "TRANS202606161854"
APPLY = "--apply" in sys.argv


def gather_and_check(c):
    """Read-only: returns (out_id, stg_id, cold_id, cold_boxes, stg_boxes) or raises SystemExit."""
    out = c.execute(text(
        "SELECT id, status FROM interunit_transfers_header WHERE challan_no = :ch"
    ), {"ch": CHALLAN}).fetchone()
    if not out:
        print(f"ABORT: transfer-out {CHALLAN} not found"); raise SystemExit(1)
    out_id = out._mapping["id"]
    print(f"OUT header id={out_id} status={out._mapping['status']!r}")

    cold = c.execute(text(
        "SELECT id, status FROM cold_transfer_in_headers WHERE transfer_out_id = :oid"
    ), {"oid": out_id}).fetchall()
    staging = c.execute(text(
        "SELECT id, status FROM interunit_transfer_in_header WHERE transfer_out_id = :oid"
    ), {"oid": out_id}).fetchall()

    if not staging:
        print("Nothing to do: no interunit staging header for this transfer (already clean).")
        raise SystemExit(0)
    if len(cold) != 1:
        print(f"ABORT: expected exactly 1 cold header, found {len(cold)}: "
              f"{[(r._mapping['id'], r._mapping['status']) for r in cold]} — manual review")
        raise SystemExit(1)
    if len(staging) != 1:
        print(f"ABORT: expected exactly 1 interunit staging header, found {len(staging)} — manual review")
        raise SystemExit(1)

    cold_id, cold_status = cold[0]._mapping["id"], cold[0]._mapping["status"]
    stg_id, stg_status = staging[0]._mapping["id"], staging[0]._mapping["status"]
    cold_boxes = c.execute(text("SELECT COUNT(*) FROM cold_transfer_inboxes WHERE header_id=:h"),
                           {"h": cold_id}).scalar()
    stg_boxes = c.execute(text("SELECT COUNT(*) FROM interunit_transfer_in_boxes WHERE header_id=:h"),
                          {"h": stg_id}).scalar()
    print(f"COLD header id={cold_id} status={cold_status!r} boxes={cold_boxes}   <-- KEEP (system of record)")
    print(f"STAGING (interunit) header id={stg_id} status={stg_status!r} boxes={stg_boxes}   <-- ORPHAN")

    if cold_id != stg_id:
        print(f"ABORT: cold id ({cold_id}) != staging id ({stg_id}); not the shared-id "
              f"finalize signature — manual review")
        raise SystemExit(1)
    if cold_status != "Received":
        print(f"ABORT: cold receipt is {cold_status!r}, not 'Received' — receive may be "
              f"in progress; refusing to purge staging")
        raise SystemExit(1)
    if cold_boxes < 1:
        print("ABORT: cold header has 0 boxes — would lose data; manual review")
        raise SystemExit(1)
    return out_id, stg_id, cold_id, cold_boxes, stg_boxes


# ── DRY RUN (read-only) ──────────────────────────────────────────────
if not APPLY:
    with e.connect() as c:
        out_id, stg_id, cold_id, cold_boxes, stg_boxes = gather_and_check(c)
        sample = c.execute(text(
            "SELECT box_id, transaction_no FROM interunit_transfer_in_boxes "
            "WHERE header_id = :h ORDER BY box_id LIMIT 5"), {"h": stg_id}).fetchall()
    print("\n=== DRY RUN — all guards passed, NO changes made ===")
    print(f"WOULD DELETE: interunit_transfer_in_boxes  WHERE header_id={stg_id}   ({stg_boxes} rows)")
    print(f"WOULD DELETE: interunit_transfer_in_header WHERE id={stg_id}          (1 row)")
    print(f"WOULD KEEP:   cold_transfer_in_headers id={cold_id} + {cold_boxes} cold boxes (+ all stock)")
    print("sample of staging boxes that would be removed:")
    for r in sample:
        print(f"    box_id={r._mapping['box_id']!r} txn={r._mapping['transaction_no']!r}")
    print("\nRe-run with --apply to perform the delete.")
    raise SystemExit(0)


# ── APPLY (transactional write) ──────────────────────────────────────
with e.begin() as c:
    out_id, stg_id, cold_id, cold_boxes, stg_boxes = gather_and_check(c)
    del_boxes = c.execute(text(
        "DELETE FROM interunit_transfer_in_boxes WHERE header_id = :h RETURNING id"
    ), {"h": stg_id}).fetchall()
    del_hdr = c.execute(text(
        "DELETE FROM interunit_transfer_in_header WHERE id = :h AND transfer_out_id = :oid RETURNING id"
    ), {"h": stg_id, "oid": out_id}).fetchall()
    print(f"\nDELETED interunit_transfer_in_boxes rows: {len(del_boxes)}")
    print(f"DELETED interunit_transfer_in_header rows: {len(del_hdr)}")

    still_staging = c.execute(text(
        "SELECT COUNT(*) FROM interunit_transfer_in_header WHERE id = :h"), {"h": stg_id}).scalar()
    cold_still = c.execute(text(
        "SELECT COUNT(*) FROM cold_transfer_in_headers WHERE id = :h"), {"h": cold_id}).scalar()
    cold_boxes_still = c.execute(text(
        "SELECT COUNT(*) FROM cold_transfer_inboxes WHERE header_id = :h"), {"h": cold_id}).scalar()
    print(f"\nVERIFY: interunit staging header remaining = {still_staging} (want 0)")
    print(f"VERIFY: cold header intact = {cold_still} (want 1), cold boxes = {cold_boxes_still} (want {cold_boxes})")
    if still_staging != 0 or cold_still != 1 or cold_boxes_still != cold_boxes:
        print("ABORT: post-verify failed — rolling back")
        raise SystemExit(1)
    print("\nOK: orphan purged, cold receipt intact. Committing.")
