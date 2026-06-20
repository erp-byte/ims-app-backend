"""Delete 2 boxes from cdpl_cold_stocks (lot 127024, Egyptian Wet Dates 10Kg, Rishi)
and record each in cold_stock_disposition first — per user request 2026-06-11.

Targets (match on transaction_no + lot_no + box_id; box_id alone is NOT unique):
    txn TR-20260421000002, lot 127024, box_id 29602000-1 and 29602000-2

Mirrors the app's _write_disposition convention (pending_stock_tools.py):
    disposition_type='manual_correction', source_table='cdpl_cold_stocks',
    full row JSON saved in snapshot_data (so the delete is recoverable).

Single transaction; backs up deleted rows to JSON; asserts exactly 2 rows.
Dry-run by default; pass --apply to commit.
"""
import os, io, sys, json
from datetime import datetime
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB)

APPLY = "--apply" in sys.argv
TXN = "TR-20260421000002"
LOT = "127024"
BOX_IDS = ["29602000-1", "29602000-2"]
COMPANY = "cdpl"
STOCKS_TABLE = "cdpl_cold_stocks"
DISPOSED_BY = "ai.1@candorfoods.in"
NOTES = "Manual delete from cdpl_cold_stocks per user request 2026-06-11"

with engine.begin() as c:
    rows = c.execute(text(f"""
        SELECT * FROM {STOCKS_TABLE}
        WHERE transaction_no=:t AND lot_no=:l AND box_id = ANY(:bx)
        ORDER BY box_id
    """), {"t": TXN, "l": LOT, "bx": BOX_IDS}).fetchall()
    snaps = [dict(r._mapping) for r in rows]
    print(f"Matched rows to delete: {len(snaps)} (expected 2)")
    for s in snaps:
        print(f"   id={s['id']}  box={s['box_id']}  lot={s['lot_no']}  {s['item_description']}  "
              f"{s['weight_kg']}kg  {s['storage_location']}/{s['unit']}")
    assert len(snaps) == 2, f"ABORT: expected exactly 2 rows, found {len(snaps)}. No changes."

    # already-recorded check (idempotency): is a manual_correction disposition already present?
    existing = c.execute(text("""
        SELECT box_id FROM cold_stock_disposition
        WHERE transaction_no=:t AND box_id = ANY(:bx) AND disposition_type='manual_correction'
    """), {"t": TXN, "bx": BOX_IDS}).fetchall()
    if existing:
        print(f"NOTE: manual_correction disposition already exists for: {[r[0] for r in existing]}")

    if not APPLY:
        print("\nWill: 1) INSERT 2 manual_correction rows into cold_stock_disposition "
              "(full snapshot), 2) DELETE the 2 cdpl_cold_stocks rows.")
        print("DRY RUN — no changes committed. Re-run with --apply to write.")
        raise SystemExit(0)

    # backup
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bpath = os.path.join(os.path.dirname(__file__), f"delete_lot127024_2boxes_{stamp}.json")
    with open(bpath, "w", encoding="utf-8") as f:
        json.dump({"txn": TXN, "lot": LOT, "deleted_rows": snaps}, f, indent=2, default=str)

    # 1) disposition rows (audit) — mirrors _write_disposition exactly
    disp_ids = []
    for s in snaps:
        rec_id = c.execute(text("""
            INSERT INTO cold_stock_disposition
                (box_id, transaction_no, lot_no, item_description,
                 from_company, unit, from_site, source_table,
                 disposition_type, disposition_ref_table,
                 disposition_ref_id, disposition_ref_no,
                 disposed_by, snapshot_data, notes)
            VALUES
                (:bid, :txn, :lot, :item,
                 :fc, :unit, :fs, :st,
                 'manual_correction', :st,
                 NULL, :dref_no,
                 :dby, CAST(:snap AS JSONB), :notes)
            RETURNING id
        """), {
            "bid": s["box_id"], "txn": s["transaction_no"], "lot": s["lot_no"],
            "item": s["item_description"], "fc": COMPANY, "unit": s["unit"],
            "fs": s["storage_location"], "st": STOCKS_TABLE, "dref_no": TXN,
            "dby": DISPOSED_BY, "snap": json.dumps(s, default=str), "notes": NOTES,
        }).scalar()
        disp_ids.append(rec_id)
    print(f"Disposition rows written: {disp_ids}")

    # 2) delete the cold rows
    deleted = c.execute(text(f"""
        DELETE FROM {STOCKS_TABLE}
        WHERE transaction_no=:t AND lot_no=:l AND box_id = ANY(:bx)
    """), {"t": TXN, "l": LOT, "bx": BOX_IDS}).rowcount
    print(f"cdpl_cold_stocks rows deleted: {deleted}")
    assert deleted == 2, f"ABORT(rollback): expected to delete 2, deleted {deleted}"

    remaining = c.execute(text(f"""
        SELECT COUNT(*) FROM {STOCKS_TABLE}
        WHERE transaction_no=:t AND lot_no=:l AND box_id = ANY(:bx)
    """), {"t": TXN, "l": LOT, "bx": BOX_IDS}).scalar()
    assert remaining == 0, f"ABORT(rollback): {remaining} target rows still present"
    print(f"Backup: {bpath}")
    print("COMMITTED.")
