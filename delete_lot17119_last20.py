"""Delete the LAST 20 boxes (by box number) of lot 17119 from cfpl_cold_stocks and
record each in cold_stock_disposition first — per user request 2026-06-11.

Lot 17119 'MEDJOUL LARGE PITTED WET DATES', txn TR-20260421000006, base 29606000,
200 boxes (1..200). Last 20 = 29606000-181 .. 29606000-200 (4kg each).

Mirrors app _write_disposition: disposition_type='manual_correction',
source_table='cfpl_cold_stocks', full row JSON in snapshot_data (recoverable).
Single transaction; backup JSON; asserts exactly 20 rows. Dry-run unless --apply.
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
TXN = "TR-20260421000006"
LOT = "17119"
BASE = "29606000"
COMPANY = "cfpl"
STOCKS_TABLE = "cfpl_cold_stocks"
DISPOSED_BY = "ai.1@candorfoods.in"
NOTES = "Manual delete (last 20 boxes of lot) from cfpl_cold_stocks per user request 2026-06-11"
BOX_IDS = [f"{BASE}-{i}" for i in range(181, 201)]   # 181..200

with engine.begin() as c:
    rows = c.execute(text(f"""
        SELECT * FROM {STOCKS_TABLE}
        WHERE transaction_no=:t AND lot_no=:l AND box_id = ANY(:bx)
        ORDER BY (split_part(box_id,'-',2))::int
    """), {"t": TXN, "l": LOT, "bx": BOX_IDS}).fetchall()
    snaps = [dict(r._mapping) for r in rows]
    print(f"Matched rows to delete: {len(snaps)} (expected 20)")
    if snaps:
        print(f"   range: {snaps[0]['box_id']} .. {snaps[-1]['box_id']}  | "
              f"{snaps[0]['item_description']} | {snaps[0]['weight_kg']}kg each | "
              f"{snaps[0]['storage_location']}/{snaps[0]['unit']}")
        print(f"   total weight: {sum(float(s['weight_kg']) for s in snaps)} kg")
    assert len(snaps) == 20, f"ABORT: expected exactly 20 rows, found {len(snaps)}. No changes."

    if not APPLY:
        print("\nWill: 1) INSERT 20 manual_correction rows into cold_stock_disposition "
              "(full snapshot), 2) DELETE the 20 cfpl_cold_stocks rows.")
        print("DRY RUN — no changes committed. Re-run with --apply to write.")
        raise SystemExit(0)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bpath = os.path.join(os.path.dirname(__file__), f"delete_lot17119_last20_{stamp}.json")
    with open(bpath, "w", encoding="utf-8") as f:
        json.dump({"txn": TXN, "lot": LOT, "deleted_rows": snaps}, f, indent=2, default=str)

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
                (:bid, :txn, :lot, :item, :fc, :unit, :fs, :st,
                 'manual_correction', :st, NULL, :dref_no,
                 :dby, CAST(:snap AS JSONB), :notes)
            RETURNING id
        """), {
            "bid": s["box_id"], "txn": s["transaction_no"], "lot": s["lot_no"],
            "item": s["item_description"], "fc": COMPANY, "unit": s["unit"],
            "fs": s["storage_location"], "st": STOCKS_TABLE, "dref_no": TXN,
            "dby": DISPOSED_BY, "snap": json.dumps(s, default=str), "notes": NOTES,
        }).scalar()
        disp_ids.append(rec_id)
    print(f"Disposition rows written: {len(disp_ids)} (ids {disp_ids[0]}..{disp_ids[-1]})")

    deleted = c.execute(text(f"""
        DELETE FROM {STOCKS_TABLE}
        WHERE transaction_no=:t AND lot_no=:l AND box_id = ANY(:bx)
    """), {"t": TXN, "l": LOT, "bx": BOX_IDS}).rowcount
    print(f"cfpl_cold_stocks rows deleted: {deleted}")
    assert deleted == 20, f"ABORT(rollback): expected to delete 20, deleted {deleted}"

    remaining_total = c.execute(text(f"""
        SELECT COUNT(*) FROM {STOCKS_TABLE} WHERE transaction_no=:t AND lot_no=:l
    """), {"t": TXN, "l": LOT}).scalar()
    print(f"Backup: {bpath}")
    print(f"lot {LOT} boxes remaining (was 200): {remaining_total}  (expected 180)")
    assert remaining_total == 180, f"WARNING: expected 180 remaining, got {remaining_total}"
    print("COMMITTED.")
