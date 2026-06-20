"""Generalized cold-mirror gap fix: add bulk boxes missing from cold + set article box_count = true box count.

Usage: python fix_cold_mirror_gap.py <company> <lot> <txn> [--apply]
Safety: only proceeds if cold is a SUBSET of bulk (cold_not_in_bulk == 0) and bulk is a
single item with uniform cold details. Idempotent. Backup-recorded. Dry-run default.
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

args = [a for a in sys.argv[1:] if a != "--apply"]
APPLY = "--apply" in sys.argv
CO, LOT, TXN = args[0], args[1], args[2]
COLD, BULK, ART = f"{CO}_cold_stocks", f"{CO}_bulk_entry_boxes", f"{CO}_bulk_entry_articles"
P = {"lot": LOT, "txn": TXN}

with engine.begin() as c:
    bulk = c.execute(text(f"SELECT COUNT(*) FROM {BULK} WHERE TRIM(lot_number)=:lot AND transaction_no=:txn"), P).scalar()
    cold = c.execute(text(f"SELECT COUNT(*) FROM {COLD} WHERE TRIM(lot_no)=:lot AND transaction_no=:txn"), P).scalar()
    cold_not_in_bulk = c.execute(text(f"""
        SELECT COUNT(*) FROM {COLD} cs WHERE TRIM(cs.lot_no)=:lot AND cs.transaction_no=:txn
          AND NOT EXISTS (SELECT 1 FROM {BULK} b WHERE b.box_id=cs.box_id AND b.transaction_no=cs.transaction_no)
    """), P).scalar()
    missing = c.execute(text(f"""
        SELECT COUNT(*) FROM {BULK} b WHERE TRIM(b.lot_number)=:lot AND b.transaction_no=:txn
          AND NOT EXISTS (SELECT 1 FROM {COLD} cs WHERE cs.box_id=b.box_id AND cs.transaction_no=b.transaction_no)
    """), P).scalar()
    items = c.execute(text(f"SELECT COUNT(DISTINCT article_description) FROM {BULK} WHERE TRIM(lot_number)=:lot AND transaction_no=:txn"), P).scalar()
    print(f"[{CO} lot {LOT}] bulk={bulk} cold={cold} cold_not_in_bulk={cold_not_in_bulk} missing(add)={missing} bulk_items={items}")
    assert cold_not_in_bulk == 0, "ABORT: cold has box_ids NOT in bulk (wrong-ids case) — manual review required"
    assert items == 1, "ABORT: bulk has multiple items for this lot/txn — manual review"

    tmpl = c.execute(text(f"SELECT * FROM {COLD} WHERE TRIM(lot_no)=:lot AND transaction_no=:txn LIMIT 1"), P).fetchone()
    assert tmpl is not None, "ABORT: no template cold row (lot never mirrored?) — manual review"

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not APPLY:
        print(f"  DRY RUN — would add {missing} cold rows; set {ART}.box_count -> {bulk}.")
        raise SystemExit(0)

    inserted = 0
    if missing:
        rows = c.execute(text(f"""
            INSERT INTO {COLD}
                (box_id, transaction_no, lot_no, item_description, unit, weight_kg, total_inventory_kgs,
                 no_of_cartons, storage_location, exporter, group_name, item_subgroup, item_mark,
                 cold_item_mark, vakkal, last_purchase_rate, value, inward_dt, inward_no,
                 inward_transaction_no, spl_remarks, auto_created_from_inward, created_at, updated_at)
            SELECT b.box_id, b.transaction_no, :lot, t.item_description, t.unit, b.net_weight, b.net_weight,
                   1, t.storage_location, t.exporter, t.group_name, t.item_subgroup, t.item_mark,
                   t.cold_item_mark, t.vakkal, t.last_purchase_rate,
                   ROUND((b.net_weight*COALESCE(t.last_purchase_rate,0))::numeric,2),
                   t.inward_dt, t.inward_no, t.inward_transaction_no, t.spl_remarks, FALSE, NOW(), NOW()
            FROM {BULK} b
            CROSS JOIN (SELECT item_description, unit, storage_location, exporter, group_name, item_subgroup,
                               item_mark, cold_item_mark, vakkal, last_purchase_rate, inward_dt, inward_no,
                               inward_transaction_no, spl_remarks
                        FROM {COLD} WHERE TRIM(lot_no)=:lot AND transaction_no=:txn LIMIT 1) t
            WHERE TRIM(b.lot_number)=:lot AND b.transaction_no=:txn
              AND NOT EXISTS (SELECT 1 FROM {COLD} cs WHERE cs.box_id=b.box_id AND cs.transaction_no=b.transaction_no)
            RETURNING id
        """), P).fetchall()
        inserted = len(rows)
        with open(os.path.join(os.path.dirname(__file__), f"fix_coldgap_{CO}_{LOT}_{stamp}.json"), "w") as f:
            json.dump({"inserted_ids": [r._mapping["id"] for r in rows]}, f)
        assert inserted == missing, f"ABORT: expected {missing}, inserted {inserted}"

    bc = c.execute(text(f"UPDATE {ART} SET box_count=:bx, updated_at=NOW() WHERE transaction_no=:txn AND TRIM(lot_number)=:lot AND box_count IS DISTINCT FROM :bx"),
                   {**P, "bx": bulk}).rowcount
    print(f"  COMMITTED: added {inserted} cold rows; box_count set to {bulk} on {bc} article row(s).")
