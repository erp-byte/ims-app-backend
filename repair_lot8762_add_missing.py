"""Add the 103 missing cold-mirror boxes for lot 8762 (dried cranberry sliced).

Root cause (2026-06-08): cfpl_bulk_entry_articles id 56 has quantity_units=1403 (true box
count) but box_count=1300 (stale). The bulk->cold mirror wrote box_count (1300) rows, so
cfpl_cold_stocks has only 1300 of the 1403 real boxes. The missing 103 (box_number 1301-1403,
bases 92909xxx/92910417) exist in cfpl_bulk_entry_boxes with their real box_ids. Cold's 1300
box_ids are CORRECT (all match bulk); cold detail fields are uniform for the lot.

Action: INSERT the 103 missing bulk boxes into cfpl_cold_stocks — real box_id + weight from
bulk; uniform cold detail fields copied from an existing cold row of this lot; value = weight*rate.
Idempotent (NOT EXISTS). Records inserted ids for rollback. Dry-run default; --apply to commit.
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
LOT, TXN, EXPECT = "8762", "TR-20260603161433", 103
P = {"lot": LOT, "txn": TXN}

with engine.begin() as c:
    bulk = c.execute(text("SELECT COUNT(*) FROM cfpl_bulk_entry_boxes WHERE TRIM(lot_number)=:lot AND transaction_no=:txn"), P).scalar()
    cold = c.execute(text("SELECT COUNT(*) FROM cfpl_cold_stocks WHERE TRIM(lot_no)=:lot AND transaction_no=:txn"), P).scalar()
    missing = c.execute(text("""
        SELECT COUNT(*) FROM cfpl_bulk_entry_boxes b
        WHERE TRIM(b.lot_number)=:lot AND b.transaction_no=:txn
          AND NOT EXISTS (SELECT 1 FROM cfpl_cold_stocks cs WHERE cs.box_id=b.box_id AND cs.transaction_no=b.transaction_no)
    """), P).scalar()
    print(f"bulk={bulk}  cold={cold}  missing(to add)={missing}")
    if missing == 0:
        print("Already complete — nothing to add."); sys.exit(0)
    assert bulk == 1403 and missing == EXPECT, f"ABORT: expected bulk 1403 / missing {EXPECT}, got {bulk}/{missing}"

    tmpl = c.execute(text("""
        SELECT item_description, unit, storage_location, exporter, group_name, item_subgroup,
               item_mark, cold_item_mark, vakkal, last_purchase_rate, inward_dt, inward_no,
               inward_transaction_no, spl_remarks
        FROM cfpl_cold_stocks WHERE TRIM(lot_no)=:lot AND transaction_no=:txn LIMIT 1
    """), P).fetchone()
    print("cold detail template:", dict(tmpl._mapping))

    prev = c.execute(text("""
        SELECT b.box_id, b.net_weight FROM cfpl_bulk_entry_boxes b
        WHERE TRIM(b.lot_number)=:lot AND b.transaction_no=:txn
          AND NOT EXISTS (SELECT 1 FROM cfpl_cold_stocks cs WHERE cs.box_id=b.box_id AND cs.transaction_no=b.transaction_no)
        ORDER BY b.box_number LIMIT 3
    """), P).fetchall()
    print("missing sample:", [dict(r._mapping) for r in prev])

    if not APPLY:
        print(f"\nDRY RUN — would INSERT {missing} rows into cfpl_cold_stocks. Re-run with --apply.")
        raise SystemExit(0)

    ins = c.execute(text("""
        INSERT INTO cfpl_cold_stocks
            (box_id, transaction_no, lot_no, item_description, unit, weight_kg, total_inventory_kgs,
             no_of_cartons, storage_location, exporter, group_name, item_subgroup, item_mark,
             cold_item_mark, vakkal, last_purchase_rate, value, inward_dt, inward_no,
             inward_transaction_no, spl_remarks, auto_created_from_inward, created_at, updated_at)
        SELECT b.box_id, b.transaction_no, :lot, t.item_description, t.unit, b.net_weight, b.net_weight,
               1, t.storage_location, t.exporter, t.group_name, t.item_subgroup, t.item_mark,
               t.cold_item_mark, t.vakkal, t.last_purchase_rate,
               ROUND((b.net_weight * COALESCE(t.last_purchase_rate,0))::numeric, 2),
               t.inward_dt, t.inward_no, t.inward_transaction_no, t.spl_remarks, FALSE, NOW(), NOW()
        FROM cfpl_bulk_entry_boxes b
        CROSS JOIN (SELECT item_description, unit, storage_location, exporter, group_name, item_subgroup,
                           item_mark, cold_item_mark, vakkal, last_purchase_rate, inward_dt, inward_no,
                           inward_transaction_no, spl_remarks
                    FROM cfpl_cold_stocks WHERE TRIM(lot_no)=:lot AND transaction_no=:txn LIMIT 1) t
        WHERE TRIM(b.lot_number)=:lot AND b.transaction_no=:txn
          AND NOT EXISTS (SELECT 1 FROM cfpl_cold_stocks cs WHERE cs.box_id=b.box_id AND cs.transaction_no=b.transaction_no)
        RETURNING id, box_id
    """), P).fetchall()
    inserted = [dict(r._mapping) for r in ins]
    print(f"INSERTED {len(inserted)} rows into cfpl_cold_stocks.")
    assert len(inserted) == EXPECT, f"ABORT: expected {EXPECT}, inserted {len(inserted)}"

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    rec = os.path.join(os.path.dirname(__file__), f"repair_lot8762_inserted_{stamp}.json")
    with open(rec, "w", encoding="utf-8") as f:
        json.dump({"inserted_ids": [r["id"] for r in inserted], "rows": inserted}, f, default=str, indent=2)
    print(f"Rollback record (delete these ids to undo): {rec}")
    print("\nCOMMITTED.")
