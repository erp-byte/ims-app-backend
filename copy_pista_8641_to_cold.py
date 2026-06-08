"""Copy lot 8641 'california pista inshell' boxes from cfpl_bulk_entry_boxes -> cfpl_cold_stocks.

User decisions (2026-06-08): unit = Savla D-39 (unit='D-39', storage_location='Savla');
COPY ONLY (source bulk_entry rows left untouched — boxes will count in both tables).

Mapping: box_id, transaction_no -> same; lot_number->lot_no; article_description->item_description;
net_weight->weight_kg & total_inventory_kgs; no_of_cartons=1; unit='D-39'; storage_location='Savla';
inward_transaction_no->same; created/updated=NOW(); auto_created_from_inward=FALSE.
Classification fields (group/subgroup/item_mark/exporter/vakkal/rate/value/inward_dt) left NULL
(no source data). canonical_* are trigger-maintained.

Safety: idempotent (NOT EXISTS on box_id+transaction_no), asserts source count, dry-run default,
records inserted ids to JSON for rollback. Pass --apply to commit.
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
LOT, ITEM, UNIT, LOC = "8641", "california pista inshell", "D-39", "Savla"
EXPECT = 15

SRC_WHERE = "TRIM(b.lot_number) = :lot AND b.article_description ILIKE :item"
PARAMS = {"lot": LOT, "item": ITEM}

with engine.begin() as c:
    src = c.execute(text(f"""
        SELECT b.box_id, b.transaction_no, b.lot_number, b.article_description, b.net_weight
        FROM cfpl_bulk_entry_boxes b WHERE {SRC_WHERE} ORDER BY b.box_id
    """), PARAMS).fetchall()
    print(f"Source bulk_entry rows (lot {LOT}, {ITEM}): {len(src)}")
    assert len(src) == EXPECT, f"ABORT: expected {EXPECT} source rows, got {len(src)}"

    # Collision check: do any of these box_ids already exist in cold_stocks (any lot)?
    coll = c.execute(text("""
        SELECT cs.box_id, cs.transaction_no, cs.lot_no, cs.item_description
        FROM cfpl_cold_stocks cs
        WHERE (cs.box_id, cs.transaction_no) IN (
            SELECT b.box_id, b.transaction_no FROM cfpl_bulk_entry_boxes b WHERE """ + SRC_WHERE + """
        )
    """), PARAMS).fetchall()
    print(f"Already present in cold_stocks (idempotent skip): {len(coll)}")
    for r in coll:
        print("   exists:", dict(r._mapping))
    to_insert = len(src) - len(coll)
    print(f"Will INSERT: {to_insert} new cold_stocks rows  (unit={UNIT}, storage_location={LOC})")

    if to_insert == 0:
        print("Nothing to insert — already copied."); sys.exit(0)

    if not APPLY:
        print("\nDRY RUN — no rows inserted. Re-run with --apply to write.")
        for r in src[:3]:
            print("   would insert:", dict(r._mapping))
        raise SystemExit(0)

    ins = c.execute(text(f"""
        INSERT INTO cfpl_cold_stocks
            (box_id, transaction_no, lot_no, item_description, weight_kg, total_inventory_kgs,
             no_of_cartons, unit, storage_location, inward_transaction_no,
             auto_created_from_inward, created_at, updated_at)
        SELECT b.box_id, b.transaction_no, b.lot_number, b.article_description,
               b.net_weight, b.net_weight, 1, :unit, :loc, b.inward_transaction_no,
               FALSE, NOW(), NOW()
        FROM cfpl_bulk_entry_boxes b
        WHERE {SRC_WHERE}
          AND NOT EXISTS (
              SELECT 1 FROM cfpl_cold_stocks cs
              WHERE cs.box_id = b.box_id AND cs.transaction_no = b.transaction_no
          )
        RETURNING id, box_id, lot_no, unit
    """), {**PARAMS, "unit": UNIT, "loc": LOC}).fetchall()

    inserted = [dict(r._mapping) for r in ins]
    print(f"INSERTED {len(inserted)} rows into cfpl_cold_stocks.")
    assert len(inserted) == to_insert, f"ABORT: expected {to_insert}, inserted {len(inserted)}"

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    rec = os.path.join(os.path.dirname(__file__), f"copy_pista_8641_inserted_{stamp}.json")
    with open(rec, "w", encoding="utf-8") as f:
        json.dump({"inserted_ids": [r["id"] for r in inserted], "rows": inserted}, f, default=str, indent=2)
    print(f"Rollback record (delete these ids to undo): {rec}")
    print("\nCOMMITTED.")
