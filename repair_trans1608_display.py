"""DISPLAY repair for TRANS202605151608 (OUT header 665, IN GRN 331).

Bug: the OUT had 130 boxes but only 3 box_ids ({base}-1 repeated per article); IN recorded
only 3 boxes (box_id is the dedupe key). This relabels the 130 OUT box_ids to unique
({base}-{box_number}) and rebuilds the IN box list 3 -> 130 from the relabeled OUT boxes.

SCOPE = display/record only. Does NOT touch cold_stocks / pending_transfer_stock — the
cold-source deduction is intentionally NOT verified/changed (per user decision).

DRY-RUN (default):  python repair_trans1608_display.py
APPLY:              python repair_trans1608_display.py --apply
"""
import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

if not os.environ.get("DATABASE_URL"):
    for line in open(os.path.join(os.path.dirname(__file__), ".env"), encoding="utf-8"):
        line = line.strip()
        if line.startswith("DATABASE_URL") and "=" in line:
            os.environ["DATABASE_URL"] = line.split("=", 1)[1].strip().strip('"').strip("'")
            break

APPLY = "--apply" in sys.argv
OUT_HEADER = 665
IN_HEADER = 331

DB = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
db = sessionmaker(bind=create_engine(DB))()

out_n = db.execute(text("SELECT COUNT(*) FROM interunit_transfer_boxes WHERE header_id=:h"), {"h": OUT_HEADER}).scalar()
out_distinct = db.execute(text("SELECT COUNT(DISTINCT box_id) FROM interunit_transfer_boxes WHERE header_id=:h"), {"h": OUT_HEADER}).scalar()
in_n = db.execute(text("SELECT COUNT(*) FROM interunit_transfer_in_boxes WHERE header_id=:h"), {"h": IN_HEADER}).scalar()
print(f"BEFORE: OUT boxes={out_n} (distinct box_id={out_distinct})   IN boxes={in_n}")

# Preview the new unique OUT box_ids
prev = db.execute(text("""
    SELECT article, box_number, box_id AS old_id,
           split_part(box_id,'-',1) || '-' || box_number AS new_id
    FROM interunit_transfer_boxes WHERE header_id=:h ORDER BY box_number LIMIT 6
"""), {"h": OUT_HEADER}).fetchall()
print("relabel preview (first 6):")
for r in prev:
    m = r._mapping
    print(f"   box#{m['box_number']} {m['old_id']} -> {m['new_id']}  [{m['article']}]")
# collision safety: new ids must all be unique
newdistinct = db.execute(text("""
    SELECT COUNT(*) total, COUNT(DISTINCT split_part(box_id,'-',1) || '-' || box_number) uniq
    FROM interunit_transfer_boxes WHERE header_id=:h
"""), {"h": OUT_HEADER}).fetchone()
print(f"relabel uniqueness check: total={newdistinct._mapping['total']} unique_new_ids={newdistinct._mapping['uniq']}")
assert newdistinct._mapping["total"] == newdistinct._mapping["uniq"], "new box_ids would NOT be unique — abort"

if not APPLY:
    print("\n(DRY-RUN — nothing written. Re-run with --apply.)")
    sys.exit(0)

print("\n=== APPLYING ===")
# 1) relabel OUT box_ids -> unique {base}-{box_number}
u1 = db.execute(text("""
    UPDATE interunit_transfer_boxes
    SET box_id = split_part(box_id,'-',1) || '-' || box_number, updated_at = NOW()
    WHERE header_id = :h
"""), {"h": OUT_HEADER})
# 2) rebuild IN boxes from the (now-unique) OUT boxes
db.execute(text("DELETE FROM interunit_transfer_in_boxes WHERE header_id = :h"), {"h": IN_HEADER})
ins = db.execute(text("""
    INSERT INTO interunit_transfer_in_boxes
        (header_id, transfer_out_box_id, box_id, article, batch_number, lot_number,
         transaction_no, net_weight, gross_weight, scanned_at, is_matched, created_at, updated_at)
    SELECT :inh, otb.id, otb.box_id, otb.article, otb.batch_number, otb.lot_number,
           otb.transaction_no, otb.net_weight, otb.gross_weight, NOW(), true, NOW(), NOW()
    FROM interunit_transfer_boxes otb WHERE otb.header_id = :outh
"""), {"inh": IN_HEADER, "outh": OUT_HEADER})
db.commit()

out_distinct2 = db.execute(text("SELECT COUNT(DISTINCT box_id) FROM interunit_transfer_boxes WHERE header_id=:h"), {"h": OUT_HEADER}).scalar()
in_n2 = db.execute(text("SELECT COUNT(*) FROM interunit_transfer_in_boxes WHERE header_id=:h"), {"h": IN_HEADER}).scalar()
print(f"AFTER:  OUT distinct box_id={out_distinct2} (was {out_distinct})   IN boxes={in_n2} (was {in_n})")
print("Done. (cold_stocks / pending NOT touched — display/record repair only.)")
