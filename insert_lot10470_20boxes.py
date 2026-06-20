"""Insert 20 NEW boxes into cdpl_cold_stocks for lot 10470 (Medjoul), txn
TR-20260314174556 — to bring the system count 27 -> 47 to match the Savla physical count.

Per user (2026-06-11): same transaction_no, all fields identical to the existing 27
(template = box 90556000-21), ONLY box_id is new. box_id format (inward_tools.py logic):
    base = str(int(time.time()*1000))[-8:]   # one fresh base for the batch
    box_id = f"{base}-{box_number}"           # box_number 1..20

Safety:
  - GUARD: refuses unless lot 10470 currently has EXACTLY 27 boxes in this txn
    (prevents double-insert; not idempotent because base changes each run).
  - Collision check on (transaction_no, box_id) before writing.
  - Single transaction; records inserted (id, box_id) to JSON for rollback.
  - Dry-run by default; pass --apply to commit.
"""
import os, io, sys, json, time
from datetime import datetime
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB)

APPLY = "--apply" in sys.argv
TXN = "TR-20260314174556"
LOT = "10470"
TEMPLATE_BOX = "90556000-21"
N_NEW = 20

# Fresh base for the whole batch (mirrors generate_box_ids()).
BASE = str(int(time.time() * 1000))[-8:]
NEW_BOX_IDS = [f"{BASE}-{i}" for i in range(1, N_NEW + 1)]

with engine.begin() as c:
    # GUARD: lot must be at exactly 27 right now.
    cur = c.execute(text("""
        SELECT COUNT(*) FROM cdpl_cold_stocks WHERE transaction_no=:t AND TRIM(lot_no)=:l
    """), {"t": TXN, "l": LOT}).scalar()
    print(f"lot {LOT} current box count in {TXN}: {cur}")
    if cur == 27 + N_NEW:
        print("Already at 47 — nothing to do (looks already inserted)."); sys.exit(0)
    assert cur == 27, f"ABORT: expected 27 existing boxes, found {cur}. Refusing to insert."

    # Load template row.
    tmpl = c.execute(text("""
        SELECT * FROM cdpl_cold_stocks WHERE transaction_no=:t AND box_id=:b
    """), {"t": TXN, "b": TEMPLATE_BOX}).fetchone()
    assert tmpl is not None, f"ABORT: template box {TEMPLATE_BOX} not found"
    tmpl = dict(tmpl._mapping)

    # Columns to insert = everything except auto PK id and box_id (set per-row).
    cols = [k for k in tmpl.keys() if k not in ("id", "box_id")]

    # Collision check on (transaction_no, box_id).
    clash = c.execute(text("""
        SELECT box_id FROM cdpl_cold_stocks WHERE transaction_no=:t AND box_id = ANY(:bx)
    """), {"t": TXN, "bx": NEW_BOX_IDS}).fetchall()
    assert not clash, f"ABORT: box_id collision in txn: {[r[0] for r in clash]}"

    print(f"\nBatch base (last 8 epoch-ms digits): {BASE}")
    print(f"New box_ids: {NEW_BOX_IDS[0]} .. {NEW_BOX_IDS[-1]}  (count {len(NEW_BOX_IDS)})")
    print("\nField values replicated from template box", TEMPLATE_BOX, ":")
    for k in cols:
        print(f"   {k} = {tmpl[k]}")

    if not APPLY:
        print("\nDRY RUN — no rows inserted. Re-run with --apply to commit.")
        raise SystemExit(0)

    # Build a single parameterised INSERT, one row per new box_id.
    collist = ", ".join(cols)
    inserted = []
    for bx in NEW_BOX_IDS:
        params = {k: tmpl[k] for k in cols}
        params["box_id"] = bx
        placeholders = ", ".join(f":{k}" for k in cols)
        new_id = c.execute(text(f"""
            INSERT INTO cdpl_cold_stocks ({collist}, box_id)
            VALUES ({placeholders}, :box_id)
            RETURNING id
        """), params).scalar()
        inserted.append({"id": new_id, "box_id": bx})

    # Rollback record.
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    rec = {"txn": TXN, "lot": LOT, "base": BASE, "template_box": TEMPLATE_BOX,
           "inserted": inserted}
    rpath = os.path.join(os.path.dirname(__file__), f"insert_lot10470_20boxes_{stamp}.json")
    with open(rpath, "w", encoding="utf-8") as f:
        json.dump(rec, f, indent=2, default=str)

    final = c.execute(text("""
        SELECT COUNT(*) FROM cdpl_cold_stocks WHERE transaction_no=:t AND TRIM(lot_no)=:l
    """), {"t": TXN, "l": LOT}).scalar()
    print(f"\nInserted {len(inserted)} rows. Rollback record: {rpath}")
    print(f"lot {LOT} box count now: {final}  (expected 47)")
    assert final == 47, f"WARNING: expected 47, got {final}"
    print("COMMITTED.")
