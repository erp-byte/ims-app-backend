"""Correct CFPL lot 3585 in cfpl_cold_stocks: its transaction_no + box_id bases were
synthetic/wrong. v2 AND bulk agree the real inward is txn TR-20260421123317 (base 55004682).
Cold holds 390 of the original 779 boxes (15 kg each) -> relabel to the source scheme.

Action (user-confirmed 2026-06-14):
  - transaction_no -> TR-20260421123317  (all 390 rows)
  - box_id         -> 55004682-1 .. 55004682-390  (ordered by id; real v2/bulk base)

Safety:
  - GUARD: lot 3585 must currently have exactly 390 rows and 0 already at the target txn.
  - Collision check: target box_ids must not already exist in cfpl_cold_stocks.
  - Full before-image (id, box_id, transaction_no) saved to JSON for rollback.
  - Single transaction. Dry-run by default; --apply to commit.
"""
import os, io, sys, json
from datetime import datetime
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB, pool_pre_ping=True)
APPLY = "--apply" in sys.argv

TABLE = "cfpl_cold_stocks"
LOT = "3585"
NEW_TXN = "TR-20260421123317"
NEW_BASE = "55004682"

with engine.connect() as c:
    rows = c.execute(text(f"SELECT id, box_id, transaction_no FROM {TABLE} WHERE TRIM(lot_no)=:l ORDER BY id"),
                     {"l": LOT}).mappings().all()
    n = len(rows)
    print(f"lot {LOT}: {n} cold rows")
    assert n == 390, f"ABORT: expected 390 rows, found {n} (state changed) — review before applying."
    already = sum(1 for r in rows if r["transaction_no"] == NEW_TXN)
    if already == n:
        print("Already corrected (all at target txn) — nothing to do."); sys.exit(0)
    assert already == 0, f"ABORT: {already} rows already at {NEW_TXN}; partial state, review."

    new_ids = [f"{NEW_BASE}-{i}" for i in range(1, n + 1)]
    # collision: target box_ids must not exist elsewhere in the cold table
    clash = c.execute(text(f"SELECT box_id FROM {TABLE} WHERE box_id = ANY(:b)"), {"b": new_ids}).scalars().all()
    assert not clash, f"ABORT: {len(clash)} target box_ids already exist in {TABLE}: {clash[:5]}"

    print(f"  current txns : {sorted({r['transaction_no'] for r in rows})}")
    print(f"  current bases: {sorted({r['box_id'].rsplit('-',1)[0] for r in rows})}")
    print(f"  -> new txn   : {NEW_TXN}")
    print(f"  -> new box_id: {NEW_BASE}-1 .. {NEW_BASE}-{n}")

if not APPLY:
    print("\nDRY RUN — nothing written. Re-run with --apply to commit.")
    raise SystemExit(0)

stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
bpath = os.path.join(os.path.dirname(__file__), f"fix_3585_backup_{stamp}.json")
with engine.begin() as c:
    rows = c.execute(text(f"SELECT id, box_id, transaction_no FROM {TABLE} WHERE TRIM(lot_no)=:l ORDER BY id"),
                     {"l": LOT}).mappings().all()
    assert len(rows) == 390, "ABORT: row count changed inside txn"
    with open(bpath, "w", encoding="utf-8") as f:
        json.dump({"table": TABLE, "lot": LOT, "before": [dict(r) for r in rows]}, f, indent=2, default=str)

    for i, r in enumerate(rows, start=1):
        c.execute(text(f"UPDATE {TABLE} SET box_id=:bx, transaction_no=:t, updated_at=NOW() WHERE id=:id"),
                  {"bx": f"{NEW_BASE}-{i}", "t": NEW_TXN, "id": r["id"]})

    chk = c.execute(text(f"SELECT COUNT(*), COUNT(DISTINCT box_id), COUNT(DISTINCT transaction_no) "
                         f"FROM {TABLE} WHERE TRIM(lot_no)=:l AND transaction_no=:t AND box_id LIKE :p"),
                    {"l": LOT, "t": NEW_TXN, "p": f"{NEW_BASE}-%"}).fetchone()
    assert chk[0] == 390 and chk[1] == 390 and chk[2] == 1, f"ABORT: post-check failed {tuple(chk)}"
    print(f"Updated 390 rows -> txn {NEW_TXN}, box_id {NEW_BASE}-1..390")
    print(f"Backup (before-image) saved: {bpath}")
    print("COMMITTED.")
