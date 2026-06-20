"""De-duplicate lots 9903 & 9917 in cfpl_cold_stocks.

Yesterday's IMS insert added synthetic duplicates of stock already mirrored in cold under a
BLANK lot_no (base 54261171, real txn TR-20260612142059 = bulk lots 9903[471]+9917[29]).

Fix (user-confirmed 2026-06-15):
  1) DELETE my synthetic dupes:  box_id LIKE '40332870-%' (lot 9903, 471) and '40332831-%' (lot 9917, 29).
  2) LABEL the real blank-lot mirror (base 54261171, lot_no NULL, 500 boxes) per bulk mapping:
       54261171-1..471  -> lot 9903
       54261171-472..500 -> lot 9917

Safety: guards on counts + blank-lot precondition; full before-image backup; single txn;
dry-run by default, --apply to commit; connection retry for the flaky RDS.
"""
import os, io, sys, json, time
from datetime import datetime
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB, pool_pre_ping=True)
APPLY = "--apply" in sys.argv
TBL = "cfpl_cold_stocks"

def get_conn(tries=40, delay=5):
    for i in range(tries):
        try:
            cc = engine.connect(); cc.execute(text("SELECT 1")); return cc
        except Exception:
            print(f"  ...db busy, retry {i+1}/{tries}", flush=True); time.sleep(delay)
    raise SystemExit("db unreachable")

def begin(tries=40, delay=5):
    for i in range(tries):
        try:
            return engine.begin()
        except Exception:
            print(f"  ...db busy (begin), retry {i+1}/{tries}", flush=True); time.sleep(delay)
    raise SystemExit("db unreachable")

# ---- preflight (read-only) ----
with get_conn() as c:
    d9903 = c.execute(text(f"SELECT COUNT(*) FROM {TBL} WHERE TRIM(lot_no)='9903' AND box_id LIKE '40332870-%'")).scalar()
    d9917 = c.execute(text(f"SELECT COUNT(*) FROM {TBL} WHERE TRIM(lot_no)='9917' AND box_id LIKE '40332831-%'")).scalar()
    mirror = c.execute(text(f"SELECT COUNT(*), MIN((split_part(box_id,'-',2))::int), MAX((split_part(box_id,'-',2))::int) "
                            f"FROM {TBL} WHERE box_id LIKE '54261171-%' AND lot_no IS NULL")).fetchone()
    m_lot9903 = c.execute(text(f"SELECT COUNT(*) FROM {TBL} WHERE box_id LIKE '54261171-%' AND lot_no IS NULL AND (split_part(box_id,'-',2))::int BETWEEN 1 AND 471")).scalar()
    m_lot9917 = c.execute(text(f"SELECT COUNT(*) FROM {TBL} WHERE box_id LIKE '54261171-%' AND lot_no IS NULL AND (split_part(box_id,'-',2))::int BETWEEN 472 AND 500")).scalar()

print("PREFLIGHT")
print(f"  synthetic dupes to DELETE: 9903={d9903} (expect 471), 9917={d9917} (expect 29)")
print(f"  blank-lot mirror base 54261171: n={mirror[0]} box#[{mirror[1]}..{mirror[2]}] (expect 500, 1..500)")
print(f"  mirror split: ->9903 (#1..471)={m_lot9903} (expect 471), ->9917 (#472..500)={m_lot9917} (expect 29)")

assert d9903 == 471 and d9917 == 29, "ABORT: synthetic dupe counts off — state changed."
assert mirror[0] == 500 and mirror[1] == 1 and mirror[2] == 500, "ABORT: blank-lot mirror not 500 boxes 1..500."
assert m_lot9903 == 471 and m_lot9917 == 29, "ABORT: mirror box# split doesn't match bulk mapping."

if not APPLY:
    print("\nDRY RUN — nothing written. Re-run with --apply to commit.")
    raise SystemExit(0)

stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
bpath = os.path.join(os.path.dirname(__file__), f"fix_dedup_9903_9917_backup_{stamp}.json")
with begin() as c:
    before = {
        "deleted_dupes": [dict(r) for r in c.execute(text(
            f"SELECT id, lot_no, box_id, transaction_no FROM {TBL} WHERE box_id LIKE '40332870-%' OR box_id LIKE '40332831-%'")).mappings()],
        "relabeled_mirror": [dict(r) for r in c.execute(text(
            f"SELECT id, lot_no, box_id, transaction_no FROM {TBL} WHERE box_id LIKE '54261171-%' AND lot_no IS NULL")).mappings()],
    }
    with open(bpath, "w", encoding="utf-8") as f:
        json.dump(before, f, indent=2, default=str)

    # 1) delete synthetic dupes
    dele = c.execute(text(f"DELETE FROM {TBL} WHERE box_id LIKE '40332870-%' OR box_id LIKE '40332831-%'")).rowcount
    # 2) label blank-lot mirror
    u1 = c.execute(text(f"UPDATE {TBL} SET lot_no='9903', updated_at=NOW() WHERE box_id LIKE '54261171-%' AND lot_no IS NULL AND (split_part(box_id,'-',2))::int BETWEEN 1 AND 471")).rowcount
    u2 = c.execute(text(f"UPDATE {TBL} SET lot_no='9917', updated_at=NOW() WHERE box_id LIKE '54261171-%' AND lot_no IS NULL AND (split_part(box_id,'-',2))::int BETWEEN 472 AND 500")).rowcount

    # verify
    f9903 = c.execute(text(f"SELECT COUNT(*), SUM(weight_kg) FROM {TBL} WHERE TRIM(lot_no)='9903'")).fetchone()
    f9917 = c.execute(text(f"SELECT COUNT(*), SUM(weight_kg) FROM {TBL} WHERE TRIM(lot_no)='9917'")).fetchone()
    leftover_blank = c.execute(text(f"SELECT COUNT(*) FROM {TBL} WHERE box_id LIKE '54261171-%' AND lot_no IS NULL")).scalar()
    assert dele == 500, f"ABORT: deleted {dele} != 500"
    assert u1 == 471 and u2 == 29, f"ABORT: relabel {u1}/{u2}"
    assert f9903[0] == 471 and f9917[0] == 29 and leftover_blank == 0, "ABORT: final counts off"
    print(f"  deleted {dele} synthetic dupes")
    print(f"  labeled mirror -> 9903={u1}, 9917={u2}")
    print(f"  FINAL: lot 9903={f9903[0]} boxes/{f9903[1]}kg, lot 9917={f9917[0]} boxes/{f9917[1]}kg, blank-lot 54261171 leftover={leftover_blank}")
    print(f"  backup: {bpath}")
    print("COMMITTED.")
