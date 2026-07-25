"""
Repair cold_stocks piles whose rows have NULL box_id AND NULL transaction_no — these
trip the Cold-transfer guard because pick-boxes returns NULLs (the frontend Set collapses
multiple NULLs to one, and the boxes cannot be uniquely saved/received anyway).

These rows came from '#N/A' Excel ranges in replace_cold_stocks_from_excel.py where no
boxes_v2 / cold_stocks fallback existed, so a single NULL-identity box row was inserted.

FIX: assign each NULL row a real, unique physical identity:
  - one fresh transaction_no  TR-{inward_dt YYYYMMDD}{HHMMSS}  per pile (unique in the table)
  - box_id = {fresh_base}-{1..N}  where fresh_base is an 8-digit prefix not already used
    as '{base}-%' anywhere in that table.
This makes the pile pickable with N distinct box_ids and keeps (transaction_no, box_id)
globally unique.

USAGE
  Dry-run (default):  python fix_null_boxid_piles.py
  Apply:              python fix_null_boxid_piles.py --apply
"""
import argparse
import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

# (table, item_description, lot_no, inward_no)
PILES = [
    ("cdpl_cold_stocks", "Wet Dates Zahidi Seedless", "127890", "Rishi Cold"),
    ("cfpl_cold_stocks", "KING SOLOMON MEDJOUL JUMBO DATES 500GM", "17066", "TRANS202605221549"),
    ("cfpl_cold_stocks", "Organic Khidri Large dates", "13788", "GR8220"),
]


def fresh_base(c, table, seed):
    """8-digit base not used as '{base}-%' in `table`; deterministic start from seed."""
    base = seed % 100000000
    for _ in range(100000):
        b = str(base).zfill(8)
        hit = c.execute(text(f"SELECT 1 FROM {table} WHERE box_id LIKE :p LIMIT 1"), {"p": f"{b}-%"}).first()
        if not hit:
            return b
        base = (base + 1) % 100000000
    raise RuntimeError("no free base")


def fresh_txn(c, table, inward_dt):
    datepart = inward_dt.strftime("%Y%m%d") if inward_dt else datetime.now().strftime("%Y%m%d")
    for bump in range(86400):
        cand = f"TR-{datepart}{(int(datetime.now().strftime('%H%M%S')) + bump) % 1000000:06d}"
        if not c.execute(text(f"SELECT 1 FROM {table} WHERE transaction_no=:t LIMIT 1"), {"t": cand}).first():
            return cand
    raise RuntimeError("no free txn")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    raw = os.environ["DATABASE_URL"]
    url = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
    engine = create_engine(url)

    with engine.begin() as c:
        seed = 61000000  # arbitrary starting prefix for fresh bases
        for table, item, lot, inward in PILES:
            rows = c.execute(text(f"""
                SELECT id, inward_dt FROM {table}
                WHERE item_description=:i AND CAST(lot_no AS TEXT)=:l AND COALESCE(inward_no,'')=:w
                  AND (box_id IS NULL OR TRIM(box_id)='')
                ORDER BY id ASC
            """), {"i": item, "l": lot, "w": inward}).fetchall()
            print(f"\n-- {table} | {item} | lot {lot} | inward {inward}")
            if not rows:
                print("   (no NULL-box_id rows — already fixed or gone)")
                continue
            inward_dt = rows[0]._mapping["inward_dt"]
            base = fresh_base(c, table, seed)
            seed += 1000
            txn = fresh_txn(c, table, inward_dt)
            ids = [r._mapping["id"] for r in rows]
            print(f"   {len(ids)} NULL rows -> txn {txn}, box_ids {base}-1..{base}-{len(ids)}")
            print(f"   ids: {ids}")

            if args.apply:
                for n, rid in enumerate(ids, start=1):
                    c.execute(text(f"""
                        UPDATE {table} SET box_id=:bid, transaction_no=:txn
                        WHERE id=:id AND (box_id IS NULL OR TRIM(box_id)='')
                    """), {"bid": f"{base}-{n}", "txn": txn, "id": rid})
                chk = c.execute(text(f"""
                    SELECT COUNT(*) AS rows, COUNT(DISTINCT box_id) AS distinct_ids,
                           COUNT(*) FILTER (WHERE box_id IS NULL OR TRIM(box_id)='') AS nulls
                    FROM {table}
                    WHERE item_description=:i AND CAST(lot_no AS TEXT)=:l AND COALESCE(inward_no,'')=:w
                """), {"i": item, "l": lot, "w": inward}).fetchone()._mapping
                ok = chk["rows"] == chk["distinct_ids"] and chk["nulls"] == 0
                print(f"   After: rows={chk['rows']} distinct={chk['distinct_ids']} nulls={chk['nulls']} "
                      f"({'OK' if ok else 'REVIEW'})")

        if not args.apply:
            print("\n(dry-run — no changes written. Re-run with --apply to commit.)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
