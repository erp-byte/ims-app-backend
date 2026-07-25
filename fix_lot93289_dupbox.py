"""
Repair the cross-transaction box_id collision blocking Cold-transfer for
'10KG AL BARAKAH DATE POWDER V2', lot 93289, inward GR6534 (cdpl_cold_stocks).

PROBLEM (same class as lot 125859)
----------------------------------
Two transactions both minted box_id base '90512000':
  TR-20260314174512 : 114 boxes  90512000-1 .. 90512000-114
  TR-20260314174513 :  77 boxes  90512000-1 .. 90512000-77   <-- collides on 1..77
pick-boxes groups by (item, lot, inward) and returns all 191, so 90512000-1..77 each
appear twice -> frontend aborts with "Duplicate Box IDs From Source".

FIX
---
Renumber the SMALLER transaction's (TR-20260314174513) 77 boxes from suffix N to N+114,
i.e. 90512000-1..77  ->  90512000-115..191. The larger txn keeps 1..114, so the pile's
191 box_ids become fully distinct. Pure identity relabel; no carton/weight change. The
new range 115..191 is disjoint from every existing suffix in this pile, so no UNIQUE
(transaction_no, box_id) violation at any point.

USAGE
  Dry-run (default):  python fix_lot93289_dupbox.py
  Apply:              python fix_lot93289_dupbox.py --apply
"""
import argparse
import os
import sys
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

TABLE = "cdpl_cold_stocks"
ITEM = "10KG AL BARAKAH DATE POWDER V2"
LOT = "93289"
INWARD = "GR6534"
BASE = "90512000"
SMALL_TXN = "TR-20260314174513"   # the 77-box txn to renumber
OFFSET = 114                       # larger txn occupies 1..114


def pile_stats(c):
    return c.execute(text(f"""
        SELECT COUNT(*) AS rows, COUNT(DISTINCT box_id) AS distinct_ids
        FROM {TABLE}
        WHERE item_description=:i AND CAST(lot_no AS TEXT)=:l AND COALESCE(inward_no,'')=:w
    """), {"i": ITEM, "l": LOT, "w": INWARD}).fetchone()._mapping


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    raw = os.environ["DATABASE_URL"]
    url = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
    engine = create_engine(url)

    with engine.begin() as c:
        before = pile_stats(c)
        print(f"Before: rows={before['rows']} distinct_box_ids={before['distinct_ids']}")

        # target rows in the smaller txn
        rows = c.execute(text(f"""
            SELECT id, box_id, CAST(SPLIT_PART(box_id,'-',2) AS INT) AS suffix
            FROM {TABLE}
            WHERE item_description=:i AND CAST(lot_no AS TEXT)=:l AND COALESCE(inward_no,'')=:w
              AND transaction_no=:t
            ORDER BY suffix ASC
        """), {"i": ITEM, "l": LOT, "w": INWARD, "t": SMALL_TXN}).fetchall()
        if not rows:
            print(f"ABORT: no rows for {SMALL_TXN}.")
            return 1

        suffixes = [r._mapping["suffix"] for r in rows]
        print(f"{SMALL_TXN}: {len(rows)} boxes, suffixes {min(suffixes)}..{max(suffixes)}")
        if max(suffixes) > OFFSET:
            print(f"ABORT: a suffix ({max(suffixes)}) already exceeds offset {OFFSET} — would risk overlap. Re-check.")
            return 1

        # sanity: none of the target NEW box_ids exist yet in this pile
        new_ids = [f"{BASE}-{s + OFFSET}" for s in suffixes]
        exists = c.execute(text(f"""
            SELECT COUNT(*) FROM {TABLE}
            WHERE item_description=:i AND CAST(lot_no AS TEXT)=:l AND COALESCE(inward_no,'')=:w
              AND box_id = ANY(:ids)
        """), {"i": ITEM, "l": LOT, "w": INWARD, "ids": new_ids}).scalar()
        if exists:
            print(f"ABORT: {exists} of the target new box_ids already exist in the pile. Pick a different offset.")
            return 1

        print(f"Planned: {SMALL_TXN} box_ids {BASE}-{min(suffixes)}..{max(suffixes)} "
              f"-> {BASE}-{min(suffixes)+OFFSET}..{max(suffixes)+OFFSET}  ({len(rows)} rows)")

        if args.apply:
            # single arithmetic UPDATE; new range disjoint from old -> no mid-statement clash
            c.execute(text(f"""
                UPDATE {TABLE}
                SET box_id = :base || '-' || (CAST(SPLIT_PART(box_id,'-',2) AS INT) + :off)
                WHERE item_description=:i AND CAST(lot_no AS TEXT)=:l AND COALESCE(inward_no,'')=:w
                  AND transaction_no=:t
            """), {"base": BASE, "off": OFFSET, "i": ITEM, "l": LOT, "w": INWARD, "t": SMALL_TXN})
            after = pile_stats(c)
            ok = after["rows"] == after["distinct_ids"]
            print(f"After: rows={after['rows']} distinct_box_ids={after['distinct_ids']} ({'OK' if ok else 'STILL BROKEN'})")
            print("[OK] COMMITTED." if ok else "WARNING: pile still not distinct — review.")
        else:
            print("\n(dry-run — no changes written. Re-run with --apply to commit.)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
