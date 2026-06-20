"""
Backfill / repair script for transfer TRANS202605131331 (header_id=653).

CONTEXT
-------
Due to a frontend bug in directtransferform/page.tsx (the cold-storage pickBoxes
call's silent fallback to a single article.cs_box_id), 711 physical boxes were
saved into interunit_transfer_boxes carrying only 3 distinct box_id values:
    - AL BARAKAH FARD DATES STANDARD C 10KG  →  700 rows, all box_id 90675000-1000
    - AL BARAKAH FARD DATES STANDARD A 10KG  →    9 rows, all box_id 90677000-1
    - Wet Dates Khaneizi                     →    2 rows, all box_id 90593000-1

When Transfer-In acknowledged these, ON CONFLICT (header_id, box_id) DO UPDATE
collapsed each group to 1 row in interunit_transfer_in_boxes and only 3 boxes
landed in the destination warehouse cold-stocks → net inventory loss of 708
physical boxes.

WHAT THIS SCRIPT DOES
---------------------
For each item in transfer 653:
  1. Look up the source rows in cdpl_cold_stocks for that (item_description, lot_no,
     transaction_no) — FIFO by id ASC.
  2. Take the FIRST N source rows where N = current number of duplicate transfer rows.
  3. Re-stamp interunit_transfer_boxes.box_id of the duplicates with those unique
     source box_ids in order (sorted by interunit_transfer_boxes.box_number).

WHAT THIS SCRIPT DOES *NOT* DO (intentionally — these need separate handling)
----------------------------------------------------------------------------
  • Does not touch interunit_transfer_in_boxes (only 3 rows survive there;
    you need to re-run the Transfer-In acknowledge flow after this fix, OR
    a follow-up script needs to expand those 3 rows into 711 rows mirroring the
    repaired Transfer-Out).
  • Does not touch destination cold-stocks (only 3 boxes were inserted at W202;
    after fixing Transfer-In, finalize logic / a separate adjustment will insert
    the missing 708).
  • Does not delete from source cdpl_cold_stocks. The source rows we are
    "borrowing" box_ids from may have already been consumed by a *correctly*
    processed transfer-out; in that case this script will surface the conflict.

USAGE
-----
Dry-run (default — prints what would change, makes NO writes):
    python fix_TRANS202605131331_duplicate_box_ids.py

Apply (writes changes inside a transaction; rolls back on any error):
    python fix_TRANS202605131331_duplicate_box_ids.py --apply

Inspect a single item only:
    python fix_TRANS202605131331_duplicate_box_ids.py --article "AL BARAKAH FARD DATES STANDARD C 10KG"
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import List

import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass


HEADER_ID = 653
CHALLAN_NO = "TRANS202605131331"
SOURCE_TABLE = "cdpl_cold_stocks"  # source warehouse "Cold Storage" → cdpl per investigation


@dataclass
class ItemGroup:
    article: str
    lot_no: str
    transaction_no: str
    duplicate_box_id: str
    rows: List[tuple]  # (transfer_box.id, box_number)


def fetch_duplicate_groups(cur) -> List[ItemGroup]:
    """Return groups of transfer_box rows that share the same box_id within this transfer."""
    cur.execute(
        """
        SELECT article, lot_number, transaction_no, box_id, id, box_number
        FROM interunit_transfer_boxes
        WHERE header_id = %s
        ORDER BY article, transaction_no, box_id, box_number
        """,
        (HEADER_ID,),
    )
    rows = cur.fetchall()
    groups: dict[tuple, ItemGroup] = {}
    for r in rows:
        key = (r["article"], r["lot_number"], r["transaction_no"], r["box_id"])
        if key not in groups:
            groups[key] = ItemGroup(
                article=r["article"],
                lot_no=r["lot_number"],
                transaction_no=r["transaction_no"],
                duplicate_box_id=r["box_id"],
                rows=[],
            )
        groups[key].rows.append((r["id"], r["box_number"]))
    return [g for g in groups.values() if len(g.rows) > 1]


def fetch_source_box_ids(cur, group: ItemGroup, need: int) -> List[str]:
    """FIFO-pull `need` source box_ids from cdpl_cold_stocks for this item/lot/transaction."""
    cur.execute(
        f"""
        SELECT box_id
        FROM {SOURCE_TABLE}
        WHERE item_description = %s
          AND CAST(lot_no AS TEXT) = %s
          AND transaction_no = %s
        ORDER BY id ASC
        LIMIT %s
        """,
        (group.article, group.lot_no, group.transaction_no, need),
    )
    return [r["box_id"] for r in cur.fetchall()]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Actually write changes (default: dry-run)")
    parser.add_argument("--article", type=str, default=None, help="Only process this article")
    args = parser.parse_args()

    url = os.environ.get("DATABASE_URL", "").replace("postgresql+psycopg://", "postgresql://")
    if not url:
        print("ERROR: DATABASE_URL not set", file=sys.stderr)
        return 2

    conn = psycopg2.connect(url)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=RealDictCursor)

    print(f"Mode: {'APPLY (will write)' if args.apply else 'DRY-RUN (no writes)'}")
    print(f"Target: header_id={HEADER_ID}  challan_no={CHALLAN_NO}")
    print(f"Source: {SOURCE_TABLE}\n")

    groups = fetch_duplicate_groups(cur)
    if args.article:
        groups = [g for g in groups if g.article == args.article]

    if not groups:
        print("No duplicate groups found — nothing to do.")
        conn.close()
        return 0

    total_rows_to_fix = 0
    blockers: List[str] = []

    for g in groups:
        need = len(g.rows)
        source_ids = fetch_source_box_ids(cur, g, need)
        print(f"-- {g.article}")
        print(f"   lot={g.lot_no}  tx={g.transaction_no}  duplicate_box_id={g.duplicate_box_id}")
        print(f"   rows to fix: {need}    source rows available: {len(source_ids)}")

        if len(source_ids) < need:
            msg = (
                f"   BLOCKER: only {len(source_ids)} source rows in {SOURCE_TABLE} for this "
                f"(item, lot, tx) — cannot supply {need} unique box_ids."
            )
            print(msg)
            blockers.append(f"{g.article}: {msg.strip()}")
            continue

        if len(set(source_ids)) != need:
            msg = (
                f"   BLOCKER: source pull returned duplicate box_ids — investigate "
                f"{SOURCE_TABLE} integrity."
            )
            print(msg)
            blockers.append(f"{g.article}: {msg.strip()}")
            continue

        # Pair the existing transfer_box rows (sorted by box_number) with the source box_ids in order
        plan = list(zip(g.rows, source_ids))
        print(f"   sample mapping (first 3):")
        for (row_id, box_number), new_box_id in plan[:3]:
            print(f"     transfer_box.id={row_id}  box_number={box_number}  ->  box_id={new_box_id}")
        if len(plan) > 3:
            print(f"     ... and {len(plan) - 3} more")

        if args.apply:
            for (row_id, _bn), new_box_id in plan:
                cur.execute(
                    "UPDATE interunit_transfer_boxes SET box_id = %s, updated_at = NOW() WHERE id = %s",
                    (new_box_id, row_id),
                )
            print(f"   [OK] updated {len(plan)} rows")
        total_rows_to_fix += need
        print()

    print("----------------------------------------------")
    print(f"Total rows {'updated' if args.apply else 'would be updated'}: {total_rows_to_fix}")
    if blockers:
        print(f"\nBLOCKERS ({len(blockers)}):")
        for b in blockers:
            print(f"  - {b}")
        print("\nNot committing due to blockers." if args.apply else "")
        conn.rollback()
        conn.close()
        return 1

    if args.apply:
        conn.commit()
        print("\n[OK] COMMITTED.")
        print("\nNEXT STEPS (manual):")
        print("  1. Verify: SELECT box_id, COUNT(*) FROM interunit_transfer_boxes WHERE header_id=%d GROUP BY box_id HAVING COUNT(*)>1; -- should return 0 rows" % HEADER_ID)
        print("  2. The receive-side (interunit_transfer_in_boxes) still has only 3 rows.")
        print("     Decide whether to: (a) re-open Transfer-In and re-acknowledge all 711 boxes,")
        print("     or (b) bulk-insert the missing 708 rows + corresponding cold-stocks entries")
        print("     via a follow-up reconciliation script.")
    else:
        conn.rollback()
        print("\n(dry-run - no changes written. Re-run with --apply to commit.)")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
