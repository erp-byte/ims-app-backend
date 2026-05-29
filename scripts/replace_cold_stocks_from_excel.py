"""
Replace cfpl_cold_stocks / cdpl_cold_stocks with rows from
'updated cfpl and cdpl cold data.xlsx'.

Parser rules (locked with user 2026-05-25):
  - Box ID Range may be multi-segment, comma-separated; sum boxes across all segments.
  - Each segment can have its own prefix (e.g. '02402000-1 to 02402000-80, 02403000-1 to 02403000-4').
  - Whitespace inside a range is ignored.
  - Typo '77127595-1 to 77127594-235' -> treat both ends as prefix 77127595.
  - Box ID Range = '#N/A' (broken VLOOKUP) -> look up box_ids by lot_no in current
      cfpl/cdpl_cold_stocks; insert one row per existing box_id. If lot not found in DB,
      insert 1 row with box_id NULL.
  - Truly empty Box ID Range with Company present -> 1 row, box_id NULL.
  - Fully blank rows (no Company) -> skip.
  - Duplicate consumption: if the same lot is looked up via #N/A twice, skip the
      duplicate to avoid double-inserting the same boxes.

cold_item_mark policy: for each Lot No, lookup mode of cold_item_mark from
existing rows in cfpl_cold_stocks UNION cdpl_cold_stocks; apply to all
exploded rows for that lot. Leave NULL if no match.

Other fields:
  inward_transaction_no = NULL
  auto_created_from_inward = FALSE
  created_at = updated_at = Inward Dt
  item_mark = Excel "Item Mark"
"""

from __future__ import annotations
import os
import re
import sys
import argparse
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import openpyxl
import psycopg

EXCEL_PATH = Path(__file__).resolve().parents[1] / "updated cfpl and cdpl cold data.xlsx"
DSN = "postgresql://wmsadmin:Candorfoods@wms-postgres-db.cpis084golp7.ap-south-1.rds.amazonaws.com:5432/warehouse_db"

# Excel column indices (0-based) for sheet "Final Cold Stocks Master"
COL = {
    "inward_dt": 0,
    "unit": 1,
    "inward_no": 2,
    "vakkal": 3,
    "lot_no": 4,
    "no_of_cartons": 5,
    "weight_kg": 6,
    "total_inventory_kgs": 7,
    "group_name": 8,
    "item_subgroup": 9,
    "item_mark": 10,
    "spl_remarks": 11,
    "item_description": 12,
    "company_name": 13,
    "storage_location": 14,
    "exporter": 15,
    "last_purchase_rate": 16,
    "value": 17,
    # 18 Ageing, 19 Ageing Bucket -> derived, skip
    # 20 Box Count -> ignored per user
    "box_id_range": 21,
    "transaction_no": 22,
}

INSERT_COLUMNS = [
    "inward_dt", "unit", "inward_no", "cold_item_mark", "vakkal", "lot_no",
    "no_of_cartons", "weight_kg", "total_inventory_kgs", "group_name",
    "item_description", "storage_location", "exporter", "last_purchase_rate",
    "created_at", "updated_at", "box_id", "transaction_no", "item_subgroup",
    "item_mark", "value", "inward_transaction_no", "auto_created_from_inward",
    "spl_remarks",
]


def _norm(s):
    if s is None:
        return None
    if isinstance(s, str):
        s = s.strip()
        return s if s else None
    return s


def _to_str(s):
    """Coerce to string (e.g. Inward No like 23007 int -> '23007'), trim, None->None."""
    if s is None:
        return None
    if isinstance(s, float) and s.is_integer():
        return str(int(s))
    if isinstance(s, datetime):
        return s.date().isoformat()
    return str(s).strip() or None


def _parse_segment(seg: str, row_idx: int, errors: list) -> list[str]:
    """Parse one segment, returns list of box_ids."""
    seg = re.sub(r"\s+", "", seg)  # nuke all whitespace per user rule
    if not seg:
        return []

    # 'PREFIX-N to PREFIX-N' (whitespace already gone -> 'PREFIX-Nto PREFIX-N' won't match)
    # but since we stripped whitespace, 'to' is glued: 'PREFIX-NtoPREFIX-N'
    m = re.match(r"^([0-9A-Za-z]+)-(\d+)to([0-9A-Za-z]+)-(\d+)$", seg)
    if m:
        p1, n1, p2, n2 = m.group(1), int(m.group(2)), m.group(3), int(m.group(4))
        if p1 != p2:
            # Typo per user rule (77127595 vs 77127594): force to p1
            errors.append(f"Row {row_idx}: prefix mismatch '{p1}' vs '{p2}', using '{p1}'")
        if n1 > n2:
            errors.append(f"Row {row_idx}: reversed range {p1}-{n1} to {p1}-{n2}, skipping")
            return []
        return [f"{p1}-{i}" for i in range(n1, n2 + 1)]

    # Single id 'PREFIX-N'
    m = re.match(r"^([0-9A-Za-z]+)-(\d+)$", seg)
    if m:
        return [f"{m.group(1)}-{m.group(2)}"]

    errors.append(f"Row {row_idx}: unparseable segment '{seg}'")
    return []


def parse_box_range(raw, row_idx: int, errors: list) -> list[str | None]:
    """Returns list of box_ids. Empty range -> [None] (one row with NULL box_id)."""
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return [None]
    if not isinstance(raw, str):
        raw = str(raw)
    segments = [s for s in raw.split(",") if s.strip()]
    box_ids = []
    for seg in segments:
        box_ids.extend(_parse_segment(seg, row_idx, errors))
    if not box_ids:
        # Unparseable -> 1 row with NULL box_id and a logged error
        return [None]
    return box_ids


def _parse_date(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str) and v.strip():
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(v.strip(), fmt).date()
            except ValueError:
                continue
    return None


def _num(v):
    """Coerce to numeric or None."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def load_excel():
    wb = openpyxl.load_workbook(str(EXCEL_PATH), read_only=True, data_only=True)
    ws = wb.active
    return list(ws.iter_rows(min_row=2, values_only=True))


def build_lot_to_mark_map(conn) -> dict[str, str]:
    """Map lot_no -> most common cold_item_mark across both tables."""
    cur = conn.cursor()
    pairs: dict[str, Counter] = defaultdict(Counter)
    for t in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
        cur.execute(
            f"SELECT lot_no, cold_item_mark FROM {t} "
            f"WHERE lot_no IS NOT NULL AND lot_no <> '' "
            f"  AND cold_item_mark IS NOT NULL AND cold_item_mark <> ''"
        )
        for lot, mark in cur.fetchall():
            pairs[str(lot).strip()][mark.strip()] += 1
    return {lot: ctr.most_common(1)[0][0] for lot, ctr in pairs.items()}


def build_lot_to_boxids_map(conn) -> dict[tuple[str, str], list[str]]:
    """Map (company, lot_no) -> list of DISTINCT box_ids from cold_stocks tables.
    Used as a fallback for #N/A Excel rows."""
    cur = conn.cursor()
    out: dict[tuple[str, str], list[str]] = {}
    for company, t in (("CFPL", "cfpl_cold_stocks"), ("CDPL", "cdpl_cold_stocks")):
        cur.execute(
            f"SELECT DISTINCT lot_no, box_id FROM {t} "
            f"WHERE lot_no IS NOT NULL AND lot_no <> '' "
            f"  AND box_id IS NOT NULL AND box_id <> ''"
        )
        for lot, box in cur.fetchall():
            key = (company, str(lot).strip())
            out.setdefault(key, []).append(box.strip())
    return out


def build_lot_to_boxes_v2_map(conn) -> dict[tuple[str, str], list[tuple[str, str]]]:
    """Map (company, lot_no) -> list of (box_id, transaction_no) from boxes_v2.
    This is the PRIORITY source for box_id+txn_no when the lot exists here."""
    cur = conn.cursor()
    out: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for company, t in (("CFPL", "cfpl_boxes_v2"), ("CDPL", "cdpl_boxes_v2")):
        cur.execute(
            f"SELECT DISTINCT lot_number, box_id, transaction_no FROM {t} "
            f"WHERE lot_number IS NOT NULL AND lot_number <> '' "
            f"  AND box_id IS NOT NULL AND box_id <> ''"
        )
        for lot, box, txn in cur.fetchall():
            key = (company, str(lot).strip())
            out.setdefault(key, []).append((box.strip(), (txn or "").strip() or None))
    return out


# User-confirmed manual overrides for typo'd Box ID Range cells.
# Excel row index (1-based, including header) -> corrected range string.
RANGE_OVERRIDES = {
    177: "55189387-5 to 55189387-1004",      # was '55189387-4 , 55189387-1004', cartons=1000
    178: "55189387-1005 to 55189387-1444",   # was reversed '1005 to 436', cartons=440
}


def build_rows(excel_rows, lot_to_mark, lot_to_boxids, lot_to_v2, errors):
    out_cfpl, out_cdpl = [], []
    skipped_blank = 0
    unmatched_lots = set()
    consumed_na_lots: set[tuple[str, str]] = set()  # (company, lot_no)
    na_lookup_stats = {"hit": 0, "miss": 0, "duplicate_skipped": 0}
    v2_hit_count = 0  # number of Excel rows where lot found in boxes_v2

    for idx, r in enumerate(excel_rows, start=2):
        company = _norm(r[COL["company_name"]])
        if not company:
            skipped_blank += 1
            continue

        company = company.upper()
        if company not in ("CFPL", "CDPL"):
            errors.append(f"Row {idx}: unknown Company Name {company!r}, skipped")
            continue

        lot_no = _to_str(r[COL["lot_no"]])
        inward_dt = _parse_date(r[COL["inward_dt"]])
        # created_at/updated_at = Inward Dt; fall back to NOW if missing
        ts = datetime.combine(inward_dt, datetime.min.time()) if inward_dt else datetime.utcnow()

        cold_item_mark = None
        if lot_no:
            cold_item_mark = lot_to_mark.get(lot_no)
            if not cold_item_mark:
                unmatched_lots.add(lot_no)

        raw_range = r[COL["box_id_range"]]
        if idx in RANGE_OVERRIDES:
            errors.append(f"Row {idx}: applying manual override range {RANGE_OVERRIDES[idx]!r} (was {raw_range!r})")
            raw_range = RANGE_OVERRIDES[idx]
        excel_txn_no = _to_str(r[COL["transaction_no"]])
        is_na = isinstance(raw_range, str) and raw_range.strip() == "#N/A"

        if is_na:
            # #N/A row: priority chain = boxes_v2 -> cold_stocks -> NULL
            if not lot_no:
                errors.append(f"Row {idx}: '#N/A' range with no lot_no, inserting 1 NULL box row")
                box_pairs = [(None, excel_txn_no)]
            else:
                key = (company, lot_no)
                if key in consumed_na_lots:
                    errors.append(
                        f"Row {idx}: '#N/A' for lot {lot_no} already consumed earlier, skipping to avoid duplicate boxes"
                    )
                    na_lookup_stats["duplicate_skipped"] += 1
                    continue
                consumed_na_lots.add(key)

                # Priority 1: boxes_v2 lookup (uses v2 txn_no, not Excel's)
                v2_pairs = lot_to_v2.get(key)
                if v2_pairs:
                    box_pairs = list(v2_pairs)
                    v2_hit_count += 1
                    na_lookup_stats["hit"] += 1
                else:
                    # Priority 2: cold_stocks lookup (uses Excel's txn_no)
                    found = lot_to_boxids.get(key)
                    if found:
                        box_pairs = [(b, excel_txn_no) for b in found]
                        na_lookup_stats["hit"] += 1
                    else:
                        errors.append(
                            f"Row {idx}: '#N/A' for lot {lot_no} not in boxes_v2 nor {company} cold_stocks, inserting 1 NULL box row"
                        )
                        box_pairs = [(None, excel_txn_no)]
                        na_lookup_stats["miss"] += 1
        else:
            # Excel has explicit box range -> trust Excel
            ids = parse_box_range(raw_range, idx, errors)
            box_pairs = [(b, excel_txn_no) for b in ids]

        # txn_no for base dict (overridden per-row in the emit loop below)
        txn_no = excel_txn_no

        base = {
            "inward_dt": inward_dt,
            "unit": _to_str(r[COL["unit"]]),
            "inward_no": _to_str(r[COL["inward_no"]]),
            "cold_item_mark": cold_item_mark,
            "vakkal": _to_str(r[COL["vakkal"]]),
            "lot_no": lot_no,
            "no_of_cartons": _num(r[COL["no_of_cartons"]]),
            "weight_kg": _num(r[COL["weight_kg"]]),
            "total_inventory_kgs": _num(r[COL["total_inventory_kgs"]]),
            "group_name": _to_str(r[COL["group_name"]]),
            "item_description": _to_str(r[COL["item_description"]]),
            "storage_location": _to_str(r[COL["storage_location"]]),
            "exporter": _to_str(r[COL["exporter"]]),
            "last_purchase_rate": _num(r[COL["last_purchase_rate"]]),
            "created_at": ts,
            "updated_at": ts,
            "transaction_no": txn_no,
            "item_subgroup": _to_str(r[COL["item_subgroup"]]),
            "item_mark": _to_str(r[COL["item_mark"]]),
            "value": _num(r[COL["value"]]),
            "inward_transaction_no": None,
            "auto_created_from_inward": False,
            "spl_remarks": _to_str(r[COL["spl_remarks"]]),
        }

        bucket = out_cfpl if company == "CFPL" else out_cdpl
        for bid, btxn in box_pairs:
            row = dict(base)
            row["box_id"] = bid
            row["transaction_no"] = btxn
            bucket.append(row)

    return out_cfpl, out_cdpl, skipped_blank, unmatched_lots, na_lookup_stats, v2_hit_count


def dedupe_rows(rows: list[dict], table_label: str, errors: list) -> list[dict]:
    """Remove (transaction_no, box_id) collisions where box_id is not NULL.
    The unique index is partial: WHERE box_id IS NOT NULL. NULL box_ids never
    collide. Keeps the row with the MAX no_of_cartons (the 'master'/largest)."""
    # Group by (txn, box_id) keeping only the row with max cartons
    by_key: dict[tuple, dict] = {}
    null_box_rows: list[dict] = []
    collision_keys: set[tuple] = set()

    for r in rows:
        if r["box_id"] is None:
            null_box_rows.append(r)
            continue
        key = (r["transaction_no"], r["box_id"])
        if key in by_key:
            collision_keys.add(key)
            existing = by_key[key]
            cur_cartons = r.get("no_of_cartons") or 0
            ex_cartons = existing.get("no_of_cartons") or 0
            if cur_cartons > ex_cartons:
                by_key[key] = r
        else:
            by_key[key] = r

    if collision_keys:
        # Log one summary line per colliding key (avoid 5000-line walls)
        sample = list(collision_keys)[:10]
        for k in sample:
            winner = by_key[k]
            errors.append(
                f"DEDUPE [{table_label}]: collision on (txn={k[0]!r}, box={k[1]!r}) "
                f"- kept row with cartons={winner.get('no_of_cartons')} for lot {winner.get('lot_no')!r}"
            )
        errors.append(
            f"DEDUPE [{table_label}]: {len(collision_keys)} unique colliding (txn, box) keys; "
            f"kept MAX-cartons winner each"
        )

    return list(by_key.values()) + null_box_rows


def insert_rows(cur, table: str, rows: list[dict]):
    if not rows:
        return
    cols_sql = ", ".join(INSERT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(INSERT_COLUMNS))
    sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"
    values = [tuple(r[c] for c in INSERT_COLUMNS) for r in rows]
    cur.executemany(sql, values)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="parse + report, no DB writes")
    ap.add_argument("--execute", action="store_true", help="DELETE existing rows and INSERT new ones")
    args = ap.parse_args()

    if not args.dry_run and not args.execute:
        print("Specify --dry-run or --execute", file=sys.stderr)
        sys.exit(2)

    print(f"Loading Excel: {EXCEL_PATH}")
    excel_rows = load_excel()
    print(f"  {len(excel_rows)} raw data rows")

    print(f"Connecting to DB: {DSN.split('@')[1]}")
    conn = psycopg.connect(DSN, autocommit=False)

    print("Building lot_no -> cold_item_mark map from existing data...")
    lot_to_mark = build_lot_to_mark_map(conn)
    print(f"  {len(lot_to_mark)} lots have a cold_item_mark")

    print("Building (company, lot_no) -> [box_id] map from cold_stocks (#N/A fallback)...")
    lot_to_boxids = build_lot_to_boxids_map(conn)
    print(f"  {len(lot_to_boxids)} (company, lot) combos with box_ids")

    print("Building (company, lot_no) -> [(box_id, txn_no)] map from boxes_v2 (PRIORITY)...")
    lot_to_v2 = build_lot_to_boxes_v2_map(conn)
    print(f"  {len(lot_to_v2)} (company, lot) combos in boxes_v2")

    errors: list[str] = []
    rows_cfpl, rows_cdpl, skipped, unmatched, na_stats, v2_hits = build_rows(
        excel_rows, lot_to_mark, lot_to_boxids, lot_to_v2, errors
    )

    rows_cfpl = dedupe_rows(rows_cfpl, "CFPL", errors)
    rows_cdpl = dedupe_rows(rows_cdpl, "CDPL", errors)

    print()
    print("=== PROJECTION ===")
    print(f"  CFPL rows to insert: {len(rows_cfpl)}")
    print(f"  CDPL rows to insert: {len(rows_cdpl)}")
    print(f"  Total:               {len(rows_cfpl) + len(rows_cdpl)}")
    print(f"  Excel rows skipped (no Company): {skipped}")
    print(f"  Unmatched lots (cold_item_mark=NULL): {len(unmatched)}")
    print(f"  #N/A lookups: hit={na_stats['hit']}, miss={na_stats['miss']}, duplicate_skipped={na_stats['duplicate_skipped']}")
    print(f"  boxes_v2 priority lookups: {v2_hits} Excel rows resolved via boxes_v2")
    print(f"  Parser/dedupe warnings: {len(errors)}")
    for e in errors[:50]:
        print(f"    - {e}")
    if len(errors) > 50:
        print(f"    ... (+{len(errors)-50} more)")
    if unmatched:
        sample = sorted(unmatched)[:20]
        print(f"  Sample unmatched lots: {sample}")

    if args.dry_run:
        conn.close()
        print()
        print("DRY RUN — no changes.")
        return

    # ---- DESTRUCTIVE ----
    print()
    print("EXECUTING REPLACE...")
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM cfpl_cold_stocks")
        before_cfpl = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM cdpl_cold_stocks")
        before_cdpl = cur.fetchone()[0]
        print(f"  Before: cfpl={before_cfpl}, cdpl={before_cdpl}")

        cur.execute("DELETE FROM cfpl_cold_stocks")
        cur.execute("DELETE FROM cdpl_cold_stocks")
        print("  DELETE done.")

        insert_rows(cur, "cfpl_cold_stocks", rows_cfpl)
        insert_rows(cur, "cdpl_cold_stocks", rows_cdpl)

        cur.execute("SELECT COUNT(*) FROM cfpl_cold_stocks")
        after_cfpl = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM cdpl_cold_stocks")
        after_cdpl = cur.fetchone()[0]
        print(f"  After (in-transaction): cfpl={after_cfpl}, cdpl={after_cdpl}")

        conn.commit()
        print("COMMIT OK.")
    except Exception:
        conn.rollback()
        print("ROLLBACK — error during operation.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
