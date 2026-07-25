"""
Insert the NEVER-DIGITIZED lots from the 22-Jul Savla/Rishi closing into
cfpl_/cdpl_cold_stocks with SYNTHETIC, MARKED box IDs (user-confirmed 2026-07-24:
the closing xlsx is the real, current stock; add the un-entered lots to the DB).

A lot is "never digitized" iff it is on the closing sheet but appears in NO
box/movement/cold table anywhere in the DB (checked live at run time). These are
real physical stock that was never entered — as opposed to lots whose gap is
explained by in-transit / disposed / job-work movement (those are left alone).

Per never-digitized sheet pile row we insert `Net Qty On Cartons` box rows:
  box_id           = 'RC22JUL-<lot>-<NNNN>'   (unique marker prefix, never collides)
  transaction_no   = 'RECON22JUL'             (marks the batch; easy to find/undo)
  no_of_cartons    = 1
  weight_kg        = sheet Weight KG (per carton)
  total_inventory_kgs = weight_kg  (per-box convention)
  value            = round(sheet Value / cartons, 2)  (per box)
  auto_created_from_inward = FALSE
  all descriptive fields    = the sheet row
Synthetic IDs give correct counts immediately but WON'T match the physical
stickers — when a box is physically handled it must be re-scanned / re-inwarded.
Find/undo the batch any time via  transaction_no='RECON22JUL'  or  box_id LIKE 'RC22JUL-%'.

Idempotent: a lot already carrying RC22JUL boxes is topped up to its target, not
double-inserted; a lot that has since been digitized (now present elsewhere) is skipped.

Run:  python scripts/insert_never_digitized_from_closing.py --dry-run
      python scripts/insert_never_digitized_from_closing.py --execute
      python scripts/insert_never_digitized_from_closing.py --self-check
"""
from __future__ import annotations
import os
import sys
import argparse
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path

import openpyxl
import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
EXCEL_PATH = ROOT / "data" / "Cold Storage Stock Details 22nd July Closing.xlsx"
DSN = os.environ["DATABASE_URL"]

HEADER_ROW = 6  # 1-based; data from row 7
COL = {"inward_dt": 0, "unit": 1, "inward_no": 2, "cold_item_mark": 4, "vakkal": 5,
       "lot_no": 6, "cartons": 7, "weight_kg": 8, "group_name": 10, "item_subgroup": 11,
       "item_mark": 12, "spl_remarks": 13, "item_description": 14, "company": 15,
       "storage_location": 16, "exporter": 17, "last_purchase_rate": 20, "value": 21}

TABLE = {"CFPL": "cfpl_cold_stocks", "CDPL": "cdpl_cold_stocks"}
MARK_TXN = "RECON22JUL"
BOX_PREFIX = "RC22JUL"

# every box/movement/cold table with a string lot column — presence here means the
# lot is digitized somewhere, so it is NOT a never-digitized candidate.
PRESENCE = [
    ("cfpl_boxes_v2", "lot_number"), ("cdpl_boxes_v2", "lot_number"),
    ("cfpl_boxes", "lot_number"), ("cdpl_boxes", "lot_number"),
    ("cfpl_articles", "lot_number"), ("cdpl_articles", "lot_number"),
    ("cfpl_articles_v2", "lot_number"), ("cdpl_articles_v2", "lot_number"),
    ("cfpl_bulk_entry_boxes", "lot_number"), ("cdpl_bulk_entry_boxes", "lot_number"),
    ("interunit_transfer_boxes", "lot_number"), ("interunit_transfer_in_boxes", "lot_number"),
    ("pending_transfer_stock", "lot_no"), ("cold_stock_disposition", "lot_no"),
    ("transfer_box_reconciliation", "lot_no"), ("jb_materialout_lines", "lot_number"),
    ("cold_transfer_inboxes", "lot_no"), ("inner_cold_transfer", "new_lot_number"),
    ("inner_cold_transfer", "old_lot_number"), ("outward_boxes", "lot_number"),
    ("cst_transferout_items", "lot_number"), ("floor_inventory", "lot_number"),
    ("cfpl_cold_stocks", "lot_no"), ("cdpl_cold_stocks", "lot_no"),
]

INSERT_COLUMNS = [
    "inward_dt", "unit", "inward_no", "cold_item_mark", "vakkal", "lot_no",
    "no_of_cartons", "weight_kg", "total_inventory_kgs", "group_name",
    "item_description", "storage_location", "exporter", "last_purchase_rate",
    "created_at", "updated_at", "box_id", "transaction_no", "item_subgroup",
    "item_mark", "value", "inward_transaction_no", "auto_created_from_inward",
    "spl_remarks",
]


# ---------- coercion ----------
def _s(v):
    if v is None:
        return None
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    if isinstance(v, (datetime, date)):
        return v.date().isoformat() if isinstance(v, datetime) else v.isoformat()
    s = str(v).strip()
    return s or None


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _date(v):
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        for f in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(v.strip(), f).date()
            except ValueError:
                pass
    return None


def synth_box_id(lot, seq):
    return f"{BOX_PREFIX}-{lot}-{seq:04d}"


# ---------- load ----------
def load_sheet_rows():
    """Return list of dict rows (company, lot, cartons, fields...) for the closing."""
    wb = openpyxl.load_workbook(str(EXCEL_PATH), read_only=True, data_only=True)
    ws = wb["Sheet1"]
    out = []
    for r in ws.iter_rows(min_row=HEADER_ROW + 1, values_only=True):
        company = _s(r[COL["company"]])
        lot = _s(r[COL["lot_no"]])
        cartons = _num(r[COL["cartons"]])
        if not company or not lot or not cartons or cartons <= 0:
            continue
        company = company.upper()
        if company not in TABLE:
            continue
        out.append({
            "company": company, "lot_no": lot, "cartons": int(round(cartons)),
            "inward_dt": _date(r[COL["inward_dt"]]), "unit": _s(r[COL["unit"]]),
            "inward_no": _s(r[COL["inward_no"]]), "cold_item_mark": _s(r[COL["cold_item_mark"]]),
            "vakkal": _s(r[COL["vakkal"]]), "weight_kg": _num(r[COL["weight_kg"]]),
            "group_name": _s(r[COL["group_name"]]), "item_subgroup": _s(r[COL["item_subgroup"]]),
            "item_mark": _s(r[COL["item_mark"]]), "spl_remarks": _s(r[COL["spl_remarks"]]),
            "item_description": _s(r[COL["item_description"]]),
            "storage_location": _s(r[COL["storage_location"]]), "exporter": _s(r[COL["exporter"]]),
            "last_purchase_rate": _num(r[COL["last_purchase_rate"]]), "value": _num(r[COL["value"]]),
        })
    return out


def never_digitized_lots(conn, candidates):
    """Given candidate (company, lot) keys, return the subset present in NO table
    other than as a candidate — i.e. digitized nowhere."""
    lots = sorted({lot for _, lot in candidates})
    present = set()
    for tbl, col in PRESENCE:
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT DISTINCT {col}::text FROM {tbl} WHERE {col}::text = ANY(%s)", (lots,))
            for (l,) in cur.fetchall():
                l = _s(l)
                if l:
                    present.add(l)
        except psycopg.Error:
            conn.rollback()  # unknown col/table -> skip cleanly
    return {(co, lot) for (co, lot) in candidates if lot not in present}


def existing_synth_counts(conn):
    """(company, lot) -> count of already-inserted RC22JUL boxes (for idempotent top-up)."""
    out = defaultdict(int)
    for company, tbl in TABLE.items():
        cur = conn.cursor()
        cur.execute(
            f"SELECT lot_no, COUNT(*) FROM {tbl} WHERE box_id LIKE %s GROUP BY lot_no",
            (BOX_PREFIX + "-%",))
        for lot, n in cur.fetchall():
            out[(company, _s(lot))] = n
    return out


# ---------- plan ----------
def build_rows(sheet_rows, nd_keys, already):
    """Return ({company: [row,...]}, skipped). Aggregate each never-digitized lot,
    insert (lot total cartons - already inserted) boxes, sequenced per lot, never
    exceeding the lot total. Descriptive fields come from the lot's largest pile row;
    value is spread per box over the lot total."""
    now = datetime.utcnow()
    lots = {}
    for r in sheet_rows:
        key = (r["company"], r["lot_no"])
        if key not in nd_keys:
            continue
        d = lots.setdefault(key, {"cartons": 0, "value": 0.0, "field": r})
        d["cartons"] += r["cartons"]
        d["value"] += (r["value"] or 0.0)
        if r["cartons"] > d["field"]["cartons"]:
            d["field"] = r

    buckets = {"CFPL": [], "CDPL": []}
    skipped = 0
    for (company, lot), d in lots.items():
        target = d["cartons"]
        have = already.get((company, lot), 0)
        if have >= target:
            skipped += target
            continue
        val_box = round(d["value"] / target, 2) if target else None
        f = d["field"]
        for seq in range(have + 1, target + 1):
            buckets[company].append({
                "inward_dt": f["inward_dt"], "unit": f["unit"], "inward_no": f["inward_no"],
                "cold_item_mark": f["cold_item_mark"], "vakkal": f["vakkal"], "lot_no": lot,
                "no_of_cartons": 1, "weight_kg": f["weight_kg"],
                "total_inventory_kgs": f["weight_kg"], "group_name": f["group_name"],
                "item_description": f["item_description"], "storage_location": f["storage_location"],
                "exporter": f["exporter"], "last_purchase_rate": f["last_purchase_rate"],
                "created_at": now, "updated_at": now, "box_id": synth_box_id(lot, seq),
                "transaction_no": MARK_TXN, "item_subgroup": f["item_subgroup"],
                "item_mark": f["item_mark"], "value": val_box,
                "inward_transaction_no": None, "auto_created_from_inward": False,
                "spl_remarks": f["spl_remarks"],
            })
        skipped += have
    return buckets, skipped


# ---------- execute ----------
def insert_rows(cur, table, rows):
    if not rows:
        return
    cols = ", ".join(INSERT_COLUMNS)
    ph = ", ".join(["%s"] * len(INSERT_COLUMNS))
    cur.executemany(
        f"INSERT INTO {table} ({cols}) VALUES ({ph})",
        [tuple(r[c] for c in INSERT_COLUMNS) for r in rows])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--self-check", action="store_true")
    args = ap.parse_args()
    if args.self_check:
        demo()
        return
    if not (args.dry_run or args.execute):
        print("Specify --dry-run or --execute", file=sys.stderr)
        sys.exit(2)

    print(f"Excel:  {EXCEL_PATH}")
    sheet_rows = load_sheet_rows()
    print(f"  {len(sheet_rows)} sheet pile rows")

    conn = psycopg.connect(DSN, autocommit=False)
    # candidates = sheet lots with ZERO cold_stocks rows (understated-empty)
    cur = conn.cursor()
    cold_lots = set()
    for tbl in TABLE.values():
        cur.execute(f"SELECT DISTINCT lot_no FROM {tbl} WHERE lot_no IS NOT NULL")
        for (l,) in cur.fetchall():
            cold_lots.add(_s(l))
    candidates = {(r["company"], r["lot_no"]) for r in sheet_rows if r["lot_no"] not in cold_lots}
    nd_keys = never_digitized_lots(conn, candidates)
    already = existing_synth_counts(conn)
    print(f"  {len(candidates)} lots with 0 cold_stocks rows -> {len(nd_keys)} never-digitized")

    buckets, skipped = build_rows(sheet_rows, nd_keys, already)
    n_cfpl, n_cdpl = len(buckets["CFPL"]), len(buckets["CDPL"])

    # report
    by_item = defaultdict(int)
    for co in buckets:
        for r in buckets[co]:
            by_item[r["item_description"]] += 1
    print("\n==================== INSERT PROJECTION ====================")
    print(f"  never-digitized lots to insert: {len({(r['lot_no']) for co in buckets for r in buckets[co]})}")
    print(f"  boxes to insert: CFPL={n_cfpl}, CDPL={n_cdpl}, TOTAL={n_cfpl + n_cdpl}")
    print(f"  already-inserted (skipped, idempotent): {skipped}")
    print(f"  box_id scheme: {synth_box_id('<lot>', 1)}  txn={MARK_TXN}")
    print("  top items (boxes | item):")
    for it, n in sorted(by_item.items(), key=lambda x: -x[1])[:12]:
        print(f"    {n:6} | {it}")
    sample = (buckets["CFPL"] or buckets["CDPL"])[:3]
    print("  sample rows:")
    for r in sample:
        print(f"    {r['lot_no']} {r['box_id']} wt={r['weight_kg']} val={r['value']} loc={r['storage_location']} item={r['item_description']}")

    if args.dry_run:
        conn.close()
        print("\nDRY RUN -- nothing written.")
        return

    print("\nEXECUTING in one transaction...")
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM cfpl_cold_stocks")
        b_cfpl = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM cdpl_cold_stocks")
        b_cdpl = cur.fetchone()[0]
        insert_rows(cur, "cfpl_cold_stocks", buckets["CFPL"])
        insert_rows(cur, "cdpl_cold_stocks", buckets["CDPL"])
        cur.execute("SELECT COUNT(*) FROM cfpl_cold_stocks")
        a_cfpl = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM cdpl_cold_stocks")
        a_cdpl = cur.fetchone()[0]
        print(f"  cfpl {b_cfpl} -> {a_cfpl} (+{a_cfpl - b_cfpl}) | cdpl {b_cdpl} -> {a_cdpl} (+{a_cdpl - b_cdpl})")
        conn.commit()
        print("COMMIT OK.")
    except Exception:
        conn.rollback()
        print("ROLLBACK -- error during operation.")
        raise
    finally:
        conn.close()


def demo():
    """Self-check: sequencing tops up past existing synth boxes and never exceeds the
    lot's sheet total; box_id format is unique/marked."""
    sheet = [
        {"company": "CFPL", "lot_no": "L1", "cartons": 3, "value": 300, "weight_kg": 10,
         "inward_dt": None, "unit": None, "inward_no": None, "cold_item_mark": None,
         "vakkal": None, "group_name": None, "item_subgroup": None, "item_mark": None,
         "spl_remarks": None, "item_description": "X", "storage_location": None, "exporter": None,
         "last_purchase_rate": None},
    ]
    nd = {("CFPL", "L1")}
    # fresh: expect 3 rows seq 0001..0003, value/box = 100
    b, sk = build_rows(sheet, nd, {})
    ids = [r["box_id"] for r in b["CFPL"]]
    assert ids == ["RC22JUL-L1-0001", "RC22JUL-L1-0002", "RC22JUL-L1-0003"], ids
    assert all(r["value"] == 100 for r in b["CFPL"]) and sk == 0
    # idempotent top-up: 2 already there -> insert only 1 more (0003), never exceed total 3
    b2, sk2 = build_rows(sheet, nd, {("CFPL", "L1"): 2})
    assert [r["box_id"] for r in b2["CFPL"]] == ["RC22JUL-L1-0003"], [r["box_id"] for r in b2["CFPL"]]
    # fully present -> nothing
    b3, _ = build_rows(sheet, nd, {("CFPL", "L1"): 3})
    assert b3["CFPL"] == [], b3["CFPL"]
    assert synth_box_id("129520", 7) == "RC22JUL-129520-0007"
    print("self-check OK: per-lot sequencing, idempotent top-up, capped at sheet total, marked IDs.")


if __name__ == "__main__":
    main()
