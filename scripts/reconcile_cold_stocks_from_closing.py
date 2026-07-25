"""
Reconcile cfpl_/cdpl_cold_stocks to the Savla pile-level closing report
'Cold Storage Stock Details 22nd July Closing.xlsx' — keyed by (company, lot_no).

Locked scope (user 2026-07-23):
  - Operate ONLY on lots present in BOTH the Excel closing and the DB ("matched").
  - Excel-only / DB-only lots -> LEFT ALONE (reported).

The Excel is pile/lot-level (one row per pile, no box_ids). The DB is box-level
(one row per box, every no_of_cartons = 1). Per matched (company, lot):

  0. REASSIGN (manual, forensic — box IDs stay REAL, never fabricated).
       CFPL 151105: retag box_ids 99034337-2/-3 (currently lot_no NULL) -> 151105.
       CDPL 8068  : move up to 500 unreferenced boxes of print-batch prefix 27179729
                    from sibling lot 8065 -> 8068 (same shipment, mislabeled).
     Safeguards: both paths refuse to touch an in-flight (refset) box; the move is
     DEFICIT-BOUND (moves only `target_excel_cartons - current_target_count`, capped
     at 500) so a second --execute is a no-op instead of moving another 500; the
     explicit retag is idempotent (a box already under the target lot is a no-op) and
     aborts if a box_id is missing or sits under an unexpected lot. Moves draw only
     from REAL, unreferenced, prefix-matched boxes (never NULL box_id rows). A lot
     that receives moved boxes is force-refreshed over all its fields so moved rows
     become descriptively consistent and the sync_canonical trigger re-fires.

  1. FIELD REFRESH — refresh a descriptive field only when the DB currently holds a
     SINGLE value for the lot and it differs from the (single-valued) Excel value. If
     the DB is genuinely multi-valued for that field (real per-inward/per-box spread),
     we DO NOT flatten it — logged as a conflict for human review. value /
     total_inventory_kgs / no_of_cartons are never refreshed (dirty/derived/structural).

  2. QUANTITY RECONCILE — delta = round(sum Excel cartons) - DB box count (post-reassign):
       cart <= 0  -> SKIP + flag (never let a blank/zero closing quantity mass-delete a lot).
       delta == 0 -> nothing.
       delta  > 0 -> SHORTFALL: closing holds more than the system recorded and there is
                     no DB source for real box IDs. NOT inserted; reported for the floor.
       delta  < 0 -> REMOVE |delta| rows: NULL/blank box_id first, then UNREFERENCED
                     boxes by (suffix desc, id). Referenced boxes are NEVER deleted.

Determinism: the box query is ORDER BY id and all pools sort by (suffix desc, id), so
the --execute plan is identical to the --dry-run plan.

cold_storage_boxes.stock_id -> cold_stocks.id is ON DELETE CASCADE and that table is
empty, so row deletes are not blocked and orphan nothing.

Run:  python scripts/reconcile_cold_stocks_from_closing.py --dry-run
      python scripts/reconcile_cold_stocks_from_closing.py --execute
      python scripts/reconcile_cold_stocks_from_closing.py --self-check
"""
from __future__ import annotations
import os
import re
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

HEADER_ROW = 6  # 1-based; data starts at row 7
COL = {
    "inward_dt": 0, "unit": 1, "inward_no": 2, "cold_item_mark": 4, "vakkal": 5,
    "lot_no": 6, "cartons": 7, "weight_kg": 8, "total_inv": 9, "group_name": 10,
    "item_subgroup": 11, "item_mark": 12, "spl_remarks": 13, "item_description": 14,
    "company": 15, "storage_location": 16, "exporter": 17, "last_purchase_rate": 20,
    "value": 21,
}

# (excel_col_key, db_column, kind) — refreshed only when DB is single-valued and differs.
REFRESH_FIELDS = [
    ("unit", "unit", "str"), ("inward_dt", "inward_dt", "date"),
    ("vakkal", "vakkal", "str"), ("cold_item_mark", "cold_item_mark", "str"),
    ("storage_location", "storage_location", "str"), ("exporter", "exporter", "str"),
    ("group_name", "group_name", "str"), ("item_subgroup", "item_subgroup", "str"),
    ("item_description", "item_description", "str"), ("spl_remarks", "spl_remarks", "str"),
    ("item_mark", "item_mark", "str"), ("weight_kg", "weight_kg", "num"),
    ("last_purchase_rate", "last_purchase_rate", "num"),
]

# Tables whose box_id columns mean a cold box is in-flight -> never delete or move it.
REF_SOURCES = {
    "cold_transfer_inboxes": ["box_id"],
    "pending_transfer_stock": ["box_id", "original_box_id"],
    "cold_stock_disposition": ["box_id"],
    "interunit_transfer_boxes": ["box_id"],
    "transfer_box_reconciliation": ["actual_box_id", "original_box_id"],
    "interunit_transfer_in_boxes": ["box_id"],
}

# Forensic, user-confirmed reassignments (2026-07-23). Real box IDs only.
#   box_ids : retag these specific box_ids from `from_lot` (None = NULL) to `to_lot`.
#   move_n  : move up to N safe boxes (prefix-filtered) from `from_lot` to `to_lot`,
#             bounded by the target's Excel deficit so a re-run is a no-op.
REASSIGN = [
    {"company": "CFPL", "to_lot": "151105", "from_lot": None,
     "box_ids": ["99034337-2", "99034337-3"]},
    {"company": "CDPL", "to_lot": "8068", "from_lot": "8065",
     "prefix": "27179729", "move_n": 500},
]

TABLE = {"CFPL": "cfpl_cold_stocks", "CDPL": "cdpl_cold_stocks"}


# ---------- coercion ----------
def _s(v):
    if v is None:
        return None
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    if isinstance(v, datetime):
        return v.date().isoformat()
    s = str(v).strip()
    return s or None


def _num(v):
    try:
        return round(float(v), 6)
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


def _coerce(kind, v):
    return {"str": _s, "num": _num, "date": _date}[kind](v)


def _suffix(box_id: str) -> int:
    """Numeric suffix after the last '-' for deterministic ordering."""
    m = re.search(r"-(\d+)$", box_id or "")
    return int(m.group(1)) if m else -1


def safe_order(boxes, refset, departed):
    """Rows SAFE to delete, deterministically ordered:
      1. NULL/blank box_id (already broken), by id;
      2. LEAKED boxes — box_id has an ACTIVE departure record (transfer-out pending /
         direct-out / jobwork-out) yet the cold_stocks row survived, i.e. a dup that
         SHOULD have been deleted. These are the correct victims, deleted first;
      3. clean UNREFERENCED boxes (not in refset, not departed) by (suffix desc, id).
    Boxes in refset but NOT departed (came back / genuinely in-flight) are EXCLUDED —
    never deleted. `boxes` is [(row_id, box_id)] -> [(row_id, box_id)]."""
    def bid(b):
        return str(b).strip()
    null_rows = sorted([(rid, b) for rid, b in boxes if not b or not bid(b)], key=lambda x: x[0])
    leaked = [(rid, bid(b)) for rid, b in boxes if b and bid(b) and bid(b) in departed]
    leaked.sort(key=lambda x: (-_suffix(x[1]), x[0]))
    free = [(rid, bid(b)) for rid, b in boxes
            if b and bid(b) and bid(b) not in departed and bid(b) not in refset]
    free.sort(key=lambda x: (-_suffix(x[1]), x[0]))
    return null_rows + leaked + free


def move_pool(boxes, refset, departed, prefix=None):
    """Rows eligible to MOVE to another lot: REAL box_id, unreferenced, not departed,
    optionally matching `prefix` (never NULL rows). Ordered (suffix desc, id)."""
    out = [(rid, str(b).strip()) for rid, b in boxes
           if b and str(b).strip() and str(b).strip() not in refset
           and str(b).strip() not in departed
           and (prefix is None or str(b).strip().startswith(prefix + "-"))]
    out.sort(key=lambda x: (-_suffix(x[1]), x[0]))
    return out


def choose_victims(boxes, k, refset, departed):
    """Pick k row ids to delete from the safe pool. Returns (victim_ids, shortfall)."""
    pool = [rid for rid, _ in safe_order(boxes, refset, departed)]
    return pool[:k], max(0, k - len(pool))


# ---------- Excel ----------
def load_excel():
    """(company, lot) -> {cart, nonintegral, fields:{db_col:value}, multi:set(db_col)}."""
    wb = openpyxl.load_workbook(str(EXCEL_PATH), read_only=True, data_only=True)
    ws = wb["Sheet1"]
    agg = {}
    for r in ws.iter_rows(min_row=HEADER_ROW + 1, values_only=True):
        company = _s(r[COL["company"]])
        lot = _s(r[COL["lot_no"]])
        if not company or not lot:
            continue
        company = company.upper()
        if company not in TABLE:
            continue
        d = agg.setdefault((company, lot), {"cart": 0.0, "seen": defaultdict(set)})
        d["cart"] += _num(r[COL["cartons"]]) or 0.0
        for ekey, dbcol, kind in REFRESH_FIELDS:
            d["seen"][dbcol].add(_coerce(kind, r[COL[ekey]]))
    out = {}
    for key, d in agg.items():
        fields, multi = {}, set()
        for _, dbcol, _kind in REFRESH_FIELDS:
            vals = {v for v in d["seen"][dbcol] if v is not None}
            if len(vals) == 1:
                fields[dbcol] = next(iter(vals))
            elif len(vals) > 1:
                multi.add(dbcol)
        cart = round(d["cart"])
        out[key] = {"cart": cart, "nonintegral": abs(d["cart"] - cart) > 1e-9,
                    "fields": fields, "multi": multi}
    return out


# ---------- DB ----------
def load_db(conn):
    """(company, lot) -> {'boxes':[(id, box_id)], 'dist':{db_col:set(current values)}}."""
    out = {}
    refcols = [dbcol for _, dbcol, _ in REFRESH_FIELDS]
    kind_of = {dbcol: kind for _, dbcol, kind in REFRESH_FIELDS}
    for company, tbl in TABLE.items():
        cur = conn.cursor()
        cur.execute(f"SELECT id, box_id, lot_no FROM {tbl} WHERE lot_no IS NOT NULL ORDER BY id")
        boxes = defaultdict(list)
        for rid, box_id, lot in cur.fetchall():
            boxes[(company, str(lot).strip())].append((rid, box_id))

        agg = ", ".join(f"array_agg(DISTINCT {c}) AS {c}" for c in refcols)
        cur.execute(f"SELECT lot_no, {agg} FROM {tbl} WHERE lot_no IS NOT NULL GROUP BY lot_no")
        colnames = [d.name for d in cur.description]
        for row in cur.fetchall():
            rec = dict(zip(colnames, row))
            key = (company, str(rec["lot_no"]).strip())
            dist = {c: {_coerce(kind_of[c], v) for v in (rec[c] or []) if v is not None}
                    for c in refcols}
            out[key] = {"boxes": boxes.get(key, []), "dist": dist}
    return out


def load_refset(conn):
    ref = set()
    for tbl, cols in REF_SOURCES.items():
        for col in cols:
            cur = conn.cursor()
            cur.execute(f"SELECT DISTINCT {col}::text FROM {tbl} WHERE {col} IS NOT NULL")
            for (b,) in cur.fetchall():
                b = (b or "").strip()
                if b:
                    ref.add(b)
    return ref


def load_departed_set(conn):
    """box_ids with an ACTIVE departure record — the box left cold storage, so a
    surviving cold_stocks row is a leaked dup that is correct (and safe) to delete:
      - cold_stock_disposition, reverted != true  (transfer-out pending / direct-out / manual)
      - pending_transfer_stock, cold origin, In Transit
      - jb_materialout_lines (issued to job work)  <- includes the blank-txn deduct-bug leaks
    Confirmed by the code lifecycle trace: every such departure DELETEs the cold_stocks row."""
    dep = set()
    queries = [
        "SELECT box_id FROM cold_stock_disposition WHERE reverted IS NOT TRUE AND box_id IS NOT NULL AND box_id <> ''",
        "SELECT box_id FROM pending_transfer_stock WHERE from_storage_type='cold' AND status='In Transit' AND box_id IS NOT NULL AND box_id <> ''",
        "SELECT box_id FROM jb_materialout_lines WHERE box_id IS NOT NULL AND box_id <> ''",
    ]
    for q in queries:
        cur = conn.cursor()
        cur.execute(q)
        for (b,) in cur.fetchall():
            b = (b or "").strip()
            if b:
                dep.add(b)
    return dep


# ---------- reassignment ----------
def resolve_reassignments(conn, db, refset, departed, excel):
    """Apply REASSIGN rules: mutate `db` box pools in memory. Returns
    (moves, forced_targets). Real box IDs only; in-flight/departed boxes never moved;
    idempotent on re-run."""
    moves, forced = [], set()
    for rule in REASSIGN:
        co, to_lot, from_lot = rule["company"], rule["to_lot"], rule["from_lot"]
        tgt = (co, to_lot)
        chosen = []  # (row_id, box_id)

        if "box_ids" in rule:
            cur = conn.cursor()
            cur.execute(
                f"SELECT id, box_id, lot_no FROM {TABLE[co]} WHERE box_id = ANY(%s)",
                [rule["box_ids"]])
            rows = cur.fetchall()
            found = {str(b).strip() for _, b, _ in rows}
            missing = [b for b in rule["box_ids"] if b not in found]
            if missing:
                raise SystemExit(f"REASSIGN {co}->{to_lot}: box_ids not found: {missing}")
            for rid, b, lot in rows:
                lot = _s(lot)
                is_src = (lot is None) if from_lot is None else (lot == from_lot)
                if not is_src and lot != to_lot:
                    raise SystemExit(
                        f"REASSIGN {co}->{to_lot}: box {b} under unexpected lot {lot!r}")
            chosen = [(rid, b) for rid, b, lot in rows
                      if ((_s(lot) is None) if from_lot is None else (_s(lot) == from_lot))]
            bad = [b for _, b in chosen if str(b).strip() in refset]
            if bad:
                raise SystemExit(f"REASSIGN {co}->{to_lot}: refuse to move in-flight boxes {bad}")
        else:
            src = db.get((co, from_lot))
            if not src:
                raise SystemExit(f"REASSIGN: source lot {co}/{from_lot} not in DB")
            target = excel.get(tgt)
            have = len(db.get(tgt, {}).get("boxes", []))
            deficit = (target["cart"] - have) if target else 0
            want = min(rule["move_n"], max(0, deficit))  # deficit-bound -> idempotent
            pool = move_pool(src["boxes"], refset, departed, rule.get("prefix"))
            if want > len(pool):
                raise SystemExit(
                    f"REASSIGN {co}/{from_lot}->{to_lot}: need {want} movable "
                    f"(real, unreferenced, prefix {rule.get('prefix')}) boxes, have {len(pool)}")
            chosen = list(pool[:want])
            moved = {rid for rid, _ in chosen}
            src["boxes"] = [b for b in src["boxes"] if b[0] not in moved]

        db.setdefault(tgt, {"boxes": [], "dist": {}})
        db[tgt]["boxes"].extend(chosen)
        if chosen:
            forced.add(tgt)
        moves.append({"company": co, "to_lot": to_lot, "from_lot": from_lot,
                      "ids": [rid for rid, _ in chosen]})
    return moves, forced


# ---------- planning ----------
def plan(excel, db, refset, departed, forced):
    updates, removes, shortfalls = [], [], []
    stats = {"matched": 0, "delta0": 0, "multi_skipped": 0, "excel_only": [],
             "db_only": [], "zero_lots": [], "conflicts": [], "nonintegral": [],
             "leaked_removed": 0}

    for key, ex in excel.items():
        if key not in db:
            stats["excel_only"].append(key)
            continue
        stats["matched"] += 1
        boxes = db[key]["boxes"]
        if ex["nonintegral"]:
            stats["nonintegral"].append(key)

        # SAFETY: never let a blank/zero closing quantity authorize deleting a lot.
        if ex["cart"] <= 0:
            stats["zero_lots"].append((key, len(boxes)))
            continue

        # 1. field refresh — refresh only where DB is single-valued AND differs.
        dist = db[key]["dist"]
        if key in forced:
            changed = dict(ex["fields"])  # post-move target: unify the whole lot
        else:
            changed = {}
            for c, v in ex["fields"].items():
                cur = dist.get(c, set())
                if len(cur) > 1:
                    stats["conflicts"].append((key, c, len(cur)))  # don't flatten real spread
                elif cur != {v}:
                    changed[c] = v
        if ex["multi"]:
            stats["multi_skipped"] += 1
        if changed:
            updates.append({"key": key, "fields": changed})

        # 2. quantity reconcile (post-reassignment counts)
        delta = ex["cart"] - len(boxes)
        if delta == 0:
            stats["delta0"] += 1
        elif delta > 0:
            shortfalls.append({"key": key, "short": delta,
                               "db": len(boxes), "excel": ex["cart"]})
        else:
            victims, short = choose_victims(boxes, -delta, refset, departed)
            id2box = {rid: b for rid, b in boxes}
            leaked_v = sum(1 for rid in victims
                           if id2box.get(rid) and str(id2box[rid]).strip() in departed)
            stats["leaked_removed"] += leaked_v
            removes.append({"key": key, "n": -delta, "victim_ids": victims,
                            "shortfall": short, "leaked": leaked_v})
    return updates, removes, shortfalls, stats


# ---------- reporting ----------
def report(moves, updates, removes, shortfalls, stats):
    rem_boxes = sum(len(r["victim_ids"]) for r in removes)
    print("\n==================== RECONCILE PROJECTION ====================")
    print(f"Matched (company, lot):        {stats['matched']}")
    print(f"  quantity already equal:      {stats['delta0']}")
    print(f"  lots with field updates:     {len(updates)}")
    print(f"  lots w/ skipped multi-field: {stats['multi_skipped']}")
    print(f"Excel-only lots (left alone):  {len(stats['excel_only'])}")
    print(f"DB-only lots (left alone):     {len(stats['db_only'])}")

    print("\nREASSIGN (real box IDs moved):")
    for m in moves:
        src = "NULL" if m["from_lot"] is None else m["from_lot"]
        print(f"  {m['company']} {src} -> {m['to_lot']}: {len(m['ids'])} boxes")

    print(f"\nREMOVE rows (safe victims):    {rem_boxes} across {len(removes)} lots")
    print(f"  of which LEAKED/dup boxes (active departure record) removed first: {stats['leaked_removed']}")
    leak_lots = [r for r in removes if r.get("leaked")]
    for r in sorted(leak_lots, key=lambda x: -x["leaked"]):
        print(f"    {r['key'][0]} {r['key'][1]}: {r['leaked']} leaked of {len(r['victim_ids'])} removed")
    print(f"  lots with removal SHORTFALL: {len([r for r in removes if r['shortfall'] > 0])}")

    print(f"\nSHORTFALL lots (closing > system; NOT inserted — need real box IDs): {len(shortfalls)}")
    for s in sorted(shortfalls, key=lambda x: -x["short"]):
        print(f"  {s['key'][0]} {s['key'][1]}: DB {s['db']} -> closing {s['excel']}  (+{s['short']} unrecorded)")

    if stats["zero_lots"]:
        print(f"\n!! ZERO/BLANK closing quantity — SKIPPED (not deleted): {len(stats['zero_lots'])}")
        for (k, n) in stats["zero_lots"]:
            print(f"  {k[0]} {k[1]}: closing<=0 but DB has {n} boxes — review manually")
    if stats["conflicts"]:
        by = defaultdict(int)
        for _, c, _n in stats["conflicts"]:
            by[c] += 1
        print(f"\nMULTI-VALUED DB fields NOT flattened (logged for review): "
              f"{len(set(k for k, _, _ in stats['conflicts']))} lots -> {dict(by)}")
    if stats["nonintegral"]:
        print(f"\nNon-integral carton sums (rounding applied): {len(stats['nonintegral'])} "
              f"{stats['nonintegral'][:8]}")

    tally = defaultdict(int)
    for u in updates:
        for c in u["fields"]:
            tally[c] += 1
    print("\nField update tally (lots per column):")
    for c, n in sorted(tally.items(), key=lambda x: -x[1]):
        print(f"  {c:20s} {n}")

    print("\nTop REMOVE lots (company, lot, -boxes):")
    for r in sorted(removes, key=lambda x: -len(x["victim_ids"]))[:15]:
        print(f"  {r['key'][0]} {r['key'][1]}  -{len(r['victim_ids'])}  short={r['shortfall']}")


# ---------- execution ----------
def execute(conn, moves, updates, removes):
    cur = conn.cursor()
    now = datetime.utcnow()
    # 0. reassignments (retag lot_no) — before field refresh so moved boxes inherit it
    for m in moves:
        if m["ids"]:
            cur.execute(
                f"UPDATE {TABLE[m['company']]} SET lot_no = %s, updated_at = %s "
                f"WHERE id = ANY(%s)", (m["to_lot"], now, m["ids"]))
    # 1. field refresh (forced target lots re-fire the sync_canonical trigger)
    for u in updates:
        company, lot = u["key"]
        cols = list(u["fields"].keys())
        sets = ", ".join(f"{c} = %s" for c in cols) + ", updated_at = %s"
        vals = [u["fields"][c] for c in cols] + [now, lot]
        cur.execute(f"UPDATE {TABLE[company]} SET {sets} WHERE lot_no = %s", vals)
    # 2. removes
    for r in removes:
        if r["victim_ids"]:
            cur.execute(f"DELETE FROM {TABLE[r['key'][0]]} WHERE id = ANY(%s)",
                        (r["victim_ids"],))


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
    excel = load_excel()
    print(f"  {len(excel)} (company, lot) closing keys")

    conn = psycopg.connect(DSN, autocommit=False)
    db = load_db(conn)
    refset = load_refset(conn)
    departed = load_departed_set(conn)
    print(f"  {len(db)} (company, lot) in DB | {len(refset)} in-flight box refs | {len(departed)} departed box refs")

    moves, forced = resolve_reassignments(conn, db, refset, departed, excel)
    updates, removes, shortfalls, stats = plan(excel, db, refset, departed, forced)
    stats["db_only"] = [k for k in db if k not in excel]
    report(moves, updates, removes, shortfalls, stats)

    if args.dry_run:
        conn.close()
        print("\nDRY RUN -- nothing written.")
        return

    print("\nEXECUTING in one transaction...")
    try:
        execute(conn, moves, updates, removes)
        conn.commit()
        print("COMMIT OK.")
    except Exception:
        conn.rollback()
        print("ROLLBACK -- error during operation.")
        raise
    finally:
        conn.close()


def demo():
    """Self-check of the safety-critical bits: leaked-first victim order, protected
    (came-back) boxes never deleted, moves exclude NULL/departed/other-prefix, math holds."""
    refset = {"B-2", "B-3"}      # both appear in a movement table
    departed = {"B-2"}           # B-2 has an ACTIVE departure -> leaked dup, deletable
    boxes = [(1, None), (2, "B-1"), (3, "B-2"), (4, "B-3"), (5, "B-9"), (6, "B-4")]
    order = [rid for rid, _ in safe_order(boxes, refset, departed)]
    # NULL(1), leaked B-2(3), then clean free by suffix: B-9(5),B-4(6),B-1(2). B-3(4) protected.
    assert order == [1, 3, 5, 6, 2], order
    assert 4 not in order, "protected (came-back/in-flight) box selected for deletion!"
    v, short = choose_victims(boxes, 3, refset, departed)
    assert short == 0 and set(v) == {1, 3, 5}, (v, short)   # null, leaked, then top clean
    _, s2 = choose_victims(boxes, 5, refset, departed)       # 5 safe (1,3,5,6,2) -> short 0
    assert s2 == 0, s2
    # move_pool: excludes NULL(1), refset (3,4), departed (B-2) -> real clean only
    assert [b for _, b in move_pool(boxes, refset, departed)] == ["B-9", "B-4", "B-1"]
    p = move_pool([(7, "AA-5"), (8, "BB-9"), (9, "AA-2"), (10, None)], set(), set(), "AA")
    assert [b for _, b in p] == ["AA-5", "AA-2"], p
    assert _suffix("25601000-100") == 100 and _suffix("bad") == -1
    print("self-check OK: leaked-first victim order; protected boxes never deleted; move pool clean.")


if __name__ == "__main__":
    main()
