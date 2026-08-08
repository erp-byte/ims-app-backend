"""
Apply the 29-Jul-2026 closing reconciliation. DRY-RUN BY DEFAULT.

User instruction (2026-07-30):
    NEVER_IN_ANY_TABLE   -> create synthetic data for it
    COLD_EXCEEDS_SHEET   -> reduce the cartons
    MATCH_WRONG_COMPANY  -> correct the company
Everything else (COLD_SHORT_OF_SHEET, DEPARTED_NOT_IN_COLD, IN_DB_NOT_IN_COLD, and the
109 lots in cold_stocks but absent from the sheet) is LEFT ALONE.

THE ONE THING THAT IS NOT A NAIVE DIFF
--------------------------------------
The sheet is an as-on-29-Jul snapshot; the DB is live. A lot that shipped boxes on 30-Jul
should now hold LESS than the sheet says. So the delete quantity is computed against the
movement-adjusted target, never the raw gap:

    departed_after = cold_stock_disposition rows created after the cutoff, whose
                     source_table is a *_cold_stocks table and reverted IS NOT TRUE
    target_now     = sheet_cartons - departed_after
    delete_n       = cold_now - target_now

For 8081 that is 1250, not the raw 750; for 12484 200 not 100; for 13355 192 not 186.
Using the raw gap would silently leave 606 departed boxes standing.

VICTIM ORDER FOR DELETES (a real sticker must always outlive a placeholder)
    1. RECON22JUL synthetic boxes   -- when a lot holds both, the synthetic IS the duplicate
    2. NULL / blank box_id
    3. leaked boxes (an active departure record exists yet the cold row survived)
    4. clean unreferenced boxes, by (numeric suffix desc, id desc)
An in-flight box that is not leaked is NEVER deleted; if that leaves a lot short of its
delete quota the shortfall is reported and the lot is left partially trimmed.

EVERY delete and every company move writes a cold_stock_disposition row carrying the full
prior row in snapshot_data, so this run is reversible box-by-box. The 22-Jul reconciler
skipped that and left permanent audit holes in Lot Search; this one does not.

CARVE-OUTS (--include-carveouts to override)
The user separately instructed that three lots be built from REAL scanned boxes sitting in
the NULL-lot pool, not from synthetics. Those beat this general sweep, so they are excluded:
    8130         -- 1892 synthetics to be replaced by 1772 real boxes of TR-20260710122315
    8186, 8187   -- to be relabelled from real boxes of TR-20260726162836
They are handled by a separate script once their open questions are answered.

Run:  python scripts/apply_reconcile_29jul.py --dry-run
      python scripts/apply_reconcile_29jul.py --execute
"""
from __future__ import annotations
import os
import re
import sys
import json
import argparse
from collections import defaultdict
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path

import openpyxl
import psycopg
from psycopg.types.json import Jsonb
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from report_lots_in_closing_29jul import norm, _s, COL, HEADER_ROW, PREFIX

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
EXCEL_PATH = ROOT / "data" / "Cold Storage Stock Details 29th July Closing.xlsx"
CUTOFF = "2026-07-29 23:59:59"
BATCH = "RECON29JUL"
SYNTH_PREFIX = "RC29JUL"
ACTOR = os.environ.get("RECON_ACTOR", "ai.1@candorfoods.in")
CARVEOUT_LOTS = {"8130", "8186", "8187"}
# Lots the user explicitly approved for synthetic creation even though a NON-STOCK row
# mentions them elsewhere. 129737 has one cfpl_rtv_lines row (an RTV document line, qty 3000,
# zero boxes attached) and no stock record anywhere -- user call 2026-07-31: treat as missing.
FORCE_SYNTHETIC = {"129737"}

REF_SOURCES = {
    "cold_transfer_inboxes": ["box_id"],
    "pending_transfer_stock": ["box_id", "original_box_id"],
    "cold_stock_disposition": ["box_id"],
    "interunit_transfer_boxes": ["box_id"],
    "transfer_box_reconciliation": ["actual_box_id", "original_box_id"],
    "interunit_transfer_in_boxes": ["box_id"],
}

# sheet column -> cold_stocks column, for synthetic inserts
SHEET_TO_COLD = {
    "inward_dt": ("inward_dt", "date"), "unit": ("unit", "str"),
    "inward_no": ("inward_no", "str"), "cold_item_mark": ("cold_item_mark", "str"),
    "vakkal": ("vakkal", "str"), "weight_kg": ("weight_kg", "num"),
    "group_name": ("group_name", "str"), "item_subgroup": ("item_subgroup", "str"),
    "item_description": ("item_description", "str"), "item_mark": ("item_mark", "str"),
    "spl_remarks": ("spl_remarks", "str"), "storage_location": ("storage_location", "str"),
    "exporter": ("exporter", "str"), "last_purchase_rate": ("last_purchase_rate", "num"),
}
# 0-based sheet column indexes (header row 6, 1-based). COL from the report module only
# carries the few the report needed, so the full map lives here.
SHEET_COL_IDX = {
    "inward_dt": 0, "unit": 1, "inward_no": 2, "cold_item_desc": 3, "cold_item_mark": 4,
    "vakkal": 5, "lot_no": 6, "cartons": 7, "weight_kg": 8, "total_inventory_kgs": 9,
    "group_name": 10, "item_subgroup": 11, "item_mark": 12, "spl_remarks": 13,
    "item_description": 14, "company": 15, "storage_location": 16, "exporter": 17,
    "last_purchase_rate": 20, "value": 21,
}


def _num(v):
    try:
        return Decimal(str(round(float(v), 6)))
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


def coerce(kind, v):
    return {"str": _s, "num": _num, "date": _date}[kind](v)


def suffix(box_id):
    m = re.search(r"-(\d+)$", box_id or "")
    return int(m.group(1)) if m else -1


# ---------------------------------------------------------------- sheet
def load_sheet():
    ws = openpyxl.load_workbook(str(EXCEL_PATH), read_only=True, data_only=True)["Sheet1"]
    agg, order = {}, []
    for r in ws.iter_rows(min_row=HEADER_ROW + 1, values_only=True):
        co, lot = _s(r[COL["company"]]).upper(), _s(r[COL["lot_no"]])
        if not lot or co not in PREFIX:
            continue
        k = norm(lot)
        if k not in agg:
            agg[k] = {"lot": lot, "company": co, "cart": 0.0, "fields": defaultdict(set)}
            order.append(k)
        agg[k]["cart"] += float(r[COL["cartons"]] or 0)
        for skey, (dbcol, kind) in SHEET_TO_COLD.items():
            v = coerce(kind, r[SHEET_COL_IDX[skey]])
            if v is not None:
                agg[k]["fields"][dbcol].add(v)
    for d in agg.values():
        d["cart"] = round(d["cart"])
        # a single value per field, else the most common; multi-valued fields are rare here
        d["row"] = {c: (next(iter(vs)) if len(vs) == 1 else sorted(vs, key=str)[0])
                    for c, vs in d["fields"].items() if vs}
        d["multi"] = {c for c, vs in d["fields"].items() if len(vs) > 1}
    return agg, order


# ---------------------------------------------------------------- db
def load_db(conn):
    cur = conn.cursor()
    boxes = defaultdict(list)   # lot_key -> [(company, id, box_id, transaction_no)]
    for co, p in PREFIX.items():
        cur.execute(f"""SELECT id, box_id, lot_no::text, transaction_no
                        FROM {p}_cold_stocks WHERE lot_no IS NOT NULL ORDER BY id""")
        for rid, box_id, lot, txn in cur.fetchall():
            k = norm(lot)
            if k:
                boxes[k].append((co, rid, box_id, txn))

    refset = set()
    for t, cols in REF_SOURCES.items():
        for c in cols:
            cur.execute(f"SELECT DISTINCT {c}::text FROM {t} WHERE {c} IS NOT NULL")
            refset.update(b.strip() for (b,) in cur.fetchall() if b and b.strip())

    # a box with an ACTIVE departure record that still has a cold row = leaked duplicate
    departed = set()
    cur.execute("""SELECT DISTINCT box_id::text FROM cold_stock_disposition
                   WHERE box_id IS NOT NULL AND reverted IS NOT TRUE""")
    departed.update(b.strip() for (b,) in cur.fetchall() if b and b.strip())
    cur.execute("""SELECT DISTINCT box_id::text FROM interunit_transfer_boxes
                   WHERE box_id IS NOT NULL""")
    departed.update(b.strip() for (b,) in cur.fetchall() if b and b.strip())

    dep_after = defaultdict(int)
    cur.execute("""SELECT lot_no::text, count(*) FROM cold_stock_disposition
                   WHERE created_at > %s AND source_table LIKE '%%_cold_stocks'
                     AND reverted IS NOT TRUE GROUP BY 1""", (CUTOFF,))
    for lot, n in cur.fetchall():
        k = norm(lot)
        if k:
            dep_after[k] += n

    existing_box_ids = {co: set() for co in PREFIX}
    for co, p in PREFIX.items():
        cur.execute(f"SELECT box_id FROM {p}_cold_stocks WHERE box_id IS NOT NULL")
        existing_box_ids[co].update(b.strip() for (b,) in cur.fetchall() if b)
    return boxes, refset, departed, dep_after, existing_box_ids


def victim_order(rows, refset, departed):
    """rows: [(company, id, box_id, txn)] -> deletable rows, best victims first."""
    synth, nulls, leaked, clean = [], [], [], []
    for r in rows:
        _co, rid, box_id, txn = r
        b = (box_id or "").strip()
        if txn == "RECON22JUL":
            synth.append(r)
        elif not b:
            nulls.append(r)
        elif b in departed:
            leaked.append(r)
        elif b not in refset:
            clean.append(r)
        # else: genuinely in-flight -> never a victim
    synth.sort(key=lambda r: (-suffix(r[2]), -r[1]))
    nulls.sort(key=lambda r: -r[1])
    leaked.sort(key=lambda r: (-suffix(r[2]), -r[1]))
    clean.sort(key=lambda r: (-suffix(r[2]), -r[1]))
    return synth + nulls + leaked + clean


# ---------------------------------------------------------------- plan
def never_in_any_table(conn):
    """Lot keys with ZERO rows in every lot-bearing IMS table. Only these get synthetics —
    'no cold rows' is NOT the same test: 128496 has 614 transfer-out rows and 126030 has
    169 jobwork rows, so both are recorded stock, not missing stock."""
    from gap_report_29jul import load_counts, ALL_COLS
    cnt, _ = load_counts(conn)
    seen = set()
    for col in ALL_COLS:
        for k, per in cnt[col].items():
            if per.get("ALL", 0) or per.get("CFPL", 0) or per.get("CDPL", 0):
                seen.add(k)
    return seen


def build_plan(sheet, order, boxes, refset, departed, dep_after, existing, carveouts,
               anywhere):
    inserts, deletes, moves, notes = [], [], [], []
    for k in order:
        s = sheet[k]
        lot, co, cart = s["lot"], s["company"], s["cart"]
        rows = boxes.get(k, [])
        now = len(rows)
        dep = dep_after.get(k, 0)
        target = cart - dep
        in_db_cos = {r[0] for r in rows}

        if not carveouts and lot in CARVEOUT_LOTS:
            notes.append(f"lot {lot}: CARVED OUT (real-box instruction pending) — untouched")
            continue

        # A. absent from every IMS table -> synthetic
        if now == 0 and cart > 0 and not dep and (
                k not in anywhere or lot in FORCE_SYNTHETIC):
            base = s["row"]
            width = max(4, len(str(cart)))
            new = []
            for i in range(1, cart + 1):
                bid = f"{SYNTH_PREFIX}-{lot}-{i:0{width}d}"
                if bid in existing[co]:
                    continue           # idempotent: a re-run inserts nothing
                new.append(bid)
            if new:
                inserts.append({"lot": lot, "company": co, "n": len(new),
                                "box_ids": new, "row": base, "multi": s["multi"]})
            continue

        # C. counts agree but the lot sits under the wrong company
        if now and target == now and co not in in_db_cos:
            src = sorted(in_db_cos)[0]
            collide = [r[2] for r in rows if r[2] and r[2].strip() in existing[co]]
            moves.append({"lot": lot, "from": src, "to": co, "n": now,
                          "rows": rows, "collisions": collide})
            continue

        # B. cold exceeds the movement-adjusted target -> trim
        if now > target:
            need = now - target
            pool = victim_order(rows, refset, departed)
            picked = pool[:need]
            deletes.append({"lot": lot, "n": len(picked), "need": need,
                            "shortfall": need - len(picked), "rows": picked,
                            "cold_now": now, "sheet": cart, "departed_after": dep,
                            "target": target,
                            "synth_hit": sum(1 for r in picked if r[3] == "RECON22JUL")})
    return inserts, deletes, moves, notes


# ---------------------------------------------------------------- write
# ponytail: every write below is SET-BASED (one statement per lot per company, not per box).
# The per-row version issued ~20k round trips and blew a 10-minute timeout mid-transaction.
MOVE_SKIP = ("id", "created_at", "updated_at",
             "canonical_warehouse", "canonical_group", "canonical_subgroup")


def fetch_snaps(cur, table, ids):
    cur.execute(f"SELECT to_jsonb(t) FROM {table} t WHERE id = ANY(%s)", (ids,))
    return [r[0] for r in cur.fetchall()]


def dispose_bulk(cur, company, table, ids, lot, reason):
    """One INSERT..SELECT: the full prior row lands in snapshot_data without a round trip."""
    p = PREFIX[company]
    cur.execute(f"""
        INSERT INTO cold_stock_disposition
          (box_id, transaction_no, lot_no, item_description, from_company, unit, from_site,
           source_table, disposition_type, disposition_ref_table, disposition_ref_no,
           disposed_by, snapshot_data, notes)
        SELECT COALESCE(t.box_id, '<null-row-' || t.id || '>'),
               COALESCE(t.transaction_no, %s), %s, t.item_description, %s, t.unit,
               t.storage_location, %s, 'manual_correction', %s, %s, %s, to_jsonb(t), %s
        FROM {table} t WHERE t.id = ANY(%s)""",
                (BATCH, lot, p, table, table, BATCH, ACTOR, reason, ids))


def apply(conn, inserts, deletes, moves):
    cur = conn.cursor()
    backup = {"batch": BATCH, "inserts": [], "deletes": [], "moves": []}

    for d in deletes:
        by_co = defaultdict(list)
        for co, rid, _b, _t in d["rows"]:
            by_co[co].append(rid)
        reason = (f"{BATCH}: trim lot {d['lot']} to 29-Jul closing "
                  f"({d['cold_now']} -> {d['target']}; sheet {d['sheet']}, "
                  f"departed after 29-Jul {d['departed_after']})")
        for co, ids in by_co.items():
            tbl = f"{PREFIX[co]}_cold_stocks"
            backup["deletes"].extend(fetch_snaps(cur, tbl, ids))
            dispose_bulk(cur, co, tbl, ids, d["lot"], reason)
            cur.execute(f"DELETE FROM {tbl} WHERE id = ANY(%s)", (ids,))

    for m in moves:
        src = f"{PREFIX[m['from']]}_cold_stocks"
        dst = f"{PREFIX[m['to']]}_cold_stocks"
        ids = [rid for _co, rid, _b, _t in m["rows"]]
        snaps = fetch_snaps(cur, src, ids)
        if not snaps:
            continue
        cols = [c for c in snaps[0] if c not in MOVE_SKIP]
        cur.execute(f"INSERT INTO {dst} ({', '.join(cols)}) "
                    f"SELECT {', '.join('t.' + c for c in cols)} "
                    f"FROM {src} t WHERE t.id = ANY(%s)", (ids,))
        dispose_bulk(cur, m["from"], src, ids, m["lot"],
                     f"{BATCH}: company correction {m['from']} -> {m['to']} "
                     f"per 29-Jul closing (row re-inserted in {dst})")
        cur.execute(f"DELETE FROM {src} WHERE id = ANY(%s)", (ids,))
        backup["moves"].append({"from": src, "to": dst, "rows": snaps})

    for ins in inserts:
        tbl = f"{PREFIX[ins['company']]}_cold_stocks"
        row = ins["row"]
        wt, rate = row.get("weight_kg"), row.get("last_purchase_rate")
        val = wt * rate if wt is not None and rate is not None else None
        cols = ["box_id", "lot_no", "transaction_no", "no_of_cartons",
                "total_inventory_kgs", "value", "auto_created_from_inward"] + list(row)
        tail = [row[c] for c in row]
        cur.executemany(
            f"INSERT INTO {tbl} ({', '.join(cols)}) "
            f"VALUES ({', '.join(['%s'] * len(cols))})",
            [[bid, ins["lot"], BATCH, Decimal("1"), wt, val, False] + tail
             for bid in ins["box_ids"]])
        backup["inserts"].append({"table": tbl, "lot_no": ins["lot"],
                                  "n": len(ins["box_ids"]),
                                  "box_ids": ins["box_ids"]})
    return backup


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--execute", action="store_true")
    ap.add_argument("--include-carveouts", action="store_true",
                    help="also act on 8130/8186/8187 with synthetics (NOT recommended: "
                         "the user asked for real boxes there)")
    args = ap.parse_args()

    sheet, order = load_sheet()
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        boxes, refset, departed, dep_after, existing = load_db(conn)
        anywhere = never_in_any_table(conn)
        inserts, deletes, moves, notes = build_plan(
            sheet, order, boxes, refset, departed, dep_after, existing,
            args.include_carveouts, anywhere)

        print(f"=== {BATCH} PLAN ({'EXECUTE' if args.execute else 'DRY RUN'}) ===\n")

        print(f"-- A. SYNTHETIC INSERTS ({len(inserts)} lots, "
              f"{sum(i['n'] for i in inserts):,} boxes) --")
        print(f"   {'LOT':>8s}{'CO':>6s}{'BOXES':>7s}  {'BOX_ID RANGE':<44s}DESCRIPTION")
        for i in sorted(inserts, key=lambda x: -x["n"]):
            rng = f"{i['box_ids'][0]} .. {i['box_ids'][-1]}" if i["box_ids"] else "-"
            print(f"   {i['lot']:>8s}{i['company']:>6s}{i['n']:>7d}  {rng:<44s}"
                  f"{(i['row'].get('item_description') or '')[:30]}")
            if i["multi"]:
                print(f"            ^ sheet is multi-valued on {sorted(i['multi'])}; "
                      f"took the first")

        print(f"\n-- B. TRIM ({len(deletes)} lots, "
              f"{sum(d['n'] for d in deletes):,} boxes) --")
        print(f"   {'LOT':>8s}{'SHEET':>7s}{'DEP':>5s}{'TARGET':>7s}{'NOW':>7s}"
              f"{'DELETE':>7s}{'SYNTH':>7s}{'SHORT':>6s}")
        for d in sorted(deletes, key=lambda x: -x["n"]):
            flag = "  <- raw gap would be %d" % (d["cold_now"] - d["sheet"]) \
                if d["departed_after"] else ""
            print(f"   {d['lot']:>8s}{d['sheet']:>7d}{d['departed_after']:>5d}"
                  f"{d['target']:>7d}{d['cold_now']:>7d}{d['n']:>7d}"
                  f"{d['synth_hit']:>7d}{d['shortfall']:>6d}{flag}")

        print(f"\n-- C. COMPANY CORRECTION ({len(moves)} lots, "
              f"{sum(m['n'] for m in moves):,} boxes) --")
        for m in moves:
            print(f"   lot {m['lot']:>8s}  {m['from']} -> {m['to']}  {m['n']} boxes"
                  + (f"  !! box_id collision in target: {m['collisions'][:3]}"
                     if m["collisions"] else ""))

        for n in notes:
            print(f"\n   NOTE: {n}")

        short = [d for d in deletes if d["shortfall"]]
        if short:
            print(f"\n   !! {len(short)} lots cannot be fully trimmed "
                  f"({sum(d['shortfall'] for d in short)} boxes are in-flight):")
            for d in short:
                print(f"      lot {d['lot']}: wanted {d['need']}, can delete {d['n']}")

        net = sum(i["n"] for i in inserts) - sum(d["n"] for d in deletes)
        print(f"\n   net change to cold_stocks: {net:+,} boxes")

        if args.execute:
            collides = [m for m in moves if m["collisions"]]
            if collides:
                raise SystemExit("ABORT: box_id collision in a move target; resolve first.")
            backup = apply(conn, inserts, deletes, moves)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = ROOT / f"apply_reconcile_29jul_backup_{stamp}.json"
            path.write_text(json.dumps(backup, indent=1, default=str))
            conn.commit()
            print(f"\nCOMMITTED. Backup of every changed row: {path}")
            print(f"Reversible: cold_stock_disposition WHERE disposition_ref_no = '{BATCH}' "
                  f"carries the full prior row in snapshot_data.")
        else:
            conn.rollback()
            print("\nDRY RUN — nothing written. Re-run with --execute to apply.")


if __name__ == "__main__":
    main()
