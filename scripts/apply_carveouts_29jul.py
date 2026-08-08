"""
RECON29JUL carve-outs: the three lots that must be built from REAL scanned boxes sitting in
the NULL-lot pool, not from synthetics. DRY-RUN BY DEFAULT.

User instructions:
  2026-07-30  "TR-20260726113513 ... don't even exist in db"
              "TR-20260726162836 ... only 295 left so remove two boxes, now lot no 8186 for
               266 boxes and 8187 for the remaining"
              "TR-20260710122315 ... only 1772 boxes left, lot no 8130"
  2026-07-31  8186/8187 land under CFPL, "go with the sheet"

The sheet is authoritative for identity, so the relabelled rows get the sheet's descriptive
fields. This deliberately overwrites the inward-recorded exporter on the Bunarinja boxes
(DB: 'OMAN AGRICULTURE DEVELOPMENT CO.' -> sheet: 'AL BARAKAH'); box_id and transaction_no are
preserved, so the physical sticker and the inward link both survive and the change is traceable.

    A. TR-20260726162836 (297 rows, cdpl, lot NULL)
         delete suffixes 296,297                                          -2
         suffixes   1..266  -> lot 8186, moved to cfpl_cold_stocks       266
         suffixes 267..295  -> lot 8187, moved to cfpl_cold_stocks        29
    B. TR-20260726113513 (300 rows, cdpl, lot NULL) -- no inward record anywhere
         delete all                                                     -300
    C. TR-20260710122315 (2399 rows, cdpl, lot NULL) -- IS lot 8130's real digitization
         suffixes 1..1772 -> lot 8130 (stays cdpl)                      1772
         delete suffixes 1773..2399                                     -627
    D. the RC22JUL-8130-* placeholders those real boxes replace
         delete all                                                   -1,892

Ends: 8130 = 1772 real (was 1892 synthetic), 8186 = 266, 8187 = 29. All three match the sheet.

Rows are selected by transaction_no + NUMERIC box_id suffix, never by `box_id LIKE 'prefix%'`
-- the 8-digit prefix is last-8-of-epoch-ms and recycles about every 27.8 hours.
Every delete and move writes a cold_stock_disposition row carrying the full prior row in
snapshot_data (ref RECON29JUL-CARVEOUT), so this is reversible box-by-box.

Run:  python scripts/apply_carveouts_29jul.py --dry-run
      python scripts/apply_carveouts_29jul.py --execute
"""
from __future__ import annotations
import os
import sys
import json
import argparse
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import openpyxl
import psycopg
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from report_lots_in_closing_29jul import norm, _s, COL, HEADER_ROW, PREFIX
from apply_reconcile_29jul import SHEET_COL_IDX, SHEET_TO_COLD, coerce, REF_SOURCES

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
EXCEL_PATH = ROOT / "data" / "Cold Storage Stock Details 29th July Closing.xlsx"
BATCH = "RECON29JUL-CARVEOUT"
ACTOR = os.environ.get("RECON_ACTOR", "ai.1@candorfoods.in")

# (txn, from_lo, from_hi, lot, target_company) — suffix range is inclusive
RELABEL = [
    ("TR-20260726162836", 1, 266, "8186", "CFPL"),
    ("TR-20260726162836", 267, 295, "8187", "CFPL"),
    ("TR-20260710122315", 1, 1772, "8130", "CDPL"),
]
# (txn, lo, hi | None = all) rows to remove; lo/hi are numeric suffixes
DROP = [
    ("TR-20260726162836", 296, 297, "surplus over the 295 physically present"),
    ("TR-20260726113513", None, None, "no inward record anywhere in the DB; not on the sheet"),
    ("TR-20260710122315", 1773, 2399, "surplus over the 1772 physically present"),
]
DROP_SYNTHETIC_LOT = ("8130", "CDPL")   # RC22JUL placeholders replaced by real boxes


def load_sheet_fields():
    """lot -> {cold column: value} straight from the 29-Jul sheet."""
    ws = openpyxl.load_workbook(str(EXCEL_PATH), read_only=True, data_only=True)["Sheet1"]
    want = {lot for _t, _a, _b, lot, _c in RELABEL}
    out, carts = {}, {}
    for r in ws.iter_rows(min_row=HEADER_ROW + 1, values_only=True):
        lot = norm(_s(r[COL["lot_no"]]))
        if lot not in want:
            continue
        out.setdefault(lot, {})
        carts[lot] = carts.get(lot, 0) + float(r[COL["cartons"]] or 0)
        for skey, (dbcol, kind) in SHEET_TO_COLD.items():
            v = coerce(kind, r[SHEET_COL_IDX[skey]])
            if v is not None:
                out[lot].setdefault(dbcol, v)
    return out, {k: round(v) for k, v in carts.items()}


def ids_for(cur, txn, lo, hi, company="CDPL"):
    """Row ids for a transaction, optionally bounded by NUMERIC box_id suffix."""
    tbl = f"{PREFIX[company]}_cold_stocks"
    sql = (f"SELECT id, box_id FROM {tbl} WHERE transaction_no = %s AND lot_no IS NULL")
    args = [txn]
    if lo is not None:
        sql += " AND NULLIF(split_part(box_id, '-', 2), '')::int BETWEEN %s AND %s"
        args += [lo, hi]
    sql += " ORDER BY NULLIF(split_part(box_id, '-', 2), '')::int"
    cur.execute(sql, args)
    return cur.fetchall()


def in_flight(cur, box_ids):
    """Any of these box_ids referenced by a live movement table."""
    hits = {}
    for tbl, cols in REF_SOURCES.items():
        for c in cols:
            cur.execute(f"SELECT count(*) FROM {tbl} WHERE {c}::text = ANY(%s)", (box_ids,))
            n = cur.fetchone()[0]
            if n:
                hits[f"{tbl}.{c}"] = n
    return hits


def dispose_bulk(cur, table, ids, lot, reason):
    p = table.split("_")[0]
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


def snaps(cur, table, ids):
    cur.execute(f"SELECT to_jsonb(t) FROM {table} t WHERE id = ANY(%s)", (ids,))
    return [r[0] for r in cur.fetchall()]


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    fields, carts = load_sheet_fields()
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        cur = conn.cursor()
        print(f"=== {BATCH} ({'EXECUTE' if args.execute else 'DRY RUN'}) ===\n")
        plan_rel, plan_drop, unsafe = [], [], {}

        for txn, lo, hi, lot, target in RELABEL:
            rows = ids_for(cur, txn, lo, hi)
            box_ids = [b for _i, b in rows]
            hits = in_flight(cur, box_ids) if box_ids else {}
            unsafe.update({f"{lot}:{k}": v for k, v in hits.items()})
            coll = 0
            if target != "CDPL":
                cur.execute(f"SELECT count(*) FROM {PREFIX[target]}_cold_stocks "
                            "WHERE transaction_no = %s AND box_id = ANY(%s)", (txn, box_ids))
                coll = cur.fetchone()[0]
            plan_rel.append({"txn": txn, "lot": lot, "target": target, "rows": rows,
                             "sheet": carts.get(lot), "collisions": coll, "inflight": hits})
            print(f"  RELABEL {len(rows):>5d} rows -> lot {lot:<7s} ({target})  "
                  f"sheet={carts.get(lot)}  suffix {lo}..{hi}  {txn}")
            if box_ids:
                print(f"          {box_ids[0]} .. {box_ids[-1]}"
                      + (f"   !! {coll} box_id collisions in target" if coll else "")
                      + (f"   !! IN-FLIGHT {hits}" if hits else ""))
            if carts.get(lot) is not None and len(rows) != carts[lot]:
                print(f"          !! count {len(rows)} != sheet {carts[lot]}")

        for txn, lo, hi, why in DROP:
            rows = ids_for(cur, txn, lo, hi)
            box_ids = [b for _i, b in rows]
            hits = in_flight(cur, box_ids) if box_ids else {}
            unsafe.update({f"{txn}:{k}": v for k, v in hits.items()})
            plan_drop.append({"txn": txn, "rows": rows, "why": why, "company": "CDPL",
                              "lot": None, "inflight": hits})
            rng = f"suffix {lo}..{hi}" if lo else "ALL"
            print(f"  DELETE  {len(rows):>5d} rows  {txn}  {rng}  -- {why}"
                  + (f"   !! IN-FLIGHT {hits}" if hits else ""))

        lot, co = DROP_SYNTHETIC_LOT
        cur.execute(f"SELECT id, box_id FROM {PREFIX[co]}_cold_stocks "
                    "WHERE lot_no::text = %s AND transaction_no = 'RECON22JUL'", (lot,))
        syn = cur.fetchall()
        syn_hits = in_flight(cur, [b for _i, b in syn]) if syn else {}
        unsafe.update({f"synthetic-{lot}:{k}": v for k, v in syn_hits.items()})
        plan_drop.append({"txn": "RECON22JUL", "rows": syn, "company": co, "lot": lot,
                          "why": f"placeholders replaced by the real boxes of "
                                 f"TR-20260710122315", "inflight": syn_hits})
        print(f"  DELETE  {len(syn):>5d} rows  RC22JUL-{lot}-*  -- synthetic placeholders"
              + (f"   !! IN-FLIGHT {syn_hits}" if syn_hits else ""))

        moved = sum(len(p["rows"]) for p in plan_rel if p["target"] != "CDPL")
        dropped = sum(len(p["rows"]) for p in plan_drop)
        print(f"\n  cdpl_cold_stocks: {-(dropped + moved):+,} rows   "
              f"cfpl_cold_stocks: {moved:+,} rows")
        for p in plan_rel:
            print(f"  lot {p['lot']:>7s} ends at {len(p['rows']):>5d} "
                  f"(sheet {p['sheet']}) in {p['target']}")

        if unsafe:
            print(f"\n  !! ABORT-WORTHY: in-flight references found: {unsafe}")
        if any(p["collisions"] for p in plan_rel):
            print("\n  !! ABORT-WORTHY: box_id collision in a target table")

        if not args.execute:
            conn.rollback()
            print("\nDRY RUN - nothing written.")
            return

        if unsafe or any(p["collisions"] for p in plan_rel):
            raise SystemExit("ABORT: unsafe preconditions; nothing written.")

        backup = {"batch": BATCH, "relabel": [], "deleted": []}
        # 1. deletes first, so the surplus never competes for a lot number
        for p in plan_drop:
            tbl = f"{PREFIX[p['company']]}_cold_stocks"
            ids = [i for i, _b in p["rows"]]
            if not ids:
                continue
            backup["deleted"].extend(snaps(cur, tbl, ids))
            dispose_bulk(cur, tbl, ids, p["lot"], f"{BATCH}: {p['why']} ({p['txn']})")
            cur.execute(f"DELETE FROM {tbl} WHERE id = ANY(%s)", (ids,))

        # 2. relabel: set lot_no + the sheet's descriptive fields, keep box_id/transaction_no
        for p in plan_rel:
            ids = [i for i, _b in p["rows"]]
            if not ids:
                continue
            f = dict(fields.get(p["lot"], {}))
            wt, rate = f.get("weight_kg"), f.get("last_purchase_rate")
            f["total_inventory_kgs"] = wt
            f["value"] = wt * rate if wt is not None and rate is not None else None
            f["no_of_cartons"] = Decimal("1")
            sets = ", ".join(f"{c} = %s" for c in f)
            vals = list(f.values())
            if p["target"] == "CDPL":
                cur.execute(f"UPDATE cdpl_cold_stocks SET lot_no = %s, {sets}, "
                            f"updated_at = CURRENT_TIMESTAMP WHERE id = ANY(%s)",
                            [p["lot"]] + vals + [ids])
            else:
                src, dst = "cdpl_cold_stocks", f"{PREFIX[p['target']]}_cold_stocks"
                backup["relabel"].extend(snaps(cur, src, ids))
                keep = ["box_id", "transaction_no", "inward_transaction_no",
                        "auto_created_from_inward", "created_at"]
                cols = ["lot_no"] + list(f) + keep
                cur.execute(
                    f"INSERT INTO {dst} ({', '.join(cols)}) "
                    f"SELECT %s, {', '.join(['%s'] * len(f))}, "
                    f"{', '.join('t.' + c for c in keep)} "
                    f"FROM {src} t WHERE t.id = ANY(%s)",
                    [p["lot"]] + vals + [ids])
                dispose_bulk(cur, src, ids, p["lot"],
                             f"{BATCH}: company correction cdpl -> "
                             f"{PREFIX[p['target']]} and lot {p['lot']} per 29-Jul closing")
                cur.execute(f"DELETE FROM {src} WHERE id = ANY(%s)", (ids,))

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = ROOT / f"apply_carveouts_29jul_backup_{stamp}.json"
        path.write_text(json.dumps(backup, indent=1, default=str))
        conn.commit()
        print(f"\nCOMMITTED. Backup: {path}")
        print(f"Reversible via cold_stock_disposition WHERE disposition_ref_no = '{BATCH}'.")


if __name__ == "__main__":
    main()
