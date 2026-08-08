"""
Fill every lot that is SHORT of the 29-Jul-2026 closing sheet with marked synthetic boxes.
DRY-RUN BY DEFAULT.  User instruction 2026-07-31: "if it is short then create synthetic".

Shortfall is measured against the movement-adjusted target, never the raw gap:

    departed_after = cold_stock_disposition rows created after 29-Jul whose source_table is a
                     *_cold_stocks table and reverted IS NOT TRUE, EXCLUDING this session's own
                     reconciliation batches (RECON29JUL%, SWAP-%) -- otherwise my own deletes
                     read back as post-snapshot departures and the lot looks short again.
    target_now     = sheet_cartons - departed_after
    shortfall      = target_now - cold_now        (only positive values are filled)

Boxes that left cold storage (transfer-out, jobwork, disposition) or sit In Transit are by
definition ABSENT from cold_stocks, so filling a cold-side shortfall does not double-count
them. Verified for the two lots where that mattered: 126030 (169 jobwork lines, Mar-2026) and
128496 (614 transferred out Jun-2026, 125 still In Transit) -- both hold 0 cold rows, and the
sheet still lists them as physically present.

Synthetic ids are marked and reversible:
    box_id          RC29JUL-<lot>-NNNN
    transaction_no  RECON29JUL-SHORT
    auto_created_from_inward = false
They do NOT match physical stickers. When the warehouse scans this stock for real, run
scripts/swap_synthetic_for_real.py rather than letting the placeholder stand -- that is the
mistake that doubled 14570, 13766, 8130 and 13865-13872.

Run:  python scripts/fill_short_lots_29jul.py --dry-run
      python scripts/fill_short_lots_29jul.py --execute
"""
from __future__ import annotations
import os
import sys
import json
import argparse
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import openpyxl
import psycopg
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from report_lots_in_closing_29jul import norm, _s, COL, HEADER_ROW, PREFIX
from apply_reconcile_29jul import SHEET_COL_IDX, SHEET_TO_COLD, coerce

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
EXCEL_PATH = ROOT / "data" / "Cold Storage Stock Details 29th July Closing.xlsx"
CUTOFF = "2026-07-29 23:59:59"
BATCH = "RECON29JUL-SHORT"
SYNTH_PREFIX = "RC29JUL"
ACTOR = os.environ.get("RECON_ACTOR", "ai.1@candorfoods.in")


def load_sheet():
    ws = openpyxl.load_workbook(str(EXCEL_PATH), read_only=True, data_only=True)["Sheet1"]
    agg, order = {}, []
    for r in ws.iter_rows(min_row=HEADER_ROW + 1, values_only=True):
        co, lot = _s(r[COL["company"]]).upper(), _s(r[COL["lot_no"]])
        if not lot or co not in PREFIX:
            continue
        k = norm(lot)
        if k not in agg:
            agg[k] = {"lot": lot, "company": co, "cart": 0.0, "fields": {}}
            order.append(k)
        agg[k]["cart"] += float(r[COL["cartons"]] or 0)
        for skey, (dbcol, kind) in SHEET_TO_COLD.items():
            v = coerce(kind, r[SHEET_COL_IDX[skey]])
            if v is not None:
                agg[k]["fields"].setdefault(dbcol, v)
    for d in agg.values():
        d["cart"] = round(d["cart"])
    return agg, order


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    sheet, order = load_sheet()
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        cur = conn.cursor()

        cold, existing = defaultdict(int), {co: set() for co in PREFIX}
        for co, p in PREFIX.items():
            cur.execute(f"SELECT lot_no::text, count(*) FROM {p}_cold_stocks "
                        "WHERE lot_no IS NOT NULL GROUP BY 1")
            for lot, n in cur.fetchall():
                k = norm(lot)
                if k:
                    cold[k] += n
            cur.execute(f"SELECT box_id FROM {p}_cold_stocks WHERE box_id IS NOT NULL")
            existing[co].update(b.strip() for (b,) in cur.fetchall() if b)

        dep = defaultdict(int)
        cur.execute("""SELECT lot_no::text, count(*) FROM cold_stock_disposition
                       WHERE created_at > %s AND source_table LIKE '%%_cold_stocks'
                         AND reverted IS NOT TRUE
                         AND (disposition_ref_no IS NULL
                              OR (disposition_ref_no NOT LIKE 'RECON29JUL%%'
                                  AND disposition_ref_no NOT LIKE 'SWAP-%%'))
                       GROUP BY 1""", (CUTOFF,))
        for lot, n in cur.fetchall():
            k = norm(lot)
            if k:
                dep[k] += n

        plan = []
        for k in order:
            s = sheet[k]
            short = (s["cart"] - dep.get(k, 0)) - cold.get(k, 0)
            if short <= 0:
                continue
            co = s["company"]
            width = max(4, len(str(short)))
            ids = [f"{SYNTH_PREFIX}-{s['lot']}-{i:0{width}d}" for i in range(1, short + 1)]
            ids = [b for b in ids if b not in existing[co]]
            plan.append({"lot": s["lot"], "company": co, "n": len(ids), "box_ids": ids,
                         "sheet": s["cart"], "cold": cold.get(k, 0), "dep": dep.get(k, 0),
                         "fields": s["fields"]})

        print(f"=== {BATCH} ({'EXECUTE' if args.execute else 'DRY RUN'}) ===\n")
        print(f"  {'LOT':>8s}{'CO':>6s}{'SHEET':>7s}{'DEP':>5s}{'COLD':>7s}{'CREATE':>7s}"
              f"  {'BOX_ID RANGE':<42s}DESCRIPTION")
        for p in sorted(plan, key=lambda x: -x["n"]):
            rng = f"{p['box_ids'][0]} .. {p['box_ids'][-1]}" if p["box_ids"] else "-"
            print(f"  {p['lot']:>8s}{p['company']:>6s}{p['sheet']:>7d}{p['dep']:>5d}"
                  f"{p['cold']:>7d}{p['n']:>7d}  {rng:<42s}"
                  f"{(p['fields'].get('item_description') or '')[:28]}")
        print(f"\n  {len(plan)} lots, {sum(p['n'] for p in plan):,} synthetic boxes")

        if not args.execute:
            conn.rollback()
            print("\nDRY RUN - nothing written.")
            return

        made = []
        for p in plan:
            tbl = f"{PREFIX[p['company']]}_cold_stocks"
            f = dict(p["fields"])
            wt, rate = f.get("weight_kg"), f.get("last_purchase_rate")
            cols = ["box_id", "lot_no", "transaction_no", "no_of_cartons",
                    "total_inventory_kgs", "value", "auto_created_from_inward"] + list(f)
            tail = [f[c] for c in f]
            cur.executemany(
                f"INSERT INTO {tbl} ({', '.join(cols)}) "
                f"VALUES ({', '.join(['%s'] * len(cols))})",
                [[b, p["lot"], BATCH, Decimal("1"), wt,
                  (wt * rate if wt is not None and rate is not None else None), False] + tail
                 for b in p["box_ids"]])
            made.append({"table": tbl, "lot": p["lot"], "n": p["n"],
                         "box_ids": p["box_ids"]})

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = ROOT / f"fill_short_lots_29jul_backup_{stamp}.json"
        path.write_text(json.dumps({"batch": BATCH, "created": made}, indent=1, default=str))
        conn.commit()
        print(f"\nCOMMITTED. Backup: {path}")
        print(f"Reversible: DELETE FROM <company>_cold_stocks "
              f"WHERE transaction_no = '{BATCH}'.")


if __name__ == "__main__":
    main()
