"""
Recover the missing lot / date / warehouse / transaction fields on cold-stock rows.

WHERE THE VALUES CAN COME FROM (checked in this order, never invented):
  1. cold_stock_disposition — the box's own snapshot from when it left cold storage
     (snapshot_data holds inward_dt / inward_no / item_mark / storage_location / unit).
  2. movement ledgers — pending_transfer_stock, interunit_transfer_boxes,
     interunit_transfer_in_boxes, cold_transfer_inboxes — for transaction_no, but ONLY
     when the box_id resolves to exactly one non-empty value (box_id 'ART-1' style
     placeholders hit many rows; guessing there mis-parks stock).
  3. sibling rows of the same pile — for item_mark.

WHERE IT IS WRITTEN — this is the part that makes the repair stick:
  * auto_created_from_inward = FALSE -> UPDATE the cold row. Durable.
  * auto_created_from_inward = TRUE  -> NOT written here. sync_cold_stocks_from_inward
    (inward_tools.py) DELETEs and re-INSERTs those rows from the inward on every
    approve/edit, taking lot_no = COALESCE(box.lot_number, article.lot_number). A cold-side
    patch survives until the next edit, then silently reverts. Those rows are listed as a
    worklist: fix the lot on the INWARD and the sync carries it into cold stock.

Nothing is guessed. A pile whose lot was never captured anywhere (the inward itself has
lot_number = NULL) is reported as needing warehouse input, not filled with an inference.

USAGE
  Dry-run (default, NO writes):      python recover_cold_missing_fields.py
  Apply the durable fixes:           python recover_cold_missing_fields.py --apply
  Also write the worklist workbook:  python recover_cold_missing_fields.py --xlsx
"""
import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

TABLES = (("cfpl_cold_stocks", "cfpl"), ("cdpl_cold_stocks", "cdpl"))
OUT_XLSX = os.path.join("data", "cold_missing_field_worklist.xlsx")

DAMAGED = """
SELECT id, box_id, transaction_no, CAST(lot_no AS TEXT) AS lot, item_description,
       inward_dt, inward_no, unit, storage_location, item_mark,
       auto_created_from_inward AS auto, inward_transaction_no AS inw_txn,
       no_of_cartons, weight_kg
FROM {t}
WHERE lot_no IS NULL OR TRIM(CAST(lot_no AS TEXT)) = ''
   OR transaction_no IS NULL OR TRIM(transaction_no) = ''
   OR inward_dt IS NULL
   OR unit IS NULL OR TRIM(unit) = ''
   OR storage_location IS NULL OR TRIM(storage_location) = ''
   OR item_mark IS NULL OR TRIM(item_mark) = ''
ORDER BY id
"""

# transaction_no candidates for one box, from every ledger that records a movement.
TXN_SOURCES = """
SELECT DISTINCT src, txn FROM (
    SELECT 'disposition' src, transaction_no txn, COALESCE(lot_no,'') lot
      FROM cold_stock_disposition WHERE box_id = :b
    UNION ALL
    SELECT 'pending', transaction_no, COALESCE(lot_no,'')
      FROM pending_transfer_stock WHERE box_id = :b
    UNION ALL
    SELECT 'transfer_out', transaction_no, COALESCE(lot_number,'')
      FROM interunit_transfer_boxes WHERE box_id = :b
    UNION ALL
    SELECT 'transfer_in', transaction_no, COALESCE(lot_number,'')
      FROM interunit_transfer_in_boxes WHERE box_id = :b
    UNION ALL
    SELECT 'cold_in', transaction_no, COALESCE(lot_no,'')
      FROM cold_transfer_inboxes WHERE box_id = :b
) x
WHERE COALESCE(TRIM(txn),'') <> ''
  AND (:lot = '' OR lot = '' OR lot = :lot)
"""

FIELDS = ("lot_no", "transaction_no", "inward_dt", "unit", "storage_location", "item_mark")


def blank(v):
    return v is None or (isinstance(v, str) and not v.strip())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Write the durable fixes (default: dry-run)")
    ap.add_argument("--xlsx", action="store_true", help="Also write the worklist workbook")
    args = ap.parse_args()

    raw = os.environ["DATABASE_URL"]
    url = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
    engine = create_engine(url)

    fixes = []        # (table, id, {col: value}, source)  — durable, applied with --apply
    worklist = []     # rows whose value must come from the inward / the warehouse
    ambiguous = []    # a value existed but resolved to >1 candidate — never guessed

    with engine.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        for table, company in TABLES:
            rows = c.execute(text(DAMAGED.format(t=table))).fetchall()
            print(f"\n===== {table}: {len(rows)} rows with a missing field =====")
            # pile -> the item_mark its non-blank siblings agree on
            marks = defaultdict(set)
            for r in c.execute(text(f"""
                    SELECT item_description, COALESCE(CAST(lot_no AS TEXT),'') lot, item_mark
                    FROM {table} WHERE COALESCE(TRIM(item_mark),'') <> ''
                    GROUP BY 1,2,3""")):
                marks[(r[0], r[1])].add(r[2])

            for r in rows:
                m = dict(r._mapping)
                found, missing = {}, []

                disp = c.execute(text("""
                    SELECT lot_no, transaction_no, snapshot_data
                    FROM cold_stock_disposition
                    WHERE box_id = :b
                      AND (:t = '' OR transaction_no = :t)
                      AND LOWER(TRIM(COALESCE(item_description,''))) = LOWER(TRIM(:item))
                    ORDER BY disposed_at DESC LIMIT 1
                """), {"b": m["box_id"], "t": (m["transaction_no"] or "").strip(),
                       "item": m["item_description"] or ""}).fetchone()
                snap = (disp._mapping["snapshot_data"] if disp is not None else None) or {}

                if blank(m["lot"]):
                    v = (disp._mapping["lot_no"] if disp is not None else None)
                    (found.update({"lot_no": v}) if not blank(v) else missing.append("lot_no"))

                if blank(m["transaction_no"]):
                    cands = {x[1] for x in c.execute(
                        text(TXN_SOURCES), {"b": m["box_id"], "lot": (m["lot"] or "").strip()}).fetchall()}
                    if len(cands) == 1:
                        found["transaction_no"] = cands.pop()
                    elif len(cands) > 1:
                        ambiguous.append((table, m["id"], "transaction_no", sorted(cands)))
                        missing.append("transaction_no")
                    else:
                        missing.append("transaction_no")

                if m["inward_dt"] is None:
                    v = snap.get("inward_dt")
                    (found.update({"inward_dt": v}) if v else missing.append("inward_dt"))

                for col, key in (("unit", "unit"), ("storage_location", "storage_location")):
                    if blank(m[col]):
                        v = snap.get(key)
                        (found.update({col: v}) if not blank(v) else missing.append(col))

                if blank(m["item_mark"]):
                    v = snap.get("item_mark")
                    if blank(v):
                        sib = marks.get((m["item_description"], (m["lot"] or "").strip()), set())
                        v = sib.pop() if len(sib) == 1 else None
                    (found.update({"item_mark": v}) if not blank(v) else missing.append("item_mark"))

                if found:
                    if m["auto"]:
                        # A cold-side write here is undone by the next inward approve/edit.
                        worklist.append((table, company, m, sorted(found) + missing, "inward re-sync owns this row"))
                    else:
                        fixes.append((table, m["id"], found, "disposition/ledger/sibling"))
                if missing and not (found and m["auto"]):
                    worklist.append((table, company, m, missing,
                                     "inward is blank too — needs warehouse input"
                                     if m["auto"] else "no digital source"))

    print(f"\nDurable fixes available : {len(fixes)} row(s)")
    by_col = defaultdict(int)
    for _t, _i, f, _s in fixes:
        for k in f:
            by_col[k] += 1
    for k, n in sorted(by_col.items(), key=lambda kv: -kv[1]):
        print(f"   {k:<18} {n}")
    if ambiguous:
        print(f"Ambiguous, left alone   : {len(ambiguous)} row(s) (>1 candidate value)")

    # Worklist grouped by pile — this is what the warehouse has to supply.
    piles = defaultdict(lambda: {"boxes": 0, "missing": set(), "why": set()})
    for table, company, m, missing, why in worklist:
        k = (company, m["item_description"], (m["lot"] or "").strip(), m["inw_txn"] or m["transaction_no"],
             str(m["inward_dt"]), m["storage_location"])
        p = piles[k]
        p["boxes"] += 1
        p["missing"].update(missing)
        p["why"].add(why)
    print(f"\nNeeds a human-supplied value: {len(piles)} pile(s) / {sum(p['boxes'] for p in piles.values())} boxes")
    print(f"  {'co':5}{'item':<34}{'lot':<9}{'source txn':<22}{'date':<12}{'site':<12}{'bx':>6}  missing")
    for k, p in sorted(piles.items(), key=lambda kv: -kv[1]["boxes"])[:25]:
        co, item, lot, txn, dt, site = k
        print(f"  {co:5}{str(item)[:33]:<34}{(lot or '-'):<9}{str(txn)[:21]:<22}{dt[:11]:<12}"
              f"{str(site)[:11]:<12}{p['boxes']:>6}  {','.join(sorted(p['missing']))}")

    if args.xlsx:
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Needs Input"
        ws.append(["Company", "Item", "Lot", "Source Txn", "Inward Dt", "Site", "Boxes", "Missing", "Why"])
        for k, p in sorted(piles.items(), key=lambda kv: -kv[1]["boxes"]):
            ws.append(list(k) + [p["boxes"], ", ".join(sorted(p["missing"])), "; ".join(sorted(p["why"]))])
        ws2 = wb.create_sheet("Auto-Fixable")
        ws2.append(["Table", "Row id", "Field", "Value", "Source"])
        for table, rid, f, src in fixes:
            for col, val in f.items():
                ws2.append([table, rid, col, str(val), src])
        for w in (ws, ws2):
            w.freeze_panes = "A2"
            w.auto_filter.ref = w.dimensions
        os.makedirs("data", exist_ok=True)
        wb.save(OUT_XLSX)
        print(f"\nwrote {OUT_XLSX}")

    if args.apply and fixes:
        backup = f"recover_cold_backup_{datetime.now():%Y%m%d_%H%M%S}.json"
        with engine.begin() as c:
            before = []
            for table, rid, f, _src in fixes:
                row = c.execute(text(f"SELECT {', '.join(FIELDS)} FROM {table} WHERE id = :i"),
                                {"i": rid}).fetchone()
                before.append({"table": table, "id": rid,
                               "before": {k: str(v) for k, v in row._mapping.items()}, "set": {k: str(v) for k, v in f.items()}})
                sets = ", ".join(f"{col} = :{col}" for col in f)
                c.execute(text(f"UPDATE {table} SET {sets}, updated_at = NOW() WHERE id = :i"),
                          {**f, "i": rid})
            with open(backup, "w", encoding="utf-8") as fh:
                json.dump(before, fh, indent=2)
        print(f"\n[OK] COMMITTED {len(fixes)} row update(s). Backup: {backup}")
    elif fixes:
        print("\n(dry-run — no changes written. Re-run with --apply to commit.)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
