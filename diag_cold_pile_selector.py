"""Cold-stock SELECTOR ambiguity checker (READ-ONLY).

The cold-transfer form shows one row per search group but picks boxes with a
COARSER key, so some rows are not addressable:

  /cold-storage/stocks/search   groups by (item_description, lot_no, inward_no,
                                item_mark, storage_location, unit)  [MIN(inward_dt)]
  /cold-storage/stocks/pick-boxes filters by (item_description, lot_no, inward_no)

Any pick key covering >1 search row = the 2nd (3rd…) row cannot be selected:
pick-boxes returns the FIRST row's boxes (FIFO by id), so the form either
re-picks already-added box_ids or the save is rejected as duplicate.

Classes reported:
  AMBIGUOUS   one pick key -> several search rows (date / unit / site / mark differ)
  DATE-MERGED one search row hides >1 inward_dt (search MINs the date away)
  NO-LOT      lot_no NULL/empty -> the form blocks the add outright
  NO-DATE / NO-UNIT / NO-SITE  missing discriminators (disposition-recovered rows)

USAGE:  python diag_cold_pile_selector.py [--xlsx out.xlsx]
Exit code 0 = clean, 1 = ambiguous selectors found.
"""
import os
import sys

from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

_raw = os.environ["DATABASE_URL"]
DB_URL = _raw.replace("postgresql://", "postgresql+psycopg://", 1) if _raw.startswith("postgresql://") else _raw
engine = create_engine(DB_URL)

# One row per search group, annotated with how many groups share its pick key.
SCAN = """
    WITH grp AS (
        SELECT item_description,
               COALESCE(CAST(lot_no AS TEXT), '')      AS lot,
               COALESCE(inward_no, '')                  AS inward_no,
               COALESCE(item_mark, '')                  AS item_mark,
               COALESCE(storage_location, '')            AS site,
               COALESCE(unit, '')                       AS unit,
               COUNT(*)                                 AS rows,
               SUM(COALESCE(no_of_cartons, 0))           AS cartons,
               COUNT(DISTINCT inward_dt)                 AS dates,
               MIN(inward_dt)                            AS first_dt,
               MAX(inward_dt)                            AS last_dt,
               COUNT(DISTINCT transaction_no)            AS txns,
               COUNT(*) FILTER (WHERE inward_dt IS NULL) AS null_dt,
               COUNT(*) FILTER (WHERE box_id IS NULL OR TRIM(box_id) = '') AS null_box,
               COUNT(*) FILTER (WHERE transaction_no IS NULL OR TRIM(transaction_no) = '') AS null_txn
        FROM {t}
        GROUP BY 1, 2, 3, 4, 5, 6
    )
    SELECT g.*,
           COUNT(*) OVER (PARTITION BY item_description, lot, inward_no) AS rows_per_pickkey,
           SUM(rows) OVER (PARTITION BY item_description, lot, inward_no) AS boxes_per_pickkey,
           COUNT(*) OVER (PARTITION BY item_description, lot) AS rows_per_guardkey
    FROM grp g
    ORDER BY rows_per_pickkey DESC, rows_per_guardkey DESC, item_description, lot
"""


def classes(r):
    out = []
    if r["rows_per_pickkey"] > 1:
        out.append("AMBIGUOUS")
    if r["rows_per_guardkey"] > 1:
        # The cold-transfer form's old "already added?" key was (item, lot) only, so
        # the 2nd pile of a lot was rejected as a duplicate — the multi-select failure.
        out.append("GUARD-DUP")
    if r["dates"] > 1:
        out.append("DATE-MERGED")
    if not r["lot"]:
        out.append("NO-LOT")
    if r["null_dt"]:
        out.append("NO-DATE")
    if not r["unit"]:
        out.append("NO-UNIT")
    if not r["site"]:
        out.append("NO-SITE")
    if r["null_box"]:
        out.append("NO-BOXID")
    if r["null_txn"]:
        # Dispatching these leaves the box in cold_stocks AND in-transit: the
        # deduction looks the source row up by (box_id, transaction_no) and
        # `transaction_no = ''` never matches NULL.
        out.append("NO-TXN")
    return out


def main() -> int:
    all_rows = []
    with engine.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        for tbl, company in (("cfpl_cold_stocks", "cfpl"), ("cdpl_cold_stocks", "cdpl")):
            if not c.execute(text("SELECT to_regclass(:t)"), {"t": f"public.{tbl}"}).scalar():
                continue
            for r in c.execute(text(SCAN.replace("{t}", tbl))).fetchall():
                m = dict(r._mapping)
                m["company"] = company
                m["classes"] = classes(m)
                all_rows.append(m)

    tally = {}
    for m in all_rows:
        for cl in m["classes"]:
            tally[cl] = tally.get(cl, 0) + 1
    print(f"search rows scanned: {len(all_rows)}")
    for cl, n in sorted(tally.items(), key=lambda kv: -kv[1]):
        print(f"  {cl:12s} {n:5d} rows")

    amb = [m for m in all_rows if "GUARD-DUP" in m["classes"] or "AMBIGUOUS" in m["classes"]]
    keys = {(m["company"], m["item_description"], m["lot"]) for m in amb}
    print(f"\nMulti-pile lots: {len(keys)} (item, lot) keys covering {len(amb)} piles that only a "
          f"pile_key can separate")
    print(f"  {'co':5s}{'item':<34s}{'lot':<10s}{'inward':<14s}{'date':<12s}{'unit':<7s}{'site':<12s}{'mark':<16s}{'bx':>5s}")
    for m in sorted(amb, key=lambda x: (x["item_description"], x["lot"], str(x["first_dt"] or "")))[:60]:
        print(f"  {m['company']:5s}{str(m['item_description'])[:33]:<34s}{m['lot'][:9]:<10s}"
              f"{m['inward_no'][:13]:<14s}{str(m['first_dt'] or '-'):<12s}{m['unit'][:6]:<7s}"
              f"{m['site'][:11]:<12s}{m['item_mark'][:15]:<16s}{m['rows']:>5d}")
    if len(amb) > 60:
        print(f"  … {len(amb) - 60} more (use --xlsx for the full list)")

    if "--xlsx" in sys.argv:
        from openpyxl import Workbook
        out = sys.argv[sys.argv.index("--xlsx") + 1] if len(sys.argv) > sys.argv.index("--xlsx") + 1 \
            else "data/cold_pile_selector_audit.xlsx"
        cols = ["company", "item_description", "lot", "inward_no", "item_mark", "site", "unit",
                "first_dt", "last_dt", "dates", "rows", "cartons", "txns", "null_dt", "null_box",
                "null_txn", "rows_per_pickkey", "boxes_per_pickkey", "rows_per_guardkey"]
        wb = Workbook()
        ws = wb.active
        ws.title = "Selector Audit"
        ws.append(cols + ["classes"])
        for m in all_rows:
            if m["classes"]:
                ws.append([str(m[c]) if c.endswith("_dt") else m[c] for c in cols] + [",".join(m["classes"])])
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        wb.save(out)
        print(f"\nwrote {out}")

    return 1 if amb else 0


if __name__ == "__main__":
    sys.exit(main())
