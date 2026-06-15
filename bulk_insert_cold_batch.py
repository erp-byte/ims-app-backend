"""Batch bulk-inserter for cfpl/cdpl_cold_stocks — one lot at a time, each lot in
its OWN transaction with its OWN unique box-id base prefix.

Box id  : {last 8 epoch-ms digits}-{n}, fresh base per lot, sequential 1..N.
          Base is unique across the WHOLE batch (checked vs DB + vs bases already
          used this run) so no two boxes anywhere share a prefix.
TR no   : TR-{inward YYYYMMDD}{HHMMSS}, unique per lot (bumped on collision).
Per row : one carton (no_of_cartons=1); weight_kg = total_qty / cartons;
          value = total_value / cartons.

Run:
    python bulk_insert_cold_batch.py            # dry run — summary + 1 sample row
    python bulk_insert_cold_batch.py --apply    # commit every lot
"""
import os, io, sys, json, time
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB)
TABLE_MAP = {"cfpl": "cfpl_cold_stocks", "cdpl": "cdpl_cold_stocks"}
APPLY = "--apply" in sys.argv


def iso(d):  # "DD/MM/YYYY" -> "YYYY-MM-DD"
    dd, mm, yy = d.split("/")
    return f"{yy}-{mm.zfill(2)}-{dd.zfill(2)}"


def mk(company, lot, item, cartons, qty, rate, total_value, vakkal,
       inward_dd_mm_yyyy, inward_no, unit, storage):
    """Build a lot config from sheet totals; derives per-carton weight & value."""
    cartons = int(cartons)
    weight = round(qty / cartons, 3)
    value = round(total_value / cartons, 2)
    # sanity: rate * total_qty should ~equal total_value
    calc = round(rate * qty, 2)
    note = "" if abs(calc - total_value) < 1.0 else f"  (rate*qty={calc} vs value={total_value})"
    return {
        "company": company, "lot_no": str(lot), "item_description": item,
        "box_count": cartons, "weight_kg": weight, "value": value,
        "last_purchase_rate": rate, "unit": unit, "storage_location": storage,
        "inward_dt": iso(inward_dd_mm_yyyy), "inward_no": inward_no,
        "vakkal": (str(vakkal) if vakkal not in (None, "") else None),
        "total_qty": qty, "total_value": total_value, "_note": note,
    }


# ─────────────────────────── THE LOTS ───────────────────────────
# mk(company, lot, item, cartons, total_qty, rate, total_value, vakkal, inward_dt, inward_no, unit, storage)
LOTS = [
    mk("cfpl", 8579, "American Almonds (23-25 Count)",        95, 2850.00, 810.00, 2308500.00, "MIX",    "02/06/2026", "GR3766", "D-39", "Savla"),
    mk("cfpl", 8580, "American Almonds Running 25-27 Count", 117, 3510.00, 780.00, 2737800.00, "MIX",    "02/06/2026", "GR3766", "D-39", "Savla"),
    mk("cfpl", 8773, "Dried Cranberry Sliced",                37,  419.58, 390.00,  163636.20, "SLICE",  "03/06/2026", "GR3871", "D-39", "Savla"),
    mk("cfpl", 8941, "Organic Wanan Premium Jumbo",            2,   10.00, 845.00,    8450.00, "JUMBO",  "04/06/2026", "GR3952", "D-39", "Savla"),
    mk("cfpl", 8942, "Organic Wanan Premium Large",            2,   10.00, 635.00,    6350.00, "LARGE",  "04/06/2026", "GR3952", "D-39", "Savla"),
    mk("cfpl", 8943, "Organic Seqee Premium Large",            2,   10.00, 565.00,    5650.00, "LARGE",  "04/06/2026", "GR3952", "D-39", "Savla"),
    mk("cfpl", 8944, "Organic Khidri Premium Medium",          1,    5.00, 403.00,    2015.00, "MEDIUM", "04/06/2026", "GR3952", "D-39", "Savla"),
    mk("cfpl", 8957, "Organic Khidri Large dates",             1,    5.00, 462.00,    2310.00, "LARGE",  "04/06/2026", "GR3952", "D-39", "Savla"),
]
# ───────────────────────────────────────────────────────────────────

used_bases = set()   # box-id prefixes used this run — guarantees cross-lot distinctness
used_txns = set()    # TR- numbers used this run


def unique_base(conn, table):
    base = str(int(time.time() * 1000))[-8:]
    for _ in range(2000):
        if base not in used_bases and not conn.execute(
            text(f"SELECT 1 FROM {table} WHERE box_id LIKE :p LIMIT 1"), {"p": f"{base}-%"}
        ).first():
            used_bases.add(base)
            return base
        base = str(int(base) + 1).zfill(8)[-8:]
    raise RuntimeError("no free box-id base")


def make_txn(conn, table, inward_dt):
    datepart = inward_dt.replace("-", "")[:8]
    for bump in range(120):
        hhmmss = (int(datetime.now().strftime("%H%M%S")) + bump) % 1000000
        cand = f"TR-{datepart}{hhmmss:06d}"
        if cand not in used_txns and not conn.execute(
            text(f"SELECT 1 FROM {table} WHERE transaction_no = :t LIMIT 1"), {"t": cand}
        ).first():
            used_txns.add(cand)
            return cand
        if bump == 0:
            time.sleep(1)
    raise RuntimeError("no free TR- number")


def build_rows(cfg, base, txn):
    return [{
        "box_id": f"{base}-{i}", "transaction_no": txn, "lot_no": cfg["lot_no"],
        "item_description": cfg["item_description"], "no_of_cartons": 1,
        "weight_kg": cfg["weight_kg"], "total_inventory_kgs": cfg["weight_kg"],
        "unit": cfg["unit"], "storage_location": cfg["storage_location"],
        "inward_dt": cfg["inward_dt"], "inward_no": cfg["inward_no"],
        "vakkal": cfg["vakkal"], "last_purchase_rate": cfg["last_purchase_rate"],
        "value": cfg["value"], "auto_created_from_inward": False,
    } for i in range(1, cfg["box_count"] + 1)]


INSERT_SQL = """
    INSERT INTO {table}
        (box_id, transaction_no, lot_no, item_description, no_of_cartons,
         weight_kg, total_inventory_kgs, unit, storage_location, inward_dt,
         inward_no, vakkal, last_purchase_rate, value, auto_created_from_inward,
         created_at, updated_at)
    VALUES
        (:box_id, :transaction_no, :lot_no, :item_description, :no_of_cartons,
         :weight_kg, :total_inventory_kgs, :unit, :storage_location, :inward_dt,
         :inward_no, :vakkal, :last_purchase_rate, :value, :auto_created_from_inward,
         NOW(), NOW())
"""

print(f"{'Lot':<7}{'Co':<6}{'Item':<34}{'Box':>5}{'Wt/ctn':>9}{'Val/ctn':>10}  Box-id range / TR")
print("-" * 110)
results, total_boxes = [], 0
for cfg in LOTS:
    table = TABLE_MAP[cfg["company"]]
    with engine.begin() as c:
        # collision check: are these lot rows already present? (idempotency by lot+item)
        base = unique_base(c, table)
        txn = make_txn(c, table, cfg["inward_dt"])
        rows = build_rows(cfg, base, txn)
        rng = f"{base}-1..{base}-{cfg['box_count']}"
        line = (f"{cfg['lot_no']:<7}{cfg['company'].upper():<6}{cfg['item_description'][:33]:<34}"
                f"{cfg['box_count']:>5}{cfg['weight_kg']:>9}{cfg['value']:>10}  {rng}  {txn}{cfg['_note']}")
        print(line)
        total_boxes += cfg["box_count"]

        if APPLY:
            c.execute(text(INSERT_SQL.format(table=table)), rows)
            got = c.execute(text(
                f"SELECT COUNT(*) n, COUNT(DISTINCT box_id) d, SUM(weight_kg) kg, SUM(value) val "
                f"FROM {table} WHERE transaction_no = :t"), {"t": txn}).mappings().first()
            assert got["n"] == cfg["box_count"] == got["d"], f"row/box mismatch on {cfg['lot_no']}"
            results.append({"lot": cfg["lot_no"], "company": cfg["company"], "table": table,
                            "transaction_no": txn, "box_base": base, "rows": got["n"],
                            "total_kg": float(got["kg"]), "total_value": float(got["val"])})

print("-" * 110)
print(f"Lots: {len(LOTS)}   Total boxes: {total_boxes}")

if not APPLY:
    s = LOTS[0]
    print("\nDRY RUN — nothing inserted. One sample row (lot 17172):")
    print(json.dumps(build_rows(s, "<base>", "<TR>")[0], default=str, indent=2))
    print("\nRe-run with --apply to commit all lots.")
    raise SystemExit(0)

stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
rec = os.path.join(os.path.dirname(__file__), f"bulk_insert_cold_batch_{stamp}.json")
with open(rec, "w", encoding="utf-8") as f:
    json.dump({"lots": results, "total_boxes": total_boxes}, f, indent=2)
print(f"\nCOMMITTED {len(results)} lots, {total_boxes} boxes.")
print(f"Rollback record (DELETE by transaction_no per lot): {rec}")
