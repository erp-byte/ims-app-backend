"""Insert the 7 genuinely-missing 'Only in IMS' lots (Group A) into cold_stocks.

Source: Corrections/Cold_and_IMS_Comparison_14Jun2026.xlsx, sheet IMS_vs_System_Comparison,
Status='Only in IMS'. Of the 22 such lots, only these 7 are absent from BOTH cfpl and cdpl
cold tables and have no conflicting transfer/cross-company stock (user-confirmed 2026-06-14).
All 7 are IMS-aged '<30 Days' -> inward_dt = today.

Per-lot we create N rows (N = IMS cartons), 1 carton each, weight = IMS_qty / cartons,
value = rate * weight (reconciles exactly to IMS Value), one fresh transaction_no + one
fresh epoch-ms box_id base per lot, box_id = base-1 .. base-N.

Safety:
  - GUARD: each lot must currently have 0 rows in BOTH cfpl and cdpl cold tables (abort else).
  - box_id base collision-checked against the target table.
  - Single transaction; inserted (id, box_id) written to a rollback JSON.
  - Dry-run by default; pass --apply to commit.
"""
import os, io, sys, json, time
from datetime import datetime, timedelta
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB, pool_pre_ping=True)
APPLY = "--apply" in sys.argv
INWARD_DT = "2026-06-14"   # all 7 lots are IMS-aged '<30 Days'

# entity, lot, unit, vakkal, item_description, storage, cartons, qty_kg, rate
SPEC = [
 ("CFPL","17694","D-514","500 GM","King Solomon Seedless Medjoul 500 g","Savla",4,32,1100),
 ("CFPL","17693","D-514","500 GM","King Solomon Seedless Oriental dates 500 g","Savla",5,40,400),
 ("CFPL","17695","D-514","750 GM","King Solomon Medjoul super jumbo 750 g","Savla",5,40,1250),
 ("CFPL","17696","D-514","320 GM","King Solomon medjoul super jumbo 320 g","Savla",5,40,1250),
 ("CFPL","9917","D-39","MISS MACH WEI","Wet Dates Seedless Khalas","Savla",29,290,140),
 ("CFPL","9903","D-39","-","Wet Dates Seedless Khalas","Savla",471,4710,140),
 ("CDPL","128496","Rishi","Zahidi","Wet Dates Zahidi","Rishi",612,6120,110),
]

def canon_wh(unit, storage):
    return "Rishi" if storage == "Rishi" else f"Savla {unit}"

def canon_subgroup(desc):
    d = desc.lower()
    for kw, sg in (("medjoul","Medjoul"),("zahidi","Zahidi"),("khalas","Khalas"),
                   ("ajwa","Ajwa"),("mabroom","Mabroom"),("safawi","Safawi")):
        if kw in d:
            return sg
    return None

COLS = ("inward_dt, unit, inward_no, vakkal, lot_no, no_of_cartons, weight_kg, "
        "total_inventory_kgs, item_description, storage_location, last_purchase_rate, value, "
        "box_id, transaction_no, auto_created_from_inward, spl_remarks, "
        "canonical_warehouse, canonical_group, canonical_subgroup, created_at, updated_at")
PARAMS = (":inward_dt, :unit, :inward_no, :vakkal, :lot_no, :no_of_cartons, :weight_kg, "
          ":total_inventory_kgs, :item_description, :storage_location, :last_purchase_rate, :value, "
          ":box_id, :transaction_no, :auto_created_from_inward, :spl_remarks, "
          ":canonical_warehouse, :canonical_group, :canonical_subgroup, NOW(), NOW()")

def gen_base(conn, table, used):
    n = int(time.time() * 1000)
    while True:
        base = str(n)[-8:]
        clash = conn.execute(text(f"SELECT 1 FROM {table} WHERE box_id LIKE :p LIMIT 1"),
                             {"p": f"{base}-%"}).scalar()
        if base not in used and not clash:
            used.add(base); return base
        n += 137

now0 = datetime.now()
used = set()
plan = []

with engine.connect() as conn:
    # GUARD: every lot absent from BOTH cold tables.
    for ent, lot, *_ in SPEC:
        for tbl in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
            pre = conn.execute(text(f"SELECT COUNT(*) FROM {tbl} WHERE TRIM(lot_no)=:l"), {"l": lot}).scalar()
            if pre:
                raise SystemExit(f"ABORT: lot {lot} already has {pre} rows in {tbl}; refusing (would double-count).")

    for idx, (ent, lot, unit, vak, desc, store, cartons, qty, rate) in enumerate(SPEC):
        table = "cfpl_cold_stocks" if ent == "CFPL" else "cdpl_cold_stocks"
        wpb = round(qty / cartons, 3)
        vpb = round(rate * wpb, 2)
        txn = "TR-" + (now0 + timedelta(seconds=idx)).strftime("%Y%m%d%H%M%S")
        base = gen_base(conn, table, used)
        rows = []
        for n in range(1, cartons + 1):
            rows.append({
                "inward_dt": INWARD_DT, "unit": unit, "inward_no": ("Rishi Cold" if store=="Rishi" else "Savla Cold"),
                "vakkal": (None if vak == "-" else vak), "lot_no": lot, "no_of_cartons": 1,
                "weight_kg": wpb, "total_inventory_kgs": wpb, "item_description": desc,
                "storage_location": store, "last_purchase_rate": rate, "value": vpb,
                "box_id": f"{base}-{n}", "transaction_no": txn,
                "auto_created_from_inward": False,
                "spl_remarks": "IMS-only reconciliation 2026-06-14 (Cold_and_IMS_Comparison_14Jun2026)",
                "canonical_warehouse": canon_wh(unit, store), "canonical_group": "Dates",
                "canonical_subgroup": canon_subgroup(desc),
            })
        plan.append((ent, lot, table, base, txn, wpb, vpb, rows))

print(f"{'ENT':4} {'LOT':>7} {'TABLE':16} {'BOX':>5} {'KG':>6} {'VAL':>8}  txn / box_id range")
print("-"*100)
grand = 0
for ent, lot, table, base, txn, wpb, vpb, rows in plan:
    print(f"{ent:4} {lot:>7} {table:16} {len(rows):>5} {wpb:>6} {vpb:>8}  {txn} | {base}-1..{base}-{len(rows)}")
    grand += len(rows)
print(f"\nTotal rows to insert: {grand}  (cfpl + cdpl cold_stocks)")

if not APPLY:
    print("\nDRY RUN — nothing written. Re-run with --apply to commit.")
    raise SystemExit(0)

inserted = []
with engine.begin() as conn:
    # Re-assert guard inside the txn.
    for ent, lot, table, base, txn, wpb, vpb, rows in plan:
        for tbl in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
            pre = conn.execute(text(f"SELECT COUNT(*) FROM {tbl} WHERE TRIM(lot_no)=:l"), {"l": lot}).scalar()
            assert pre == 0, f"ABORT: lot {lot} now has {pre} in {tbl}"
        conn.execute(text(f"INSERT INTO {table} ({COLS}) VALUES ({PARAMS})"), rows)
        ids = conn.execute(
            text(f"SELECT id FROM {table} WHERE transaction_no=:t ORDER BY id"), {"t": txn}
        ).scalars().all()
        chk = len(ids)
        assert chk == len(rows), f"ABORT: lot {lot} inserted {chk} != {len(rows)}"
        inserted.append({"ent": ent, "lot": lot, "table": table, "txn": txn, "base": base,
                         "n": len(rows), "ids": ids})
        print(f"  inserted {len(rows):>4} rows  {ent} {lot:>7} -> {table}  txn={txn}")

stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
rpath = os.path.join(os.path.dirname(__file__), f"insert_onlyinims_groupA_{stamp}.json")
with open(rpath, "w", encoding="utf-8") as f:
    json.dump({"inward_dt": INWARD_DT, "inserted": inserted}, f, indent=2, default=str)
print(f"\nCOMMITTED {sum(i['n'] for i in inserted)} rows. Rollback record: {rpath}")
