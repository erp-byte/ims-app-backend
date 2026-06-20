"""Reusable bulk inserter for cfpl_cold_stocks / cdpl_cold_stocks.

Give an item_description + box_count (+ a few placement fields) and this inserts
one cold_stocks row per carton (no_of_cartons=1 each), generating box_ids with the
SYSTEM format:  {last 8 digits of epoch ms}-{box_number}   e.g. 50123456-1 ... -N
(one fresh base per run, sequential within the run; bumps the base if it collides).

Safety:
  * dry-run by default — prints what it WOULD insert; pass --apply to commit.
  * collision-checks the generated base against existing box_ids and bumps it.
  * honours UNIQUE (transaction_no, box_id).
  * writes a rollback record (inserted ids) to JSON on --apply.

Fill in the CONFIG block (or pass a JSON file via --config path.json) then run:
    python bulk_insert_cold_stocks.py            # dry run
    python bulk_insert_cold_stocks.py --apply    # commit
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

# ─────────────────────────── CONFIG ───────────────────────────
# Required fields you (the user) provide each run:
CONFIG = {
    "company":          "cdpl",          # "cfpl" | "cdpl"  -> picks the table
    "item_description": "Royal Delight Ajwa Dates 5 Kgs",
    "box_count":        33,              # IMS Cartons -> number of rows
    "weight_kg":        5,               # per carton = 165 kg / 33 cartons
    "lot_no":           "81466",         # Lot No (match key)
    "transaction_no":   "",              # "" -> auto TR-{inward YYYYMMDD}{HHMMSS}
    "unit":             "D-514",         # IMS Unit
    "storage_location": "Savla",         # IMS Storage
    # Optional — leave "" / None to store NULL:
    "inward_dt":              "2025-03-21",   # IMS Inward Dt 21/03/2025 -> ISO; drives TR- date
    "inward_no":              "GR8597",        # IMS Inward No
    "vakkal":                 "AJWA",           # IMS Vakkal
    "exporter":               None,
    "group_name":             None,
    "item_subgroup":          None,
    "item_mark":              None,
    "cold_item_mark":         None,
    "last_purchase_rate":     980,            # IMS Rate
    "value":                  4900,           # IMS Value 161700 / 33 = 4900 per carton
    "inward_transaction_no":  None,
    "spl_remarks":            None,
    "auto_created_from_inward": False,
}
# ──────────────────────────────────────────────────────────────

TABLE_MAP = {"cfpl": "cfpl_cold_stocks", "cdpl": "cdpl_cold_stocks"}
APPLY = "--apply" in sys.argv

# Optional external config: --config path/to.json
if "--config" in sys.argv:
    cfg_path = sys.argv[sys.argv.index("--config") + 1]
    with open(cfg_path, encoding="utf-8") as f:
        CONFIG.update(json.load(f))


def fail(msg):
    print(f"ABORT: {msg}")
    raise SystemExit(1)


# ---- validate ----
company = str(CONFIG["company"]).lower().strip()
if company not in TABLE_MAP:
    fail(f"company must be cfpl|cdpl, got {CONFIG['company']!r}")
table = TABLE_MAP[company]

if not CONFIG["item_description"] or CONFIG["item_description"] == "REPLACE ME":
    fail("item_description is required")
box_count = int(CONFIG["box_count"])
if box_count < 1:
    fail("box_count must be >= 1")
if not CONFIG["lot_no"]:
    fail("lot_no is required")


def unique_base(conn):
    """last 8 digits of epoch ms; bump until no existing box_id starts with base-."""
    base = str(int(time.time() * 1000))[-8:]
    for _ in range(50):
        hit = conn.execute(
            text(f"SELECT 1 FROM {table} WHERE box_id LIKE :pat LIMIT 1"),
            {"pat": f"{base}-%"},
        ).first()
        if not hit:
            return base
        base = str(int(base) + 1).zfill(8)[-8:]
        time.sleep(0.001)
    fail("could not find a non-colliding box_id base after 50 tries")


def make_txn(conn):
    """Use given txn, else TR-{inward YYYYMMDD}{HHMMSS} (system format: TR-YYYYMMDDHHMMSS).
    Date comes from inward_dt so backdated entries read the inward date, not today.
    HHMMSS from current time gives uniqueness; bump a second on collision."""
    if CONFIG["transaction_no"]:
        return CONFIG["transaction_no"]
    inw = CONFIG.get("inward_dt")
    datepart = (str(inw).replace("-", "")[:8] if inw else datetime.now().strftime("%Y%m%d"))
    for bump in range(60):
        cand = f"TR-{datepart}{datetime.now().strftime('%H%M%S')}"
        if bump:
            cand = f"TR-{datepart}{(int(datetime.now().strftime('%H%M%S')) + bump):06d}"
        exists = conn.execute(
            text(f"SELECT 1 FROM {table} WHERE transaction_no = :t LIMIT 1"), {"t": cand}
        ).first()
        if not exists:
            return cand
        time.sleep(1)
    fail("could not generate a unique TR- transaction_no")


with engine.begin() as c:
    base = unique_base(c)
    txn = make_txn(c)
    box_ids = [f"{base}-{i}" for i in range(1, box_count + 1)]

    rows = [{
        "box_id": bid,
        "transaction_no": txn,
        "lot_no": CONFIG["lot_no"],
        "item_description": CONFIG["item_description"],
        "no_of_cartons": 1,
        "weight_kg": CONFIG["weight_kg"],
        "total_inventory_kgs": CONFIG["weight_kg"],
        "unit": CONFIG["unit"] or None,
        "storage_location": CONFIG["storage_location"] or None,
        "inward_dt": CONFIG["inward_dt"] or None,
        "inward_no": CONFIG["inward_no"],
        "vakkal": CONFIG["vakkal"],
        "exporter": CONFIG["exporter"],
        "group_name": CONFIG["group_name"],
        "item_subgroup": CONFIG["item_subgroup"],
        "item_mark": CONFIG["item_mark"],
        "cold_item_mark": CONFIG["cold_item_mark"],
        "last_purchase_rate": CONFIG["last_purchase_rate"],
        "value": CONFIG["value"],
        "inward_transaction_no": CONFIG["inward_transaction_no"],
        "spl_remarks": CONFIG["spl_remarks"],
        "auto_created_from_inward": bool(CONFIG["auto_created_from_inward"]),
    } for bid in box_ids]

    print(f"Table:            {table}")
    print(f"Item:             {CONFIG['item_description']}")
    print(f"Lot:              {CONFIG['lot_no']}")
    print(f"Transaction:      {txn}")
    print(f"Cartons (rows):   {box_count}")
    print(f"Weight/carton:    {CONFIG['weight_kg']} kg   (total {round(float(CONFIG['weight_kg'] or 0) * box_count, 3)} kg)")
    print(f"Unit / location:  {CONFIG['unit']!r} / {CONFIG['storage_location']!r}")
    print(f"Box IDs:          {box_ids[0]} ... {box_ids[-1]}")

    if not APPLY:
        print("\nDRY RUN — nothing inserted. Re-run with --apply to commit.")
        print("Sample row:", json.dumps(rows[0], default=str, indent=2))
        raise SystemExit(0)

    # executemany INSERT (a RETURNING fetch is not supported in this mode);
    # read the ids back by the freshly-generated, unique transaction_no.
    c.execute(text(f"""
        INSERT INTO {table}
            (box_id, transaction_no, lot_no, item_description, no_of_cartons,
             weight_kg, total_inventory_kgs, unit, storage_location, inward_dt,
             inward_no, vakkal, exporter, group_name, item_subgroup, item_mark,
             cold_item_mark, last_purchase_rate, value, inward_transaction_no,
             spl_remarks, auto_created_from_inward, created_at, updated_at)
        VALUES
            (:box_id, :transaction_no, :lot_no, :item_description, :no_of_cartons,
             :weight_kg, :total_inventory_kgs, :unit, :storage_location, :inward_dt,
             :inward_no, :vakkal, :exporter, :group_name, :item_subgroup, :item_mark,
             :cold_item_mark, :last_purchase_rate, :value, :inward_transaction_no,
             :spl_remarks, :auto_created_from_inward, NOW(), NOW())
    """), rows)

    fetched = c.execute(text(
        f"SELECT id, box_id, lot_no FROM {table} WHERE transaction_no = :t ORDER BY id"
    ), {"t": txn}).fetchall()
    inserted = [dict(r._mapping) for r in fetched]
    assert len(inserted) == box_count, f"expected {box_count}, found {len(inserted)} for {txn}"
    print(f"\nINSERTED {len(inserted)} rows into {table}.")

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    rec = os.path.join(os.path.dirname(__file__), f"bulk_insert_cold_{company}_{stamp}.json")
    with open(rec, "w", encoding="utf-8") as f:
        json.dump({"table": table, "transaction_no": txn,
                   "inserted_ids": [r["id"] for r in inserted], "rows": inserted},
                  f, default=str, indent=2)
    print(f"Rollback record (DELETE these ids to undo): {rec}")
    print("COMMITTED.")
