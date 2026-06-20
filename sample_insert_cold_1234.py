"""
SAMPLE INSERT (user-requested, reversible): mimic what a bulk-sticker COLD inward
SHOULD do — 10 boxes of lot 1234 into cfpl_cold_stocks. Clearly marked; delete anytime.

DELETE:  DELETE FROM cfpl_cold_stocks WHERE transaction_no = 'TEST-COLDFIX-1234';
   (or)  DELETE FROM cfpl_cold_stocks WHERE lot_no = '1234' AND inward_no = 'TEST-COLDFIX-1234';
"""
import io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import psycopg2

env = {}
with open(".env", encoding="utf-8") as f:
    for ln in f:
        ln = ln.strip()
        if ln and not ln.startswith("#") and "=" in ln:
            k, v = ln.split("=", 1); env[k.strip()] = v.strip()
conn = psycopg2.connect(host=env["DB_HOST"], port=int(env.get("DB_PORT", "5432")),
                        dbname=env["DB_NAME"], user=env["DB_USER"], password=env["DB_PASSWORD"])
cur = conn.cursor()

TXNO = "TEST-COLDFIX-1234"
LOT = "1234"
WH = "Savla D-39"
UNIT = "D-39"
NBOX = 10
PER_BOX_KG = 10.0
RATE = 100.0

# safety: refuse if this test txn already there (avoid dup)
cur.execute("SELECT count(*) FROM cfpl_cold_stocks WHERE transaction_no=%s", (TXNO,))
if cur.fetchone()[0]:
    print(f"Already present ({TXNO}) — nothing inserted. Delete first if you want a fresh run.")
    conn.close(); sys.exit(0)

rows = []
for i in range(1, NBOX + 1):
    rows.append({
        "inward_dt": "2026-06-05", "unit": UNIT, "inward_no": TXNO,
        "cold_item_mark": "TEST", "vakkal": "TEST", "lot_no": LOT,
        "no_of_cartons": 1, "weight_kg": PER_BOX_KG, "total_inventory_kgs": PER_BOX_KG,
        "group_name": "TEST", "item_description": "TEST SAMPLE - DELETE ME (cold-mirror demo)",
        "storage_location": WH, "exporter": "TEST", "last_purchase_rate": RATE,
        "box_id": f"TEST1234-{i}", "transaction_no": TXNO,
        "item_subgroup": "TEST", "item_mark": "TEST", "value": round(PER_BOX_KG * RATE, 2),
        "inward_transaction_no": TXNO, "auto_created_from_inward": True,
        "spl_remarks": "SAMPLE INSERT (Claude) — bulk-sticker cold-mirror demo; safe to delete",
        "canonical_warehouse": WH, "canonical_group": "TEST", "canonical_subgroup": "TEST",
    })

cur.executemany("""
    INSERT INTO cfpl_cold_stocks (
        inward_dt, unit, inward_no, cold_item_mark, vakkal, lot_no,
        no_of_cartons, weight_kg, total_inventory_kgs, group_name,
        item_description, storage_location, exporter, last_purchase_rate,
        box_id, transaction_no, item_subgroup, item_mark, value,
        inward_transaction_no, auto_created_from_inward, spl_remarks,
        canonical_warehouse, canonical_group, canonical_subgroup
    ) VALUES (
        %(inward_dt)s, %(unit)s, %(inward_no)s, %(cold_item_mark)s, %(vakkal)s, %(lot_no)s,
        %(no_of_cartons)s, %(weight_kg)s, %(total_inventory_kgs)s, %(group_name)s,
        %(item_description)s, %(storage_location)s, %(exporter)s, %(last_purchase_rate)s,
        %(box_id)s, %(transaction_no)s, %(item_subgroup)s, %(item_mark)s, %(value)s,
        %(inward_transaction_no)s, %(auto_created_from_inward)s, %(spl_remarks)s,
        %(canonical_warehouse)s, %(canonical_group)s, %(canonical_subgroup)s
    )
""", rows)
conn.commit()

cur.execute("""SELECT count(*), sum(no_of_cartons), sum(total_inventory_kgs),
                      string_agg(box_id, ', ' ORDER BY box_id)
               FROM cfpl_cold_stocks WHERE transaction_no=%s""", (TXNO,))
n, cart, kg, bids = cur.fetchone()
print("INSERTED into cfpl_cold_stocks:")
print(f"  transaction_no = {TXNO}")
print(f"  lot_no = {LOT} | warehouse/unit = {WH}/{UNIT}")
print(f"  rows = {n} (each 1 carton) | total cartons = {cart} | total kg = {kg}")
print(f"  box_ids = {bids}")
print("\nDELETE when done:")
print(f"  DELETE FROM cfpl_cold_stocks WHERE transaction_no = '{TXNO}';")
conn.close()
print("\nDONE.")
