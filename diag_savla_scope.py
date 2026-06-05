"""READ-ONLY: determine reconciliation scope. Parse physical sheet + probe DB. No writes."""
import io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import openpyxl, psycopg2
from collections import Counter, defaultdict

# ---------- parse env ----------
env = {}
with open(".env", encoding="utf-8") as f:
    for ln in f:
        ln = ln.strip()
        if ln and not ln.startswith("#") and "=" in ln:
            k, v = ln.split("=", 1); env[k.strip()] = v.strip()
conn = psycopg2.connect(host=env["DB_HOST"], port=int(env.get("DB_PORT", "5432")),
                        dbname=env["DB_NAME"], user=env["DB_USER"], password=env["DB_PASSWORD"])
cur = conn.cursor()

# ---------- parse sheet ----------
wb = openpyxl.load_workbook("../Corrections/Savla_Rishi_Inventory_4th June 2026.xlsx",
                            read_only=True, data_only=True)
ws = wb["4th June 26"]
rows = list(ws.iter_rows(min_row=7, values_only=True))

def num(x):
    try: return float(x)
    except: return 0.0

data = []
for r in rows:
    lot = r[5]
    if lot is None or str(lot).strip() == "":
        continue
    # skip obvious total/footer rows: require a unit or inward date present
    data.append({
        "inward_dt": r[0], "unit": (str(r[1]).strip() if r[1] else ""),
        "inward_no": (str(r[2]).strip() if r[2] else ""),
        "lot": str(lot).strip(), "cartons": num(r[6]), "wt_kg": num(r[7]),
        "total_kg": num(r[8]), "group": r[9], "subgroup": r[10],
        "tally": r[13], "company": (str(r[14]).strip() if r[14] else ""),
        "loc": (str(r[15]).strip() if r[15] else ""),
    })

print(f"PHYSICAL SHEET: {len(data)} data rows")
print("  Company Name:", dict(Counter(d["company"] for d in data)))
print("  Storage Location:", dict(Counter(d["loc"] for d in data)))
print("  Unit:", dict(Counter(d["unit"] for d in data)))
print(f"  total cartons: {sum(d['cartons'] for d in data):.1f}")
print(f"  total kg: {sum(d['total_kg'] for d in data):.1f}")
lots = [d["lot"] for d in data]
print(f"  distinct lots: {len(set(lots))}  (rows {len(lots)})")
dups = [l for l, c in Counter(lots).items() if c > 1]
print(f"  lots appearing in >1 row: {len(dups)} e.g. {dups[:10]}")

# lot+unit combos
lotunit = Counter((d["lot"], d["unit"]) for d in data)
# does any single lot span >1 unit?
lot2units = defaultdict(set)
for d in data:
    lot2units[d["lot"]].add(d["unit"])
span = {l: u for l, u in lot2units.items() if len(u) > 1}
print(f"  lots spanning >1 unit: {len(span)} e.g. {dict(list(span.items())[:5])}")

# ---------- probe DB for these lots ----------
lotset = list(set(lots))
print("\nDB PROBE — where do the sheet's lots live?")
for tbl in ("cdpl_cold_stocks", "cfpl_cold_stocks"):
    cur.execute(
        f"SELECT lot_no, count(*), COALESCE(sum(total_inventory_kgs),0), "
        f"  string_agg(DISTINCT COALESCE(storage_location,'?'),'|'), "
        f"  string_agg(DISTINCT COALESCE(unit,'?'),'|') "
        f"FROM {tbl} WHERE lot_no = ANY(%s) GROUP BY lot_no", (lotset,))
    res = cur.fetchall()
    matched_lots = {r[0] for r in res}
    tot_cart = sum(r[1] for r in res)
    tot_kg = sum(float(r[2]) for r in res)
    print(f"  {tbl}: {len(matched_lots)} of {len(lotset)} sheet-lots present | "
          f"cartons={tot_cart} kg={tot_kg:.1f}")
    # storage_location distribution of matched
    loc_ct = Counter()
    for r in res:
        loc_ct[r[3]] += 1
    print(f"    matched-lot storage_location combos: {dict(loc_ct.most_common(12))}")

# lots in sheet but in NEITHER table
cur.execute("SELECT DISTINCT lot_no FROM cdpl_cold_stocks WHERE lot_no = ANY(%s)", (lotset,))
in_cdpl = {r[0] for r in cur.fetchall()}
cur.execute("SELECT DISTINCT lot_no FROM cfpl_cold_stocks WHERE lot_no = ANY(%s)", (lotset,))
in_cfpl = {r[0] for r in cur.fetchall()}
neither = set(lotset) - in_cdpl - in_cfpl
print(f"\n  sheet-lots in NEITHER cold table: {len(neither)} e.g. {sorted(neither)[:20]}")
both = in_cdpl & in_cfpl
print(f"  sheet-lots in BOTH tables (ambiguous): {len(both)} e.g. {sorted(both)[:20]}")

conn.close()
print("\nDONE (read-only).")
