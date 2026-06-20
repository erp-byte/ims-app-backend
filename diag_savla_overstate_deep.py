"""READ-ONLY deep dive on OVERSTATED lots: duplicate cold rows vs inward vs migration date."""
import io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import openpyxl, psycopg2
from collections import Counter

env = {}
with open(".env", encoding="utf-8") as f:
    for ln in f:
        ln = ln.strip()
        if ln and not ln.startswith("#") and "=" in ln:
            k, v = ln.split("=", 1); env[k.strip()] = v.strip()
conn = psycopg2.connect(host=env["DB_HOST"], port=int(env.get("DB_PORT", "5432")),
                        dbname=env["DB_NAME"], user=env["DB_USER"], password=env["DB_PASSWORD"])
cur = conn.cursor()
def q(s, a=None): cur.execute(s, a or ()); return cur.fetchall()

wb = openpyxl.load_workbook("../Corrections/Savla_Rishi_Reconciliation_4thJun2026.xlsx", data_only=True)
ws = wb["Reconciliation"]; rows = list(ws.iter_rows(values_only=True)); hdr = rows[0]
ix = {h: i for i, h in enumerate(hdr)}
over = [(str(r[ix["Lot No"]]), r[ix["Company"]], r[ix["Phys Cartons"]], r[ix["DB Cartons"]], r[ix["Diff Ctn"]])
        for r in rows[1:] if r[ix["Status"]] == "SYSTEM OVERSTATED"]
lots = [o[0] for o in over]

# per lot: cold rows, distinct box_id, dup (box_id,txn), inward boxes_v2 count, created_at range, inward_txn set
def tbl_for(co): return "cdpl_cold_stocks" if co == "CDPL" else "cfpl_cold_stocks"
def boxtbl_for(co): return "cdpl_boxes_v2" if co == "CDPL" else "cfpl_boxes_v2"

print("lot      co   phys  db  excess | coldRows distBoxId dup(bid,txn) | inwardBoxes | coldCreated(min..max) | n_inwardTxn")
mig = Counter()
dupcount = 0
overbeyond = 0
for lot, co, phys, db, diff in sorted(over, key=lambda x: -x[4]):
    t = tbl_for(co); bt = boxtbl_for(co)
    n, dbid, dpair, cmin, cmax, ninwtxn = q(
        f"""SELECT count(*), count(DISTINCT box_id),
                   count(*)-count(DISTINCT (box_id,transaction_no)),
                   min(created_at), max(created_at), count(DISTINCT inward_transaction_no)
            FROM {t} WHERE lot_no=%s""", (lot,))[0]
    try:
        inwb = q(f"SELECT count(*) FROM {bt} WHERE lot_no=%s", (lot,))[0][0]
    except Exception:
        conn.rollback(); inwb = -1
    cminx = str(cmin)[:10] if cmin else "?"; cmaxx = str(cmax)[:10] if cmax else "?"
    mig[cminx] += 1
    if dpair and dpair > 0: dupcount += 1
    if inwb >= 0 and db > inwb: overbeyond += 1
    print(f"{lot:8} {co} {phys:5.0f} {db:4.0f} +{diff:<5.0f}| {n:7} {dbid:8} {dpair:11} | {inwb:9} | {cminx}..{cmaxx} | {ninwtxn}")

print(f"\nLots with duplicate (box_id,txn) rows in cold: {dupcount}/{len(over)}")
print(f"Lots where DB cold cartons > inward boxes_v2 (phantom beyond inward): {overbeyond}/{len(over)}")
print("\ncold-row min(created_at) date histogram (migration clustering):")
for d, c in mig.most_common():
    print(f"   {d}: {c} lots")

conn.close(); print("\nDONE (read-only).")
