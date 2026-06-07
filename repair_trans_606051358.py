"""
REPAIR TRANS202606051358 (warehouse A185 -> W202, 66 boxes).

Bug: transfer_in_boxes were saved with transaction_no=NULL + relabeled box_ids,
so pick_from_pending matched 0 -> all 66 stuck 'In Transit', 1 box lost its lot.

Fix: rebuild the 66 transfer_in_boxes straight FROM the pending rows (authoritative
dispatched boxes: real box_id + transaction_no + lot), clear those pending rows,
flip IN header + Transfer OUT header to 'Received'.

SAFE: DRY-RUN by default. Set REPAIR_APPLY=true to commit. Backs up old rows to JSON.
"""
import io, sys, os, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import psycopg2
from psycopg2.extras import RealDictCursor
from collections import Counter
from datetime import datetime

CH = "TRANS202606051358"
APPLY = os.environ.get("REPAIR_APPLY", "").lower() == "true"

env = {}
with open(".env", encoding="utf-8") as f:
    for ln in f:
        ln = ln.strip()
        if ln and not ln.startswith("#") and "=" in ln:
            k, v = ln.split("=", 1); env[k.strip()] = v.strip()
conn = psycopg2.connect(host=env["DB_HOST"], port=int(env.get("DB_PORT", "5432")),
                        dbname=env["DB_NAME"], user=env["DB_USER"], password=env["DB_PASSWORD"])
cur = conn.cursor(cursor_factory=RealDictCursor)
def q(s, a=None): cur.execute(s, a or ()); return cur.fetchall()

print(f"=== {'APPLY' if APPLY else 'DRY-RUN'} repair {CH} @ {datetime.now()} ===\n")

hdr = q("SELECT id, status, from_site, to_site FROM interunit_transfers_header WHERE challan_no=%s", (CH,))[0]
hid = hdr["id"]
inh = q("SELECT id, status, grn_number FROM interunit_transfer_in_header WHERE transfer_out_id=%s", (hid,))[0]
in_hid = inh["id"]
print(f"Transfer OUT id={hid} status={hdr['status']} {hdr['from_site']}->{hdr['to_site']}")
print(f"IN header id={in_hid} status={inh['status']} grn={inh['grn_number']}")

pend = q("""SELECT id, box_id, transaction_no, lot_no, article, net_weight, gross_weight,
                   weight_kg, destination_table, source_table
            FROM pending_transfer_stock
            WHERE transfer_out_id=%s AND status='In Transit'
            ORDER BY id""", (hid,))
print(f"\nPending 'In Transit' rows: {len(pend)}")
print("  destination_table:", dict(Counter(p["destination_table"] for p in pend)))
print("  per (txn,lot):", dict(Counter((p["transaction_no"], p["lot_no"]) for p in pend)))

old_in = q("SELECT * FROM interunit_transfer_in_boxes WHERE header_id=%s ORDER BY id", (in_hid,))
print(f"\nExisting (bad) IN boxes: {len(old_in)}  (txn NULL: {sum(1 for r in old_in if not r['transaction_no'])})")

# map OUT (box_id,txn) -> interunit_transfer_boxes.id  for transfer_out_box_id FK
outb = q("SELECT id, box_id, transaction_no FROM interunit_transfer_boxes WHERE header_id=%s", (hid,))
out_map = {(r["box_id"], r["transaction_no"]): r["id"] for r in outb}

# guard: this repair only handles warehouse-destination (transfer_in_boxes = final state)
cold_dest = [p for p in pend if (p["destination_table"] or "").endswith("_cold_stocks")]
print(f"\nCold-destination pending rows (would need cold insert): {len(cold_dest)}")
if cold_dest:
    print("  !! cold dest present — this script only handles warehouse dest. ABORT.")
    conn.close(); sys.exit(1)

print(f"\nPLAN:")
print(f"  1. backup {len(old_in)} old IN boxes + {len(pend)} pending rows -> JSON")
print(f"  2. DELETE {len(old_in)} bad IN boxes (header_id={in_hid})")
print(f"  3. INSERT {len(pend)} proper IN boxes from pending (real box_id+txn+lot, is_matched=true, reconciled=true)")
print(f"  4. DELETE {len(pend)} pending rows (warehouse dest -> transfer_in_boxes is final)")
print(f"  5. UPDATE IN header {in_hid} -> 'Received'; Transfer OUT {hid} -> 'Received'")

if not APPLY:
    print("\nDRY-RUN only. Set REPAIR_APPLY=true to commit.")
    conn.close(); sys.exit(0)

# ---- APPLY ----
backup = {"transfer": CH, "ts": datetime.now().isoformat(),
          "old_in_boxes": [dict(r) for r in old_in],
          "pending_rows": [dict(r) for r in pend]}
bpath = f"repair_backup_{CH}.json"
with open(bpath, "w", encoding="utf-8") as f:
    json.dump(backup, f, default=str, indent=2)
print(f"\nBackup saved -> {bpath}")

w = conn.cursor()
# 2) delete bad IN boxes
w.execute("DELETE FROM interunit_transfer_in_boxes WHERE header_id=%s", (in_hid,))
# 3) insert proper IN boxes from pending
now = datetime.now()
for p in pend:
    w.execute("""
        INSERT INTO interunit_transfer_in_boxes
            (header_id, transfer_out_box_id, box_id, article, batch_number, lot_number,
             transaction_no, net_weight, gross_weight, scanned_at, is_matched,
             created_at, updated_at, reconciled, scan_source)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,true,%s,%s,true,'repair-claude')
    """, (in_hid, out_map.get((p["box_id"], p["transaction_no"])), p["box_id"], p["article"],
          None, p["lot_no"], p["transaction_no"],
          p["net_weight"] if p["net_weight"] is not None else p["weight_kg"],
          p["gross_weight"], now, now, now))
# 4) delete pending rows (warehouse dest -> final state already in transfer_in_boxes)
w.execute("DELETE FROM pending_transfer_stock WHERE transfer_out_id=%s AND status='In Transit'", (hid,))
# 5) finalize headers
w.execute("UPDATE interunit_transfer_in_header SET status='Received', received_at=CURRENT_TIMESTAMP WHERE id=%s", (in_hid,))
w.execute("UPDATE interunit_transfers_header SET status='Received', updated_ts=CURRENT_TIMESTAMP WHERE id=%s", (hid,))
conn.commit()
print("COMMITTED.")

# ---- verify ----
v = conn.cursor(cursor_factory=RealDictCursor)
v.execute("SELECT count(*) n, count(transaction_no) txn FROM interunit_transfer_in_boxes WHERE header_id=%s", (in_hid,))
r = v.fetchone(); print(f"\nVerify IN boxes: {r['n']} rows, {r['txn']} with txn")
v.execute("SELECT count(*) n FROM pending_transfer_stock WHERE transfer_out_id=%s AND status='In Transit'", (hid,))
print("Verify pending In Transit remaining:", v.fetchone()["n"])
v.execute("SELECT status FROM interunit_transfers_header WHERE id=%s", (hid,))
print("Transfer OUT status:", v.fetchone()["status"])
v.execute("SELECT status FROM interunit_transfer_in_header WHERE id=%s", (in_hid,))
print("Transfer IN status:", v.fetchone()["status"])
conn.close()
print("\nDONE.")
