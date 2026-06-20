"""READ-ONLY: is_matched/reconciled + per-lot OUT/IN/pending for TRANS202606051358."""
import io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import psycopg2
from collections import Counter

CH = "TRANS202606051358"
env = {}
with open(".env", encoding="utf-8") as f:
    for ln in f:
        ln = ln.strip()
        if ln and not ln.startswith("#") and "=" in ln:
            k, v = ln.split("=", 1); env[k.strip()] = v.strip()
c = psycopg2.connect(host=env["DB_HOST"], port=int(env.get("DB_PORT", "5432")),
                     dbname=env["DB_NAME"], user=env["DB_USER"], password=env["DB_PASSWORD"]).cursor()
def q(s, a=None): c.execute(s, a or ()); return c.fetchall()

hid = q("SELECT id FROM interunit_transfers_header WHERE challan_no=%s", (CH,))[0][0]
out = q("SELECT box_id, transaction_no, lot_number FROM interunit_transfer_boxes WHERE header_id=%s", (hid,))
inh = q("SELECT id FROM interunit_transfer_in_header WHERE transfer_out_id=%s OR transfer_out_no=%s", (hid, CH))
inb = []
for ih in inh:
    inb += q("""SELECT box_id, transaction_no, lot_number, original_box_id, is_matched, reconciled, scan_source, issue
                FROM interunit_transfer_in_boxes WHERE header_id=%s""", (ih[0],))

print(f"OUT boxes={len(out)} | IN boxes={len(inb)}")
print("\nIN is_matched:", dict(Counter(r[4] for r in inb)))
print("IN reconciled:", dict(Counter(r[5] for r in inb)))
print("IN scan_source:", dict(Counter(r[6] for r in inb)))
print("IN has issue (non-null):", sum(1 for r in inb if r[7]))
print("IN original_box_id set:", sum(1 for r in inb if r[3]), "/ null:", sum(1 for r in inb if not r[3]))

print("\nPer (txn,lot): OUT | IN | pending(In Transit) | cold_present")
out_lot = Counter((r[1], r[2]) for r in out)
in_lot = Counter((r[1], r[2]) for r in inb)
for k in sorted(set(out_lot) | set(in_lot), key=lambda x: str(x)):
    txn, lot = k
    pend = q("""SELECT count(*) FROM pending_transfer_stock
                WHERE transfer_out_challan_no=%s AND transaction_no=%s AND lot_no=%s""", (CH, txn, lot))[0][0]
    cold = 0
    for t in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
        cold += q(f"SELECT count(*) FROM {t} WHERE transaction_no=%s AND lot_no=%s", (txn, lot))[0][0]
    print(f"  txn={txn} lot={lot}: OUT={out_lot.get(k,0)} IN={in_lot.get(k,0)} pending={pend} cold_now={cold}")

# pending overall for this challan
print("\npending_transfer_stock totals for challan:",
      q("SELECT status, count(*) FROM pending_transfer_stock WHERE transfer_out_challan_no=%s GROUP BY status", (CH,)))
# transfer_box_reconciliation
tbr = q("""SELECT reconciliation_status, count(*) FROM transfer_box_reconciliation
           WHERE transfer_in_id IN (SELECT id FROM interunit_transfer_in_header WHERE transfer_out_no=%s)
           GROUP BY reconciliation_status""", (CH,))
print("transfer_box_reconciliation:", tbr)
print("\nDONE (read-only).")
