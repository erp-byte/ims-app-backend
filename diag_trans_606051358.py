"""READ-ONLY: forensic on TRANS202606051358 — 66 out vs 62 transfer_in. No writes."""
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

# header
h = q("""SELECT id, challan_no, status, from_site, to_site, stock_trf_date, unallocated_boxes
         FROM interunit_transfers_header WHERE challan_no=%s""", (CH,))
print("HEADER:", h)
if not h:
    print("not found"); sys.exit()
hid = h[0][0]

# OUT boxes
out = q("""SELECT box_id, transaction_no, lot_number, article, net_weight
           FROM interunit_transfer_boxes WHERE header_id=%s""", (hid,))
print(f"\nOUT boxes (interunit_transfer_boxes): {len(out)}")
out_keys = [(r[0], r[1], r[2]) for r in out]
dup = [(k, n) for k, n in Counter(out_keys).items() if n > 1]
print(f"  duplicate (box_id,txn,lot) in OUT: {len(dup)}")
for k, n in dup[:15]: print(f"    {k} x{n}")
dup_bid = [(r[0]) for r in out]
dupbid = [(k, n) for k, n in Counter(dup_bid).items() if n > 1]
print(f"  duplicate box_id alone in OUT: {len(dupbid)} -> {dupbid[:15]}")

# IN header(s)
inh = q("""SELECT id, grn_number, status, receiving_warehouse, received_at, transfer_out_no
           FROM interunit_transfer_in_header WHERE transfer_out_id=%s OR transfer_out_no=%s""", (hid, CH))
print(f"\nIN headers (interunit_transfer_in_header): {len(inh)}")
for r in inh: print("   ", r)

# IN boxes
inb = []
for ih in inh:
    rows = q("""SELECT box_id, transaction_no, lot_number, article, original_box_id, is_matched, reconciled
                FROM interunit_transfer_in_boxes WHERE header_id=%s""", (ih[0],))
    inb += rows
print(f"\nIN boxes (interunit_transfer_in_boxes): {len(inb)}")
in_bids = [r[0] for r in inb]
in_dupbid = [(k, n) for k, n in Counter(in_bids).items() if n > 1]
print(f"  duplicate box_id in IN: {len(in_dupbid)} -> {in_dupbid[:15]}")

# OUT box_ids NOT present in IN (the leaked ones)
out_bids = set(r[0] for r in out)
in_bidset = set(in_bids)
missing = out_bids - in_bidset
print(f"\nOUT box_ids NOT in IN ({len(missing)}):")
for b in sorted(missing):
    # show that out box's lot/txn
    info = [r for r in out if r[0] == b][0]
    print(f"    {b} | txn={info[1]} lot={info[2]} art={str(info[3])[:30]}")

# any IN box_ids not in OUT (relabels)?
extra = in_bidset - out_bids
print(f"\nIN box_ids NOT in OUT (relabel/extra) ({len(extra)}): {sorted(extra)[:15]}")

# pending for this transfer
pend = q("""SELECT status, count(*), COALESCE(sum(no_of_cartons),0)
            FROM pending_transfer_stock WHERE transfer_out_challan_no=%s GROUP BY status""", (CH,))
print(f"\npending_transfer_stock for {CH}: {pend}")

# unique constraints on transfer_in_boxes
cons = q("""SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
            WHERE conrelid='interunit_transfer_in_boxes'::regclass AND contype IN ('u','p')""")
print(f"\ntransfer_in_boxes unique/pk constraints:")
for r in cons: print("   ", r)
print("\nDONE (read-only).")
