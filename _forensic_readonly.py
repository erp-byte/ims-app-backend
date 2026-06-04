"""STRICTLY READ-ONLY forensic for the pending-stock mismatch.
No INSERT/UPDATE/DELETE. Session is forced read-only. Safe to run.

Credentials come from the environment (never hardcode prod secrets):
    export WMS_DB_HOST=... WMS_DB_NAME=... WMS_DB_USER=... WMS_DB_PASSWORD=...
"""
import os
import psycopg2

conn = psycopg2.connect(
    host=os.environ["WMS_DB_HOST"],
    port=int(os.environ.get("WMS_DB_PORT", "5432")),
    dbname=os.environ["WMS_DB_NAME"],
    user=os.environ["WMS_DB_USER"],
    password=os.environ["WMS_DB_PASSWORD"],
    connect_timeout=15,
)
conn.set_session(readonly=True, autocommit=True)  # hard guarantee: no writes
cur = conn.cursor()

CHALLANS = ['TRANS202605290848', 'TRANS202605281743']
LOTS = ['122909', '125320', '123296']

def q(sql, args=()):
    cur.execute(sql, args)
    return cur.fetchall()

for ch in CHALLANS:
    hdr = q("""SELECT id, from_site, to_site, status, created_ts
               FROM interunit_transfers_header WHERE challan_no=%s""", (ch,))
    if not hdr:
        print(f"\n### {ch}: NOT FOUND"); continue
    hid, frm, to, status, cts = hdr[0]
    print(f"\n{'='*70}\n### {ch}  (id={hid})  {frm} -> {to}  status={status}  {cts}")

    print("-- ORDER lines (interunit_transfers_lines) --")
    for r in q("""SELECT item_desc_raw, lot_number, qty, net_weight
                  FROM interunit_transfers_lines WHERE header_id=%s ORDER BY id""", (hid,)):
        print(f"   item={r[0]!r:45} lot={r[1]:>8} qty={r[2]:>5} net_wt={r[3]}")

    bx = q("""SELECT COUNT(*), COUNT(DISTINCT lot_number),
                     COALESCE(SUM(net_weight),0)
              FROM interunit_transfer_boxes WHERE header_id=%s""", (hid,))[0]
    print(f"-- BOX rows (interunit_transfer_boxes): count={bx[0]} lots={bx[1]} sum_net_wt={bx[2]}")

    pend = q("""SELECT COUNT(*), COALESCE(SUM(no_of_cartons),0),
                       COALESCE(SUM(weight_kg),0), COUNT(DISTINCT lot_no)
                FROM pending_transfer_stock WHERE transfer_out_id=%s AND status='In Transit'""", (hid,))[0]
    print(f"-- PARKED (pending_transfer_stock): rows={pend[0]} sum_cartons={pend[1]} "
          f"sum_kg={pend[2]} lots={pend[3]}")

    # how many transfer_boxes match cold_stocks STRICT (box_id+txn) vs not
    matched = q("""
        SELECT
          SUM(CASE WHEN c.hit THEN 1 ELSE 0 END) AS matched,
          SUM(CASE WHEN c.hit THEN 0 ELSE 1 END) AS unmatched
        FROM (
          SELECT b.box_id, b.transaction_no,
                 EXISTS(SELECT 1 FROM cfpl_cold_stocks s WHERE s.box_id=b.box_id AND s.transaction_no=b.transaction_no
                        UNION ALL SELECT 1 FROM cdpl_cold_stocks s WHERE s.box_id=b.box_id AND s.transaction_no=b.transaction_no) AS hit
          FROM interunit_transfer_boxes b WHERE b.header_id=%s
            AND COALESCE(b.box_id,'')<>'' AND COALESCE(b.transaction_no,'') NOT IN ('','DIRECT')
        ) c""", (hid,))[0]
    print(f"-- BOX_ID strict match vs cold_stocks: matched={matched[0]} unmatched={matched[1]}")

print(f"\n{'='*70}\n### CURRENT STOCK IN SHEETS BY LOT (cfpl + cdpl cold_stocks)")
for lot in LOTS:
    rows = q("""
        SELECT 'cfpl' AS co, COUNT(*), COALESCE(SUM(no_of_cartons),0), COALESCE(SUM(weight_kg),0)
        FROM cfpl_cold_stocks WHERE lot_no=%s
        UNION ALL
        SELECT 'cdpl', COUNT(*), COALESCE(SUM(no_of_cartons),0), COALESCE(SUM(weight_kg),0)
        FROM cdpl_cold_stocks WHERE lot_no=%s""", (lot, lot))
    print(f"   lot {lot}:")
    for co, n, cart, kg in rows:
        print(f"      {co}: rows={n} cartons={cart} kg={kg}")

# pending grouped by lot (what the +in-transit badge sums)
print(f"\n### PENDING (In Transit) BY LOT")
for lot in LOTS:
    r = q("""SELECT COUNT(*), COALESCE(SUM(no_of_cartons),0), COALESCE(SUM(weight_kg),0),
                    COUNT(DISTINCT transfer_out_challan_no)
             FROM pending_transfer_stock WHERE lot_no=%s AND status='In Transit'""", (lot,))[0]
    print(f"   lot {lot}: rows={r[0]} cartons={r[1]} kg={r[2]} challans={r[3]}")

conn.close()
print("\n[done - read-only, no changes made]")
