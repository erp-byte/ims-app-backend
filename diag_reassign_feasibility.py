"""FEASIBILITY (read-only): for each double/spurious box in a NOT-yet-received
transfer, can we pull a fresh AVAILABLE cold box of the same (item, lot, txn) to
reassign? Compares needed vs available, per (transfer, item, lot, txn).

Target = OUT boxes whose (box_id,txn,lot) is owned (in-transit/received) by ANOTHER
transfer, where THIS listing transfer is still Dispatch/Partial (needs receiving).
Available cold box = a cold_stocks row with the same item+lot+txn whose box_id is
NOT currently In-Transit in pending (i.e. genuinely free to dispatch).
"""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)

# Needed: spurious boxes in non-received transfers, grouped by (transfer,item,lot,txn)
NEEDED = text("""
    WITH dd AS (
        SELECT box_id, transaction_no, COALESCE(lot_number,'') lot
        FROM interunit_transfer_boxes WHERE box_id IS NOT NULL AND box_id<>''
        GROUP BY 1,2,3 HAVING COUNT(DISTINCT header_id)>1),
    occ AS (
        SELECT DISTINCT ob.header_id, ob.box_id, ob.transaction_no, ob.lot_number, ob.article,
            COALESCE(
              (SELECT ti.transfer_out_id FROM interunit_transfer_in_boxes ib JOIN interunit_transfer_in_header ti ON ti.id=ib.header_id
               WHERE ib.box_id=ob.box_id AND ib.transaction_no=ob.transaction_no AND TRIM(COALESCE(ib.lot_number,''))=TRIM(COALESCE(ob.lot_number,'')) LIMIT 1),
              (SELECT p.transfer_out_id FROM pending_transfer_stock p WHERE p.box_id=ob.box_id AND p.transaction_no=ob.transaction_no
               AND TRIM(COALESCE(p.lot_no,''))=TRIM(COALESCE(ob.lot_number,'')) AND p.status='In Transit' LIMIT 1)) owner
        FROM interunit_transfer_boxes ob
        JOIN dd ON dd.box_id=ob.box_id AND dd.transaction_no=ob.transaction_no AND dd.lot=COALESCE(ob.lot_number,''))
    SELECT o.header_id, h.challan_no, h.status, o.article, o.lot_number, o.transaction_no,
           COUNT(*) needed
    FROM occ o
    JOIN interunit_transfers_header h ON h.id=o.header_id
    WHERE o.owner IS NOT NULL AND o.owner <> o.header_id
      AND h.status IN ('Dispatch','Partial')
    GROUP BY o.header_id, h.challan_no, h.status, o.article, o.lot_number, o.transaction_no
    ORDER BY needed DESC
""")

# Available in cold for an (item, lot, txn): cold rows whose box_id is NOT In-Transit
AVAIL = text("""
    SELECT COUNT(*) FROM (
      SELECT s.box_id FROM cfpl_cold_stocks s
       WHERE TRIM(COALESCE(s.item_description,''))=TRIM(COALESCE(:item,'')) AND TRIM(COALESCE(s.lot_no,''))=TRIM(COALESCE(:lot,'')) AND s.transaction_no=:txn
      UNION ALL
      SELECT s.box_id FROM cdpl_cold_stocks s
       WHERE TRIM(COALESCE(s.item_description,''))=TRIM(COALESCE(:item,'')) AND TRIM(COALESCE(s.lot_no,''))=TRIM(COALESCE(:lot,'')) AND s.transaction_no=:txn
    ) a
    WHERE NOT EXISTS (SELECT 1 FROM pending_transfer_stock p
        WHERE p.box_id=a.box_id AND p.transaction_no=:txn AND p.status='In Transit')
""")

tot_needed = tot_avail = tot_short = 0
groups = 0
short_groups = []
with e.connect() as c:
    c.execute(text("SET TRANSACTION READ ONLY"))
    rows = c.execute(NEEDED).fetchall()
    for r in rows:
        m = r._mapping
        avail = c.execute(AVAIL, {"item": m["article"], "lot": m["lot_number"], "txn": m["transaction_no"]}).scalar() or 0
        needed = m["needed"]; short = max(0, needed - avail)
        tot_needed += needed; tot_avail += min(avail, needed); tot_short += short; groups += 1
        if short > 0:
            short_groups.append((m["header_id"], m["challan_no"], m["article"], m["lot_number"], m["transaction_no"], needed, avail, short))

print(f"Target transfers (Dispatch/Partial) with double boxes — groups: {groups}")
print(f"  Needed boxes (to reassign): {tot_needed}")
print(f"  Satisfiable from available cold stock: {tot_needed - tot_short}")
print(f"  SHORTFALL (no available cold box of same item+lot+txn): {tot_short}")
print(f"\nShortfall groups (cannot fully satisfy): {len(short_groups)}")
for g in short_groups[:25]:
    print(f"  transfer {g[0]} {g[1]} | {g[2]} lot {g[3]} txn {g[4]}: needed {g[5]}, avail {g[6]}, short {g[7]}")
print("\nDone (read-only).")
