"""READ-ONLY inspection of the two GRNs that finalize_grns.py --confirm would finalize.
Shows header sites/status, the destination_table of their In-Transit pending rows, and
current transfer-OUT status. Writes nothing (rolls back)."""
from shared.database import SessionLocal
from sqlalchemy import text

TIDS = [419, 486]  # transfer_out ids for GRN 195 and GRN 248

db = SessionLocal()
try:
    for tid in TIDS:
        h = db.execute(text(
            "SELECT id, challan_no, from_site, to_site, status FROM interunit_transfers_header WHERE id=:t"
        ), {"t": tid}).fetchone()
        print(f"\n=== transfer_out {tid}: {h.challan_no} [{h.status}] {h.from_site} -> {h.to_site}")
        dests = db.execute(text(
            "SELECT destination_table, COUNT(*) n, COALESCE(SUM(no_of_cartons),0) cartons, "
            "COALESCE(SUM(weight_kg),0) kg "
            "FROM pending_transfer_stock WHERE transfer_out_id=:t AND status='In Transit' "
            "GROUP BY destination_table"
        ), {"t": tid}).fetchall()
        for d in dests:
            kind = "INSERT into cold_stocks + delete pending" if (d.destination_table or "").endswith("_cold_stocks") \
                   else "delete pending only (transfer_in boxes are final state)"
            print(f"    dest={d.destination_table} | rows={d.n} cartons={d.cartons} kg={d.kg} -> {kind}")
        acked = db.execute(text(
            "SELECT COUNT(*) FROM interunit_transfer_in_boxes b "
            "JOIN interunit_transfer_in_header hh ON hh.id=b.header_id WHERE hh.transfer_out_id=:t"
        ), {"t": tid}).scalar()
        print(f"    acknowledged transfer-in boxes = {acked}")
finally:
    db.rollback()
    db.close()
    print("\n(READ-ONLY — rolled back, nothing written.)")
