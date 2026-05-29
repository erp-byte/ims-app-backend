"""
READ-ONLY diagnostic for the Transfer-Out carton mismatch.

Checks whether the `pending_transfer_stock` 'In Transit' rows for lot 125860
(and any other lot) are real, or stale/duplicated. Runs SELECT-only queries
inside a read-only transaction so it cannot mutate production data.
"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.environ["DATABASE_URL"].replace("postgresql://", "postgresql+psycopg://", 1) \
    if os.environ.get("DATABASE_URL", "").startswith("postgresql://") \
    else os.environ["DATABASE_URL"]

LOT = os.environ.get("DIAG_LOT", "125860")

engine = create_engine(DB_URL)

def run(title, sql, params=None):
    print(f"\n===== {title} =====")
    with engine.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        rows = c.execute(text(sql), params or {}).fetchall()
        if not rows:
            print("  (no rows)")
            return
        cols = rows[0]._mapping.keys()
        print("  " + " | ".join(cols))
        for r in rows:
            print("  " + " | ".join(str(r._mapping[k]) for k in cols))

# 1) Raw pending rows for the lot — one row per box, with status + challan + receipt linkage
run("pending_transfer_stock rows for lot", f"""
    SELECT pts.id, pts.box_id, pts.transaction_no, pts.status,
           pts.no_of_cartons, pts.weight_kg,
           pts.transfer_out_id, pts.transfer_out_challan_no,
           pts.from_company, pts.from_site, pts.to_site,
           pts.dispatched_at
    FROM pending_transfer_stock pts
    WHERE pts.lot_no = :lot
    ORDER BY pts.status, pts.dispatched_at
""", {"lot": LOT})

# 2) Aggregate exactly as pending_by_lot() does (status = In Transit)
run("aggregate (what the UI subtracts) — In Transit only", f"""
    SELECT from_company,
           COUNT(*)                       AS box_rows,
           COALESCE(SUM(no_of_cartons),0) AS pending_cartons,
           COALESCE(SUM(weight_kg),0)     AS pending_kg
    FROM pending_transfer_stock
    WHERE lot_no = :lot AND status = 'In Transit'
    GROUP BY from_company
""", {"lot": LOT})

# 3) Duplicate box_id within In Transit (would double-count cartons)
run("duplicate box_id among In Transit rows", f"""
    SELECT box_id, transaction_no, COUNT(*) AS copies, SUM(no_of_cartons) AS cartons
    FROM pending_transfer_stock
    WHERE lot_no = :lot AND status = 'In Transit'
    GROUP BY box_id, transaction_no
    HAVING COUNT(*) > 1
    ORDER BY copies DESC
""", {"lot": LOT})

# 4) STALE check: In-Transit rows whose Transfer-Out already has a Received Transfer-In.
#    If the transfer was received but the pending row was never picked/cleared, it is stale.
run("possibly-stale In Transit rows (transfer-out already received)", f"""
    SELECT pts.transfer_out_id, pts.transfer_out_challan_no,
           COUNT(*) AS stuck_box_rows, SUM(pts.no_of_cartons) AS stuck_cartons,
           h.status AS header_status
    FROM pending_transfer_stock pts
    LEFT JOIN interunit_transfers_header h ON h.id = pts.transfer_out_id
    WHERE pts.lot_no = :lot AND pts.status = 'In Transit'
    GROUP BY pts.transfer_out_id, pts.transfer_out_challan_no, h.status
    ORDER BY stuck_cartons DESC
""", {"lot": LOT})

# 5) Current physical stock in cold_stocks for the lot (the dashboard number)
for tbl in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
    run(f"net stock in {tbl}", f"""
        SELECT lot_no, item_description,
               COUNT(*) AS box_rows,
               SUM(no_of_cartons) AS net_cartons,
               SUM(no_of_cartons * weight_kg) AS net_kg
        FROM {tbl}
        WHERE lot_no = :lot
        GROUP BY lot_no, item_description
    """, {"lot": LOT})

print("\nDone (read-only).")
