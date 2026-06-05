"""READ-ONLY: trace box-id matching for txn TR-20260314174659 and the two box ids."""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)

TXN = "TR-20260314174659"
BOXES = ("90619000-90", "90596000-1")


def run(t, sql, p=None, limit=60):
    print(f"\n== {t} ==")
    with e.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        rows = c.execute(text(sql), p or {}).fetchall()
        if not rows:
            print("  (none)"); return
        cols = rows[0]._mapping.keys()
        print("  " + " | ".join(cols))
        for r in rows[:limit]:
            print("  " + " | ".join(str(r._mapping[k]) for k in cols))
        if len(rows) > limit:
            print(f"  ... (+{len(rows)-limit} more)")


# Which transfer header owns this txn (OUT side)?
run("OUT header(s) for txn", """
    SELECT DISTINCT b.header_id, h.challan_no, h.status, h.from_site, h.to_site
    FROM interunit_transfer_boxes b JOIN interunit_transfers_header h ON h.id=b.header_id
    WHERE b.transaction_no=:t
""", {"t": TXN})

# OUT boxes for txn — distinct box_ids + dup counts
run("OUT boxes for txn (distinct box_id, row copies)", """
    SELECT box_id, COUNT(*) rows, MIN(lot_number) lot, MIN(article) article
    FROM interunit_transfer_boxes WHERE transaction_no=:t
    GROUP BY box_id ORDER BY box_id
""", {"t": TXN})

# IN boxes for txn — what was acknowledged
run("IN boxes for txn (interunit_transfer_in_boxes)", """
    SELECT ib.header_id, ti.transfer_out_id, ib.box_id, ib.is_matched,
           ib.transfer_out_box_id, ib.original_box_id, ib.reconciled, ib.lot_number, ib.article
    FROM interunit_transfer_in_boxes ib
    LEFT JOIN interunit_transfer_in_header ti ON ti.id=ib.header_id
    WHERE ib.transaction_no=:t ORDER BY ib.box_id
""", {"t": TXN})

# pending rows for txn
run("pending_transfer_stock for txn", """
    SELECT transfer_out_id, box_id, status, item_description, lot_no, dispatched_by, reconciled, original_box_id
    FROM pending_transfer_stock WHERE transaction_no=:t ORDER BY box_id
""", {"t": TXN})

# reconciliation ledger for txn
run("transfer_box_reconciliation for txn", """
    SELECT transfer_in_id, transfer_out_id, original_box_id, actual_box_id,
           reconciliation_status, scan_source, lot_no
    FROM transfer_box_reconciliation WHERE transaction_no=:t ORDER BY id
""", {"t": TXN})

# Specific boxes — where does each live across all tables?
for b in BOXES:
    run(f"box {b}: presence across tables (txn-scoped where relevant)", """
        SELECT 'OUT_boxes' src, COUNT(*) n FROM interunit_transfer_boxes WHERE box_id=:b AND transaction_no=:t
        UNION ALL SELECT 'IN_boxes', COUNT(*) FROM interunit_transfer_in_boxes WHERE box_id=:b AND transaction_no=:t
        UNION ALL SELECT 'pending_InTransit', COUNT(*) FROM pending_transfer_stock WHERE box_id=:b AND transaction_no=:t AND status='In Transit'
        UNION ALL SELECT 'cfpl_cold', COUNT(*) FROM cfpl_cold_stocks WHERE box_id=:b AND transaction_no=:t
        UNION ALL SELECT 'cdpl_cold', COUNT(*) FROM cdpl_cold_stocks WHERE box_id=:b AND transaction_no=:t
        UNION ALL SELECT 'recon_actual', COUNT(*) FROM transfer_box_reconciliation WHERE actual_box_id=:b AND transaction_no=:t
        UNION ALL SELECT 'recon_original', COUNT(*) FROM transfer_box_reconciliation WHERE original_box_id=:b AND transaction_no=:t
    """, {"b": b, "t": TXN})

print("\nDone (read-only).")
