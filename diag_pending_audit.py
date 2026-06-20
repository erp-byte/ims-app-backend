"""READ-ONLY full audit of pending_transfer_stock logic + leakage checks."""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
eng = create_engine(DB)


def run(title, sql, params=None, limit=40):
    print(f"\n===== {title} =====")
    with eng.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        rows = c.execute(text(sql), params or {}).fetchall()
        if not rows:
            print("  (none)")
            return
        cols = rows[0]._mapping.keys()
        print("  " + " | ".join(cols))
        for r in rows[:limit]:
            print("  " + " | ".join(str(r._mapping[k]) for k in cols))
        if len(rows) > limit:
            print(f"  ... (+{len(rows)-limit} more)")


# A. Overview
run("A. rows by status / transfer_type", """
    SELECT status, transfer_type, COUNT(*) n,
           ROUND(SUM(weight_kg)::numeric,1) kg
    FROM pending_transfer_stock GROUP BY status, transfer_type ORDER BY n DESC
""")

# B. Tracking-only LINE rows
run("B. LINE- tracking-only rows (box-less transfers)", """
    SELECT status, COUNT(*) n FROM pending_transfer_stock
    WHERE box_id LIKE 'LINE-%' GROUP BY status
""")

# C. Duplicate (box_id, txn) — uniqueness should prevent, verify
run("C. duplicate (box_id, transaction_no) [should be none]", """
    SELECT box_id, transaction_no, COUNT(*) copies
    FROM pending_transfer_stock GROUP BY box_id, transaction_no
    HAVING COUNT(*) > 1 ORDER BY copies DESC
""")

# D. STALE: In Transit but transfer-out header already Received
run("D. STALE In-Transit whose OUT header = Received", """
    SELECT COUNT(*) stale_rows, COUNT(DISTINCT pts.transfer_out_id) transfers,
           ROUND(SUM(pts.weight_kg)::numeric,1) kg
    FROM pending_transfer_stock pts
    JOIN interunit_transfers_header h ON h.id = pts.transfer_out_id
    WHERE pts.status='In Transit' AND h.status='Received'
""")

# E. ORPHAN: In Transit but no matching OUT header
run("E. ORPHAN In-Transit with no OUT header", """
    SELECT COUNT(*) n FROM pending_transfer_stock pts
    LEFT JOIN interunit_transfers_header h ON h.id = pts.transfer_out_id
    WHERE pts.status='In Transit' AND h.id IS NULL
""")

# F. item_description vs article mismatch (box_id-fallback corruption signature)
run("F. article <> item_description (corruption signature)", """
    SELECT COUNT(*) mismatched, COUNT(DISTINCT transfer_out_id) transfers
    FROM pending_transfer_stock
    WHERE status='In Transit' AND article IS NOT NULL AND article <> ''
      AND lower(trim(article)) <> lower(trim(item_description))
""")

# G. source_table sanity: distribution + storage-type agreement
run("G. source_table vs from_storage_type", """
    SELECT source_table, from_storage_type, COUNT(*) n
    FROM pending_transfer_stock WHERE status='In Transit'
    GROUP BY source_table, from_storage_type ORDER BY n DESC
""")

# H. cold source but missing cold_storage_data snapshot (can't restore on cancel)
run("H. cold source rows with NULL cold_storage_data (restore risk)", """
    SELECT COUNT(*) n FROM pending_transfer_stock
    WHERE status='In Transit' AND from_storage_type='cold'
      AND box_id NOT LIKE 'LINE-%' AND cold_storage_data IS NULL
""")

# I. weight anomalies
run("I. In-Transit real boxes with zero/null weight_kg", """
    SELECT COUNT(*) n FROM pending_transfer_stock
    WHERE status='In Transit' AND box_id NOT LIKE 'LINE-%'
      AND COALESCE(weight_kg,0) = 0
""")

# J. blank company
run("J. In-Transit rows with blank/null from_company", """
    SELECT from_company, COUNT(*) n FROM pending_transfer_stock
    WHERE status='In Transit' AND box_id NOT LIKE 'LINE-%'
      AND COALESCE(from_company,'')='' GROUP BY from_company
""")

# K. LEAK CHECK 1: box parked In Transit but STILL present in source (never deducted = phantom)
run("K. PHANTOM: In-Transit box still present in a cold source table", """
    SELECT 'cfpl_cold_stocks' src, COUNT(*) n
    FROM pending_transfer_stock p
    JOIN cfpl_cold_stocks s ON s.box_id=p.box_id AND s.transaction_no=p.transaction_no
    WHERE p.status='In Transit'
    UNION ALL
    SELECT 'cdpl_cold_stocks', COUNT(*)
    FROM pending_transfer_stock p
    JOIN cdpl_cold_stocks s ON s.box_id=p.box_id AND s.transaction_no=p.transaction_no
    WHERE p.status='In Transit'
""")

# L. LEAK CHECK 2: box parked In Transit but ALSO already in destination (received not cleared)
run("L. DOUBLE: In-Transit box also already in a destination cold table", """
    SELECT 'cfpl_cold_stocks' dst, COUNT(*) n
    FROM pending_transfer_stock p
    JOIN cfpl_cold_stocks s ON s.box_id=p.box_id AND s.transaction_no=p.transaction_no
    WHERE p.status='In Transit' AND p.destination_table='cfpl_cold_stocks'
    UNION ALL
    SELECT 'cdpl_cold_stocks', COUNT(*)
    FROM pending_transfer_stock p
    JOIN cdpl_cold_stocks s ON s.box_id=p.box_id AND s.transaction_no=p.transaction_no
    WHERE p.status='In Transit' AND p.destination_table='cdpl_cold_stocks'
""")

# M. Cancelled rows lingering (should be deleted, not retained)
run("M. rows in status other than In Transit (Cancelled leftovers)", """
    SELECT status, COUNT(*) n FROM pending_transfer_stock
    WHERE status <> 'In Transit' GROUP BY status
""")

print("\nDone (read-only).")
