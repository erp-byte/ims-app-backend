import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
eng = create_engine(DB)


def run(title, sql, limit=40):
    print(f"\n===== {title} =====")
    with eng.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        rows = c.execute(text(sql)).fetchall()
        if not rows:
            print("  (none)"); return
        cols = rows[0]._mapping.keys()
        print("  " + " | ".join(cols))
        for r in rows[:limit]:
            print("  " + " | ".join(str(r._mapping[k]) for k in cols))
        if len(rows) > limit:
            print(f"  ... (+{len(rows)-limit} more)")


# K-detail: phantom boxes — cold->cold (dup risk) vs cold->warehouse, by transfer
run("K-detail: phantom In-Transit boxes still in CDPL source", """
    SELECT p.transfer_out_id, p.transfer_out_challan_no,
           p.from_storage_type, p.to_storage_type, p.destination_table,
           COUNT(*) n
    FROM pending_transfer_stock p
    JOIN cdpl_cold_stocks s ON s.box_id=p.box_id AND s.transaction_no=p.transaction_no
    WHERE p.status='In Transit'
    GROUP BY p.transfer_out_id, p.transfer_out_challan_no,
             p.from_storage_type, p.to_storage_type, p.destination_table
    ORDER BY n DESC
""")
run("K-detail: phantom In-Transit boxes still in CFPL source", """
    SELECT p.transfer_out_id, p.transfer_out_challan_no,
           p.from_storage_type, p.to_storage_type, p.destination_table, COUNT(*) n
    FROM pending_transfer_stock p
    JOIN cfpl_cold_stocks s ON s.box_id=p.box_id AND s.transaction_no=p.transaction_no
    WHERE p.status='In Transit'
    GROUP BY p.transfer_out_id, p.transfer_out_challan_no,
             p.from_storage_type, p.to_storage_type, p.destination_table
    ORDER BY n DESC
""")

# G-detail: the 46 cdpl_bulk_entry_boxes rows tagged from_storage_type='cold'
run("G-detail: source_table=cdpl_bulk_entry_boxes but from_storage_type=cold", """
    SELECT transfer_out_id, transfer_out_challan_no, from_site, to_site,
           from_storage_type, to_storage_type, destination_table,
           cold_storage_data IS NOT NULL AS has_csd, COUNT(*) n
    FROM pending_transfer_stock
    WHERE status='In Transit' AND source_table='cdpl_bulk_entry_boxes'
      AND from_storage_type='cold'
    GROUP BY transfer_out_id, transfer_out_challan_no, from_site, to_site,
             from_storage_type, to_storage_type, destination_table,
             (cold_storage_data IS NOT NULL)
    ORDER BY n DESC
""")

# H-detail: where do NULL-csd cold rows come from (by transfer + dispatched_by)
run("H-detail: cold rows with NULL cold_storage_data, by transfer", """
    SELECT transfer_out_id, transfer_out_challan_no, from_site, to_site,
           dispatched_by, COUNT(*) n
    FROM pending_transfer_stock
    WHERE status='In Transit' AND from_storage_type='cold'
      AND box_id NOT LIKE 'LINE-%' AND cold_storage_data IS NULL
    GROUP BY transfer_out_id, transfer_out_challan_no, from_site, to_site, dispatched_by
    ORDER BY n DESC
""")
print("\nDone (read-only).")
