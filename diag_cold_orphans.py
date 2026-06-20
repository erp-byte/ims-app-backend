"""READ-ONLY: find interunit transfer-in staging headers orphaned by cold finalize.
Signature: an interunit_transfer_in_header whose id ALSO exists in cold_transfer_in_headers
with the same transfer_out_id (finalize reused the staging id to build the cold record,
but never purged the staging header). NO writes.
"""
import os
from sqlalchemy import create_engine, text

if not os.environ.get("DATABASE_URL"):
    for line in open(os.path.join(os.path.dirname(__file__), ".env"), encoding="utf-8"):
        line = line.strip()
        if line.startswith("DATABASE_URL") and "=" in line:
            os.environ["DATABASE_URL"] = line.split("=", 1)[1].strip().strip('"').strip("'")
            break

DB = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
e = create_engine(DB)

with e.connect() as c:
    rows = c.execute(text("""
        SELECT iih.id            AS staging_id,
               iih.status        AS iih_status,
               iih.receiving_warehouse,
               iih.transfer_out_no,
               cth.status        AS cold_status,
               (SELECT COUNT(*) FROM interunit_transfer_in_boxes b WHERE b.header_id = iih.id) AS iih_boxes,
               (SELECT COUNT(*) FROM cold_transfer_inboxes b WHERE b.header_id = cth.id)        AS cold_boxes
        FROM interunit_transfer_in_header iih
        JOIN cold_transfer_in_headers cth
          ON cth.id = iih.id
         AND cth.transfer_out_id = iih.transfer_out_id
        ORDER BY iih.id
    """)).fetchall()

    print(f"=== Orphaned interunit staging headers (cold finalize left them behind) = {len(rows)} ===")
    total_orphan_boxes = 0
    for r in rows:
        m = r._mapping
        total_orphan_boxes += m["iih_boxes"]
        print(f"  staging_id={m['staging_id']} out_no={m['transfer_out_no']!r} "
              f"dest={m['receiving_warehouse']!r} iih_status={m['iih_status']!r} "
              f"iih_boxes={m['iih_boxes']}  ||  cold_status={m['cold_status']!r} cold_boxes={m['cold_boxes']}")
    print(f"\nTotal orphaned interunit_transfer_in_boxes rows across all: {total_orphan_boxes}")
