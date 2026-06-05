"""READ-ONLY: verify the 'pending = bridge' invariant.

Invariant the user wants:
  Transfer-Out parks every box in pending; Transfer-In picks per box;
  Transfer-Out stays 'Dispatch'/'Partial' (and keeps pending rows visible)
  UNTIL every dispatched box is received. Only then -> 'Received'.
"""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)


def run(title, sql, limit=40):
    print(f"\n===== {title} =====")
    with e.connect() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        rows = c.execute(text(sql)).fetchall()
        if not rows:
            print("  (none) ✓"); return
        cols = rows[0]._mapping.keys()
        print("  " + " | ".join(cols))
        for r in rows[:limit]:
            print("  " + " | ".join(str(r._mapping[k]) for k in cols))
        if len(rows) > limit:
            print(f"  ... (+{len(rows)-limit} more)")


# LINK A — OUT parks into pending. Dispatch/Partial transfers with NO pending rows
# (these rely on the header-fallback UNION; not truly bridged).
run("A. Dispatch/Partial transfers with NO pending rows", """
    SELECT h.id, h.challan_no, h.status,
           (SELECT COUNT(*) FROM interunit_transfer_boxes b WHERE b.header_id=h.id) AS out_boxes
    FROM interunit_transfers_header h
    WHERE h.status IN ('Dispatch','Partial')
      AND NOT EXISTS (SELECT 1 FROM pending_transfer_stock p
                      WHERE p.transfer_out_id=h.id AND p.status='In Transit')
    ORDER BY h.id DESC
""")

# LINK B (THE INVARIANT) — header 'Received' but >=1 DISTINCT dispatched box
# neither received in its own IN nor currently In Transit. (DISTINCT because
# interunit_transfer_boxes has duplicate rows, e.g. 711 rows for 3 boxes.)
run("B. 'Received' transfers with genuinely-unaccounted boxes", """
    SELECT h.id, h.challan_no,
           (SELECT COUNT(DISTINCT (box_id,transaction_no))
            FROM interunit_transfer_boxes WHERE header_id=h.id) dispatched_distinct,
           (SELECT COUNT(DISTINCT (ib.box_id,ib.transaction_no))
            FROM interunit_transfer_in_boxes ib
            JOIN interunit_transfer_in_header ti ON ti.id=ib.header_id
            WHERE ti.transfer_out_id=h.id) received_distinct,
           (SELECT COUNT(*) FROM (
              SELECT DISTINCT ob.box_id, ob.transaction_no
              FROM interunit_transfer_boxes ob
              WHERE ob.header_id=h.id AND ob.box_id<>'' AND ob.transaction_no<>''
                AND NOT EXISTS (SELECT 1 FROM interunit_transfer_in_boxes ib
                     JOIN interunit_transfer_in_header ti ON ti.id=ib.header_id
                     WHERE ti.transfer_out_id=h.id AND ib.box_id=ob.box_id AND ib.transaction_no=ob.transaction_no)
                AND NOT EXISTS (SELECT 1 FROM pending_transfer_stock p
                     WHERE p.box_id=ob.box_id AND p.transaction_no=ob.transaction_no AND p.status='In Transit')
           ) m) AS unaccounted
    FROM interunit_transfers_header h
    WHERE h.status='Received'
      AND (SELECT COUNT(*) FROM (
              SELECT DISTINCT ob.box_id, ob.transaction_no
              FROM interunit_transfer_boxes ob
              WHERE ob.header_id=h.id AND ob.box_id<>'' AND ob.transaction_no<>''
                AND NOT EXISTS (SELECT 1 FROM interunit_transfer_in_boxes ib
                     JOIN interunit_transfer_in_header ti ON ti.id=ib.header_id
                     WHERE ti.transfer_out_id=h.id AND ib.box_id=ob.box_id AND ib.transaction_no=ob.transaction_no)
                AND NOT EXISTS (SELECT 1 FROM pending_transfer_stock p
                     WHERE p.box_id=ob.box_id AND p.transaction_no=ob.transaction_no AND p.status='In Transit')
           ) m) > 0
    ORDER BY unaccounted DESC
""")

# B2 — header 'Received' but pending rows still In Transit (bridge not cleared)
run("B2. 'Received' transfers that STILL have In-Transit pending rows", """
    SELECT h.id, h.challan_no, h.status,
           COUNT(*) FILTER (WHERE p.box_id NOT LIKE 'LINE-%') AS stuck_real,
           COUNT(*) FILTER (WHERE p.box_id LIKE 'LINE-%')    AS stuck_lineonly
    FROM interunit_transfers_header h
    JOIN pending_transfer_stock p ON p.transfer_out_id=h.id AND p.status='In Transit'
    WHERE h.status='Received'
    GROUP BY h.id, h.challan_no, h.status
    ORDER BY stuck_real DESC
""")

# LINK D (frontend) — incomplete transfers WRONGLY hidden from the pending modal
# because OUT is 'Received' (modal filters status != 'Received'). DISTINCT-based.
run("D. 'Received' transfers hidden from pending modal but genuinely incomplete", """
    SELECT COUNT(*) AS hidden_incomplete_transfers,
           COALESCE(SUM(unaccounted),0) AS total_unaccounted_boxes
    FROM (
      SELECT h.id,
        (SELECT COUNT(*) FROM (
           SELECT DISTINCT ob.box_id, ob.transaction_no
           FROM interunit_transfer_boxes ob
           WHERE ob.header_id=h.id AND ob.box_id<>'' AND ob.transaction_no<>''
             AND NOT EXISTS (SELECT 1 FROM interunit_transfer_in_boxes ib
                  JOIN interunit_transfer_in_header ti ON ti.id=ib.header_id
                  WHERE ti.transfer_out_id=h.id AND ib.box_id=ob.box_id AND ib.transaction_no=ob.transaction_no)
             AND NOT EXISTS (SELECT 1 FROM pending_transfer_stock p
                  WHERE p.box_id=ob.box_id AND p.transaction_no=ob.transaction_no AND p.status='In Transit')
        ) m) AS unaccounted
      FROM interunit_transfers_header h
      WHERE h.status='Received'
    ) x
    WHERE unaccounted > 0
""")

# Sanity — what header statuses exist + counts
run("Z. header status distribution (interunit_transfers_header)", """
    SELECT status, COUNT(*) n FROM interunit_transfers_header GROUP BY status ORDER BY n DESC
""")
print("\nDone (read-only).")
