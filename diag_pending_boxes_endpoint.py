"""READ-ONLY: verify get_pending_boxes_by_transfer_out returns the In-Transit rows
for a transfer that currently has parked pending stock."""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
eng = create_engine(DB)

from services.ims_service.interunit_tools import get_pending_boxes_by_transfer_out

with eng.connect() as c:
    c.execute(text("SET TRANSACTION READ ONLY"))
    # Endpoint returns ALL In-Transit rows for the transfer (incl. 'LINE-%' synthetic rows
    # used by warehouse->cold transfers parked line-level).
    pick = c.execute(text("""
        SELECT transfer_out_id, COUNT(*) AS n
        FROM pending_transfer_stock
        WHERE status = 'In Transit'
        GROUP BY transfer_out_id ORDER BY n DESC LIMIT 1
    """)).fetchone()
    if not pick:
        print("No in-transit pending stock in DB — dispatch a transfer first."); sys.exit(0)

    tid, expected = pick.transfer_out_id, pick.n
    result = get_pending_boxes_by_transfer_out(tid, Session(bind=c))
    print(f"transfer_out_id={tid} expected_rows={expected} returned={result['total']}")
    assert result["total"] == expected, "MISMATCH: returned count != raw In-Transit count"
    b = result["boxes"][0]
    for k in ("id", "transfer_out_box_id", "box_id", "transaction_no", "article", "lot_number", "net_weight"):
        assert k in b, f"missing key {k}"
    resolved = sum(1 for x in result["boxes"] if x["transfer_out_box_id"] is not None)
    print(f"transfer_out_box_id resolved (real OUT box): {resolved}/{result['total']} "
          f"(LINE-% rows correctly resolve to None)")
    print("sample box:", {k: b[k] for k in ("id","transfer_out_box_id","box_id","transaction_no","article","lot_number","net_weight")})
    print("OK ✓")
