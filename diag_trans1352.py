"""READ-ONLY diagnostic for TRANS202606111352 (A185 -> W202).
Why is transfer-out stuck 'Dispatch' when transfer-in shows all acknowledged?
Inspects: OUT header, OUT boxes, IN header(s), IN boxes, pending_transfer_stock.
NO writes. Safe to run against the live DB.
"""
import os
from sqlalchemy import create_engine, text

# Load DATABASE_URL from backend/.env without printing it.
if not os.environ.get("DATABASE_URL"):
    envp = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(envp):
        for line in open(envp, encoding="utf-8"):
            line = line.strip()
            if line.startswith("DATABASE_URL") and "=" in line:
                os.environ["DATABASE_URL"] = line.split("=", 1)[1].strip().strip('"').strip("'")
                break

DB = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
e = create_engine(DB)
CH = "TRANS202606111352"


def show(title, rows, cols):
    print(f"\n=== {title} ({len(rows)}) ===")
    if not rows:
        print("  (none)")
        return
    for r in rows:
        print("  " + " | ".join(f"{c}={r._mapping.get(c)!r}" for c in cols))


with e.connect() as c:
    out = c.execute(text(
        "SELECT id, challan_no, status, from_site, to_site, created_ts "
        "FROM interunit_transfers_header WHERE challan_no = :ch"), {"ch": CH}).fetchall()
    show("OUT header  interunit_transfers_header", out,
         ["id", "challan_no", "status", "from_site", "to_site", "created_ts"])
    if not out:
        print("OUT header not found by challan_no — aborting.")
        raise SystemExit
    oid = out[0]._mapping["id"]

    obx = c.execute(text(
        "SELECT box_id, transaction_no, lot_number, article, box_number "
        "FROM interunit_transfer_boxes WHERE header_id = :id ORDER BY id"), {"id": oid}).fetchall()
    show(f"OUT boxes  interunit_transfer_boxes (header_id={oid})", obx,
         ["box_number", "box_id", "transaction_no", "lot_number", "article"])

    inh = c.execute(text(
        "SELECT id, transfer_out_id, transfer_out_no, grn_number, status, receiving_warehouse, received_at "
        "FROM interunit_transfer_in_header WHERE transfer_out_id = :id OR transfer_out_no = :ch"),
        {"id": oid, "ch": CH}).fetchall()
    show("IN header  interunit_transfer_in_header", inh,
         ["id", "transfer_out_id", "transfer_out_no", "grn_number", "status", "receiving_warehouse", "received_at"])

    for h in inh:
        hid = h._mapping["id"]
        ibx = c.execute(text(
            "SELECT box_id, transaction_no, lot_number, article, is_matched "
            "FROM interunit_transfer_in_boxes WHERE header_id = :h ORDER BY id"), {"h": hid}).fetchall()
        show(f"IN boxes  interunit_transfer_in_boxes (header_id={hid})", ibx,
             ["box_id", "transaction_no", "lot_number", "article", "is_matched"])

    pend = c.execute(text(
        "SELECT id, box_id, transaction_no, lot_no, article, status, destination_table "
        "FROM pending_transfer_stock WHERE transfer_out_id = :id ORDER BY id"), {"id": oid}).fetchall()
    show(f"pending_transfer_stock (transfer_out_id={oid})", pend,
         ["id", "box_id", "transaction_no", "lot_no", "article", "status", "destination_table"])

    in_transit_real = c.execute(text(
        "SELECT COUNT(*) FROM pending_transfer_stock WHERE transfer_out_id = :id "
        "AND status = 'In Transit' AND COALESCE(box_id,'') NOT LIKE 'LINE-%'"), {"id": oid}).scalar()
    line_rows = c.execute(text(
        "SELECT COUNT(*) FROM pending_transfer_stock WHERE transfer_out_id = :id "
        "AND COALESCE(box_id,'') LIKE 'LINE-%'"), {"id": oid}).scalar()
    print(f"\n=== completion gate ===")
    print(f"  count_remaining_in_transit (real, non-LINE, In Transit) = {in_transit_real}")
    print(f"  LINE-sentinel rows total = {line_rows}")
    print("  -> status flips to 'Received' ONLY when the real count above is 0")
