"""READ-ONLY diagnostic for TRANS202606161854 (ANJEER, 202 -> savla).
Checks BOTH interunit and cold transfer-in tables to see where records landed.
NO writes.
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
CH = "TRANS202606161854"

with e.connect() as c:
    # ── 1. OUT header ────────────────────────────────────────────────
    out = c.execute(text(
        "SELECT id, challan_no, status, from_site, to_site, created_ts "
        "FROM interunit_transfers_header WHERE challan_no = :ch"), {"ch": CH}).fetchone()
    if not out:
        print(f"OUT header for {CH} NOT FOUND in interunit_transfers_header")
        raise SystemExit
    oid = out._mapping["id"]
    print("=== OUT HEADER (interunit_transfers_header) ===")
    print(f"  id={oid} challan={out._mapping['challan_no']!r} status={out._mapping['status']!r}")
    print(f"  from_site={out._mapping['from_site']!r} -> to_site={out._mapping['to_site']!r}")
    print(f"  created={out._mapping['created_ts']}")

    # ── 2. OUT boxes ─────────────────────────────────────────────────
    obx = c.execute(text(
        "SELECT box_number, box_id, transaction_no, lot_number, article "
        "FROM interunit_transfer_boxes WHERE header_id = :id ORDER BY article, box_number"),
        {"id": oid}).fetchall()
    print(f"\n=== OUT BOXES (interunit_transfer_boxes) total = {len(obx)} ===")
    for r in obx:
        m = r._mapping
        print(f"  box#{m['box_number']} box_id={m['box_id']!r} txn={m['transaction_no']!r} "
              f"lot={m['lot_number']!r} article={m['article']!r}")

    # ── 3. INTERUNIT transfer-in header(s) ──────────────────────────
    inh = c.execute(text(
        "SELECT id, status, grn_number, receiving_warehouse, received_at "
        "FROM interunit_transfer_in_header "
        "WHERE transfer_out_id = :id OR transfer_out_no = :ch "
        "ORDER BY id"), {"id": oid, "ch": CH}).fetchall()
    print(f"\n=== INTERUNIT transfer-in header(s) = {len(inh)} ===")
    for h in inh:
        m = h._mapping
        cnt = c.execute(text("SELECT COUNT(*) FROM interunit_transfer_in_boxes WHERE header_id=:h"),
                        {"h": m["id"]}).scalar()
        print(f"  IN id={m['id']} status={m['status']!r} grn={m['grn_number']!r} "
              f"dest={m['receiving_warehouse']!r} received={m['received_at']} in_boxes={cnt}")
        bx = c.execute(text(
            "SELECT box_id, transaction_no, lot_number, article "
            "FROM interunit_transfer_in_boxes WHERE header_id=:h ORDER BY box_id"),
            {"h": m["id"]}).fetchall()
        for b in bx:
            bm = b._mapping
            print(f"      box_id={bm['box_id']!r} txn={bm['transaction_no']!r} "
                  f"lot={bm['lot_number']!r} article={bm['article']!r}")

    # ── 4. COLD transfer-in header(s) ───────────────────────────────
    ch_rows = c.execute(text(
        "SELECT id, status, grn_number, from_site, to_site, to_company, received_at "
        "FROM cold_transfer_in_headers "
        "WHERE transfer_out_id = :id OR transfer_out_no = :ch "
        "ORDER BY id"), {"id": oid, "ch": CH}).fetchall()
    print(f"\n=== COLD transfer-in header(s) (cold_transfer_in_headers) = {len(ch_rows)} ===")
    for h in ch_rows:
        m = h._mapping
        cnt = c.execute(text("SELECT COUNT(*) FROM cold_transfer_inboxes WHERE header_id=:h"),
                        {"h": m["id"]}).scalar()
        print(f"  COLD IN id={m['id']} status={m['status']!r} grn={m['grn_number']!r} "
              f"{m['from_site']!r}->{m['to_site']!r} company={m['to_company']!r} "
              f"received={m['received_at']} cold_boxes={cnt}")
        bx = c.execute(text(
            "SELECT box_id, transaction_no, lot_no, item_description "
            "FROM cold_transfer_inboxes WHERE header_id=:h ORDER BY box_id"),
            {"h": m["id"]}).fetchall()
        for b in bx:
            bm = b._mapping
            print(f"      box_id={bm['box_id']!r} txn={bm['transaction_no']!r} "
                  f"lot={bm['lot_no']!r} desc={bm['item_description']!r}")

    # ── 5. pending_transfer_stock ───────────────────────────────────
    pend = c.execute(text(
        "SELECT status, COUNT(*) FROM pending_transfer_stock WHERE transfer_out_id=:id "
        "GROUP BY status"), {"id": oid}).fetchall()
    print(f"\n=== pending_transfer_stock for out_id={oid} ===")
    for p in pend:
        print(f"  status={p[0]!r} count={p[1]}")

    # ── 6. cold stock rows referencing this transaction ─────────────
    for tbl in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
        try:
            rows = c.execute(text(
                f"SELECT COUNT(*) FROM {tbl} WHERE transaction_no = ANY(:txns)"),
                {"txns": [r._mapping["transaction_no"] for r in obx] or [""]}).scalar()
            print(f"\n=== {tbl}: rows matching OUT txns = {rows} ===")
        except Exception as ex:
            print(f"\n=== {tbl}: query failed ({ex}) ===")
