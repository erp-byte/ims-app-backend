"""READ-ONLY diagnostic for TRANS202605151608 — duplicate box_id problem.
Shows OUT boxes grouped by article with box_id duplication, IN boxes, pending rows.
NO writes.
"""
import os
from collections import Counter
from sqlalchemy import create_engine, text

if not os.environ.get("DATABASE_URL"):
    for line in open(os.path.join(os.path.dirname(__file__), ".env"), encoding="utf-8"):
        line = line.strip()
        if line.startswith("DATABASE_URL") and "=" in line:
            os.environ["DATABASE_URL"] = line.split("=", 1)[1].strip().strip('"').strip("'")
            break

DB = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
e = create_engine(DB)
CH = "TRANS202605151608"

with e.connect() as c:
    out = c.execute(text(
        "SELECT id, challan_no, status, from_site, to_site, created_ts "
        "FROM interunit_transfers_header WHERE challan_no = :ch"), {"ch": CH}).fetchone()
    if not out:
        print("OUT header not found"); raise SystemExit
    oid = out._mapping["id"]
    print(f"OUT header id={oid} status={out._mapping['status']!r} "
          f"{out._mapping['from_site']} -> {out._mapping['to_site']} created={out._mapping['created_ts']}")

    obx = c.execute(text(
        "SELECT id, box_number, box_id, transaction_no, lot_number, article "
        "FROM interunit_transfer_boxes WHERE header_id = :id ORDER BY article, box_number"),
        {"id": oid}).fetchall()
    print(f"\n=== OUT boxes total = {len(obx)} ===")
    distinct_box_ids = {r._mapping["box_id"] for r in obx}
    print(f"distinct box_id values across ALL boxes = {len(distinct_box_ids)}")

    # Per-article: total boxes vs distinct box_ids
    by_art = {}
    for r in obx:
        m = r._mapping
        by_art.setdefault(m["article"], []).append(m)
    print("\n=== per-article: total boxes vs distinct box_ids ===")
    for art, rows in by_art.items():
        ids = [x["box_id"] for x in rows]
        dups = {bid: ctr for bid, ctr in Counter(ids).items() if ctr > 1}
        flag = f"  <-- DUPLICATE box_ids ({len(dups)} id(s) repeated)" if dups else ""
        print(f"  {art!r}: boxes={len(rows)}  distinct_box_id={len(set(ids))}  "
              f"txn={rows[0]['transaction_no']!r} lot={rows[0]['lot_number']!r}{flag}")
        for bid, ctr in list(dups.items())[:3]:
            print(f"        box_id {bid!r} appears {ctr}x")

    # Show a sample of the worst-duplicated article's rows
    worst = max(by_art.items(), key=lambda kv: len(kv[1]) - len({x["box_id"] for x in kv[1]}), default=None)
    if worst:
        art, rows = worst
        ids = [x["box_id"] for x in rows]
        if len(ids) != len(set(ids)):
            print(f"\n=== sample rows for worst article {art!r} (first 8) ===")
            for x in rows[:8]:
                print(f"   box_number={x['box_number']} box_id={x['box_id']!r} txn={x['transaction_no']!r}")

    inh = c.execute(text(
        "SELECT id, status, grn_number, receiving_warehouse FROM interunit_transfer_in_header "
        "WHERE transfer_out_id = :id OR transfer_out_no = :ch"), {"id": oid, "ch": CH}).fetchall()
    print(f"\n=== IN header(s) = {len(inh)} ===")
    for h in inh:
        m = h._mapping
        cnt = c.execute(text("SELECT COUNT(*) FROM interunit_transfer_in_boxes WHERE header_id=:h"),
                        {"h": m["id"]}).scalar()
        print(f"  GRN id={m['id']} status={m['status']!r} grn={m['grn_number']!r} dest={m['receiving_warehouse']!r} in_boxes={cnt}")

    pend = c.execute(text(
        "SELECT COUNT(*) FROM pending_transfer_stock WHERE transfer_out_id=:id"), {"id": oid}).scalar()
    pend_it = c.execute(text(
        "SELECT COUNT(*) FROM pending_transfer_stock WHERE transfer_out_id=:id AND status='In Transit'"),
        {"id": oid}).scalar()
    print(f"\n=== pending_transfer_stock: total={pend} in_transit={pend_it} ===")
