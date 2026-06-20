import os, io, sys, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB, pool_pre_ping=True)

def get_conn(tries=40, delay=5):
    for i in range(tries):
        try:
            cc = engine.connect(); cc.execute(text("SELECT 1")); return cc
        except Exception:
            print(f"  ...db busy, retry {i+1}/{tries}", flush=True); time.sleep(delay)
    raise SystemExit("db unreachable")

with get_conn() as c:
    print("### Transfer TRANS202606151243 — what's in it (pending_transfer_stock)? ###")
    for r in c.execute(text("""SELECT transfer_out_challan_no, item_description, lot_no,
                               string_agg(DISTINCT split_part(box_id,'-',1),',') bases,
                               COUNT(*) n, SUM(weight_kg) kg, string_agg(DISTINCT status,',') st,
                               string_agg(DISTINCT to_site,',') dest
                               FROM pending_transfer_stock WHERE transfer_out_challan_no='TRANS202606151243'
                               GROUP BY 1,2,3 ORDER BY n DESC""")).mappings():
        print("  ", dict(r))
    tot = c.execute(text("SELECT COUNT(*) FROM pending_transfer_stock WHERE transfer_out_challan_no='TRANS202606151243'")).scalar()
    print(f"  total rows in this challan: {tot}  (my 128496 dupes = 400 with base 40332910)")

    print("\n### Is there a transfer_out header/lines for this challan? ###")
    for tname in ("interunit_transfers_header",):
        rows = c.execute(text(f"SELECT id, challan_no, from_site, to_site, status, stock_trf_date FROM {tname} WHERE challan_no=:ch"), {"ch": "TRANS202606151243"}).mappings().all()
        print(f"  {tname}: {[dict(r) for r in rows] or 'none'}")
    # cst_transferout (cold out)?
    try:
        rows = c.execute(text("SELECT * FROM cst_transferout WHERE transaction_no LIKE '%202606151243%' OR challan_no='TRANS202606151243' LIMIT 5")).mappings().all()
        print(f"  cst_transferout: {len(rows)} rows")
    except Exception as ex:
        print("  cst_transferout probe:", str(ex)[:80])

    print("\n### 10397 box# split per bulk (verify 1..612=128496, 613..1000=128573) ###")
    a = c.execute(text("SELECT COUNT(*) FROM cdpl_cold_stocks WHERE TRIM(lot_no)='10397' AND (split_part(box_id,'-',2))::int BETWEEN 1 AND 612")).scalar()
    b = c.execute(text("SELECT COUNT(*) FROM cdpl_cold_stocks WHERE TRIM(lot_no)='10397' AND (split_part(box_id,'-',2))::int BETWEEN 613 AND 1000")).scalar()
    print(f"  10397 cold box# 1..612 = {a} (->128496?), 613..1000 = {b} (->128573?)")
    bk1 = c.execute(text("SELECT MIN((split_part(box_id,'-',2))::int), MAX((split_part(box_id,'-',2))::int) FROM cdpl_bulk_entry_boxes WHERE transaction_no='TR-20260611123518' AND TRIM(lot_number)='128496' AND box_id LIKE '61528012-%'")).fetchone()
    bk2 = c.execute(text("SELECT MIN((split_part(box_id,'-',2))::int), MAX((split_part(box_id,'-',2))::int) FROM cdpl_bulk_entry_boxes WHERE transaction_no='TR-20260611123518' AND TRIM(lot_number)='128573' AND box_id LIKE '61528012-%'")).fetchone()
    print(f"  bulk 128496 box# range on 61528012: {bk1[0]}..{bk1[1]}")
    print(f"  bulk 128573 box# range on 61528012: {bk2[0]}..{bk2[1]}")
    print("  bulk 128496 straggler boxes:", c.execute(text("SELECT box_id FROM cdpl_bulk_entry_boxes WHERE TRIM(lot_number)='128496' AND box_id NOT LIKE '61528012-%'")).scalars().all())

    print("\n### Is lot 128573 referenced anywhere in cold already? ###")
    for comp in ("cdpl","cfpl"):
        n = c.execute(text(f"SELECT COUNT(*) FROM {comp}_cold_stocks WHERE TRIM(lot_no)='128573'")).scalar()
        print(f"  {comp}_cold_stocks lot 128573: {n}")
