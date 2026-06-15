"""READ-ONLY: gather everything needed to correct 9903, 9917 (cold-only) and 128496
(coordinated cold + pending) to their real bulk_entry sources."""
import os, io, sys, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB, pool_pre_ping=True)

def connect_retry(tries=40, delay=5):
    last = None
    for i in range(tries):
        try:
            cc = engine.connect(); cc.execute(text("SELECT 1")); return cc
        except Exception as ex:
            last = ex; print(f"  ...db busy, retry {i+1}/{tries}", flush=True); time.sleep(delay)
    raise last

with connect_retry() as c:
    print("===== leakage of synthetic bases into pending / transfers =====")
    for lot, b in [("9903", "40332870"), ("9917", "40332831"), ("128496", "40332910")]:
        p = c.execute(text("SELECT COUNT(*) FROM pending_transfer_stock WHERE box_id LIKE :p"), {"p": f"{b}-%"}).scalar()
        ob = c.execute(text("SELECT COUNT(*) FROM interunit_transfer_boxes WHERE box_id LIKE :p"), {"p": f"{b}-%"}).scalar()
        ibx = c.execute(text("SELECT COUNT(*) FROM interunit_transfer_in_boxes WHERE box_id LIKE :p"), {"p": f"{b}-%"}).scalar()
        print(f"  lot {lot} base {b}: pending={p} transfer_out_boxes={ob} transfer_in_boxes={ibx}")

    print("\n===== bulk source box_ids (ordered) for the three lots =====")
    for comp, lot in [("cfpl", "9903"), ("cfpl", "9917"), ("cdpl", "128496")]:
        bk = f"{comp}_bulk_entry_boxes"
        rows = c.execute(text(f"SELECT box_id, transaction_no, net_weight FROM {bk} WHERE TRIM(lot_number)=:l ORDER BY id"), {"l": lot}).all()
        bases = {}
        for r in rows:
            bases.setdefault(r[0].rsplit("-", 1)[0], 0)
            bases[r[0].rsplit("-", 1)[0]] += 1
        print(f"  {comp} bulk {lot}: n={len(rows)} txn={sorted({r[1] for r in rows})} bases={bases}")
        print(f"     first/last box_id: {rows[0][0]} .. {rows[-1][0]}")

    print("\n===== 128496 pending rows structure (UNIQUE box_id + UNIQUE transaction_no!) =====")
    prow = c.execute(text("""SELECT id, box_id, transaction_no, transfer_out_challan_no, status, no_of_cartons, weight_kg, destination_table, to_company, to_site
                            FROM pending_transfer_stock WHERE box_id LIKE '40332910-%' ORDER BY (split_part(box_id,'-',2))::int LIMIT 3""")).mappings().all()
    for r in prow: print("   ", dict(r))
    pn = c.execute(text("SELECT COUNT(*), MIN((split_part(box_id,'-',2))::int), MAX((split_part(box_id,'-',2))::int), COUNT(DISTINCT transaction_no) FROM pending_transfer_stock WHERE box_id LIKE '40332910-%'")).fetchone()
    print(f"   pending 128496: n={pn[0]} box#[{pn[1]}..{pn[2]}] distinct_txn={pn[3]}")
    cn = c.execute(text("SELECT COUNT(*), MIN((split_part(box_id,'-',2))::int), MAX((split_part(box_id,'-',2))::int), COUNT(DISTINCT transaction_no) FROM cdpl_cold_stocks WHERE box_id LIKE '40332910-%'")).fetchone()
    print(f"   cold    128496: n={cn[0]} box#[{cn[1]}..{cn[2]}] distinct_txn={cn[3]}")
    # would real bulk box_ids collide if placed into pending (UNIQUE box_id) or cold?
    coll_p = c.execute(text("SELECT COUNT(*) FROM pending_transfer_stock WHERE box_id LIKE '61528012-%'")).scalar()
    coll_c = c.execute(text("SELECT COUNT(*) FROM cdpl_cold_stocks WHERE box_id LIKE '61528012-%'")).scalar()
    print(f"   target base 61528012 already in pending={coll_p} cold={coll_c} (want 0)")
