"""READ-ONLY: structure of the 2 mismatch lots, to decide a safe txn/box_id correction."""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB, pool_pre_ping=True)

def base(b): return (b or "").rsplit("-", 1)[0]

def dump(c, table, lotcol, txncol, desccol, lot, label):
    rows = c.execute(text(f"SELECT box_id, {txncol} txn, net_or_wt, {desccol} d FROM ("
                          f"  SELECT box_id, {txncol}, {desccol}, "
                          f"  COALESCE(net_weight, NULL) net_or_wt FROM {table} WHERE TRIM({lotcol})=:l"
                          f") s") if False else
                     text(f"SELECT box_id, {txncol} AS txn, {desccol} AS d FROM {table} WHERE TRIM({lotcol})=:l"),
                     {"l": lot}).all()
    from collections import Counter
    bybase = Counter(base(r[0]) for r in rows)
    bytxn = Counter(r[1] for r in rows)
    print(f"  [{label}] rows={len(rows)} | bases={dict(list(bybase.items())[:12])}{'...' if len(bybase)>12 else ''}")
    print(f"           txns={dict(bytxn)}")

with engine.connect() as c:
    print("######## CFPL lot 3585 ########")
    # weights
    w = c.execute(text("SELECT COUNT(*), SUM(weight_kg), MIN(weight_kg), MAX(weight_kg) FROM cfpl_cold_stocks WHERE TRIM(lot_no)='3585'")).fetchone()
    print(f"  cold: n={w[0]} sum_kg={w[1]} per-box[{w[2]}..{w[3]}]")
    wv = c.execute(text("SELECT COUNT(*), SUM(net_weight) FROM cfpl_boxes_v2 WHERE TRIM(lot_number)='3585'")).fetchone()
    wb = c.execute(text("SELECT COUNT(*), SUM(net_weight) FROM cfpl_bulk_entry_boxes WHERE TRIM(lot_number)='3585'")).fetchone()
    print(f"  v2:   n={wv[0]} sum_net={wv[1]}    bulk: n={wb[0]} sum_net={wb[1]}")
    dump(c, "cfpl_cold_stocks", "lot_no", "transaction_no", "item_description", "3585", "COLD")
    dump(c, "cfpl_boxes_v2", "lot_number", "transaction_no", "article_description", "3585", "V2")
    dump(c, "cfpl_bulk_entry_boxes", "lot_number", "transaction_no", "article_description", "3585", "BULK")
    # do any cold box_ids collide with v2/bulk box_ids for this lot?
    ov = c.execute(text("SELECT COUNT(*) FROM cfpl_cold_stocks cs WHERE TRIM(cs.lot_no)='3585' AND EXISTS(SELECT 1 FROM cfpl_boxes_v2 v WHERE v.box_id=cs.box_id)")).scalar()
    print(f"  cold box_ids also present in v2 (collision if rebased): {ov}")

    print("\n######## CDPL lot 128350 ########")
    w = c.execute(text("SELECT COUNT(*), SUM(weight_kg), MIN(weight_kg), MAX(weight_kg) FROM cdpl_cold_stocks WHERE TRIM(lot_no)='128350'")).fetchone()
    print(f"  cold: n={w[0]} sum_kg={w[1]} per-box[{w[2]}..{w[3]}]")
    wb = c.execute(text("SELECT COUNT(*), SUM(net_weight) FROM cdpl_bulk_entry_boxes WHERE TRIM(lot_number)='128350'")).fetchone()
    print(f"  bulk: n={wb[0]} sum_net={wb[1]}")
    dump(c, "cdpl_cold_stocks", "lot_no", "transaction_no", "item_description", "128350", "COLD")
    dump(c, "cdpl_bulk_entry_boxes", "lot_number", "transaction_no", "article_description", "128350", "BULK")
    print("  bulk single row detail:")
    for r in c.execute(text("SELECT box_id, transaction_no, article_description, net_weight, status, inward_transaction_no, created_at FROM cdpl_bulk_entry_boxes WHERE TRIM(lot_number)='128350'")).mappings():
        print("     ", dict(r))
    # is there a separate inward/txn that 128350 cold actually came from? show distinct cold inward fields
    print("  cold 128350 inward fields:")
    for r in c.execute(text("SELECT DISTINCT inward_no, inward_transaction_no, transaction_no, inward_dt, storage_location, unit FROM cdpl_cold_stocks WHERE TRIM(lot_no)='128350'")).mappings():
        print("     ", dict(r))
