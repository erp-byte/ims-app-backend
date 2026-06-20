import os, io, sys, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB, pool_pre_ping=True)

def connect_retry(tries=40, delay=5):
    for i in range(tries):
        try:
            cc = engine.connect(); cc.execute(text("SELECT 1")); return cc
        except Exception as ex:
            print(f"  ...db busy, retry {i+1}/{tries}", flush=True); time.sleep(delay)
    raise SystemExit("db unreachable")

with connect_retry() as c:
    # 9903/9917 target base 54261171 already present in cfpl cold?
    n = c.execute(text("SELECT COUNT(*) FROM cfpl_cold_stocks WHERE box_id LIKE '54261171-%'")).scalar()
    print(f"9903/9917 target base 54261171 already in cfpl_cold_stocks: {n} (want 0)")

    # 128496: exact bulk box numbers on base 61528012 for lot 128496
    bulk = c.execute(text("""SELECT box_id FROM cdpl_bulk_entry_boxes WHERE TRIM(lot_number)='128496'
                             AND box_id LIKE '61528012-%' ORDER BY (split_part(box_id,'-',2))::int""")).scalars().all()
    bn = sorted(int(b.split('-')[1]) for b in bulk)
    print(f"128496 bulk base 61528012: n={len(bulk)} box#[{bn[0]}..{bn[-1]}]")
    # full bulk set (all bases) for 128496
    allbulk = c.execute(text("SELECT box_id FROM cdpl_bulk_entry_boxes WHERE TRIM(lot_number)='128496' ORDER BY id")).scalars().all()
    print(f"128496 bulk all box_ids: n={len(allbulk)} sample {allbulk[:3]} ... {allbulk[-3:]}")

    # lot 10397 cold box numbers on base 61528012
    c10397 = c.execute(text("SELECT box_id FROM cdpl_cold_stocks WHERE TRIM(lot_no)='10397' AND box_id LIKE '61528012-%'")).scalars().all()
    n10397 = sorted(int(b.split('-')[1]) for b in c10397)
    print(f"10397 cold base 61528012: n={len(c10397)} box#[{n10397[0]}..{n10397[-1]}]")

    # overlap between 128496 bulk box#s and 10397 cold box#s on the shared base
    overlap = set(bn) & set(n10397)
    print(f"OVERLAP of 128496-bulk vs 10397-cold box#s on base 61528012: {len(overlap)} {sorted(overlap)[:10]}")
    # do 128496's exact bulk box_ids already exist anywhere in cdpl cold (collision if we adopt them)?
    coll = c.execute(text("""SELECT COUNT(*) FROM cdpl_cold_stocks WHERE box_id = ANY(:b)"""), {"b": allbulk}).scalar()
    print(f"128496 bulk box_ids already present in cdpl_cold_stocks: {coll} (collision if adopt & >0)")
