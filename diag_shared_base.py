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
        except Exception:
            print(f"  ...db busy, retry {i+1}/{tries}", flush=True); time.sleep(delay)
    raise SystemExit("db unreachable")

with connect_retry() as c:
    print("### Who holds base 54261171 in cfpl_cold_stocks (500 boxes)? ###")
    for r in c.execute(text("""SELECT TRIM(lot_no) lot, item_description, transaction_no, COUNT(*) n,
                               MIN(box_id), MAX(box_id), SUM(weight_kg) kg, MIN(inward_dt) inw
                               FROM cfpl_cold_stocks WHERE box_id LIKE '54261171-%'
                               GROUP BY 1,2,3 ORDER BY n DESC""")).mappings():
        print("  ", dict(r))
    print("\n### Who holds base 61528012 in cdpl_cold_stocks (1000 boxes)? ###")
    for r in c.execute(text("""SELECT TRIM(lot_no) lot, item_description, transaction_no, COUNT(*) n,
                               MIN(box_id), MAX(box_id), SUM(weight_kg) kg, MIN(inward_dt) inw
                               FROM cdpl_cold_stocks WHERE box_id LIKE '61528012-%'
                               GROUP BY 1,2,3 ORDER BY n DESC""")).mappings():
        print("  ", dict(r))
    # Is lot 9903/9917 stock possibly ALREADY in cold under another lot? compare desc 'wet dates seedless khalas'
    print("\n### cfpl cold lots with desc ~ 'wet dates seedless khalas' (possible dup of 9903/9917) ###")
    for r in c.execute(text("""SELECT TRIM(lot_no) lot, COUNT(*) n, SUM(weight_kg) kg, string_agg(DISTINCT transaction_no,',') txn,
                               string_agg(DISTINCT split_part(box_id,'-',1),',') bases
                               FROM cfpl_cold_stocks WHERE lower(item_description) LIKE '%wet dates seedless khalas%'
                               GROUP BY 1 ORDER BY n DESC""")).mappings():
        print("  ", dict(r))
    print("\n### cdpl cold lots with desc ~ 'wet dates zahidi' (possible dup of 128496) ###")
    for r in c.execute(text("""SELECT TRIM(lot_no) lot, COUNT(*) n, SUM(weight_kg) kg, string_agg(DISTINCT transaction_no,',') txn,
                               string_agg(DISTINCT split_part(box_id,'-',1),',') bases
                               FROM cdpl_cold_stocks WHERE lower(item_description) LIKE '%wet dates zahidi%'
                               GROUP BY 1 ORDER BY n DESC""")).mappings():
        print("  ", dict(r))
