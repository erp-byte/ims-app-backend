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
    print("### bulk txn TR-20260611123518 (128496 source) lot breakdown ###")
    for r in c.execute(text("""SELECT TRIM(lot_number) lot, COUNT(*) n, SUM(net_weight) kg,
                               string_agg(DISTINCT split_part(box_id,'-',1),',') bases
                               FROM cdpl_bulk_entry_boxes WHERE transaction_no='TR-20260611123518'
                               GROUP BY 1 ORDER BY n DESC""")).mappings():
        print("  ", dict(r))

    print("\n### Is 128496 stock already mirrored in cold? cdpl cold rows on txn TR-20260611123518 ###")
    for r in c.execute(text("""SELECT TRIM(lot_no) lot, COUNT(*) n, SUM(weight_kg) kg,
                               string_agg(DISTINCT split_part(box_id,'-',1),',') bases
                               FROM cdpl_cold_stocks WHERE transaction_no='TR-20260611123518'
                               GROUP BY 1 ORDER BY n DESC""")).mappings():
        print("  ", dict(r))

    print("\n### 3585 safety: any cfpl cold rows on txn TR-20260421123317 besides lot 3585? (NULL/other-lot mirror) ###")
    for r in c.execute(text("""SELECT TRIM(lot_no) lot, COUNT(*) n, SUM(weight_kg) kg,
                               string_agg(DISTINCT split_part(box_id,'-',1),',') bases
                               FROM cfpl_cold_stocks WHERE transaction_no='TR-20260421123317'
                               GROUP BY 1 ORDER BY n DESC""")).mappings():
        print("  ", dict(r))

    print("\n### NULL/blank-lot cold rows summary (how widespread is blank-lot mirror) ###")
    for comp in ("cfpl", "cdpl"):
        r = c.execute(text(f"SELECT COUNT(*) FROM {comp}_cold_stocks WHERE lot_no IS NULL OR TRIM(lot_no)=''")).scalar()
        print(f"  {comp}_cold_stocks blank-lot rows: {r}")

    print("\n### Confirm 9903/9917 dup: my synthetic inserts still present? ###")
    for lot, b in [("9903", "40332870"), ("9917", "40332831")]:
        n = c.execute(text("SELECT COUNT(*) FROM cfpl_cold_stocks WHERE TRIM(lot_no)=:l AND box_id LIKE :p"),
                      {"l": lot, "p": f"{b}-%"}).scalar()
        print(f"  lot {lot} synthetic base {b}: {n} rows (my insert)")
