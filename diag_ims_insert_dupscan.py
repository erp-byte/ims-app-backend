"""Scan all 7 IMS-inserted lots for pre-existing cold mirrors (blank-lot or other-lot)
that my lot-based insert guard missed -> i.e., which of my inserts are duplicates."""
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

# my synthetic inserts (from rollback json): ent, lot, desc, synth_base, cartons
INS = [
 ("cfpl","17694","King Solomon Seedless Medjoul 500 g","40332727",4),
 ("cfpl","17693","King Solomon Seedless Oriental dates 500 g","40332748",5),
 ("cfpl","17695","King Solomon Medjoul super jumbo 750 g","40332767",5),
 ("cfpl","17696","King Solomon medjoul super jumbo 320 g","40332796",5),
 ("cfpl","9917","Wet Dates Seedless Khalas","40332831",29),
 ("cfpl","9903","Wet Dates Seedless Khalas","40332870",471),
 ("cdpl","128496","Wet Dates Zahidi","40332910",612),
]

with connect_retry() as c:
    print(f"{'lot':>7} {'co':4} | my_insert | OTHER cold rows w/ same desc (potential pre-existing mirror)")
    for comp, lot, desc, synth, n in INS:
        cs = f"{comp}_cold_stocks"
        mine = c.execute(text(f"SELECT COUNT(*) FROM {cs} WHERE box_id LIKE :p"), {"p": f"{synth}-%"}).scalar()
        # other cold rows (not my synthetic base) with same/similar description
        others = c.execute(text(f"""SELECT TRIM(lot_no) lot, COUNT(*) n, SUM(weight_kg) kg,
                                    string_agg(DISTINCT transaction_no, ',') txn,
                                    string_agg(DISTINCT split_part(box_id,'-',1), ',') bases
                                    FROM {cs}
                                    WHERE lower(regexp_replace(item_description,'\\s+',' ','g'))=lower(regexp_replace(:d,'\\s+',' ','g'))
                                      AND box_id NOT LIKE :p
                                    GROUP BY 1 ORDER BY n DESC"""),
                            {"d": desc, "p": f"{synth}-%"}).mappings().all()
        print(f"{lot:>7} {comp:4} | mine={mine:>4} ({synth}) | desc={desc!r}")
        if not others:
            print("           -> NO other cold rows with this description (likely genuinely new)")
        for o in others:
            print(f"           -> lot={o['lot']!r} n={o['n']} kg={o['kg']} txn={o['txn']} bases={o['bases']}")
