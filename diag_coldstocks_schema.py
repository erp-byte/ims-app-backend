import os, io, sys, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text, inspect

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
e = create_engine(DB)
insp = inspect(e)

print("== cdpl_cold_stocks columns (name | type | nullable | default | autoincrement) ==")
for c in insp.get_columns("cdpl_cold_stocks"):
    print(f"  {c['name']} | {c['type']} | null={c['nullable']} | default={c.get('default')} | auto={c.get('autoincrement')}")
print("\nPK:", insp.get_pk_constraint("cdpl_cold_stocks"))
print("UNIQUE:", [u for u in insp.get_unique_constraints("cdpl_cold_stocks")])

print("\n== template row (box 90556000-21, lot 10470) — full ==")
with e.connect() as c:
    c.execute(text("SET TRANSACTION READ ONLY"))
    r = c.execute(text("""SELECT * FROM cdpl_cold_stocks
        WHERE transaction_no='TR-20260314174556' AND box_id='90556000-21'""")).fetchone()
    print(json.dumps({k: str(r._mapping[k]) for k in r._mapping.keys()}, indent=2, ensure_ascii=False))
