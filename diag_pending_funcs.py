"""Smoke-test the rewritten read functions (no column refs to from_company/article)."""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
Session = sessionmaker(bind=create_engine(DB))
db = Session()

from services.ims_service.pending_stock_tools import list_pending_transfers, pending_by_lot

r = list_pending_transfers(db)
print("list_pending_transfers: total =", r["total"],
      "| sample keys =", list(r["records"][0].keys()) if r["records"] else "(no records)")

p = pending_by_lot(db, lot_no="185900", from_company="cfpl")
print("pending_by_lot(185900, cfpl): pending_cartons =", p["pending_cartons"],
      "| transfers =", len(p["transfers"]))

db.close()
print("OK")
