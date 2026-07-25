"""READ-ONLY: is my box_id relabel durable? cold_stocks is a derived mirror that
sync_cold_stocks_from_inward rebuilds from the inward box table for rows where
auto_created_from_inward=TRUE (matched by inward_transaction_no). If the repaired piles
are auto_created_from_inward=FALSE with NULL inward_transaction_no (migration rows), the
sync never touches them -> the fix is durable."""
import os
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

_raw = os.environ["DATABASE_URL"]
DB_URL = _raw.replace("postgresql://", "postgresql+psycopg://", 1) if _raw.startswith("postgresql://") else _raw
engine = create_engine(DB_URL)

PILES = [
    ("cdpl_cold_stocks", "Fresho Kimia Dates 500 Gm", "125859"),
    ("cdpl_cold_stocks", "10KG AL BARAKAH DATE POWDER V2", "93289"),
    ("cdpl_cold_stocks", "Wet Dates Zahidi Seedless", "127890"),
    ("cfpl_cold_stocks", "KING SOLOMON MEDJOUL JUMBO DATES 500GM", "17066"),
    ("cfpl_cold_stocks", "Organic Khidri Large dates", "13788"),
]

Q = """
    SELECT COUNT(*) AS rows,
           COUNT(*) FILTER (WHERE auto_created_from_inward IS TRUE) AS auto_true,
           COUNT(*) FILTER (WHERE auto_created_from_inward IS NOT TRUE) AS auto_false,
           COUNT(*) FILTER (WHERE inward_transaction_no IS NOT NULL AND TRIM(inward_transaction_no) <> '') AS has_inward_txn
    FROM {t}
    WHERE item_description = :item AND CAST(lot_no AS TEXT) = :lot
"""

with engine.connect() as c:
    c.execute(text("SET TRANSACTION READ ONLY"))
    print(f"{'table':20s} {'item':34s} {'lot':8s} {'rows':>5s} {'auto_T':>7s} {'auto_F':>7s} {'inw_txn':>8s}  durable?")
    for tbl, item, lot in PILES:
        m = c.execute(text(Q.replace("{t}", tbl)), {"item": item, "lot": lot}).fetchone()._mapping
        durable = "YES (migration rows, sync won't touch)" if m["auto_true"] == 0 and m["has_inward_txn"] == 0 \
                  else "AT RISK — sync may revert; patch inward box table too"
        print(f"{tbl:20s} {item[:34]:34s} {lot:8s} {m['rows']:5d} {m['auto_true']:7d} {m['auto_false']:7d} {m['has_inward_txn']:8d}  {durable}")

print("\nDone (read-only).")
