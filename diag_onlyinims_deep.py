"""READ-ONLY deep-dive on the ambiguous 'Only in IMS' lots:
 - 5 already-in-cold (same company)   : why flagged Only-in-IMS? compare attrs.
 - cross-company strays (other table) : 10470, 128204, 17170, 17168, 17166, 17167, 17177
 - case2 partial                      : 2378
Show what's actually stored so a human can decide. No writes.
"""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB, pool_pre_ping=True)

def show_cold(c, table, lot):
    rows = c.execute(text(f"""
        SELECT transaction_no, unit, storage_location, vakkal, item_description,
               COUNT(*) rows, COALESCE(SUM(no_of_cartons),0) cart, COALESCE(SUM(weight_kg),0) kg,
               MIN(inward_dt) inw, MIN(box_id) min_box, MAX(box_id) max_box
        FROM {table} WHERE TRIM(lot_no)=:l
        GROUP BY transaction_no, unit, storage_location, vakkal, item_description
        ORDER BY rows DESC
    """), {"l": lot}).mappings().all()
    for r in rows:
        print(f"    [{table[:4]}] txn={r['transaction_no']} unit={r['unit']} store={r['storage_location']} "
              f"vak={r['vakkal']} desc={r['item_description']!r}")
        print(f"           rows={r['rows']} cart={r['cart']} kg={r['kg']} inw={r['inw']} box[{r['min_box']}..{r['max_box']}]")

LOTS_ALREADY = [("CDPL","81466"),("CDPL","128534"),("CDPL","128533"),("CDPL","128288"),("CDPL","128350")]
LOTS_CROSS   = [("CFPL","10470","cdpl_cold_stocks"),("CDPL","128204","cfpl_cold_stocks"),
                ("CDPL","17170","cfpl_cold_stocks"),("CDPL","17168","cfpl_cold_stocks"),
                ("CDPL","17166","cfpl_cold_stocks"),("CDPL","17167","cfpl_cold_stocks"),
                ("CDPL","17177","cfpl_cold_stocks")]

with engine.connect() as c:
    print("################ ALREADY-IN-COLD (same company) — IMS says only-in-IMS ################")
    for ent, lot in LOTS_ALREADY:
        tbl = "cfpl_cold_stocks" if ent=="CFPL" else "cdpl_cold_stocks"
        print(f"\n--- {ent} lot {lot} (expected in {tbl}) ---")
        show_cold(c, tbl, lot)

    print("\n\n################ CROSS-COMPANY STRAYS (exists in the OTHER company) ################")
    for ent, lot, othertbl in LOTS_CROSS:
        wanttbl = "cfpl_cold_stocks" if ent=="CFPL" else "cdpl_cold_stocks"
        print(f"\n--- {ent} lot {lot}: IMS wants {wanttbl}, but rows exist in {othertbl} ---")
        print(f"  >> in IMS-side table {wanttbl}:")
        show_cold(c, wanttbl, lot)
        print(f"  >> in OTHER table {othertbl}:")
        show_cold(c, othertbl, lot)

    print("\n\n################ case2 partial: CFPL 2378 ################")
    print("  cold cfpl:"); show_cold(c, "cfpl_cold_stocks", "2378")
    print("  cold cdpl:"); show_cold(c, "cdpl_cold_stocks", "2378")
    for src, q in [
        ("interunit_transfer_in_boxes (IN)",
         "SELECT h.receiving_warehouse wh, b.transaction_no, COUNT(*) n, SUM(b.net_weight) kg "
         "FROM interunit_transfer_in_boxes b JOIN interunit_transfer_in_header h ON h.id=b.header_id "
         "WHERE TRIM(b.lot_number)='2378' GROUP BY h.receiving_warehouse, b.transaction_no"),
        ("interunit_transfer_boxes (OUT)",
         "SELECT hh.to_site wh, b.transaction_no, COUNT(*) n, SUM(b.net_weight) kg "
         "FROM interunit_transfer_boxes b JOIN interunit_transfers_header hh ON hh.id=b.header_id "
         "WHERE TRIM(b.lot_number)='2378' GROUP BY hh.to_site, b.transaction_no"),
    ]:
        print(f"  {src}:")
        for r in c.execute(text(q)).mappings().all():
            print(f"      wh={r['wh']} txn={r['transaction_no']} n={r['n']} kg={r['kg']}")
