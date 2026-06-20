"""READ-ONLY diagnostic: classify the 22 'Only in IMS' lots from
Cold_and_IMS_Comparison_14Jun2026.xlsx (sheet IMS_vs_System_Comparison) against the
live cold_stocks + transfer tables, to bucket each into:

  case1: exact required carton count exists in a cold/interunit transfer-in table
         (boxes exist but were never mirrored into cfpl/cdpl_cold_stocks) -> copy them.
  case2: some qty present in a transfer table but NOT the exact count -> use available
         details, insert needed boxes with fresh box_ids.
  case3: lot not present anywhere -> insert new with unique box_ids + new txn + inward date.

Nothing is written. Pure SELECTs.
"""
import os, io, sys, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB, pool_pre_ping=True)

# (entity, lot, ims_unit, vakkal, item_desc, storage, ims_qty_kg, ims_cartons, ims_rate, ims_value, ageing, days)
LOTS = [
 ("CFPL","182083","D-39","442501364","Medjoul Dates Large","Savla",0,0,871.2,0),
 ("CDPL","17168","D-514","-","Wet Dates (Mabroom)","Savla",5,1,800,4000),
 ("CDPL","17177","D-514","PREMIUM LARGE","Organic Wanan Premium Large","Savla",5,1,715,3575),
 ("CDPL","17166","D-514","PRE JUMBO","Organic Wanan Premium Jumbo","Savla",12.5,1,938,11725),
 ("CDPL","17167","D-514","PRE LARGE","Organic Seqee Premium Large","Savla",12.5,1,641,8012.5),
 ("CDPL","81942","D-514","12.5 KG","Organic Seqee Premium Large","Savla",12.5,1,641,8012.5),
 ("CFPL","17694","D-514","500 GM","King Solomon Seedless Medjoul 500 g","Savla",32,4,1100,35200),
 ("CFPL","17693","D-514","500 GM","King Solomon Seedless Oriental dates 500 g","Savla",40,5,400,16000),
 ("CFPL","17695","D-514","750 GM","King Solomon Medjoul super jumbo 750 g","Savla",40,5,1250,50000),
 ("CFPL","17696","D-514","320 GM","King Solomon medjoul super jumbo 320 g","Savla",40,5,1250,50000),
 ("CDPL","81466","D-514","AJWA","Royal Delight Ajwa Dates 5 Kgs","Savla",165,33,980,161700),
 ("CFPL","10470","D-514","-","Medjoul Dates Large","Savla",235,47,860,202100),
 ("CDPL","128534","Rishi","Khaneizi Label","Al Barakah Fard Dates Seedless","Rishi",265,53,230,60950),
 ("CFPL","2378","D-39","-","Indian Green Raisins","Savla",270,18,382,103140),
 ("CFPL","9917","D-39","MISS MACH WEI","Wet Dates Seedless Khalas","Savla",290,29,140,40600),
 ("CDPL","17170","D-514","MIX","Off-Grade Dates","Savla",405,81,200,81000),
 ("CDPL","128533","Rishi","500gm","Al Barakah Premium Emirate Date Seedless 500gm","Rishi",888,111,143,126984),
 ("CDPL","128288","Rishi",None,"Egyptian Wet Dates 10Kg","Rishi",2980,298,120,357600),
 ("CFPL","9903","D-39","-","Wet Dates Seedless Khalas","Savla",4710,471,140,659400),
 ("CDPL","128204","Rishi",None,"Wet Dates Seedless Khalas","Rishi",5000,500,135,675000),
 ("CDPL","128496","Rishi","Zahidi","Wet Dates Zahidi","Rishi",6120,612,110,673200),
 ("CDPL","128350","Rishi",None,"AL BARAKAH KHALAS PREMIUM DATES","Rishi",14980,1498,116,1737680),
]

def cold_count(c, table, lot):
    return c.execute(text(f"SELECT COUNT(*), COALESCE(SUM(no_of_cartons),0), COALESCE(SUM(weight_kg),0) "
                          f"FROM {table} WHERE TRIM(lot_no)=:l"), {"l": lot}).fetchone()

with engine.connect() as c:
    print(f"{'ENT':4} {'LOT':>7} | cold(rows/cart/kg)            | cti_inbox  itin_box  ito_box  | classify")
    print("-"*120)
    summary = []
    for (ent, lot, unit, vak, desc, store, qty, cart, rate, val) in LOTS:
        coldtbl = "cfpl_cold_stocks" if ent == "CFPL" else "cdpl_cold_stocks"
        # also check the OTHER cold table for cross-company stray
        rows_c, cart_c, kg_c = cold_count(c, coldtbl, lot)
        other = "cdpl_cold_stocks" if ent == "CFPL" else "cfpl_cold_stocks"
        rows_o, cart_o, kg_o = cold_count(c, other, lot)

        # cold_transfer_inboxes
        cti = c.execute(text("""
            SELECT COUNT(*) n, COALESCE(SUM(b.no_of_cartons),0) cart, COALESCE(SUM(b.weight_kg),0) kg
            FROM cold_transfer_inboxes b WHERE TRIM(b.lot_no)=:l
        """), {"l": lot}).fetchone()
        # interunit_transfer_in_boxes (IN side, box-per-row)
        itin = c.execute(text("""
            SELECT COUNT(*) n, COALESCE(SUM(net_weight),0) kg
            FROM interunit_transfer_in_boxes WHERE TRIM(lot_number)=:l
        """), {"l": lot}).fetchone()
        # interunit_transfer_boxes (OUT side, box-per-row)
        ito = c.execute(text("""
            SELECT COUNT(*) n, COALESCE(SUM(net_weight),0) kg
            FROM interunit_transfer_boxes WHERE TRIM(lot_number)=:l
        """), {"l": lot}).fetchone()

        # classify
        if rows_c > 0:
            cls = f"ALREADY-IN-COLD({rows_c})"
        elif cti.n == cart:
            cls = "case1(cold_transfer_inboxes EXACT)"
        elif itin.n == cart:
            cls = "case1(interunit_in_boxes EXACT)"
        elif ito.n == cart:
            cls = "case1(interunit_out_boxes EXACT)"
        elif (cti.n + itin.n + ito.n) > 0:
            cls = "case2(partial source)"
        else:
            cls = "case3(brand new)"
        if rows_o > 0:
            cls += f" [!OTHER {other}={rows_o}]"

        print(f"{ent:4} {lot:>7} | {rows_c:>3}r {cart_c:>5}c {kg_c:>8}kg in {coldtbl[:4]} | "
              f"cti={cti.n:>4}/{cti.cart}  itin={itin.n:>4}  ito={ito.n:>4} | {cls}")
        summary.append({"ent":ent,"lot":lot,"need_cartons":cart,"ims_qty":qty,
                        "cold_rows":rows_c,"cti":cti.n,"itin":itin.n,"ito":ito.n,
                        "other_cold":rows_o,"class":cls})

    print("\n=== summary counts ===")
    from collections import Counter
    cc = Counter(s["class"].split(" [")[0] for s in summary)
    for k,v in sorted(cc.items()): print(f"  {v:>2}  {k}")
    with open(os.path.join(os.path.dirname(__file__), "diag_onlyinims_classify_out.json"),"w",encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)
