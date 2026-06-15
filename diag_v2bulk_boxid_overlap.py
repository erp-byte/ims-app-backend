"""READ-ONLY: for the cold lots that overlap v2/bulk, measure box_id agreement
(how many cold box_ids also exist in v2 / bulk for the same lot) + box_id bases."""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text
raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB, pool_pre_ping=True)

def bases(idlist):
    return sorted({(b or "").rsplit("-", 1)[0] for b in idlist})

for comp in ["cfpl", "cdpl"]:
    cs, v2, bk = f"{comp}_cold_stocks", f"{comp}_boxes_v2", f"{comp}_bulk_entry_boxes"
    with engine.connect() as c:
        lots = c.execute(text(f"""
          SELECT DISTINCT TRIM(cs.lot_no) lot FROM {cs} cs WHERE cs.box_id IS NOT NULL
            AND (EXISTS(SELECT 1 FROM {v2} v WHERE TRIM(v.lot_number)=TRIM(cs.lot_no))
              OR EXISTS(SELECT 1 FROM {bk} b WHERE TRIM(b.lot_number)=TRIM(cs.lot_no)))
        """)).scalars().all()
        print(f"\n===== [{comp}] box_id agreement for {len(lots)} overlapping lots =====")
        for lot in sorted(lots):
            cold_ids = c.execute(text(f"SELECT box_id FROM {cs} WHERE TRIM(lot_no)=:l AND box_id IS NOT NULL"), {"l": lot}).scalars().all()
            v2_ids = c.execute(text(f"SELECT box_id FROM {v2} WHERE TRIM(lot_number)=:l"), {"l": lot}).scalars().all()
            bk_ids = c.execute(text(f"SELECT box_id FROM {bk} WHERE TRIM(lot_number)=:l"), {"l": lot}).scalars().all()
            cset, vset, bset = set(cold_ids), set(v2_ids), set(bk_ids)
            ov_v = len(cset & vset); ov_b = len(cset & bset)
            box_ok = (vset and cset <= vset) or (bset and cset <= bset)
            print(f"  lot {lot:>8} | cold={len(cset)} v2={len(vset)} bulk={len(bset)} | "
                  f"cold∩v2={ov_v} cold∩bulk={ov_b} | box_match={'Y' if box_ok else 'N'}")
            print(f"      cold bases: {bases(cold_ids)[:6]}")
            print(f"      v2 bases  : {bases(v2_ids)[:6]}   bulk bases: {bases(bk_ids)[:6]}")
