"""Repair transfers that were wrongly marked 'Received' while incomplete.

For every Transfer-Out whose header is 'Received' but which is not fully received
(dispatched OUT boxes > received IN boxes, OR has stuck In-Transit pending rows):

  1. Re-park each MISSING box (an OUT box not in transfer_in_boxes and not already
     In Transit) into pending_transfer_stock as 'In Transit'. Item/lot/weights come
     from interunit_transfer_boxes (the dispatch record = source of truth).
     NO physical stock is touched (source was already deducted at OUT); this only
     restores the in-transit ledger.
  2. Re-open the headers: Transfer-Out -> 'Dispatch', Transfer-In -> 'Pending'.

DRY-RUN by default (prints the plan). Pass --apply to write. Single transaction.

NOTE: from_company/to_company/article columns are nullable now and omitted from
the INSERT (matching current code). Company is derived only to choose the right
source_table/destination_table.
"""
import os, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
APPLY = "--apply" in sys.argv
eng = create_engine(DB)

COLD_SITES = {"cold storage", "rishi cold", "savla d-39 cold", "savla d-514 cold"}


def is_cold(site):
    return (site or "").strip().lower() in COLD_SITES


def derive_company(c, box_id, txn, from_site):
    r = c.execute(text("""SELECT from_company FROM cold_stock_disposition
                          WHERE box_id=:b AND transaction_no=:t AND from_company IS NOT NULL
                          ORDER BY disposed_at DESC LIMIT 1"""), {"b": box_id, "t": txn}).fetchone()
    if r and r.from_company:
        return r.from_company.lower()
    for comp, tbl in (("cfpl", "cfpl_cold_stocks"), ("cdpl", "cdpl_cold_stocks")):
        if c.execute(text(f"SELECT 1 FROM {tbl} WHERE transaction_no=:t LIMIT 1"), {"t": txn}).fetchone():
            return comp
    s = (from_site or "").lower()
    return "cdpl" if ("rishi" in s or "cdpl" in s) else "cfpl"


# IMPORTANT: interunit_transfer_boxes contains duplicate rows (e.g. transfer 653
# has 711 rows for only 3 distinct boxes). All counts MUST use DISTINCT
# (box_id, transaction_no), and "received" is matched against THIS transfer's own
# Transfer-In, so genuinely-complete transfers are not falsely reopened.

# A box of this transfer is MISSING if its distinct key is neither received in
# this transfer's IN nor currently parked In Transit.
MISSING_BOXES = text("""
    SELECT DISTINCT ob.box_id, ob.transaction_no, ob.article, ob.lot_number,
           ob.net_weight, ob.gross_weight
    FROM interunit_transfer_boxes ob
    WHERE ob.header_id=:hid
      AND ob.box_id IS NOT NULL AND ob.box_id <> ''
      AND ob.transaction_no IS NOT NULL AND ob.transaction_no <> ''
      AND NOT EXISTS (
          SELECT 1 FROM interunit_transfer_in_boxes ib
          JOIN interunit_transfer_in_header ti ON ti.id=ib.header_id
          WHERE ti.transfer_out_id=:hid
            AND ib.box_id=ob.box_id AND ib.transaction_no=ob.transaction_no)
      AND NOT EXISTS (
          SELECT 1 FROM pending_transfer_stock p
          WHERE p.box_id=ob.box_id AND p.transaction_no=ob.transaction_no
            AND p.status='In Transit')
""")

# Candidate transfers: 'Received' that still have >=1 missing box OR stuck pending.
FIND = text("""
    SELECT h.id, h.challan_no, h.from_site, h.to_site, h.created_by, h.created_ts,
           (SELECT COUNT(DISTINCT (box_id, transaction_no))
            FROM interunit_transfer_boxes WHERE header_id=h.id) dispatched,
           (SELECT COUNT(DISTINCT (ib.box_id, ib.transaction_no))
            FROM interunit_transfer_in_boxes ib
            JOIN interunit_transfer_in_header ti ON ti.id=ib.header_id
            WHERE ti.transfer_out_id=h.id) received,
           (SELECT COUNT(*) FROM pending_transfer_stock p
            WHERE p.transfer_out_id=h.id AND p.status='In Transit'
              AND p.box_id NOT LIKE 'LINE-%') in_transit
    FROM interunit_transfers_header h
    WHERE h.status='Received'
      AND (
        EXISTS (
          SELECT 1 FROM interunit_transfer_boxes ob
          WHERE ob.header_id=h.id
            AND ob.box_id IS NOT NULL AND ob.box_id <> ''
            AND ob.transaction_no IS NOT NULL AND ob.transaction_no <> ''
            AND NOT EXISTS (
                SELECT 1 FROM interunit_transfer_in_boxes ib
                JOIN interunit_transfer_in_header ti ON ti.id=ib.header_id
                WHERE ti.transfer_out_id=h.id
                  AND ib.box_id=ob.box_id AND ib.transaction_no=ob.transaction_no)
            AND NOT EXISTS (
                SELECT 1 FROM pending_transfer_stock p
                WHERE p.box_id=ob.box_id AND p.transaction_no=ob.transaction_no
                  AND p.status='In Transit'))
        OR EXISTS (
          SELECT 1 FROM pending_transfer_stock p
          WHERE p.transfer_out_id=h.id AND p.status='In Transit'
            AND p.box_id NOT LIKE 'LINE-%')
      )
    ORDER BY h.id
""")

INSERT_PENDING = text("""
    INSERT INTO pending_transfer_stock
        (transfer_type, transfer_out_id, transfer_out_challan_no,
         box_id, transaction_no, from_site, to_site,
         from_storage_type, to_storage_type,
         source_table, destination_table,
         item_description, lot_no, weight_kg, no_of_cartons,
         gross_weight, net_weight,
         status, dispatched_at, dispatched_by)
    VALUES
        ('INTERUNIT', :toid, :challan, :box_id, :txn, :from_site, :to_site,
         :fst, :tst, :src, :dst,
         :item, :lot, :wkg, 1, :gross, :net,
         'In Transit', :ts, :by)
    ON CONFLICT (box_id, transaction_no) DO NOTHING
""")

total_transfers = total_reparked = 0
with eng.begin() as c:
    transfers = c.execute(FIND).fetchall()
    print(f"Transfers to repair: {len(transfers)}  APPLY={APPLY}\n")
    for t in transfers:
        missing = c.execute(MISSING_BOXES, {"hid": t.id}).fetchall()
        print(f"  #{t.id} {t.challan_no}: dispatched={t.dispatched} received={t.received} "
              f"in_transit={t.in_transit} -> re-park {len(missing)} missing; "
              f"reopen OUT->Dispatch, IN->Pending")
        total_transfers += 1
        total_reparked += len(missing)
        if not APPLY:
            continue
        fst = "cold" if is_cold(t.from_site) else "warehouse"
        tst = "cold" if is_cold(t.to_site) else "warehouse"
        for b in missing:
            comp = derive_company(c, b.box_id, b.transaction_no, t.from_site)
            src = f"{comp}_cold_stocks" if fst == "cold" else f"{comp}_bulk_entry_boxes"
            dst = f"{comp}_cold_stocks" if tst == "cold" else f"{comp}_boxes_v2"
            c.execute(INSERT_PENDING, {
                "toid": t.id, "challan": t.challan_no,
                "box_id": b.box_id, "txn": b.transaction_no,
                "from_site": t.from_site, "to_site": t.to_site,
                "fst": fst, "tst": tst, "src": src, "dst": dst,
                "item": b.article, "lot": b.lot_number,
                "wkg": float(b.net_weight or 0),
                "gross": b.gross_weight, "net": b.net_weight,
                "ts": t.created_ts, "by": t.created_by or "bridge_repair",
            })
        # Re-open headers
        c.execute(text("UPDATE interunit_transfers_header SET status='Dispatch' "
                       "WHERE id=:id AND status='Received'"), {"id": t.id})
        c.execute(text("UPDATE interunit_transfer_in_header SET status='Pending' "
                       "WHERE transfer_out_id=:id AND status='Received'"), {"id": t.id})

    print(f"\nSUMMARY: {total_transfers} transfers, {total_reparked} boxes re-parked.")
    if not APPLY:
        print("DRY-RUN only — no writes. Re-run with --apply to commit.")
print("Done.")
