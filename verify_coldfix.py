"""
Verify the bulk-sticker -> cold_stocks mirror fix by calling create_inward_bulk_sticker
directly for a COLD warehouse (expect cold rows) and a DRY warehouse (expect none).
Cleans up all test rows afterwards. Self-contained; safe.
"""
import io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import os
os.environ.setdefault("PYTHONUNBUFFERED", "1")

from shared.database import SessionLocal
from services.ims_service.inward_models import (
    BulkStickerPayload, TransactionIn, BulkStickerArticleIn,
)
from services.ims_service.inward_tools import create_inward_bulk_sticker, table_names
from sqlalchemy import text

COLD_TXN = "TEST-COLDFIX-VERIFY-COLD"
DRY_TXN = "TEST-COLDFIX-VERIFY-DRY"
COMPANY = "CFPL"


def build(txno, warehouse, nboxes):
    return BulkStickerPayload(
        company=COMPANY,
        transaction=TransactionIn(
            transaction_no=txno, entry_date="2026-06-05",
            warehouse=warehouse, vendor_supplier_name="VERIFY-VENDOR",
        ),
        articles=[BulkStickerArticleIn(
            transaction_no=txno, item_description="VERIFY ITEM - cold mirror test",
            item_category="DATES", sub_category="Test", lot_number="VERIFY9999",
            unit_rate=100, box_count=nboxes, box_net_weight=10,
        )],
    )


def cold_count(db, txno):
    return db.execute(text("SELECT count(*) FROM cfpl_cold_stocks WHERE transaction_no=:t"),
                      {"t": txno}).scalar()


def box_count(db, txno):
    return db.execute(text("SELECT count(*) FROM cfpl_boxes_v2 WHERE transaction_no=:t"),
                      {"t": txno}).scalar()


def cleanup(db):
    t = table_names(COMPANY)
    for txno in (COLD_TXN, DRY_TXN):
        for tbl in (t["cold_stocks"], t["box"], t["art"], t["tx"]):
            try:
                db.execute(text(f"DELETE FROM {tbl} WHERE transaction_no=:t"), {"t": txno})
            except Exception as e:
                db.rollback(); print(f"  cleanup skip {tbl}: {e}")
    db.commit()


db = SessionLocal()
try:
    print("Pre-clean any leftovers...")
    cleanup(db)

    print("\n[1] COLD warehouse (Savla D-39), 3 boxes:")
    create_inward_bulk_sticker(build(COLD_TXN, "Savla D-39", 3), db)
    cc, bc = cold_count(db, COLD_TXN), box_count(db, COLD_TXN)
    print(f"    boxes_v2 rows = {bc} (expect 3) | cold_stocks rows = {cc} (expect 3)")
    ok_cold = (bc == 3 and cc == 3)

    print("\n[2] DRY warehouse (W202), 2 boxes:")
    create_inward_bulk_sticker(build(DRY_TXN, "W202", 2), db)
    cc2, bc2 = cold_count(db, DRY_TXN), box_count(db, DRY_TXN)
    print(f"    boxes_v2 rows = {bc2} (expect 2) | cold_stocks rows = {cc2} (expect 0)")
    ok_dry = (bc2 == 2 and cc2 == 0)

    # inspect one cold row to confirm fields
    row = db.execute(text("""SELECT lot_no, unit, storage_location, no_of_cartons, weight_kg,
                                    auto_created_from_inward, box_id
                             FROM cfpl_cold_stocks WHERE transaction_no=:t LIMIT 1"""),
                     {"t": COLD_TXN}).fetchone()
    print(f"\n    sample cold row: {row}")

    print("\nRESULT:", "PASS ✅" if (ok_cold and ok_dry) else "FAIL ❌")
finally:
    print("\nCleaning up test data...")
    cleanup(db)
    print("  done — verified leftovers removed:",
          "cold", cold_count(db, COLD_TXN), "/", cold_count(db, DRY_TXN))
    db.close()
