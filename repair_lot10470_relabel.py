"""One-time repair: relabel 27 mislabeled cold boxes back to lot 10470 (Medjoul).

Context (forensics in chat 2026-06-08):
  box_id 90556000-21..-47 / txn TR-20260314174556 are physically Medjoul (lot 10470)
  but are stored in cdpl_cold_stocks mislabeled as lot 12324 'Wet Dates Safavi'.
  The lot-10470 transfer_out_pending disposition (ids 4498-4524) is a PHANTOM — those
  boxes never left cold. User-confirmed ground truth: the boxes are Medjoul (10470).

Action (IDENTITY ONLY — rate/value left untouched per user):
  1. Backup the 27 cold rows + 27 disposition rows to JSON.
  2. UPDATE the 27 cdpl_cold_stocks rows: lot_no/item_description/item_mark/item_subgroup/exporter -> Medjoul.
  3. Mark disposition ids 4498-4524 reverted (phantom deduction).

Safety: EXACT box_id match (no string ranges), guarded on current lot_no='12324',
asserts exactly 27 targets, single transaction, idempotent (re-run = 0 changes).
Dry-run by default; pass --apply to commit.
"""
import os, io, sys, json
from datetime import datetime
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB)

APPLY = "--apply" in sys.argv
TXN = "TR-20260314174556"
DISP_MIN, DISP_MAX = 4498, 4524
NEW = dict(lot_no="10470", item_description="Medjoul Dates Large",
           item_mark="Medjoul-Large HM", item_subgroup="Medjoul-Large", exporter="Hadiklaim")

with engine.begin() as c:
    # 1. The exact 27 phantom-disposition rows -> their box_ids (the ONLY targets).
    disp = c.execute(text("""
        SELECT id, box_id, transaction_no, lot_no, item_description
        FROM cold_stock_disposition
        WHERE id BETWEEN :a AND :b AND transaction_no = :t AND TRIM(lot_no) = '10470'
        ORDER BY id
    """), {"a": DISP_MIN, "b": DISP_MAX, "t": TXN}).fetchall()
    box_ids = [r._mapping["box_id"] for r in disp]
    print(f"Disposition phantom rows (lot 10470): {len(disp)}  box_ids: {box_ids[0]}..{box_ids[-1]}")
    assert len(disp) == 27, f"ABORT: expected 27 disposition rows, got {len(disp)}"

    # 2. The exact cold_stocks rows to relabel — EXACT box_id IN-list, guarded on lot 12324.
    cold = c.execute(text("""
        SELECT id, box_id, lot_no, item_description, item_mark, exporter, weight_kg, value
        FROM cdpl_cold_stocks
        WHERE transaction_no = :t AND lot_no = '12324' AND box_id = ANY(:bx)
        ORDER BY box_id
    """), {"t": TXN, "bx": box_ids}).fetchall()
    print(f"cold_stocks rows currently mislabeled 12324 to relabel: {len(cold)}")
    for r in cold[:3]:
        print("   sample:", dict(r._mapping))

    already = c.execute(text("""
        SELECT COUNT(*) FROM cdpl_cold_stocks
        WHERE transaction_no = :t AND lot_no = '10470' AND box_id = ANY(:bx)
    """), {"t": TXN, "bx": box_ids}).scalar()
    print(f"cold_stocks rows already lot 10470 (idempotent skip): {already}")

    if len(cold) == 0 and already == 27:
        print("Already repaired — nothing to do."); sys.exit(0)
    assert len(cold) + already == 27, f"ABORT: 27 boxes not fully accounted ({len(cold)}+{already})"

    # 3. Backup BEFORE writing.
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = {
        "txn": TXN, "box_ids": box_ids,
        "cold_rows": [dict(r._mapping) for r in c.execute(text(
            "SELECT * FROM cdpl_cold_stocks WHERE transaction_no=:t AND box_id = ANY(:bx)"),
            {"t": TXN, "bx": box_ids}).fetchall()],
        "disposition_rows": [dict(r._mapping) for r in c.execute(text(
            "SELECT * FROM cold_stock_disposition WHERE id BETWEEN :a AND :b"),
            {"a": DISP_MIN, "b": DISP_MAX}).fetchall()],
    }
    bpath = os.path.join(os.path.dirname(__file__), f"repair_backup_lot10470_{stamp}.json")
    with open(bpath, "w", encoding="utf-8") as f:
        json.dump(backup, f, default=str, indent=2)
    print(f"Backup written: {bpath}  ({len(backup['cold_rows'])} cold rows, {len(backup['disposition_rows'])} disp rows)")

    if not APPLY:
        print("\nDRY RUN — no changes committed. Re-run with --apply to write.")
        raise SystemExit(0)

    # 4. Relabel identity (rate/value left untouched).
    upd = c.execute(text("""
        UPDATE cdpl_cold_stocks
        SET lot_no = :lot_no, item_description = :item_description, item_mark = :item_mark,
            item_subgroup = :item_subgroup, exporter = :exporter
        WHERE transaction_no = :t AND lot_no = '12324' AND box_id = ANY(:bx)
    """), {**NEW, "t": TXN, "bx": box_ids}).rowcount
    print(f"cold_stocks rows relabeled -> lot 10470 Medjoul: {upd}")
    assert upd == len(cold), f"ABORT: expected to update {len(cold)}, updated {upd}"

    # 5. Mark phantom disposition reverted.
    rev = c.execute(text("""
        UPDATE cold_stock_disposition
        SET reverted = TRUE, reverted_at = NOW(),
            reverted_reason = 'Phantom deduction — boxes 90556000-21..47 never left cold; relabel correction 12324->10470 (Medjoul), 2026-06-08'
        WHERE id BETWEEN :a AND :b AND reverted = FALSE
    """), {"a": DISP_MIN, "b": DISP_MAX}).rowcount
    print(f"disposition rows marked reverted: {rev}")
    print("\nCOMMITTED. Re-run (without --apply) to confirm idempotency.")
