"""Two-sided repair for the lot-125320 / lot-183027 cross-company box_id collision.

Forensics (2026-06-08): transfer 869 (TRANS202605290848) = 600 boxes of lot 125320
'Wet Date Sayer' from Rishi(cdpl) -> W202, Received. But the cold deduction removed the
WRONG boxes: 407 cdpl lot-125320 + 193 CFPL lot-183027 'Deri Dates' (Savla D-39) — a
cross-company box_id collision on txn TR-20260314174724 / box 90644000-... So:
  - 193 lot-125320 boxes (cdpl, 90644000-618..810) stayed in cold (should be 0 — they left).
  - 193 lot-183027 'Deri Dates' boxes (cfpl/D-39, 5kg) were wrongly deleted (should be restored).

Action (user-approved full two-sided fix), ONE transaction:
  1. Backup the to-delete cdpl rows + the 183027 disposition rows.
  2. RESTORE 193 Deri Dates into cfpl_cold_stocks (from disp columns + snapshot; weight=total_inventory_kgs).
  3. Write 193 lot-125320 dispositions for the to-delete cdpl boxes (ref TRANS202605290848).
  4. Mark the 193 lot-183027 dispositions reverted (wrong deduction, now restored).
  5. DELETE the 193 cdpl lot-125320 boxes.
Idempotent (guarded). Dry-run default; --apply to commit.
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
REF = "TRANS202605290848"
DEL_W = "TRIM(lot_no) = '125320' AND transaction_no = 'TR-20260314174724'"   # cdpl rows to delete
DISP_W = f"disposition_ref_no = '{REF}' AND TRIM(lot_no) = '183027'"          # Deri Dates to restore/revert
N = 193

with engine.begin() as c:
    del_cnt = c.execute(text(f"SELECT COUNT(*) FROM cdpl_cold_stocks WHERE {DEL_W}")).scalar()
    res_cnt = c.execute(text("SELECT COUNT(*) FROM cfpl_cold_stocks WHERE TRIM(lot_no)='183027' AND transaction_no='TR-20260314174724'")).scalar()
    print(f"cdpl lot-125320 to delete: {del_cnt}   |   cfpl lot-183027 already present: {res_cnt}")
    if del_cnt == 0 and res_cnt >= N:
        print("Already repaired — nothing to do."); sys.exit(0)
    assert del_cnt == N, f"ABORT: expected {N} cdpl rows to delete, got {del_cnt}"
    disp_cnt = c.execute(text(f"SELECT COUNT(*) FROM cold_stock_disposition WHERE {DISP_W}")).scalar()
    assert disp_cnt == N, f"ABORT: expected {N} disposition rows for 183027, got {disp_cnt}"

    # Preview the restore reconstruction (what cfpl rows we'd build).
    preview = c.execute(text(f"""
        SELECT d.box_id, d.lot_no, d.item_description, d.unit,
               (d.snapshot_data->>'total_inventory_kgs')::numeric AS weight_kg,
               d.snapshot_data->>'storage_location' AS storage_location,
               d.snapshot_data->>'exporter' AS exporter
        FROM cold_stock_disposition d WHERE {DISP_W} ORDER BY d.id LIMIT 3
    """)).fetchall()
    print("Restore preview (cfpl Deri Dates):")
    for r in preview:
        print("   ", dict(r._mapping))

    # Backup
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = {
        "del_cdpl_125320": [dict(r._mapping) for r in c.execute(text(f"SELECT * FROM cdpl_cold_stocks WHERE {DEL_W}")).fetchall()],
        "disp_183027": [dict(r._mapping) for r in c.execute(text(f"SELECT * FROM cold_stock_disposition WHERE {DISP_W}")).fetchall()],
    }
    bpath = os.path.join(os.path.dirname(__file__), f"repair_backup_125320_183027_{stamp}.json")
    with open(bpath, "w", encoding="utf-8") as f:
        json.dump(backup, f, default=str, indent=2)
    print(f"Backup: {bpath}  ({len(backup['del_cdpl_125320'])} del rows, {len(backup['disp_183027'])} disp rows)")

    if not APPLY:
        print("\nDRY RUN — no changes. Re-run with --apply to commit the two-sided fix.")
        raise SystemExit(0)

    # 2. RESTORE Deri Dates into cfpl_cold_stocks (idempotent via NOT EXISTS).
    restored = c.execute(text(f"""
        INSERT INTO cfpl_cold_stocks
            (box_id, transaction_no, lot_no, item_description, unit, weight_kg, total_inventory_kgs,
             no_of_cartons, storage_location, exporter, group_name, item_subgroup, item_mark, vakkal,
             last_purchase_rate, value, inward_no, inward_transaction_no, spl_remarks,
             created_at, updated_at, auto_created_from_inward)
        SELECT d.box_id, d.transaction_no, d.lot_no, d.item_description, d.unit,
               (d.snapshot_data->>'total_inventory_kgs')::numeric,
               (d.snapshot_data->>'total_inventory_kgs')::numeric,
               1,
               d.snapshot_data->>'storage_location', d.snapshot_data->>'exporter',
               d.snapshot_data->>'group_name', d.snapshot_data->>'item_subgroup',
               d.snapshot_data->>'item_mark', d.snapshot_data->>'vakkal',
               NULLIF(d.snapshot_data->>'last_purchase_rate','')::numeric,
               NULLIF(d.snapshot_data->>'value','')::numeric,
               d.snapshot_data->>'inward_no', d.snapshot_data->>'inward_transaction_no',
               d.snapshot_data->>'spl_remarks', NOW(), NOW(), FALSE
        FROM cold_stock_disposition d
        WHERE {DISP_W}
          AND NOT EXISTS (SELECT 1 FROM cfpl_cold_stocks cs
                          WHERE cs.box_id = d.box_id AND cs.transaction_no = d.transaction_no)
    """)).rowcount
    print(f"Deri Dates restored into cfpl_cold_stocks: {restored}")

    # 3. Write lot-125320 dispositions for the boxes being deleted (full snapshot via to_jsonb).
    new_disp = c.execute(text(f"""
        INSERT INTO cold_stock_disposition
            (box_id, transaction_no, lot_no, item_description, from_company, unit, from_site,
             source_table, disposition_type, disposition_ref_no, disposed_by, snapshot_data, notes, reverted)
        SELECT cs.box_id, cs.transaction_no, cs.lot_no, cs.item_description, 'cdpl', cs.unit, 'Cold Storage',
               'cdpl_cold_stocks', 'transfer_out_pending', :ref, 'system-repair-125320',
               to_jsonb(cs),
               'Leak/collision repair 2026-06-08: correct lot-125320 deduction for transfer 869 (mis-deducted 183027 reverted).',
               FALSE
        FROM cdpl_cold_stocks cs WHERE {DEL_W}
    """), {"ref": REF}).rowcount
    print(f"lot-125320 dispositions written: {new_disp}")

    # 4. Revert the wrong lot-183027 dispositions.
    reverted = c.execute(text(f"""
        UPDATE cold_stock_disposition
        SET reverted = TRUE, reverted_at = NOW(),
            reverted_reason = 'Wrong-lot deduction by transfer 869 (cross-company box_id collision); Deri Dates restored to cfpl_cold_stocks 2026-06-08'
        WHERE {DISP_W} AND reverted = FALSE
    """)).rowcount
    print(f"lot-183027 dispositions reverted: {reverted}")

    # 5. Delete the 193 leftover cdpl lot-125320 boxes.
    deleted = c.execute(text(f"DELETE FROM cdpl_cold_stocks WHERE {DEL_W}")).rowcount
    print(f"cdpl lot-125320 deleted: {deleted}")

    assert restored == N and new_disp == N and deleted == N, "ABORT: count mismatch"
    print(f"\nCOMMITTED. cdpl 125320 -> 0; cfpl 183027 -> {res_cnt + restored}; ledger reattributed.")
