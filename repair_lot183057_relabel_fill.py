"""Lot 183057 'Deri Dates': relabel existing 33 + add 68 -> 101 boxes, ALL with fresh epoch box_ids.

User decision (2026-06-08): make cfpl_cold_stocks lot 183057 = 101 boxes; give all 101 brand-new
box_ids by the standard format {last 8 digits of epoch ms}-{box_number}, n=1..101. This avoids the
box_id collision with lot 125276 'Al Barakah Fard' (which owns 90640000-43..957 in the same txn).

Steps (one txn): backup the 33 existing rows -> relabel them {base}-1..33 -> insert 68 new rows
{base}-34..101 with the lot's uniform Deri-Dates details (5kg, D-39, value=weight*rate).
Idempotent guard: if lot already = 101 with a single fresh base, skip. Dry-run default; --apply.

NOTE (flagged to user): the 68 added boxes assume the stock physically exists; the Al Barakah
Direct Out count is left unchanged (potential double-count is the user's accepted call).
"""
import os, io, sys, json, time
from datetime import datetime
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from sqlalchemy import create_engine, text

raw = os.environ["DATABASE_URL"]
DB = raw.replace("postgresql://", "postgresql+psycopg://", 1) if raw.startswith("postgresql://") else raw
engine = create_engine(DB)

APPLY = "--apply" in sys.argv
LOT, TXN, TARGET = "183057", "TR-20260314174720", 101
P = {"lot": LOT, "txn": TXN}
BASE = str(int(time.time() * 1000))[-8:]

with engine.begin() as c:
    existing = c.execute(text("SELECT COUNT(*) FROM cfpl_cold_stocks WHERE TRIM(lot_no)=:lot AND transaction_no=:txn"), P).scalar()
    print(f"existing cold rows for lot {LOT}: {existing}   target: {TARGET}   fresh base: {BASE}")
    assert existing == 33, f"ABORT: expected 33 existing rows, got {existing} (re-investigate before running)"
    to_add = TARGET - existing
    assert to_add == 68, f"ABORT: expected to add 68, computed {to_add}"

    # base must be globally unused as a box_id prefix (fresh epoch base => essentially unique)
    clash = c.execute(text("SELECT COUNT(*) FROM cfpl_cold_stocks WHERE box_id LIKE :p"), {"p": f"{BASE}-%"}).scalar()
    assert clash == 0, f"ABORT: base {BASE} already used by {clash} box_ids — re-run for a new epoch base"

    tmpl = c.execute(text("""
        SELECT item_description, unit, weight_kg, storage_location, exporter, group_name, item_subgroup,
               item_mark, cold_item_mark, vakkal, last_purchase_rate, inward_dt, inward_no,
               inward_transaction_no, spl_remarks
        FROM cfpl_cold_stocks WHERE TRIM(lot_no)=:lot AND transaction_no=:txn LIMIT 1
    """), P).fetchone()
    t = dict(tmpl._mapping)
    print("Deri-Dates template:", {k: t[k] for k in ("item_description","unit","weight_kg","last_purchase_rate","vakkal")})

    backup = [dict(r._mapping) for r in c.execute(text(f"SELECT * FROM cfpl_cold_stocks WHERE TRIM(lot_no)=:lot AND transaction_no=:txn ORDER BY box_id"), P).fetchall()]
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bpath = os.path.join(os.path.dirname(__file__), f"repair_backup_lot183057_{stamp}.json")
    with open(bpath, "w", encoding="utf-8") as f:
        json.dump({"lot": LOT, "txn": TXN, "base": BASE, "original_rows": backup}, f, default=str, indent=2)
    print(f"Backup: {bpath} ({len(backup)} original rows)")

    if not APPLY:
        print(f"\nDRY RUN — would relabel 33 -> {BASE}-1..33 and insert 68 -> {BASE}-34..101 (total {TARGET}). Re-run with --apply.")
        raise SystemExit(0)

    # 1. Relabel the 33 existing rows -> {base}-1..33
    relbl = c.execute(text(f"""
        WITH numbered AS (
            SELECT id, ROW_NUMBER() OVER (ORDER BY box_id) rn
            FROM cfpl_cold_stocks WHERE TRIM(lot_no)=:lot AND transaction_no=:txn
        )
        UPDATE cfpl_cold_stocks cs
        SET box_id = :base || '-' || numbered.rn, updated_at = NOW()
        FROM numbered WHERE cs.id = numbered.id
    """), {**P, "base": BASE}).rowcount
    print(f"relabeled existing rows: {relbl}")
    assert relbl == 33

    # 2. Insert 68 new rows -> {base}-34..101
    ins = c.execute(text(f"""
        INSERT INTO cfpl_cold_stocks
            (box_id, transaction_no, lot_no, item_description, unit, weight_kg, total_inventory_kgs,
             no_of_cartons, storage_location, exporter, group_name, item_subgroup, item_mark,
             cold_item_mark, vakkal, last_purchase_rate, value, inward_dt, inward_no,
             inward_transaction_no, spl_remarks, auto_created_from_inward, created_at, updated_at)
        SELECT :base || '-' || gs, :txn, :lot, :item, :unit, :wt, :wt, 1, :loc, :exp, :grp, :subgrp,
               :mark, :cmark, :vak, :rate, ROUND((:wt * COALESCE(:rate,0))::numeric,2),
               :idt, :ino, :itxn, :spl, FALSE, NOW(), NOW()
        FROM generate_series(34, :target) gs
    """), {**P, "base": BASE, "target": TARGET,
           "item": t["item_description"], "unit": t["unit"], "wt": t["weight_kg"], "loc": t["storage_location"],
           "exp": t["exporter"], "grp": t["group_name"], "subgrp": t["item_subgroup"], "mark": t["item_mark"],
           "cmark": t["cold_item_mark"], "vak": t["vakkal"], "rate": t["last_purchase_rate"],
           "idt": t["inward_dt"], "ino": t["inward_no"], "itxn": t["inward_transaction_no"], "spl": t["spl_remarks"]}).rowcount
    print(f"inserted new rows: {ins}")
    assert ins == 68

    total = c.execute(text("SELECT COUNT(*) FROM cfpl_cold_stocks WHERE TRIM(lot_no)=:lot AND transaction_no=:txn"), P).scalar()
    print(f"\nCOMMITTED. lot {LOT} cold now = {total} (box_ids {BASE}-1..{TARGET}).")
    assert total == TARGET
