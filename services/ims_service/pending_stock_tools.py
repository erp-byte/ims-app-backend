"""Pending Transfer Stock helpers.

Middleware between Transfer Out (dispatch) and Transfer In (receive).

Flow:
    create_transfer       → park_in_pending     (deduct source, insert pending)
    finalize_transfer_in  → pick_from_pending   (insert destination, delete pending)
    create_transfer_in    → pick_from_pending   (one-shot variant)
    delete_transfer_in    → unpick_to_pending   (delete destination, restore pending)
    delete_transfer       → restore_to_source   (restore source, delete pending)

Read APIs (for frontend):
    list_pending_transfers   — grouped per transfer_out for the Pending modal
    pending_by_lot           — totals per (lot_no, item_description) for inventory display
"""
import json
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger

logger = get_logger("ims.pending_stock")


COLD_STORAGE_SITE_NAMES = {
    "cold storage",
    "rishi cold",
    "savla d-39 cold",
    "savla d-514 cold",
}


def _is_cold_site(site: Optional[str]) -> bool:
    return (site or "").strip().lower() in COLD_STORAGE_SITE_NAMES


def _table_exists(db: Session, table: str) -> bool:
    return bool(
        db.execute(
            text("SELECT to_regclass(:t)"),
            {"t": f"public.{table}"},
        ).scalar()
    )


def _find_in_cold_stocks(db: Session, box_id: str, transaction_no: str):
    """Look up a cold-stock row by (box_id, transaction_no). Returns (table, row) or (None, None)."""
    for table in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
        if not _table_exists(db, table):
            continue
        row = db.execute(
            text(f"SELECT * FROM {table} WHERE box_id = :bid AND transaction_no = :tno LIMIT 1"),
            {"bid": box_id, "tno": transaction_no},
        ).fetchone()
        if not row:
            row = db.execute(
                text(f"SELECT * FROM {table} WHERE box_id = :bid LIMIT 1"),
                {"bid": box_id},
            ).fetchone()
        if row:
            return table, row
    return None, None


def _find_in_bulk_entry(db: Session, box_id: str, transaction_no: str):
    """Look up a bulk_entry_boxes row by (box_id, transaction_no). Returns (table, row) or (None, None)."""
    for table in ("cfpl_bulk_entry_boxes", "cdpl_bulk_entry_boxes"):
        if not _table_exists(db, table):
            continue
        row = db.execute(
            text(f"SELECT * FROM {table} WHERE box_id = :bid AND transaction_no = :tno LIMIT 1"),
            {"bid": box_id, "tno": transaction_no},
        ).fetchone()
        if row:
            return table, row
    return None, None


def _company_from_table(table: str) -> str:
    return "cfpl" if table.startswith("cfpl") else "cdpl"


def _destination_table(to_storage_type: str, to_company: str) -> str:
    if to_storage_type == "cold":
        return f"{to_company}_cold_stocks"
    return f"{to_company}_bulk_entry_boxes"


def _cold_row_to_json(row) -> dict:
    """Extract cold_stocks columns into a JSON-serializable dict for pending_transfer_stock.cold_storage_data."""
    def g(name, default=None):
        v = getattr(row, name, default)
        if isinstance(v, datetime):
            return v.isoformat()
        if hasattr(v, "isoformat"):
            return v.isoformat()
        if v is None:
            return None
        try:
            float(v)
            return float(v) if not isinstance(v, (int, str, bool)) else v
        except (TypeError, ValueError):
            return v

    return {
        "inward_dt": g("inward_dt"),
        "unit": g("unit"),
        "inward_no": g("inward_no"),
        "item_mark": g("item_mark"),
        "vakkal": g("vakkal"),
        "group_name": g("group_name"),
        "item_subgroup": g("item_subgroup"),
        "storage_location": g("storage_location"),
        "exporter": g("exporter"),
        "last_purchase_rate": g("last_purchase_rate"),
        "value": g("value"),
        "total_inventory_kgs": g("total_inventory_kgs"),
        "spl_remarks": g("spl_remarks"),
        "inward_transaction_no": g("inward_transaction_no"),
    }


# ----------------------------------------------------------------------------
#  park_in_pending — called from create_transfer
# ----------------------------------------------------------------------------
def park_in_pending(
    transfer_out_id: int,
    challan_no: str,
    from_site: str,
    to_site: str,
    boxes: list,
    dispatched_by: str,
    db: Session,
    transfer_type: str = "INTERUNIT",
) -> int:
    """Deduct each box from its source table and insert a corresponding row in
    pending_transfer_stock (status='In Transit'). Returns count parked."""
    from_storage_type = "cold" if _is_cold_site(from_site) else "warehouse"
    to_storage_type = "cold" if _is_cold_site(to_site) else "warehouse"
    parked = 0
    now = datetime.now()

    for box in boxes:
        box_id = (getattr(box, "box_id", "") or "").strip()
        transaction_no = (getattr(box, "transaction_no", "") or "").strip()
        if not box_id or not transaction_no or transaction_no == "DIRECT":
            logger.info("PARK_PENDING: skip box without id/txn (box_id=%r, tno=%r)", box_id, transaction_no)
            continue

        source_table, source_row = (None, None)
        cold_data = None
        warehouse_data = {}

        if from_storage_type == "cold":
            source_table, source_row = _find_in_cold_stocks(db, box_id, transaction_no)
            if source_row is None:
                logger.warning("PARK_PENDING: no cold_stocks match for box_id=%s tno=%s", box_id, transaction_no)
                continue
            cold_data = _cold_row_to_json(source_row)
            item_description = getattr(source_row, "item_description", None) or getattr(box, "article", "")
            lot_no = getattr(source_row, "lot_no", None) or getattr(box, "lot_number", None)
            weight_kg = float(getattr(source_row, "weight_kg", 0) or getattr(box, "net_weight", 0) or 0)
            no_of_cartons = int(getattr(source_row, "no_of_cartons", 1) or 1)
        else:
            source_table, source_row = _find_in_bulk_entry(db, box_id, transaction_no)
            if source_row is None:
                logger.warning("PARK_PENDING: no bulk_entry_boxes match for box_id=%s tno=%s", box_id, transaction_no)
                continue
            item_description = getattr(source_row, "article_description", None) or getattr(box, "article", "")
            lot_no = getattr(source_row, "lot_number", None) or getattr(box, "lot_number", None)
            weight_kg = float(getattr(source_row, "net_weight", 0) or getattr(box, "net_weight", 0) or 0)
            no_of_cartons = 1
            warehouse_data = {
                "gross_weight": float(getattr(source_row, "gross_weight", 0) or 0),
                "net_weight": weight_kg,
                "article": getattr(source_row, "article_description", None),
            }

        from_company = _company_from_table(source_table)
        to_company = from_company  # default; may be overridden by destination table later
        destination_table = _destination_table(to_storage_type, to_company)

        db.execute(
            text("""
                INSERT INTO pending_transfer_stock
                    (transfer_type, transfer_out_id, transfer_out_challan_no,
                     box_id, transaction_no,
                     from_company, to_company, from_site, to_site,
                     from_storage_type, to_storage_type,
                     source_table, source_row_id, destination_table,
                     item_description, lot_no, weight_kg, no_of_cartons,
                     cold_storage_data,
                     gross_weight, net_weight, article,
                     status, dispatched_at, dispatched_by)
                VALUES
                    (:transfer_type, :transfer_out_id, :challan_no,
                     :box_id, :transaction_no,
                     :from_company, :to_company, :from_site, :to_site,
                     :from_storage_type, :to_storage_type,
                     :source_table, :source_row_id, :destination_table,
                     :item_description, :lot_no, :weight_kg, :no_of_cartons,
                     CAST(:cold_storage_data AS JSONB),
                     :gross_weight, :net_weight, :article,
                     'In Transit', :dispatched_at, :dispatched_by)
                ON CONFLICT (box_id, transaction_no) DO NOTHING
            """),
            {
                "transfer_type": transfer_type,
                "transfer_out_id": transfer_out_id,
                "challan_no": challan_no,
                "box_id": box_id,
                "transaction_no": transaction_no,
                "from_company": from_company,
                "to_company": to_company,
                "from_site": from_site,
                "to_site": to_site,
                "from_storage_type": from_storage_type,
                "to_storage_type": to_storage_type,
                "source_table": source_table,
                "source_row_id": getattr(source_row, "id", None),
                "destination_table": destination_table,
                "item_description": item_description,
                "lot_no": lot_no,
                "weight_kg": weight_kg,
                "no_of_cartons": no_of_cartons,
                "cold_storage_data": json.dumps(cold_data) if cold_data else None,
                "gross_weight": warehouse_data.get("gross_weight"),
                "net_weight": warehouse_data.get("net_weight"),
                "article": warehouse_data.get("article"),
                "dispatched_at": now,
                "dispatched_by": dispatched_by,
            },
        )

        # Delete from source table (atomic with pending insert via outer transaction)
        db.execute(
            text(f"DELETE FROM {source_table} WHERE id = :rid"),
            {"rid": getattr(source_row, "id")},
        )

        parked += 1
        logger.info(
            "PARK_PENDING: box_id=%s tno=%s source=%s → pending (transfer_out_id=%s)",
            box_id, transaction_no, source_table, transfer_out_id,
        )

    logger.info("PARK_PENDING: parked %d/%d boxes for transfer_out_id=%s", parked, len(boxes), transfer_out_id)
    return parked


# ----------------------------------------------------------------------------
#  pick_from_pending — called from create_transfer_in / finalize_transfer_in
# ----------------------------------------------------------------------------
def pick_from_pending(transfer_out_id: int, db: Session, challan_no_for_inward: Optional[str] = None) -> int:
    """Move every 'In Transit' row tied to this transfer_out into its destination
    table, then delete the pending row. Returns count picked."""
    pending_rows = db.execute(
        text("""
            SELECT * FROM pending_transfer_stock
            WHERE transfer_out_id = :tid AND status = 'In Transit'
        """),
        {"tid": transfer_out_id},
    ).fetchall()

    picked = 0
    for p in pending_rows:
        dest = p.destination_table
        if not _table_exists(db, dest):
            logger.warning("PICK_PENDING: destination table %s missing, skip box_id=%s", dest, p.box_id)
            continue

        cold_json = p.cold_storage_data or {}

        if dest.endswith("_cold_stocks"):
            db.execute(
                text(f"""
                    INSERT INTO {dest}
                        (inward_dt, unit, inward_no, item_description, item_mark,
                         vakkal, lot_no, no_of_cartons, weight_kg,
                         total_inventory_kgs, group_name, item_subgroup, storage_location,
                         exporter, last_purchase_rate, value,
                         box_id, transaction_no, spl_remarks)
                    VALUES
                        (:inward_dt, :unit, :inward_no, :item_description, :item_mark,
                         :vakkal, :lot_no, :no_of_cartons, :weight_kg,
                         :total_inventory_kgs, :group_name, :item_subgroup, :storage_location,
                         :exporter, :last_purchase_rate, :value,
                         :box_id, :transaction_no, :spl_remarks)
                """),
                {
                    "inward_dt": cold_json.get("inward_dt"),
                    "unit": cold_json.get("unit") or p.to_site,
                    "inward_no": challan_no_for_inward or cold_json.get("inward_no") or p.transfer_out_challan_no,
                    "item_description": p.item_description,
                    "item_mark": cold_json.get("item_mark"),
                    "vakkal": cold_json.get("vakkal"),
                    "lot_no": p.lot_no,
                    "no_of_cartons": p.no_of_cartons or 1,
                    "weight_kg": p.weight_kg,
                    "total_inventory_kgs": cold_json.get("total_inventory_kgs") or float(p.weight_kg or 0),
                    "group_name": cold_json.get("group_name"),
                    "item_subgroup": cold_json.get("item_subgroup"),
                    "storage_location": cold_json.get("storage_location") or p.to_site,
                    "exporter": cold_json.get("exporter"),
                    "last_purchase_rate": cold_json.get("last_purchase_rate"),
                    "value": cold_json.get("value"),
                    "box_id": p.box_id,
                    "transaction_no": p.transaction_no,
                    "spl_remarks": cold_json.get("spl_remarks"),
                },
            )
        else:
            db.execute(
                text(f"""
                    INSERT INTO {dest}
                        (box_id, transaction_no, article_description, lot_number,
                         net_weight, gross_weight)
                    VALUES
                        (:box_id, :transaction_no, :article, :lot_no,
                         :net_weight, :gross_weight)
                """),
                {
                    "box_id": p.box_id,
                    "transaction_no": p.transaction_no,
                    "article": p.article or p.item_description,
                    "lot_no": p.lot_no,
                    "net_weight": p.net_weight or p.weight_kg,
                    "gross_weight": p.gross_weight,
                },
            )

        db.execute(
            text("DELETE FROM pending_transfer_stock WHERE id = :id"),
            {"id": p.id},
        )
        picked += 1
        logger.info("PICK_PENDING: box_id=%s tno=%s → %s (transfer_out_id=%s)",
                    p.box_id, p.transaction_no, dest, transfer_out_id)

    logger.info("PICK_PENDING: picked %d rows for transfer_out_id=%s", picked, transfer_out_id)
    return picked


# ----------------------------------------------------------------------------
#  unpick_to_pending — called from delete_transfer_in
# ----------------------------------------------------------------------------
def unpick_to_pending(transfer_in_id: int, transfer_out_id: int, db: Session) -> int:
    """Reverse a Transfer In: delete the boxes from destination table and
    re-insert them into pending_transfer_stock (status='In Transit'). Returns count restored."""
    transfer_out = db.execute(
        text("SELECT id, challan_no, from_site, to_site, created_by FROM interunit_transfers_header WHERE id = :tid"),
        {"tid": transfer_out_id},
    ).fetchone()
    if not transfer_out:
        logger.warning("UNPICK_PENDING: transfer_out %s not found, nothing to restore", transfer_out_id)
        return 0

    from_site = transfer_out.from_site or ""
    to_site = transfer_out.to_site or ""
    from_storage_type = "cold" if _is_cold_site(from_site) else "warehouse"
    to_storage_type = "cold" if _is_cold_site(to_site) else "warehouse"

    in_boxes = db.execute(
        text("""
            SELECT box_id, transaction_no, article, lot_number, batch_number,
                   net_weight, gross_weight
            FROM interunit_transfer_in_boxes WHERE header_id = :hid
        """),
        {"hid": transfer_in_id},
    ).fetchall()

    restored = 0
    now = datetime.now()

    for b in in_boxes:
        if not b.box_id or not b.transaction_no:
            continue

        # Determine destination table (where the box currently lives)
        dest_table = None
        if to_storage_type == "cold":
            for t in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
                if _table_exists(db, t):
                    found = db.execute(
                        text(f"SELECT id FROM {t} WHERE box_id = :bid AND transaction_no = :tno LIMIT 1"),
                        {"bid": b.box_id, "tno": b.transaction_no},
                    ).fetchone()
                    if found:
                        dest_table = t
                        break
        else:
            for t in ("cfpl_bulk_entry_boxes", "cdpl_bulk_entry_boxes"):
                if _table_exists(db, t):
                    found = db.execute(
                        text(f"SELECT id FROM {t} WHERE box_id = :bid AND transaction_no = :tno LIMIT 1"),
                        {"bid": b.box_id, "tno": b.transaction_no},
                    ).fetchone()
                    if found:
                        dest_table = t
                        break

        # Capture row snapshot before delete so we can rebuild cold_storage_data
        cold_data = None
        item_description = b.article or ""
        lot_no = b.lot_number or None
        weight_kg = float(b.net_weight or 0)
        no_of_cartons = 1

        if dest_table and dest_table.endswith("_cold_stocks"):
            row = db.execute(
                text(f"SELECT * FROM {dest_table} WHERE box_id = :bid AND transaction_no = :tno LIMIT 1"),
                {"bid": b.box_id, "tno": b.transaction_no},
            ).fetchone()
            if row is not None:
                cold_data = _cold_row_to_json(row)
                item_description = getattr(row, "item_description", None) or item_description
                lot_no = getattr(row, "lot_no", None) or lot_no
                weight_kg = float(getattr(row, "weight_kg", 0) or weight_kg)
                no_of_cartons = int(getattr(row, "no_of_cartons", 1) or 1)

        # Delete from destination
        if dest_table:
            db.execute(
                text(f"DELETE FROM {dest_table} WHERE box_id = :bid AND transaction_no = :tno"),
                {"bid": b.box_id, "tno": b.transaction_no},
            )

        # Re-insert into pending_transfer_stock
        from_company = "cfpl"
        to_company = "cfpl"
        if dest_table and dest_table.startswith("cdpl"):
            to_company = "cdpl"
        source_table_guess = (
            "cfpl_cold_stocks" if from_storage_type == "cold" and to_company == "cfpl"
            else "cdpl_cold_stocks" if from_storage_type == "cold"
            else "cfpl_bulk_entry_boxes" if to_company == "cfpl"
            else "cdpl_bulk_entry_boxes"
        )
        destination_table_keep = dest_table or _destination_table(to_storage_type, to_company)

        db.execute(
            text("""
                INSERT INTO pending_transfer_stock
                    (transfer_type, transfer_out_id, transfer_out_challan_no,
                     box_id, transaction_no,
                     from_company, to_company, from_site, to_site,
                     from_storage_type, to_storage_type,
                     source_table, source_row_id, destination_table,
                     item_description, lot_no, weight_kg, no_of_cartons,
                     cold_storage_data,
                     gross_weight, net_weight, article,
                     status, dispatched_at, dispatched_by)
                VALUES
                    ('INTERUNIT', :transfer_out_id, :challan_no,
                     :box_id, :transaction_no,
                     :from_company, :to_company, :from_site, :to_site,
                     :from_storage_type, :to_storage_type,
                     :source_table, NULL, :destination_table,
                     :item_description, :lot_no, :weight_kg, :no_of_cartons,
                     CAST(:cold_storage_data AS JSONB),
                     :gross_weight, :net_weight, :article,
                     'In Transit', :dispatched_at, :dispatched_by)
                ON CONFLICT (box_id, transaction_no) DO NOTHING
            """),
            {
                "transfer_out_id": transfer_out_id,
                "challan_no": transfer_out.challan_no,
                "box_id": b.box_id,
                "transaction_no": b.transaction_no,
                "from_company": from_company,
                "to_company": to_company,
                "from_site": from_site,
                "to_site": to_site,
                "from_storage_type": from_storage_type,
                "to_storage_type": to_storage_type,
                "source_table": source_table_guess,
                "destination_table": destination_table_keep,
                "item_description": item_description,
                "lot_no": lot_no,
                "weight_kg": weight_kg,
                "no_of_cartons": no_of_cartons,
                "cold_storage_data": json.dumps(cold_data) if cold_data else None,
                "gross_weight": float(b.gross_weight) if b.gross_weight is not None else None,
                "net_weight": float(b.net_weight) if b.net_weight is not None else None,
                "article": b.article,
                "dispatched_at": now,
                "dispatched_by": transfer_out.created_by or "system",
            },
        )
        restored += 1
        logger.info("UNPICK_PENDING: box_id=%s tno=%s removed from %s → back to pending",
                    b.box_id, b.transaction_no, dest_table)

    logger.info("UNPICK_PENDING: restored %d boxes to pending for transfer_in_id=%s", restored, transfer_in_id)
    return restored


# ----------------------------------------------------------------------------
#  restore_to_source — called from delete_transfer
# ----------------------------------------------------------------------------
def restore_to_source(transfer_out_id: int, db: Session) -> int:
    """Restore every pending row (any status) tied to this transfer_out back to
    its source table, then delete the pending row. Returns count restored."""
    pending_rows = db.execute(
        text("""
            SELECT * FROM pending_transfer_stock
            WHERE transfer_out_id = :tid
        """),
        {"tid": transfer_out_id},
    ).fetchall()

    restored = 0
    for p in pending_rows:
        src = p.source_table
        if not _table_exists(db, src):
            logger.warning("RESTORE_SOURCE: source table %s missing, skip box_id=%s", src, p.box_id)
            db.execute(text("DELETE FROM pending_transfer_stock WHERE id = :id"), {"id": p.id})
            continue

        cold_json = p.cold_storage_data or {}

        if src.endswith("_cold_stocks"):
            db.execute(
                text(f"""
                    INSERT INTO {src}
                        (inward_dt, unit, inward_no, item_description, item_mark,
                         vakkal, lot_no, no_of_cartons, weight_kg,
                         total_inventory_kgs, group_name, item_subgroup, storage_location,
                         exporter, last_purchase_rate, value,
                         box_id, transaction_no, spl_remarks)
                    VALUES
                        (:inward_dt, :unit, :inward_no, :item_description, :item_mark,
                         :vakkal, :lot_no, :no_of_cartons, :weight_kg,
                         :total_inventory_kgs, :group_name, :item_subgroup, :storage_location,
                         :exporter, :last_purchase_rate, :value,
                         :box_id, :transaction_no, :spl_remarks)
                """),
                {
                    "inward_dt": cold_json.get("inward_dt"),
                    "unit": cold_json.get("unit") or p.from_site,
                    "inward_no": cold_json.get("inward_no"),
                    "item_description": p.item_description,
                    "item_mark": cold_json.get("item_mark"),
                    "vakkal": cold_json.get("vakkal"),
                    "lot_no": p.lot_no,
                    "no_of_cartons": p.no_of_cartons or 1,
                    "weight_kg": p.weight_kg,
                    "total_inventory_kgs": cold_json.get("total_inventory_kgs") or float(p.weight_kg or 0),
                    "group_name": cold_json.get("group_name"),
                    "item_subgroup": cold_json.get("item_subgroup"),
                    "storage_location": cold_json.get("storage_location") or p.from_site,
                    "exporter": cold_json.get("exporter"),
                    "last_purchase_rate": cold_json.get("last_purchase_rate"),
                    "value": cold_json.get("value"),
                    "box_id": p.box_id,
                    "transaction_no": p.transaction_no,
                    "spl_remarks": cold_json.get("spl_remarks"),
                },
            )
        else:
            db.execute(
                text(f"""
                    INSERT INTO {src}
                        (box_id, transaction_no, article_description, lot_number,
                         net_weight, gross_weight)
                    VALUES
                        (:box_id, :transaction_no, :article, :lot_no,
                         :net_weight, :gross_weight)
                """),
                {
                    "box_id": p.box_id,
                    "transaction_no": p.transaction_no,
                    "article": p.article or p.item_description,
                    "lot_no": p.lot_no,
                    "net_weight": p.net_weight or p.weight_kg,
                    "gross_weight": p.gross_weight,
                },
            )

        db.execute(text("DELETE FROM pending_transfer_stock WHERE id = :id"), {"id": p.id})
        restored += 1
        logger.info("RESTORE_SOURCE: box_id=%s tno=%s → %s (transfer_out_id=%s)",
                    p.box_id, p.transaction_no, src, transfer_out_id)

    logger.info("RESTORE_SOURCE: restored %d rows for transfer_out_id=%s", restored, transfer_out_id)
    return restored


# ----------------------------------------------------------------------------
#  Read APIs for frontend
# ----------------------------------------------------------------------------
def list_pending_transfers(
    db: Session,
    from_site: Optional[str] = None,
    to_site: Optional[str] = None,
    company: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    search: Optional[str] = None,
) -> dict:
    """Group rows in pending_transfer_stock by transfer_out_id for the
    Pending Transfer Status modal. Returns one row per dispatch."""
    if not _table_exists(db, "pending_transfer_stock"):
        return {"records": [], "total": 0, "filter_options": {"from_sites": [], "to_sites": []}}

    clauses = ["pts.status = 'In Transit'"]
    params: dict = {}

    if from_site:
        clauses.append("pts.from_site = :from_site")
        params["from_site"] = from_site
    if to_site:
        clauses.append("pts.to_site = :to_site")
        params["to_site"] = to_site
    if company:
        clauses.append("(pts.from_company = :company OR pts.to_company = :company)")
        params["company"] = company.lower()
    if from_date:
        clauses.append("pts.dispatched_at::date >= :from_date::date")
        params["from_date"] = from_date
    if to_date:
        clauses.append("pts.dispatched_at::date <= :to_date::date")
        params["to_date"] = to_date
    if search:
        clauses.append("(pts.transfer_out_challan_no ILIKE :s OR pts.item_description ILIKE :s OR pts.lot_no ILIKE :s)")
        params["s"] = f"%{search}%"

    where = " AND ".join(clauses)

    rows = db.execute(
        text(f"""
            SELECT
                pts.transfer_out_id,
                pts.transfer_out_challan_no,
                MIN(pts.dispatched_at)                AS dispatched_at,
                pts.from_site,
                pts.to_site,
                pts.from_company,
                pts.to_company,
                pts.from_storage_type,
                pts.to_storage_type,
                COUNT(*)                              AS total_boxes,
                COALESCE(SUM(pts.no_of_cartons), 0)   AS total_cartons,
                COALESCE(SUM(pts.weight_kg), 0)       AS total_kg,
                MIN(pts.dispatched_by)                AS dispatched_by,
                MIN(pts.status)                       AS status
            FROM pending_transfer_stock pts
            WHERE {where}
            GROUP BY pts.transfer_out_id, pts.transfer_out_challan_no,
                     pts.from_site, pts.to_site,
                     pts.from_company, pts.to_company,
                     pts.from_storage_type, pts.to_storage_type
            ORDER BY MIN(pts.dispatched_at) DESC
        """),
        params,
    ).fetchall()

    records = [
        {
            "transfer_out_id": r.transfer_out_id,
            "transfer_out_challan_no": r.transfer_out_challan_no,
            "dispatched_at": r.dispatched_at.isoformat() if r.dispatched_at else None,
            "from_site": r.from_site,
            "to_site": r.to_site,
            "from_company": r.from_company,
            "to_company": r.to_company,
            "from_storage_type": r.from_storage_type,
            "to_storage_type": r.to_storage_type,
            "total_boxes": int(r.total_boxes or 0),
            "total_cartons": float(r.total_cartons or 0),
            "total_kg": float(r.total_kg or 0),
            "dispatched_by": r.dispatched_by or "",
            "status": r.status or "In Transit",
        }
        for r in rows
    ]

    # Filter option chips — union of (a) distinct sites currently in pending,
    # and (b) all active warehouse_sites — so chips are always visible even when
    # pending is empty. Each chip carries a count of how many In-Transit rows match.
    pending_site_rows = db.execute(
        text("""
            SELECT from_site, to_site, COUNT(*) AS n
            FROM pending_transfer_stock
            WHERE status = 'In Transit'
            GROUP BY from_site, to_site
        """),
    ).fetchall()
    from_counts: dict = {}
    to_counts: dict = {}
    for r in pending_site_rows:
        if r.from_site:
            from_counts[r.from_site] = from_counts.get(r.from_site, 0) + int(r.n or 0)
        if r.to_site:
            to_counts[r.to_site] = to_counts.get(r.to_site, 0) + int(r.n or 0)

    all_sites: list = []
    if _table_exists(db, "warehouse_sites"):
        try:
            ws_rows = db.execute(
                text("SELECT site_name FROM warehouse_sites WHERE COALESCE(is_active, true) = true ORDER BY site_name")
            ).fetchall()
            all_sites = [r.site_name for r in ws_rows if r.site_name]
        except Exception:
            all_sites = []

    from_chips = sorted({*from_counts.keys(), *all_sites})
    to_chips = sorted({*to_counts.keys(), *all_sites})

    return {
        "records": records,
        "total": len(records),
        "filter_options": {
            "from_sites": from_chips,
            "to_sites": to_chips,
            "from_site_counts": from_counts,
            "to_site_counts": to_counts,
        },
    }


def pending_by_lot(
    db: Session,
    lot_no: Optional[str] = None,
    item_description: Optional[str] = None,
    from_site: Optional[str] = None,
    from_company: Optional[str] = None,
) -> dict:
    """Sum pending cartons and weight for a given lot+item (used by inventory
    UI to subtract from displayed available quantity). Also returns the list
    of pending transactions joined with the transfer header for richer context."""
    if not _table_exists(db, "pending_transfer_stock"):
        return {"pending_cartons": 0, "pending_kg": 0, "transfers": [], "boxes": []}

    clauses = ["pts.status = 'In Transit'"]
    params: dict = {}

    if lot_no:
        clauses.append("pts.lot_no = :lot_no")
        params["lot_no"] = lot_no
    if item_description:
        clauses.append("pts.item_description = :item_description")
        params["item_description"] = item_description
    if from_site:
        clauses.append("pts.from_site = :from_site")
        params["from_site"] = from_site
    if from_company:
        clauses.append("pts.from_company = :from_company")
        params["from_company"] = from_company.lower()

    where = " AND ".join(clauses)

    total = db.execute(
        text(f"""
            SELECT
                COALESCE(SUM(pts.no_of_cartons), 0)  AS pending_cartons,
                COALESCE(SUM(pts.weight_kg), 0)      AS pending_kg,
                COUNT(*)                              AS box_count
            FROM pending_transfer_stock pts
            WHERE {where}
        """),
        params,
    ).fetchone()

    # Aggregate per transfer — one row per challan, summed over all boxes.
    # JOIN header so we can surface vehicle/driver/approved_by/reason/remark/status.
    header_join_ok = _table_exists(db, "interunit_transfers_header")
    join_clause = (
        "LEFT JOIN interunit_transfers_header h ON h.id = pts.transfer_out_id"
        if header_join_ok else ""
    )
    header_cols = (
        ",\n                   MIN(h.vehicle_no)    AS vehicle_no,"
        "\n                   MIN(h.driver_name)   AS driver_name,"
        "\n                   MIN(h.approved_by)   AS approved_by,"
        "\n                   MIN(h.remark)        AS remark,"
        "\n                   MIN(h.reason_code)   AS reason_code,"
        "\n                   MIN(h.status)        AS transfer_status,"
        "\n                   BOOL_OR(COALESCE(h.has_variance, false)) AS has_variance"
    ) if header_join_ok else (
        ",\n                   NULL::text AS vehicle_no,"
        "\n                   NULL::text AS driver_name,"
        "\n                   NULL::text AS approved_by,"
        "\n                   NULL::text AS remark,"
        "\n                   NULL::text AS reason_code,"
        "\n                   NULL::text AS transfer_status,"
        "\n                   false      AS has_variance"
    )

    transfer_rows = db.execute(
        text(f"""
            SELECT pts.transfer_out_id,
                   pts.transfer_out_challan_no,
                   MIN(pts.dispatched_at)            AS dispatched_at,
                   pts.from_site,
                   pts.to_site,
                   pts.from_storage_type,
                   pts.to_storage_type,
                   COUNT(*)                           AS box_count,
                   COALESCE(SUM(pts.no_of_cartons),0) AS cartons,
                   COALESCE(SUM(pts.weight_kg), 0)    AS weight_kg,
                   MIN(pts.dispatched_by)            AS dispatched_by
                   {header_cols}
            FROM pending_transfer_stock pts
            {join_clause}
            WHERE {where}
            GROUP BY pts.transfer_out_id, pts.transfer_out_challan_no,
                     pts.from_site, pts.to_site,
                     pts.from_storage_type, pts.to_storage_type
            ORDER BY MIN(pts.dispatched_at) DESC
        """),
        params,
    ).fetchall()

    return {
        "pending_cartons": float(total.pending_cartons or 0),
        "pending_kg": float(total.pending_kg or 0),
        "box_count": int(total.box_count or 0),
        "transfers": [
            {
                "transfer_out_id": r.transfer_out_id,
                "challan_no": r.transfer_out_challan_no,
                "dispatched_at": r.dispatched_at.isoformat() if r.dispatched_at else None,
                "from_site": r.from_site,
                "to_site": r.to_site,
                "from_storage_type": r.from_storage_type,
                "to_storage_type": r.to_storage_type,
                "box_count": int(r.box_count or 0),
                "cartons": float(r.cartons or 0),
                "weight_kg": float(r.weight_kg or 0),
                "dispatched_by": r.dispatched_by or "",
                "vehicle_no": getattr(r, "vehicle_no", None) or "",
                "driver_name": getattr(r, "driver_name", None) or "",
                "approved_by": getattr(r, "approved_by", None) or "",
                "remark": getattr(r, "remark", None) or "",
                "reason_code": getattr(r, "reason_code", None) or "",
                "transfer_status": getattr(r, "transfer_status", None) or "",
                "has_variance": bool(getattr(r, "has_variance", False)),
            }
            for r in transfer_rows
        ],
        # legacy alias retained briefly for any callers expecting `boxes`
        "boxes": [],
    }


# ----------------------------------------------------------------------------
#  backfill_pending_from_existing_transfers — one-time migration
#
#  Parks every box from every in-transit Transfer Out (status Dispatch/Partial,
#  no 'Received' Transfer In) into pending_transfer_stock, deducting source
#  stock the way create_transfer would have if pending_transfer_stock had
#  existed at dispatch time. Safe to re-run — already-parked transfers are
#  skipped via the unique (box_id, transaction_no) constraint.
# ----------------------------------------------------------------------------
def backfill_pending_from_existing_transfers(db: Session) -> dict:
    if not _table_exists(db, "pending_transfer_stock"):
        return {"error": "pending_transfer_stock table missing"}

    candidates = db.execute(
        text("""
            SELECT h.id, h.challan_no, h.from_site, h.to_site,
                   h.status, h.created_by, h.created_ts
            FROM interunit_transfers_header h
            WHERE LOWER(COALESCE(h.status, '')) IN ('dispatch', 'partial', 'completed', 'in transit')
              AND NOT EXISTS (
                SELECT 1 FROM interunit_transfer_in_header ti
                WHERE ti.transfer_out_id = h.id
                  AND LOWER(COALESCE(ti.status, '')) = 'received'
              )
            ORDER BY h.created_ts ASC
        """)
    ).fetchall()

    summary = {
        "transfers_scanned": len(candidates),
        "transfers_with_existing_pending": 0,
        "boxes_parked_from_cold": 0,
        "boxes_parked_from_warehouse": 0,
        "boxes_parked_without_source": 0,
        "boxes_skipped_already_parked": 0,
        "boxes_with_missing_id": 0,
    }

    for t in candidates:
        # Skip transfers that already have at least one pending row (avoid double-park)
        existing = db.execute(
            text("SELECT COUNT(*) FROM pending_transfer_stock WHERE transfer_out_id = :tid"),
            {"tid": t.id},
        ).scalar()
        if existing and existing > 0:
            summary["transfers_with_existing_pending"] += 1
            continue

        boxes = db.execute(
            text("""
                SELECT box_id, transaction_no, article, lot_number, batch_number,
                       net_weight, gross_weight, box_number
                FROM interunit_transfer_boxes
                WHERE header_id = :tid
            """),
            {"tid": t.id},
        ).fetchall()

        from_storage_type = "cold" if _is_cold_site(t.from_site) else "warehouse"
        to_storage_type = "cold" if _is_cold_site(t.to_site) else "warehouse"
        dispatched_at = t.created_ts or datetime.now()
        dispatched_by = t.created_by or "backfill"

        for b in boxes:
            box_id = (b.box_id or "").strip()
            tno = (b.transaction_no or "").strip()
            if not box_id or not tno or tno == "DIRECT":
                summary["boxes_with_missing_id"] += 1
                continue

            # Already in pending from a parallel run? skip.
            dup = db.execute(
                text("SELECT 1 FROM pending_transfer_stock WHERE box_id = :bid AND transaction_no = :tno LIMIT 1"),
                {"bid": box_id, "tno": tno},
            ).fetchone()
            if dup:
                summary["boxes_skipped_already_parked"] += 1
                continue

            # Find source row
            source_table = None
            source_row = None
            cold_data = None
            warehouse_data = {}

            if from_storage_type == "cold":
                source_table, source_row = _find_in_cold_stocks(db, box_id, tno)
                if source_row is not None:
                    cold_data = _cold_row_to_json(source_row)

            if source_row is None:
                # Try warehouse tables (whether or not from_storage_type said warehouse)
                wh_table, wh_row = _find_in_bulk_entry(db, box_id, tno)
                if wh_row is not None:
                    source_table = wh_table
                    source_row = wh_row
                    warehouse_data = {
                        "gross_weight": float(getattr(wh_row, "gross_weight", 0) or 0),
                        "net_weight": float(getattr(wh_row, "net_weight", 0) or b.net_weight or 0),
                        "article": getattr(wh_row, "article_description", None),
                    }

            # Resolve descriptor fields
            if cold_data is not None and source_row is not None:
                item_description = getattr(source_row, "item_description", None) or b.article or ""
                lot_no = getattr(source_row, "lot_no", None) or b.lot_number or None
                weight_kg = float(getattr(source_row, "weight_kg", 0) or b.net_weight or 0)
                no_of_cartons = int(getattr(source_row, "no_of_cartons", 1) or 1)
            elif warehouse_data:
                item_description = warehouse_data.get("article") or b.article or ""
                lot_no = b.lot_number or None
                weight_kg = float(warehouse_data.get("net_weight") or b.net_weight or 0)
                no_of_cartons = 1
            else:
                # No source row anywhere — park with whatever transfer_box has.
                # Pick a sensible default source_table from site naming (Rishi → cdpl, Savla → cfpl).
                item_description = b.article or ""
                lot_no = b.lot_number or None
                weight_kg = float(b.net_weight or 0)
                no_of_cartons = 1
                site_lower = (t.from_site or "").strip().lower()
                guessed_company = "cdpl" if ("rishi" in site_lower or "cdpl" in site_lower) else "cfpl"
                source_table = (
                    f"{guessed_company}_cold_stocks"
                    if from_storage_type == "cold"
                    else f"{guessed_company}_bulk_entry_boxes"
                )

            from_company = "cfpl" if (source_table or "").startswith("cfpl") else "cdpl"
            to_company = from_company
            destination_table = _destination_table(to_storage_type, to_company)

            db.execute(
                text("""
                    INSERT INTO pending_transfer_stock
                        (transfer_type, transfer_out_id, transfer_out_challan_no,
                         box_id, transaction_no,
                         from_company, to_company, from_site, to_site,
                         from_storage_type, to_storage_type,
                         source_table, source_row_id, destination_table,
                         item_description, lot_no, weight_kg, no_of_cartons,
                         cold_storage_data,
                         gross_weight, net_weight, article,
                         status, dispatched_at, dispatched_by)
                    VALUES
                        ('INTERUNIT', :transfer_out_id, :challan_no,
                         :box_id, :transaction_no,
                         :from_company, :to_company, :from_site, :to_site,
                         :from_storage_type, :to_storage_type,
                         :source_table, :source_row_id, :destination_table,
                         :item_description, :lot_no, :weight_kg, :no_of_cartons,
                         CAST(:cold_storage_data AS JSONB),
                         :gross_weight, :net_weight, :article,
                         'In Transit', :dispatched_at, :dispatched_by)
                    ON CONFLICT (box_id, transaction_no) DO NOTHING
                """),
                {
                    "transfer_out_id": t.id,
                    "challan_no": t.challan_no,
                    "box_id": box_id,
                    "transaction_no": tno,
                    "from_company": from_company,
                    "to_company": to_company,
                    "from_site": t.from_site,
                    "to_site": t.to_site,
                    "from_storage_type": from_storage_type,
                    "to_storage_type": to_storage_type,
                    "source_table": source_table,
                    "source_row_id": getattr(source_row, "id", None) if source_row is not None else None,
                    "destination_table": destination_table,
                    "item_description": item_description,
                    "lot_no": lot_no,
                    "weight_kg": weight_kg,
                    "no_of_cartons": no_of_cartons,
                    "cold_storage_data": json.dumps(cold_data) if cold_data else None,
                    "gross_weight": warehouse_data.get("gross_weight") if warehouse_data else (float(b.gross_weight) if b.gross_weight is not None else None),
                    "net_weight": warehouse_data.get("net_weight") if warehouse_data else (float(b.net_weight) if b.net_weight is not None else None),
                    "article": warehouse_data.get("article") or b.article,
                    "dispatched_at": dispatched_at,
                    "dispatched_by": dispatched_by,
                },
            )

            # Delete source row (only if it actually exists — keeps idempotency)
            if source_row is not None and getattr(source_row, "id", None) is not None and source_table:
                db.execute(
                    text(f"DELETE FROM {source_table} WHERE id = :rid"),
                    {"rid": source_row.id},
                )

            if cold_data is not None:
                summary["boxes_parked_from_cold"] += 1
            elif warehouse_data:
                summary["boxes_parked_from_warehouse"] += 1
            else:
                summary["boxes_parked_without_source"] += 1

    db.commit()
    logger.info("BACKFILL_PENDING: %s", summary)
    return summary
