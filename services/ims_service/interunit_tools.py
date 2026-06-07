import json
from datetime import datetime
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger
from services.ims_service.interunit_models import (
    RequestCreate, RequestUpdate, TransferCreate, TransferInCreate,
    PendingTransferInCreate, PendingBoxAcknowledge, FinalizeTransferIn,
    CategorialSearchItem, CategorialSearchResponse,
    CategorialDropdownOptions, CategorialDropdownMeta, CategorialDropdownResponse,
    BoxCreate, TransferInEdit,
)
from services.ims_service.pending_stock_tools import (
    park_in_pending, park_lines_in_pending, pick_from_pending, unpick_to_pending,
    restore_to_source, reconcile_transfer_to_order, _is_cold_site, _table_exists,
    count_remaining_in_transit,
)

logger = get_logger("ims.interunit")


# Cold destinations that must route through /interunit/cold-transfer-in/* instead
# of the legacy /transfer-in endpoints (2026-06-06). Umbrella label "Cold Storage"
# excluded — IN-side receiving_warehouse is always a concrete sub-warehouse.
_COLD_DEST_LOWER = {"savla d-39", "savla d-514", "rishi", "supreme"}


def _is_cold_destination_name(name: Optional[str]) -> bool:
    return bool(name) and name.strip().lower() in _COLD_DEST_LOWER


# -- Cold sub-warehouse mapping --
#
# Cold-source transfers always store from_site='Cold Storage' on the header; the
# actual sub-cold (D-39 / D-514 / Rishi / Supreme) is only knowable from the
# source cold_stocks row (or its JSONB snapshot in pending_transfer_stock). We
# persist a canonical sub-cold value on interunit_transfers_header.from_cold_unit
# at create-transfer time so the Transfer Out Records filter can drill in.
#
# The canonical names are the ones the frontend chips display:
#   Savla D-39, Savla D-514, Rishi, Supreme Cold.

_COLD_UNIT_ALIASES: dict[str, set[str]] = {
    "Savla D-39":  {"d-39", "d39", "savla d-39", "savla d39", "savla-d-39"},
    "Savla D-514": {"d-514", "d514", "savla d-514", "savla d514", "savla-d-514"},
    "Rishi":       {"rishi", "rishi cold"},
    "Supreme Cold":{"supreme", "supreme cold"},
}

# Reverse lookup: alias-key (lowercased, stripped) → canonical
_COLD_UNIT_BY_ALIAS: dict[str, str] = {
    a: canon for canon, aliases in _COLD_UNIT_ALIASES.items() for a in aliases
}


def _normalize_cold_unit(raw: Optional[str]) -> Optional[str]:
    """Map a free-form cold-unit value to its canonical sub-cold name, or None."""
    if not raw:
        return None
    return _COLD_UNIT_BY_ALIAS.get(raw.strip().lower())


_interunit_schema_ensured = False

def _ensure_interunit_schema(db: Session) -> None:
    """Lazy idempotent ALTERs for interunit-side schema add-ons."""
    global _interunit_schema_ensured
    if _interunit_schema_ensured:
        return
    try:
        db.execute(text(
            "ALTER TABLE interunit_transfers_header "
            "ADD COLUMN IF NOT EXISTS from_cold_unit VARCHAR(50)"
        ))
        db.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_interunit_header_from_cold_unit "
            "ON interunit_transfers_header(from_cold_unit) WHERE from_cold_unit IS NOT NULL"
        ))
        db.execute(text(
            "ALTER TABLE interunit_transfer_in_header "
            "ADD COLUMN IF NOT EXISTS inward_transaction_no VARCHAR(50)"
        ))
        db.execute(text(
            "ALTER TABLE interunit_transfer_in_boxes "
            "ADD COLUMN IF NOT EXISTS inward_box_id VARCHAR(50)"
        ))
        db.execute(text(
            "ALTER TABLE interunit_transfer_in_boxes "
            "ADD COLUMN IF NOT EXISTS original_box_id VARCHAR(100)"
        ))
        db.execute(text(
            "ALTER TABLE interunit_transfer_in_boxes "
            "ADD COLUMN IF NOT EXISTS reconciled BOOLEAN DEFAULT FALSE"
        ))
        db.execute(text(
            "ALTER TABLE interunit_transfer_in_boxes "
            "ADD COLUMN IF NOT EXISTS reconciliation_id VARCHAR(100)"
        ))
        db.execute(text(
            "ALTER TABLE interunit_transfer_in_boxes "
            "ADD COLUMN IF NOT EXISTS scan_source VARCHAR(50)"
        ))
        db.commit()
    except Exception as e:
        logger.warning("Interunit schema ensure failed: %s", e)
    _interunit_schema_ensured = True


# -- Helpers --


def _generate_request_no() -> str:
    return f"REQ{datetime.now().strftime('%Y%m%d%H%M')}"


def _convert_date(date_str: str):
    try:
        return datetime.strptime(date_str, "%d-%m-%Y").date()
    except ValueError:
        raise HTTPException(400, "Invalid date format. Use DD-MM-YYYY")


def _map_line_row(row) -> dict:
    return {
        "id": row.id,
        "request_id": row.request_id,
        "material_type": row.rm_pm_fg_type or "",
        "item_category": row.item_category or "",
        "sub_category": row.sub_category or "",
        "item_description": row.item_desc_raw or "",
        "quantity": str(row.qty) if row.qty is not None else "0",
        "uom": row.uom or "",
        "pack_size": str(row.pack_size) if row.pack_size is not None else "0",
        "unit_pack_size": str(row.unit_pack_size) if row.unit_pack_size is not None else None,
        "net_weight": str(row.net_weight) if row.net_weight is not None else "0",
        "lot_number": row.lot_number or "",
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _map_header_row(row) -> dict:
    return {
        "id": row.id,
        "request_no": row.request_no or "",
        "request_date": row.request_date.strftime("%d-%m-%Y"),
        "from_warehouse": row.from_site or "",
        "to_warehouse": row.to_site or "",
        "reason_description": row.reason_code or "",
        "status": row.status or "Pending",
        "reject_reason": row.reject_reason,
        "created_by": row.created_by,
        "created_ts": row.created_ts,
        "rejected_ts": row.rejected_ts,
        "updated_at": row.updated_at,
    }


def _fetch_lines(db: Session, request_id: int) -> list:
    rows = db.execute(
        text("""
            SELECT id, request_id, rm_pm_fg_type, item_category, sub_category,
                   item_desc_raw, pack_size, qty, uom,
                   unit_pack_size, net_weight, total_weight, lot_number,
                   created_at, updated_at
            FROM interunit_transfer_request_lines
            WHERE request_id = :rid
            ORDER BY id
        """),
        {"rid": request_id},
    ).fetchall()
    return [_map_line_row(r) for r in rows]


# -- Warehouse dropdown --


def get_warehouse_sites(active_only: bool, db: Session) -> list:
    where = "WHERE is_active = true" if active_only else ""
    rows = db.execute(
        text(f"""
            SELECT id, site_code, site_name, is_active
            FROM warehouse_sites
            {where}
            ORDER BY site_code ASC
        """)
    ).fetchall()
    return [
        {"id": r.id, "site_code": r.site_code, "site_name": r.site_name, "is_active": r.is_active}
        for r in rows
    ]


# -- Create request --


def create_request(data: RequestCreate, created_by: str, db: Session) -> dict:
    request_date = _convert_date(data.form_data.request_date)

    request_no = (
        data.computed_fields.request_no
        if data.computed_fields and data.computed_fields.request_no
        else _generate_request_no()
    )

    header = db.execute(
        text("""
            INSERT INTO interunit_transfer_requests
                (request_no, request_date, from_site, to_site,
                 reason_code, remarks, status, created_by, created_ts)
            VALUES
                (:request_no, :request_date, :from_site, :to_site,
                 :reason_code, :remarks, 'Pending', :created_by, :created_ts)
            RETURNING id, request_no, request_date, from_site, to_site,
                      reason_code, remarks, status, reject_reason,
                      created_by, created_ts, rejected_ts, updated_at
        """),
        {
            "request_no": request_no,
            "request_date": request_date,
            "from_site": data.form_data.from_warehouse,
            "to_site": data.form_data.to_warehouse,
            "reason_code": data.form_data.reason_description or "General Transfer",
            "remarks": data.form_data.reason_description or "No remarks",
            "created_by": created_by,
            "created_ts": datetime.now(),
        },
    ).fetchone()

    request_id = header.id

    lines = []
    for line in data.article_data:
        pack_size_f = float(line.pack_size) if line.pack_size else 0.0
        qty_i = int(line.quantity) if line.quantity else 0
        unit_pack_size_val = float(line.unit_pack_size) if line.unit_pack_size else 0.0

        # Use frontend-provided net_weight if available; otherwise calculate
        frontend_net_weight = float(line.net_weight) if line.net_weight else 0.0

        # If user provided net_weight directly, prefer it
        if frontend_net_weight > 0:
            net_weight = round(frontend_net_weight, 3)
        elif line.material_type.upper() == "FG":
            net_weight = round(unit_pack_size_val * pack_size_f * qty_i, 3)
        else:
            net_weight = round(pack_size_f * qty_i, 3)

        # Use frontend-provided total_weight if available; otherwise fallback to net_weight
        frontend_total_weight = float(line.total_weight) if line.total_weight else 0.0
        total_weight = round(frontend_total_weight, 3) if frontend_total_weight > 0 else net_weight

        logger.info(
            "Request line: type=%s, pack_size=%s, qty=%s, unit_pack_size=%s, "
            "net_weight=%s kg, total_weight=%s kg",
            line.material_type, pack_size_f, qty_i, unit_pack_size_val,
            net_weight, total_weight,
        )

        row = db.execute(
            text("""
                INSERT INTO interunit_transfer_request_lines
                    (request_id, rm_pm_fg_type, item_category, sub_category,
                     item_desc_raw, pack_size, qty, uom,
                     unit_pack_size, net_weight, total_weight, lot_number)
                VALUES
                    (:request_id, :material_type, :item_category, :sub_category,
                     :item_desc_raw, :pack_size, :quantity, :uom,
                     :unit_pack_size, :net_weight, :total_weight, :lot_number)
                RETURNING id, request_id, rm_pm_fg_type, item_category, sub_category,
                          item_desc_raw, pack_size, qty, uom,
                          unit_pack_size, net_weight, total_weight, lot_number,
                          created_at, updated_at
            """),
            {
                "request_id": request_id,
                "material_type": line.material_type,
                "item_category": line.item_category,
                "sub_category": line.sub_category,
                "item_desc_raw": line.item_description,
                "pack_size": pack_size_f,
                "quantity": qty_i,
                "uom": line.uom or None,
                "unit_pack_size": unit_pack_size_val,
                "net_weight": net_weight,
                "total_weight": total_weight,
                "lot_number": line.lot_number,
            },
        ).fetchone()
        lines.append(_map_line_row(row))

    result = _map_header_row(header)
    result["lines"] = lines
    return result


# -- List requests --


def list_requests(
    page: int,
    per_page: int,
    status: Optional[str],
    from_warehouse: Optional[str],
    to_warehouse: Optional[str],
    created_by: Optional[str],
    db: Session,
) -> dict:
    clauses = ["r.status != 'Deleted'"]
    params: dict = {}

    if status:
        clauses.append("r.status = :status")
        params["status"] = status
    if from_warehouse:
        clauses.append("r.from_site = :from_warehouse")
        params["from_warehouse"] = from_warehouse.upper()
    if to_warehouse:
        clauses.append("r.to_site = :to_warehouse")
        params["to_warehouse"] = to_warehouse.upper()
    if created_by:
        clauses.append("r.created_by = :created_by")
        params["created_by"] = created_by

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    # Total count for pagination
    total = db.execute(
        text(f"SELECT COUNT(*) FROM interunit_transfer_requests r {where}"),
        params,
    ).scalar()

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    # Fetch paginated headers
    requests = db.execute(
        text(f"""
            SELECT id, request_no, request_date, from_site, to_site,
                   reason_code, remarks, status, reject_reason,
                   created_by, created_ts, rejected_ts, updated_at
            FROM interunit_transfer_requests r
            {where}
            ORDER BY r.created_ts DESC
            LIMIT :limit OFFSET :offset
        """),
        params,
    ).fetchall()

    if not requests:
        return {
            "records": [],
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": 0,
        }

    # Batch-fetch all lines for the page in a single query (eliminates N+1)
    request_ids = [req.id for req in requests]
    lines_rows = db.execute(
        text("""
            SELECT id, request_id, rm_pm_fg_type, item_category, sub_category,
                   item_desc_raw, pack_size, qty, uom,
                   unit_pack_size, net_weight, total_weight, lot_number,
                   created_at, updated_at
            FROM interunit_transfer_request_lines
            WHERE request_id = ANY(:rids)
            ORDER BY id
        """),
        {"rids": request_ids},
    ).fetchall()

    # Group lines by request_id
    lines_by_request: dict = {}
    for row in lines_rows:
        lines_by_request.setdefault(row.request_id, []).append(_map_line_row(row))

    results = []
    for req in requests:
        item = _map_header_row(req)
        item["lines"] = lines_by_request.get(req.id, [])
        results.append(item)

    return {
        "records": results,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if total else 0,
    }


# -- Get single request --


def get_request(request_id: int, db: Session) -> dict:
    row = db.execute(
        text("""
            SELECT id, request_no, request_date, from_site, to_site,
                   reason_code, remarks, status, reject_reason,
                   created_by, created_ts, rejected_ts, updated_at
            FROM interunit_transfer_requests
            WHERE id = :rid
        """),
        {"rid": request_id},
    ).fetchone()

    if not row:
        raise HTTPException(404, "Request not found")

    result = _map_header_row(row)
    result["lines"] = _fetch_lines(db, request_id)
    return result


# -- Update request (Accept / Reject) --


def update_request(request_id: int, data: RequestUpdate, db: Session) -> dict:
    existing = db.execute(
        text("SELECT id, status FROM interunit_transfer_requests WHERE id = :rid"),
        {"rid": request_id},
    ).fetchone()

    if not existing:
        raise HTTPException(404, "Request not found")

    fields = []
    params: dict = {"rid": request_id}

    if data.status:
        fields.append("status = :status")
        params["status"] = data.status
    if data.reject_reason:
        fields.append("reject_reason = :reject_reason")
        params["reject_reason"] = data.reject_reason
    if data.rejected_ts:
        fields.append("rejected_ts = :rejected_ts")
        params["rejected_ts"] = data.rejected_ts

    if not fields:
        raise HTTPException(400, "No fields to update")

    row = db.execute(
        text(f"""
            UPDATE interunit_transfer_requests
            SET {", ".join(fields)}
            WHERE id = :rid
            RETURNING id, request_no, request_date, from_site, to_site,
                      reason_code, status, reject_reason,
                      created_by, created_ts, rejected_ts, updated_at
        """),
        params,
    ).fetchone()

    return _map_header_row(row)


# -- Delete request --


def delete_request(request_id: int, db: Session) -> dict:
    existing = db.execute(
        text("SELECT id FROM interunit_transfer_requests WHERE id = :rid"),
        {"rid": request_id},
    ).fetchone()

    if not existing:
        raise HTTPException(404, "Request not found")

    db.execute(
        text("DELETE FROM interunit_transfer_request_lines WHERE request_id = :rid"),
        {"rid": request_id},
    )
    db.execute(
        text("DELETE FROM interunit_transfer_requests WHERE id = :rid"),
        {"rid": request_id},
    )

    return {"success": True, "message": "Request deleted successfully"}


# ----------------------------------------------
#  Phase B   Transfer helpers
# ----------------------------------------------


def _generate_challan_no() -> str:
    return f"TRANS{datetime.now().strftime('%Y%m%d%H%M%S')}"


def _map_transfer_line(row) -> dict:
    return {
        "id": row.id,
        "header_id": row.header_id,
        "material_type": row.rm_pm_fg_type or "",
        "item_category": row.item_category or "",
        "sub_category": row.sub_category or "",
        "item_description": row.item_desc_raw or "",
        "quantity": str(row.qty) if row.qty is not None else "0",
        "uom": row.uom or "",
        "pack_size": str(row.pack_size) if row.pack_size is not None else "0",
        "unit_pack_size": str(row.unit_pack_size) if row.unit_pack_size is not None else None,
        "net_weight": str(row.net_weight) if row.net_weight is not None else "0",
        "total_weight": str(row.total_weight) if row.total_weight is not None else "0",
        "batch_number": row.batch_number or "",
        "lot_number": row.lot_number or "",
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _map_transfer_header(row, request_no: Optional[str] = None) -> dict:
    return {
        "id": row.id,
        "challan_no": row.challan_no or "",
        "stock_trf_date": row.stock_trf_date.strftime("%d-%m-%Y") if row.stock_trf_date else "",
        "from_warehouse": row.from_site or "",
        "to_warehouse": row.to_site or "",
        "vehicle_no": row.vehicle_no or "",
        "driver_name": row.driver_name,
        "approved_by": row.approved_by,
        "remark": row.remark,
        "reason_code": row.reason_code,
        "status": row.status or "Pending",
        "request_id": row.request_id,
        "request_no": request_no or getattr(row, "request_no", None),
        "created_by": row.created_by,
        "created_ts": row.created_ts,
        "approved_ts": getattr(row, "approved_ts", None),
        "has_variance": getattr(row, "has_variance", False) or False,
        "from_cold_unit": getattr(row, "from_cold_unit", None) or None,
    }


def _map_box_row(row) -> dict:
    raw_box_id = row.box_id
    logger.info(
        "BOX_ID_DEBUG: row.id=%s, raw box_id=%r, type=%s, columns=%s",
        row.id, raw_box_id, type(raw_box_id).__name__,
        list(row._mapping.keys()) if hasattr(row, '_mapping') else 'N/A',
    )
    return {
        "id": row.id,
        "header_id": row.header_id,
        "transfer_line_id": row.transfer_line_id,
        "box_number": row.box_number,
        "box_id": raw_box_id if raw_box_id else "",
        "article": row.article or "",
        "lot_number": row.lot_number,
        "batch_number": row.batch_number,
        "transaction_no": row.transaction_no,
        "net_weight": str(row.net_weight) if row.net_weight is not None else "0",
        "gross_weight": str(row.gross_weight) if row.gross_weight is not None else "0",
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        "source_storage": getattr(row, "source_storage", None) or None,
        "source_unit": getattr(row, "source_unit", None) or None,
    }


def _fetch_transfer_lines(db: Session, header_id: int) -> list:
    rows = db.execute(
        text("""
            SELECT id, header_id, rm_pm_fg_type, item_category, sub_category,
                   item_desc_raw, pack_size, qty, uom,
                   unit_pack_size, net_weight, total_weight, batch_number, lot_number,
                   created_at, updated_at
            FROM interunit_transfers_lines
            WHERE header_id = :hid
            ORDER BY id
        """),
        {"hid": header_id},
    ).fetchall()
    return [_map_transfer_line(r) for r in rows]


def _fetch_boxes(db: Session, header_id: int) -> list:
    # source_unit is normalized to the canonical chip name (Savla D-39 / Savla
    # D-514 / Rishi / Supreme Cold) so per-box attribution is comparable to
    # the header's from_cold_unit and the frontend chip values. Raw
    # cold_storage_data->>'unit' values like 'D-39' / 'RISHI COLD' are mapped
    # in-SQL; unrecognized values pass through unchanged (so we never lie).
    rows = db.execute(
        text("""
            SELECT itb.id, itb.header_id, itb.transfer_line_id, itb.box_number,
                   itb.box_id, itb.article, itb.lot_number, itb.batch_number,
                   itb.transaction_no, itb.net_weight, itb.gross_weight,
                   itb.created_at, itb.updated_at,
                   pts.cold_storage_data->>'storage_location' AS source_storage,
                   CASE
                     WHEN LOWER(pts.cold_storage_data->>'unit') IN ('d-39','d39','savla d-39','savla d39','savla-d-39') THEN 'Savla D-39'
                     WHEN LOWER(pts.cold_storage_data->>'unit') IN ('d-514','d514','savla d-514','savla d514','savla-d-514') THEN 'Savla D-514'
                     WHEN LOWER(pts.cold_storage_data->>'unit') IN ('rishi','rishi cold') THEN 'Rishi'
                     WHEN LOWER(pts.cold_storage_data->>'unit') IN ('supreme','supreme cold') THEN 'Supreme Cold'
                     ELSE pts.cold_storage_data->>'unit'
                   END AS source_unit
            FROM interunit_transfer_boxes itb
            LEFT JOIN pending_transfer_stock pts
                ON pts.box_id = itb.box_id AND pts.status = 'In Transit'
            WHERE itb.header_id = :hid
            ORDER BY itb.box_number
        """),
        {"hid": header_id},
    ).fetchall()
    logger.info("FETCH_BOXES_DEBUG: header_id=%s, row_count=%d", header_id, len(rows))
    for r in rows:
        logger.info("FETCH_BOXES_DEBUG: row id=%s, box_id=%r, box_number=%s", r.id, r.box_id, r.box_number)
    result = [_map_box_row(r) for r in rows]
    logger.info("FETCH_BOXES_DEBUG: mapped result box_ids=%s", [b["box_id"] for b in result])
    return result


# -- Auto-derive warehouse boxes (Plan C: hybrid) --
#
# When a Transfer-Out is submitted from a warehouse source (W202 / A185 / A101 / A68 /
# F53 / etc.) and the frontend sent no `boxes` array, derive box rows server-side
# by joining boxes ⨝ transactions on transaction_no and filtering
# transactions.warehouse = from_site. Picks FIFO by created_at, id.
#
# Inventory location precedence (matches _find_in_bulk_entry in pending_stock_tools):
#   1. {company}_boxes_v2   ⨝ {company}_transactions_v2   (current target — no status col)
#   2. {company}_bulk_entry_boxes ⨝ {company}_bulk_entry_transactions  (legacy fallback,
#      filtered by status='available'; being decommissioned)
#
# Boxes already in pending_transfer_stock ('In Transit') are excluded. Raises
# HTTPException(400) on ambiguity (both companies match, multiple lots when none
# specified) or insufficient stock.

_AVAILABLE_BOX_SQL_V2 = """
    SELECT b.id, b.box_id, b.transaction_no, b.article_description, b.lot_number,
           b.net_weight, b.gross_weight, b.box_number, b.created_at
    FROM {tbl_boxes} b
    JOIN {tbl_txns}  t ON t.transaction_no = b.transaction_no
    WHERE UPPER(TRIM(t.warehouse)) = UPPER(TRIM(:wh))
      AND COALESCE(b.box_id, '') <> ''
      AND COALESCE(b.transaction_no, '') <> ''
      AND UPPER(TRIM(b.article_description)) = UPPER(TRIM(:art))
      {lot_clause}
      AND b.box_id NOT IN (
          SELECT box_id FROM pending_transfer_stock WHERE status = 'In Transit'
      )
    ORDER BY b.created_at ASC NULLS LAST, b.id ASC
"""

_AVAILABLE_BOX_SQL_LEGACY = """
    SELECT b.id, b.box_id, b.transaction_no, b.article_description, b.lot_number,
           b.net_weight, b.gross_weight, b.box_number, b.created_at
    FROM {tbl_boxes} b
    JOIN {tbl_txns}  t ON t.transaction_no = b.transaction_no
    WHERE UPPER(TRIM(t.warehouse)) = UPPER(TRIM(:wh))
      AND b.status = 'available'
      AND COALESCE(b.box_id, '') <> ''
      AND COALESCE(b.transaction_no, '') <> ''
      AND UPPER(TRIM(b.article_description)) = UPPER(TRIM(:art))
      {lot_clause}
      AND b.box_id NOT IN (
          SELECT box_id FROM pending_transfer_stock WHERE status = 'In Transit'
      )
    ORDER BY b.created_at ASC NULLS LAST, b.id ASC
"""


def _auto_derive_warehouse_boxes(db: Session, from_site: str, lines: list) -> list:
    """Server-side FIFO box picker for warehouse-source transfers when the
    frontend submitted no boxes. Returns a list of BoxCreate, sequentially
    numbered. Raises HTTPException(400) on ambiguity / insufficient stock so
    the operator is prompted to scan/pick manually."""
    derived: list = []
    used_box_ids: set = set()
    box_number_counter = 1

    for line in lines:
        article = (getattr(line, "item_desc_raw", "") or "").strip()
        lot     = (getattr(line, "lot_number", "") or "").strip()
        qty     = int(getattr(line, "qty", 0) or 0)
        if not article or qty <= 0:
            continue

        company_rows: dict[str, list] = {}
        lot_clause = "AND UPPER(TRIM(b.lot_number)) = UPPER(TRIM(:lot))" if lot else ""
        params = {"wh": from_site, "art": article}
        if lot:
            params["lot"] = lot
        for company in ("cfpl", "cdpl"):
            # v2 first (live inventory target); legacy bulk_entry_boxes fallback.
            attempts = (
                (_AVAILABLE_BOX_SQL_V2,     f"{company}_boxes_v2",          f"{company}_transactions_v2"),
                (_AVAILABLE_BOX_SQL_LEGACY, f"{company}_bulk_entry_boxes",  f"{company}_bulk_entry_transactions"),
            )
            rows: list = []
            for sql_tmpl, tbl_boxes, tbl_txns in attempts:
                sql = sql_tmpl.format(tbl_boxes=tbl_boxes, tbl_txns=tbl_txns, lot_clause=lot_clause)
                try:
                    fetched = db.execute(text(sql), params).fetchall()
                except Exception as e:
                    logger.warning("AUTO_BOX_DERIVE: query failed on %s: %s", tbl_boxes, e)
                    continue
                fetched = [r for r in fetched if r.box_id not in used_box_ids]
                if fetched:
                    rows = fetched
                    break
            if rows:
                company_rows[company] = rows

        if not company_rows:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"No available bulk-entry boxes found for '{article}'"
                    + (f" (lot='{lot}')" if lot else "")
                    + f" at warehouse '{from_site}'. Scan/pick boxes manually or check stock."
                ),
            )
        if len(company_rows) > 1:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Ambiguous source for '{article}' at '{from_site}': "
                    f"matching boxes exist in BOTH cfpl and cdpl bulk-entry tables. "
                    f"Open the box picker and select specific boxes."
                ),
            )

        company, rows = next(iter(company_rows.items()))

        if not lot:
            lots_in_first_qty = {(r.lot_number or "").strip() for r in rows[:qty]}
            if len(lots_in_first_qty) > 1:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Multiple lots ({sorted(lots_in_first_qty)}) available for '{article}' "
                        f"at '{from_site}'. Specify a lot_number on the line or scan/pick boxes."
                    ),
                )

        if len(rows) < qty:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Insufficient stock at '{from_site}' for '{article}'"
                    + (f" lot='{lot}'" if lot else "")
                    + f": need {qty}, only {len(rows)} available. Scan/pick boxes manually "
                      f"or adjust the line qty."
                ),
            )

        for r in rows[:qty]:
            used_box_ids.add(r.box_id)
            derived.append(BoxCreate(
                box_number=box_number_counter,
                box_id=r.box_id,
                article=r.article_description or article,
                lot_number=(r.lot_number or lot or ""),
                batch_number="",
                transaction_no=r.transaction_no,
                net_weight=str(r.net_weight) if r.net_weight is not None else "0",
                gross_weight=str(r.gross_weight) if r.gross_weight is not None else "0",
            ))
            box_number_counter += 1

    logger.info(
        "AUTO_BOX_DERIVE: derived %d boxes for warehouse-source transfer from %s "
        "(across %d line(s))", len(derived), from_site, len(lines),
    )
    return derived


# -- Create transfer --


def create_transfer(data: TransferCreate, created_by: str, db: Session) -> dict:
    # Cold-source dispatches must use POST /interunit/cold-transfer-out/create.
    # That endpoint owns cold_stocks deduction + cold metadata preservation.
    from services.ims_service.pending_stock_tools import _is_cold_site as _is_cold
    _from = getattr(data.header, "from_warehouse", None) or ""
    if _is_cold(_from):
        raise HTTPException(
            status_code=400,
            detail=(
                "Cold-source transfers must use POST /interunit/cold-transfer-out/create. "
                f"from_warehouse={_from!r}"
            ),
        )

    stock_trf_date = _convert_date(data.header.stock_trf_date)
    challan_no = data.header.challan_no or _generate_challan_no()

    # Insert header
    header = db.execute(
        text("""
            INSERT INTO interunit_transfers_header
                (challan_no, stock_trf_date, from_site, to_site,
                 vehicle_no, driver_name, approved_by, remark, reason_code,
                 status, request_id, created_by, created_ts)
            VALUES
                (:challan_no, :stock_trf_date, :from_site, :to_site,
                 :vehicle_no, :driver_name, :approved_by, :remark, :reason_code,
                 'Dispatch', :request_id, :created_by, :created_ts)
            RETURNING id, challan_no, stock_trf_date, from_site, to_site,
                      vehicle_no, driver_name, approved_by, remark, reason_code,
                      status, request_id, created_by, created_ts,
                      approved_ts, has_variance
        """),
        {
            "challan_no": challan_no,
            "stock_trf_date": stock_trf_date,
            "from_site": data.header.from_warehouse,
            "to_site": data.header.to_warehouse,
            "vehicle_no": data.header.vehicle_no,
            "driver_name": data.header.driver_name,
            "approved_by": data.header.approved_by,
            "remark": data.header.remark,
            "reason_code": data.header.reason_code,
            "request_id": data.request_id,
            "created_by": created_by,
            "created_ts": datetime.now(),
        },
    ).fetchone()

    header_id = header.id

    # Insert lines
    lines = []
    for idx, line in enumerate(data.lines):
        logger.info("CREATE_TRANSFER_LINE[%d]: net_weight=%r, total_weight=%r, pack_size=%r, unit_pack_size=%r, qty=%r, item=%r",
                     idx, line.net_weight, line.total_weight, line.pack_size, line.unit_pack_size, line.quantity, line.item_description)
        pack_size_f = float(line.pack_size) if line.pack_size else 0.0
        qty_i = int(line.quantity) if line.quantity else 1
        unit_pack_size_val = float(line.unit_pack_size) if line.unit_pack_size else 1.0

        # Use frontend-provided net_weight if available; otherwise calculate
        frontend_net_weight = float(line.net_weight) if line.net_weight else 0.0
        if frontend_net_weight > 0:
            net_weight = round(frontend_net_weight, 3)
        elif line.material_type.upper() == "FG":
            net_weight = round(unit_pack_size_val * pack_size_f * qty_i, 3)
        else:
            net_weight = round(pack_size_f * qty_i, 3)

        # Use frontend-provided total_weight if available; otherwise fallback to net_weight
        frontend_total_weight = float(line.total_weight) if line.total_weight else 0.0
        total_weight = round(frontend_total_weight, 3) if frontend_total_weight > 0 else net_weight

        row = db.execute(
            text("""
                INSERT INTO interunit_transfers_lines
                    (header_id, rm_pm_fg_type, item_category, sub_category,
                     item_desc_raw, pack_size, qty, uom,
                     unit_pack_size, net_weight, total_weight, batch_number, lot_number)
                VALUES
                    (:header_id, :material_type, :item_category, :sub_category,
                     :item_desc_raw, :pack_size, :quantity, :uom,
                     :unit_pack_size, :net_weight, :total_weight, :batch_number, :lot_number)
                RETURNING id, header_id, rm_pm_fg_type, item_category, sub_category,
                          item_desc_raw, pack_size, qty, uom,
                          unit_pack_size, net_weight, total_weight, batch_number, lot_number,
                          created_at, updated_at
            """),
            {
                "header_id": header_id,
                "material_type": line.material_type,
                "item_category": line.item_category,
                "sub_category": line.sub_category,
                "item_desc_raw": line.item_description,
                "pack_size": pack_size_f,
                "quantity": qty_i,
                "uom": line.uom or None,
                "unit_pack_size": float(line.unit_pack_size) if line.unit_pack_size else 0.0,
                "net_weight": net_weight,
                "total_weight": total_weight,
                "batch_number": line.batch_number or "",
                "lot_number": line.lot_number or "",
            },
        ).fetchone()
        lines.append(row)

    # NOTE: cold->warehouse has no real box scanning (the frontend "scanned boxes" are an
    # artifact), so ordered qty == shipped qty. The order (lot+qty) is authoritative and
    # reconcile_transfer_to_order corrects pending to match it (deduct the ordered lot,
    # restore wrong-lot/excess rows). No scan-based block here — there is nothing real to
    # block against.

    # Plan C: auto-derive boxes for warehouse-source transfers when the frontend
    # didn't scan any. Best-effort — if the stock is not box-tracked (no matching
    # boxes), ambiguous, or insufficient, we DO NOT block the save: the transfer
    # falls through to line-level pending below so every transfer is still tracked.
    if (not data.boxes) and lines and not _is_cold_site(data.header.from_warehouse):
        try:
            data.boxes = _auto_derive_warehouse_boxes(db, data.header.from_warehouse, lines)
        except HTTPException as e:
            logger.info(
                "TRANSFER: box auto-derive skipped (%s) — falling back to line-level pending",
                getattr(e, "detail", e),
            )
            data.boxes = []

    # Insert boxes (if provided)
    boxes = []
    if data.boxes:
        # Reject duplicate (box_id, transaction_no) pairs within the same transfer.
        # Cold-storage box_ids must be unique per physical box; duplicates here mean
        # the frontend fell back to a single cs_box_id for many boxes (see
        # TRANS202605131331 incident) — saving would silently lose inventory on receive
        # because the Transfer-In ON CONFLICT collapses duplicates.
        seen_keys: set = set()
        for box in data.boxes:
            bid = (box.box_id or "").strip()
            tno = (box.transaction_no or "").strip()
            if bid and tno and tno != "DIRECT":
                key = (bid, tno)
                if key in seen_keys:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"Duplicate box_id '{bid}' for transaction '{tno}' in this transfer. "
                            "Every physical box must carry a unique box_id."
                        ),
                    )
                seen_keys.add(key)


        # Build article-to-line-id lookup for correct box-to-line association
        line_id_by_article: dict = {}
        for l in lines:
            line_id_by_article[(l.item_desc_raw or "").strip().upper()] = l.id
        fallback_line_id = lines[0].id if lines else None

        for box in data.boxes:
            box_article_key = (box.article or "").strip().upper()
            matched_line_id = line_id_by_article.get(box_article_key, fallback_line_id)

            box_row = db.execute(
                text("""
                    INSERT INTO interunit_transfer_boxes
                        (header_id, transfer_line_id, box_number, box_id, article,
                         lot_number, batch_number, transaction_no,
                         net_weight, gross_weight)
                    VALUES
                        (:header_id, :transfer_line_id, :box_number, :box_id, :article,
                         :lot_number, :batch_number, :transaction_no,
                         :net_weight, :gross_weight)
                    RETURNING id, header_id, transfer_line_id, box_number, box_id,
                              article, lot_number, batch_number, transaction_no,
                              net_weight, gross_weight, created_at, updated_at
                """),
                {
                    "header_id": header_id,
                    "transfer_line_id": matched_line_id,
                    "box_number": box.box_number,
                    "box_id": box.box_id or "",
                    "article": box.article,
                    "lot_number": box.lot_number or "",
                    "batch_number": box.batch_number or "",
                    "transaction_no": box.transaction_no or "",
                    "net_weight": float(box.net_weight),
                    "gross_weight": float(box.gross_weight),
                },
            ).fetchone()
            boxes.append(box_row)

    # Line weights are already set correctly from frontend per-box values.
    # Each line represents one box entry   no need to sum box weights back into lines.

    # Park dispatched boxes into pending_transfer_stock (deducts source inventory)
    if boxes:
        park_in_pending(
            transfer_out_id=header_id,
            challan_no=header.challan_no,
            from_site=data.header.from_warehouse,
            to_site=data.header.to_warehouse,
            boxes=data.boxes or [],
            dispatched_by=getattr(header, "created_by", "system") or "system",
            db=db,
        )

        # For cold-source transfers, persist the canonical sub-cold(s) (D-39/D-514/
        # Rishi/Supreme Cold) on the header so the Transfer Out Records chip
        # filter can drill in. A single transfer can span multiple sub-units
        # (e.g. boxes from both D-39 and Rishi) — those land as a comma-separated
        # canonical list ("Rishi, Savla D-39"), which the chip filter matches via
        # ILIKE. Pulled from the JSONB snapshots park_in_pending just wrote into
        # pending_transfer_stock.
        if _is_cold_site(data.header.from_warehouse):
            _ensure_interunit_schema(db)
            raw_units = db.execute(
                text(
                    "SELECT DISTINCT cold_storage_data->>'unit' AS u "
                    "FROM pending_transfer_stock "
                    "WHERE transfer_out_id = :id AND cold_storage_data IS NOT NULL "
                    "  AND cold_storage_data->>'unit' IS NOT NULL"
                ),
                {"id": header_id},
            ).fetchall()
            canonical_set = {_normalize_cold_unit(r.u) for r in raw_units}
            canonical_set.discard(None)
            if canonical_set:
                joined = ", ".join(sorted(canonical_set))
                db.execute(
                    text("UPDATE interunit_transfers_header SET from_cold_unit = :u WHERE id = :id"),
                    {"u": joined, "id": header_id},
                )

    # MIXED scan + manual: park ALSO the lines that have no scanned/derived box, so
    # manually-filled entries are NEVER dropped. The old `elif lines` skipped line parking
    # whenever ANY box was scanned → manual stock vanished + transfer went Partial.
    # Coverage is counted per (article, lot); each article entry == 1 box in this form.
    _covered: dict = {}
    for _b in (data.boxes or []):
        _k = ((_b.article or "").strip().upper(), (_b.lot_number or "").strip())
        _covered[_k] = _covered.get(_k, 0) + 1
    uncovered_lines = []
    for _l in lines:
        _k = ((_l.item_desc_raw or "").strip().upper(), (_l.lot_number or "").strip())
        if _covered.get(_k, 0) > 0:
            _covered[_k] -= 1            # this line's box was scanned / auto-derived
        else:
            uncovered_lines.append(_l)   # manual / unscanned → park box-less so it isn't lost
    if uncovered_lines:
        park_lines_in_pending(
            transfer_out_id=header_id,
            challan_no=header.challan_no,
            from_site=data.header.from_warehouse,
            to_site=data.header.to_warehouse,
            lines=uncovered_lines,
            dispatched_by=getattr(header, "created_by", "system") or "system",
            db=db,
        )

    # Determine status. Everything is now parked (scanned boxes + box-less manual lines),
    # so a mixed / short-scan transfer is still fully dispatched — not Partial.
    if boxes:
        total_expected = sum(int(l.qty) for l in lines)
        actual_dispatched = len(boxes) + len(uncovered_lines)
        transfer_status = "Dispatch" if actual_dispatched >= total_expected else "Partial"

        db.execute(
            text("""
                UPDATE interunit_transfers_header
                SET status = :status
                WHERE id = :hid
            """),
            {"status": transfer_status, "hid": header_id},
        )

    # Update originating request status to 'Transferred'
    if data.request_id:
        db.execute(
            text("""
                UPDATE interunit_transfer_requests
                SET status = 'Transferred', updated_at = :now
                WHERE id = :rid
            """),
            {"now": datetime.now(), "rid": data.request_id},
        )

    # Reconcile pending_transfer_stock up to the ordered qty (fills any box shortfall
    # BY LOT from the main sheet) so every pending surface matches the order.
    reconcile_transfer_to_order(header_id, db, dry_run=False)

    # Re-fetch header for latest status
    header = db.execute(
        text("""
            SELECT id, challan_no, stock_trf_date, from_site, to_site,
                   vehicle_no, driver_name, approved_by, remark, reason_code,
                   status, request_id, created_by, created_ts,
                   approved_ts, has_variance
            FROM interunit_transfers_header
            WHERE id = :hid
        """),
        {"hid": header_id},
    ).fetchone()

    result = _map_transfer_header(header)
    result["lines"] = [_map_transfer_line(l) for l in lines]
    return result


# -- List transfers --


def list_transfers(
    page: int,
    per_page: int,
    status: Optional[str],
    from_site: Optional[str],
    to_site: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    challan_no: Optional[str],
    sort_by: str,
    sort_order: str,
    db: Session,
) -> dict:
    _ensure_interunit_schema(db)

    clauses = ["1=1"]
    params: dict = {}

    if status:
        clauses.append("h.status = :status")
        params["status"] = status
    if from_site:
        # Cold sub-warehouse chips (Savla D-39 / Savla D-514 / Rishi / Supreme
        # Cold) need to be translated: the header's from_site is always
        # 'Cold Storage' for these — the sub-cold(s) are on from_cold_unit.
        # Multi-unit transfers store a comma-separated list ("Rishi, Savla D-39")
        # so we match via ILIKE — the canonical names don't share substrings
        # so this stays unambiguous (D-39 won't match Savla D-514, etc.).
        cu_canon = _normalize_cold_unit(from_site)
        if cu_canon:
            clauses.append("h.from_site ILIKE 'cold%' AND h.from_cold_unit ILIKE :from_cold_unit")
            params["from_cold_unit"] = f"%{cu_canon}%"
        else:
            clauses.append("h.from_site = :from_site")
            params["from_site"] = from_site
    if to_site:
        clauses.append("h.to_site = :to_site")
        params["to_site"] = to_site
    if from_date:
        clauses.append("h.stock_trf_date >= :from_date")
        params["from_date"] = _convert_date(from_date)
    if to_date:
        clauses.append("h.stock_trf_date <= :to_date")
        params["to_date"] = _convert_date(to_date)
    if challan_no:
        clauses.append("h.challan_no = :challan_no")
        params["challan_no"] = challan_no

    where = " AND ".join(clauses)

    valid_sort = {"challan_no", "stock_trf_date", "from_site", "to_site", "status", "created_ts"}
    if sort_by not in valid_sort:
        sort_by = "created_ts"
    direction = "DESC" if sort_order.lower() == "desc" else "ASC"

    # Total count
    total = db.execute(
        text(f"SELECT COUNT(*) FROM interunit_transfers_header h WHERE {where}"),
        params,
    ).scalar()

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    rows = db.execute(
        text(f"""
            SELECT
                h.id, h.challan_no, h.stock_trf_date, h.from_site, h.to_site,
                h.vehicle_no, h.driver_name, h.remark, h.reason_code,
                h.status, h.request_id, h.created_by, h.created_ts,
                h.approved_by, h.approved_ts, h.has_variance,
                h.from_cold_unit,
                r.request_no,
                COALESCE(lc.items_count, 0) AS items_count,
                COALESCE(bc.boxes_count, 0) AS boxes_count,
                COALESCE(lc.total_qty, 0) AS total_qty,
                COALESCE(lt.lot_numbers_text, '') AS lot_numbers_text
            FROM interunit_transfers_header h
            LEFT JOIN interunit_transfer_requests r ON h.request_id = r.id
            LEFT JOIN (
                SELECT header_id,
                       COUNT(DISTINCT item_desc_raw) AS items_count,
                       COUNT(*) AS total_qty
                FROM interunit_transfers_lines
                GROUP BY header_id
            ) lc ON h.id = lc.header_id
            LEFT JOIN (
                SELECT header_id,
                       COUNT(DISTINCT COALESCE(box_id, id::text)) AS boxes_count
                FROM interunit_transfer_boxes
                GROUP BY header_id
            ) bc ON h.id = bc.header_id
            LEFT JOIN (
                SELECT header_id,
                       STRING_AGG(DISTINCT lot_number, ' ') AS lot_numbers_text
                FROM interunit_transfer_boxes
                WHERE lot_number IS NOT NULL AND lot_number <> ''
                GROUP BY header_id
            ) lt ON h.id = lt.header_id
            WHERE {where}
            ORDER BY h.{sort_by} {direction}
            LIMIT :limit OFFSET :offset
        """),
        params,
    ).fetchall()

    records = []
    for row in rows:
        item = _map_transfer_header(row)
        item["items_count"] = row.items_count or 0
        item["boxes_count"] = row.boxes_count or 0
        item["total_qty"] = row.total_qty or 0
        item["pending_items"] = max(0, int(row.total_qty or 0) - int(row.boxes_count or 0))
        item["lot_numbers_text"] = getattr(row, "lot_numbers_text", None) or ""
        records.append(item)

    return {
        "records": records,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if total else 0,
    }


# -- Bulk entry box lookup --


def get_bulk_entry_box(company: str, box_id: str, transaction_no: str, db: Session) -> dict:
    """Look up a single box by box_id and transaction_no from bulk entry boxes tables."""

    for prefix in ("cfpl", "cdpl"):
        table = f"{prefix}_bulk_entry_boxes"

        # Check if table exists first
        exists = db.execute(
            text("SELECT to_regclass(:tbl)"),
            {"tbl": f"public.{table}"},
        ).scalar()
        if not exists:
            continue

        box_res = db.execute(
            text(f"""
                SELECT transaction_no, box_id, article_description,
                       lot_number, net_weight, gross_weight, box_number
                FROM {table}
                WHERE box_id = :box_id AND transaction_no = :txno
                LIMIT 1
            """),
            {"box_id": box_id, "txno": transaction_no},
        ).fetchone()

        if box_res:
            return {
                "success": True,
                "box": {
                    "box_id": box_res.box_id,
                    "transaction_no": box_res.transaction_no,
                    "box_number": box_res.box_number if hasattr(box_res, "box_number") else 0,
                    "article_description": box_res.article_description,
                    "item_description": box_res.article_description,
                    "lot_number": box_res.lot_number,
                    "net_weight": float(box_res.net_weight) if box_res.net_weight else 0,
                    "gross_weight": float(box_res.gross_weight) if box_res.gross_weight else 0,
                    "material_type": "RM",
                    "item_category": "",
                    "sub_category": "",
                    "uom": "BAG",
                    "batch_number": "",
                    "sku_id": None,
                },
            }

    raise HTTPException(404, f"Box with box_id '{box_id}' and transaction_no '{transaction_no}' not found in bulk entry boxes")


# -- Get single transfer --


def get_transfer(transfer_id: int, db: Session) -> dict:
    row = db.execute(
        text("""
            SELECT h.id, h.challan_no, h.stock_trf_date, h.from_site, h.to_site,
                   h.vehicle_no, h.driver_name, h.approved_by, h.remark,
                   h.reason_code, h.status, h.request_id, h.created_by,
                   h.created_ts, h.approved_ts, h.has_variance,
                   h.from_cold_unit,
                   r.request_no
            FROM interunit_transfers_header h
            LEFT JOIN interunit_transfer_requests r ON h.request_id = r.id
            WHERE h.id = :tid
        """),
        {"tid": transfer_id},
    ).fetchone()

    if not row:
        raise HTTPException(404, "Transfer not found")

    result = _map_transfer_header(row)
    result["lines"] = _fetch_transfer_lines(db, transfer_id)
    result["boxes"] = _fetch_boxes(db, transfer_id)

    # Per-lot dominant sub-cold attribution: each lot's authoritative source is
    # the master cold_stocks table (where it was originally inwarded). We pick
    # the most-rows unit per lot across cfpl_cold_stocks + cdpl_cold_stocks +
    # pending_transfer_stock JSONB. The result is a single canonical value per
    # lot — clean and unambiguous, unlike the per-box JSONB which can carry
    # noise from prior transfers that shared the same box_id.
    try:
        lot_numbers = sorted({(x.get("lot_number") or "").strip() for x in (result["boxes"] + result["lines"])} - {""})
        lot_origin_unit: dict[str, str] = {}
        if lot_numbers:
            rows = db.execute(
                text("""
                    WITH lot_sources AS (
                        SELECT lot_no, unit AS raw_u FROM cfpl_cold_stocks WHERE lot_no = ANY(:lots)
                        UNION ALL
                        SELECT lot_no, unit AS raw_u FROM cdpl_cold_stocks WHERE lot_no = ANY(:lots)
                        UNION ALL
                        SELECT lot_no, cold_storage_data->>'unit' AS raw_u
                        FROM pending_transfer_stock
                        WHERE lot_no = ANY(:lots) AND cold_storage_data IS NOT NULL
                    ),
                    normalized AS (
                        SELECT lot_no,
                            CASE
                                WHEN LOWER(raw_u) IN ('d-39','d39','savla d-39','savla d39','savla-d-39') THEN 'Savla D-39'
                                WHEN LOWER(raw_u) IN ('d-514','d514','savla d-514','savla d514','savla-d-514') THEN 'Savla D-514'
                                WHEN LOWER(raw_u) IN ('rishi','rishi cold') THEN 'Rishi'
                                WHEN LOWER(raw_u) IN ('supreme','supreme cold') THEN 'Supreme Cold'
                                ELSE NULL
                            END AS unit
                        FROM lot_sources
                        WHERE raw_u IS NOT NULL
                    ),
                    counted AS (
                        SELECT lot_no, unit, COUNT(*) AS n,
                               ROW_NUMBER() OVER (PARTITION BY lot_no ORDER BY COUNT(*) DESC, unit) AS rk
                        FROM normalized
                        WHERE unit IS NOT NULL
                        GROUP BY lot_no, unit
                    )
                    SELECT lot_no, unit FROM counted WHERE rk = 1
                """),
                {"lots": lot_numbers},
            ).fetchall()
            for r in rows:
                lot_origin_unit[r.lot_no] = r.unit
        # Attach to each box AND line so the frontend renders a single per-lot
        # "From" chip consistently (boxes via groupBoxesByItem, lines via
        # groupLinesByItem) — the unified hover style.
        for x in (result["boxes"] + result["lines"]):
            lot = (x.get("lot_number") or "").strip()
            x["lot_origin_unit"] = lot_origin_unit.get(lot)
    except Exception as e:
        logger.warning("LOT_ORIGIN: per-lot dominant unit lookup failed (transfer_id=%s): %s", transfer_id, e)

    # Attach any GRN (Transfer-In) records so the pending-transfer hover card
    # can show whether a receipt has already been started or completed.
    try:
        grn_rows = db.execute(
            text("""
                SELECT tih.id, tih.grn_number, tih.status, tih.received_by, tih.received_at,
                       COUNT(tib.id) AS received_boxes
                FROM interunit_transfer_in_header tih
                LEFT JOIN interunit_transfer_in_boxes tib ON tib.header_id = tih.id
                WHERE tih.transfer_out_id = :tid
                GROUP BY tih.id, tih.grn_number, tih.status, tih.received_by, tih.received_at
                ORDER BY tih.created_at DESC
            """),
            {"tid": transfer_id},
        ).fetchall()
        result["grn_records"] = [
            {
                "id": g.id,
                "grn_number": g.grn_number or "",
                "status": g.status or "",
                "received_by": g.received_by or "",
                "received_at": g.received_at.isoformat() if g.received_at else None,
                "received_boxes": int(g.received_boxes or 0),
            }
            for g in grn_rows
        ]
    except Exception:
        result["grn_records"] = []

    logger.info(
        "GET_TRANSFER_DEBUG: transfer_id=%s, boxes_count=%d, box_ids_in_response=%s",
        transfer_id, len(result["boxes"]),
        [b.get("box_id") for b in result["boxes"]],
    )
    return result


# -- Update transfer --


def update_transfer(transfer_id: int, data: TransferCreate, db: Session) -> dict:
    """Update an existing transfer by replacing header, lines, and boxes."""
    # Cold-source edits must use POST /interunit/cold-transfer-out/{id}/edit.
    from services.ims_service.pending_stock_tools import _is_cold_site as _is_cold
    _from = getattr(data.header, "from_warehouse", None) or ""
    if _is_cold(_from):
        raise HTTPException(
            status_code=400,
            detail=(
                "Cold-source transfers must use POST /interunit/cold-transfer-out/{id}/edit. "
                f"from_warehouse={_from!r}"
            ),
        )

    existing = db.execute(
        text("SELECT id, challan_no, status, request_id FROM interunit_transfers_header WHERE id = :tid"),
        {"tid": transfer_id},
    ).fetchone()

    if not existing:
        raise HTTPException(404, "Transfer not found")

    # No status restriction   authorized users can edit transfers in any status

    # Roll back any existing pending_transfer_stock rows back to source before re-parking
    # (update_transfer replaces lines and boxes, so previous source deductions must be undone)
    restore_to_source(transfer_out_id=transfer_id, db=db)

    stock_trf_date = _convert_date(data.header.stock_trf_date)

    # Update header
    header = db.execute(
        text("""
            UPDATE interunit_transfers_header
            SET stock_trf_date = :stock_trf_date,
                from_site = :from_site,
                to_site = :to_site,
                vehicle_no = :vehicle_no,
                driver_name = :driver_name,
                approved_by = :approved_by,
                remark = :remark,
                reason_code = :reason_code,
                request_id = :request_id
            WHERE id = :tid
            RETURNING id, challan_no, stock_trf_date, from_site, to_site,
                      vehicle_no, driver_name, approved_by, remark, reason_code,
                      status, request_id, created_by, created_ts,
                      approved_ts, has_variance
        """),
        {
            "tid": transfer_id,
            "stock_trf_date": stock_trf_date,
            "from_site": data.header.from_warehouse,
            "to_site": data.header.to_warehouse,
            "vehicle_no": data.header.vehicle_no,
            "driver_name": data.header.driver_name,
            "approved_by": data.header.approved_by,
            "remark": data.header.remark,
            "reason_code": data.header.reason_code,
            "request_id": data.request_id,
        },
    ).fetchone()

    header_id = header.id

    # Delete existing lines and boxes, then re-insert
    db.execute(
        text("DELETE FROM interunit_transfer_boxes WHERE header_id = :hid"),
        {"hid": header_id},
    )
    db.execute(
        text("DELETE FROM interunit_transfers_lines WHERE header_id = :hid"),
        {"hid": header_id},
    )

    # Insert lines (same logic as create_transfer)
    lines = []
    for line in data.lines:
        pack_size_f = float(line.pack_size) if line.pack_size else 0.0
        qty_i = int(line.quantity) if line.quantity else 1
        unit_pack_size_val = float(line.unit_pack_size) if line.unit_pack_size else 0.0

        # Use frontend-provided net_weight if available; otherwise calculate
        frontend_net_weight = float(line.net_weight) if line.net_weight else 0.0
        if frontend_net_weight > 0:
            net_weight = round(frontend_net_weight, 3)
        elif line.material_type.upper() == "FG" and unit_pack_size_val:
            net_weight = round(unit_pack_size_val * pack_size_f * qty_i, 3)
        else:
            net_weight = round(pack_size_f * qty_i, 3)

        # Use frontend-provided total_weight if available; otherwise fallback to net_weight
        frontend_total_weight = float(line.total_weight) if line.total_weight else 0.0
        total_weight = round(frontend_total_weight, 3) if frontend_total_weight > 0 else net_weight

        row = db.execute(
            text("""
                INSERT INTO interunit_transfers_lines
                    (header_id, rm_pm_fg_type, item_category, sub_category,
                     item_desc_raw, pack_size, qty, uom,
                     unit_pack_size, net_weight, total_weight, batch_number, lot_number)
                VALUES
                    (:header_id, :material_type, :item_category, :sub_category,
                     :item_desc_raw, :pack_size, :quantity, :uom,
                     :unit_pack_size, :net_weight, :total_weight, :batch_number, :lot_number)
                RETURNING id, header_id, rm_pm_fg_type, item_category, sub_category,
                          item_desc_raw, pack_size, qty, uom,
                          unit_pack_size, net_weight, total_weight, batch_number, lot_number,
                          created_at, updated_at
            """),
            {
                "header_id": header_id,
                "material_type": line.material_type,
                "item_category": line.item_category,
                "sub_category": line.sub_category,
                "item_desc_raw": line.item_description,
                "pack_size": pack_size_f,
                "quantity": qty_i,
                "uom": line.uom or None,
                "unit_pack_size": float(line.unit_pack_size) if line.unit_pack_size else 0.0,
                "net_weight": net_weight,
                "total_weight": total_weight,
                "batch_number": line.batch_number or "",
                "lot_number": line.lot_number or "",
            },
        ).fetchone()
        lines.append(row)

    # NOTE: cold->warehouse has no real box scanning (the frontend "scanned boxes" are an
    # artifact), so ordered qty == shipped qty. The order (lot+qty) is authoritative and
    # reconcile_transfer_to_order corrects pending to match it (deduct the ordered lot,
    # restore wrong-lot/excess rows). No scan-based block here — there is nothing real to
    # block against.

    # Plan C: auto-derive boxes for warehouse-source transfers when the frontend
    # didn't scan any. Best-effort — if the stock is not box-tracked (no matching
    # boxes), ambiguous, or insufficient, we DO NOT block the save: the transfer
    # falls through to line-level pending below so every transfer is still tracked.
    if (not data.boxes) and lines and not _is_cold_site(data.header.from_warehouse):
        try:
            data.boxes = _auto_derive_warehouse_boxes(db, data.header.from_warehouse, lines)
        except HTTPException as e:
            logger.info(
                "TRANSFER: box auto-derive skipped (%s) — falling back to line-level pending",
                getattr(e, "detail", e),
            )
            data.boxes = []

    # Insert boxes (if provided)
    boxes = []
    if data.boxes:
        # Reject duplicate (box_id, transaction_no) pairs within the same transfer.
        # Cold-storage box_ids must be unique per physical box; duplicates here mean
        # the frontend fell back to a single cs_box_id for many boxes (see
        # TRANS202605131331 incident) — saving would silently lose inventory on receive
        # because the Transfer-In ON CONFLICT collapses duplicates.
        seen_keys: set = set()
        for box in data.boxes:
            bid = (box.box_id or "").strip()
            tno = (box.transaction_no or "").strip()
            if bid and tno and tno != "DIRECT":
                key = (bid, tno)
                if key in seen_keys:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"Duplicate box_id '{bid}' for transaction '{tno}' in this transfer. "
                            "Every physical box must carry a unique box_id."
                        ),
                    )
                seen_keys.add(key)


        # Build article-to-line-id lookup for correct box-to-line association
        line_id_by_article: dict = {}
        for l in lines:
            line_id_by_article[(l.item_desc_raw or "").strip().upper()] = l.id
        fallback_line_id = lines[0].id if lines else None

        for box in data.boxes:
            box_article_key = (box.article or "").strip().upper()
            matched_line_id = line_id_by_article.get(box_article_key, fallback_line_id)

            box_row = db.execute(
                text("""
                    INSERT INTO interunit_transfer_boxes
                        (header_id, transfer_line_id, box_number, box_id, article,
                         lot_number, batch_number, transaction_no,
                         net_weight, gross_weight)
                    VALUES
                        (:header_id, :transfer_line_id, :box_number, :box_id, :article,
                         :lot_number, :batch_number, :transaction_no,
                         :net_weight, :gross_weight)
                    RETURNING id, header_id, transfer_line_id, box_number, box_id,
                              article, lot_number, batch_number, transaction_no,
                              net_weight, gross_weight, created_at, updated_at
                """),
                {
                    "header_id": header_id,
                    "transfer_line_id": matched_line_id,
                    "box_number": box.box_number,
                    "box_id": box.box_id or "",
                    "article": box.article,
                    "lot_number": box.lot_number or "",
                    "batch_number": box.batch_number or "",
                    "transaction_no": box.transaction_no or "",
                    "net_weight": float(box.net_weight),
                    "gross_weight": float(box.gross_weight),
                },
            ).fetchone()
            boxes.append(box_row)

    # Line weights are already set correctly from frontend per-box values.
    # Each line represents one box entry   no need to sum box weights back into lines.

    # Park dispatched boxes into pending_transfer_stock (deducts source inventory)
    if boxes:
        park_in_pending(
            transfer_out_id=header_id,
            challan_no=header.challan_no,
            from_site=data.header.from_warehouse,
            to_site=data.header.to_warehouse,
            boxes=data.boxes or [],
            dispatched_by=getattr(header, "created_by", "system") or "system",
            db=db,
        )

        # For cold-source transfers, persist the canonical sub-cold(s) (D-39/D-514/
        # Rishi/Supreme Cold) on the header so the Transfer Out Records chip
        # filter can drill in. A single transfer can span multiple sub-units
        # (e.g. boxes from both D-39 and Rishi) — those land as a comma-separated
        # canonical list ("Rishi, Savla D-39"), which the chip filter matches via
        # ILIKE. Pulled from the JSONB snapshots park_in_pending just wrote into
        # pending_transfer_stock.
        if _is_cold_site(data.header.from_warehouse):
            _ensure_interunit_schema(db)
            raw_units = db.execute(
                text(
                    "SELECT DISTINCT cold_storage_data->>'unit' AS u "
                    "FROM pending_transfer_stock "
                    "WHERE transfer_out_id = :id AND cold_storage_data IS NOT NULL "
                    "  AND cold_storage_data->>'unit' IS NOT NULL"
                ),
                {"id": header_id},
            ).fetchall()
            canonical_set = {_normalize_cold_unit(r.u) for r in raw_units}
            canonical_set.discard(None)
            if canonical_set:
                joined = ", ".join(sorted(canonical_set))
                db.execute(
                    text("UPDATE interunit_transfers_header SET from_cold_unit = :u WHERE id = :id"),
                    {"u": joined, "id": header_id},
                )

    # MIXED scan + manual: park ALSO the lines that have no scanned/derived box, so
    # manually-filled entries are NEVER dropped. The old `elif lines` skipped line parking
    # whenever ANY box was scanned → manual stock vanished + transfer went Partial.
    # Coverage is counted per (article, lot); each article entry == 1 box in this form.
    _covered: dict = {}
    for _b in (data.boxes or []):
        _k = ((_b.article or "").strip().upper(), (_b.lot_number or "").strip())
        _covered[_k] = _covered.get(_k, 0) + 1
    uncovered_lines = []
    for _l in lines:
        _k = ((_l.item_desc_raw or "").strip().upper(), (_l.lot_number or "").strip())
        if _covered.get(_k, 0) > 0:
            _covered[_k] -= 1            # this line's box was scanned / auto-derived
        else:
            uncovered_lines.append(_l)   # manual / unscanned → park box-less so it isn't lost
    if uncovered_lines:
        park_lines_in_pending(
            transfer_out_id=header_id,
            challan_no=header.challan_no,
            from_site=data.header.from_warehouse,
            to_site=data.header.to_warehouse,
            lines=uncovered_lines,
            dispatched_by=getattr(header, "created_by", "system") or "system",
            db=db,
        )

    # Determine status. Everything is now parked (scanned boxes + box-less manual lines),
    # so a mixed / short-scan transfer is still fully dispatched — not Partial.
    if boxes:
        total_expected = sum(int(l.qty) for l in lines)
        actual_dispatched = len(boxes) + len(uncovered_lines)
        transfer_status = "Dispatch" if actual_dispatched >= total_expected else "Partial"

        db.execute(
            text("""
                UPDATE interunit_transfers_header
                SET status = :status
                WHERE id = :hid
            """),
            {"status": transfer_status, "hid": header_id},
        )

    # Reconcile pending_transfer_stock up to the ordered qty after the edit so the
    # in-transit ledger follows the changed lines/boxes (fills shortfall BY LOT).
    reconcile_transfer_to_order(header_id, db, dry_run=False)

    # Stamp a GENUINE edit marker (reconcile already ensured the column exists).
    # updated_ts is auto-managed (default now() + a BEFORE-UPDATE trigger), so it moves
    # on every create-reconcile / receive / sync and can't distinguish a real edit.
    # edited_at is written ONLY here, so the pending list's "Edited" badge is honest.
    db.execute(
        text("UPDATE interunit_transfers_header SET edited_at = :now WHERE id = :hid"),
        {"now": datetime.now(), "hid": header_id},
    )

    # Re-fetch header for latest status
    header = db.execute(
        text("""
            SELECT id, challan_no, stock_trf_date, from_site, to_site,
                   vehicle_no, driver_name, approved_by, remark, reason_code,
                   status, request_id, created_by, created_ts,
                   approved_ts, has_variance
            FROM interunit_transfers_header
            WHERE id = :hid
        """),
        {"hid": header_id},
    ).fetchone()

    result = _map_transfer_header(header)
    result["lines"] = [_map_transfer_line(l) for l in lines]
    result["boxes"] = [_map_box_row(b) for b in boxes]
    return result


# -- Delete transfer --


def delete_transfer(transfer_id: int, db: Session) -> dict:
    existing = db.execute(
        text("SELECT id, challan_no, status FROM interunit_transfers_header WHERE id = :tid"),
        {"tid": transfer_id},
    ).fetchone()

    if not existing:
        raise HTTPException(404, "Transfer not found")

    # No status restriction   authorized users can delete transfers in any status

    # Delete associated transfer-in records first (cascade)
    transfer_in_headers = db.execute(
        text("SELECT id FROM interunit_transfer_in_header WHERE transfer_out_id = :tid"),
        {"tid": transfer_id},
    ).fetchall()

    # Step 1: reverse every Transfer In — delete from destination, restore to pending
    for ti in transfer_in_headers:
        unpick_to_pending(transfer_in_id=ti.id, transfer_out_id=transfer_id, db=db)
        db.execute(
            text("DELETE FROM interunit_transfer_in_boxes WHERE header_id = :hid"),
            {"hid": ti.id},
        )

    # Step 2: restore every pending row back to source table
    restore_to_source(transfer_out_id=transfer_id, db=db)

    db.execute(
        text("DELETE FROM interunit_transfer_in_header WHERE transfer_out_id = :tid"),
        {"tid": transfer_id},
    )

    # Step 2b: cascade-remove COLD transfer-IN receipts for this transfer-out. Cold
    # receipts live in cold_transfer_in_headers / cold_transfer_inboxes (the unpick
    # above only reverses cold_stocks for boxes tracked in interunit_transfer_in_boxes),
    # so without this they orphan and keep showing in the cold Transfer-In records.
    # We intentionally do NOT delete <company>_cold_stocks here — unpick already
    # reversed them for the normal flow; deleting blindly could lose stock in the rare
    # cold-receipt-without-interunit-header case.
    cold_in_headers = db.execute(
        text("SELECT id FROM cold_transfer_in_headers WHERE transfer_out_id = :tid"),
        {"tid": transfer_id},
    ).fetchall()
    for ch in cold_in_headers:
        db.execute(
            text("DELETE FROM cold_transfer_inboxes WHERE header_id = :hid"),
            {"hid": ch.id},
        )
    db.execute(
        text("DELETE FROM cold_transfer_in_headers WHERE transfer_out_id = :tid"),
        {"tid": transfer_id},
    )

    # Delete transfer-out in FK order: boxes ? lines ? header
    db.execute(
        text("DELETE FROM interunit_transfer_boxes WHERE header_id = :tid"),
        {"tid": transfer_id},
    )
    db.execute(
        text("DELETE FROM interunit_transfers_lines WHERE header_id = :tid"),
        {"tid": transfer_id},
    )
    db.execute(
        text("DELETE FROM interunit_transfers_header WHERE id = :tid"),
        {"tid": transfer_id},
    )

    ti_count = len(transfer_in_headers)
    msg = "Transfer deleted successfully"
    if ti_count:
        msg += f" (along with {ti_count} transfer-in record{'s' if ti_count > 1 else ''})"

    return {
        "success": True,
        "message": msg,
        "transfer_id": existing.id,
        "challan_no": existing.challan_no,
    }


# ----------------------------------------------
#  Phase C   Transfer IN helpers
# ----------------------------------------------


def _map_transfer_in_header(row) -> dict:
    result = {
        "id": row.id,
        "transfer_out_id": row.transfer_out_id,
        "transfer_out_no": row.transfer_out_no or "",
        "grn_number": row.grn_number or "",
        "grn_date": row.grn_date,
        "receiving_warehouse": row.receiving_warehouse or "",
        "received_by": row.received_by or "",
        "received_at": row.received_at,
        "box_condition": row.box_condition,
        "condition_remarks": getattr(row, "condition_remarks", None),
        "status": row.status or "Received",
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        "inward_transaction_no": getattr(row, "inward_transaction_no", None) or None,
    }
    # Include from_warehouse if available (from JOIN with transfers header)
    if hasattr(row, "from_warehouse") and row.from_warehouse:
        result["from_warehouse"] = row.from_warehouse
    return result


def _map_transfer_in_box(row) -> dict:
    return {
        "id": row.id,
        "header_id": row.header_id,
        "box_id": row.box_id or "",
        "transfer_out_box_id": getattr(row, "transfer_out_box_id", None),
        "article": row.article,
        "batch_number": row.batch_number,
        "lot_number": row.lot_number,
        "transaction_no": row.transaction_no,
        "net_weight": float(row.net_weight) if row.net_weight is not None else None,
        "gross_weight": float(row.gross_weight) if row.gross_weight is not None else None,
        "scanned_at": row.scanned_at,
        "is_matched": row.is_matched if row.is_matched is not None else True,
        "issue": getattr(row, "issue", None),
        "line_index": getattr(row, "line_index", None),
        "inward_box_id": getattr(row, "inward_box_id", None) or None,
    }


def _fetch_transfer_in_boxes(db: Session, header_id: int) -> list:
    # lot_number is read from the IN box, but for older/article-level acknowledges it
    # may be blank. Fall back to the matching Transfer-OUT box's lot (scoped to THIS
    # transfer so box-id collisions across transfers can't bleed in) so the Transfer-In
    # view always shows the lot when it's known on the dispatch side.
    rows = db.execute(
        text("""
            SELECT itb.id, itb.header_id, itb.box_id, itb.article, itb.batch_number,
                   COALESCE(NULLIF(itb.lot_number, ''),
                       (SELECT ob.lot_number
                          FROM interunit_transfer_boxes ob
                          JOIN interunit_transfer_in_header tih ON tih.id = itb.header_id
                          WHERE ob.header_id = tih.transfer_out_id
                            AND ob.box_id = itb.box_id
                            AND ob.transaction_no = itb.transaction_no
                            AND COALESCE(ob.lot_number, '') <> ''
                          LIMIT 1)
                   ) AS lot_number,
                   itb.transaction_no, itb.net_weight, itb.gross_weight,
                   itb.scanned_at, itb.is_matched, itb.transfer_out_box_id, itb.issue, itb.line_index,
                   itb.inward_box_id
            FROM interunit_transfer_in_boxes itb
            WHERE itb.header_id = :hid
            ORDER BY itb.scanned_at
        """),
        {"hid": header_id},
    ).fetchall()
    return [_map_transfer_in_box(r) for r in rows]


# -- Create transfer IN (GRN) --


def create_transfer_in(data: TransferInCreate, db: Session) -> dict:
    # Cold destinations must use the dedicated cold-transfer-in endpoint.
    if _is_cold_destination_name(data.receiving_warehouse):
        raise HTTPException(
            status_code=400,
            detail="Cold receipts must use POST /interunit/cold-transfer-in/create",
        )

    # Verify Transfer OUT exists
    transfer_out = db.execute(
        text("SELECT id, challan_no, to_site FROM interunit_transfers_header WHERE id = :id"),
        {"id": data.transfer_out_id},
    ).fetchone()

    if not transfer_out:
        raise HTTPException(404, "Transfer OUT not found")

    # Check Transfer OUT not already received
    existing_in = db.execute(
        text("SELECT id, status FROM interunit_transfer_in_header WHERE transfer_out_id = :toid"),
        {"toid": data.transfer_out_id},
    ).fetchone()

    if existing_in:
        if existing_in.status == "Pending":
            raise HTTPException(400, "Transfer OUT has a pending Transfer IN. Please resume or delete it first.")
        else:
            raise HTTPException(400, "Transfer OUT already has a Transfer IN (GRN) record")

    # Check GRN number not duplicate
    existing_grn = db.execute(
        text("SELECT id FROM interunit_transfer_in_header WHERE grn_number = :grn"),
        {"grn": data.grn_number},
    ).fetchone()

    if existing_grn:
        raise HTTPException(400, f"GRN number {data.grn_number} already exists")

    # Insert Transfer IN header
    header = db.execute(
        text("""
            INSERT INTO interunit_transfer_in_header
                (transfer_out_id, transfer_out_no, grn_number, grn_date,
                 receiving_warehouse, received_by, received_at,
                 box_condition, condition_remarks, status)
            VALUES
                (:transfer_out_id, :transfer_out_no, :grn_number, CURRENT_TIMESTAMP,
                 :receiving_warehouse, :received_by, CURRENT_TIMESTAMP,
                 :box_condition, :condition_remarks, 'Pending')
            RETURNING id, transfer_out_id, transfer_out_no, grn_number, grn_date,
                      receiving_warehouse, received_by, received_at,
                      box_condition, condition_remarks, status,
                      created_at, updated_at
        """),
        {
            "transfer_out_id": data.transfer_out_id,
            "transfer_out_no": transfer_out.challan_no,
            "grn_number": data.grn_number,
            "receiving_warehouse": data.receiving_warehouse,
            "received_by": data.received_by,
            "box_condition": data.box_condition,
            "condition_remarks": data.condition_remarks,
        },
    ).fetchone()

    header_id = header.id

    # Bridge invariant: pick ONLY the boxes scanned/received now; the rest stay
    # In Transit so a partial receipt can't leak them. Pass lots too so relabeled /
    # transaction_no-less scans still reconcile fungibly by lot (no phantom leak).
    acknowledged_boxes = [
        {"box_id": b.box_id, "transaction_no": b.transaction_no, "lot_number": b.lot_number}
        for b in data.scanned_boxes
    ]
    picked = pick_from_pending(transfer_out_id=data.transfer_out_id, db=db,
                               acknowledged_boxes=acknowledged_boxes)

    # Insert scanned boxes
    boxes = []
    for box in data.scanned_boxes:
        issue_json = json.dumps(box.issue) if box.issue else None
        box_row = db.execute(
            text("""
                INSERT INTO interunit_transfer_in_boxes
                    (header_id, box_id, article, batch_number, lot_number,
                     transaction_no, net_weight, gross_weight,
                     scanned_at, is_matched, transfer_out_box_id, issue)
                VALUES
                    (:header_id, :box_id, :article, :batch_number, :lot_number,
                     :transaction_no, :net_weight, :gross_weight,
                     CURRENT_TIMESTAMP, :is_matched, :transfer_out_box_id, :issue)
                RETURNING id, header_id, box_id, article, batch_number,
                          lot_number, transaction_no, net_weight, gross_weight,
                          scanned_at, is_matched, transfer_out_box_id, issue
            """),
            {
                "header_id": header_id,
                "box_id": box.box_id,
                "article": box.article,
                "batch_number": box.batch_number,
                "lot_number": box.lot_number,
                "transaction_no": box.transaction_no,
                "net_weight": box.net_weight,
                "gross_weight": box.gross_weight,
                "is_matched": box.is_matched,
                "transfer_out_box_id": box.transfer_out_box_id,
                "issue": issue_json,
            },
        ).fetchone()
        boxes.append(box_row)

    # Bridge invariant completion gate: flip to 'Received' only when no real box
    # remains In Transit. Otherwise the IN header stays 'Pending' and the Transfer
    # OUT stays 'Dispatch', so the unreceived boxes keep showing on the bridge.
    remaining = count_remaining_in_transit(data.transfer_out_id, db)
    final_status = header.status
    if remaining == 0:
        db.execute(
            text("UPDATE interunit_transfer_in_header SET status = 'Received', received_at = CURRENT_TIMESTAMP WHERE id = :hid"),
            {"hid": header_id},
        )
        db.execute(
            text("UPDATE interunit_transfers_header SET status = 'Received' WHERE id = :toid"),
            {"toid": data.transfer_out_id},
        )
        final_status = "Received"

    result = _map_transfer_in_header(header)
    result["boxes"] = [_map_transfer_in_box(b) for b in boxes]
    result["total_boxes_scanned"] = len(boxes)
    result["status"] = final_status
    result["remaining_in_transit"] = remaining
    return result


# -- Cold storage helper (shared by create_transfer_in & finalize) --


def _insert_cold_storage_items(header_id: int, cold_storage_items, challan_no: str, db: Session, to_site: str = None):
    """Insert cold storage items directly into cfpl/cdpl_cold_stocks tables.
    to_site used as fallback for unit/storage_location."""
    cold_stocks_table_map = {"cfpl": "cfpl_cold_stocks", "cdpl": "cdpl_cold_stocks"}

    for cs_item in cold_storage_items:
        cold_company = (cs_item.cold_company or "").strip().lower()
        cold_table = cold_stocks_table_map.get(cold_company)

        if not cold_table:
            logger.warning("COLD_STOCKS_INSERT: Unknown cold_company=%s, skipping item=%s", cs_item.cold_company, cs_item.item_description)
            continue

        if cs_item.box_details:
            # Per-box insert: one row per box in cold stocks.
            # Rate is ₹/kg, so each box carries the same rate; value is computed
            # per-box as (box weight × rate) so heavier boxes carry more value.
            num_boxes = len(cs_item.box_details)
            rate_kg = cs_item.rate or 0

            for box_detail in cs_item.box_details:
                box_weight = float(box_detail.weight_kg or 0)
                rate_per_box = rate_kg or None
                value_per_box = (box_weight * rate_kg) if (rate_kg and box_weight) else None
                db.execute(
                    text(f"""
                        INSERT INTO {cold_table}
                            (inward_dt, unit, inward_no, item_description, item_mark,
                             vakkal, lot_no, no_of_cartons, weight_kg,
                             total_inventory_kgs, group_name, item_subgroup, storage_location,
                             exporter, last_purchase_rate, value,
                             box_id, transaction_no, spl_remarks)
                        VALUES
                            (:inward_dt, :unit, :inward_no, :item_description, :item_mark,
                             :vakkal, :lot_no, 1, :weight_kg,
                             :weight_kg, :group_name, :item_subgroup, :storage_location,
                             :exporter, :last_purchase_rate, :value,
                             :box_id, :transaction_no, :spl_remarks)
                    """),
                    {
                        "inward_dt": cs_item.inward_dt,
                        "unit": cs_item.storage_location or to_site,
                        "inward_no": challan_no,
                        "item_description": cs_item.item_description,
                        "item_mark": cs_item.item_mark,
                        "vakkal": cs_item.vakkal,
                        "lot_no": cs_item.lot_no,
                        "weight_kg": box_detail.weight_kg,
                        "group_name": cs_item.group_name,
                        "item_subgroup": cs_item.item_subgroup,
                        "storage_location": cs_item.storage_location or to_site,
                        "exporter": cs_item.exporter,
                        "last_purchase_rate": rate_per_box,
                        "value": value_per_box,
                        "box_id": box_detail.box_id,
                        "transaction_no": box_detail.transaction_no,
                        "spl_remarks": cs_item.spl_remarks,
                    },
                )
            logger.info(
                "COLD_STOCKS_INSERT: table=%s, item=%s, boxes=%d, storage=%s",
                cold_table, cs_item.item_description, len(cs_item.box_details),
                cs_item.storage_location or to_site,
            )
        else:
            # No box details   insert a single summary row
            db.execute(
                text(f"""
                    INSERT INTO {cold_table}
                        (inward_dt, unit, inward_no, item_description, item_mark,
                         vakkal, lot_no, no_of_cartons, weight_kg,
                         total_inventory_kgs, group_name, item_subgroup, storage_location,
                         exporter, last_purchase_rate, value, spl_remarks)
                    VALUES
                        (:inward_dt, :unit, :inward_no, :item_description, :item_mark,
                         :vakkal, :lot_no, :no_of_cartons, :weight_kg,
                         :weight_kg, :group_name, :item_subgroup, :storage_location,
                         :exporter, :last_purchase_rate, :value, :spl_remarks)
                """),
                {
                    "inward_dt": cs_item.inward_dt,
                    "unit": cs_item.storage_location or to_site,
                    "inward_no": challan_no,
                    "item_description": cs_item.item_description,
                    "item_mark": cs_item.item_mark,
                    "vakkal": cs_item.vakkal,
                    "lot_no": cs_item.lot_no,
                    "no_of_cartons": cs_item.no_of_cartons,
                    "weight_kg": cs_item.weight_kg,
                    "group_name": cs_item.group_name,
                    "item_subgroup": cs_item.item_subgroup,
                    "storage_location": cs_item.storage_location or to_site,
                    "exporter": cs_item.exporter,
                    "last_purchase_rate": cs_item.rate,
                    "value": cs_item.value,
                    "spl_remarks": cs_item.spl_remarks,
                },
            )
            logger.info(
                "COLD_STOCKS_INSERT: table=%s, item=%s (no box details), storage=%s",
                cold_table, cs_item.item_description, cs_item.storage_location or to_site,
            )


# -- Pending Transfer IN (Phase C - real-time acknowledge) --


def create_pending_transfer_in(data: PendingTransferInCreate, db: Session) -> dict:
    """Create a Pending transfer-in header. Idempotent: returns existing pending if one exists."""
    # Verify Transfer OUT exists
    transfer_out = db.execute(
        text("SELECT id, challan_no FROM interunit_transfers_header WHERE id = :id"),
        {"id": data.transfer_out_id},
    ).fetchone()
    if not transfer_out:
        raise HTTPException(404, "Transfer OUT not found")

    # Check if transfer-in already exists for this transfer-out
    existing_in = db.execute(
        text("SELECT id, status FROM interunit_transfer_in_header WHERE transfer_out_id = :toid"),
        {"toid": data.transfer_out_id},
    ).fetchone()

    if existing_in:
        if existing_in.status == "Pending":
            # Idempotent: return existing pending header
            row = db.execute(
                text("""
                    SELECT id, transfer_out_id, transfer_out_no, grn_number,
                           grn_date, receiving_warehouse, received_by, received_at,
                           box_condition, condition_remarks, status,
                           created_at, updated_at
                    FROM interunit_transfer_in_header WHERE id = :hid
                """),
                {"hid": existing_in.id},
            ).fetchone()
            result = _map_transfer_in_header(row)
            result["boxes"] = _fetch_transfer_in_boxes(db, existing_in.id)
            result["total_boxes_scanned"] = len(result["boxes"])
            return result
        else:
            raise HTTPException(400, "Transfer OUT already has a completed Transfer IN (GRN) record")

    # Check GRN number not duplicate
    existing_grn = db.execute(
        text("SELECT id FROM interunit_transfer_in_header WHERE grn_number = :grn"),
        {"grn": data.grn_number},
    ).fetchone()
    if existing_grn:
        raise HTTPException(400, f"GRN number {data.grn_number} already exists")

    # Insert header with status='Pending'
    header = db.execute(
        text("""
            INSERT INTO interunit_transfer_in_header
                (transfer_out_id, transfer_out_no, grn_number, grn_date,
                 receiving_warehouse, received_by, received_at,
                 box_condition, condition_remarks, status)
            VALUES
                (:transfer_out_id, :transfer_out_no, :grn_number, CURRENT_TIMESTAMP,
                 :receiving_warehouse, :received_by, CURRENT_TIMESTAMP,
                 :box_condition, :condition_remarks, 'Pending')
            RETURNING id, transfer_out_id, transfer_out_no, grn_number, grn_date,
                      receiving_warehouse, received_by, received_at,
                      box_condition, condition_remarks, status,
                      created_at, updated_at
        """),
        {
            "transfer_out_id": data.transfer_out_id,
            "transfer_out_no": transfer_out.challan_no,
            "grn_number": data.grn_number,
            "receiving_warehouse": data.receiving_warehouse,
            "received_by": data.received_by,
            "box_condition": data.box_condition,
            "condition_remarks": data.condition_remarks,
        },
    ).fetchone()

    result = _map_transfer_in_header(header)
    result["boxes"] = []
    result["total_boxes_scanned"] = 0
    return result


def acknowledge_pending_box(header_id: int, data: PendingBoxAcknowledge, db: Session, autofinalize: bool = True) -> dict:
    """UPSERT a single box/article into a pending transfer-in.

    Transparently runs STBR (Scan-Time Box ID Reconciliation) before the
    UPSERT. If the scanned box_id differs from the placeholder IMS picked
    at dispatch, the pending row is remapped — the wrongly-picked box is
    restored to source inventory and the actually-shipped box is
    re-deducted. A series offset detected on the first scan also
    propagates the same swap to all remaining siblings in the same
    (transfer_out_id, transaction_no, lot_no) batch.

    See docs/conventions.md#pending-transfer-stock-middleware for the
    full flow.
    """
    # Verify header exists and is Pending
    header = db.execute(
        text("""
            SELECT id, status, transfer_out_id
            FROM interunit_transfer_in_header WHERE id = :hid
        """),
        {"hid": header_id},
    ).fetchone()
    if not header:
        raise HTTPException(404, "Transfer IN header not found")
    if header.status != "Pending":
        raise HTTPException(400, "Transfer IN is not in Pending status")

    issue_json = json.dumps(data.issue) if data.issue else None

    # Atomic upsert — safe against concurrent clients acknowledging the same box
    try:
        db.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_transfer_in_boxes_header_box
            ON interunit_transfer_in_boxes (header_id, box_id)
        """))
    except Exception:
        pass

    # ── STBR: reconcile placeholder ↔ scanned box BEFORE the UPSERT ──
    stbr_result: dict = {
        "status": "noop", "reconciliation_id": None,
        "original_box_id": None, "propagated_count": 0, "siblings": [],
    }
    scanned_box_id = (data.box_id or "").strip()
    scanned_txn = (data.transaction_no or "").strip()
    # Only run STBR when pending_transfer_stock has placeholder rows for this
    # specific (transfer_out_id, transaction_no) pair. Transfers that dispatch
    # items as plain lines (no per-box QR on the outward side) have no slots to
    # reconcile against — STBR would always return "conflict" and block acknowledge.
    _outward_slot_count = 0
    if scanned_box_id and scanned_txn and scanned_txn != "DIRECT" and header.transfer_out_id:
        _outward_slot_count = db.execute(
            text("""
                SELECT COUNT(*) FROM pending_transfer_stock
                WHERE transfer_out_id = :tid AND transaction_no = :txn
            """),
            {"tid": header.transfer_out_id, "txn": scanned_txn},
        ).scalar() or 0

    if scanned_box_id and scanned_txn and scanned_txn != "DIRECT" and header.transfer_out_id and _outward_slot_count > 0:
        try:
            from services.ims_service.pending_stock_tools import reconcile_box_in_pending
            scan_source = getattr(data, "scan_source", None) or "manual"
            scanned_by = getattr(data, "scanned_by", None)
            stbr_result = reconcile_box_in_pending(
                db,
                scanned_box_id=scanned_box_id,
                scanned_transaction_no=scanned_txn,
                transfer_in_header_id=header_id,
                transfer_out_id=header.transfer_out_id,
                scan_source=scan_source,
                scanned_by=scanned_by,
            ) or stbr_result
            if stbr_result.get("status") == "duplicate":
                raise HTTPException(
                    409,
                    f"Box {scanned_box_id} is already acknowledged elsewhere — "
                    f"{stbr_result.get('reason') or 'duplicate scan'}",
                )
            if stbr_result.get("status") == "conflict":
                # 422 lets the frontend show a Conflict banner without losing
                # the rest of the batch.
                raise HTTPException(
                    422,
                    f"Reconciliation conflict for box {scanned_box_id}: "
                    f"{stbr_result.get('reason') or 'no matching slot'}",
                )
        except HTTPException:
            raise
        except Exception as e:
            # STBR failure must not block the UPSERT — log and continue.
            logger.warning("STBR reconciliation skipped for %s/%s: %s",
                           scanned_box_id, scanned_txn, e)

    row = db.execute(
        text("""
            INSERT INTO interunit_transfer_in_boxes
                (header_id, box_id, article, batch_number, lot_number,
                 transaction_no, net_weight, gross_weight,
                 scanned_at, is_matched, transfer_out_box_id, issue, line_index,
                 original_box_id, reconciled, reconciliation_id, scan_source)
            VALUES
                (:header_id, :box_id, :article, :batch_number, :lot_number,
                 :transaction_no, :net_weight, :gross_weight,
                 CURRENT_TIMESTAMP, :is_matched, :transfer_out_box_id, :issue, :line_index,
                 :original_box_id, :reconciled, :reconciliation_id, :scan_source)
            ON CONFLICT (header_id, box_id) DO UPDATE SET
                article = EXCLUDED.article,
                batch_number = EXCLUDED.batch_number,
                lot_number = EXCLUDED.lot_number,
                transaction_no = EXCLUDED.transaction_no,
                net_weight = EXCLUDED.net_weight,
                gross_weight = EXCLUDED.gross_weight,
                is_matched = EXCLUDED.is_matched,
                transfer_out_box_id = EXCLUDED.transfer_out_box_id,
                issue = EXCLUDED.issue,
                line_index = EXCLUDED.line_index,
                scanned_at = CURRENT_TIMESTAMP,
                original_box_id = COALESCE(interunit_transfer_in_boxes.original_box_id, EXCLUDED.original_box_id),
                reconciled = EXCLUDED.reconciled OR interunit_transfer_in_boxes.reconciled,
                reconciliation_id = COALESCE(EXCLUDED.reconciliation_id, interunit_transfer_in_boxes.reconciliation_id),
                scan_source = EXCLUDED.scan_source
            RETURNING id, header_id, box_id, article, batch_number,
                      lot_number, transaction_no, net_weight, gross_weight,
                      scanned_at, is_matched, transfer_out_box_id, issue, line_index
        """),
        {
            "header_id": header_id,
            "box_id": data.box_id,
            "article": data.article,
            "batch_number": data.batch_number,
            "lot_number": data.lot_number,
            "transaction_no": data.transaction_no,
            "net_weight": data.net_weight,
            "gross_weight": data.gross_weight,
            "is_matched": data.is_matched,
            "transfer_out_box_id": data.transfer_out_box_id,
            "issue": issue_json,
            "line_index": data.line_index,
            "original_box_id": stbr_result.get("original_box_id"),
            "reconciled": stbr_result.get("status") in ("matched", "overridden", "propagated"),
            "reconciliation_id": stbr_result.get("reconciliation_id"),
            "scan_source": getattr(data, "scan_source", None) or "manual",
        },
    ).fetchone()

    result = _map_transfer_in_box(row)
    # Surface STBR outcome to the frontend (live UI updates: audit tooltip,
    # propagation count, conflict banner). Existing callers ignore extra keys.
    result["reconciliation"] = {
        "status": stbr_result.get("status"),
        "original_box_id": stbr_result.get("original_box_id"),
        "propagated_count": stbr_result.get("propagated_count", 0),
        "siblings": stbr_result.get("siblings", []),
        "reconciliation_id": stbr_result.get("reconciliation_id"),
    }
    # Auto-finalize once every in-transit box has been acknowledged (single-box callers).
    # Batch passes autofinalize=False and finalizes once after its whole loop instead.
    if autofinalize and _autofinalize_if_complete(db, header_id):
        result["auto_finalized"] = True
    return result


def unacknowledge_pending_box(header_id: int, box_id: str, db: Session) -> dict:
    """Delete a single box/article from a pending transfer-in."""
    header = db.execute(
        text("SELECT id, status FROM interunit_transfer_in_header WHERE id = :hid"),
        {"hid": header_id},
    ).fetchone()
    if not header:
        raise HTTPException(404, "Transfer IN header not found")
    if header.status != "Pending":
        raise HTTPException(400, "Transfer IN is not in Pending status")

    result = db.execute(
        text("DELETE FROM interunit_transfer_in_boxes WHERE header_id = :hid AND box_id = :bid RETURNING id"),
        {"hid": header_id, "bid": box_id},
    ).fetchone()

    if not result:
        raise HTTPException(404, f"Box {box_id} not found in this transfer-in")

    return {"success": True, "deleted_box_id": box_id}


def acknowledge_pending_boxes_batch(header_id: int, boxes: list, db: Session) -> dict:
    """Batch acknowledge multiple boxes in a pending transfer-in.

    Each box runs the same STBR reconciliation as the single-box endpoint —
    so a series-offset detected on the first box propagates to its siblings
    automatically before the loop even reaches them.
    """
    # Delegate to acknowledge_pending_box per-box so STBR + audit columns
    # apply uniformly. Conflicts on individual boxes are surfaced per row
    # instead of failing the entire batch.
    results = []
    conflicts = []
    for box_data in boxes:
        try:
            ack = acknowledge_pending_box(header_id, box_data, db, autofinalize=False)
            results.append(ack)
        except HTTPException as e:
            conflicts.append({
                "box_id": getattr(box_data, "box_id", None),
                "transaction_no": getattr(box_data, "transaction_no", None),
                "status_code": e.status_code,
                "detail": e.detail,
            })
    # Auto-finalize once, after the whole batch, if acknowledgements now cover the
    # in-transit set (so a completed receipt no longer lingers as 'Partial').
    auto_finalized = _autofinalize_if_complete(db, header_id)
    return {
        "success": len(conflicts) == 0,
        "count": len(results),
        "boxes": results,
        "conflicts": conflicts,
        "auto_finalized": auto_finalized,
    }


def close_transfer_in_with_shortage(header_id: int, shortage_reason, closed_by, db: Session) -> dict:
    """Explicitly close a Pending transfer-in that has a genuine shortage.

    Picks the boxes actually acknowledged, then WRITES OFF the remaining in-transit
    boxes (they won't be received) and marks both headers 'Received' with a shortage
    note. Use only when the missing boxes are truly not coming — the bridge invariant
    otherwise correctly keeps the transfer 'Pending'.
    """
    header = db.execute(
        text("SELECT id, status, transfer_out_id, transfer_out_no FROM interunit_transfer_in_header WHERE id = :hid"),
        {"hid": header_id},
    ).fetchone()
    if not header:
        raise HTTPException(404, "Transfer IN header not found")
    if header.status == "Received":
        raise HTTPException(400, "Transfer IN is already Received")
    if header.status != "Pending":
        raise HTTPException(400, "Transfer IN is not in Pending status")

    # Pick the boxes actually acknowledged on this GRN.
    ack_rows = db.execute(
        text("SELECT box_id, transaction_no, lot_number FROM interunit_transfer_in_boxes WHERE header_id = :hid"),
        {"hid": header_id},
    ).fetchall()
    acknowledged_boxes = [{"box_id": r.box_id, "transaction_no": r.transaction_no, "lot_number": r.lot_number} for r in ack_rows]
    pick_from_pending(transfer_out_id=header.transfer_out_id, db=db, acknowledged_boxes=acknowledged_boxes)

    # Count the real shortage, then write off ALL remaining in-transit rows for the transfer.
    shortage = count_remaining_in_transit(header.transfer_out_id, db)
    written_off = db.execute(
        text("DELETE FROM pending_transfer_stock WHERE transfer_out_id = :tid AND status = 'In Transit' RETURNING id"),
        {"tid": header.transfer_out_id},
    ).fetchall()

    note = f"Closed with shortage: {shortage} box(es) written off by {closed_by or 'unknown'}."
    if shortage_reason:
        note += f" Reason: {shortage_reason}"
    cur = db.execute(
        text("SELECT condition_remarks FROM interunit_transfer_in_header WHERE id = :hid"),
        {"hid": header_id},
    ).scalar()
    new_remarks = f"{cur} | {note}" if cur else note

    updated = db.execute(
        text("""
            UPDATE interunit_transfer_in_header
            SET status = 'Received', received_at = CURRENT_TIMESTAMP,
                condition_remarks = :rem, updated_at = CURRENT_TIMESTAMP
            WHERE id = :hid
            RETURNING id, transfer_out_id, transfer_out_no, grn_number, grn_date,
                      receiving_warehouse, received_by, received_at,
                      box_condition, condition_remarks, status, created_at, updated_at
        """),
        {"hid": header_id, "rem": new_remarks},
    ).fetchone()
    db.execute(
        text("UPDATE interunit_transfers_header SET status = 'Received' WHERE id = :toid"),
        {"toid": header.transfer_out_id},
    )

    result = _map_transfer_in_header(updated)
    result["boxes"] = _fetch_transfer_in_boxes(db, header_id)
    result["written_off"] = len(written_off)
    result["shortage"] = shortage
    logger.info("CLOSE_SHORTAGE: GRN header=%s transfer_out=%s shortage=%s written_off=%s by=%s",
                header_id, header.transfer_out_id, shortage, len(written_off), closed_by)
    return result


def finalize_transfer_in(header_id: int, data: FinalizeTransferIn, db: Session) -> dict:
    """Finalize a Pending transfer-in: transition status to Received."""
    # Cold destinations must use the dedicated cold-transfer-in finalize endpoint.
    hdr_row = db.execute(
        text("SELECT receiving_warehouse FROM interunit_transfer_in_header WHERE id=:hid"),
        {"hid": header_id},
    ).fetchone()
    if hdr_row and _is_cold_destination_name(hdr_row._mapping["receiving_warehouse"]):
        raise HTTPException(
            status_code=400,
            detail="Cold receipts must use POST /interunit/cold-transfer-in/{header_id}/finalize",
        )

    header = db.execute(
        text("""
            SELECT id, status, transfer_out_id, transfer_out_no
            FROM interunit_transfer_in_header WHERE id = :hid
        """),
        {"hid": header_id},
    ).fetchone()
    if not header:
        raise HTTPException(404, "Transfer IN header not found")
    if header.status == "Received":
        # Idempotent: already finalized. Also clear any stray In-Transit rows that
        # appeared AFTER this header was marked Received (orphan-trap fix): pick the
        # boxes recorded on this GRN so they don't linger forever on the bridge.
        _ack = db.execute(
            text("SELECT box_id, transaction_no, lot_number FROM interunit_transfer_in_boxes WHERE header_id = :hid"),
            {"hid": header_id},
        ).fetchall()
        if _ack:
            pick_from_pending(transfer_out_id=header.transfer_out_id, db=db,
                              acknowledged_boxes=[{"box_id": r.box_id, "transaction_no": r.transaction_no, "lot_number": r.lot_number} for r in _ack])
        full = db.execute(
            text("""SELECT id, transfer_out_id, transfer_out_no, grn_number, grn_date,
                           receiving_warehouse, received_by, received_at, box_condition,
                           condition_remarks, status, created_at, updated_at
                    FROM interunit_transfer_in_header WHERE id = :hid"""),
            {"hid": header_id},
        ).fetchone()
        result = _map_transfer_in_header(full)
        result["boxes"] = _fetch_transfer_in_boxes(db, header_id)
        result["already_finalized"] = True
        return result
    if header.status != "Pending":
        raise HTTPException(400, "Transfer IN is not in Pending status")

    # Verify at least 1 box exists
    box_count = db.execute(
        text("SELECT COUNT(*) FROM interunit_transfer_in_boxes WHERE header_id = :hid"),
        {"hid": header_id},
    ).scalar()
    if box_count == 0:
        raise HTTPException(400, "No boxes/articles acknowledged. Cannot finalize.")

    # Bridge invariant: pick ONLY the boxes acknowledged on this GRN; the rest stay
    # In Transit. The transfer flips to 'Received' only when none remain.
    ack_rows = db.execute(
        text("SELECT box_id, transaction_no, lot_number FROM interunit_transfer_in_boxes WHERE header_id = :hid"),
        {"hid": header_id},
    ).fetchall()
    acknowledged_boxes = [{"box_id": r.box_id, "transaction_no": r.transaction_no, "lot_number": r.lot_number} for r in ack_rows]
    picked = pick_from_pending(transfer_out_id=header.transfer_out_id, db=db,
                               acknowledged_boxes=acknowledged_boxes)


    # Completion gate: 'Received' only when no real box remains In Transit.
    remaining = count_remaining_in_transit(header.transfer_out_id, db)
    if remaining == 0:
        updated = db.execute(
            text("""
                UPDATE interunit_transfer_in_header
                SET status = 'Received',
                    received_at = CURRENT_TIMESTAMP,
                    box_condition = :box_condition,
                    condition_remarks = :condition_remarks,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :hid
                RETURNING id, transfer_out_id, transfer_out_no, grn_number, grn_date,
                          receiving_warehouse, received_by, received_at,
                          box_condition, condition_remarks, status,
                          created_at, updated_at
            """),
            {"hid": header_id, "box_condition": data.box_condition,
             "condition_remarks": data.condition_remarks},
        ).fetchone()
        db.execute(
            text("UPDATE interunit_transfers_header SET status = 'Received' WHERE id = :toid"),
            {"toid": header.transfer_out_id},
        )
    else:
        # Incomplete receipt — persist condition notes but stay 'Pending' so the
        # unreceived boxes keep showing on the bridge (Transfer OUT stays 'Dispatch').
        updated = db.execute(
            text("""
                UPDATE interunit_transfer_in_header
                SET box_condition = :box_condition,
                    condition_remarks = :condition_remarks,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :hid
                RETURNING id, transfer_out_id, transfer_out_no, grn_number, grn_date,
                          receiving_warehouse, received_by, received_at,
                          box_condition, condition_remarks, status,
                          created_at, updated_at
            """),
            {"hid": header_id, "box_condition": data.box_condition,
             "condition_remarks": data.condition_remarks},
        ).fetchone()

    result = _map_transfer_in_header(updated)
    result["boxes"] = _fetch_transfer_in_boxes(db, header_id)
    result["total_boxes_scanned"] = len(result["boxes"])
    result["remaining_in_transit"] = remaining
    return result


def _autofinalize_if_complete(db: Session, header_id: int) -> bool:
    """Auto-finalize a Pending transfer-in once its acknowledged boxes cover every box
    still in transit for the dispatch. Fixes the 'acknowledged but never finalized' gap
    that left a fully-received transfer stuck in the Pending modal as 'Partial (GRN raised)'.
    Returns True if it finalized. Caller owns the surrounding transaction/commit."""
    h = db.execute(
        text("SELECT id, status, transfer_out_id FROM interunit_transfer_in_header WHERE id = :hid"),
        {"hid": header_id},
    ).fetchone()
    if not h or h.status != "Pending":
        return False
    acked = db.execute(
        text("SELECT COUNT(*) FROM interunit_transfer_in_boxes WHERE header_id = :hid"),
        {"hid": header_id},
    ).scalar() or 0
    in_transit = db.execute(
        text("SELECT COUNT(*) FROM pending_transfer_stock "
             "WHERE transfer_out_id = :tid AND status = 'In Transit'"),
        {"tid": h.transfer_out_id},
    ).scalar() or 0
    if acked > 0 and in_transit > 0 and acked >= in_transit:
        # SAVEPOINT-isolate the finalize: it does multiple writes (header->Received,
        # pick_from_pending, transfer-out->Received). If any step fails, roll back ONLY
        # the finalize so the acknowledgement that triggered it still commits — never
        # leave a half-finalized header. The backlog sweep can retry later.
        try:
            with db.begin_nested():
                finalize_transfer_in(header_id, FinalizeTransferIn(), db)
            logger.info("AUTO-FINALIZE: GRN header=%s transfer_out=%s (acked %s >= in_transit %s)",
                        header_id, h.transfer_out_id, acked, in_transit)
            return True
        except Exception:
            logger.exception("AUTO-FINALIZE failed for GRN header=%s transfer_out=%s; "
                             "acknowledgement preserved, finalize deferred to sweep",
                             header_id, h.transfer_out_id)
            return False
    return False


def finalize_complete_pending_grns(db: Session, dry_run: bool = False) -> dict:
    """Backlog sweep: finalize every Pending transfer-in whose acknowledged boxes already
    cover the in-transit set (the acknowledged-but-not-finalized backlog). Commits per GRN
    on apply. Returns a summary; writes nothing when dry_run=True."""
    rows = db.execute(text("""
        SELECT tih.id AS grn_id, tih.transfer_out_id, tih.grn_number,
               (SELECT COUNT(*) FROM interunit_transfer_in_boxes b WHERE b.header_id = tih.id) AS acked,
               (SELECT COUNT(*) FROM pending_transfer_stock p
                  WHERE p.transfer_out_id = tih.transfer_out_id AND p.status = 'In Transit') AS in_transit
        FROM interunit_transfer_in_header tih
        WHERE tih.status = 'Pending'
        ORDER BY tih.id
    """)).fetchall()
    summary = {"pending_grns_scanned": len(rows), "finalized": [], "skipped": []}
    for r in rows:
        complete = r.acked > 0 and r.in_transit > 0 and r.acked >= r.in_transit
        rec = {"grn_id": r.grn_id, "transfer_out_id": r.transfer_out_id,
               "grn_number": r.grn_number, "acked": int(r.acked), "in_transit": int(r.in_transit)}
        if complete:
            summary["finalized"].append(rec)
            if not dry_run:
                finalize_transfer_in(r.grn_id, FinalizeTransferIn(), db)
                db.commit()
        else:
            summary["skipped"].append(rec)
    return summary


def get_pending_by_transfer_out(transfer_out_id: int, db: Session) -> dict:
    """Lookup pending transfer-in header + boxes by transfer_out_id."""
    row = db.execute(
        text("""
            SELECT id, transfer_out_id, transfer_out_no, grn_number,
                   grn_date, receiving_warehouse, received_by, received_at,
                   box_condition, condition_remarks, status,
                   inward_transaction_no,
                   created_at, updated_at
            FROM interunit_transfer_in_header
            WHERE transfer_out_id = :toid AND status = 'Pending'
        """),
        {"toid": transfer_out_id},
    ).fetchone()

    if not row:
        return {"exists": False, "header": None}

    header = _map_transfer_in_header(row)
    header["boxes"] = _fetch_transfer_in_boxes(db, row.id)
    header["total_boxes_scanned"] = len(header["boxes"])
    return {"exists": True, "header": header}


def get_pending_boxes_by_transfer_out(transfer_out_id: int, db: Session) -> dict:
    """Return the 'In Transit' pending_transfer_stock rows for a transfer-out, mapped to
    the box shape the cold receive form consumes. Includes 'LINE-%' synthetic rows so
    warehouse→cold transfers (parked line-level because the warehouse source has no
    per-box stock) can still be received on the cold IN page. Read-only; no writes."""
    rows = db.execute(
        text("""
            SELECT pts.id, pts.box_id, pts.transaction_no, pts.item_description, pts.lot_no,
                   pts.batch_number, pts.weight_kg, pts.gross_weight, pts.net_weight,
                   pts.article, pts.cold_storage_data,
                   otb.id AS transfer_out_box_id
            FROM pending_transfer_stock pts
            LEFT JOIN interunit_transfer_boxes otb
              ON otb.header_id = pts.transfer_out_id
             AND otb.box_id = pts.box_id
             AND COALESCE(otb.transaction_no, '') = COALESCE(pts.transaction_no, '')
            WHERE pts.transfer_out_id = :tid
              AND pts.status = 'In Transit'
            ORDER BY pts.id
        """),
        {"tid": transfer_out_id},
    ).fetchall()

    boxes = []
    for r in rows:
        cold = r.cold_storage_data if isinstance(r.cold_storage_data, dict) else {}
        net = r.net_weight if r.net_weight is not None else r.weight_kg
        boxes.append({
            "id": r.id,
            "transfer_out_box_id": r.transfer_out_box_id,
            "box_id": r.box_id,
            "transaction_no": r.transaction_no,
            "article": r.article or r.item_description,
            "lot_number": r.lot_no,
            "batch_number": r.batch_number or cold.get("batch_number"),
            "net_weight": float(net) if net is not None else None,
            "gross_weight": float(r.gross_weight) if r.gross_weight is not None else None,
        })

    return {"transfer_out_id": transfer_out_id, "total": len(boxes), "boxes": boxes}


# -- List transfer INs --


def list_transfer_ins(
    page: int,
    per_page: int,
    receiving_warehouse: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    sort_by: str,
    sort_order: str,
    db: Session,
    search: Optional[str] = None,
) -> dict:
    clauses = ["1=1"]
    params: dict = {}

    if receiving_warehouse:
        clauses.append("h.receiving_warehouse = :rw")
        params["rw"] = receiving_warehouse.upper()
    if from_date:
        clauses.append("h.grn_date >= :from_date")
        params["from_date"] = _convert_date(from_date)
    if to_date:
        clauses.append("h.grn_date <= :to_date")
        params["to_date"] = _convert_date(to_date)
    if search and search.strip():
        # Search by GRN / challan / receiver / LOT / box-id / article. Lot is matched
        # against BOTH the received (IN) boxes and the dispatch (OUT) boxes, since the
        # IN-box lot can be blank while the OUT box carries it.
        clauses.append("""(
            h.grn_number ILIKE :s
            OR h.transfer_out_no ILIKE :s
            OR h.received_by ILIKE :s
            OR EXISTS (SELECT 1 FROM interunit_transfer_in_boxes ib
                       WHERE ib.header_id = h.id
                         AND (ib.lot_number ILIKE :s OR ib.box_id ILIKE :s OR ib.article ILIKE :s))
            OR EXISTS (SELECT 1 FROM interunit_transfer_boxes ob
                       WHERE ob.header_id = h.transfer_out_id
                         AND (ob.lot_number ILIKE :s OR ob.box_id ILIKE :s OR ob.article ILIKE :s))
        )""")
        params["s"] = f"%{search.strip()}%"

    where = " AND ".join(clauses)

    valid_sort = {"grn_number", "grn_date", "receiving_warehouse", "status", "created_at"}
    if sort_by not in valid_sort:
        sort_by = "created_at"
    direction = "DESC" if sort_order.lower() == "desc" else "ASC"

    total = db.execute(
        text(f"SELECT COUNT(*) FROM interunit_transfer_in_header h WHERE {where}"),
        params,
    ).scalar()

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    rows = db.execute(
        text(f"""
            SELECT
                h.id, h.transfer_out_id, h.transfer_out_no, h.grn_number,
                h.grn_date, h.receiving_warehouse, h.received_by, h.received_at,
                h.box_condition, h.condition_remarks, h.status,
                h.created_at, h.updated_at,
                COUNT(b.id) AS total_boxes_scanned,
                t.from_site AS from_warehouse,
                (SELECT STRING_AGG(DISTINCT lot, ' ') FROM (
                     SELECT lot_number AS lot FROM interunit_transfer_in_boxes
                       WHERE header_id = h.id AND COALESCE(lot_number,'') <> ''
                     UNION
                     SELECT lot_number FROM interunit_transfer_boxes
                       WHERE header_id = h.transfer_out_id AND COALESCE(lot_number,'') <> ''
                 ) lx) AS lot_numbers
            FROM interunit_transfer_in_header h
            LEFT JOIN interunit_transfer_in_boxes b ON h.id = b.header_id
            LEFT JOIN interunit_transfers_header t ON h.transfer_out_id = t.id
            WHERE {where}
            GROUP BY h.id, t.from_site
            ORDER BY h.{sort_by} {direction}
            LIMIT :limit OFFSET :offset
        """),
        params,
    ).fetchall()

    records = []
    for row in rows:
        item = _map_transfer_in_header(row)
        item["total_boxes_scanned"] = row.total_boxes_scanned or 0
        item["lot_numbers"] = getattr(row, "lot_numbers", None) or ""
        records.append(item)

    return {
        "records": records,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if total else 0,
    }


# -- List Cold-only Transfer-IN records (new dedicated cold tables) --
#
# Reads exclusively from `cold_transfer_in_headers` + `cold_transfer_inboxes`
# (no fallthrough to `interunit_transfer_in_header*`). Shape-identical to
# list_transfer_ins() so the cold-transfer page can swap with one identifier
# change. Column-name diffs vs. the legacy tables: lot_no (not lot_number),
# weight_kg (not net_weight), item_description (not article), to_site (≈
# receiving_warehouse), from_site (≈ from_warehouse). Cold-destination is
# guaranteed by the table itself per the 2026-06-06 correction.
def list_cold_transfer_ins(
    page: int,
    per_page: int,
    receiving_warehouse: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    sort_by: str,
    sort_order: str,
    db: Session,
    search: Optional[str] = None,
) -> dict:
    clauses = ["1=1"]
    params: dict = {}

    if receiving_warehouse:
        clauses.append("UPPER(h.to_site) = :rw")
        params["rw"] = receiving_warehouse.upper()
    if from_date:
        clauses.append("h.grn_date >= :from_date")
        params["from_date"] = _convert_date(from_date)
    if to_date:
        clauses.append("h.grn_date <= :to_date")
        params["to_date"] = _convert_date(to_date)
    if search and search.strip():
        # Match what the legacy endpoint searches over, mapped to the new
        # column names. TODO: extend if the cold-transfer page surfaces more
        # searchable fields.
        clauses.append("""(
            h.grn_number ILIKE :s
            OR h.transfer_out_no ILIKE :s
            OR h.received_by ILIKE :s
            OR h.from_site ILIKE :s
            OR h.to_site ILIKE :s
            OR EXISTS (SELECT 1 FROM cold_transfer_inboxes ib
                       WHERE ib.header_id = h.id
                         AND (ib.lot_no ILIKE :s
                              OR ib.box_id ILIKE :s
                              OR ib.item_description ILIKE :s))
        )""")
        params["s"] = f"%{search.strip()}%"

    where = " AND ".join(clauses)

    valid_sort = {"grn_number", "grn_date", "to_site", "status", "created_at"}
    # Map the legacy sort key "receiving_warehouse" → cold column "to_site"
    # so the existing frontend params keep working unchanged.
    sort_alias = {"receiving_warehouse": "to_site"}
    sort_col = sort_alias.get(sort_by, sort_by)
    if sort_col not in valid_sort:
        sort_col = "created_at"
    direction = "DESC" if sort_order.lower() == "desc" else "ASC"

    total = db.execute(
        text(f"SELECT COUNT(*) FROM cold_transfer_in_headers h WHERE {where}"),
        params,
    ).scalar()

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    rows = db.execute(
        text(f"""
            SELECT
                h.id,
                h.transfer_out_id,
                h.transfer_out_no,
                h.grn_number,
                h.grn_date,
                h.to_site AS receiving_warehouse,
                h.from_site AS from_warehouse,
                h.received_by,
                h.received_at,
                h.box_condition,
                h.condition_remarks,
                h.status,
                h.inward_transaction_no,
                h.to_company,
                h.created_at,
                h.updated_at,
                (SELECT COUNT(*) FROM cold_transfer_inboxes b
                   WHERE b.header_id = h.id) AS total_boxes_scanned,
                (SELECT STRING_AGG(DISTINCT u, ', ') FROM (
                     SELECT unit AS u FROM cold_transfer_inboxes
                       WHERE header_id = h.id AND COALESCE(unit, '') <> ''
                 ) ux) AS from_cold_unit,
                (SELECT STRING_AGG(DISTINCT lot, ' ') FROM (
                     SELECT lot_no AS lot FROM cold_transfer_inboxes
                       WHERE header_id = h.id AND COALESCE(lot_no, '') <> ''
                 ) lx) AS lot_numbers
            FROM cold_transfer_in_headers h
            WHERE {where}
            ORDER BY h.{sort_col} {direction}
            LIMIT :limit OFFSET :offset
        """),
        params,
    ).fetchall()

    records = []
    for row in rows:
        item = {
            "id": row.id,
            "transfer_out_id": row.transfer_out_id,
            "transfer_out_no": row.transfer_out_no or "",
            "grn_number": row.grn_number or "",
            "grn_date": row.grn_date,
            "receiving_warehouse": row.receiving_warehouse or "",
            "from_warehouse": row.from_warehouse or "",
            "received_by": row.received_by or "",
            "received_at": row.received_at,
            "box_condition": row.box_condition,
            "condition_remarks": row.condition_remarks,
            "status": row.status or "Received",
            "inward_transaction_no": row.inward_transaction_no or None,
            "to_company": row.to_company or None,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
            "total_boxes_scanned": row.total_boxes_scanned or 0,
            "from_cold_unit": row.from_cold_unit or "",
            "lot_numbers": row.lot_numbers or "",
        }
        records.append(item)

    return {
        "records": records,
        "total": total or 0,
        "page": page,
        "per_page": per_page,
        "total_pages": ((total or 0) + per_page - 1) // per_page if total else 0,
    }


# -- Generate QR codes for a Transfer-IN --


def generate_transfer_in_qrs(transfer_in_id: int, db: Session) -> dict:
    """Generate inward_transaction_no + inward_box_id for every acknowledged box.

    Idempotent guard: returns 409 if QRs were already generated.
    Uses the same epoch-based box_id format as generate_box_ids() in inward_tools.
    """
    _ensure_interunit_schema(db)

    header = db.execute(
        text("""
            SELECT id, inward_transaction_no
            FROM interunit_transfer_in_header
            WHERE id = :id
        """),
        {"id": transfer_in_id},
    ).fetchone()

    if not header:
        raise HTTPException(404, "Transfer-IN not found")

    if header.inward_transaction_no:
        raise HTTPException(409, f"QRs already generated: {header.inward_transaction_no}")

    boxes = db.execute(
        text("""
            SELECT id, article, lot_number, batch_number, net_weight, gross_weight, line_index
            FROM interunit_transfer_in_boxes
            WHERE header_id = :hid
            ORDER BY scanned_at, id
        """),
        {"hid": transfer_in_id},
    ).fetchall()

    if not boxes:
        raise HTTPException(400, "No acknowledged boxes found for this Transfer-IN")

    inward_txn_no = f"TR-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    base = str(int(time.time() * 1000))[-8:]

    result_boxes = []
    for i, box in enumerate(boxes, start=1):
        inward_box_id = f"{base}-{i}"
        db.execute(
            text("UPDATE interunit_transfer_in_boxes SET inward_box_id = :bid WHERE id = :id"),
            {"bid": inward_box_id, "id": box.id},
        )
        result_boxes.append({
            "id": box.id,
            "box_number": i,
            "line_index": box.line_index,
            "article": box.article or "",
            "lot_number": box.lot_number or "",
            "batch_number": box.batch_number or "",
            "net_weight": float(box.net_weight) if box.net_weight is not None else 0.0,
            "gross_weight": float(box.gross_weight) if box.gross_weight is not None else 0.0,
            "inward_box_id": inward_box_id,
        })

    db.execute(
        text("UPDATE interunit_transfer_in_header SET inward_transaction_no = :txn WHERE id = :id"),
        {"txn": inward_txn_no, "id": transfer_in_id},
    )
    db.commit()

    return {"inward_transaction_no": inward_txn_no, "boxes": result_boxes}


# -- Get single transfer IN --


def get_transfer_in(transfer_in_id: int, db: Session) -> dict:
    row = db.execute(
        text("""
            SELECT h.id, h.transfer_out_id, h.transfer_out_no, h.grn_number,
                   h.grn_date, h.receiving_warehouse, h.received_by, h.received_at,
                   h.box_condition, h.condition_remarks, h.status,
                   h.inward_transaction_no,
                   h.created_at, h.updated_at,
                   t.from_site AS from_warehouse
            FROM interunit_transfer_in_header h
            LEFT JOIN interunit_transfers_header t ON h.transfer_out_id = t.id
            WHERE h.id = :tid
        """),
        {"tid": transfer_in_id},
    ).fetchone()

    if not row:
        raise HTTPException(404, "Transfer IN not found")

    boxes = _fetch_transfer_in_boxes(db, transfer_in_id)

    result = _map_transfer_in_header(row)
    result["boxes"] = boxes
    result["total_boxes_scanned"] = len(boxes)
    return result


# -- Edit transfer IN (privileged full-receipt edit) --

TRANSFER_IN_EDIT_ALLOWED_EMAILS = {"b.hrithik@candorfoods.in"}


def get_transfer_in_by_transfer_out(transfer_out_id: int, db: Session) -> dict:
    """Fetch the transfer-in (header + boxes) for a transfer-out, ANY status.
    Used to pre-fill the privileged edit form (the receive screen only holds the
    transfer-out id). Returns the latest if more than one exists."""
    row = db.execute(
        text("""
            SELECT h.id, h.transfer_out_id, h.transfer_out_no, h.grn_number,
                   h.grn_date, h.receiving_warehouse, h.received_by, h.received_at,
                   h.box_condition, h.condition_remarks, h.status,
                   h.inward_transaction_no, h.created_at, h.updated_at,
                   t.from_site AS from_warehouse
            FROM interunit_transfer_in_header h
            LEFT JOIN interunit_transfers_header t ON h.transfer_out_id = t.id
            WHERE h.transfer_out_id = :toid
            ORDER BY h.id DESC
            LIMIT 1
        """),
        {"toid": transfer_out_id},
    ).fetchone()
    if not row:
        return {"exists": False, "header": None}
    header = _map_transfer_in_header(row)
    header["boxes"] = _fetch_transfer_in_boxes(db, row.id)
    header["total_boxes_scanned"] = len(header["boxes"])
    return {"exists": True, "header": header}


def edit_transfer_in(transfer_out_id: int, data: TransferInEdit, user_email: str, db: Session) -> dict:
    """Privileged full-receipt edit. Updates the transfer-in header + boxes, and
    keeps the two other copies in sync: the source transfer-out boxes
    (interunit_transfer_boxes, via the transfer_out_box_id FK) and the destination
    cold-storage stock (cfpl/cdpl_cold_stocks, keyed by box_id + transaction_no).
    Per-field COALESCE means omitted (null) fields are left untouched; pass a value
    (including an empty string) to change one. Gated to TRANSFER_IN_EDIT_ALLOWED_EMAILS.
    """
    if (user_email or "").strip().lower() not in TRANSFER_IN_EDIT_ALLOWED_EMAILS:
        raise HTTPException(403, "You are not authorized to edit transfer-in records.")

    # Only 'Received' receipts are editable here. A Pending receipt's stock still
    # lives in pending_transfer_stock (not the destination), so editing it would
    # desync; correct a Pending receipt on the receive screen before finalizing.
    rows = db.execute(
        text("""SELECT id, status, transfer_out_id, grn_number
                FROM interunit_transfer_in_header
                WHERE transfer_out_id = :toid AND status = 'Received'
                ORDER BY id DESC"""),
        {"toid": transfer_out_id},
    ).fetchall()
    if not rows:
        raise HTTPException(404, "No 'Received' transfer-in found for this transfer (only received receipts can be edited).")
    if len(rows) > 1:
        raise HTTPException(409, "Multiple received transfer-ins exist for this transfer; resolve manually.")
    header = rows[0]
    header_id = header.id

    # ── Header fields ── (GRN number is UNIQUE — re-check on change)
    new_grn = data.grn_number.strip() if (data.grn_number and data.grn_number.strip()) else None
    if new_grn and new_grn != (header.grn_number or ""):
        clash = db.execute(
            text("SELECT 1 FROM interunit_transfer_in_header WHERE grn_number = :g AND id != :hid LIMIT 1"),
            {"g": new_grn, "hid": header_id},
        ).fetchone()
        if clash:
            raise HTTPException(409, f"GRN number '{new_grn}' is already in use.")

    db.execute(
        text("""
            UPDATE interunit_transfer_in_header SET
                grn_number = COALESCE(:grn_number, grn_number),
                receiving_warehouse = COALESCE(:receiving_warehouse, receiving_warehouse),
                box_condition = COALESCE(:box_condition, box_condition),
                condition_remarks = COALESCE(:condition_remarks, condition_remarks),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :hid
        """),
        {
            "grn_number": new_grn,
            "receiving_warehouse": data.receiving_warehouse,
            "box_condition": data.box_condition,
            "condition_remarks": data.condition_remarks,
            "hid": header_id,
        },
    )

    # Destination cold-stock tables that actually exist (for the stock sync).
    cold_tables = [t for t in ("cfpl_cold_stocks", "cdpl_cold_stocks") if _table_exists(db, t)]

    updated_boxes = 0
    affected_line_ids: set = set()
    for b in (data.boxes or []):
        if not b.box_id:
            continue
        ex = db.execute(
            text("""SELECT id, transfer_out_box_id, transaction_no
                    FROM interunit_transfer_in_boxes
                    WHERE header_id = :hid AND box_id = :bid"""),
            {"hid": header_id, "bid": b.box_id},
        ).fetchone()
        if not ex:
            continue
        params = {
            "article": b.article, "batch_number": b.batch_number,
            "lot_number": b.lot_number, "net_weight": b.net_weight,
            "gross_weight": b.gross_weight,
        }

        # 1) The transfer-in receipt box.
        db.execute(
            text("""
                UPDATE interunit_transfer_in_boxes SET
                    article = COALESCE(:article, article),
                    batch_number = COALESCE(:batch_number, batch_number),
                    lot_number = COALESCE(:lot_number, lot_number),
                    net_weight = COALESCE(:net_weight, net_weight),
                    gross_weight = COALESCE(:gross_weight, gross_weight),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
            """),
            {**params, "id": ex.id},
        )

        # 2) The source transfer-out box (singular table) via the FK link.
        #    Capture its parent line so we can re-roll the line aggregates after.
        if ex.transfer_out_box_id:
            src = db.execute(
                text("""
                    UPDATE interunit_transfer_boxes SET
                        article = COALESCE(:article, article),
                        batch_number = COALESCE(:batch_number, batch_number),
                        lot_number = COALESCE(:lot_number, lot_number),
                        net_weight = COALESCE(:net_weight, net_weight),
                        gross_weight = COALESCE(:gross_weight, gross_weight),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :sid
                    RETURNING transfer_line_id
                """),
                {**params, "sid": ex.transfer_out_box_id},
            ).fetchone()
            if src and src.transfer_line_id:
                affected_line_ids.add(src.transfer_line_id)

        # 3) The destination cold-storage stock (keyed by box_id + transaction_no;
        #    a no-op where no row matches). weight_kg AND total_inventory_kgs are
        #    kept in lock-step — the cold dashboard sums total_inventory_kgs.
        if ex.transaction_no:
            for t in cold_tables:
                cres = db.execute(
                    text(f"""
                        UPDATE {t} SET
                            lot_no = COALESCE(:lot_number, lot_no),
                            weight_kg = COALESCE(:net_weight, weight_kg),
                            total_inventory_kgs = COALESCE(:net_weight, total_inventory_kgs),
                            item_description = COALESCE(:article, item_description),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE box_id = :bid AND transaction_no = :tno
                    """),
                    {"lot_number": b.lot_number, "net_weight": b.net_weight,
                     "article": b.article, "bid": b.box_id, "tno": ex.transaction_no},
                )
                if cres.rowcount and cres.rowcount > 1:
                    logger.warning(
                        "EDIT_TRANSFER_IN: cold update matched %s rows in %s for box %s / txn %s",
                        cres.rowcount, t, b.box_id, ex.transaction_no)
        updated_boxes += 1

    # Re-roll the source transfer-out line aggregates from their (now-updated) boxes
    # so the line totals stay consistent with the edited box weights.
    for lid in affected_line_ids:
        db.execute(
            text("""
                UPDATE interunit_transfers_lines SET
                    net_weight = COALESCE(
                        (SELECT SUM(net_weight) FROM interunit_transfer_boxes WHERE transfer_line_id = :lid),
                        net_weight),
                    total_weight = COALESCE(
                        (SELECT SUM(gross_weight) FROM interunit_transfer_boxes WHERE transfer_line_id = :lid),
                        total_weight)
                WHERE id = :lid
            """),
            {"lid": lid},
        )

    db.commit()
    logger.info("EDIT_TRANSFER_IN: transfer_in=%s, transfer_out=%s, boxes=%s, lines=%s, by %s",
                header_id, transfer_out_id, updated_boxes, len(affected_line_ids), user_email)

    return get_transfer_in(header_id, db)


# -- Delete transfer IN --

TRANSFER_IN_DELETE_ALLOWED_EMAILS = {"yash@candorfoods.in"}

# Users allowed to re-open a Received transfer-in back to Pending (to correct a
# lot number / raise a box issue, then re-finalize).
TRANSFER_IN_REOPEN_ALLOWED_EMAILS = {"b.hrithik@candorfoods.in"}


def reopen_transfer_in(transfer_out_id: int, user_email: str, db: Session) -> dict:
    """Re-open a Received transfer-in (looked up by its Transfer OUT id) back to
    Pending.

    Non-destructive: the acknowledged boxes (interunit_transfer_in_boxes) are
    KEPT so the user can un-acknowledge a box, change its lot number / raise an
    issue, and then re-finalize. The receipt's stock movement is reversed — the
    destination rows are removed and the stock is restored to
    pending_transfer_stock (In Transit) — and the Transfer OUT is reverted to
    'Dispatch', exactly so a subsequent finalize re-picks the stock cleanly.
    Keyed by transfer_out_id (1:1 with the receipt) so the receive screen, which
    only holds the transfer-out id, can call it directly.
    Gated to TRANSFER_IN_REOPEN_ALLOWED_EMAILS.
    """
    if (user_email or "").strip().lower() not in TRANSFER_IN_REOPEN_ALLOWED_EMAILS:
        raise HTTPException(403, "You are not authorized to re-open transfer-in records.")

    headers = db.execute(
        text("""SELECT id, status, transfer_out_id, transfer_out_no, grn_number
                FROM interunit_transfer_in_header
                WHERE transfer_out_id = :toid AND status = 'Received'
                ORDER BY id DESC"""),
        {"toid": transfer_out_id},
    ).fetchall()
    if not headers:
        raise HTTPException(404, "No 'Received' transfer-in found for this transfer.")
    if len(headers) > 1:
        # Don't silently reopen one of several — a human must resolve which receipt.
        raise HTTPException(409, "Multiple received transfer-ins exist for this transfer; resolve manually.")
    header = headers[0]
    transfer_in_id = header.id

    box_count = db.execute(
        text("SELECT COUNT(*) FROM interunit_transfer_in_boxes WHERE header_id = :hid"),
        {"hid": transfer_in_id},
    ).scalar() or 0

    # Reverse the receipt's stock movement: destination -> pending_transfer_stock.
    # Boxes are intentionally NOT deleted (unlike delete_transfer_in) so the user
    # resumes from their acknowledged state.
    restored = unpick_to_pending(transfer_in_id=transfer_in_id, transfer_out_id=transfer_out_id, db=db)

    # Legacy / partial receipts (e.g. finalized before pending_transfer_stock existed,
    # or boxes with missing keys) cannot be cleanly reversed — re-parking fewer rows
    # than there are boxes risks a stock mismatch, so refuse rather than corrupt stock.
    if box_count > 0 and (restored or 0) < box_count:
        db.rollback()
        raise HTTPException(
            409,
            "This receipt can't be safely re-opened (legacy or partial stock mapping). "
            "Delete and re-create it instead.",
        )

    updated = db.execute(
        text("""
            UPDATE interunit_transfer_in_header
            SET status = 'Pending',
                received_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :hid
            RETURNING id, transfer_out_id, transfer_out_no, grn_number, grn_date,
                      receiving_warehouse, received_by, received_at,
                      box_condition, condition_remarks, status,
                      created_at, updated_at
        """),
        {"hid": transfer_in_id},
    ).fetchone()

    # Revert the Transfer OUT back to 'Dispatch' (in-transit) so re-finalize works.
    db.execute(
        text("UPDATE interunit_transfers_header SET status = 'Dispatch' WHERE id = :toid"),
        {"toid": transfer_out_id},
    )

    db.commit()

    logger.info("REOPEN_TRANSFER_IN: transfer_in=%s, transfer_out=%s, grn=%s, reopened by %s",
                transfer_in_id, transfer_out_id, header.grn_number, user_email)

    result = _map_transfer_in_header(updated)
    result["boxes"] = _fetch_transfer_in_boxes(db, transfer_in_id)
    result["total_boxes_scanned"] = len(result["boxes"])
    return result


def delete_transfer_in(transfer_in_id: int, user_email: str, db: Session) -> dict:
    """Delete a Transfer IN and its related data from all tables."""
    if user_email not in TRANSFER_IN_DELETE_ALLOWED_EMAILS:
        raise HTTPException(403, "You are not authorized to delete transfer-in records.")

    # Fetch header to get transfer_out_id and challan_no
    header = db.execute(
        text("""SELECT h.id, h.grn_number, h.transfer_out_id, h.transfer_out_no
                FROM interunit_transfer_in_header h
                WHERE h.id = :tid"""),
        {"tid": transfer_in_id},
    ).fetchone()

    if not header:
        raise HTTPException(404, "Transfer IN not found")

    grn_number = header.grn_number
    transfer_out_id = header.transfer_out_id

    # Reverse the receive: remove from destination, restore boxes to pending_transfer_stock
    if transfer_out_id:
        unpick_to_pending(transfer_in_id=transfer_in_id, transfer_out_id=transfer_out_id, db=db)

    # Count for log
    in_boxes = db.execute(
        text("SELECT box_id FROM interunit_transfer_in_boxes WHERE header_id = :hid"),
        {"hid": transfer_in_id},
    ).fetchall()

    # Delete from interunit_transfer_in_boxes
    db.execute(
        text("DELETE FROM interunit_transfer_in_boxes WHERE header_id = :hid"),
        {"hid": transfer_in_id},
    )

    # Delete from interunit_transfer_in_header
    db.execute(
        text("DELETE FROM interunit_transfer_in_header WHERE id = :tid"),
        {"tid": transfer_in_id},
    )

    # Revert Transfer OUT status back to 'Dispatch'
    if transfer_out_id:
        db.execute(
            text("UPDATE interunit_transfers_header SET status = 'Dispatch' WHERE id = :toid"),
            {"toid": transfer_out_id},
        )

    db.commit()

    logger.info("DELETE_TRANSFER_IN: id=%s, grn=%s, deleted by %s, boxes=%d",
                transfer_in_id, grn_number, user_email, len(in_boxes))

    return {
        "status": "success",
        "message": f"Transfer IN {grn_number} deleted successfully.",
    }


# ----------------------------------------------
#  All SKU Lookup (for Transfer & Request article section)
# ----------------------------------------------

_CATEGORIAL_TABLE = "public.all_sku"


def categorial_global_search(
    search: Optional[str],
    limit: int,
    offset: int,
    db: Session,
) -> CategorialSearchResponse:
    """Global search on all_sku.particulars   bypasses hierarchy."""
    search_term = search.strip() if search else None

    where_clauses = ["1=1"]
    params: dict = {}

    if search_term:
        where_clauses.append('LOWER(particulars) LIKE :search')
        params["search"] = f"%{search_term.lower()}%"

    where_sql = " AND ".join(where_clauses)

    total = db.execute(
        text(f"SELECT COUNT(*) FROM (SELECT DISTINCT UPPER(particulars), UPPER(item_type) FROM {_CATEGORIAL_TABLE} WHERE {where_sql}) t"),
        params,
    ).scalar_one()

    # DISTINCT on (particulars + material_type) so all FG/RM/PM variants are returned
    rows = db.execute(
        text(f"""
            SELECT desc_upper, mt, grp, sc, uom
            FROM (
                SELECT DISTINCT ON (UPPER(particulars), UPPER(item_type))
                       UPPER(particulars) AS desc_upper,
                       UPPER(item_type) AS mt,
                       UPPER(item_group) AS grp,
                       UPPER(sub_group) AS sc,
                       uom
                FROM {_CATEGORIAL_TABLE}
                WHERE {where_sql}
                ORDER BY UPPER(particulars) ASC, UPPER(item_type) ASC
            ) sub
            ORDER BY
                CASE LOWER(sub.mt)
                    WHEN 'rm' THEN 1
                    WHEN 'fg' THEN 2
                    WHEN 'pm' THEN 3
                    ELSE 4
                END,
                sub.desc_upper ASC
            LIMIT :limit OFFSET :offset
        """),
        {**params, "limit": limit, "offset": offset},
    ).fetchall()

    items = [
        CategorialSearchItem(
            id=idx + 1 + offset,
            item_description=r[0] or "",
            material_type=r[1],
            group=r[2],
            sub_group=r[3],
            uom=float(r[4]) if r[4] is not None else None,
        )
        for idx, r in enumerate(rows)
    ]

    return CategorialSearchResponse(
        items=items,
        meta={
            "total_items": total,
            "limit": limit,
            "offset": offset,
            "search": search_term,
            "has_more": (offset + limit) < total,
        },
    )


def categorial_dropdown(
    material_type: Optional[str],
    item_category: Optional[str],
    sub_category: Optional[str],
    search: Optional[str],
    limit: int,
    offset: int,
    db: Session,
) -> CategorialDropdownResponse:
    """Cascading dropdown on all_sku: item_type -> item_group -> sub_group -> particulars."""
    material_type = material_type.strip() if material_type else None
    item_category = item_category.strip() if item_category else None
    sub_category = sub_category.strip() if sub_category else None
    search = search.strip() if search else None

    # 1) All material types   sorted by priority: rm ? fg ? pm (only RM, PM, FG)
    material_types = db.execute(
        text(f"""
            SELECT mt FROM (
                SELECT DISTINCT UPPER(item_type) AS mt FROM {_CATEGORIAL_TABLE}
                WHERE item_type IS NOT NULL
                  AND UPPER(item_type) IN ('RM', 'PM', 'FG')
            ) sub
            ORDER BY
                CASE LOWER(sub.mt)
                    WHEN 'rm' THEN 1
                    WHEN 'fg' THEN 2
                    WHEN 'pm' THEN 3
                    ELSE 4
                END
        """)
    ).scalars().all()

    # 2) Item categories (groups) filtered by material_type (case-insensitive dedup)
    item_categories = []
    if material_type:
        item_categories = db.execute(
            text(f"""
                SELECT DISTINCT UPPER(item_group) AS grp FROM {_CATEGORIAL_TABLE}
                WHERE UPPER(item_type) = UPPER(:mt) AND item_group IS NOT NULL
                ORDER BY grp ASC
            """),
            {"mt": material_type},
        ).scalars().all()

    # 3) Sub categories filtered by material_type + group (case-insensitive dedup)
    sub_categories = []
    if material_type and item_category:
        sub_categories = db.execute(
            text(f"""
                SELECT DISTINCT UPPER(sub_group) AS sc FROM {_CATEGORIAL_TABLE}
                WHERE UPPER(item_type) = UPPER(:mt)
                  AND UPPER(item_group) = UPPER(:ic)
                  AND sub_group IS NOT NULL
                ORDER BY sc ASC
            """),
            {"mt": material_type, "ic": item_category},
        ).scalars().all()

    # 4) Item descriptions + uom (filtered by full hierarchy, case-insensitive dedup)
    item_descs: list[str] = []
    uom_values: list = []
    total_item_descriptions = 0

    if material_type and item_category and sub_category:
        where = [
            'UPPER(item_type) = UPPER(:mt)',
            'UPPER(item_group) = UPPER(:ic)',
            "UPPER(sub_group) = UPPER(:sc)",
        ]
        params: dict = {"mt": material_type, "ic": item_category, "sc": sub_category}

        if search:
            where.append("LOWER(particulars) LIKE :search")
            params["search"] = f"%{search.lower()}%"

        where_sql = " AND ".join(where)

        total_item_descriptions = db.execute(
            text(f"SELECT COUNT(DISTINCT UPPER(particulars)) FROM {_CATEGORIAL_TABLE} WHERE {where_sql}"),
            params,
        ).scalar_one()

        rows = db.execute(
            text(f"""
                SELECT desc_upper, uom FROM (
                    SELECT DISTINCT ON (UPPER(particulars))
                           UPPER(particulars) AS desc_upper, uom
                    FROM {_CATEGORIAL_TABLE}
                    WHERE {where_sql} AND particulars IS NOT NULL
                    ORDER BY UPPER(particulars) ASC
                ) sub
                ORDER BY sub.desc_upper ASC
                LIMIT :limit OFFSET :offset
            """),
            {**params, "limit": limit, "offset": offset},
        ).fetchall()

        item_descs = [r[0] for r in rows]
        uom_values = [float(r[1]) if r[1] is not None else None for r in rows]

    return CategorialDropdownResponse(
        selected={
            "material_type": material_type,
            "item_category": item_category,
            "sub_category": sub_category,
        },
        options=CategorialDropdownOptions(
            material_types=material_types,
            item_categories=item_categories,
            sub_categories=sub_categories,
            item_descriptions=item_descs,
            uom_values=uom_values,
        ),
        meta=CategorialDropdownMeta(
            total_material_types=len(material_types),
            total_item_descriptions=total_item_descriptions,
            total_categories=len(item_categories),
            total_sub_categories=len(sub_categories),
            limit=limit,
            offset=offset,
            search=search,
        ),
    )
