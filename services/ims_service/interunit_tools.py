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
)

logger = get_logger("ims.interunit")


# ── Helpers ──


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


# ── Warehouse dropdown ──


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


# ── Create request ──


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


# ── List requests ──


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


# ── Get single request ──


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


# ── Update request (Accept / Reject) ──


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


# ── Delete request ──


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


# ══════════════════════════════════════════════
#  Phase B – Transfer helpers
# ══════════════════════════════════════════════


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
    rows = db.execute(
        text("""
            SELECT id, header_id, transfer_line_id, box_number, box_id, article,
                   lot_number, batch_number, transaction_no,
                   net_weight, gross_weight, created_at, updated_at
            FROM interunit_transfer_boxes
            WHERE header_id = :hid
            ORDER BY box_number
        """),
        {"hid": header_id},
    ).fetchall()
    logger.info("FETCH_BOXES_DEBUG: header_id=%s, row_count=%d", header_id, len(rows))
    for r in rows:
        logger.info("FETCH_BOXES_DEBUG: row id=%s, box_id=%r, box_number=%s", r.id, r.box_id, r.box_number)
    result = [_map_box_row(r) for r in rows]
    logger.info("FETCH_BOXES_DEBUG: mapped result box_ids=%s", [b["box_id"] for b in result])
    return result


# ── Create transfer ──


def create_transfer(data: TransferCreate, created_by: str, db: Session) -> dict:
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

    # Insert boxes (if provided)
    boxes = []
    if data.boxes:
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
    # Each line represents one box entry — no need to sum box weights back into lines.

    # Determine status based on box count vs expected qty
    if boxes:
        total_expected = sum(int(l.qty) for l in lines)
        actual_scanned = len(boxes)
        transfer_status = "Dispatch" if actual_scanned >= total_expected else "Partial"

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


# ── List transfers ──


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
    clauses = ["1=1"]
    params: dict = {}

    if status:
        clauses.append("h.status = :status")
        params["status"] = status
    if from_site:
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
                r.request_no,
                COALESCE(lc.items_count, 0) AS items_count,
                COALESCE(bc.boxes_count, 0) AS boxes_count,
                COALESCE(lc.total_qty, 0) AS total_qty
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
        records.append(item)

    return {
        "records": records,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if total else 0,
    }


# ── Bulk entry box lookup ──


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


# ── Get single transfer ──


def get_transfer(transfer_id: int, db: Session) -> dict:
    row = db.execute(
        text("""
            SELECT h.id, h.challan_no, h.stock_trf_date, h.from_site, h.to_site,
                   h.vehicle_no, h.driver_name, h.approved_by, h.remark,
                   h.reason_code, h.status, h.request_id, h.created_by,
                   h.created_ts, h.approved_ts, h.has_variance,
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
    logger.info(
        "GET_TRANSFER_DEBUG: transfer_id=%s, boxes_count=%d, box_ids_in_response=%s",
        transfer_id, len(result["boxes"]),
        [b.get("box_id") for b in result["boxes"]],
    )
    return result


# ── Update transfer ──


def update_transfer(transfer_id: int, data: TransferCreate, db: Session) -> dict:
    """Update an existing transfer by replacing header, lines, and boxes."""
    existing = db.execute(
        text("SELECT id, challan_no, status, request_id FROM interunit_transfers_header WHERE id = :tid"),
        {"tid": transfer_id},
    ).fetchone()

    if not existing:
        raise HTTPException(404, "Transfer not found")

    # No status restriction — authorized users can edit transfers in any status

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

    # Insert boxes (if provided)
    boxes = []
    if data.boxes:
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
    # Each line represents one box entry — no need to sum box weights back into lines.

    # Determine status based on box count vs expected qty
    if boxes:
        total_expected = sum(int(l.qty) for l in lines)
        actual_scanned = len(boxes)
        transfer_status = "Dispatch" if actual_scanned >= total_expected else "Partial"

        db.execute(
            text("""
                UPDATE interunit_transfers_header
                SET status = :status
                WHERE id = :hid
            """),
            {"status": transfer_status, "hid": header_id},
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


# ── Delete transfer ──


def delete_transfer(transfer_id: int, db: Session) -> dict:
    existing = db.execute(
        text("SELECT id, challan_no, status FROM interunit_transfers_header WHERE id = :tid"),
        {"tid": transfer_id},
    ).fetchone()

    if not existing:
        raise HTTPException(404, "Transfer not found")

    # No status restriction — authorized users can delete transfers in any status

    # Delete associated transfer-in records first (cascade)
    transfer_in_headers = db.execute(
        text("SELECT id FROM interunit_transfer_in_header WHERE transfer_out_id = :tid"),
        {"tid": transfer_id},
    ).fetchall()

    for ti in transfer_in_headers:
        # Restore cold stock snapshots (cold-to-warehouse: rows subtracted during finalize)
        _restore_cold_stock_snapshots(ti.id, transfer_id, db)

        # Delete cold_stocks entries inserted during warehouse-to-cold transfer-in
        in_boxes = db.execute(
            text("SELECT box_id, transaction_no FROM interunit_transfer_in_boxes WHERE header_id = :hid"),
            {"hid": ti.id},
        ).fetchall()
        for cs_table in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
            tbl_exists = db.execute(text("SELECT to_regclass(:t)"), {"t": f"public.{cs_table}"}).scalar()
            if not tbl_exists:
                continue
            for bx in in_boxes:
                if bx.box_id and bx.transaction_no:
                    db.execute(
                        text(f"DELETE FROM {cs_table} WHERE box_id = :bid AND transaction_no = :txno"),
                        {"bid": bx.box_id, "txno": bx.transaction_no},
                    )
            # Also delete by inward_no (challan_no) to catch entries without box_id/transaction_no
            if existing.challan_no:
                db.execute(
                    text(f"DELETE FROM {cs_table} WHERE inward_no = :challan"),
                    {"challan": existing.challan_no},
                )
        # Delete boxes
        db.execute(
            text("DELETE FROM interunit_transfer_in_boxes WHERE header_id = :hid"),
            {"hid": ti.id},
        )

    # If no transfer-in existed, still try restoring snapshots by transfer_out_id
    if not transfer_in_headers:
        _restore_cold_stock_snapshots(None, transfer_id, db)

    db.execute(
        text("DELETE FROM interunit_transfer_in_header WHERE transfer_out_id = :tid"),
        {"tid": transfer_id},
    )

    # Delete transfer-out in FK order: boxes → lines → header
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


# ══════════════════════════════════════════════
#  Phase C – Transfer IN helpers
# ══════════════════════════════════════════════


def _map_transfer_in_header(row) -> dict:
    return {
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
    }


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
    }


def _fetch_transfer_in_boxes(db: Session, header_id: int) -> list:
    rows = db.execute(
        text("""
            SELECT id, header_id, box_id, article, batch_number,
                   lot_number, transaction_no, net_weight, gross_weight,
                   scanned_at, is_matched, transfer_out_box_id, issue, line_index
            FROM interunit_transfer_in_boxes
            WHERE header_id = :hid
            ORDER BY scanned_at
        """),
        {"hid": header_id},
    ).fetchall()
    return [_map_transfer_in_box(r) for r in rows]


# ── Create transfer IN (GRN) ──


def create_transfer_in(data: TransferInCreate, db: Session) -> dict:
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
                 :box_condition, :condition_remarks, 'Received')
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

    # Insert cold storage items directly into cfpl/cdpl_cold_stocks
    if data.cold_storage_items:
        to_site_val = transfer_out.to_site if hasattr(transfer_out, 'to_site') else None
        _insert_cold_storage_items(header_id, data.cold_storage_items, transfer_out.challan_no, db, to_site=to_site_val)

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

    # Update Transfer OUT status to 'Received'
    db.execute(
        text("""
            UPDATE interunit_transfers_header
            SET status = 'Received'
            WHERE id = :toid
        """),
        {"toid": data.transfer_out_id},
    )

    result = _map_transfer_in_header(header)
    result["boxes"] = [_map_transfer_in_box(b) for b in boxes]
    result["total_boxes_scanned"] = len(boxes)
    return result


# ── Cold storage helper (shared by create_transfer_in & finalize) ──


def _restore_cold_stock_snapshots(transfer_in_id: Optional[int], transfer_out_id: Optional[int], db: Session):
    """Restore cold stock rows from snapshots (saved during finalize_transfer_in).
    Used when deleting transfer-in or transfer-out records."""
    logger.info(
        "COLD_STOCK_RESTORE: Called with transfer_in_id=%s, transfer_out_id=%s",
        transfer_in_id, transfer_out_id,
    )

    snapshot_table_exists = db.execute(
        text("SELECT to_regclass('public.cold_stock_snapshots')")
    ).scalar()
    if not snapshot_table_exists:
        logger.info("COLD_STOCK_RESTORE: Snapshot table does not exist, skipping")
        return

    # Find snapshots by transfer_in_header_id or transfer_out_id
    clauses = []
    params: dict = {}
    if transfer_in_id:
        clauses.append("transfer_in_header_id = :ti_id")
        params["ti_id"] = transfer_in_id
    if transfer_out_id:
        clauses.append("transfer_out_id = :to_id")
        params["to_id"] = transfer_out_id
    if not clauses:
        return

    where = " OR ".join(clauses)
    snapshots = db.execute(
        text(f"SELECT * FROM cold_stock_snapshots WHERE {where}"),
        params,
    ).fetchall()

    logger.info("COLD_STOCK_RESTORE: Found %d snapshot(s) matching query", len(snapshots))

    if not snapshots:
        return

    for snap in snapshots:
        cold_table = snap.source_table
        logger.info(
            "COLD_STOCK_RESTORE: Processing snapshot — table=%s, box_id=%s, item=%s",
            cold_table, snap.box_id, snap.item_description,
        )

        # Verify table exists
        tbl_exists = db.execute(
            text("SELECT to_regclass(:t)"), {"t": f"public.{cold_table}"}
        ).scalar()
        if not tbl_exists:
            logger.warning("COLD_STOCK_RESTORE: Table %s does not exist, skipping", cold_table)
            continue

        # Check if already exists (avoid duplicates) — handle NULL transaction_no
        if snap.transaction_no:
            exists = db.execute(
                text(f"SELECT id FROM {cold_table} WHERE box_id = :bid AND transaction_no = :txno"),
                {"bid": snap.box_id, "txno": snap.transaction_no},
            ).fetchone()
        else:
            exists = db.execute(
                text(f"SELECT id FROM {cold_table} WHERE box_id = :bid"),
                {"bid": snap.box_id},
            ).fetchone()

        if exists:
            logger.info("COLD_STOCK_RESTORE: Already exists in %s, skipping box_id=%s", cold_table, snap.box_id)
            continue

        # Re-insert the full original cold stock row from snapshot
        db.execute(
            text(f"""
                INSERT INTO {cold_table}
                    (inward_dt, unit, inward_no, item_description, item_mark, vakkal,
                     lot_no, no_of_cartons, weight_kg, total_inventory_kgs,
                     group_name, item_subgroup, storage_location,
                     exporter, last_purchase_rate, value,
                     box_id, transaction_no, spl_remarks)
                VALUES
                    (:inward_dt, :unit, :inward_no, :item_description, :item_mark, :vakkal,
                     :lot_no, :no_of_cartons, :weight_kg, :total_inventory_kgs,
                     :group_name, :item_subgroup, :storage_location,
                     :exporter, :last_purchase_rate, :value,
                     :box_id, :transaction_no, :spl_remarks)
            """),
            {
                "inward_dt": snap.inward_dt,
                "unit": snap.unit,
                "inward_no": snap.inward_no,
                "item_description": snap.item_description,
                "item_mark": snap.item_mark,
                "vakkal": snap.vakkal,
                "lot_no": snap.lot_no,
                "no_of_cartons": snap.no_of_cartons,
                "weight_kg": snap.weight_kg,
                "total_inventory_kgs": snap.total_inventory_kgs,
                "group_name": snap.group_name,
                "item_subgroup": snap.item_subgroup,
                "storage_location": snap.storage_location,
                "exporter": snap.exporter,
                "last_purchase_rate": snap.last_purchase_rate,
                "value": snap.value,
                "box_id": snap.box_id,
                "transaction_no": snap.transaction_no,
                "spl_remarks": snap.spl_remarks,
            },
        )
        logger.info(
            "COLD_STOCK_RESTORE: Restored to %s — box_id=%s, transaction_no=%s, item=%s",
            cold_table, snap.box_id, snap.transaction_no, snap.item_description,
        )

    # Clean up snapshots after restoration
    db.execute(
        text(f"DELETE FROM cold_stock_snapshots WHERE {where}"),
        params,
    )
    logger.info("COLD_STOCK_RESTORE: Cleaned up %d snapshot(s)", len(snapshots))


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
            # Per-box insert: one row per box in cold stocks
            num_boxes = len(cs_item.box_details)
            rate_per_box = (cs_item.rate / num_boxes) if (cs_item.rate and num_boxes > 0) else cs_item.rate
            value_per_box = (cs_item.value / num_boxes) if (cs_item.value and num_boxes > 0) else cs_item.value

            for box_detail in cs_item.box_details:
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
            # No box details — insert a single summary row
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


# ── Pending Transfer IN (Phase C - real-time acknowledge) ──


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


def acknowledge_pending_box(header_id: int, data: PendingBoxAcknowledge, db: Session) -> dict:
    """UPSERT a single box/article into a pending transfer-in."""
    # Verify header exists and is Pending
    header = db.execute(
        text("SELECT id, status FROM interunit_transfer_in_header WHERE id = :hid"),
        {"hid": header_id},
    ).fetchone()
    if not header:
        raise HTTPException(404, "Transfer IN header not found")
    if header.status != "Pending":
        raise HTTPException(400, "Transfer IN is not in Pending status")

    issue_json = json.dumps(data.issue) if data.issue else None

    # Atomic upsert — safe against concurrent clients acknowledging the same box
    # First ensure a unique constraint exists (idempotent)
    try:
        db.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_transfer_in_boxes_header_box
            ON interunit_transfer_in_boxes (header_id, box_id)
        """))
    except Exception:
        pass  # Index may already exist or table doesn't support it

    row = db.execute(
        text("""
            INSERT INTO interunit_transfer_in_boxes
                (header_id, box_id, article, batch_number, lot_number,
                 transaction_no, net_weight, gross_weight,
                 scanned_at, is_matched, transfer_out_box_id, issue, line_index)
            VALUES
                (:header_id, :box_id, :article, :batch_number, :lot_number,
                 :transaction_no, :net_weight, :gross_weight,
                 CURRENT_TIMESTAMP, :is_matched, :transfer_out_box_id, :issue, :line_index)
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
                scanned_at = CURRENT_TIMESTAMP
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
        },
    ).fetchone()

    return _map_transfer_in_box(row)


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
    """Batch acknowledge multiple boxes in a pending transfer-in."""
    header = db.execute(
        text("SELECT id, status FROM interunit_transfer_in_header WHERE id = :hid"),
        {"hid": header_id},
    ).fetchone()
    if not header:
        raise HTTPException(404, "Transfer IN header not found")
    if header.status != "Pending":
        raise HTTPException(400, "Transfer IN is not in Pending status")

    results = []
    for box_data in boxes:
        issue_json = json.dumps(box_data.issue) if box_data.issue else None

        row = db.execute(
            text("""
                INSERT INTO interunit_transfer_in_boxes
                    (header_id, box_id, article, batch_number, lot_number,
                     transaction_no, net_weight, gross_weight,
                     scanned_at, is_matched, transfer_out_box_id, issue, line_index)
                VALUES
                    (:header_id, :box_id, :article, :batch_number, :lot_number,
                     :transaction_no, :net_weight, :gross_weight,
                     CURRENT_TIMESTAMP, :is_matched, :transfer_out_box_id, :issue, :line_index)
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
                    scanned_at = CURRENT_TIMESTAMP
                RETURNING id, header_id, box_id, article, batch_number,
                          lot_number, transaction_no, net_weight, gross_weight,
                          scanned_at, is_matched, transfer_out_box_id, issue, line_index
            """),
            {
                "header_id": header_id,
                "box_id": box_data.box_id,
                "article": box_data.article,
                "batch_number": box_data.batch_number,
                "lot_number": box_data.lot_number,
                "transaction_no": box_data.transaction_no,
                "net_weight": box_data.net_weight,
                "gross_weight": box_data.gross_weight,
                "is_matched": box_data.is_matched,
                "transfer_out_box_id": box_data.transfer_out_box_id,
                "issue": issue_json,
                "line_index": box_data.line_index,
            },
        ).fetchone()

        results.append(_map_transfer_in_box(row))

    return {"success": True, "count": len(results), "boxes": results}


def finalize_transfer_in(header_id: int, data: FinalizeTransferIn, db: Session) -> dict:
    """Finalize a Pending transfer-in: transition status to Received."""
    header = db.execute(
        text("""
            SELECT id, status, transfer_out_id, transfer_out_no
            FROM interunit_transfer_in_header WHERE id = :hid
        """),
        {"hid": header_id},
    ).fetchone()
    if not header:
        raise HTTPException(404, "Transfer IN header not found")
    if header.status != "Pending":
        raise HTTPException(400, "Transfer IN is not in Pending status")

    # Verify at least 1 box exists
    box_count = db.execute(
        text("SELECT COUNT(*) FROM interunit_transfer_in_boxes WHERE header_id = :hid"),
        {"hid": header_id},
    ).scalar()
    if box_count == 0:
        raise HTTPException(400, "No boxes/articles acknowledged. Cannot finalize.")

    # Update header to Received
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
        {
            "hid": header_id,
            "box_condition": data.box_condition,
            "condition_remarks": data.condition_remarks,
        },
    ).fetchone()

    # Process cold storage items if provided
    if data.cold_storage_items:
        # Fetch to_site from transfer-out header for storage_location autofill
        tout = db.execute(
            text("SELECT to_site FROM interunit_transfers_header WHERE id = :id"),
            {"id": header.transfer_out_id},
        ).fetchone()
        to_site = tout.to_site if tout else None
        _insert_cold_storage_items(header_id, data.cold_storage_items, header.transfer_out_no, db, to_site=to_site)

    # ── Subtract from cold stocks when transfer is FROM Cold Storage ──
    transfer_out = db.execute(
        text("SELECT from_site, to_site FROM interunit_transfers_header WHERE id = :toid"),
        {"toid": header.transfer_out_id},
    ).fetchone()

    cold_storage_names = ["cold storage", "rishi cold", "savla d-39 cold", "savla d-514 cold"]
    from_site_lower = (transfer_out.from_site or "").strip().lower() if transfer_out else ""

    if from_site_lower in cold_storage_names:
        # Ensure snapshot table exists for restoration on delete
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS cold_stock_snapshots (
                id SERIAL PRIMARY KEY,
                transfer_out_id INTEGER,
                transfer_in_header_id INTEGER,
                source_table VARCHAR(50),
                original_id INTEGER,
                inward_dt VARCHAR(50), unit VARCHAR(50), inward_no VARCHAR(100),
                item_description VARCHAR(500), item_mark VARCHAR(255), vakkal VARCHAR(255),
                lot_no VARCHAR(100), no_of_cartons NUMERIC(12,3), weight_kg NUMERIC(12,3),
                total_inventory_kgs NUMERIC(12,3), group_name VARCHAR(100),
                item_subgroup VARCHAR(100), storage_location VARCHAR(255),
                exporter VARCHAR(255), last_purchase_rate NUMERIC(12,3),
                value NUMERIC(12,3), box_id VARCHAR(255), transaction_no VARCHAR(255),
                spl_remarks TEXT, created_at TIMESTAMP DEFAULT NOW()
            )
        """))

        # Get all boxes from the transfer-out that have box_id (cold stock refs)
        out_boxes = db.execute(
            text("""
                SELECT box_id, transaction_no, article, net_weight
                FROM interunit_transfer_boxes
                WHERE header_id = :toid
                  AND box_id IS NOT NULL AND box_id != ''
            """),
            {"toid": header.transfer_out_id},
        ).fetchall()

        logger.info(
            "COLD_STOCK_SUBTRACT: from_site=%s, transfer_out_id=%s, out_boxes_count=%d, box_ids=%s",
            transfer_out.from_site, header.transfer_out_id, len(out_boxes),
            [b.box_id for b in out_boxes],
        )

        if out_boxes:
            cold_tables = ["cfpl_cold_stocks", "cdpl_cold_stocks"]
            for box in out_boxes:
                for cold_table in cold_tables:
                    # Try matching by box_id + transaction_no first, then box_id only
                    if box.transaction_no:
                        original = db.execute(
                            text(f"SELECT * FROM {cold_table} WHERE box_id = :box_id AND transaction_no = :txn_no LIMIT 1"),
                            {"box_id": box.box_id, "txn_no": box.transaction_no},
                        ).fetchone()
                    else:
                        original = None

                    # Fallback: match by box_id only
                    if not original:
                        original = db.execute(
                            text(f"SELECT * FROM {cold_table} WHERE box_id = :box_id LIMIT 1"),
                            {"box_id": box.box_id},
                        ).fetchone()

                    if not original:
                        logger.info("COLD_STOCK_SUBTRACT: No match in %s for box_id=%s", cold_table, box.box_id)
                        continue

                    # Save snapshot of the full row before deleting
                    db.execute(
                        text("""
                            INSERT INTO cold_stock_snapshots
                                (transfer_out_id, transfer_in_header_id, source_table, original_id,
                                 inward_dt, unit, inward_no, item_description, item_mark, vakkal,
                                 lot_no, no_of_cartons, weight_kg, total_inventory_kgs,
                                 group_name, item_subgroup, storage_location,
                                 exporter, last_purchase_rate, value,
                                 box_id, transaction_no, spl_remarks)
                            VALUES
                                (:transfer_out_id, :transfer_in_header_id, :source_table, :original_id,
                                 :inward_dt, :unit, :inward_no, :item_description, :item_mark, :vakkal,
                                 :lot_no, :no_of_cartons, :weight_kg, :total_inventory_kgs,
                                 :group_name, :item_subgroup, :storage_location,
                                 :exporter, :last_purchase_rate, :value,
                                 :box_id, :transaction_no, :spl_remarks)
                        """),
                        {
                            "transfer_out_id": header.transfer_out_id,
                            "transfer_in_header_id": header_id,
                            "source_table": cold_table,
                            "original_id": original.id,
                            "inward_dt": getattr(original, "inward_dt", None),
                            "unit": getattr(original, "unit", None),
                            "inward_no": getattr(original, "inward_no", None),
                            "item_description": getattr(original, "item_description", None),
                            "item_mark": getattr(original, "item_mark", None),
                            "vakkal": getattr(original, "vakkal", None),
                            "lot_no": getattr(original, "lot_no", None),
                            "no_of_cartons": getattr(original, "no_of_cartons", None),
                            "weight_kg": getattr(original, "weight_kg", None),
                            "total_inventory_kgs": getattr(original, "total_inventory_kgs", None),
                            "group_name": getattr(original, "group_name", None),
                            "item_subgroup": getattr(original, "item_subgroup", None),
                            "storage_location": getattr(original, "storage_location", None),
                            "exporter": getattr(original, "exporter", None),
                            "last_purchase_rate": getattr(original, "last_purchase_rate", None),
                            "value": getattr(original, "value", None),
                            "box_id": box.box_id,
                            "transaction_no": getattr(original, "transaction_no", None),
                            "spl_remarks": getattr(original, "spl_remarks", None),
                        },
                    )

                    # Delete the cold stock row by its primary id
                    db.execute(
                        text(f"DELETE FROM {cold_table} WHERE id = :orig_id"),
                        {"orig_id": original.id},
                    )
                    logger.info(
                        "COLD_STOCK_SUBTRACT: Snapshot saved & deleted from %s — box_id=%s, article=%s",
                        cold_table, box.box_id, box.article,
                    )
                    break  # Found in this table, move to next box

    # Update Transfer OUT status to 'Received'
    db.execute(
        text("UPDATE interunit_transfers_header SET status = 'Received' WHERE id = :toid"),
        {"toid": header.transfer_out_id},
    )

    result = _map_transfer_in_header(updated)
    result["boxes"] = _fetch_transfer_in_boxes(db, header_id)
    result["total_boxes_scanned"] = len(result["boxes"])
    return result


def get_pending_by_transfer_out(transfer_out_id: int, db: Session) -> dict:
    """Lookup pending transfer-in header + boxes by transfer_out_id."""
    row = db.execute(
        text("""
            SELECT id, transfer_out_id, transfer_out_no, grn_number,
                   grn_date, receiving_warehouse, received_by, received_at,
                   box_condition, condition_remarks, status,
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


# ── List transfer INs ──


def list_transfer_ins(
    page: int,
    per_page: int,
    receiving_warehouse: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    sort_by: str,
    sort_order: str,
    db: Session,
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
                COUNT(b.id) AS total_boxes_scanned
            FROM interunit_transfer_in_header h
            LEFT JOIN interunit_transfer_in_boxes b ON h.id = b.header_id
            WHERE {where}
            GROUP BY h.id
            ORDER BY h.{sort_by} {direction}
            LIMIT :limit OFFSET :offset
        """),
        params,
    ).fetchall()

    records = []
    for row in rows:
        item = _map_transfer_in_header(row)
        item["total_boxes_scanned"] = row.total_boxes_scanned or 0
        records.append(item)

    return {
        "records": records,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if total else 0,
    }


# ── Get single transfer IN ──


def get_transfer_in(transfer_in_id: int, db: Session) -> dict:
    row = db.execute(
        text("""
            SELECT id, transfer_out_id, transfer_out_no, grn_number,
                   grn_date, receiving_warehouse, received_by, received_at,
                   box_condition, condition_remarks, status,
                   created_at, updated_at
            FROM interunit_transfer_in_header
            WHERE id = :tid
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


# ── Delete transfer IN ──

TRANSFER_IN_DELETE_ALLOWED_EMAILS = {"yash@candorfoods.in"}


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

    # Fetch boxes from transfer-in
    in_boxes = db.execute(
        text("""
            SELECT box_id, transaction_no, article
            FROM interunit_transfer_in_boxes
            WHERE header_id = :hid
        """),
        {"hid": transfer_in_id},
    ).fetchall()

    # Determine transfer direction
    transfer_out_row = None
    if transfer_out_id:
        transfer_out_row = db.execute(
            text("SELECT from_site, to_site FROM interunit_transfers_header WHERE id = :toid"),
            {"toid": transfer_out_id},
        ).fetchone()

    cold_storage_names = ["cold storage", "rishi cold", "savla d-39 cold", "savla d-514 cold"]
    from_site_lower = (transfer_out_row.from_site or "").strip().lower() if transfer_out_row else ""
    to_site_lower = (transfer_out_row.to_site or "").strip().lower() if transfer_out_row else ""
    is_from_cold = from_site_lower in cold_storage_names
    is_to_cold = to_site_lower in cold_storage_names

    logger.info(
        "DELETE_TRANSFER_IN: from_site=%s, to_site=%s, is_from_cold=%s, is_to_cold=%s",
        from_site_lower, to_site_lower, is_from_cold, is_to_cold,
    )

    if is_to_cold:
        # Warehouse → Cold Storage: delete the cold stock rows that were inserted during transfer-in
        challan_no = getattr(header, "transfer_out_no", None)
        cold_stocks_tables = ["cfpl_cold_stocks", "cdpl_cold_stocks"]
        for cs_table in cold_stocks_tables:
            table_exists = db.execute(
                text("SELECT to_regclass(:tbl)"),
                {"tbl": f"public.{cs_table}"},
            ).scalar()
            if not table_exists:
                continue

            if challan_no:
                db.execute(
                    text(f"DELETE FROM {cs_table} WHERE inward_no = :challan"),
                    {"challan": challan_no},
                )

            for box_row in in_boxes:
                if box_row.box_id and box_row.transaction_no:
                    db.execute(
                        text(f"DELETE FROM {cs_table} WHERE box_id = :box_id AND transaction_no = :txno"),
                        {"box_id": box_row.box_id, "txno": box_row.transaction_no},
                    )

    if is_from_cold:
        # Cold Storage → Warehouse: restore the cold stock rows from snapshots
        _restore_cold_stock_snapshots(transfer_in_id, transfer_out_id, db)

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


# ══════════════════════════════════════════════
#  Categorial Inventory Lookup (for Transfer & Request article section)
# ══════════════════════════════════════════════

_CATEGORIAL_TABLE = "public.categorial_inv"


def categorial_global_search(
    search: Optional[str],
    limit: int,
    offset: int,
    db: Session,
) -> CategorialSearchResponse:
    """Global search on categorial_inv.particulars — bypasses hierarchy."""
    search_term = search.strip() if search else None

    where_clauses = ["1=1"]
    params: dict = {}

    if search_term:
        where_clauses.append('LOWER(particulars) LIKE :search')
        params["search"] = f"%{search_term.lower()}%"

    where_sql = " AND ".join(where_clauses)

    total = db.execute(
        text(f"SELECT COUNT(*) FROM (SELECT DISTINCT UPPER(particulars), UPPER(\"fg/rm/pm\") FROM {_CATEGORIAL_TABLE} WHERE {where_sql}) t"),
        params,
    ).scalar_one()

    # DISTINCT on (particulars + material_type) so all FG/RM/PM variants are returned
    rows = db.execute(
        text(f"""
            SELECT desc_upper, mt, grp, sc, uom
            FROM (
                SELECT DISTINCT ON (UPPER(particulars), UPPER("fg/rm/pm"))
                       UPPER(particulars) AS desc_upper,
                       UPPER("fg/rm/pm") AS mt,
                       UPPER("group") AS grp,
                       UPPER(sub_group) AS sc,
                       uom
                FROM {_CATEGORIAL_TABLE}
                WHERE {where_sql}
                ORDER BY UPPER(particulars) ASC, UPPER("fg/rm/pm") ASC
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
    """Cascading dropdown on categorial_inv: fg/rm/pm -> group -> sub_group -> particulars."""
    material_type = material_type.strip() if material_type else None
    item_category = item_category.strip() if item_category else None
    sub_category = sub_category.strip() if sub_category else None
    search = search.strip() if search else None

    # 1) All material types — sorted by priority: rm → fg → pm (only RM, PM, FG)
    material_types = db.execute(
        text(f"""
            SELECT mt FROM (
                SELECT DISTINCT UPPER("fg/rm/pm") AS mt FROM {_CATEGORIAL_TABLE}
                WHERE "fg/rm/pm" IS NOT NULL
                  AND UPPER("fg/rm/pm") IN ('RM', 'PM', 'FG')
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
                SELECT DISTINCT UPPER("group") AS grp FROM {_CATEGORIAL_TABLE}
                WHERE UPPER("fg/rm/pm") = UPPER(:mt) AND "group" IS NOT NULL
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
                WHERE UPPER("fg/rm/pm") = UPPER(:mt)
                  AND UPPER("group") = UPPER(:ic)
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
            'UPPER("fg/rm/pm") = UPPER(:mt)',
            'UPPER("group") = UPPER(:ic)',
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
