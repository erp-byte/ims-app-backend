from datetime import datetime
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger
from services.ims_service.interunit_models import (
    RequestCreate, RequestUpdate, TransferCreate, TransferInCreate,
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
        "package_size": str(row.packaging_type) if row.packaging_type else None,
        "net_weight": str(row.net_weight) if row.net_weight is not None else "0",
        "batch_number": row.batch_number or "",
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
                   item_desc_raw, pack_size, qty, uom, packaging_type,
                   net_weight, total_weight, batch_number, lot_number,
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
        pack_size_f = float(line.pack_size)
        qty_i = int(line.quantity)
        packaging_type = int(line.package_size) if line.package_size else 1

        if line.material_type.upper() == "FG":
            net_weight = packaging_type * pack_size_f * qty_i
        else:
            net_weight = pack_size_f * qty_i

        total_weight = net_weight * 1.1

        row = db.execute(
            text("""
                INSERT INTO interunit_transfer_request_lines
                    (request_id, rm_pm_fg_type, item_category, sub_category,
                     item_desc_raw, pack_size, qty, uom, packaging_type,
                     net_weight, total_weight, batch_number, lot_number)
                VALUES
                    (:request_id, :material_type, :item_category, :sub_category,
                     :item_desc_raw, :pack_size, :quantity, :uom, :packaging_type,
                     :net_weight, :total_weight, :batch_number, :lot_number)
                RETURNING id, request_id, rm_pm_fg_type, item_category, sub_category,
                          item_desc_raw, pack_size, qty, uom, packaging_type,
                          net_weight, total_weight, batch_number, lot_number,
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
                "uom": line.uom,
                "packaging_type": packaging_type,
                "net_weight": net_weight,
                "total_weight": total_weight,
                "batch_number": line.batch_number,
                "lot_number": line.lot_number,
            },
        ).fetchone()
        lines.append(_map_line_row(row))

    result = _map_header_row(header)
    result["lines"] = lines
    return result


# ── List requests ──


def list_requests(
    status: Optional[str],
    from_warehouse: Optional[str],
    to_warehouse: Optional[str],
    created_by: Optional[str],
    db: Session,
) -> list:
    clauses = []
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

    requests = db.execute(
        text(f"""
            SELECT id, request_no, request_date, from_site, to_site,
                   reason_code, remarks, status, reject_reason,
                   created_by, created_ts, rejected_ts, updated_at
            FROM interunit_transfer_requests r
            {where}
            ORDER BY r.created_ts DESC
        """),
        params,
    ).fetchall()

    results = []
    for req in requests:
        item = _map_header_row(req)
        item["lines"] = _fetch_lines(db, req.id)
        results.append(item)

    return results


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
        "package_size": str(row.packaging_type) if row.packaging_type else None,
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
    return {
        "id": row.id,
        "header_id": row.header_id,
        "transfer_line_id": row.transfer_line_id,
        "box_number": row.box_number,
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
                   item_desc_raw, pack_size, qty, uom, packaging_type,
                   net_weight, total_weight, batch_number, lot_number,
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
            SELECT id, header_id, transfer_line_id, box_number, article,
                   lot_number, batch_number, transaction_no,
                   net_weight, gross_weight, created_at, updated_at
            FROM interunit_transfer_boxes
            WHERE header_id = :hid
            ORDER BY box_number
        """),
        {"hid": header_id},
    ).fetchall()
    return [_map_box_row(r) for r in rows]


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
                 'Pending', :request_id, :created_by, :created_ts)
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
    for line in data.lines:
        pack_size_f = float(line.pack_size)
        qty_i = int(line.quantity)
        packaging_type = int(line.package_size) if line.package_size else 1

        if line.material_type.upper() == "FG":
            net_weight = packaging_type * pack_size_f * qty_i
        else:
            net_weight = pack_size_f * qty_i

        total_weight = net_weight * 1.1

        row = db.execute(
            text("""
                INSERT INTO interunit_transfers_lines
                    (header_id, rm_pm_fg_type, item_category, sub_category,
                     item_desc_raw, pack_size, qty, uom, packaging_type,
                     net_weight, total_weight, batch_number, lot_number)
                VALUES
                    (:header_id, :material_type, :item_category, :sub_category,
                     :item_desc_raw, :pack_size, :quantity, :uom, :packaging_type,
                     :net_weight, :total_weight, :batch_number, :lot_number)
                RETURNING id, header_id, rm_pm_fg_type, item_category, sub_category,
                          item_desc_raw, pack_size, qty, uom, packaging_type,
                          net_weight, total_weight, batch_number, lot_number,
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
                "uom": line.uom,
                "packaging_type": packaging_type,
                "net_weight": net_weight,
                "total_weight": total_weight,
                "batch_number": line.batch_number,
                "lot_number": line.lot_number,
            },
        ).fetchone()
        lines.append(row)

    # Insert boxes (if provided)
    boxes = []
    if data.boxes:
        first_line_id = lines[0].id if lines else None
        for box in data.boxes:
            box_row = db.execute(
                text("""
                    INSERT INTO interunit_transfer_boxes
                        (header_id, transfer_line_id, box_number, article,
                         lot_number, batch_number, transaction_no,
                         net_weight, gross_weight)
                    VALUES
                        (:header_id, :transfer_line_id, :box_number, :article,
                         :lot_number, :batch_number, :transaction_no,
                         :net_weight, :gross_weight)
                    RETURNING id, header_id, transfer_line_id, box_number,
                              article, lot_number, batch_number, transaction_no,
                              net_weight, gross_weight, created_at, updated_at
                """),
                {
                    "header_id": header_id,
                    "transfer_line_id": first_line_id,
                    "box_number": box.box_number,
                    "article": box.article,
                    "lot_number": box.lot_number or "",
                    "batch_number": box.batch_number or "",
                    "transaction_no": box.transaction_no or "",
                    "net_weight": float(box.net_weight),
                    "gross_weight": float(box.gross_weight),
                },
            ).fetchone()
            boxes.append(box_row)

    # Determine status based on box count vs expected qty
    if boxes:
        total_expected = sum(int(l.qty) for l in lines)
        actual_scanned = len(boxes)
        transfer_status = "Completed" if actual_scanned >= total_expected else "Partial"

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
                COUNT(DISTINCT l.id) AS items_count,
                COUNT(DISTINCT b.id) AS boxes_count,
                COALESCE(SUM(l.qty), 0) AS total_qty
            FROM interunit_transfers_header h
            LEFT JOIN interunit_transfer_requests r ON h.request_id = r.id
            LEFT JOIN interunit_transfers_lines l ON h.id = l.header_id
            LEFT JOIN interunit_transfer_boxes b ON h.id = b.header_id
            WHERE {where}
            GROUP BY h.id, r.request_no
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
        item["pending_items"] = max(0, int(row.total_qty or 0) - int(row.boxes_count or 0))
        records.append(item)

    return {
        "records": records,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if total else 0,
    }


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
    return result


# ── Delete transfer ──


def delete_transfer(transfer_id: int, db: Session) -> dict:
    existing = db.execute(
        text("SELECT id, challan_no, status FROM interunit_transfers_header WHERE id = :tid"),
        {"tid": transfer_id},
    ).fetchone()

    if not existing:
        raise HTTPException(404, "Transfer not found")

    if existing.status in ("Received", "Completed"):
        raise HTTPException(
            400,
            f"Cannot delete transfer with status '{existing.status}'. "
            "Only Pending or Partial transfers can be deleted.",
        )

    # Delete in FK order: boxes → lines → header
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

    return {
        "success": True,
        "message": "Transfer deleted successfully",
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
        "box_number": row.box_number or "",
        "transfer_out_box_id": getattr(row, "transfer_out_box_id", None),
        "article": row.article,
        "batch_number": row.batch_number,
        "lot_number": row.lot_number,
        "transaction_no": row.transaction_no,
        "net_weight": float(row.net_weight) if row.net_weight is not None else None,
        "gross_weight": float(row.gross_weight) if row.gross_weight is not None else None,
        "scanned_at": row.scanned_at,
        "is_matched": row.is_matched if row.is_matched is not None else True,
    }


def _fetch_transfer_in_boxes(db: Session, header_id: int) -> list:
    rows = db.execute(
        text("""
            SELECT id, header_id, box_number, article, batch_number,
                   lot_number, transaction_no, net_weight, gross_weight,
                   scanned_at, is_matched, transfer_out_box_id
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
        text("SELECT id, challan_no FROM interunit_transfers_header WHERE id = :id"),
        {"id": data.transfer_out_id},
    ).fetchone()

    if not transfer_out:
        raise HTTPException(404, "Transfer OUT not found")

    # Check Transfer OUT not already received
    existing_in = db.execute(
        text("SELECT id FROM interunit_transfer_in_header WHERE transfer_out_id = :toid"),
        {"toid": data.transfer_out_id},
    ).fetchone()

    if existing_in:
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

    # Ensure transfer_out_box_id column exists (idempotent)
    db.execute(text("""
        ALTER TABLE interunit_transfer_in_boxes
        ADD COLUMN IF NOT EXISTS transfer_out_box_id INTEGER
        REFERENCES interunit_transfer_boxes(id)
    """))

    # Insert scanned boxes
    boxes = []
    for box in data.scanned_boxes:
        box_row = db.execute(
            text("""
                INSERT INTO interunit_transfer_in_boxes
                    (header_id, box_number, article, batch_number, lot_number,
                     transaction_no, net_weight, gross_weight,
                     scanned_at, is_matched, transfer_out_box_id)
                VALUES
                    (:header_id, :box_number, :article, :batch_number, :lot_number,
                     :transaction_no, :net_weight, :gross_weight,
                     CURRENT_TIMESTAMP, :is_matched, :transfer_out_box_id)
                RETURNING id, header_id, box_number, article, batch_number,
                          lot_number, transaction_no, net_weight, gross_weight,
                          scanned_at, is_matched, transfer_out_box_id
            """),
            {
                "header_id": header_id,
                "box_number": box.box_number,
                "article": box.article,
                "batch_number": box.batch_number,
                "lot_number": box.lot_number,
                "transaction_no": box.transaction_no,
                "net_weight": box.net_weight,
                "gross_weight": box.gross_weight,
                "is_matched": box.is_matched,
                "transfer_out_box_id": box.transfer_out_box_id,
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
