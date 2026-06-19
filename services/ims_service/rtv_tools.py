import time
from datetime import datetime
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger
from shared.canonicalize import canonical_warehouse
from services.ims_service.inward_models import Company
from services.ims_service.rtv_models import (
    RTVCreate,
    RTVHeaderUpdate,
    RTVBoxUpsertRequest,
    RTVBulkBoxUpdateRequest,
    RTVLinesUpdateRequest,
    RTVApprovalRequest,
    RTVBoxEditLogRequest,
)
from shared.whatsapp import send_rtv_notification

logger = get_logger("ims.rtv")


# ══════════════════════════════════════════════
#  Helpers
# ══════════════════════════════════════════════


def rtv_table_names(company: Company) -> dict:
    prefix = "cfpl" if company == "CFPL" else "cdpl"
    return {
        "header": f"{prefix}_rtv_header",
        "lines": f"{prefix}_rtv_lines",
        "boxes": f"{prefix}_rtv_boxes",
    }


def _generate_rtv_id() -> str:
    # Prefix is "CR-" (Customer Return). NOT "RTV-": downstream sale/production
    # systems read "RTV" as Return-To-Vendor (the opposite flow), which
    # mis-routes these inbound customer returns. Forward-only — legacy RTV-*
    # ids and their already-printed QR labels stay valid.
    return f"CR-{datetime.now().strftime('%Y%m%d%H%M%S')}"


def _canonical_factory_unit(raw):
    """Map a factory_unit string to its canonical warehouse code, or return it
    unchanged if unrecognized (display-time mapping handles legacy values)."""
    if not raw:
        return raw
    return canonical_warehouse(raw, raw) or raw


# Cold-stock mirror: RTV cold returns also belong to cold-storage inventory.
_RTV_COLD_UNIT_MAP = {
    "Savla D-39": "D-39",
    "Savla D-514": "D-514",
    "Rishi": "Rishi",
    "Supreme": "Supreme",
}


def sync_cold_stocks_from_rtv(company: Company, rtv_id_int: int, db: Session) -> int:
    """Mirror an RTV's cold boxes into {prefix}_cold_stocks so the returned lots
    show in the cold-storage inventory + dashboard. Idempotent: owns only the rows
    it auto-creates (inward_transaction_no = the RTV id, auto_created_from_inward=true) —
    deletes them then re-inserts one row per box with a box_id. Only cold warehouses
    are mirrored; a dry/unknown warehouse just clears stale auto rows and inserts
    nothing. Does NOT commit (caller owns the transaction). The DB trigger fills
    canonical_warehouse/group/subgroup on insert."""
    tables = rtv_table_names(company)
    prefix = "cfpl" if company == "CFPL" else "cdpl"
    cold = f"{prefix}_cold_stocks"

    header = db.execute(
        text(f"SELECT factory_unit, customer, rtv_id, rtv_date FROM {tables['header']} WHERE id = :hid"),
        {"hid": rtv_id_int},
    ).fetchone()
    if not header:
        return 0

    rtv_str = header.rtv_id or ""
    wh = _canonical_factory_unit(header.factory_unit)

    # Always clear our own auto rows first (warehouse change / re-submit safe).
    db.execute(
        text(f"DELETE FROM {cold} WHERE inward_transaction_no = :tx AND auto_created_from_inward = true"),
        {"tx": rtv_str},
    )

    unit = _RTV_COLD_UNIT_MAP.get(wh)
    if unit is None:
        return 0  # not a cold warehouse — nothing mirrored

    inserted = db.execute(
        text(f"""
            INSERT INTO {cold} (
                inward_dt, unit, inward_no, cold_item_mark, vakkal, lot_no,
                no_of_cartons, weight_kg, total_inventory_kgs, group_name,
                item_description, storage_location, exporter, last_purchase_rate,
                box_id, transaction_no, item_subgroup, item_mark, value,
                inward_transaction_no, auto_created_from_inward, spl_remarks,
                canonical_warehouse, canonical_group, canonical_subgroup
            )
            SELECT
                :rtv_date, :unit, :rtv_str,
                COALESCE(b.item_mark, l.item_mark), COALESCE(b.vakkal, l.vakkal),
                COALESCE(b.lot_number, l.lot_number),
                1, b.net_weight, b.net_weight, l.item_category,
                l.item_description, :wh, :exporter, l.rate,
                b.box_id, :rtv_str, l.sub_category, COALESCE(b.item_mark, l.item_mark),
                ROUND(COALESCE(b.net_weight, 0) * COALESCE(l.rate, 0), 2),
                :rtv_str, true, COALESCE(b.spl_remarks, l.spl_remarks),
                :wh, l.item_category, l.sub_category
            FROM {tables['boxes']} b
            JOIN LATERAL (
                SELECT l2.item_category, l2.sub_category, l2.item_description,
                       l2.rate, l2.lot_number, l2.item_mark, l2.spl_remarks, l2.vakkal
                FROM {tables['lines']} l2
                WHERE l2.header_id = b.header_id
                  AND l2.item_description = b.article_description
                ORDER BY l2.id
                LIMIT 1
            ) l ON true
            WHERE b.header_id = :hid
              AND b.box_id IS NOT NULL
        """),
        {
            "hid": rtv_id_int,
            "unit": unit,
            "wh": wh,
            "rtv_str": rtv_str,
            "rtv_date": header.rtv_date,
            "exporter": header.customer,
        },
    ).rowcount

    return inserted


def _convert_date(date_str: str):
    try:
        return datetime.strptime(date_str, "%d-%m-%Y").date()
    except ValueError:
        raise HTTPException(400, "Invalid date format. Use DD-MM-YYYY")


def _map_header_row(row) -> dict:
    return {
        "id": row.id,
        "rtv_id": row.rtv_id or "",
        "rtv_date": row.rtv_date,
        "factory_unit": row.factory_unit or "",
        "customer": row.customer or "",
        "invoice_number": row.invoice_number,
        "challan_no": row.challan_no,
        "dn_no": row.dn_no,
        "conversion": str(row.conversion) if row.conversion is not None else "0",
        "sales_poc": row.sales_poc,
        "sales_poc_email": getattr(row, "sales_poc_email", None),
        "business_head": getattr(row, "business_head", None),
        "remark": row.remark,
        "status": row.status or "Pending",
        "created_by": row.created_by,
        "created_ts": row.created_ts,
        "updated_at": row.updated_at,
        "vehicle_number": getattr(row, "vehicle_number", None),
        "transporter_name": getattr(row, "transporter_name", None),
        "driver_name": getattr(row, "driver_name", None),
        "inward_manager": getattr(row, "inward_manager", None),
    }


def _map_line_row(row) -> dict:
    return {
        "id": row.id,
        "header_id": row.header_id,
        "material_type": row.material_type or "",
        "item_category": row.item_category or "",
        "sub_category": row.sub_category or "",
        "item_description": row.item_description or "",
        "uom": row.uom or "",
        "qty": str(row.qty) if row.qty is not None else "0",
        "rate": str(row.rate) if row.rate is not None else "0",
        "value": str(row.value) if row.value is not None else "0",
        "net_weight": str(row.net_weight) if row.net_weight is not None else "0",
        "carton_weight": str(row.carton_weight) if row.carton_weight is not None else "0",
        "lot_number": getattr(row, "lot_number", None),
        "item_mark": getattr(row, "item_mark", None),
        "spl_remarks": getattr(row, "spl_remarks", None),
        "vakkal": getattr(row, "vakkal", None),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _map_box_row(row) -> dict:
    return {
        "id": row.id,
        "header_id": row.header_id,
        "rtv_line_id": row.rtv_line_id,
        "box_number": row.box_number,
        "box_id": row.box_id or "",
        "article_description": row.article_description or "",
        "uom": row.uom or None,
        "conversion": str(row.conversion) if row.conversion is not None else None,
        "lot_number": row.lot_number,
        "item_mark": getattr(row, "item_mark", None),
        "spl_remarks": getattr(row, "spl_remarks", None),
        "vakkal": getattr(row, "vakkal", None),
        "net_weight": str(row.net_weight) if row.net_weight is not None else "0",
        "gross_weight": str(row.gross_weight) if row.gross_weight is not None else "0",
        "count": row.count,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _fetch_lines(db: Session, tables: dict, header_id: int) -> list:
    rows = db.execute(
        text(f"""
            SELECT id, header_id, material_type, item_category, sub_category,
                   item_description, uom, qty, rate, value, net_weight, carton_weight,
                   lot_number, item_mark, spl_remarks, vakkal,
                   created_at, updated_at
            FROM {tables['lines']}
            WHERE header_id = :hid
            ORDER BY id
        """),
        {"hid": header_id},
    ).fetchall()
    return [_map_line_row(r) for r in rows]


def _fetch_boxes(db: Session, tables: dict, header_id: int) -> list:
    rows = db.execute(
        text(f"""
            SELECT id, header_id, rtv_line_id, box_number, box_id,
                   article_description, uom, conversion, lot_number,
                   item_mark, spl_remarks, vakkal, net_weight, gross_weight,
                   count, created_at, updated_at
            FROM {tables['boxes']}
            WHERE header_id = :hid
            ORDER BY box_number
        """),
        {"hid": header_id},
    ).fetchall()
    return [_map_box_row(r) for r in rows]


# ══════════════════════════════════════════════
#  CRUD
# ══════════════════════════════════════════════


def set_rtv_status_via_email_action(
    company: Company, rtv_db_id: int, new_status: str, actor_email: str, db: Session
) -> dict:
    """Set the header status (Hold/Rejected) and record the actor. For magic-link email actions."""
    tables = rtv_table_names(company)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    row = db.execute(
        text(f"""
            UPDATE {tables['header']}
            SET status = :status,
                approved_by = :actor,
                approved_at = :ts,
                updated_at = NOW()
            WHERE id = :hid
            RETURNING id, rtv_id, status
        """),
        {"status": new_status, "actor": actor_email, "ts": now, "hid": rtv_db_id},
    ).fetchone()
    if not row:
        raise HTTPException(404, "RTV not found")
    return {"id": row.id, "rtv_id": row.rtv_id, "status": row.status}


def create_rtv(data: RTVCreate, created_by: str, db: Session) -> dict:
    tables = rtv_table_names(data.company)
    rtv_id = _generate_rtv_id()

    header = db.execute(
        text(f"""
            INSERT INTO {tables['header']}
                (rtv_id, rtv_date, factory_unit, customer,
                 invoice_number, challan_no, dn_no, conversion,
                 sales_poc, sales_poc_email, business_head, remark,
                 vehicle_number, transporter_name, driver_name, inward_manager,
                 status, created_by, created_ts)
            VALUES
                (:rtv_id, NOW(), :factory_unit, :customer,
                 :invoice_number, :challan_no, :dn_no, :conversion,
                 :sales_poc, :sales_poc_email, :business_head, :remark,
                 :vehicle_number, :transporter_name, :driver_name, :inward_manager,
                 'Pending', :created_by, NOW())
            RETURNING id, rtv_id, rtv_date, factory_unit, customer,
                      invoice_number, challan_no, dn_no, conversion,
                      sales_poc, sales_poc_email, business_head, remark, status, created_by, created_ts, updated_at,
                      vehicle_number, transporter_name, driver_name, inward_manager
        """),
        {
            "rtv_id": rtv_id,
            "factory_unit": _canonical_factory_unit(data.header.factory_unit),
            "customer": data.header.customer,
            "invoice_number": data.header.invoice_number,
            "challan_no": data.header.challan_no,
            "dn_no": data.header.dn_no,
            "conversion": float(data.header.conversion) if data.header.conversion else 0,
            "sales_poc": data.header.sales_poc,
            "sales_poc_email": data.header.sales_poc_email,
            "business_head": data.header.business_head,
            "remark": data.header.remark,
            "vehicle_number": data.header.vehicle_number,
            "transporter_name": data.header.transporter_name,
            "driver_name": data.header.driver_name,
            "inward_manager": data.header.inward_manager,
            "created_by": created_by,
        },
    ).fetchone()

    header_id = header.id

    lines = []
    for line in data.lines:
        qty_i = int(line.qty) if line.qty else 0
        rate_f = float(line.rate) if line.rate else 0.0
        value_f = float(line.value) if line.value and float(line.value) > 0 else qty_i * rate_f
        net_weight_f = float(line.net_weight) if line.net_weight else 0.0
        carton_weight_f = float(line.carton_weight) if line.carton_weight else 0.0

        row = db.execute(
            text(f"""
                INSERT INTO {tables['lines']}
                    (header_id, material_type, item_category, sub_category,
                     item_description, uom, qty, rate, value, net_weight, carton_weight,
                     lot_number, item_mark, spl_remarks, vakkal)
                VALUES
                    (:header_id, :material_type, :item_category, :sub_category,
                     :item_description, :uom, :qty, :rate, :value, :net_weight, :carton_weight,
                     :lot_number, :item_mark, :spl_remarks, :vakkal)
                RETURNING id, header_id, material_type, item_category, sub_category,
                          item_description, uom, qty, rate, value, net_weight, carton_weight,
                          lot_number, item_mark, spl_remarks, vakkal,
                          created_at, updated_at
            """),
            {
                "header_id": header_id,
                "material_type": line.material_type,
                "item_category": line.item_category,
                "sub_category": line.sub_category,
                "item_description": line.item_description,
                "uom": line.uom,
                "qty": qty_i,
                "rate": rate_f,
                "value": value_f,
                "net_weight": net_weight_f,
                "carton_weight": carton_weight_f,
                "lot_number": line.lot_number,
                "item_mark": line.item_mark,
                "spl_remarks": line.spl_remarks,
                "vakkal": line.vakkal,
            },
        ).fetchone()
        lines.append(_map_line_row(row))

    result = _map_header_row(header)
    result["lines"] = lines
    result["boxes"] = []

    # Send WhatsApp notification (non-blocking, won't fail the request)
    try:
        send_rtv_notification(
            rtv_id=rtv_id,
            customer=data.header.customer,
            lines=lines,
            remark=data.header.remark,
        )
    except Exception as exc:
        logger.warning("WhatsApp notification failed for %s: %s", rtv_id, exc)

    return result


def list_rtvs(
    company: Company,
    page: int,
    per_page: int,
    status: Optional[str],
    factory_unit: Optional[str],
    customer: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    sort_by: str,
    sort_order: str,
    db: Session,
) -> dict:
    tables = rtv_table_names(company)
    clauses = ["1=1"]
    params: dict = {}

    if status:
        clauses.append("h.status = :status")
        params["status"] = status
    if factory_unit:
        clauses.append("h.factory_unit = :factory_unit")
        params["factory_unit"] = factory_unit
    if customer:
        clauses.append("h.customer ILIKE :customer")
        params["customer"] = f"%{customer}%"
    if from_date:
        clauses.append("h.rtv_date::date >= :from_date")
        params["from_date"] = _convert_date(from_date)
    if to_date:
        clauses.append("h.rtv_date::date <= :to_date")
        params["to_date"] = _convert_date(to_date)

    where = " AND ".join(clauses)

    valid_sort = {"rtv_id", "rtv_date", "factory_unit", "customer", "status", "created_ts"}
    if sort_by not in valid_sort:
        sort_by = "created_ts"
    direction = "DESC" if sort_order.lower() == "desc" else "ASC"

    total = db.execute(
        text(f"SELECT COUNT(*) FROM {tables['header']} h WHERE {where}"),
        params,
    ).scalar()

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    rows = db.execute(
        text(f"""
            SELECT h.id, h.rtv_id, h.rtv_date, h.factory_unit, h.customer,
                   h.invoice_number, h.challan_no, h.dn_no, h.conversion,
                   h.sales_poc, h.sales_poc_email, h.business_head, h.remark, h.status, h.created_by, h.created_ts, h.updated_at,
                   h.vehicle_number, h.transporter_name, h.driver_name, h.inward_manager,
                   (SELECT COUNT(*) FROM {tables['lines']} l WHERE l.header_id = h.id) AS items_count,
                   (SELECT COUNT(*) FROM {tables['boxes']} b WHERE b.header_id = h.id) AS boxes_count,
                   (SELECT COALESCE(SUM(l.qty), 0) FROM {tables['lines']} l WHERE l.header_id = h.id) AS total_qty,
                   (SELECT COALESCE(SUM(b.net_weight), 0) FROM {tables['boxes']} b WHERE b.header_id = h.id) AS total_net_weight
            FROM {tables['header']} h
            WHERE {where}
            ORDER BY h.{sort_by} {direction}
            LIMIT :limit OFFSET :offset
        """),
        params,
    ).fetchall()

    records = []
    for row in rows:
        item = _map_header_row(row)
        item["items_count"] = row.items_count or 0
        item["boxes_count"] = row.boxes_count or 0
        item["total_qty"] = int(row.total_qty or 0)
        item["total_net_weight"] = float(row.total_net_weight or 0)
        records.append(item)

    return {
        "records": records,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if total else 0,
    }


def get_rtv(company: Company, rtv_id_int: int, db: Session) -> dict:
    tables = rtv_table_names(company)

    header = db.execute(
        text(f"""
            SELECT id, rtv_id, rtv_date, factory_unit, customer,
                   invoice_number, challan_no, dn_no, conversion,
                   sales_poc, sales_poc_email, business_head, remark, status, created_by, created_ts, updated_at,
                   vehicle_number, transporter_name, driver_name, inward_manager
            FROM {tables['header']}
            WHERE id = :hid
        """),
        {"hid": rtv_id_int},
    ).fetchone()

    if not header:
        raise HTTPException(404, "RTV not found")

    result = _map_header_row(header)
    result["lines"] = _fetch_lines(db, tables, header.id)
    result["boxes"] = _fetch_boxes(db, tables, header.id)
    return result


def update_rtv(company: Company, rtv_id_int: int, data: RTVHeaderUpdate, db: Session) -> dict:
    tables = rtv_table_names(company)

    existing = db.execute(
        text(f"SELECT id FROM {tables['header']} WHERE id = :hid"),
        {"hid": rtv_id_int},
    ).fetchone()
    if not existing:
        raise HTTPException(404, "RTV not found")

    updates = []
    params: dict = {"hid": rtv_id_int}

    field_map = {
        "factory_unit": data.factory_unit,
        "customer": data.customer,
        "invoice_number": data.invoice_number,
        "challan_no": data.challan_no,
        "dn_no": data.dn_no,
        "sales_poc": data.sales_poc,
        "sales_poc_email": data.sales_poc_email,
        "business_head": data.business_head,
        "remark": data.remark,
        "status": data.status,
        "vehicle_number": data.vehicle_number,
        "transporter_name": data.transporter_name,
        "driver_name": data.driver_name,
        "inward_manager": data.inward_manager,
    }

    for col, val in field_map.items():
        if val is not None:
            updates.append(f"{col} = :{col}")
            params[col] = val

    if data.conversion is not None:
        updates.append("conversion = :conversion")
        params["conversion"] = float(data.conversion)

    if not updates:
        raise HTTPException(400, "No fields to update")

    updates.append("updated_at = NOW()")
    set_clause = ", ".join(updates)

    row = db.execute(
        text(f"""
            UPDATE {tables['header']}
            SET {set_clause}
            WHERE id = :hid
            RETURNING id, rtv_id, rtv_date, factory_unit, customer,
                      invoice_number, challan_no, dn_no, conversion,
                      sales_poc, sales_poc_email, business_head, remark, status, created_by, created_ts, updated_at,
                      vehicle_number, transporter_name, driver_name, inward_manager
        """),
        params,
    ).fetchone()

    return _map_header_row(row)


def delete_rtv(company: Company, rtv_id_int: int, db: Session) -> dict:
    tables = rtv_table_names(company)

    existing = db.execute(
        text(f"SELECT id, rtv_id, business_head, created_by FROM {tables['header']} WHERE id = :hid"),
        {"hid": rtv_id_int},
    ).fetchone()
    if not existing:
        raise HTTPException(404, "RTV not found")

    # CASCADE handles children, but explicit delete for clarity
    db.execute(
        text(f"DELETE FROM {tables['boxes']} WHERE header_id = :hid"),
        {"hid": rtv_id_int},
    )
    db.execute(
        text(f"DELETE FROM {tables['lines']} WHERE header_id = :hid"),
        {"hid": rtv_id_int},
    )
    db.execute(
        text(f"DELETE FROM {tables['header']} WHERE id = :hid"),
        {"hid": rtv_id_int},
    )

    return {
        "success": True,
        "message": "RTV deleted successfully",
        "rtv_id": existing.rtv_id,
        "business_head": existing.business_head,
        "created_by": existing.created_by,
    }


# ══════════════════════════════════════════════
#  Box-by-box upsert
# ══════════════════════════════════════════════


def upsert_rtv_box(company: Company, rtv_id_int: int, payload: RTVBoxUpsertRequest, db: Session) -> dict:
    tables = rtv_table_names(company)

    header = db.execute(
        text(f"SELECT id, rtv_id FROM {tables['header']} WHERE id = :hid"),
        {"hid": rtv_id_int},
    ).fetchone()
    if not header:
        raise HTTPException(404, "RTV not found")

    # Resolve line FK
    line = db.execute(
        text(f"""
            SELECT id FROM {tables['lines']}
            WHERE header_id = :hid AND item_description = :art_desc
            LIMIT 1
        """),
        {"hid": rtv_id_int, "art_desc": payload.article_description},
    ).fetchone()
    rtv_line_id = line.id if line else None

    # Check existing box
    existing = db.execute(
        text(f"""
            SELECT id, box_id FROM {tables['boxes']}
            WHERE header_id = :hid
              AND article_description = :art_desc
              AND box_number = :box_num
        """),
        {"hid": rtv_id_int, "art_desc": payload.article_description, "box_num": payload.box_number},
    ).fetchone()

    params = {
        "hid": rtv_id_int,
        "line_id": rtv_line_id,
        "art_desc": payload.article_description,
        "box_num": payload.box_number,
        "uom": payload.uom,
        "conversion": payload.conversion,
        "net_weight": float(payload.net_weight) if payload.net_weight is not None else None,
        "gross_weight": float(payload.gross_weight) if payload.gross_weight is not None else None,
        "lot_number": payload.lot_number,
        "item_mark": payload.item_mark,
        "spl_remarks": payload.spl_remarks,
        "vakkal": payload.vakkal,
        "count": payload.count,
    }

    if existing and existing.box_id:
        # Already printed — update weights/lot only, preserve existing values
        db.execute(
            text(f"""
                UPDATE {tables['boxes']}
                SET uom = COALESCE(:uom, uom),
                    conversion = COALESCE(:conversion, conversion),
                    net_weight = COALESCE(:net_weight, net_weight),
                    gross_weight = COALESCE(:gross_weight, gross_weight),
                    lot_number = COALESCE(:lot_number, lot_number),
                    item_mark = COALESCE(:item_mark, item_mark),
                    spl_remarks = COALESCE(:spl_remarks, spl_remarks),
                    vakkal = COALESCE(:vakkal, vakkal),
                    count = COALESCE(:count, count),
                    rtv_line_id = :line_id, updated_at = NOW()
                WHERE header_id = :hid
                  AND article_description = :art_desc
                  AND box_number = :box_num
            """),
            params,
        )
        box_id = existing.box_id
        status = "updated"
    else:
        # Generate new box_id
        base = str(int(time.time() * 1000))[-8:]
        box_id = f"{base}-{payload.box_number}"
        params["box_id"] = box_id

        if existing:
            db.execute(
                text(f"""
                    UPDATE {tables['boxes']}
                    SET uom = COALESCE(:uom, uom),
                        conversion = COALESCE(:conversion, conversion),
                        net_weight = COALESCE(:net_weight, net_weight),
                        gross_weight = COALESCE(:gross_weight, gross_weight),
                        lot_number = COALESCE(:lot_number, lot_number),
                        item_mark = COALESCE(:item_mark, item_mark),
                        spl_remarks = COALESCE(:spl_remarks, spl_remarks),
                        vakkal = COALESCE(:vakkal, vakkal),
                        count = COALESCE(:count, count),
                        box_id = :box_id, rtv_line_id = :line_id, updated_at = NOW()
                    WHERE header_id = :hid
                      AND article_description = :art_desc
                      AND box_number = :box_num
                """),
                params,
            )
        else:
            db.execute(
                text(f"""
                    INSERT INTO {tables['boxes']}
                        (header_id, rtv_line_id, box_number, box_id,
                         article_description, uom, conversion, lot_number,
                         item_mark, spl_remarks, vakkal,
                         net_weight, gross_weight, count)
                    VALUES
                        (:hid, :line_id, :box_num, :box_id,
                         :art_desc, :uom, :conversion, :lot_number,
                         :item_mark, :spl_remarks, :vakkal,
                         :net_weight, :gross_weight, :count)
                """),
                params,
            )
        status = "inserted"

    return {
        "status": status,
        "box_id": box_id,
        "rtv_id": header.rtv_id,
        "article_description": payload.article_description,
        "box_number": payload.box_number,
    }


def bulk_save_boxes(
    company: Company, rtv_id_int: int, data, db: Session, notify_discrepancy: bool = True
) -> dict:
    """State-aware full sync of the box set for a CR. Insert new, update existing
    (preserving box_id), delete boxes no longer present. Persists cold fields."""
    # NOTE: notify_discrepancy is reserved for a future net-weight discrepancy
    # notification and is currently a no-op (Phase 1).
    tables = rtv_table_names(company)

    header = db.execute(
        text(f"SELECT id, rtv_id FROM {tables['header']} WHERE id = :hid"),
        {"hid": rtv_id_int},
    ).fetchone()
    if not header:
        raise HTTPException(404, "RTV not found")

    existing_rows = db.execute(
        text(f"""SELECT box_number, box_id, article_description
                 FROM {tables['boxes']} WHERE header_id = :hid"""),
        {"hid": rtv_id_int},
    ).fetchall()
    existing_keys = {(r.article_description, r.box_number): r.box_id for r in existing_rows}

    _seen = {}
    for b in data.boxes:
        _seen[(b.article_description, b.box_number)] = b   # keep last occurrence
    incoming_boxes = list(_seen.values())
    incoming_keys = set(_seen.keys())

    inserted = updated = 0
    for b in incoming_boxes:
        line = db.execute(
            text(f"SELECT id FROM {tables['lines']} WHERE header_id = :hid AND item_description = :art LIMIT 1"),
            {"hid": rtv_id_int, "art": b.article_description},
        ).fetchone()
        params = {
            "hid": rtv_id_int,
            "line_id": line.id if line else None,
            "art_desc": b.article_description,
            "box_num": b.box_number,
            "uom": b.uom,
            "conversion": b.conversion,
            "lot_number": b.lot_number,
            "item_mark": b.item_mark,
            "spl_remarks": b.spl_remarks,
            "vakkal": b.vakkal,
            "net_weight": float(b.net_weight) if b.net_weight is not None else None,
            "gross_weight": float(b.gross_weight) if b.gross_weight is not None else None,
            "count": b.count,
        }
        if (b.article_description, b.box_number) in existing_keys:
            db.execute(text(f"""
                UPDATE {tables['boxes']}
                SET uom = COALESCE(:uom, uom),
                    conversion = COALESCE(:conversion, conversion),
                    lot_number = COALESCE(:lot_number, lot_number),
                    item_mark = COALESCE(:item_mark, item_mark),
                    spl_remarks = COALESCE(:spl_remarks, spl_remarks),
                    vakkal = COALESCE(:vakkal, vakkal),
                    net_weight = COALESCE(:net_weight, net_weight),
                    gross_weight = COALESCE(:gross_weight, gross_weight),
                    count = COALESCE(:count, count),
                    rtv_line_id = :line_id, updated_at = NOW()
                WHERE header_id = :hid AND article_description = :art_desc AND box_number = :box_num
            """), params)
            updated += 1
        else:
            base = str(int(time.time() * 1000))[-8:]
            params["box_id"] = f"{base}-{b.box_number}-{inserted}"
            db.execute(text(f"""
                INSERT INTO {tables['boxes']}
                    (header_id, rtv_line_id, box_number, box_id, article_description,
                     uom, conversion, lot_number, item_mark, spl_remarks, vakkal,
                     net_weight, gross_weight, count)
                VALUES
                    (:hid, :line_id, :box_num, :box_id, :art_desc,
                     :uom, :conversion, :lot_number, :item_mark, :spl_remarks, :vakkal,
                     :net_weight, :gross_weight, :count)
            """), params)
            inserted += 1

    deleted = 0
    for (art, num) in existing_keys.keys() - incoming_keys:
        db.execute(
            text(f"""DELETE FROM {tables['boxes']}
                     WHERE header_id = :hid AND article_description = :art AND box_number = :num"""),
            {"hid": rtv_id_int, "art": art, "num": num},
        )
        deleted += 1

    # Mirror cold boxes into cold-storage inventory (same transaction).
    sync_cold_stocks_from_rtv(company, rtv_id_int, db)

    return {
        "status": "synced",
        "rtv_id": header.rtv_id,
        "inserted": inserted,
        "updated": updated,
        "unchanged": 0,
        "deleted": deleted,
    }


# ══════════════════════════════════════════════
#  Update lines (replace all)
# ══════════════════════════════════════════════


def update_rtv_lines(
    company: Company, rtv_id_int: int, data: RTVLinesUpdateRequest, db: Session
) -> dict:
    tables = rtv_table_names(company)

    existing = db.execute(
        text(f"SELECT id, rtv_id FROM {tables['header']} WHERE id = :hid"),
        {"hid": rtv_id_int},
    ).fetchone()
    if not existing:
        raise HTTPException(404, "RTV not found")

    header_id = existing.id

    # Delete old lines
    db.execute(
        text(f"DELETE FROM {tables['lines']} WHERE header_id = :hid"),
        {"hid": header_id},
    )

    # Insert new lines
    for line in data.lines:
        qty_i = int(line.qty) if line.qty else 0
        rate_f = float(line.rate) if line.rate else 0.0
        value_f = float(line.value) if line.value and float(line.value) > 0 else qty_i * rate_f
        net_weight_f = float(line.net_weight) if line.net_weight else 0.0
        carton_weight_f = float(line.carton_weight) if line.carton_weight else 0.0

        db.execute(
            text(f"""
                INSERT INTO {tables['lines']}
                    (header_id, material_type, item_category, sub_category,
                     item_description, uom, qty, rate, value, net_weight, carton_weight,
                     lot_number, item_mark, spl_remarks, vakkal)
                VALUES
                    (:header_id, :material_type, :item_category, :sub_category,
                     :item_description, :uom, :qty, :rate, :value, :net_weight, :carton_weight,
                     :lot_number, :item_mark, :spl_remarks, :vakkal)
            """),
            {
                "header_id": header_id,
                "material_type": line.material_type,
                "item_category": line.item_category,
                "sub_category": line.sub_category,
                "item_description": line.item_description,
                "uom": line.uom,
                "qty": qty_i,
                "rate": rate_f,
                "value": value_f,
                "net_weight": net_weight_f,
                "carton_weight": carton_weight_f,
                "lot_number": line.lot_number,
                "item_mark": line.item_mark,
                "spl_remarks": line.spl_remarks,
                "vakkal": line.vakkal,
            },
        )

    return {
        "status": "updated",
        "rtv_id": existing.rtv_id,
        "lines_count": len(data.lines),
    }


# ══════════════════════════════════════════════
#  Approval workflow
# ══════════════════════════════════════════════


def approve_rtv(
    company: Company, rtv_id_int: int, payload: RTVApprovalRequest, db: Session
) -> dict:
    tables = rtv_table_names(company)

    existing = db.execute(
        text(f"SELECT id, rtv_id, status FROM {tables['header']} WHERE id = :hid"),
        {"hid": rtv_id_int},
    ).fetchone()
    if not existing:
        raise HTTPException(404, "RTV not found")

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # 1) Update header: set status='Approved', approved_by, approved_at
    update_parts = [
        "status = 'Approved'",
        "approved_by = :approved_by",
        "approved_at = :approved_at",
        "updated_at = NOW()",
    ]
    params: dict = {
        "hid": rtv_id_int,
        "approved_by": payload.approved_by,
        "approved_at": now,
    }

    if payload.header:
        header_data = payload.header.model_dump(exclude_none=True)
        for field, value in header_data.items():
            if field == "conversion":
                update_parts.append(f"{field} = :{field}")
                params[field] = float(value) if value else 0
            elif field == "factory_unit":
                update_parts.append(f"{field} = :{field}")
                params[field] = _canonical_factory_unit(value)
            else:
                update_parts.append(f"{field} = :{field}")
                params[field] = value

    db.execute(
        text(f"""
            UPDATE {tables['header']}
            SET {', '.join(update_parts)}
            WHERE id = :hid
        """),
        params,
    )

    # 2) Update lines if provided (merge by item_description)
    if payload.lines:
        for line in payload.lines:
            line_data = line.model_dump(exclude_none=True)
            item_desc = line_data.pop("item_description")

            if line_data:
                set_parts = []
                line_params: dict = {"hid": rtv_id_int, "item_desc": item_desc}
                for k, v in line_data.items():
                    if k in ("qty", "rate", "value", "net_weight", "carton_weight"):
                        set_parts.append(f"{k} = :{k}")
                        line_params[k] = float(v) if v else 0
                    else:
                        set_parts.append(f"{k} = :{k}")
                        line_params[k] = v

                if set_parts:
                    set_parts.append("updated_at = NOW()")
                    db.execute(
                        text(f"""
                            UPDATE {tables['lines']}
                            SET {', '.join(set_parts)}
                            WHERE header_id = :hid AND item_description = :item_desc
                        """),
                        line_params,
                    )

    # 3) Upsert boxes if provided (preserve existing box_ids)
    if payload.boxes:
        for _i, b in enumerate(payload.boxes):
            box_params = {
                "hid": rtv_id_int,
                "art_desc": b.article_description,
                "box_num": b.box_number,
                "uom": b.uom,
                "conversion": b.conversion,
                "net_weight": float(b.net_weight) if b.net_weight is not None else None,
                "gross_weight": float(b.gross_weight) if b.gross_weight is not None else None,
                "count": b.count,
                "lot_number": b.lot_number,
                "item_mark": b.item_mark,
                "spl_remarks": b.spl_remarks,
                "vakkal": b.vakkal,
            }

            existing_box = db.execute(
                text(f"""
                    SELECT box_id FROM {tables['boxes']}
                    WHERE header_id = :hid
                      AND article_description = :art_desc AND box_number = :box_num
                """),
                box_params,
            ).fetchone()

            if existing_box:
                db.execute(
                    text(f"""
                        UPDATE {tables['boxes']}
                        SET uom = COALESCE(:uom, uom),
                            conversion = COALESCE(:conversion, conversion),
                            net_weight = COALESCE(:net_weight, net_weight),
                            gross_weight = COALESCE(:gross_weight, gross_weight),
                            count = COALESCE(:count, count),
                            lot_number = COALESCE(:lot_number, lot_number),
                            item_mark = COALESCE(:item_mark, item_mark),
                            spl_remarks = COALESCE(:spl_remarks, spl_remarks),
                            vakkal = COALESCE(:vakkal, vakkal),
                            updated_at = NOW()
                        WHERE header_id = :hid
                          AND article_description = :art_desc AND box_number = :box_num
                    """),
                    box_params,
                )
            else:
                base = str(int(time.time() * 1000))[-8:]
                box_params["box_id"] = f"{base}-{b.box_number}-{_i}"
                db.execute(
                    text(f"""
                        INSERT INTO {tables['boxes']}
                            (header_id, box_number, box_id, article_description,
                             uom, conversion, lot_number, item_mark, spl_remarks, vakkal,
                             net_weight, gross_weight, count)
                        VALUES
                            (:hid, :box_num, :box_id, :art_desc,
                             :uom, :conversion, :lot_number, :item_mark, :spl_remarks, :vakkal,
                             :net_weight, :gross_weight, :count)
                    """),
                    box_params,
                )

    # Mirror cold boxes into cold-storage inventory (same transaction).
    sync_cold_stocks_from_rtv(company, rtv_id_int, db)

    return {
        "status": "approved",
        "rtv_id": existing.rtv_id,
        "company": company,
        "approved_by": payload.approved_by,
        "approved_at": now,
    }


# ══════════════════════════════════════════════
#  Email-action status transition (Approve / Reject / Hold)
# ══════════════════════════════════════════════


ACTION_TO_STATUS = {
    "approve": "Approved",
    "reject":  "Rejected",
    "hold":    "On Hold",
}


def set_rtv_status(
    rtv_id: str, business_head_email: str, action: str, db: Session
) -> dict:
    """Update an RTV's status from an email action button click.

    Locates the RTV across both company-partitioned tables by its string
    rtv_id, sets the status, and records the actor in approved_by/approved_at
    (those columns already exist and now capture any state transition).
    """
    new_status = ACTION_TO_STATUS.get(action)
    if not new_status:
        raise HTTPException(400, f"Invalid action '{action}'")

    matches: list = []
    for company in ("CFPL", "CDPL"):
        tables = rtv_table_names(company)
        row = db.execute(
            text(f"SELECT id FROM {tables['header']} WHERE rtv_id = :rid"),
            {"rid": rtv_id},
        ).fetchone()
        if row:
            matches.append((company, row.id))

    if not matches:
        raise HTTPException(404, f"RTV {rtv_id} not found")
    if len(matches) > 1:
        raise HTTPException(
            409, f"RTV id {rtv_id} is ambiguous — matches in {[m[0] for m in matches]}"
        )

    company, header_id = matches[0]
    tables = rtv_table_names(company)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    db.execute(
        text(f"""
            UPDATE {tables['header']}
            SET status      = :status,
                approved_by = :actor,
                approved_at = :actioned_at,
                updated_at  = NOW()
            WHERE id = :hid
        """),
        {
            "hid": header_id,
            "status": new_status,
            "actor": business_head_email,
            "actioned_at": now,
        },
    )
    db.commit()

    return {
        "success": True,
        "rtv_id": rtv_id,
        "company": company,
        "status": new_status,
        "actioned_by": business_head_email,
        "actioned_at": now,
    }


def apply_rtv_email_action(
    rtv_id: str, bh_email: str, action: str, db: Session
) -> dict:
    """Apply an email-button action after validating BH ownership against the DB.

    Returns a dict describing the outcome. Caller is expected to fire the
    confirmation mail when ``already_actioned`` is False.
    """
    if not rtv_id or not bh_email:
        raise HTTPException(400, "Missing rtv_id or bh_email")
    if action not in ACTION_TO_STATUS:
        raise HTTPException(400, f"Invalid action '{action}'")

    new_status = ACTION_TO_STATUS[action]

    matches: list = []
    for company in ("CFPL", "CDPL"):
        tables = rtv_table_names(company)
        row = db.execute(
            text(
                f"SELECT id, status, business_head FROM {tables['header']} WHERE rtv_id = :rid"
            ),
            {"rid": rtv_id},
        ).fetchone()
        if row:
            matches.append((company, row))

    if not matches:
        raise HTTPException(404, f"RTV {rtv_id} not found")
    if len(matches) > 1:
        raise HTTPException(
            409, f"RTV id {rtv_id} ambiguous across {[m[0] for m in matches]}"
        )

    company, row = matches[0]
    header_id = row.id
    current_status = (row.status or "Pending")

    # Validate the URL's bh_email matches the BH stored on the RTV record.
    from shared.email_notifier import _lookup_business_head_email
    rtv_bh_email = _lookup_business_head_email(row.business_head)
    if not rtv_bh_email or rtv_bh_email.lower() != bh_email.lower():
        raise HTTPException(
            403, "This action link is not authorised for the recipient on file"
        )

    # The original mail's buttons stay actionable while Pending or On Hold (a
    # held return can still be approved/rejected later); a final Approved/Rejected
    # is terminal and further clicks are no-ops.
    if current_status not in ("Pending", "On Hold"):
        detail = get_rtv(company, header_id, db)
        return {
            "already_actioned": True,
            "rtv_id": rtv_id,
            "company": company,
            "status": current_status,
            "requested_status": new_status,
            "detail": detail,
        }

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    tables = rtv_table_names(company)
    result = db.execute(
        text(
            f"""
            UPDATE {tables['header']}
            SET status      = :status,
                approved_by = :actor,
                approved_at = :actioned_at,
                updated_at  = NOW()
            WHERE id = :hid AND status IN ('Pending', 'On Hold')
            """
        ),
        {
            "hid": header_id,
            "status": new_status,
            "actor": bh_email,
            "actioned_at": now,
        },
    )
    db.commit()

    if result.rowcount == 0:
        # Lost a race with a concurrent click — re-read and report as already actioned.
        detail = get_rtv(company, header_id, db)
        return {
            "already_actioned": True,
            "rtv_id": rtv_id,
            "company": company,
            "status": detail.get("status") if isinstance(detail, dict) else "",
            "requested_status": new_status,
            "detail": detail,
        }

    detail = get_rtv(company, header_id, db)
    return {
        "already_actioned": False,
        "rtv_id": rtv_id,
        "company": company,
        "status": new_status,
        "actioned_by": bh_email,
        "actioned_at": now,
        "detail": detail,
    }


# ══════════════════════════════════════════════
#  Box edit logging
# ══════════════════════════════════════════════


def log_rtv_box_edits(payload: RTVBoxEditLogRequest, db: Session) -> dict:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    for change in payload.changes:
        description = f"Changed {change.field_name} from '{change.old_value}' to '{change.new_value}'"
        db.execute(
            text("""
                INSERT INTO box_edit_logs
                    (email_id, description, transaction_no, box_id, field_name,
                     old_value, new_value, edited_at)
                VALUES
                    (:email_id, :description, :txno, :box_id, :field_name,
                     :old_value, :new_value, :edited_at)
            """),
            {
                "email_id": payload.email_id,
                "description": description,
                "txno": payload.rtv_id,
                "box_id": payload.box_id,
                "field_name": change.field_name,
                "old_value": change.old_value,
                "new_value": change.new_value,
                "edited_at": now,
            },
        )
    return {"status": "logged", "entries": len(payload.changes)}


# ══════════════════════════════════════════════
#  Excel export
# ══════════════════════════════════════════════


def export_rtv_records(
    company: Company,
    status: Optional[str],
    customer: Optional[str],
    factory_unit: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    sort_by: str,
    sort_order: str,
    db: Session,
) -> list:
    tables = rtv_table_names(company)
    clauses = ["1=1"]
    params: dict = {}

    if status:
        clauses.append("h.status = :status")
        params["status"] = status
    if factory_unit:
        clauses.append("h.factory_unit = :factory_unit")
        params["factory_unit"] = factory_unit
    if customer:
        clauses.append("h.customer ILIKE :customer")
        params["customer"] = f"%{customer}%"
    if from_date:
        clauses.append("h.rtv_date::date >= :from_date")
        params["from_date"] = _convert_date(from_date)
    if to_date:
        clauses.append("h.rtv_date::date <= :to_date")
        params["to_date"] = _convert_date(to_date)

    where = " AND ".join(clauses)

    valid_sort = {"rtv_id", "rtv_date", "factory_unit", "customer", "status", "created_ts"}
    if sort_by not in valid_sort:
        sort_by = "created_ts"
    direction = "DESC" if sort_order.lower() == "desc" else "ASC"

    records = db.execute(
        text(f"""
            SELECT
                h.rtv_id, h.rtv_date, h.factory_unit, h.customer,
                h.invoice_number, h.challan_no, h.dn_no, h.conversion,
                h.sales_poc, h.business_head, h.remark, h.status, h.created_by, h.created_ts,
                l.material_type, l.item_category, l.sub_category,
                l.item_description, l.uom, l.qty, l.rate, l.value,
                l.net_weight AS line_net_weight, l.carton_weight AS line_carton_weight,
                b.box_id, b.article_description AS box_article,
                b.box_number, b.uom AS box_uom, b.conversion AS box_conversion,
                b.net_weight AS box_net_weight,
                b.gross_weight AS box_gross_weight,
                b.lot_number AS box_lot_number, b.count AS box_count
            FROM {tables['header']} h
            LEFT JOIN {tables['lines']} l ON h.id = l.header_id
            LEFT JOIN {tables['boxes']} b ON h.id = b.header_id
            WHERE {where}
            ORDER BY h.{sort_by} {direction}, l.id ASC, b.box_number ASC
        """),
        params,
    ).fetchall()

    rows = []
    for r in records:
        rows.append({
            "RTV ID": r.rtv_id or "",
            "RTV Date": str(r.rtv_date or ""),
            "Factory Unit": r.factory_unit or "",
            "Customer": r.customer or "",
            "Invoice Number": r.invoice_number or "",
            "Challan No": r.challan_no or "",
            "DN No": r.dn_no or "",
            "Conversion": str(r.conversion) if r.conversion is not None else "",
            "Sales POC": r.sales_poc or "",
            "Business Head": r.business_head or "",
            "Remark": r.remark or "",
            "Status": r.status or "",
            "Created By": r.created_by or "",
            "Created At": str(r.created_ts or ""),
            "Material Type": r.material_type or "",
            "Item Category": r.item_category or "",
            "Sub Category": r.sub_category or "",
            "Item Description": r.item_description or "",
            "UOM": r.uom or "",
            "Qty": float(r.qty) if r.qty is not None else "",
            "Rate": float(r.rate) if r.rate is not None else "",
            "Value": float(r.value) if r.value is not None else "",
            "Line Net Weight": float(r.line_net_weight) if r.line_net_weight is not None else "",
            "Line Carton Weight": float(r.line_carton_weight) if r.line_carton_weight is not None else "",
            "Box ID": r.box_id or "",
            "Box Article": r.box_article or "",
            "Box Number": r.box_number or "",
            "Box UOM": r.box_uom or "",
            "Box Conversion": r.box_conversion or "",
            "Box Net Weight": float(r.box_net_weight) if r.box_net_weight is not None else "",
            "Box Gross Weight": float(r.box_gross_weight) if r.box_gross_weight is not None else "",
            "Box Lot Number": r.box_lot_number or "",
            "Box Count": int(r.box_count) if r.box_count is not None else "",
        })
    return rows
