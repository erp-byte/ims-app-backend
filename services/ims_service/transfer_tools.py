import json
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any, Tuple

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger
from services.ims_service.transfer_models import (
    TransferRequestCreate,
    TransferCompleteCreate,
    ScannerInput,
    BoxScanData,
)

logger = get_logger("ims.transfer")


# ============================================
# HELPER FUNCTIONS
# ============================================


def _generate_request_no(db: Session) -> str:
    """Generate request number in format REQYYYYMMDDXXX"""
    result = db.execute(text("""
        SELECT 'REQ' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') ||
               LPAD(COALESCE(MAX(CAST(SUBSTRING(request_no FROM 12) AS INTEGER)), 0) + 1, 3, '0')
        FROM transfer_requests
        WHERE request_no LIKE 'REQ' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') || '%'
    """))
    return result.scalar()


def _generate_transfer_no(db: Session) -> str:
    """Generate transfer number in format TRANSYYYYMMDDXXX"""
    result = db.execute(text("""
        SELECT 'TRANS' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') ||
               LPAD(COALESCE(MAX(CAST(SUBSTRING(transfer_no FROM 10) AS INTEGER)), 0) + 1, 3, '0')
        FROM transfer_requests
        WHERE transfer_no LIKE 'TRANS' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') || '%'
    """))
    return result.scalar()


def _get_transfer_with_details(db: Session, transfer_id: int) -> Optional[Dict[str, Any]]:
    """Get transfer request with all related details"""
    query = text("""
        SELECT
            tr.id,
            tr.request_no,
            tr.transfer_no,
            tr.request_date,
            tr.from_warehouse,
            tr.to_warehouse,
            tr.reason,
            tr.reason_description,
            tr.status,
            tr.created_by,
            tr.created_at,
            tr.updated_at,
            COALESCE(
                json_agg(
                    json_build_object(
                        'id', tri.id,
                        'line_number', tri.line_number,
                        'material_type', tri.material_type,
                        'item_category', tri.item_category,
                        'sub_category', tri.sub_category,
                        'item_description', tri.item_description,
                        'sku_id', tri.sku_id,
                        'quantity', tri.quantity,
                        'uom', tri.uom,
                        'pack_size', tri.pack_size,
                        'package_size', tri.package_size,
                        'net_weight', tri.net_weight
                    ) ORDER BY tri.line_number
                ) FILTER (WHERE tri.id IS NOT NULL),
                '[]'::json
            ) as items,
            COALESCE(
                json_agg(
                    json_build_object(
                        'id', tsb.id,
                        'box_id', tsb.box_id,
                        'transaction_no', tsb.transaction_no,
                        'sku_id', tsb.sku_id,
                        'box_number_in_array', tsb.box_number_in_array,
                        'box_number', tsb.box_number,
                        'item_description', tsb.item_description,
                        'net_weight', tsb.net_weight,
                        'gross_weight', tsb.gross_weight,
                        'scan_timestamp', tsb.scan_timestamp,
                        'qr_data', tsb.qr_data
                    ) ORDER BY tsb.box_number_in_array
                ) FILTER (WHERE tsb.id IS NOT NULL),
                '[]'::json
            ) as scanned_boxes,
            CASE
                WHEN ti.id IS NOT NULL THEN
                    json_build_object(
                        'id', ti.id,
                        'vehicle_number', ti.vehicle_number,
                        'vehicle_number_other', ti.vehicle_number_other,
                        'driver_name', ti.driver_name,
                        'driver_name_other', ti.driver_name_other,
                        'driver_phone', ti.driver_phone,
                        'approval_authority', ti.approval_authority,
                        'created_at', ti.created_at
                    )
                ELSE NULL
            END as transport_info
        FROM transfer_requests tr
        LEFT JOIN transfer_request_items tri ON tr.id = tri.transfer_id
        LEFT JOIN transfer_scanned_boxes tsb ON tr.id = tsb.transfer_id
        LEFT JOIN transfer_info ti ON tr.id = ti.transfer_id
        WHERE tr.id = :transfer_id
        GROUP BY tr.id, ti.id, ti.vehicle_number, ti.vehicle_number_other,
                 ti.driver_name, ti.driver_name_other, ti.driver_phone,
                 ti.approval_authority, ti.created_at
    """)

    result = db.execute(query, {"transfer_id": transfer_id}).fetchone()

    if result:
        return {
            "id": result.id,
            "request_no": result.request_no,
            "transfer_no": result.transfer_no,
            "request_date": result.request_date,
            "from_warehouse": result.from_warehouse,
            "to_warehouse": result.to_warehouse,
            "reason": result.reason,
            "reason_description": result.reason_description,
            "status": result.status,
            "created_by": result.created_by,
            "created_at": result.created_at,
            "updated_at": result.updated_at,
            "items": result.items,
            "scanned_boxes": result.scanned_boxes,
            "transport_info": result.transport_info,
        }

    return None


def _get_warehouse_addresses(db: Session, warehouse_codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """Get warehouse addresses for DC generation"""
    placeholders = ", ".join(f":code_{i}" for i in range(len(warehouse_codes)))
    params = {f"code_{i}": code for i, code in enumerate(warehouse_codes)}
    rows = db.execute(
        text(f"""
            SELECT warehouse_code, warehouse_name, address, city, state,
                   pincode, gstin, contact_person, contact_phone, contact_email
            FROM warehouse_master
            WHERE warehouse_code IN ({placeholders})
        """),
        params,
    ).fetchall()

    return {
        row.warehouse_code: {
            "code": row.warehouse_code,
            "name": row.warehouse_name,
            "address": row.address,
            "city": row.city,
            "state": row.state,
            "pincode": row.pincode,
            "gstin": row.gstin,
            "contact_person": row.contact_person,
            "contact_phone": row.contact_phone,
            "contact_email": row.contact_email,
        }
        for row in rows
    }


# ============================================
# WAREHOUSE ENDPOINTS
# ============================================


def get_warehouses(is_active: bool, db: Session) -> list:
    """Get all warehouses for dropdowns"""
    rows = db.execute(
        text("""
            SELECT id, warehouse_code, warehouse_name, address, city, state,
                   pincode, gstin, contact_person, contact_phone, contact_email,
                   is_active, created_at, updated_at
            FROM warehouse_master
            WHERE is_active = :is_active
            ORDER BY warehouse_name
        """),
        {"is_active": is_active},
    ).fetchall()

    return [
        {
            "id": row.id,
            "warehouse_code": row.warehouse_code,
            "warehouse_name": row.warehouse_name,
            "address": row.address,
            "city": row.city,
            "state": row.state,
            "pincode": row.pincode,
            "gstin": row.gstin,
            "contact_person": row.contact_person,
            "contact_phone": row.contact_phone,
            "contact_email": row.contact_email,
            "is_active": row.is_active,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }
        for row in rows
    ]


# ============================================
# TRANSFER REQUEST MANAGEMENT
# ============================================


def create_transfer_request(request_data: TransferRequestCreate, db: Session) -> dict:
    """Create a new transfer request with items"""
    # Use request_no from frontend if provided, otherwise generate one
    request_no = request_data.request_no if request_data.request_no else _generate_request_no(db)

    # Insert transfer request header
    header = db.execute(
        text("""
            INSERT INTO transfer_requests
                (request_no, request_date, from_warehouse, to_warehouse,
                 reason, reason_description, status, created_by)
            VALUES
                (:request_no, :request_date, :from_warehouse, :to_warehouse,
                 :reason, :reason_description, 'Pending', :created_by)
            RETURNING id, request_no
        """),
        {
            "request_no": request_no,
            "request_date": request_data.request_date,
            "from_warehouse": request_data.from_warehouse,
            "to_warehouse": request_data.to_warehouse,
            "reason": request_data.reason,
            "reason_description": request_data.reason_description,
            "created_by": request_data.created_by,
        },
    ).fetchone()

    request_id = header.id

    # Insert transfer request items
    for item_data in request_data.items:
        db.execute(
            text("""
                INSERT INTO transfer_request_items
                    (transfer_id, line_number, material_type, item_category,
                     sub_category, item_description, sku_id, quantity,
                     uom, pack_size, package_size, net_weight)
                VALUES
                    (:transfer_id, :line_number, :material_type, :item_category,
                     :sub_category, :item_description, :sku_id, :quantity,
                     :uom, :pack_size, :package_size, :net_weight)
            """),
            {
                "transfer_id": request_id,
                "line_number": item_data.line_number,
                "material_type": item_data.material_type,
                "item_category": item_data.item_category,
                "sub_category": item_data.sub_category,
                "item_description": item_data.item_description,
                "sku_id": item_data.sku_id,
                "quantity": float(item_data.quantity),
                "uom": item_data.uom,
                "pack_size": float(item_data.pack_size),
                "package_size": item_data.package_size,
                "net_weight": float(item_data.net_weight),
            },
        )

    return {
        "success": True,
        "message": "Transfer request created successfully",
        "data": {"request_no": request_no, "request_id": request_id},
    }


def get_transfer_requests(
    request_status: Optional[str],
    from_warehouse: Optional[str],
    to_warehouse: Optional[str],
    request_date_from: Optional[date],
    request_date_to: Optional[date],
    created_by: Optional[str],
    page: int,
    per_page: int,
    db: Session,
) -> dict:
    """Get transfer requests list with filtering and pagination"""
    clauses = ["1=1"]
    params: dict = {}

    if request_status:
        clauses.append("tr.status = :status")
        params["status"] = request_status
    if from_warehouse:
        clauses.append("tr.from_warehouse = :from_warehouse")
        params["from_warehouse"] = from_warehouse
    if to_warehouse:
        clauses.append("tr.to_warehouse = :to_warehouse")
        params["to_warehouse"] = to_warehouse
    if request_date_from:
        clauses.append("tr.request_date >= :date_from")
        params["date_from"] = request_date_from
    if request_date_to:
        clauses.append("tr.request_date <= :date_to")
        params["date_to"] = request_date_to
    if created_by:
        clauses.append("tr.created_by = :created_by")
        params["created_by"] = created_by

    where = " AND ".join(clauses)

    # Get total count
    total = db.execute(
        text(f"SELECT COUNT(*) FROM transfer_requests tr WHERE {where}"),
        params,
    ).scalar()

    # Get paginated results with item counts
    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    rows = db.execute(
        text(f"""
            SELECT
                tr.id, tr.request_no, tr.transfer_no, tr.request_date,
                tr.from_warehouse, tr.to_warehouse, tr.reason_description,
                tr.status, tr.created_by, tr.created_at,
                COUNT(tri.id) AS item_count
            FROM transfer_requests tr
            LEFT JOIN transfer_request_items tri ON tr.id = tri.transfer_id
            WHERE {where}
            GROUP BY tr.id
            ORDER BY tr.created_at DESC
            LIMIT :limit OFFSET :offset
        """),
        params,
    ).fetchall()

    request_list = [
        {
            "id": row.id,
            "request_no": row.request_no,
            "transfer_no": row.transfer_no,
            "request_date": row.request_date,
            "from_warehouse": row.from_warehouse,
            "to_warehouse": row.to_warehouse,
            "reason_description": row.reason_description,
            "status": row.status,
            "item_count": row.item_count or 0,
            "created_by": row.created_by,
            "created_at": row.created_at,
        }
        for row in rows
    ]

    return {
        "success": True,
        "message": "Transfer requests retrieved successfully",
        "data": request_list,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page if total else 0,
    }


def get_transfer_request_detail(request_id: int, db: Session) -> dict:
    """Get transfer request details by ID"""
    transfer_data = _get_transfer_with_details(db, request_id)

    if not transfer_data:
        raise HTTPException(status_code=404, detail="Transfer request not found")

    return {
        "success": True,
        "message": "Transfer request details retrieved successfully",
        "data": transfer_data,
    }


# ============================================
# TRANSFER SUBMISSION
# ============================================


def submit_transfer(transfer_data: TransferCompleteCreate, db: Session) -> dict:
    """Submit complete transfer with scanned boxes and transport details"""
    # Find the existing request
    existing_request = db.execute(
        text("""
            SELECT id, request_no, transfer_no, status
            FROM transfer_requests
            WHERE request_no = :request_no
        """),
        {"request_no": transfer_data.request_no},
    ).fetchone()

    if not existing_request:
        raise HTTPException(status_code=404, detail="Transfer request not found")

    request_id = existing_request.id

    # Generate transfer number if not exists
    if not existing_request.transfer_no:
        transfer_no = _generate_transfer_no(db)
        db.execute(
            text("""
                UPDATE transfer_requests
                SET transfer_no = :transfer_no, status = 'In Transit'
                WHERE id = :id
            """),
            {"transfer_no": transfer_no, "id": request_id},
        )
    else:
        transfer_no = existing_request.transfer_no
        db.execute(
            text("""
                UPDATE transfer_requests
                SET status = 'In Transit'
                WHERE id = :id
            """),
            {"id": request_id},
        )

    # Insert scanned boxes
    for box_data in transfer_data.scanned_boxes:
        db.execute(
            text("""
                INSERT INTO transfer_scanned_boxes
                    (transfer_id, box_id, transaction_no, sku_id,
                     box_number_in_array, box_number, item_description,
                     net_weight, gross_weight, qr_data)
                VALUES
                    (:transfer_id, :box_id, :transaction_no, :sku_id,
                     :box_number_in_array, :box_number, :item_description,
                     :net_weight, :gross_weight, :qr_data::json)
            """),
            {
                "transfer_id": request_id,
                "box_id": box_data.box_id,
                "transaction_no": box_data.transaction_no,
                "sku_id": box_data.sku_id,
                "box_number_in_array": box_data.box_number_in_array,
                "box_number": box_data.box_number,
                "item_description": box_data.item_description,
                "net_weight": float(box_data.net_weight),
                "gross_weight": float(box_data.gross_weight),
                "qr_data": json.dumps(box_data.qr_data) if box_data.qr_data else None,
            },
        )

    # Insert transport info
    db.execute(
        text("""
            INSERT INTO transfer_info
                (transfer_id, vehicle_number, vehicle_number_other,
                 driver_name, driver_name_other, driver_phone, approval_authority)
            VALUES
                (:transfer_id, :vehicle_number, :vehicle_number_other,
                 :driver_name, :driver_name_other, :driver_phone, :approval_authority)
        """),
        {
            "transfer_id": request_id,
            "vehicle_number": transfer_data.transport_info.vehicle_number,
            "vehicle_number_other": transfer_data.transport_info.vehicle_number_other,
            "driver_name": transfer_data.transport_info.driver_name,
            "driver_name_other": transfer_data.transport_info.driver_name_other,
            "driver_phone": transfer_data.transport_info.driver_phone,
            "approval_authority": transfer_data.transport_info.approval_authority,
        },
    )

    return {
        "success": True,
        "message": "Transfer submitted successfully",
        "data": {
            "request_no": transfer_data.request_no,
            "transfer_no": transfer_no,
            "status": "In Transit",
            "scanned_boxes_count": len(transfer_data.scanned_boxes),
        },
    }


# ============================================
# SCANNER
# ============================================


def resolve_scanner_input(scanner_input: ScannerInput, db: Session) -> dict:
    """Resolve scanned box/lot/batch information"""
    scan_value = scanner_input.scan_value.strip()

    if scan_value.startswith("TX"):
        box_data = BoxScanData(
            scan_value=scan_value,
            resolved_box=f"BOX{scan_value[-2:]}",
            resolved_lot=f"LOT{scan_value[-4:-2]}",
            resolved_batch=f"BATCH{scan_value[-6:-4]}",
            sku_id="SKU001234",
            sku_name="Wheat Flour 1kg",
            material_type="RM",
            uom="KG",
            available_qty=Decimal("100.000"),
            expiry_date=date(2024, 2, 15),
            fefo_priority=1,
        )

        return {
            "success": True,
            "message": "Scan resolved successfully",
            "data": box_data.model_dump(),
        }
    else:
        return {
            "success": False,
            "message": "Invalid scan format. Expected transaction number starting with 'TX'",
            "data": None,
        }


# ============================================
# DC GENERATION
# ============================================


def get_dc_data(company: str, transfer_no: str, db: Session) -> dict:
    """Get delivery challan data for DC generation"""
    # Get transfer request with transfer number
    transfer_request = db.execute(
        text("""
            SELECT id, request_no, transfer_no, request_date,
                   from_warehouse, to_warehouse
            FROM transfer_requests
            WHERE transfer_no = :transfer_no
        """),
        {"transfer_no": transfer_no},
    ).fetchone()

    if not transfer_request:
        raise HTTPException(status_code=404, detail="Transfer not found")

    # Get warehouse addresses
    warehouse_codes = [transfer_request.from_warehouse, transfer_request.to_warehouse]
    warehouse_addresses = _get_warehouse_addresses(db, warehouse_codes)

    # Get items
    items = db.execute(
        text("""
            SELECT line_number, material_type, item_category, sub_category,
                   item_description, sku_id, quantity, uom, pack_size,
                   package_size, net_weight
            FROM transfer_request_items
            WHERE transfer_id = :transfer_id
            ORDER BY line_number
        """),
        {"transfer_id": transfer_request.id},
    ).fetchall()

    # Get scanned boxes
    scanned_boxes = db.execute(
        text("""
            SELECT box_id, transaction_no, sku_id, box_number,
                   item_description, net_weight, gross_weight
            FROM transfer_scanned_boxes
            WHERE transfer_id = :transfer_id
            ORDER BY box_number_in_array
        """),
        {"transfer_id": transfer_request.id},
    ).fetchall()

    # Get transport info
    transport_info = db.execute(
        text("""
            SELECT vehicle_number, vehicle_number_other, driver_name,
                   driver_name_other, driver_phone, approval_authority
            FROM transfer_info
            WHERE transfer_id = :transfer_id
        """),
        {"transfer_id": transfer_request.id},
    ).fetchone()

    if not transport_info:
        raise HTTPException(status_code=400, detail="Transport information not found")

    return {
        "transfer_no": transfer_request.transfer_no,
        "request_no": transfer_request.request_no,
        "request_date": transfer_request.request_date,
        "from_warehouse": warehouse_addresses.get(transfer_request.from_warehouse, {}),
        "to_warehouse": warehouse_addresses.get(transfer_request.to_warehouse, {}),
        "items": [
            {
                "line_number": item.line_number,
                "material_type": item.material_type,
                "item_category": item.item_category,
                "sub_category": item.sub_category,
                "item_description": item.item_description,
                "sku_id": item.sku_id,
                "quantity": item.quantity,
                "uom": item.uom,
                "pack_size": item.pack_size,
                "package_size": item.package_size,
                "net_weight": item.net_weight,
            }
            for item in items
        ],
        "scanned_boxes": [
            {
                "box_id": box.box_id,
                "transaction_no": box.transaction_no,
                "sku_id": box.sku_id,
                "box_number": box.box_number,
                "item_description": box.item_description,
                "net_weight": box.net_weight,
                "gross_weight": box.gross_weight,
            }
            for box in scanned_boxes
        ],
        "transport_info": {
            "vehicle_number": transport_info.vehicle_number,
            "vehicle_number_other": transport_info.vehicle_number_other,
            "driver_name": transport_info.driver_name,
            "driver_name_other": transport_info.driver_name_other,
            "driver_phone": transport_info.driver_phone,
            "approval_authority": transport_info.approval_authority,
        },
    }


# ============================================
# UTILITY
# ============================================


def get_status_options() -> dict:
    """Get available status options for transfer requests"""
    return {
        "success": True,
        "message": "Status options retrieved successfully",
        "data": [
            {"value": "Pending", "label": "Pending"},
            {"value": "Approved", "label": "Approved"},
            {"value": "Rejected", "label": "Rejected"},
            {"value": "In Transit", "label": "In Transit"},
            {"value": "Completed", "label": "Completed"},
        ],
    }


def get_material_types() -> dict:
    """Get available material types"""
    return {
        "success": True,
        "message": "Material types retrieved successfully",
        "data": [
            {"value": "RM", "label": "Raw Material"},
            {"value": "PM", "label": "Packaging Material"},
            {"value": "FG", "label": "Finished Good"},
            {"value": "SFG", "label": "Semi-Finished Good"},
        ],
    }
