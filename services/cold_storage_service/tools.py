import time
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger

logger = get_logger("cold_storage")

TABLE = "cold_storage_stocks"

EDITABLE_COLS = [
    "inward_dt", "unit", "inward_no", "item_mark", "vakkal", "lot_no",
    "no_of_cartons", "weight_kg", "total_inventory_kgs", "group_name",
    "item_description", "storage_location", "exporter", "last_purchase_rate",
    "value",
]

ALL_COLS = (
    "id, inward_dt, unit, inward_no, item_mark, vakkal, lot_no, "
    "no_of_cartons, weight_kg, total_inventory_kgs, group_name, "
    "item_description, storage_location, exporter, last_purchase_rate, "
    "value, created_at, updated_at"
)


def _map_row(row) -> dict:
    return {
        "id": row.id,
        "inward_dt": row.inward_dt,
        "unit": row.unit,
        "inward_no": row.inward_no,
        "item_mark": row.item_mark,
        "vakkal": row.vakkal,
        "lot_no": row.lot_no,
        "no_of_cartons": float(row.no_of_cartons) if row.no_of_cartons else None,
        "weight_kg": float(row.weight_kg) if row.weight_kg else None,
        "total_inventory_kgs": float(row.total_inventory_kgs) if row.total_inventory_kgs else None,
        "group_name": row.group_name,
        "item_description": row.item_description,
        "storage_location": row.storage_location,
        "exporter": row.exporter,
        "last_purchase_rate": float(row.last_purchase_rate) if row.last_purchase_rate else None,
        "value": float(row.value) if row.value else None,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def list_cold_storage(
    page: int,
    per_page: int,
    group_name: Optional[str],
    storage_location: Optional[str],
    exporter: Optional[str],
    item_mark: Optional[str],
    search: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    sort_by: str,
    sort_order: str,
    db: Session,
) -> dict:
    conditions = []
    params: dict = {}

    if group_name:
        conditions.append("group_name = :group_name")
        params["group_name"] = group_name

    if storage_location:
        conditions.append("storage_location = :storage_location")
        params["storage_location"] = storage_location

    if exporter:
        conditions.append("exporter = :exporter")
        params["exporter"] = exporter

    if item_mark:
        conditions.append("item_mark ILIKE :item_mark")
        params["item_mark"] = f"%{item_mark}%"

    if search:
        conditions.append(
            "(item_description ILIKE :search "
            "OR group_name ILIKE :search "
            "OR exporter ILIKE :search "
            "OR item_mark ILIKE :search "
            "OR inward_no ILIKE :search "
            "OR lot_no ILIKE :search "
            "OR vakkal ILIKE :search "
            "OR unit ILIKE :search "
            "OR storage_location ILIKE :search)"
        )
        params["search"] = f"%{search}%"

    if from_date:
        conditions.append("inward_dt >= :from_date")
        params["from_date"] = from_date

    if to_date:
        conditions.append("inward_dt <= :to_date")
        params["to_date"] = to_date

    where = " AND ".join(conditions) if conditions else "1=1"

    allowed_sort = {
        "id", "inward_dt", "group_name", "storage_location",
        "exporter", "total_inventory_kgs", "value", "created_at", "item_description",
    }
    if sort_by not in allowed_sort:
        sort_by = "id"
    if sort_order.lower() not in ("asc", "desc"):
        sort_order = "desc"

    count_row = db.execute(
        text(f"SELECT COUNT(*) AS cnt FROM {TABLE} WHERE {where}"),
        params,
    ).mappings().first()
    total = count_row["cnt"]

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    rows = db.execute(
        text(
            f"SELECT {ALL_COLS} FROM {TABLE} "
            f"WHERE {where} "
            f"ORDER BY {sort_by} {sort_order} "
            f"LIMIT :limit OFFSET :offset"
        ),
        params,
    ).fetchall()

    total_pages = (total + per_page - 1) // per_page if per_page > 0 else 0

    return {
        "records": [_map_row(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
    }


def get_cold_storage(record_id: int, db: Session) -> dict:
    row = db.execute(
        text(f"SELECT {ALL_COLS} FROM {TABLE} WHERE id = :id"),
        {"id": record_id},
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Record not found")

    return _map_row(row)


def create_cold_storage(data: dict, db: Session) -> dict:
    cols = [c for c in EDITABLE_COLS if data.get(c) is not None]
    col_names = ", ".join(cols)
    col_params = ", ".join(f":{c}" for c in cols)
    params = {c: data[c] for c in cols}

    row = db.execute(
        text(
            f"INSERT INTO {TABLE} ({col_names}) "
            f"VALUES ({col_params}) "
            f"RETURNING {ALL_COLS}"
        ),
        params,
    ).fetchone()

    _generate_boxes(row.id, row.no_of_cartons, row.item_description, row.lot_no, row.weight_kg, db)

    logger.info("Created cold storage record: %s", row.id)
    return _map_row(row)


def update_cold_storage(record_id: int, data: dict, db: Session) -> dict:
    updates = {k: v for k, v in data.items() if v is not None and k in EDITABLE_COLS}

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    set_clauses = ", ".join(f"{k} = :{k}" for k in updates)
    set_clauses += ", updated_at = NOW()"
    updates["id"] = record_id

    row = db.execute(
        text(
            f"UPDATE {TABLE} SET {set_clauses} "
            f"WHERE id = :id "
            f"RETURNING {ALL_COLS}"
        ),
        updates,
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Record not found")

    _reconcile_boxes(record_id, row, data, db)

    logger.info("Updated cold storage record: %s", record_id)
    return _map_row(row)


def delete_cold_storage(record_id: int, db: Session) -> dict:
    row = db.execute(
        text(f"DELETE FROM {TABLE} WHERE id = :id RETURNING id"),
        {"id": record_id},
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Record not found")

    logger.info("Deleted cold storage record: %s", record_id)
    return {"success": True, "message": "Record deleted", "id": record_id}


def bulk_create_cold_storage(records: list[dict], db: Session) -> dict:
    created = 0
    for data in records:
        cols = [c for c in EDITABLE_COLS if data.get(c) is not None]
        if not cols:
            continue
        col_names = ", ".join(cols)
        col_params = ", ".join(f":{c}" for c in cols)
        params = {c: data[c] for c in cols}
        row = db.execute(
            text(
                f"INSERT INTO {TABLE} ({col_names}) VALUES ({col_params}) "
                f"RETURNING id, no_of_cartons, item_description, lot_no, weight_kg"
            ),
            params,
        ).fetchone()
        _generate_boxes(row.id, row.no_of_cartons, row.item_description, row.lot_no, row.weight_kg, db)
        created += 1

    logger.info("Bulk created %s cold storage records", created)
    return {"status": "created", "records_created": created}


# ── Box helpers ──────────────────────────────

BOX_TABLE = "cold_storage_boxes"
BOX_COLS = (
    "id, stock_id, box_number, box_id, item_description, lot_no, "
    "weight_kg, status, created_at, updated_at"
)


def _map_box_row(row) -> dict:
    return {
        "id": row.id,
        "stock_id": row.stock_id,
        "box_number": row.box_number,
        "box_id": row.box_id,
        "item_description": row.item_description,
        "lot_no": row.lot_no,
        "weight_kg": float(row.weight_kg) if row.weight_kg else None,
        "status": row.status,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _generate_boxes(stock_id: int, no_of_cartons, item_description, lot_no, weight_kg, db: Session) -> int:
    if not no_of_cartons:
        return 0
    count = int(no_of_cartons)
    if count <= 0:
        return 0

    base = str(int(time.time() * 1000))[-8:]

    db.execute(
        text(
            f"INSERT INTO {BOX_TABLE} (stock_id, box_number, box_id, item_description, lot_no, weight_kg) "
            f"SELECT :stock_id, g.n, :base || '-' || g.n, :item_description, :lot_no, :weight_kg "
            f"FROM generate_series(1, :count) AS g(n) "
            f"ON CONFLICT (stock_id, box_number) DO NOTHING"
        ),
        {
            "stock_id": stock_id,
            "count": count,
            "base": base,
            "item_description": item_description,
            "lot_no": lot_no,
            "weight_kg": float(weight_kg) if weight_kg else None,
        },
    )
    logger.info("Generated %s boxes for stock_id=%s with base=%s", count, stock_id, base)
    return count


def _reconcile_boxes(stock_id: int, row, data: dict, db: Session):
    if "no_of_cartons" in data and row.no_of_cartons is not None:
        new_count = int(row.no_of_cartons)
        current_max = db.execute(
            text(f"SELECT COALESCE(MAX(box_number), 0) AS mx FROM {BOX_TABLE} WHERE stock_id = :sid"),
            {"sid": stock_id},
        ).scalar()

        if new_count > current_max:
            base = str(int(time.time() * 1000))[-8:]
            db.execute(
                text(
                    f"INSERT INTO {BOX_TABLE} (stock_id, box_number, box_id, item_description, lot_no, weight_kg) "
                    f"SELECT :sid, g.n, :base || '-' || g.n, :item_description, :lot_no, :weight_kg "
                    f"FROM generate_series(:start, :end) AS g(n) "
                    f"ON CONFLICT (stock_id, box_number) DO NOTHING"
                ),
                {
                    "sid": stock_id,
                    "start": current_max + 1,
                    "end": new_count,
                    "base": base,
                    "item_description": row.item_description,
                    "lot_no": row.lot_no,
                    "weight_kg": float(row.weight_kg) if row.weight_kg else None,
                },
            )
        elif new_count < current_max:
            db.execute(
                text(
                    f"DELETE FROM {BOX_TABLE} "
                    f"WHERE stock_id = :sid AND box_number > :new_count"
                ),
                {"sid": stock_id, "new_count": new_count},
            )

    if "item_description" in data or "lot_no" in data:
        db.execute(
            text(
                f"UPDATE {BOX_TABLE} SET "
                f"item_description = :item_description, lot_no = :lot_no, updated_at = NOW() "
                f"WHERE stock_id = :sid"
            ),
            {
                "sid": stock_id,
                "item_description": row.item_description,
                "lot_no": row.lot_no,
            },
        )


def list_boxes(stock_id: int, db: Session) -> dict:
    row = db.execute(
        text(f"SELECT id FROM {TABLE} WHERE id = :id"),
        {"id": stock_id},
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Stock record not found")

    rows = db.execute(
        text(f"SELECT {BOX_COLS} FROM {BOX_TABLE} WHERE stock_id = :sid ORDER BY box_number"),
        {"sid": stock_id},
    ).fetchall()

    return {"boxes": [_map_box_row(r) for r in rows], "total": len(rows)}


def upsert_box(stock_id: int, data: dict, db: Session) -> dict:
    stock = db.execute(
        text(f"SELECT id, item_description, lot_no, weight_kg FROM {TABLE} WHERE id = :id"),
        {"id": stock_id},
    ).fetchone()
    if not stock:
        raise HTTPException(status_code=404, detail="Stock record not found")

    box_number = data["box_number"]

    existing = db.execute(
        text(f"SELECT id, box_id FROM {BOX_TABLE} WHERE stock_id = :sid AND box_number = :bn"),
        {"sid": stock_id, "bn": box_number},
    ).fetchone()

    weight = data.get("weight_kg") or (float(stock.weight_kg) if stock.weight_kg else None)
    new_status = data.get("status", "available")

    if existing and existing.box_id:
        box_id = existing.box_id
        status = "updated"
        db.execute(
            text(
                f"UPDATE {BOX_TABLE} SET weight_kg = :weight, status = :status, updated_at = NOW() "
                f"WHERE id = :id"
            ),
            {"weight": weight, "status": new_status, "id": existing.id},
        )
    else:
        base = str(int(time.time() * 1000))[-8:]
        box_id = f"{base}-{box_number}"

        if existing:
            status = "updated"
            db.execute(
                text(
                    f"UPDATE {BOX_TABLE} SET box_id = :box_id, weight_kg = :weight, "
                    f"status = :status, updated_at = NOW() WHERE id = :id"
                ),
                {"box_id": box_id, "weight": weight, "status": new_status, "id": existing.id},
            )
        else:
            status = "inserted"
            db.execute(
                text(
                    f"INSERT INTO {BOX_TABLE} "
                    f"(stock_id, box_number, box_id, item_description, lot_no, weight_kg, status) "
                    f"VALUES (:sid, :bn, :box_id, :item_desc, :lot_no, :weight, :status)"
                ),
                {
                    "sid": stock_id,
                    "bn": box_number,
                    "box_id": box_id,
                    "item_desc": stock.item_description,
                    "lot_no": stock.lot_no,
                    "weight": weight,
                    "status": new_status,
                },
            )

    logger.info("Box upsert (%s) stock_id=%s box_number=%s box_id=%s", status, stock_id, box_number, box_id)
    return {"status": status, "box_id": box_id, "stock_id": stock_id, "box_number": box_number}


def lookup_box(box_id: str, db: Session) -> dict:
    row = db.execute(
        text(f"SELECT {BOX_COLS} FROM {BOX_TABLE} WHERE box_id = :box_id"),
        {"box_id": box_id},
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Box not found")
    return _map_box_row(row)


def get_cold_storage_summary(
    group_name: Optional[str],
    storage_location: Optional[str],
    exporter: Optional[str],
    db: Session,
) -> dict:
    conditions = []
    params: dict = {}

    if group_name:
        conditions.append("group_name = :group_name")
        params["group_name"] = group_name

    if storage_location:
        conditions.append("storage_location = :storage_location")
        params["storage_location"] = storage_location

    if exporter:
        conditions.append("exporter = :exporter")
        params["exporter"] = exporter

    where = " AND ".join(conditions) if conditions else "1=1"

    rows = db.execute(
        text(
            f"SELECT "
            f"  COALESCE(group_name, 'Unspecified') AS group_name, "
            f"  COUNT(*) AS total_records, "
            f"  COALESCE(SUM(no_of_cartons), 0) AS total_cartons, "
            f"  COALESCE(SUM(total_inventory_kgs), 0) AS total_inventory_kgs, "
            f"  COALESCE(SUM(value), 0) AS total_value "
            f"FROM {TABLE} "
            f"WHERE {where} "
            f"GROUP BY group_name "
            f"ORDER BY total_value DESC"
        ),
        params,
    ).fetchall()

    summary = [
        {
            "group_name": r.group_name,
            "total_records": r.total_records,
            "total_cartons": float(r.total_cartons),
            "total_inventory_kgs": float(r.total_inventory_kgs),
            "total_value": float(r.total_value),
        }
        for r in rows
    ]

    grand_total_records = sum(s["total_records"] for s in summary)
    grand_total_kgs = sum(s["total_inventory_kgs"] for s in summary)
    grand_total_value = sum(s["total_value"] for s in summary)

    return {
        "summary": summary,
        "grand_total_records": grand_total_records,
        "grand_total_inventory_kgs": grand_total_kgs,
        "grand_total_value": grand_total_value,
    }


# ── Approve ──────────────────────────────────


def approve_cold_storage(record_id: int, approved_by: str, db: Session) -> dict:
    from datetime import datetime

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    row = db.execute(
        text(
            f"UPDATE {TABLE} "
            f"SET status = 'approved', approved_by = :approved_by, "
            f"approved_at = :approved_at, updated_at = NOW() "
            f"WHERE id = :id "
            f"RETURNING id"
        ),
        {"id": record_id, "approved_by": approved_by, "approved_at": now},
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Record not found")

    db.commit()
    logger.info("Approved cold storage record: %s by %s", record_id, approved_by)
    return {
        "status": "approved",
        "id": record_id,
        "approved_by": approved_by,
        "approved_at": now,
    }


# ── Bulk delete ──────────────────────────────


def bulk_delete_cold_storage(record_ids: list[int], db: Session) -> dict:
    if not record_ids:
        raise HTTPException(status_code=400, detail="No record IDs provided")

    # Delete boxes first (FK), then stock records
    db.execute(
        text(f"DELETE FROM {BOX_TABLE} WHERE stock_id = ANY(:ids)"),
        {"ids": record_ids},
    )
    result = db.execute(
        text(f"DELETE FROM {TABLE} WHERE id = ANY(:ids) RETURNING id"),
        {"ids": record_ids},
    )
    deleted = result.rowcount

    db.commit()
    logger.info("Bulk deleted %s cold storage records", deleted)
    return {"success": True, "message": f"{deleted} record(s) deleted", "deleted_count": deleted}
