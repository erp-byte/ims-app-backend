import time
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger
from services.bulk_entry_service.models import Company

logger = get_logger("bulk_entry")


def table_names(company: Company) -> dict:
    prefix = "cfpl" if company == "CFPL" else "cdpl"
    return {
        "tx": f"{prefix}_bulk_entry_transactions",
        "art": f"{prefix}_bulk_entry_articles",
        "box": f"{prefix}_bulk_entry_boxes",
    }


def _clean_date_fields(data: dict) -> dict:
    """Convert empty-string dates to None so Postgres doesn't choke."""
    for key in ("entry_date", "system_grn_date", "manufacturing_date", "expiry_date"):
        if key in data and data[key] == "":
            data[key] = None
    return data


def _safe_str(val):
    return str(val) if val is not None else None


def _safe_float(val):
    return float(val) if val is not None else None


# ── Create (bulk entry with immediate box_id generation) ──────


def create_bulk_entry(payload, db: Session) -> dict:
    from services.bulk_entry_service.models import (
        BulkEntryResponse,
        ArticleBoxGroup,
        GeneratedBoxInfo,
    )

    tables = table_names(payload.company)
    t = payload.transaction
    txno = t.transaction_no

    if not txno:
        raise HTTPException(400, "transaction.transaction_no is required")

    for a in payload.articles:
        if a.transaction_no != txno:
            raise HTTPException(400, f"Article '{a.item_description}' has mismatched transaction_no")

    # 1) Insert transaction
    tx_data = _clean_date_fields(t.model_dump())
    result = db.execute(
        text(f"""
            INSERT INTO {tables['tx']} (
                transaction_no, entry_date, vehicle_number, transporter_name, lr_number,
                vendor_supplier_name, customer_party_name, source_location, destination_location,
                challan_number, invoice_number, po_number, grn_number, grn_quantity, system_grn_date,
                purchased_by, service_invoice_number, dn_number, approval_authority,
                total_amount, tax_amount, discount_amount, po_quantity, remark, currency,
                warehouse, status
            ) VALUES (
                :transaction_no, :entry_date, :vehicle_number, :transporter_name, :lr_number,
                :vendor_supplier_name, :customer_party_name, :source_location, :destination_location,
                :challan_number, :invoice_number, :po_number, :grn_number, :grn_quantity, :system_grn_date,
                :purchased_by, :service_invoice_number, :dn_number, :approval_authority,
                :total_amount, :tax_amount, :discount_amount, :po_quantity, :remark, :currency,
                :warehouse, 'pending'
            )
            ON CONFLICT (transaction_no) DO NOTHING
        """),
        tx_data,
    )
    if result.rowcount == 0:
        raise HTTPException(409, f"transaction_no '{txno}' already exists")

    # 2) Insert articles
    _ARTICLE_COLUMNS = (
        "transaction_no, sku_id, item_description, item_category, sub_category, "
        "material_type, quality_grade, uom, po_quantity, units, quantity_units, "
        "net_weight, total_weight, po_weight, lot_number, manufacturing_date, "
        "expiry_date, unit_rate, total_amount, carton_weight, box_count"
    )
    _ARTICLE_PARAMS = (
        ":transaction_no, :sku_id, :item_description, :item_category, :sub_category, "
        ":material_type, :quality_grade, :uom, :po_quantity, :units, :quantity_units, "
        ":net_weight, :total_weight, :po_weight, :lot_number, :manufacturing_date, "
        ":expiry_date, :unit_rate, :total_amount, :carton_weight, :box_count"
    )

    if payload.articles:
        articles_data = []
        for a in payload.articles:
            d = _clean_date_fields(a.model_dump())
            d.pop("box_net_weight", None)
            d.pop("box_gross_weight", None)
            articles_data.append(d)
        db.execute(
            text(f"""
                INSERT INTO {tables['art']} ({_ARTICLE_COLUMNS})
                VALUES ({_ARTICLE_PARAMS})
                ON CONFLICT (transaction_no, item_description) DO NOTHING
            """),
            articles_data,
        )

    # 3) Generate boxes with immediate box_id
    base = str(int(time.time() * 1000))[-8:]
    global_counter = 0
    all_box_groups = []

    for article in payload.articles:
        boxes_data = []
        box_ids = []
        box_infos = []

        for box_num in range(1, article.box_count + 1):
            global_counter += 1
            box_id = f"{base}-{global_counter}"

            boxes_data.append({
                "transaction_no": txno,
                "article_description": article.item_description,
                "box_number": box_num,
                "net_weight": article.box_net_weight,
                "gross_weight": article.box_gross_weight,
                "lot_number": article.lot_number,
                "count": article.box_count,
                "box_id": box_id,
            })
            box_ids.append(box_id)
            box_infos.append(GeneratedBoxInfo(
                box_number=box_num,
                box_id=box_id,
                article_description=article.item_description,
                net_weight=_safe_float(article.box_net_weight),
                gross_weight=_safe_float(article.box_gross_weight),
                lot_number=article.lot_number,
            ))

        if boxes_data:
            db.execute(
                text(f"""
                    INSERT INTO {tables['box']} (
                        transaction_no, article_description, box_number,
                        net_weight, gross_weight, lot_number, count, box_id
                    ) VALUES (
                        :transaction_no, :article_description, :box_number,
                        :net_weight, :gross_weight, :lot_number, :count, :box_id
                    )
                    ON CONFLICT (transaction_no, article_description, box_number) DO NOTHING
                """),
                boxes_data,
            )

        all_box_groups.append(ArticleBoxGroup(
            article_description=article.item_description,
            box_ids=box_ids,
            boxes=box_infos,
        ))

    db.commit()
    logger.info("Created bulk entry [%s]: %s with %s boxes", payload.company, txno, global_counter)

    return BulkEntryResponse(
        status="ok",
        transaction_no=txno,
        company=payload.company,
        articles_count=len(payload.articles),
        total_boxes_created=global_counter,
        articles_with_boxes=all_box_groups,
    )


# ── List ──────────────────────────────────────


def list_bulk_entries(
    company: Company,
    page: int,
    per_page: int,
    status: Optional[str],
    vendor: Optional[str],
    source_location: Optional[str],
    search: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    sort_by: str,
    sort_order: str,
    db: Session,
) -> dict:
    tables = table_names(company)
    conditions = []
    params: dict = {}

    if status:
        conditions.append("status = :status")
        params["status"] = status

    if vendor:
        conditions.append("vendor_supplier_name ILIKE :vendor")
        params["vendor"] = f"%{vendor}%"

    if source_location:
        conditions.append("source_location ILIKE :source_location")
        params["source_location"] = f"%{source_location}%"

    if search:
        conditions.append(
            "(transaction_no ILIKE :search "
            "OR vendor_supplier_name ILIKE :search "
            "OR customer_party_name ILIKE :search "
            "OR source_location ILIKE :search "
            "OR destination_location ILIKE :search "
            "OR invoice_number ILIKE :search "
            "OR po_number ILIKE :search "
            "OR challan_number ILIKE :search "
            "OR remark ILIKE :search "
            "OR warehouse ILIKE :search)"
        )
        params["search"] = f"%{search}%"

    if from_date:
        conditions.append("entry_date >= :from_date")
        params["from_date"] = from_date

    if to_date:
        conditions.append("entry_date <= :to_date")
        params["to_date"] = to_date

    where = " AND ".join(conditions) if conditions else "1=1"

    allowed_sort = {
        "transaction_no", "entry_date", "vendor_supplier_name",
        "source_location", "status", "created_at", "total_amount",
    }
    if sort_by not in allowed_sort:
        sort_by = "created_at"
    if sort_order.lower() not in ("asc", "desc"):
        sort_order = "desc"

    count_row = db.execute(
        text(f"SELECT COUNT(*) AS cnt FROM {tables['tx']} WHERE {where}"),
        params,
    ).mappings().first()
    total = count_row["cnt"]

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    rows = db.execute(
        text(
            f"SELECT * FROM {tables['tx']} "
            f"WHERE {where} "
            f"ORDER BY {sort_by} {sort_order} "
            f"LIMIT :limit OFFSET :offset"
        ),
        params,
    ).fetchall()

    total_pages = (total + per_page - 1) // per_page if per_page > 0 else 0

    return {
        "records": [_map_tx_row(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
    }


# ── Get single entry (transaction + articles + boxes) ─────────


def get_bulk_entry(company: Company, transaction_no: str, db: Session) -> dict:
    tables = table_names(company)

    tx = db.execute(
        text(f"SELECT * FROM {tables['tx']} WHERE transaction_no = :txno"),
        {"txno": transaction_no},
    ).fetchone()
    if not tx:
        raise HTTPException(404, f"transaction_no '{transaction_no}' not found for {company}")

    articles = db.execute(
        text(f"SELECT * FROM {tables['art']} WHERE transaction_no = :txno ORDER BY id"),
        {"txno": transaction_no},
    ).fetchall()

    boxes = db.execute(
        text(f"SELECT * FROM {tables['box']} WHERE transaction_no = :txno ORDER BY article_description, box_number"),
        {"txno": transaction_no},
    ).fetchall()

    return {
        "transaction": _map_tx_row(tx),
        "articles": [_map_art_row(a) for a in articles],
        "boxes": [_map_box_row(b) for b in boxes],
    }


# ── Update transaction ────────────────────────


def update_bulk_entry(company: Company, transaction_no: str, payload, db: Session) -> dict:
    tables = table_names(company)

    EDITABLE = {
        "vehicle_number", "transporter_name", "lr_number",
        "vendor_supplier_name", "customer_party_name",
        "source_location", "destination_location",
        "challan_number", "invoice_number", "po_number",
        "grn_number", "grn_quantity", "system_grn_date",
        "purchased_by", "service_invoice_number", "dn_number",
        "approval_authority", "total_amount", "tax_amount",
        "discount_amount", "po_quantity", "remark", "currency", "warehouse",
    }

    # ── Fetch current transaction state ──
    existing_tx = db.execute(
        text(f"SELECT * FROM {tables['tx']} WHERE transaction_no = :txno"),
        {"txno": transaction_no},
    ).fetchone()
    if not existing_tx:
        raise HTTPException(404, f"Transaction not found for {company}")

    has_changes = False

    # ── 1) Transaction: state-compare, only update changed fields ──
    if payload.transaction:
        tx_data = payload.transaction.model_dump(exclude_none=True)
        old_tx = dict(existing_tx._mapping)
        tx_changes = {}
        for field, new_val in tx_data.items():
            if field not in EDITABLE:
                continue
            old_val = old_tx.get(field)
            if old_val is not None and new_val is not None:
                try:
                    if float(old_val) == float(new_val):
                        continue
                except (TypeError, ValueError):
                    pass
            if str(old_val) != str(new_val):
                tx_changes[field] = new_val

        if tx_changes:
            set_clauses = ", ".join(f"{k} = :{k}" for k in tx_changes)
            set_clauses += ", updated_at = NOW()"
            tx_changes["txno"] = transaction_no
            db.execute(
                text(
                    f"UPDATE {tables['tx']} SET {set_clauses} "
                    f"WHERE transaction_no = :txno"
                ),
                tx_changes,
            )
            has_changes = True

    # ── 2) Articles: upsert by (transaction_no, item_description) ──
    if payload.articles:
        existing_arts = db.execute(
            text(f"SELECT * FROM {tables['art']} WHERE transaction_no = :txno"),
            {"txno": transaction_no},
        ).fetchall()
        existing_art_map = {r.item_description: dict(r._mapping) for r in existing_arts}

        _ARTICLE_COLUMNS = (
            "transaction_no, sku_id, item_description, item_category, sub_category, "
            "material_type, quality_grade, uom, po_quantity, units, quantity_units, "
            "net_weight, total_weight, po_weight, lot_number, manufacturing_date, "
            "expiry_date, unit_rate, total_amount, carton_weight, box_count"
        )
        _ARTICLE_PARAMS = (
            ":transaction_no, :sku_id, :item_description, :item_category, :sub_category, "
            ":material_type, :quality_grade, :uom, :po_quantity, :units, :quantity_units, "
            ":net_weight, :total_weight, :po_weight, :lot_number, :manufacturing_date, "
            ":expiry_date, :unit_rate, :total_amount, :carton_weight, :box_count"
        )

        for article in payload.articles:
            art_data = _clean_date_fields(article.model_dump())
            art_data.pop("box_net_weight", None)
            art_data.pop("box_gross_weight", None)
            art_key = art_data["item_description"]

            if art_key in existing_art_map:
                old_art = existing_art_map[art_key]
                art_changes = {}
                for field, new_val in art_data.items():
                    if field in ("transaction_no", "item_description"):
                        continue
                    if new_val is None:
                        continue
                    old_val = old_art.get(field)
                    if old_val is not None and new_val is not None:
                        try:
                            if float(old_val) == float(new_val):
                                continue
                        except (TypeError, ValueError):
                            pass
                    if str(old_val) != str(new_val):
                        art_changes[field] = new_val

                if art_changes:
                    set_parts = [f"{k} = :{k}" for k in art_changes]
                    set_parts.append("updated_at = NOW()")
                    art_changes["txno"] = transaction_no
                    art_changes["item_desc"] = art_key
                    db.execute(
                        text(
                            f"UPDATE {tables['art']} SET {', '.join(set_parts)} "
                            f"WHERE transaction_no = :txno AND item_description = :item_desc"
                        ),
                        art_changes,
                    )
                    has_changes = True
            else:
                db.execute(
                    text(f"INSERT INTO {tables['art']} ({_ARTICLE_COLUMNS}) VALUES ({_ARTICLE_PARAMS})"),
                    [art_data],
                )
                has_changes = True

    # ── 3) Boxes: upsert by (transaction_no, article_description, box_number) ──
    if payload.boxes:
        existing_boxes = db.execute(
            text(f"SELECT * FROM {tables['box']} WHERE transaction_no = :txno"),
            {"txno": transaction_no},
        ).fetchall()
        existing_box_map = {
            (r.article_description, r.box_number): dict(r._mapping) for r in existing_boxes
        }

        for box in payload.boxes:
            box_data = box.model_dump(exclude_none=True)
            art_desc = box_data["article_description"]
            box_num = box_data["box_number"]
            box_key = (art_desc, box_num)

            if box_key in existing_box_map:
                old_box = existing_box_map[box_key]
                box_changes = {}
                for field, new_val in box_data.items():
                    if field in ("article_description", "box_number"):
                        continue
                    old_val = old_box.get(field)
                    if old_val is not None and new_val is not None:
                        try:
                            if float(old_val) == float(new_val):
                                continue
                        except (TypeError, ValueError):
                            pass
                    if str(old_val) != str(new_val):
                        box_changes[field] = new_val

                if box_changes:
                    set_parts = [f"{k} = :{k}" for k in box_changes]
                    set_parts.append("updated_at = NOW()")
                    box_changes["txno"] = transaction_no
                    box_changes["art_desc"] = art_desc
                    box_changes["bn"] = box_num
                    db.execute(
                        text(
                            f"UPDATE {tables['box']} SET {', '.join(set_parts)} "
                            f"WHERE transaction_no = :txno AND article_description = :art_desc AND box_number = :bn"
                        ),
                        box_changes,
                    )
                    has_changes = True
            else:
                base = str(int(time.time() * 1000))[-8:]
                box_id = f"{base}-{box_num}"
                db.execute(
                    text(
                        f"INSERT INTO {tables['box']} "
                        f"(transaction_no, article_description, box_number, box_id, "
                        f"net_weight, gross_weight, lot_number, status) "
                        f"VALUES (:txno, :art, :bn, :box_id, :net_weight, :gross_weight, :lot_number, :status)"
                    ),
                    {
                        "txno": transaction_no,
                        "art": art_desc,
                        "bn": box_num,
                        "box_id": box_id,
                        "net_weight": box_data.get("net_weight"),
                        "gross_weight": box_data.get("gross_weight"),
                        "lot_number": box_data.get("lot_number"),
                        "status": box_data.get("status", "available"),
                    },
                )
                has_changes = True

    if has_changes:
        db.commit()

    # Re-fetch the updated transaction row
    updated_tx = db.execute(
        text(f"SELECT * FROM {tables['tx']} WHERE transaction_no = :txno"),
        {"txno": transaction_no},
    ).fetchone()

    logger.info("Updated bulk entry [%s]: %s", company, transaction_no)
    return _map_tx_row(updated_tx)


# ── Delete ────────────────────────────────────


def delete_bulk_entry(company: Company, transaction_no: str, db: Session) -> dict:
    tables = table_names(company)

    row = db.execute(
        text(f"DELETE FROM {tables['tx']} WHERE transaction_no = :txno RETURNING transaction_no"),
        {"txno": transaction_no},
    ).fetchone()

    if not row:
        raise HTTPException(404, f"Transaction not found for {company}")

    db.commit()
    logger.info("Deleted bulk entry [%s]: %s", company, transaction_no)
    return {"success": True, "message": "Entry deleted", "transaction_no": transaction_no}


# ── Box endpoints ─────────────────────────────


def list_boxes(company: Company, transaction_no: str, db: Session) -> dict:
    tables = table_names(company)

    tx = db.execute(
        text(f"SELECT transaction_no FROM {tables['tx']} WHERE transaction_no = :txno"),
        {"txno": transaction_no},
    ).fetchone()
    if not tx:
        raise HTTPException(404, f"Transaction not found for {company}")

    rows = db.execute(
        text(f"SELECT * FROM {tables['box']} WHERE transaction_no = :txno ORDER BY article_description, box_number"),
        {"txno": transaction_no},
    ).fetchall()

    return {"boxes": [_map_box_row(r) for r in rows], "total": len(rows)}


def lookup_box(company: Company, box_id: str, db: Session) -> dict:
    tables = table_names(company)

    row = db.execute(
        text(f"SELECT * FROM {tables['box']} WHERE box_id = :box_id"),
        {"box_id": box_id},
    ).fetchone()
    if not row:
        raise HTTPException(404, f"Box not found for {company}")
    return _map_box_row(row)


def upsert_box(company: Company, transaction_no: str, data: dict, db: Session) -> dict:
    tables = table_names(company)

    tx = db.execute(
        text(f"SELECT transaction_no FROM {tables['tx']} WHERE transaction_no = :txno"),
        {"txno": transaction_no},
    ).fetchone()
    if not tx:
        raise HTTPException(404, f"Transaction not found for {company}")

    article_desc = data["article_description"]
    box_number = data["box_number"]

    existing = db.execute(
        text(
            f"SELECT id, box_id FROM {tables['box']} "
            f"WHERE transaction_no = :txno AND article_description = :art AND box_number = :bn"
        ),
        {"txno": transaction_no, "art": article_desc, "bn": box_number},
    ).fetchone()

    net_weight = data.get("net_weight")
    gross_weight = data.get("gross_weight")
    lot_number = data.get("lot_number")
    new_status = data.get("status", "available")

    if existing and existing.box_id:
        box_id = existing.box_id
        status = "updated"
        db.execute(
            text(
                f"UPDATE {tables['box']} SET "
                f"net_weight = COALESCE(:net_weight, net_weight), "
                f"gross_weight = COALESCE(:gross_weight, gross_weight), "
                f"lot_number = COALESCE(:lot_number, lot_number), "
                f"status = :status, updated_at = NOW() "
                f"WHERE id = :id"
            ),
            {
                "net_weight": net_weight,
                "gross_weight": gross_weight,
                "lot_number": lot_number,
                "status": new_status,
                "id": existing.id,
            },
        )
    else:
        base = str(int(time.time() * 1000))[-8:]
        box_id = f"{base}-{box_number}"

        if existing:
            status = "updated"
            db.execute(
                text(
                    f"UPDATE {tables['box']} SET box_id = :box_id, "
                    f"net_weight = COALESCE(:net_weight, net_weight), "
                    f"gross_weight = COALESCE(:gross_weight, gross_weight), "
                    f"lot_number = COALESCE(:lot_number, lot_number), "
                    f"status = :status, updated_at = NOW() WHERE id = :id"
                ),
                {
                    "box_id": box_id,
                    "net_weight": net_weight,
                    "gross_weight": gross_weight,
                    "lot_number": lot_number,
                    "status": new_status,
                    "id": existing.id,
                },
            )
        else:
            status = "inserted"
            db.execute(
                text(
                    f"INSERT INTO {tables['box']} "
                    f"(transaction_no, article_description, box_number, box_id, "
                    f"net_weight, gross_weight, lot_number, status) "
                    f"VALUES (:txno, :art, :bn, :box_id, :net_weight, :gross_weight, :lot_number, :status)"
                ),
                {
                    "txno": transaction_no,
                    "art": article_desc,
                    "bn": box_number,
                    "box_id": box_id,
                    "net_weight": net_weight,
                    "gross_weight": gross_weight,
                    "lot_number": lot_number,
                    "status": new_status,
                },
            )

    db.commit()
    logger.info("Box upsert [%s] (%s) txno=%s art=%s box=%s id=%s", company, status, transaction_no, article_desc, box_number, box_id)
    return {
        "status": status,
        "box_id": box_id,
        "transaction_no": transaction_no,
        "article_description": article_desc,
        "box_number": box_number,
    }


# ── Row mappers ───────────────────────────────


def _map_tx_row(row) -> dict:
    return {
        "transaction_no": row.transaction_no,
        "entry_date": _safe_str(row.entry_date),
        "vehicle_number": row.vehicle_number,
        "transporter_name": row.transporter_name,
        "lr_number": row.lr_number,
        "vendor_supplier_name": row.vendor_supplier_name,
        "customer_party_name": row.customer_party_name,
        "source_location": row.source_location,
        "destination_location": row.destination_location,
        "challan_number": row.challan_number,
        "invoice_number": row.invoice_number,
        "po_number": row.po_number,
        "grn_number": row.grn_number,
        "grn_quantity": _safe_float(row.grn_quantity),
        "system_grn_date": _safe_str(row.system_grn_date),
        "purchased_by": row.purchased_by,
        "service_invoice_number": row.service_invoice_number,
        "dn_number": row.dn_number,
        "approval_authority": row.approval_authority,
        "total_amount": _safe_float(row.total_amount),
        "tax_amount": _safe_float(row.tax_amount),
        "discount_amount": _safe_float(row.discount_amount),
        "po_quantity": _safe_float(row.po_quantity),
        "remark": row.remark,
        "currency": row.currency,
        "warehouse": row.warehouse,
        "status": row.status,
        "approved_by": row.approved_by,
        "approved_at": _safe_str(row.approved_at),
        "created_at": _safe_str(row.created_at),
        "updated_at": _safe_str(row.updated_at),
    }


def _map_art_row(row) -> dict:
    return {
        "id": row.id,
        "transaction_no": row.transaction_no,
        "sku_id": row.sku_id,
        "item_description": row.item_description,
        "item_category": row.item_category,
        "sub_category": row.sub_category,
        "material_type": row.material_type,
        "quality_grade": row.quality_grade,
        "uom": row.uom,
        "units": row.units,
        "po_quantity": _safe_float(row.po_quantity),
        "quantity_units": _safe_float(row.quantity_units),
        "net_weight": _safe_float(row.net_weight),
        "total_weight": _safe_float(row.total_weight),
        "po_weight": _safe_float(row.po_weight),
        "lot_number": row.lot_number,
        "manufacturing_date": _safe_str(row.manufacturing_date),
        "expiry_date": _safe_str(row.expiry_date),
        "unit_rate": _safe_float(row.unit_rate),
        "total_amount": _safe_float(row.total_amount),
        "carton_weight": _safe_float(row.carton_weight),
        "box_count": row.box_count,
        "created_at": _safe_str(row.created_at),
        "updated_at": _safe_str(row.updated_at),
    }


def _map_box_row(row) -> dict:
    return {
        "id": row.id,
        "transaction_no": row.transaction_no,
        "article_description": row.article_description,
        "box_number": row.box_number,
        "box_id": row.box_id,
        "net_weight": _safe_float(row.net_weight),
        "gross_weight": _safe_float(row.gross_weight),
        "lot_number": row.lot_number,
        "count": row.count,
        "status": row.status,
        "created_at": _safe_str(row.created_at),
        "updated_at": _safe_str(row.updated_at),
    }
