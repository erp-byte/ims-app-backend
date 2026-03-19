from typing import Optional

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger

logger = get_logger("competitor")


# ── Mapping constants ────────────────────────────────────────────

SHELF_SHARE_MAP = {
    "very_low": 10.0,
    "low": 25.0,
    "about_same": 50.0,
    "high": 75.0,
    "very_high": 90.0,
}

FOOTFALL_MAP = {
    "less_than_100": 50,
    "100_to_300": 200,
    "300_to_500": 400,
    "above_500": 600,
}

COMPETITOR_EDITABLE = {"name", "category", "logo_url", "website", "is_active"}
PRODUCT_EDITABLE = {
    "product_name", "ean", "category", "sub_category",
    "size_kg", "mrp", "selling_price", "our_equivalent_ean",
}
PROMOTION_EDITABLE = {
    "description", "promotion_type", "start_date", "end_date", "is_active", "photo_url",
}


# ── Helpers ──────────────────────────────────────────────────────

def _paginate(total: int, per_page: int) -> int:
    return (total + per_page - 1) // per_page if per_page > 0 else 0


def _row_to_dict(row) -> dict:
    return dict(row._mapping) if row else {}


def _str_fields(d: dict) -> dict:
    """Convert UUID / datetime / Decimal values to strings for JSON response."""
    out = {}
    for k, v in d.items():
        if v is None:
            out[k] = None
        elif hasattr(v, "hex"):  # UUID
            out[k] = str(v)
        elif hasattr(v, "isoformat"):  # datetime / date
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def resolve_or_create_competitor(name: str, promoter_id: str, db: Session) -> str:
    """Find competitor by case-insensitive name or create unverified entry. Returns UUID str."""
    row = db.execute(
        text("SELECT id FROM competitors WHERE LOWER(name) = LOWER(:name) LIMIT 1"),
        {"name": name.strip()},
    ).fetchone()
    if row:
        return str(row.id)

    new = db.execute(
        text(
            "INSERT INTO competitors (name, is_verified, created_by, created_by_role) "
            "VALUES (:name, false, :pid, 'promoter') RETURNING id"
        ),
        {"name": name.strip(), "pid": promoter_id},
    ).fetchone()
    logger.info("Auto-created unverified competitor '%s'", name.strip())
    return str(new.id)


def resolve_or_create_product(
    product_name: str, competitor_id: str, promoter_id: str, db: Session,
) -> str:
    """Find product by case-insensitive name within competitor or create unverified entry."""
    row = db.execute(
        text(
            "SELECT id FROM competitor_products "
            "WHERE competitor_id = :cid AND LOWER(product_name) = LOWER(:name) LIMIT 1"
        ),
        {"cid": competitor_id, "name": product_name.strip()},
    ).fetchone()
    if row:
        return str(row.id)

    new = db.execute(
        text(
            "INSERT INTO competitor_products (competitor_id, product_name, is_verified, created_by) "
            "VALUES (:cid, :name, false, :pid) RETURNING id"
        ),
        {"cid": competitor_id, "name": product_name.strip(), "pid": promoter_id},
    ).fetchone()
    logger.info("Auto-created unverified product '%s' for competitor %s", product_name.strip(), competitor_id)
    return str(new.id)


def get_active_attendance(promoter_id, db: Session) -> dict:
    """Get active attendance for store_name and attendance_id. Raises 400 if not punched in."""
    row = db.execute(
        text(
            "SELECT id, punch_in_store FROM attendance "
            "WHERE promoter_id = :pid AND punch_out_timestamp IS NULL "
            "ORDER BY punch_in_timestamp DESC LIMIT 1"
        ),
        {"pid": str(promoter_id)},
    ).fetchone()
    if not row:
        raise HTTPException(400, "No active punch-in session. Please punch in first.")
    return {"attendance_id": str(row.id), "store_name": row.punch_in_store}


# ── Competitor CRUD (Admin) ──────────────────────────────────────

def create_competitor(data, user: dict, db: Session) -> dict:
    row = db.execute(
        text(
            "INSERT INTO competitors (name, category, logo_url, website, is_active, "
            "is_verified, created_by, created_by_role) "
            "VALUES (:name, :category, :logo_url, :website, :is_active, "
            "true, :created_by, 'admin') "
            "RETURNING *"
        ),
        {
            "name": data.name.strip(),
            "category": data.category,
            "logo_url": data.logo_url,
            "website": data.website,
            "is_active": data.is_active,
            "created_by": user["user_id"],
        },
    ).fetchone()
    logger.info("Admin created competitor '%s'", data.name)
    return _str_fields(_row_to_dict(row))


def list_competitors(
    page: int, per_page: int, search: Optional[str], is_verified: Optional[bool], db: Session,
) -> dict:
    conditions = []
    params: dict = {}

    if search:
        conditions.append("name ILIKE :search")
        params["search"] = f"%{search}%"
    if is_verified is not None:
        conditions.append("is_verified = :verified")
        params["verified"] = is_verified

    where = " AND ".join(conditions) if conditions else "1=1"

    total = db.execute(
        text(f"SELECT COUNT(*) AS cnt FROM competitors WHERE {where}"), params,
    ).scalar()

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    rows = db.execute(
        text(
            f"SELECT * FROM competitors WHERE {where} "
            f"ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
        ),
        params,
    ).fetchall()

    return {
        "records": [_str_fields(_row_to_dict(r)) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": _paginate(total, per_page),
    }


def get_competitor(competitor_id: str, db: Session) -> dict:
    row = db.execute(
        text("SELECT * FROM competitors WHERE id = :id"), {"id": competitor_id},
    ).fetchone()
    if not row:
        raise HTTPException(404, "Competitor not found")
    return _str_fields(_row_to_dict(row))


def update_competitor(competitor_id: str, updates: dict, db: Session) -> dict:
    safe = {k: v for k, v in updates.items() if k in COMPETITOR_EDITABLE}
    if not safe:
        raise HTTPException(400, "No valid fields to update")

    safe["updated_at"] = "NOW()"
    # Build SET clause — updated_at uses literal SQL
    parts = []
    params = {"id": competitor_id}
    for k, v in safe.items():
        if k == "updated_at":
            parts.append("updated_at = NOW()")
        else:
            parts.append(f"{k} = :{k}")
            params[k] = v

    set_clause = ", ".join(parts)
    row = db.execute(
        text(f"UPDATE competitors SET {set_clause} WHERE id = :id RETURNING *"), params,
    ).fetchone()
    if not row:
        raise HTTPException(404, "Competitor not found")
    logger.info("Updated competitor %s", competitor_id)
    return _str_fields(_row_to_dict(row))


def delete_competitor(competitor_id: str, db: Session) -> dict:
    row = db.execute(
        text("DELETE FROM competitors WHERE id = :id RETURNING id"), {"id": competitor_id},
    ).fetchone()
    if not row:
        raise HTTPException(404, "Competitor not found")
    logger.info("Deleted competitor %s", competitor_id)
    return {"message": "Competitor deleted successfully"}


# ── Competitor Products CRUD (Admin) ─────────────────────────────

def create_product(data, user: dict, db: Session) -> dict:
    # Verify competitor exists
    comp = db.execute(
        text("SELECT id, name FROM competitors WHERE id = :id"), {"id": data.competitor_id},
    ).fetchone()
    if not comp:
        raise HTTPException(404, "Competitor not found")

    row = db.execute(
        text(
            "INSERT INTO competitor_products "
            "(competitor_id, product_name, ean, category, sub_category, "
            "size_kg, mrp, selling_price, our_equivalent_ean, is_verified, created_by) "
            "VALUES (:competitor_id, :product_name, :ean, :category, :sub_category, "
            ":size_kg, :mrp, :selling_price, :our_equivalent_ean, true, :created_by) "
            "RETURNING *"
        ),
        {
            "competitor_id": data.competitor_id,
            "product_name": data.product_name.strip(),
            "ean": data.ean,
            "category": data.category,
            "sub_category": data.sub_category,
            "size_kg": data.size_kg,
            "mrp": data.mrp,
            "selling_price": data.selling_price,
            "our_equivalent_ean": data.our_equivalent_ean,
            "created_by": user["user_id"],
        },
    ).fetchone()
    result = _str_fields(_row_to_dict(row))
    result["competitor_name"] = comp.name
    logger.info("Admin created product '%s' for competitor %s", data.product_name, data.competitor_id)
    return result


def list_products(
    page: int, per_page: int, search: Optional[str],
    competitor_id: Optional[str], is_verified: Optional[bool], db: Session,
) -> dict:
    conditions = []
    params: dict = {}

    if search:
        conditions.append("p.product_name ILIKE :search")
        params["search"] = f"%{search}%"
    if competitor_id:
        conditions.append("p.competitor_id = :cid")
        params["cid"] = competitor_id
    if is_verified is not None:
        conditions.append("p.is_verified = :verified")
        params["verified"] = is_verified

    where = " AND ".join(conditions) if conditions else "1=1"

    total = db.execute(
        text(f"SELECT COUNT(*) FROM competitor_products p WHERE {where}"), params,
    ).scalar()

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    rows = db.execute(
        text(
            f"SELECT p.*, c.name AS competitor_name "
            f"FROM competitor_products p "
            f"LEFT JOIN competitors c ON c.id = p.competitor_id "
            f"WHERE {where} "
            f"ORDER BY p.created_at DESC LIMIT :limit OFFSET :offset"
        ),
        params,
    ).fetchall()

    return {
        "records": [_str_fields(_row_to_dict(r)) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": _paginate(total, per_page),
    }


def update_product(product_id: str, updates: dict, db: Session) -> dict:
    safe = {k: v for k, v in updates.items() if k in PRODUCT_EDITABLE}
    if not safe:
        raise HTTPException(400, "No valid fields to update")

    parts = []
    params = {"id": product_id}
    for k, v in safe.items():
        parts.append(f"{k} = :{k}")
        params[k] = v
    parts.append("updated_at = NOW()")
    set_clause = ", ".join(parts)

    row = db.execute(
        text(
            f"UPDATE competitor_products SET {set_clause} WHERE id = :id "
            f"RETURNING *, (SELECT name FROM competitors WHERE id = competitor_products.competitor_id) AS competitor_name"
        ),
        params,
    ).fetchone()
    if not row:
        raise HTTPException(404, "Product not found")
    logger.info("Updated product %s", product_id)
    return _str_fields(_row_to_dict(row))


def delete_product(product_id: str, db: Session) -> dict:
    row = db.execute(
        text("DELETE FROM competitor_products WHERE id = :id RETURNING id"), {"id": product_id},
    ).fetchone()
    if not row:
        raise HTTPException(404, "Product not found")
    logger.info("Deleted product %s", product_id)
    return {"message": "Product deleted successfully"}


# ── Auto-Suggest (Promoter) ──────────────────────────────────────

def suggest_competitors(q: str, db: Session) -> dict:
    rows = db.execute(
        text(
            "SELECT id, name, is_verified FROM competitors "
            "WHERE name ILIKE :q AND is_active = true "
            "ORDER BY is_verified DESC, name ASC LIMIT 10"
        ),
        {"q": f"%{q}%"},
    ).fetchall()
    return {
        "suggestions": [
            {"id": str(r.id), "name": r.name, "is_verified": r.is_verified}
            for r in rows
        ]
    }


def suggest_products(q: str, competitor_id: Optional[str], db: Session) -> dict:
    if competitor_id:
        rows = db.execute(
            text(
                "SELECT id, product_name AS name, is_verified FROM competitor_products "
                "WHERE product_name ILIKE :q AND competitor_id = :cid "
                "ORDER BY is_verified DESC, product_name ASC LIMIT 10"
            ),
            {"q": f"%{q}%", "cid": competitor_id},
        ).fetchall()
    else:
        rows = db.execute(
            text(
                "SELECT id, product_name AS name, is_verified FROM competitor_products "
                "WHERE product_name ILIKE :q "
                "ORDER BY is_verified DESC, product_name ASC LIMIT 10"
            ),
            {"q": f"%{q}%"},
        ).fetchall()
    return {
        "suggestions": [
            {"id": str(r.id), "name": r.name, "is_verified": r.is_verified}
            for r in rows
        ]
    }


def suggest_categories(q: str, db: Session) -> dict:
    rows = db.execute(
        text(
            "SELECT DISTINCT category FROM ("
            "  SELECT category FROM competitors WHERE category ILIKE :q AND category IS NOT NULL "
            "  UNION "
            "  SELECT category FROM competitor_products WHERE category ILIKE :q AND category IS NOT NULL"
            ") sub ORDER BY category LIMIT 10"
        ),
        {"q": f"%{q}%"},
    ).fetchall()
    return {"suggestions": [{"category": r.category} for r in rows]}


# ── Price Tracking ───────────────────────────────────────────────

def create_price_tracking(data, promoter, db: Session) -> dict:
    att = get_active_attendance(promoter.id, db)
    pid = str(promoter.id)

    competitor_id = resolve_or_create_competitor(data.competitor_name, pid, db)
    product_id = resolve_or_create_product(data.product_name, competitor_id, pid, db)

    mrp = float(data.observed_mrp)
    sp = float(data.observed_selling_price)
    discount = round(((mrp - sp) / mrp) * 100, 2) if mrp > 0 else 0.0

    row = db.execute(
        text(
            "INSERT INTO competitor_price_tracking "
            "(competitor_id, competitor_product_id, promoter_id, attendance_id, "
            "store_name, observed_mrp, observed_selling_price, discount_percentage, "
            "offer_description, shelf_position, facing_count, stock_availability, photo_url) "
            "VALUES (:cid, :cpid, :pid, :aid, :store, :mrp, :sp, :disc, "
            ":offer, :shelf, :facing, :stock, :photo) "
            "RETURNING id"
        ),
        {
            "cid": competitor_id,
            "cpid": product_id,
            "pid": pid,
            "aid": att["attendance_id"],
            "store": att["store_name"],
            "mrp": data.observed_mrp,
            "sp": data.observed_selling_price,
            "disc": discount,
            "offer": data.offer_description,
            "shelf": data.shelf_position,
            "facing": data.facing_count,
            "stock": data.stock_availability,
            "photo": data.photo_url,
        },
    ).fetchone()

    logger.info("Price tracking recorded by promoter %s at %s", pid, att["store_name"])
    return {"id": str(row.id), "message": "Price tracking entry created"}


def list_price_tracking(
    promoter_id: str,
    db: Session,
) -> dict:
    rows = db.execute(
        text(
            "SELECT c.name AS competitor_name, cp.product_name, "
            "pt.store_name, pt.observed_mrp, pt.observed_selling_price, "
            "pt.discount_percentage, pt.offer_description, pt.shelf_position, "
            "pt.facing_count, pt.stock_availability, pt.observed_at "
            "FROM competitor_price_tracking pt "
            "LEFT JOIN competitors c ON c.id = pt.competitor_id "
            "LEFT JOIN competitor_products cp ON cp.id = pt.competitor_product_id "
            "WHERE pt.promoter_id = :pid "
            "ORDER BY pt.observed_at DESC"
        ),
        {"pid": promoter_id},
    ).fetchall()

    return {"items": [_str_fields(_row_to_dict(r)) for r in rows]}


# ── Promotions ───────────────────────────────────────────────────

def create_promotion(data, promoter, db: Session) -> dict:
    att = get_active_attendance(promoter.id, db)
    pid = str(promoter.id)

    competitor_id = resolve_or_create_competitor(data.competitor_name, pid, db)

    row = db.execute(
        text(
            "INSERT INTO competitor_promotions "
            "(competitor_id, promotion_type, description, start_date, end_date, "
            "store_name, promoter_id, photo_url) "
            "VALUES (:cid, :ptype, :desc, :start, :end, :store, :pid, :photo) "
            "RETURNING id"
        ),
        {
            "cid": competitor_id,
            "ptype": data.promotion_type,
            "desc": data.description,
            "start": data.start_date,
            "end": data.end_date,
            "store": att["store_name"],
            "pid": pid,
            "photo": data.photo_url,
        },
    ).fetchone()

    logger.info("Promotion recorded by promoter %s at %s", pid, att["store_name"])
    return {"id": str(row.id), "message": "Promotion recorded"}


def list_promotions(
    page: int, per_page: int,
    competitor_id: Optional[str], promotion_type: Optional[str],
    is_active: Optional[bool], db: Session,
) -> dict:
    conditions = []
    params: dict = {}

    if competitor_id:
        conditions.append("p.competitor_id = :cid")
        params["cid"] = competitor_id
    if promotion_type:
        conditions.append("p.promotion_type = :ptype")
        params["ptype"] = promotion_type
    if is_active is not None:
        conditions.append("p.is_active = :active")
        params["active"] = is_active

    where = " AND ".join(conditions) if conditions else "1=1"

    total = db.execute(
        text(f"SELECT COUNT(*) FROM competitor_promotions p WHERE {where}"), params,
    ).scalar()

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    rows = db.execute(
        text(
            f"SELECT p.*, c.name AS competitor_name "
            f"FROM competitor_promotions p "
            f"LEFT JOIN competitors c ON c.id = p.competitor_id "
            f"WHERE {where} "
            f"ORDER BY p.created_at DESC LIMIT :limit OFFSET :offset"
        ),
        params,
    ).fetchall()

    return {
        "records": [_str_fields(_row_to_dict(r)) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": _paginate(total, per_page),
    }


def update_promotion(promo_id: str, updates: dict, db: Session) -> dict:
    safe = {k: v for k, v in updates.items() if k in PROMOTION_EDITABLE}
    if not safe:
        raise HTTPException(400, "No valid fields to update")

    parts = []
    params = {"id": promo_id}
    for k, v in safe.items():
        parts.append(f"{k} = :{k}")
        params[k] = v
    set_clause = ", ".join(parts)

    row = db.execute(
        text(
            f"UPDATE competitor_promotions SET {set_clause} WHERE id = :id "
            f"RETURNING *, (SELECT name FROM competitors WHERE id = competitor_promotions.competitor_id) AS competitor_name"
        ),
        params,
    ).fetchone()
    if not row:
        raise HTTPException(404, "Promotion not found")
    logger.info("Updated promotion %s", promo_id)
    return _str_fields(_row_to_dict(row))


def delete_promotion(promo_id: str, db: Session) -> dict:
    row = db.execute(
        text("DELETE FROM competitor_promotions WHERE id = :id RETURNING id"), {"id": promo_id},
    ).fetchone()
    if not row:
        raise HTTPException(404, "Promotion not found")
    logger.info("Deleted promotion %s", promo_id)
    return {"message": "Promotion deleted successfully"}


# ── Market Share ─────────────────────────────────────────────────

def create_market_share(data, promoter, db: Session) -> dict:
    att = get_active_attendance(promoter.id, db)
    pid = str(promoter.id)

    competitor_id = resolve_or_create_competitor(data.competitor_name, pid, db)

    our_pct = SHELF_SHARE_MAP[data.our_shelf_share]
    comp_pct = SHELF_SHARE_MAP[data.competitor_shelf_share]
    footfall = FOOTFALL_MAP.get(data.estimated_footfall) if data.estimated_footfall else None

    row = db.execute(
        text(
            "INSERT INTO competitor_market_share "
            "(store_name, promoter_id, attendance_id, category, "
            "our_shelf_share_pct, competitor_id, competitor_shelf_share_pct, estimated_footfall) "
            "VALUES (:store, :pid, :aid, :cat, :our, :cid, :comp, :foot) "
            "RETURNING id"
        ),
        {
            "store": att["store_name"],
            "pid": pid,
            "aid": att["attendance_id"],
            "cat": data.category.strip(),
            "our": our_pct,
            "cid": competitor_id,
            "comp": comp_pct,
            "foot": footfall,
        },
    ).fetchone()

    logger.info("Market share recorded by promoter %s at %s", pid, att["store_name"])
    return {"id": str(row.id), "message": "Market share observation created"}


def list_market_share(
    promoter_id: str,
    db: Session,
) -> dict:
    rows = db.execute(
        text(
            "SELECT c.name AS competitor_name, ms.category, ms.store_name, "
            "ms.our_shelf_share_pct, ms.competitor_shelf_share_pct, "
            "ms.estimated_footfall, ms.observed_at "
            "FROM competitor_market_share ms "
            "LEFT JOIN competitors c ON c.id = ms.competitor_id "
            "WHERE ms.promoter_id = :pid "
            "ORDER BY ms.observed_at DESC"
        ),
        {"pid": promoter_id},
    ).fetchall()

    return {"items": [_str_fields(_row_to_dict(r)) for r in rows]}


# ── Admin Review ─────────────────────────────────────────────────

def list_pending_reviews(page: int, per_page: int, db: Session) -> dict:
    count_row = db.execute(
        text(
            "SELECT "
            "(SELECT COUNT(*) FROM competitors WHERE is_verified = false) + "
            "(SELECT COUNT(*) FROM competitor_products WHERE is_verified = false) AS cnt"
        ),
    ).scalar()
    total = count_row or 0

    offset = (page - 1) * per_page

    rows = db.execute(
        text(
            "SELECT id, name, 'competitor' AS item_type, created_by, created_at "
            "FROM competitors WHERE is_verified = false "
            "UNION ALL "
            "SELECT id, product_name AS name, 'product' AS item_type, created_by, created_at "
            "FROM competitor_products WHERE is_verified = false "
            "ORDER BY created_at DESC "
            "LIMIT :limit OFFSET :offset"
        ),
        {"limit": per_page, "offset": offset},
    ).fetchall()

    return {
        "records": [
            {
                "id": str(r.id),
                "item_type": r.item_type,
                "name": r.name,
                "created_by": str(r.created_by),
                "created_at": r.created_at.isoformat() if hasattr(r.created_at, "isoformat") else str(r.created_at),
            }
            for r in rows
        ],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": _paginate(total, per_page),
    }


def verify_item(item_id: str, db: Session) -> dict:
    # Try competitors first
    result = db.execute(
        text("UPDATE competitors SET is_verified = true, updated_at = NOW() WHERE id = :id RETURNING id"),
        {"id": item_id},
    ).fetchone()
    if result:
        logger.info("Verified competitor %s", item_id)
        return {"message": "Competitor verified successfully"}

    # Try products
    result = db.execute(
        text("UPDATE competitor_products SET is_verified = true, updated_at = NOW() WHERE id = :id RETURNING id"),
        {"id": item_id},
    ).fetchone()
    if result:
        logger.info("Verified product %s", item_id)
        return {"message": "Product verified successfully"}

    raise HTTPException(404, "Item not found")


def merge_competitors(keep_id: str, merge_ids: list, db: Session) -> dict:
    # Verify keep_id exists
    keep = db.execute(
        text("SELECT id FROM competitors WHERE id = :id"), {"id": keep_id},
    ).fetchone()
    if not keep:
        raise HTTPException(404, "Target competitor not found")

    merged = 0
    for mid in merge_ids:
        if mid == keep_id:
            continue

        exists = db.execute(
            text("SELECT id FROM competitors WHERE id = :id"), {"id": mid},
        ).fetchone()
        if not exists:
            continue

        # Re-assign all foreign keys to keep_id
        db.execute(
            text("UPDATE competitor_price_tracking SET competitor_id = :keep WHERE competitor_id = :merge"),
            {"keep": keep_id, "merge": mid},
        )
        db.execute(
            text("UPDATE competitor_promotions SET competitor_id = :keep WHERE competitor_id = :merge"),
            {"keep": keep_id, "merge": mid},
        )
        db.execute(
            text("UPDATE competitor_market_share SET competitor_id = :keep WHERE competitor_id = :merge"),
            {"keep": keep_id, "merge": mid},
        )
        db.execute(
            text("UPDATE competitor_products SET competitor_id = :keep WHERE competitor_id = :merge"),
            {"keep": keep_id, "merge": mid},
        )
        db.execute(
            text("DELETE FROM competitors WHERE id = :id"), {"id": mid},
        )
        merged += 1

    # Mark keep as verified
    db.execute(
        text("UPDATE competitors SET is_verified = true, updated_at = NOW() WHERE id = :id"),
        {"id": keep_id},
    )

    logger.info("Merged %d competitors into %s", merged, keep_id)
    return {"message": f"Merged {merged} competitor(s) successfully", "merged_count": merged}
