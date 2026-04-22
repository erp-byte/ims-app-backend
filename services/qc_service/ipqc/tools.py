import bcrypt
import json
from datetime import date
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger

logger = get_logger("qc.ipqc")


# ── Auth helpers ────────────────────────────


def _verify_ipqc_user(username: str, password: str, db: Session) -> dict:
    """Verify user credentials against ipqc_users. Returns user dict or raises 401/403."""
    row = db.execute(
        text(
            "SELECT id, username, password_hash, display_name, is_admin, is_active "
            "FROM ipqc_users WHERE username = :u"
        ),
        {"u": username},
    ).fetchone()

    if not row:
        raise HTTPException(401, "Invalid username or password")

    if not row.is_active:
        raise HTTPException(403, "Account is deactivated")

    if not bcrypt.checkpw(password.encode("utf-8"), row.password_hash.encode("utf-8")):
        raise HTTPException(401, "Invalid username or password")

    return {
        "id": row.id,
        "username": row.username,
        "display_name": row.display_name,
        "is_admin": row.is_admin,
    }


def _require_admin(username: str, password: str, db: Session) -> dict:
    """Verify user is an authenticated admin. Raises 401/403."""
    user = _verify_ipqc_user(username, password, db)
    if not user["is_admin"]:
        raise HTTPException(403, "Admin access required")
    return user


# ── Helpers ──────────────────────────────────


def _safe_str(val):
    return str(val) if val is not None else None


def _generate_ipqc_no(db: Session) -> str:
    """Atomically generate the next IPQC number for today: IPQC-YYYYMMDD-XXXX."""
    today = date.today()
    row = db.execute(
        text(
            "INSERT INTO ipqc_daily_sequence (seq_date, last_seq) "
            "VALUES (:d, 1) "
            "ON CONFLICT (seq_date) "
            "DO UPDATE SET last_seq = ipqc_daily_sequence.last_seq + 1 "
            "RETURNING last_seq"
        ),
        {"d": today},
    ).fetchone()
    seq = row.last_seq
    return f"IPQC-{today.strftime('%Y%m%d')}-{seq:04d}"


def _map_ipqc_row(row) -> dict:
    articles = row.articles if hasattr(row, "articles") and row.articles else []
    return {
        "id": row.id,
        "ipqc_no": row.ipqc_no,
        "check_date": _safe_str(row.check_date),
        "item_description": row.item_description,
        "customer": row.customer,
        "batch_number": row.batch_number,
        "factory_code": row.factory_code,
        "floor": row.floor,
        "sensory_evaluation": row.sensory_evaluation if row.sensory_evaluation else [],
        "physical_category": row.physical_category,
        "physical_parameters": row.physical_parameters if row.physical_parameters else [],
        "label_check": row.label_check if row.label_check else [],
        "seal_check": row.seal_check,
        "verdict": row.verdict,
        "overall_remark": row.overall_remark,
        "articles": articles,
        "checked_by": row.checked_by,
        "approved_by": row.approved_by,
        "approved_at": _safe_str(row.approved_at),
        "created_at": _safe_str(row.created_at),
        "updated_at": _safe_str(row.updated_at),
    }


# ── Create ───────────────────────────────────


def create_ipqc(data, db: Session) -> dict:
    ipqc_no = _generate_ipqc_no(db)

    # Build articles list: prefer explicit articles, fall back to flat fields
    if data.articles:
        articles = [a.model_dump() for a in data.articles]
    else:
        articles = [{
            "item_description": data.item_description,
            "customer": data.customer,
            "batch_number": data.batch_number,
            "physical_category": data.physical_category,
            "sensory_evaluation": [i.model_dump() for i in data.sensory_evaluation],
            "physical_parameters": [i.model_dump() for i in data.physical_parameters],
            "label_check": [i.model_dump() for i in data.label_check],
            "seal_check": data.seal_check,
            "verdict": data.verdict,
            "overall_remark": data.overall_remark,
        }]

    # Flat fields from first article (for search / filter / listing)
    first = articles[0] if articles else {}

    params = {
        "ipqc_no": ipqc_no,
        "check_date": data.check_date or date.today(),
        "item_description": first.get("item_description"),
        "customer": first.get("customer"),
        "batch_number": first.get("batch_number"),
        "factory_code": data.factory_code,
        "floor": data.floor,
        "sensory_evaluation": json.dumps(first.get("sensory_evaluation", [])),
        "physical_category": first.get("physical_category", "other"),
        "physical_parameters": json.dumps(first.get("physical_parameters", [])),
        "label_check": json.dumps(first.get("label_check", [])),
        "seal_check": first.get("seal_check", False),
        "verdict": first.get("verdict", "accept"),
        "overall_remark": first.get("overall_remark"),
        "checked_by": data.checked_by,
        "articles": json.dumps(articles),
    }

    row = db.execute(
        text("""
            INSERT INTO ipqc_records (
                ipqc_no, check_date, item_description, customer, batch_number,
                factory_code, floor,
                sensory_evaluation, physical_category, physical_parameters,
                label_check, seal_check, verdict, overall_remark,
                checked_by, articles
            ) VALUES (
                :ipqc_no, :check_date, :item_description, :customer, :batch_number,
                :factory_code, :floor,
                CAST(:sensory_evaluation AS jsonb), :physical_category, CAST(:physical_parameters AS jsonb),
                CAST(:label_check AS jsonb), :seal_check, :verdict, :overall_remark,
                :checked_by, CAST(:articles AS jsonb)
            )
            RETURNING *
        """),
        params,
    ).fetchone()

    db.commit()
    logger.info("Created IPQC record: %s", ipqc_no)
    return _map_ipqc_row(row)


# ── List ─────────────────────────────────────


def list_ipqc(
    page: int,
    per_page: int,
    customer: Optional[str],
    verdict: Optional[str],
    factory_code: Optional[str],
    floor: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    search: Optional[str],
    sort_by: str,
    sort_order: str,
    db: Session,
) -> dict:
    conditions = []
    params: dict = {}

    if factory_code:
        conditions.append("factory_code = :factory_code")
        params["factory_code"] = factory_code

    if floor:
        conditions.append("floor = :floor")
        params["floor"] = floor

    if customer:
        conditions.append("customer ILIKE :customer")
        params["customer"] = f"%{customer}%"

    if verdict:
        conditions.append("verdict = :verdict")
        params["verdict"] = verdict

    if from_date:
        conditions.append("check_date >= :from_date")
        params["from_date"] = from_date

    if to_date:
        conditions.append("check_date <= :to_date")
        params["to_date"] = to_date

    if search:
        conditions.append(
            "(ipqc_no ILIKE :search "
            "OR item_description ILIKE :search "
            "OR customer ILIKE :search "
            "OR batch_number ILIKE :search)"
        )
        params["search"] = f"%{search}%"

    where = " AND ".join(conditions) if conditions else "1=1"

    allowed_sort = {
        "ipqc_no", "check_date", "customer", "batch_number",
        "verdict", "created_at", "updated_at",
    }
    if sort_by not in allowed_sort:
        sort_by = "created_at"
    if sort_order.lower() not in ("asc", "desc"):
        sort_order = "desc"

    count_row = db.execute(
        text(f"SELECT COUNT(*) AS cnt FROM ipqc_records WHERE {where}"),
        params,
    ).mappings().first()
    total = count_row["cnt"]

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    rows = db.execute(
        text(
            f"SELECT * FROM ipqc_records "
            f"WHERE {where} "
            f"ORDER BY {sort_by} {sort_order} "
            f"LIMIT :limit OFFSET :offset"
        ),
        params,
    ).fetchall()

    total_pages = (total + per_page - 1) // per_page if per_page > 0 else 0

    return {
        "records": [_map_ipqc_row(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
    }


# ── Get single ───────────────────────────────


def get_ipqc(ipqc_no: str, db: Session) -> dict:
    row = db.execute(
        text("SELECT * FROM ipqc_records WHERE ipqc_no = :ipqc_no"),
        {"ipqc_no": ipqc_no},
    ).fetchone()

    if not row:
        raise HTTPException(404, f"IPQC record '{ipqc_no}' not found")

    return _map_ipqc_row(row)


# ── Update ───────────────────────────────────


_JSONB_FIELDS = {"sensory_evaluation", "physical_parameters", "label_check", "articles"}
_EDITABLE = {
    "check_date", "item_description", "customer", "batch_number",
    "factory_code", "floor",
    "sensory_evaluation", "physical_category", "physical_parameters",
    "label_check", "seal_check", "verdict", "overall_remark",
    "articles",
}


def update_ipqc(ipqc_no: str, data: dict, user: dict, db: Session) -> dict:
    updates = {k: v for k, v in data.items() if v is not None and k in _EDITABLE}

    # Only admins can change the date
    if "check_date" in updates and not user["is_admin"]:
        del updates["check_date"]

    # If articles provided, sync flat fields from first article
    if "articles" in updates and isinstance(updates["articles"], list) and updates["articles"]:
        first = updates["articles"][0]
        if hasattr(first, "model_dump"):
            first = first.model_dump()
        updates["item_description"] = first.get("item_description")
        updates["customer"] = first.get("customer")
        updates["batch_number"] = first.get("batch_number")
        updates["physical_category"] = first.get("physical_category", "other")
        updates["sensory_evaluation"] = first.get("sensory_evaluation", [])
        updates["physical_parameters"] = first.get("physical_parameters", [])
        updates["label_check"] = first.get("label_check", [])
        updates["seal_check"] = first.get("seal_check", False)
        updates["verdict"] = first.get("verdict", "accept")
        updates["overall_remark"] = first.get("overall_remark")

    if not updates:
        raise HTTPException(400, "No fields to update")

    # Serialize JSONB fields
    for key in _JSONB_FIELDS:
        if key in updates:
            val = updates[key]
            if isinstance(val, list):
                updates[key] = json.dumps(
                    [item.model_dump() if hasattr(item, "model_dump") else item for item in val]
                )

    set_parts = []
    for k in updates:
        if k in _JSONB_FIELDS:
            set_parts.append(f"{k} = CAST(:{k} AS jsonb)")
        else:
            set_parts.append(f"{k} = :{k}")
    set_parts.append("updated_at = NOW()")

    set_clause = ", ".join(set_parts)
    updates["ipqc_no"] = ipqc_no

    row = db.execute(
        text(
            f"UPDATE ipqc_records SET {set_clause} "
            f"WHERE ipqc_no = :ipqc_no "
            f"RETURNING *"
        ),
        updates,
    ).fetchone()

    if not row:
        raise HTTPException(404, f"IPQC record '{ipqc_no}' not found")

    db.commit()
    logger.info("Updated IPQC record: %s", ipqc_no)
    return _map_ipqc_row(row)


# ── Delete ───────────────────────────────────


def delete_ipqc(ipqc_no: str, user: dict, db: Session) -> dict:
    if not user["is_admin"]:
        raise HTTPException(403, "Admin access required")
    row = db.execute(
        text("DELETE FROM ipqc_records WHERE ipqc_no = :ipqc_no RETURNING ipqc_no"),
        {"ipqc_no": ipqc_no},
    ).fetchone()

    if not row:
        raise HTTPException(404, f"IPQC record '{ipqc_no}' not found")

    db.commit()
    logger.info("Deleted IPQC record: %s", ipqc_no)
    return {"success": True, "message": "IPQC record deleted", "ipqc_no": ipqc_no}


# ── Approve ──────────────────────────────────


def approve_ipqc(ipqc_no: str, user: dict, db: Session) -> dict:

    row = db.execute(
        text(
            "UPDATE ipqc_records "
            "SET approved_by = :approved_by, approved_at = NOW(), updated_at = NOW() "
            "WHERE ipqc_no = :ipqc_no "
            "RETURNING *"
        ),
        {"ipqc_no": ipqc_no, "approved_by": user["display_name"]},
    ).fetchone()

    if not row:
        raise HTTPException(404, f"IPQC record '{ipqc_no}' not found")

    db.commit()
    logger.info("Approved IPQC record: %s by %s", ipqc_no, user["display_name"])
    return _map_ipqc_row(row)


# ── SKU Lookup ──────────────────────────────


def lookup_ipqc_sku(item_description: str, db: Session) -> dict | None:
    """Lookup SKU from unified ipqc_sku table (no company param needed)."""
    row = db.execute(
        text("""
            SELECT id, item_description, material_type, item_category,
                   sub_category, sale_group, source_company
            FROM ipqc_sku
            WHERE item_description ILIKE :desc
            LIMIT 1
        """),
        {"desc": item_description},
    ).mappings().first()

    if not row:
        return None

    return {
        "sku_id": row["id"],
        "item_description": row["item_description"],
        "material_type": row["material_type"],
        "item_category": row["item_category"],
        "sub_category": row["sub_category"],
        "sale_group": row["sale_group"],
        "source_company": row["source_company"],
    }


def search_ipqc_sku(search: str, db: Session) -> dict:
    """Search SKUs from unified ipqc_sku table by partial match."""
    rows = db.execute(
        text("""
            SELECT id, item_description, material_type, item_category,
                   sub_category, sale_group, source_company
            FROM ipqc_sku
            WHERE item_description ILIKE :search
            ORDER BY item_description
        """),
        {"search": f"%{search}%"},
    ).mappings().all()

    items = [
        {
            "sku_id": r["id"],
            "item_description": r["item_description"],
            "material_type": r["material_type"],
            "item_category": r["item_category"],
            "sub_category": r["sub_category"],
            "sale_group": r["sale_group"],
            "source_company": r["source_company"],
        }
        for r in rows
    ]

    return {"items": items, "total": len(items)}
