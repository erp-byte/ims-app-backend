"""Packing Details — DB access (SQLAlchemy Core via text()).

Sync sessions (shared.database.SessionLocal / get_db). psycopg3 returns JSONB
columns as Python dicts, so `details` needs no manual json.loads on read; on
write we pass json.dumps + CAST(... AS JSONB).
"""
from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

_COLS = "id, batch_code, article_name, details, created_by, created_at, updated_at"


def _row(mapping) -> dict:
    d = dict(mapping)
    det = d.get("details")
    if isinstance(det, str):  # defensive — psycopg3 usually gives a dict
        try:
            d["details"] = json.loads(det)
        except (ValueError, TypeError):
            d["details"] = {}
    elif not isinstance(det, dict):
        d["details"] = {}
    return d


def create_packing_detail(
    db: Session, *, batch_code: str, article_name: str,
    details: Optional[dict], created_by: Optional[str],
) -> dict:
    mapping = db.execute(
        text(f"""
            INSERT INTO packing_details (batch_code, article_name, details, created_by)
            VALUES (:bc, :an, CAST(:details AS JSONB), :cb)
            RETURNING {_COLS}
        """),
        {"bc": batch_code, "an": article_name,
         "details": json.dumps(details or {}), "cb": created_by},
    ).mappings().first()
    return _row(mapping)


def get_packing_detail(db: Session, packing_id: int) -> Optional[dict]:
    mapping = db.execute(
        text(f"SELECT {_COLS} FROM packing_details WHERE id = :id"),
        {"id": packing_id},
    ).mappings().first()
    return _row(mapping) if mapping else None


def list_packing_details(
    db: Session, *, batch_code: Optional[str] = None,
    article_name: Optional[str] = None, limit: int = 50, offset: int = 0,
) -> list[dict]:
    where, params = [], {"lim": limit, "off": offset}
    if batch_code is not None:
        where.append("batch_code = :bc"); params["bc"] = batch_code
    if article_name is not None:
        where.append("article_name = :an"); params["an"] = article_name
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    rows = db.execute(
        text(f"""
            SELECT {_COLS} FROM packing_details
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT :lim OFFSET :off
        """),
        params,
    ).mappings().all()
    return [_row(m) for m in rows]


def update_packing_detail(
    db: Session, packing_id: int, *, batch_code: Optional[str] = None,
    article_name: Optional[str] = None, details: Optional[dict] = None,
    details_provided: bool = False,
) -> Optional[dict]:
    sets: list[str] = []
    params: dict[str, Any] = {"id": packing_id}
    if batch_code is not None:
        sets.append("batch_code = :bc"); params["bc"] = batch_code
    if article_name is not None:
        sets.append("article_name = :an"); params["an"] = article_name
    if details_provided:
        sets.append("details = CAST(:details AS JSONB)")
        params["details"] = json.dumps(details or {})
    sets.append("updated_at = NOW()")
    mapping = db.execute(
        text(f"UPDATE packing_details SET {', '.join(sets)} WHERE id = :id RETURNING {_COLS}"),
        params,
    ).mappings().first()
    return _row(mapping) if mapping else None


def delete_packing_detail(db: Session, packing_id: int) -> bool:
    result = db.execute(
        text("DELETE FROM packing_details WHERE id = :id"), {"id": packing_id}
    )
    return result.rowcount > 0
