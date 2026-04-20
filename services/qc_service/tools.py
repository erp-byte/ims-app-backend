import bcrypt
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger

logger = get_logger("qc")


# ── Approver ─────────────────────────────────


def create_approver(data, db: Session) -> dict:
    password_hash = bcrypt.hashpw(
        data.password.encode("utf-8"), bcrypt.gensalt()
    ).decode("utf-8")

    existing = db.execute(
        text("SELECT id FROM ipqc_users WHERE username = :u"),
        {"u": data.username},
    ).fetchone()
    if existing:
        raise HTTPException(409, f"Username '{data.username}' already exists")

    row = db.execute(
        text(
            "INSERT INTO ipqc_users (username, password_hash, display_name) "
            "VALUES (:username, :password_hash, :display_name) "
            "RETURNING id, username, display_name, is_active"
        ),
        {
            "username": data.username,
            "password_hash": password_hash,
            "display_name": data.display_name,
        },
    ).fetchone()

    db.commit()
    logger.info("Created QC approver: %s", data.username)
    return {
        "id": row.id,
        "username": row.username,
        "display_name": row.display_name,
        "is_active": row.is_active,
    }


def login_approver(data, db: Session) -> dict:
    row = db.execute(
        text(
            "SELECT id, username, password_hash, display_name, is_active "
            "FROM ipqc_users WHERE username = :u"
        ),
        {"u": data.username},
    ).fetchone()

    if not row:
        raise HTTPException(401, "Invalid username or password")

    if not row.is_active:
        raise HTTPException(403, "Approver account is deactivated")

    if not bcrypt.checkpw(data.password.encode("utf-8"), row.password_hash.encode("utf-8")):
        raise HTTPException(401, "Invalid username or password")

    logger.info("QC approver logged in: %s", data.username)
    return {
        "success": True,
        "approver_id": row.id,
        "username": row.username,
        "display_name": row.display_name,
    }


def list_approvers(db: Session) -> list:
    rows = db.execute(
        text("SELECT id, username, display_name, is_active FROM ipqc_users ORDER BY id")
    ).fetchall()
    return [
        {"id": r.id, "username": r.username, "display_name": r.display_name, "is_active": r.is_active}
        for r in rows
    ]


# ── Factory / Floor dropdown ─────────────────


def get_factories_floors(db: Session) -> dict:
    factories = db.execute(
        text("SELECT id, factory_code, factory_name FROM qc_factories ORDER BY id")
    ).fetchall()

    result = []
    for f in factories:
        floors = db.execute(
            text(
                "SELECT id, floor_name, sort_order FROM qc_floors "
                "WHERE factory_id = :fid ORDER BY sort_order, id"
            ),
            {"fid": f.id},
        ).fetchall()

        result.append({
            "id": f.id,
            "factory_code": f.factory_code,
            "factory_name": f.factory_name,
            "floors": [
                {"id": fl.id, "floor_name": fl.floor_name, "sort_order": fl.sort_order}
                for fl in floors
            ],
        })

    return {"factories": result}


# ── Floor CRUD ───────────────────────────────


def create_floor(data, db: Session) -> dict:
    factory = db.execute(
        text("SELECT id FROM qc_factories WHERE factory_code = :fc"),
        {"fc": data.factory_code},
    ).fetchone()

    if not factory:
        raise HTTPException(404, f"Factory '{data.factory_code}' not found")

    existing = db.execute(
        text(
            "SELECT id FROM qc_floors "
            "WHERE factory_id = :fid AND floor_name = :fn"
        ),
        {"fid": factory.id, "fn": data.floor_name},
    ).fetchone()
    if existing:
        raise HTTPException(409, f"Floor '{data.floor_name}' already exists for {data.factory_code}")

    row = db.execute(
        text(
            "INSERT INTO qc_floors (factory_id, floor_name, sort_order) "
            "VALUES (:fid, :fn, :so) "
            "RETURNING id, floor_name, sort_order"
        ),
        {"fid": factory.id, "fn": data.floor_name, "so": data.sort_order or 0},
    ).fetchone()

    db.commit()
    logger.info("Created floor '%s' for factory %s", data.floor_name, data.factory_code)
    return {"id": row.id, "floor_name": row.floor_name, "sort_order": row.sort_order}


def update_floor(floor_id: int, data: dict, db: Session) -> dict:
    updates = {k: v for k, v in data.items() if v is not None}
    if not updates:
        raise HTTPException(400, "No fields to update")

    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    updates["fid"] = floor_id

    row = db.execute(
        text(
            f"UPDATE qc_floors SET {set_clause} "
            f"WHERE id = :fid "
            f"RETURNING id, floor_name, sort_order"
        ),
        updates,
    ).fetchone()

    if not row:
        raise HTTPException(404, f"Floor id {floor_id} not found")

    db.commit()
    logger.info("Updated floor id %s", floor_id)
    return {"id": row.id, "floor_name": row.floor_name, "sort_order": row.sort_order}


def delete_floor(floor_id: int, db: Session) -> dict:
    row = db.execute(
        text("DELETE FROM qc_floors WHERE id = :fid RETURNING id"),
        {"fid": floor_id},
    ).fetchone()

    if not row:
        raise HTTPException(404, f"Floor id {floor_id} not found")

    db.commit()
    logger.info("Deleted floor id %s", floor_id)
    return {"success": True, "message": "Floor deleted"}


# ── Factory CRUD ─────────────────────────────


def create_factory(data, db: Session) -> dict:
    existing = db.execute(
        text("SELECT id FROM qc_factories WHERE factory_code = :fc"),
        {"fc": data.factory_code},
    ).fetchone()
    if existing:
        raise HTTPException(409, f"Factory '{data.factory_code}' already exists")

    row = db.execute(
        text(
            "INSERT INTO qc_factories (factory_code, factory_name) "
            "VALUES (:fc, :fn) "
            "RETURNING id, factory_code, factory_name"
        ),
        {"fc": data.factory_code, "fn": data.factory_name or data.factory_code},
    ).fetchone()

    db.commit()
    logger.info("Created factory: %s", data.factory_code)
    return {"id": row.id, "factory_code": row.factory_code, "factory_name": row.factory_name}


def update_factory(factory_id: int, data: dict, db: Session) -> dict:
    updates = {k: v for k, v in data.items() if v is not None}
    if not updates:
        raise HTTPException(400, "No fields to update")

    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    updates["fid"] = factory_id

    row = db.execute(
        text(
            f"UPDATE qc_factories SET {set_clause} "
            f"WHERE id = :fid "
            f"RETURNING id, factory_code, factory_name"
        ),
        updates,
    ).fetchone()

    if not row:
        raise HTTPException(404, f"Factory id {factory_id} not found")

    db.commit()
    logger.info("Updated factory id %s", factory_id)
    return {"id": row.id, "factory_code": row.factory_code, "factory_name": row.factory_name}


def delete_factory(factory_id: int, db: Session) -> dict:
    row = db.execute(
        text("DELETE FROM qc_factories WHERE id = :fid RETURNING id"),
        {"fid": factory_id},
    ).fetchone()

    if not row:
        raise HTTPException(404, f"Factory id {factory_id} not found")

    db.commit()
    logger.info("Deleted factory id %s (and all its floors)", factory_id)
    return {"success": True, "message": "Factory and all its floors deleted"}
