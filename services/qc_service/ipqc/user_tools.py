import bcrypt
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger import get_logger
from services.qc_service.ipqc.jwt_utils import create_ipqc_token

logger = get_logger("qc.ipqc.users")


def _hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


def _safe_str(val):
    return str(val) if val is not None else None


# ── Create user ──────────────────────────────


def create_user(data, db: Session) -> dict:
    existing = db.execute(
        text("SELECT id FROM ipqc_users WHERE username = :u"),
        {"u": data.username},
    ).fetchone()
    if existing:
        raise HTTPException(409, f"User '{data.username}' already exists")

    password_hash = _hash_password(data.password)

    row = db.execute(
        text(
            "INSERT INTO ipqc_users (username, password_hash, display_name, is_admin) "
            "VALUES (:username, :password_hash, :display_name, :is_admin) "
            "RETURNING id, username, display_name, is_admin, is_active, created_at, updated_at"
        ),
        {
            "username": data.username,
            "password_hash": password_hash,
            "display_name": data.display_name,
            "is_admin": data.is_admin,
        },
    ).fetchone()

    db.commit()
    logger.info("Created IPQC user: %s", data.username)
    return {
        "id": row.id,
        "username": row.username,
        "display_name": row.display_name,
        "is_admin": row.is_admin,
        "is_active": row.is_active,
        "created_at": _safe_str(row.created_at),
        "updated_at": _safe_str(row.updated_at),
    }


# ── Login ────────────────────────────────────


def login_user(data, db: Session) -> dict:
    row = db.execute(
        text(
            "SELECT id, username, password_hash, display_name, is_admin, is_active "
            "FROM ipqc_users WHERE username = :u"
        ),
        {"u": data.username},
    ).fetchone()

    if not row:
        raise HTTPException(401, "Invalid username or password")

    if not row.is_active:
        raise HTTPException(403, "Account is deactivated")

    if not _verify_password(data.password, row.password_hash):
        raise HTTPException(401, "Invalid username or password")

    user = {
        "id": row.id,
        "username": row.username,
        "display_name": row.display_name,
        "is_admin": row.is_admin,
    }
    token = create_ipqc_token(user)

    logger.info("IPQC user logged in: %s", data.username)
    return {
        "success": True,
        "user_id": row.id,
        "username": row.username,
        "display_name": row.display_name,
        "is_admin": row.is_admin,
        "token": token,
    }


# ── Reset password ───────────────────────────


def reset_password(data, db: Session) -> dict:
    existing = db.execute(
        text("SELECT id, is_active FROM ipqc_users WHERE username = :u"),
        {"u": data.username},
    ).fetchone()

    if not existing:
        raise HTTPException(404, f"User '{data.username}' not found")

    if not existing.is_active:
        raise HTTPException(403, "Account is deactivated")

    new_hash = _hash_password(data.new_password)

    db.execute(
        text(
            "UPDATE ipqc_users SET password_hash = :ph, updated_at = NOW() "
            "WHERE username = :u"
        ),
        {"ph": new_hash, "u": data.username},
    )

    db.commit()
    logger.info("Password reset for IPQC user: %s", data.username)
    return {"success": True, "message": "Password reset successfully"}


# ── List users ───────────────────────────────


def list_users(db: Session) -> list:
    rows = db.execute(
        text(
            "SELECT id, username, display_name, is_admin, is_active, created_at, updated_at "
            "FROM ipqc_users ORDER BY id"
        )
    ).fetchall()

    return [
        {
            "id": r.id,
            "username": r.username,
            "display_name": r.display_name,
            "is_admin": r.is_admin,
            "is_active": r.is_active,
            "created_at": _safe_str(r.created_at),
            "updated_at": _safe_str(r.updated_at),
        }
        for r in rows
    ]
