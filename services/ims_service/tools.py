from datetime import datetime, timedelta

import bcrypt
from jose import jwt
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.config_loader import settings
from shared.logger import get_logger

logger = get_logger("ims.tools")

# Hardcoded IMS modules matching the frontend sidebar
IMS_MODULES = [
    ("dashboard",        "Dashboard"),
    ("inward",           "Inward"),
    ("transfer",         "Transfer"),
    ("consumption",      "Consumption"),
    ("inventory-ledger", "Inventory Ledger"),
    ("reordering",       "Reordering"),
    ("outward",          "Outward"),
    ("reports",          "Reports"),
    ("settings",         "Settings"),
    ("developer",        "Developer"),
]


def _create_access_token(user_id: str, email: str) -> str:
    payload = {
        "user_id": user_id,
        "email": email,
        "exp": datetime.utcnow() + timedelta(hours=settings.IMS_JWT_EXPIRATION_HOURS),
        "iat": datetime.utcnow(),
    }
    return jwt.encode(payload, settings.IMS_JWT_SECRET, algorithm=settings.IMS_JWT_ALGORITHM)


def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def create_user(
    email: str, password: str, name: str, is_developer: bool, is_active: bool, db: Session
) -> dict | None:
    existing = db.execute(
        text("SELECT id FROM users WHERE email = :email"),
        {"email": email},
    ).mappings().first()

    if existing:
        return None  # email conflict

    password_hash = _hash_password(password)

    row = db.execute(
        text("""
            INSERT INTO users (email, name, password_hash, is_developer, is_active)
            VALUES (:email, :name, :password_hash, :is_developer, :is_active)
            RETURNING id, email, name, is_developer, is_active
        """),
        {
            "email": email,
            "name": name,
            "password_hash": password_hash,
            "is_developer": is_developer,
            "is_active": is_active,
        },
    ).mappings().first()

    logger.info(f"Created user: {row['id']} ({email})")

    return {
        "id": str(row["id"]),
        "email": row["email"],
        "name": row["name"],
        "is_developer": row["is_developer"],
        "is_active": row["is_active"],
    }


def update_user(user_id: str, updates: dict, db: Session) -> dict | None:
    existing = db.execute(
        text("SELECT id FROM users WHERE id = :user_id"),
        {"user_id": user_id},
    ).mappings().first()

    if not existing:
        return None

    if "password" in updates:
        updates["password_hash"] = _hash_password(updates.pop("password"))

    if "email" in updates:
        conflict = db.execute(
            text("SELECT id FROM users WHERE email = :email AND id != :user_id"),
            {"email": updates["email"], "user_id": user_id},
        ).mappings().first()
        if conflict:
            return "email_conflict"

    if not updates:
        return "no_fields"

    set_clauses = ", ".join(f"{k} = :{k}" for k in updates)
    updates["user_id"] = user_id

    row = db.execute(
        text(f"""
            UPDATE users SET {set_clauses}
            WHERE id = :user_id
            RETURNING id, email, name, is_developer, is_active
        """),
        updates,
    ).mappings().first()

    logger.info(f"Updated user: {user_id}")

    return {
        "id": str(row["id"]),
        "email": row["email"],
        "name": row["name"],
        "is_developer": row["is_developer"],
        "is_active": row["is_active"],
    }


def list_users(db: Session) -> list[dict]:
    rows = db.execute(
        text("SELECT id, email, name, is_developer, is_active FROM users ORDER BY name ASC")
    ).mappings().all()

    return [
        {
            "id": str(r["id"]),
            "email": r["email"],
            "name": r["name"],
            "is_developer": r["is_developer"],
            "is_active": r["is_active"],
        }
        for r in rows
    ]


def delete_user(email: str, db: Session) -> bool:
    row = db.execute(
        text("DELETE FROM users WHERE email = :email RETURNING id"),
        {"email": email},
    ).mappings().first()

    if not row:
        return False

    logger.info(f"Deleted user: {email}")
    return True


def _get_company_modules(user_id: str, company_code: str, is_developer: bool, role: str, db: Session) -> list[dict]:
    """Fetch module permissions for a user in a company."""
    rows = db.execute(
        text("""
            SELECT module_code, can_access, can_view, can_create,
                   can_edit, can_delete, can_approve
            FROM module_permissions
            WHERE user_id = :user_id AND company_code = :company_code
        """),
        {"user_id": user_id, "company_code": company_code},
    ).mappings().all()

    perm_map = {r["module_code"]: r for r in rows}
    is_admin = role in ("admin", "developer")

    modules = []
    for code, name in IMS_MODULES:
        if is_developer or is_admin:
            modules.append({
                "module_code": code,
                "module_name": name,
                "permissions": {
                    "access": True, "view": True, "create": True,
                    "edit": True, "delete": True, "approve": True,
                },
            })
        else:
            mp = perm_map.get(code)
            is_dashboard = code == "dashboard"
            modules.append({
                "module_code": code,
                "module_name": name,
                "permissions": {
                    "access": True if is_dashboard else bool(mp and mp["can_access"]),
                    "view": True if is_dashboard else bool(mp and mp["can_view"]),
                    "create": bool(mp and mp["can_create"]),
                    "edit": bool(mp and mp["can_edit"]),
                    "delete": bool(mp and mp["can_delete"]),
                    "approve": bool(mp and mp["can_approve"]),
                },
            })
    return modules


def login(email: str, password: str, db: Session) -> dict:
    row = db.execute(
        text("""
            SELECT id, email, name, password_hash, is_developer, is_active
            FROM users
            WHERE email = :email AND is_active = true
        """),
        {"email": email},
    ).mappings().first()

    if not row:
        logger.warning(f"Login failed — email not found: {email}")
        return None

    if not row["password_hash"] or not bcrypt.checkpw(
        password.encode("utf-8"), row["password_hash"].encode("utf-8")
    ):
        logger.warning(f"Login failed — wrong password: {email}")
        return None

    user_id = str(row["id"])
    is_developer = row["is_developer"]
    company_rows = get_user_companies(user_id, db)

    # Enrich each company with module permissions
    companies = []
    for comp in company_rows:
        modules = _get_company_modules(user_id, comp["code"], is_developer, comp["role"], db)
        companies.append({
            "code": comp["code"],
            "name": comp["name"],
            "role": comp["role"],
            "modules": modules,
        })

    access_token = _create_access_token(user_id, row["email"])

    logger.info(f"Login successful: {user_id} ({email})")

    return {
        "id": user_id,
        "email": row["email"],
        "name": row["name"],
        "is_developer": is_developer,
        "companies": companies,
        "access_token": access_token,
        "token_type": "bearer",
    }


def get_user_companies(user_id: str, db: Session) -> list[dict]:
    # Check if user is a developer
    user_row = db.execute(
        text("SELECT is_developer FROM users WHERE id = :uid AND is_active = true"),
        {"uid": user_id},
    ).mappings().first()

    if user_row and user_row["is_developer"]:
        # Developers get access to all active companies
        rows = db.execute(
            text("""
                SELECT c.code, c.name, 'developer' AS role
                FROM companies c
                WHERE c.is_active = true
                ORDER BY c.code ASC
            """)
        ).mappings().all()
    else:
        rows = db.execute(
            text("""
                SELECT c.code, c.name, ucr.role
                FROM user_company_roles ucr
                JOIN companies c ON ucr.company_code = c.code
                WHERE ucr.user_id = :user_id AND c.is_active = true
                ORDER BY
                    CASE ucr.role
                        WHEN 'developer' THEN 6
                        WHEN 'admin'     THEN 5
                        WHEN 'ops'       THEN 4
                        WHEN 'approver'  THEN 3
                        WHEN 'viewer'    THEN 2
                        ELSE 1
                    END DESC,
                    c.code ASC
            """),
            {"user_id": user_id},
        ).mappings().all()

    return [{"code": r["code"], "name": r["name"], "role": r["role"]} for r in rows]


def get_dashboard_info(user_id: str, company_code: str, db: Session) -> dict | None:
    # Check if user is a developer
    user_row = db.execute(
        text("SELECT is_developer FROM users WHERE id = :uid AND is_active = true"),
        {"uid": user_id},
    ).mappings().first()
    is_developer = user_row and user_row["is_developer"]

    # Verify company access
    company = db.execute(
        text("""
            SELECT c.code, c.name, ucr.role
            FROM user_company_roles ucr
            JOIN companies c ON ucr.company_code = c.code
            WHERE ucr.user_id = :user_id
              AND c.code = :company_code
              AND c.is_active = true
        """),
        {"user_id": user_id, "company_code": company_code},
    ).mappings().first()

    # Developers can access any active company even without a role assignment
    if not company and is_developer:
        company = db.execute(
            text("""
                SELECT code, name, 'developer' AS role
                FROM companies
                WHERE code = :company_code AND is_active = true
            """),
            {"company_code": company_code},
        ).mappings().first()

    if not company:
        return None

    is_admin_role = company["role"] in ("admin", "developer")

    # Build module list using hardcoded IMS_MODULES + module_permissions
    module_list = _get_company_modules(user_id, company_code, is_developer, company["role"], db)

    total_modules = len(module_list)
    if is_developer or is_admin_role:
        accessible_modules = total_modules
    else:
        accessible_modules = sum(1 for m in module_list if m["permissions"]["access"])

    return {
        "company": {
            "code": company["code"],
            "name": company["name"],
            "role": company["role"],
        },
        "dashboard": {
            "stats": {
                "total_modules": total_modules,
                "accessible_modules": accessible_modules,
            },
            "permissions": {
                "modules": module_list,
            },
        },
    }


def get_current_user(user_id: str, db: Session) -> dict | None:
    row = db.execute(
        text("""
            SELECT id, email, name, is_developer
            FROM users
            WHERE id = :user_id AND is_active = true
        """),
        {"user_id": user_id},
    ).mappings().first()

    if not row:
        return None

    companies = get_user_companies(user_id, db)

    return {
        "id": str(row["id"]),
        "email": row["email"],
        "name": row["name"],
        "is_developer": row["is_developer"],
        "companies": companies,
    }


def get_user_permissions(user_id: str, company_code: str, db: Session) -> dict:
    """Get a user's module permissions for a specific company."""
    rows = db.execute(
        text("""
            SELECT module_code, can_access, can_view, can_create,
                   can_edit, can_delete, can_approve
            FROM module_permissions
            WHERE user_id = :user_id AND company_code = :company_code
        """),
        {"user_id": user_id, "company_code": company_code},
    ).mappings().all()

    perm_map = {r["module_code"]: r for r in rows}

    return {
        "user_id": user_id,
        "company_code": company_code,
        "modules": [
            {
                "module_code": code,
                "module_name": name,
                "permissions": {
                    "access": bool(perm_map.get(code, {}).get("can_access")),
                    "view": bool(perm_map.get(code, {}).get("can_view")),
                    "create": bool(perm_map.get(code, {}).get("can_create")),
                    "edit": bool(perm_map.get(code, {}).get("can_edit")),
                    "delete": bool(perm_map.get(code, {}).get("can_delete")),
                    "approve": bool(perm_map.get(code, {}).get("can_approve")),
                },
            }
            for code, name in IMS_MODULES
        ],
    }


def update_user_permissions(
    user_id: str, company_code: str, modules: list[dict], db: Session
) -> dict:
    """Update a user's module permissions for a company."""
    for mod in modules:
        p = mod["permissions"]
        db.execute(
            text("""
                INSERT INTO module_permissions
                    (user_id, company_code, module_code, can_access, can_view, can_create, can_edit, can_delete, can_approve)
                VALUES
                    (:user_id, :company_code, :module_code, :can_access, :can_view, :can_create, :can_edit, :can_delete, :can_approve)
                ON CONFLICT (user_id, company_code, module_code)
                DO UPDATE SET
                    can_access  = EXCLUDED.can_access,
                    can_view    = EXCLUDED.can_view,
                    can_create  = EXCLUDED.can_create,
                    can_edit    = EXCLUDED.can_edit,
                    can_delete  = EXCLUDED.can_delete,
                    can_approve = EXCLUDED.can_approve
            """),
            {
                "user_id": user_id,
                "company_code": company_code,
                "module_code": mod["module_code"],
                "can_access": p["access"],
                "can_view": p["view"],
                "can_create": p["create"],
                "can_edit": p["edit"],
                "can_delete": p["delete"],
                "can_approve": p["approve"],
            },
        )

    logger.info(f"Updated permissions for user {user_id} in {company_code}: {len(modules)} modules")
    return {"status": "updated", "user_id": user_id, "company_code": company_code, "modules_updated": len(modules)}


def get_user_company_roles(user_id: str, db: Session) -> list[dict]:
    """Get all active companies with the user's current role (None if unassigned)."""
    rows = db.execute(
        text("""
            SELECT c.code AS company_code, c.name AS company_name, ucr.role
            FROM companies c
            LEFT JOIN user_company_roles ucr
                ON c.code = ucr.company_code AND ucr.user_id = :uid
            WHERE c.is_active = true
            ORDER BY c.code
        """),
        {"uid": user_id},
    ).mappings().all()

    return [
        {
            "company_code": r["company_code"],
            "company_name": r["company_name"],
            "role": r["role"],
        }
        for r in rows
    ]


def update_user_company_roles(user_id: str, companies: list[dict], db: Session) -> dict:
    """Replace all company role assignments for a user."""
    # Clear existing roles
    db.execute(
        text("DELETE FROM user_company_roles WHERE user_id = :uid"),
        {"uid": user_id},
    )

    # Insert new roles
    for item in companies:
        db.execute(
            text("""
                INSERT INTO user_company_roles (user_id, company_code, role)
                VALUES (:user_id, :company_code, :role)
            """),
            {
                "user_id": user_id,
                "company_code": item["company_code"],
                "role": item["role"],
            },
        )

    logger.info(f"Updated company roles for user {user_id}: {len(companies)} companies")
    return {"status": "updated", "user_id": user_id, "companies_assigned": len(companies)}


def check_permission(
    user_id: str, company_code: str, module_code: str, action: str, db: Session
) -> dict:
    # Developers always have full permissions
    user_row = db.execute(
        text("SELECT is_developer FROM users WHERE id = :uid AND is_active = true"),
        {"uid": user_id},
    ).mappings().first()
    if user_row and user_row["is_developer"]:
        return {
            "has_permission": True,
            "user_id": user_id,
            "company": company_code,
            "module": module_code,
            "action": action,
        }

    row = db.execute(
        text("""
            SELECT
                CASE :action
                    WHEN 'access'  THEN mp.can_access
                    WHEN 'view'    THEN mp.can_view
                    WHEN 'create'  THEN mp.can_create
                    WHEN 'edit'    THEN mp.can_edit
                    WHEN 'delete'  THEN mp.can_delete
                    WHEN 'approve' THEN mp.can_approve
                    ELSE false
                END AS has_permission
            FROM module_permissions mp
            WHERE mp.user_id = :user_id
              AND mp.company_code = :company_code
              AND mp.module_code = :module_code
        """),
        {
            "user_id": user_id,
            "company_code": company_code,
            "module_code": module_code,
            "action": action,
        },
    ).mappings().first()

    return {
        "has_permission": bool(row["has_permission"]) if row else False,
        "user_id": user_id,
        "company": company_code,
        "module": module_code,
        "action": action,
    }
