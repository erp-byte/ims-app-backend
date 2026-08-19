from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.database import get_db
from services.ims_service.models import (
    LoginRequest, CreateUserRequest, UpdateUserRequest,
    UpdatePermissionsRequest, UpdateCompanyRolesRequest,
)
from services.ims_service.dependencies import verify_token
from services.ims_service.tools import (
    login,
    create_user,
    list_users,
    update_user,
    delete_user,
    get_user_companies,
    get_dashboard_info,
    get_current_user,
    check_permission,
    get_user_permissions,
    update_user_permissions,
    get_user_company_roles,
    update_user_company_roles,
)

router = APIRouter(prefix="/auth", tags=["ims-auth"])

_ADMIN_ROLES = ("admin", "developer")


def _is_developer(db: Session, user_id) -> bool:
    return bool(db.execute(
        text("SELECT is_developer FROM users WHERE id = :uid"),
        {"uid": user_id},
    ).scalar())


def _role_in(db: Session, user_id, company_code: str) -> str:
    return (db.execute(
        text("""SELECT role FROM user_company_roles
                 WHERE user_id = :uid AND company_code = :cc"""),
        {"uid": user_id, "cc": company_code},
    ).scalar() or "").strip().lower()


def _require_admin(db: Session, caller: dict, company_codes, granting_roles=()):
    """The caller must actually be allowed to hand out this access.

    `verify_token` proves only that SOMEONE is logged in — it returns just
    {user_id, email} and carries no role. The two grant endpoints below took the
    target `user_id` from the URL path and were guarded by nothing else, so any
    authenticated user could POST their own id and award themselves the admin
    role or every module permission: straight vertical privilege escalation, no
    special tooling needed.

    Rules:
      * a developer may grant anything;
      * otherwise the caller must be admin IN EVERY company being touched, so a
        cfpl admin cannot quietly grant cdpl access;
      * only a developer may hand out the 'developer' role, or an admin could
        promote themselves out of these checks entirely.
    """
    caller_id = caller.get("user_id")
    if caller_id is None:
        raise HTTPException(status_code=403, detail="Not authorized")
    if _is_developer(db, caller_id):
        return
    if any((r or "").strip().lower() == "developer" for r in granting_roles):
        raise HTTPException(
            status_code=403,
            detail="Only a developer can grant the developer role",
        )
    for cc in company_codes:
        if _role_in(db, caller_id, cc) not in _ADMIN_ROLES:
            raise HTTPException(
                status_code=403,
                detail=f"You are not an administrator of {cc}",
            )


@router.post("/login")
def login_endpoint(body: LoginRequest, db: Session = Depends(get_db)):
    result = login(email=body.email, password=body.password, db=db)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )
    return result


@router.get("/users")
def list_users_endpoint(db: Session = Depends(get_db)):
    return list_users(db)


@router.post("/users", status_code=201)
def create_user_endpoint(body: CreateUserRequest, db: Session = Depends(get_db)):
    result = create_user(
        email=body.email,
        password=body.password,
        name=body.name,
        is_developer=body.is_developer,
        is_active=body.is_active,
        db=db,
    )
    if not result:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already registered",
        )
    return result


@router.put("/users/{user_id}")
def update_user_endpoint(
    user_id: str,
    body: UpdateUserRequest,
    db: Session = Depends(get_db),
):
    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update",
        )
    result = update_user(user_id=user_id, updates=updates, db=db)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    if result == "email_conflict":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already in use",
        )
    if result == "no_fields":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update",
        )
    return result


@router.delete("/users/{email}")
def delete_user_endpoint(email: str, db: Session = Depends(get_db)):
    if not delete_user(email=email, db=db):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    return {"message": "User deleted successfully"}


@router.get("/companies")
def get_companies_endpoint(
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return get_user_companies(user["user_id"], db)


@router.get("/company/{company_code}/dashboard-info")
def get_dashboard_info_endpoint(
    company_code: str,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    result = get_dashboard_info(user["user_id"], company_code, db)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No access to this company",
        )
    return result


@router.get("/me")
def get_current_user_endpoint(
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    result = get_current_user(user["user_id"], db)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    return result


@router.post("/logout")
def logout_endpoint(user: dict = Depends(verify_token)):
    return {"message": "Logged out successfully"}


@router.get("/check-permissions/{company_code}/{module_code}/{action}")
def check_permission_endpoint(
    company_code: str,
    module_code: str,
    action: str,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return check_permission(user["user_id"], company_code, module_code, action, db)


# ---------- Module Permissions ----------


@router.get("/permissions/{company_code}/{user_id}")
def get_permissions_endpoint(
    company_code: str,
    user_id: str,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return get_user_permissions(user_id, company_code, db)


@router.put("/permissions/{company_code}/{user_id}")
def update_permissions_endpoint(
    company_code: str,
    user_id: str,
    body: UpdatePermissionsRequest,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    _require_admin(db, user, [company_code])
    modules = [
        {"module_code": m.module_code, "permissions": m.permissions.model_dump()}
        for m in body.modules
    ]
    return update_user_permissions(user_id, company_code, modules, db)


# ---------- Company Role Assignment ----------


@router.get("/users/{user_id}/companies")
def get_user_companies_roles_endpoint(
    user_id: str,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return get_user_company_roles(user_id, db)


@router.put("/users/{user_id}/companies")
def update_user_companies_roles_endpoint(
    user_id: str,
    body: UpdateCompanyRolesRequest,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    companies = [{"company_code": c.company_code, "role": c.role} for c in body.companies]
    _require_admin(db, user,
                   [c["company_code"] for c in companies],
                   granting_roles=[c["role"] for c in companies])
    return update_user_company_roles(user_id, companies, db)
