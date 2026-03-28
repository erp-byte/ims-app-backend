from typing import List

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from shared.database import get_db
from services.qc_service.ipqc.user_models import (
    UserCreateRequest,
    UserLoginRequest,
    UserResetPasswordRequest,
    UserResponse,
    UserLoginResponse,
    UserMessageResponse,
)
from services.qc_service.ipqc.user_tools import (
    create_user,
    login_user,
    reset_password,
    list_users,
)

router = APIRouter(prefix="/qc/ipqc/users", tags=["qc-ipqc-users"])


# ── Register ─────────────────────────────────


@router.post("", response_model=UserResponse, status_code=201)
def create_user_endpoint(data: UserCreateRequest, db: Session = Depends(get_db)):
    return create_user(data, db)


# ── Login ────────────────────────────────────


@router.post("/login", response_model=UserLoginResponse)
def login_endpoint(data: UserLoginRequest, db: Session = Depends(get_db)):
    return login_user(data, db)


# ── Reset Password ───────────────────────────


@router.post("/reset-password", response_model=UserMessageResponse)
def reset_password_endpoint(data: UserResetPasswordRequest, db: Session = Depends(get_db)):
    return reset_password(data, db)


# ── List Users ───────────────────────────────


@router.get("", response_model=List[UserResponse])
def list_users_endpoint(db: Session = Depends(get_db)):
    return list_users(db)
