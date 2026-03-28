from typing import Optional

from pydantic import BaseModel, EmailStr


# ── Request schemas ──────────────────────────


class UserCreateRequest(BaseModel):
    username: EmailStr          # email address
    password: str               # plain text — hashed on the server
    display_name: str
    is_admin: bool = False


class UserLoginRequest(BaseModel):
    username: EmailStr
    password: str               # plain text


class UserResetPasswordRequest(BaseModel):
    username: EmailStr
    new_password: str           # plain text — hashed on the server


# ── Response schemas ─────────────────────────


class UserResponse(BaseModel):
    id: int
    username: str
    display_name: str
    is_admin: bool
    is_active: bool
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class UserLoginResponse(BaseModel):
    success: bool
    user_id: int
    username: str
    display_name: str
    is_admin: bool
    token: str


class UserMessageResponse(BaseModel):
    success: bool
    message: str
