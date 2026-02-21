from datetime import datetime
from typing import Optional

from pydantic import BaseModel, EmailStr


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class LoginResponse(BaseModel):
    status_code: int
    message: str
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class RegisterRequest(BaseModel):
    name: str
    email: EmailStr
    password: str
    contact_number: str


class UpdatePromoterRequest(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    contact_number: Optional[str] = None
    password: Optional[str] = None


class ChangePasswordRequest(BaseModel):
    email: EmailStr
    old_password: str
    new_password: str


class SendOTPRequest(BaseModel):
    email: EmailStr


class VerifyOTPRequest(BaseModel):
    email: EmailStr
    otp: str


class ResetPasswordRequest(BaseModel):
    reset_token: str
    new_password: str


# ---- Punch-in / Punch-out ----


class PunchInRequest(BaseModel):
    latitude: float
    longitude: float


class SaleItem(BaseModel):
    ean: str
    qty_sold: int
    timestamp: datetime


class StockSummaryItem(BaseModel):
    ean: str
    opening_qty: int
    qty_received: int
    qty_sold: int
    closing_stock: int


class PunchOutRequest(BaseModel):
    latitude: float
    longitude: float
    submitted_at: datetime
    sales: list[SaleItem] = []
    stock_summary: list[StockSummaryItem] = []
