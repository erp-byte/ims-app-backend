import random
import smtplib
from datetime import datetime, date, timedelta
from email.message import EmailMessage

from sqlalchemy.orm import Session
from sqlalchemy import select, update, delete, cast, Date

from shared.models import (
    Promoter, RefreshToken, Attendance, PasswordResetOTP, Product,
    DailySale, DailyStockSummary,
)
from shared.config_loader import settings
from shared.exceptions import (
    InvalidCredentials, EmailNotFound, InvalidOTP, OTPExpired,
    NoActiveSession,
)
from shared.logger import get_logger
from shared.kafka_producer import publish_geocoding_task
from services.auth_service.authenticator import verify_password, hash_password
from services.auth_service.token_manager import (
    create_access_token, create_refresh_token, create_reset_token, decode_token,
)

logger = get_logger("auth.tools")

_registry = {}


def mcp_tool(name: str = None, description: str = None):
    """Decorator to register functions as discoverable tools."""
    def decorator(func):
        tool_name = name or func.__name__
        _registry[tool_name] = {
            "handler": func,
            "description": description or func.__doc__,
        }
        func._tool_name = tool_name
        return func
    return decorator


def get_tools() -> dict:
    return _registry


@mcp_tool(name="login", description="Authenticate promoter with email and password")
def login(
    email: str,
    password: str,
    db: Session,
) -> dict:
    result = db.execute(select(Promoter).where(Promoter.email == email))
    promoter = result.scalar_one_or_none()

    if not promoter or not verify_password(password, promoter.password_hash):
        logger.warning(f"Failed login attempt for email: {email}")
        raise InvalidCredentials()

    promoter_id = str(promoter.id)
    access_token = create_access_token(promoter_id)
    refresh_token, expires_at = create_refresh_token(promoter_id)

    db_refresh_token = RefreshToken(
        promoter_id=promoter.id,
        token=refresh_token,
        expires_at=expires_at,
    )
    db.add(db_refresh_token)

    products = db.execute(
        select(Product).order_by(Product.sr_no)
    ).scalars().all()

    products_list = [
        {
            "sr_no": p.sr_no,
            "ean": p.ean,
            "article_code": p.article_code,
            "description": p.description,
            "mrp": p.mrp,
            "size_kg": float(p.size_kg),
            "gst_rate": float(p.gst_rate),
        }
        for p in products
    ]

    logger.info(f"Login for promoter: {promoter_id}")

    return {
        "status_code": 200,
        "message": "Login successful",
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "promoter_name": promoter.name,
        "products": products_list,
    }


# --------------- Punch-in / Punch-out ---------------


@mcp_tool(name="punch_in", description="Record punch-in with geolocation")
def punch_in(
    promoter: Promoter,
    latitude: float,
    longitude: float,
    db: Session,
) -> dict:
    today = date.today()
    existing = db.execute(
        select(Attendance).where(
            Attendance.promoter_id == promoter.id,
            cast(Attendance.punch_in_timestamp, Date) == today,
            Attendance.punch_out_timestamp.is_(None),
        )
    ).scalars().first()

    products = db.execute(
        select(Product).order_by(Product.sr_no)
    ).scalars().all()

    products_list = [
        {
            "sr_no": p.sr_no,
            "ean": p.ean,
            "article_code": p.article_code,
            "description": p.description,
            "mrp": p.mrp,
            "size_kg": float(p.size_kg),
            "gst_rate": float(p.gst_rate),
        }
        for p in products
    ]

    if existing:
        logger.info(f"Already punched in today for promoter: {promoter.id}, returning products")
        return {
            "status_code": 200,
            "message": "Already punched in today",
            "attendance_id": str(existing.id),
            "already_punched_in": True,
            "punch_in_store": existing.punch_in_store,
            "products": products_list,
        }

    attendance = Attendance(
        promoter_id=promoter.id,
        punch_in_timestamp=datetime.utcnow(),
        punch_in_lat=latitude,
        punch_in_lng=longitude,
        punch_in_store="Resolving...",
    )
    db.add(attendance)
    db.flush()

    publish_geocoding_task(str(attendance.id), latitude, longitude)

    logger.info(f"Punch-in for promoter: {promoter.id} at ({latitude}, {longitude})")

    return {
        "status_code": 200,
        "message": "Punch-in recorded",
        "attendance_id": str(attendance.id),
        "already_punched_in": False,
        "punch_in_store": "Resolving...",
        "products": products_list,
    }


@mcp_tool(name="session_status", description="Check current punch-in status for today")
def session_status(
    promoter: Promoter,
    db: Session,
) -> dict:
    today = date.today()
    active = db.execute(
        select(Attendance).where(
            Attendance.promoter_id == promoter.id,
            cast(Attendance.punch_in_timestamp, Date) == today,
            Attendance.punch_out_timestamp.is_(None),
        )
    ).scalars().first()

    if active:
        return {
            "status_code": 200,
            "punched_in": True,
            "attendance_id": str(active.id),
            "punch_in_timestamp": active.punch_in_timestamp.isoformat(),
            "punch_in_store": active.punch_in_store,
        }

    # Check for the last closed session today
    last_closed = db.execute(
        select(Attendance).where(
            Attendance.promoter_id == promoter.id,
            cast(Attendance.punch_in_timestamp, Date) == today,
        )
        .order_by(Attendance.punch_in_timestamp.desc())
    ).scalars().first()

    return {
        "status_code": 200,
        "punched_in": False,
        "punch_in_store": last_closed.punch_in_store if last_closed else None,
    }


@mcp_tool(name="punch_out", description="Record punch-out with sales and stock data")
def punch_out(
    promoter: Promoter,
    latitude: float,
    longitude: float,
    submitted_at: datetime,
    sales: list[dict],
    stock_summary: list[dict],
    db: Session,
) -> dict:
    today = date.today()
    attendance = db.execute(
        select(Attendance).where(
            Attendance.promoter_id == promoter.id,
            cast(Attendance.punch_in_timestamp, Date) == today,
            Attendance.punch_out_timestamp.is_(None),
        )
    ).scalars().first()

    if not attendance:
        raise NoActiveSession()

    # Close attendance
    attendance.punch_out_timestamp = submitted_at
    attendance.punch_out_lat = latitude
    attendance.punch_out_lng = longitude
    attendance.punch_out_store = "Resolving..."

    publish_geocoding_task(str(attendance.id), latitude, longitude, is_punch_out=True)

    # Bulk insert sales
    for item in sales:
        db.add(DailySale(
            attendance_id=attendance.id,
            promoter_id=promoter.id,
            ean=item["ean"],
            qty_sold=item["qty_sold"],
            sold_at=item["timestamp"],
        ))

    # Bulk insert stock summary
    for item in stock_summary:
        db.add(DailyStockSummary(
            attendance_id=attendance.id,
            promoter_id=promoter.id,
            ean=item["ean"],
            opening_qty=item["opening_qty"],
            qty_received=item["qty_received"],
            qty_sold=item["qty_sold"],
            closing_stock=item["closing_stock"],
        ))

    db.flush()

    logger.info(
        f"Punch-out for promoter: {promoter.id} — "
        f"{len(sales)} sale(s), {len(stock_summary)} stock row(s)"
    )

    return {
        "status_code": 200,
        "message": "Punch-out recorded",
        "attendance_id": str(attendance.id),
        "sales_count": len(sales),
        "stock_count": len(stock_summary),
    }


@mcp_tool(name="register", description="Register a new promoter")
def register_promoter(
    name: str,
    email: str,
    password: str,
    contact_number: str,
    db: Session,
) -> dict:
    existing = db.execute(select(Promoter).where(Promoter.email == email))
    if existing.scalar_one_or_none():
        logger.warning(f"Registration attempt with existing email: {email}")
        return {"status_code": 409, "message": "Email already registered"}

    promoter = Promoter(
        name=name,
        email=email,
        password_hash=hash_password(password),
        contact_number=contact_number,
    )
    db.add(promoter)
    db.flush()

    logger.info(f"Registered new promoter: {promoter.id} ({email})")

    return {
        "status_code": 201,
        "message": "Registration successful",
        "promoter_id": str(promoter.id),
    }


@mcp_tool(name="update_promoter", description="Update promoter profile fields")
def update_promoter(
    promoter: Promoter,
    updates: dict,
    db: Session,
) -> dict:
    if "password" in updates:
        updates["password_hash"] = hash_password(updates.pop("password"))

    if "email" in updates:
        existing = db.execute(
            select(Promoter).where(
                Promoter.email == updates["email"],
                Promoter.id != promoter.id,
            )
        )
        if existing.scalar_one_or_none():
            return {"status_code": 409, "message": "Email already in use"}

    for field, value in updates.items():
        setattr(promoter, field, value)

    db.flush()
    logger.info(f"Updated promoter: {promoter.id}")

    return {
        "status_code": 200,
        "message": "Promoter updated successfully",
        "promoter_id": str(promoter.id),
    }


@mcp_tool(name="delete_promoter", description="Delete a promoter account")
def delete_promoter(
    promoter: Promoter,
    db: Session,
) -> dict:
    promoter_id = str(promoter.id)
    db.delete(promoter)
    db.flush()
    logger.info(f"Deleted promoter: {promoter_id}")

    return {
        "status_code": 200,
        "message": "Promoter deleted successfully",
    }


# --------------- Password management ---------------


def _build_otp_html(otp: str) -> str:
    digits = "".join(
        f'<td style="width:44px;height:52px;text-align:center;font-size:28px;'
        f'font-weight:700;font-family:monospace;background:#f4f4f5;'
        f'border-radius:8px;border:1px solid #e4e4e7;color:#18181b;">{d}</td>'
        for d in otp
    )
    return f"""\
<html>
<body style="margin:0;padding:0;background:#f9fafb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f9fafb;padding:40px 0;">
    <tr><td align="center">
      <table width="480" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,0.08);overflow:hidden;">

        <tr><td style="background:#18181b;padding:28px 32px;">
          <span style="font-size:20px;font-weight:700;color:#ffffff;letter-spacing:-0.3px;">Candor Retail</span>
        </td></tr>

        <tr><td style="padding:36px 32px 0;">
          <p style="margin:0 0 6px;font-size:22px;font-weight:700;color:#18181b;">Password Reset</p>
          <p style="margin:0;font-size:15px;color:#71717a;line-height:1.5;">
            We received a request to reset your password. Use the verification code below to proceed.
          </p>
        </td></tr>

        <tr><td style="padding:28px 32px;">
          <table cellpadding="0" cellspacing="6" style="margin:0 auto;">
            <tr>{digits}</tr>
          </table>
        </td></tr>

        <tr><td style="padding:0 32px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#fefce8;border-radius:8px;border:1px solid #fde68a;">
            <tr><td style="padding:14px 16px;font-size:13px;color:#92400e;line-height:1.5;">
              &#9200; This code expires in <strong>3 minutes</strong>. Do not share it with anyone.
            </td></tr>
          </table>
        </td></tr>

        <tr><td style="padding:32px;border-top:1px solid #f4f4f5;margin-top:24px;">
          <p style="margin:0;font-size:12px;color:#a1a1aa;line-height:1.6;">
            If you didn't request this, you can safely ignore this email.<br>
            &copy; Candor Retail &mdash; All rights reserved.
          </p>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


def _send_otp_email(recipient: str, otp: str) -> None:
    """Send OTP email via SMTP."""
    msg = EmailMessage()
    msg["Subject"] = "Your Candor Retail verification code"
    msg["From"] = settings.SMTP_EMAIL
    msg["To"] = recipient

    # Plain-text fallback
    msg.set_content(
        f"Your verification code is: {otp}\n\n"
        "This code expires in 3 minutes. Do not share it with anyone.\n\n"
        "If you didn't request this, please ignore this email."
    )

    # HTML version
    msg.add_alternative(_build_otp_html(otp), subtype="html")

    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
        server.starttls()
        server.login(settings.SMTP_EMAIL, settings.SMTP_APP_PASSWORD)
        server.send_message(msg)


@mcp_tool(name="change_password", description="Change password using old password")
def change_password(
    email: str,
    old_password: str,
    new_password: str,
    db: Session,
) -> dict:
    promoter = db.execute(
        select(Promoter).where(Promoter.email == email)
    ).scalar_one_or_none()

    if not promoter:
        raise EmailNotFound()

    if not verify_password(old_password, promoter.password_hash):
        raise InvalidCredentials()

    promoter.password_hash = hash_password(new_password)
    db.flush()

    logger.info(f"Password changed for promoter: {promoter.id}")

    return {
        "status_code": 200,
        "message": "Password changed successfully",
    }


@mcp_tool(name="send_otp", description="Send OTP to email for password reset")
def send_otp(email: str, db: Session) -> dict:
    promoter = db.execute(
        select(Promoter).where(Promoter.email == email)
    ).scalar_one_or_none()

    if not promoter:
        raise EmailNotFound()

    # Delete any existing unused OTPs for this email
    db.execute(
        delete(PasswordResetOTP)
        .where(
            PasswordResetOTP.email == email,
            PasswordResetOTP.is_used == False,
        )
    )

    otp = f"{random.randint(0, 999999):06d}"

    otp_record = PasswordResetOTP(
        email=email,
        otp_hash=hash_password(otp),
        expires_at=datetime.utcnow() + timedelta(minutes=3),
    )
    db.add(otp_record)
    db.flush()

    _send_otp_email(email, otp)

    logger.info(f"OTP sent to {email}")

    return {
        "status_code": 200,
        "message": "OTP sent to your email",
    }


@mcp_tool(name="verify_otp", description="Verify OTP and return reset token")
def verify_otp(email: str, otp: str, db: Session) -> dict:
    otp_record = db.execute(
        select(PasswordResetOTP)
        .where(
            PasswordResetOTP.email == email,
            PasswordResetOTP.is_used == False,
        )
        .order_by(PasswordResetOTP.created_at.desc())
    ).scalars().first()

    if not otp_record:
        raise InvalidOTP()

    if datetime.utcnow() > otp_record.expires_at:
        db.delete(otp_record)
        db.flush()
        raise OTPExpired()

    if not verify_password(otp, otp_record.otp_hash):
        raise InvalidOTP()

    db.delete(otp_record)
    db.flush()

    reset_token = create_reset_token(email)

    logger.info(f"OTP verified for {email}, reset token issued")

    return {
        "status_code": 200,
        "message": "OTP verified successfully",
        "reset_token": reset_token,
    }


@mcp_tool(name="reset_password", description="Reset password using reset token")
def reset_password(reset_token: str, new_password: str, db: Session) -> dict:
    payload = decode_token(reset_token)

    if not payload or payload.get("type") != "reset":
        raise InvalidCredentials()

    email = payload.get("sub")
    promoter = db.execute(
        select(Promoter).where(Promoter.email == email)
    ).scalar_one_or_none()

    if not promoter:
        raise EmailNotFound()

    promoter.password_hash = hash_password(new_password)

    # Delete all active refresh tokens
    db.execute(
        delete(RefreshToken)
        .where(
            RefreshToken.promoter_id == promoter.id,
            RefreshToken.is_revoked == False,
        )
    )

    db.flush()

    logger.info(f"Password reset for promoter: {promoter.id}")

    return {
        "status_code": 200,
        "message": "Password reset successfully",
    }
