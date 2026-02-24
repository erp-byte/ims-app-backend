from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from shared.database import get_db
from shared.constants import API_PREFIX
from shared.models import Promoter
from services.crypto_service import decrypt_request, encrypt_response, EncryptedRequest, EncryptedResponse
from services.auth_service.models import (
    LoginRequest, RegisterRequest, UpdatePromoterRequest,
    ChangePasswordRequest, SendOTPRequest, VerifyOTPRequest, ResetPasswordRequest,
    PunchInRequest, PunchOutRequest,
)
from services.auth_service.tools import (
    login, register_promoter, update_promoter, delete_promoter,
    change_password, send_otp, verify_otp, reset_password,
    punch_in, punch_out, session_status,
)
from services.auth_service.dependencies import get_current_promoter

router = APIRouter(prefix=API_PREFIX, tags=["auth"])


@router.post("/login", response_model=EncryptedResponse)
def login_endpoint(request: EncryptedRequest, db: Session = Depends(get_db)):
    decrypted = decrypt_request(request.payload)
    login_data = LoginRequest(**decrypted)

    result = login(
        email=login_data.email,
        password=login_data.password,
        db=db,
    )

    return encrypt_response(result)


@router.post("/register", response_model=EncryptedResponse)
def register_endpoint(request: EncryptedRequest, db: Session = Depends(get_db)):
    decrypted = decrypt_request(request.payload)
    reg_data = RegisterRequest(**decrypted)

    result = register_promoter(
        name=reg_data.name,
        email=reg_data.email,
        password=reg_data.password,
        contact_number=reg_data.contact_number,
        db=db,
    )

    return encrypt_response(result)


@router.put("/promoter-update", response_model=EncryptedResponse)
def update_promoter_endpoint(
    request: EncryptedRequest,
    promoter: Promoter = Depends(get_current_promoter),
    db: Session = Depends(get_db),
):
    decrypted = decrypt_request(request.payload)
    update_data = UpdatePromoterRequest(**decrypted)
    updates = update_data.model_dump(exclude_none=True)

    result = update_promoter(promoter=promoter, updates=updates, db=db)

    return encrypt_response(result)


@router.delete("/promoter-delete", response_model=EncryptedResponse)
def delete_promoter_endpoint(
    promoter: Promoter = Depends(get_current_promoter),
    db: Session = Depends(get_db),
):
    result = delete_promoter(promoter=promoter, db=db)

    return encrypt_response(result)


@router.post("/change-password", response_model=EncryptedResponse)
def change_password_endpoint(request: EncryptedRequest, db: Session = Depends(get_db)):
    decrypted = decrypt_request(request.payload)
    data = ChangePasswordRequest(**decrypted)

    result = change_password(
        email=data.email,
        old_password=data.old_password,
        new_password=data.new_password,
        db=db,
    )

    return encrypt_response(result)


@router.post("/send-otp", response_model=EncryptedResponse)
def send_otp_endpoint(request: EncryptedRequest, db: Session = Depends(get_db)):
    decrypted = decrypt_request(request.payload)
    data = SendOTPRequest(**decrypted)

    result = send_otp(email=data.email, db=db)

    return encrypt_response(result)


@router.post("/verify-otp", response_model=EncryptedResponse)
def verify_otp_endpoint(request: EncryptedRequest, db: Session = Depends(get_db)):
    decrypted = decrypt_request(request.payload)
    data = VerifyOTPRequest(**decrypted)

    result = verify_otp(email=data.email, otp=data.otp, db=db)

    return encrypt_response(result)


@router.post("/reset-password", response_model=EncryptedResponse)
def reset_password_endpoint(request: EncryptedRequest, db: Session = Depends(get_db)):
    decrypted = decrypt_request(request.payload)
    data = ResetPasswordRequest(**decrypted)

    result = reset_password(
        reset_token=data.reset_token,
        new_password=data.new_password,
        db=db,
    )

    return encrypt_response(result)


@router.get("/session-status", response_model=EncryptedResponse)
def session_status_endpoint(
    promoter: Promoter = Depends(get_current_promoter),
    db: Session = Depends(get_db),
):
    result = session_status(promoter=promoter, db=db)
    return encrypt_response(result)


@router.post("/punch-in", response_model=EncryptedResponse)
def punch_in_endpoint(
    request: EncryptedRequest,
    promoter: Promoter = Depends(get_current_promoter),
    db: Session = Depends(get_db),
):
    decrypted = decrypt_request(request.payload)
    data = PunchInRequest(**decrypted)

    result = punch_in(
        promoter=promoter,
        latitude=data.latitude,
        longitude=data.longitude,
        db=db,
    )

    return encrypt_response(result)


@router.post("/punch-out", response_model=EncryptedResponse)
def punch_out_endpoint(
    request: EncryptedRequest,
    promoter: Promoter = Depends(get_current_promoter),
    db: Session = Depends(get_db),
):
    decrypted = decrypt_request(request.payload)
    data = PunchOutRequest(**decrypted)

    result = punch_out(
        promoter=promoter,
        latitude=data.latitude,
        longitude=data.longitude,
        submitted_at=data.submitted_at,
        sales=[s.model_dump() for s in data.sales],
        stock_summary=[s.model_dump() for s in data.stock_summary],
        db=db,
    )

    return encrypt_response(result)
