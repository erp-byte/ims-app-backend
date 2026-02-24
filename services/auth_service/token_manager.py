import uuid
from datetime import datetime, timedelta

from jose import jwt, JWTError

from shared.config_loader import settings


def create_access_token(promoter_id: str) -> str:
    expires = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    payload = {
        "sub": promoter_id,
        "exp": expires,
        "type": "access",
    }
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def create_refresh_token(promoter_id: str) -> tuple[str, datetime]:
    expires = datetime.utcnow() + timedelta(hours=settings.REFRESH_TOKEN_EXPIRE_HOURS)
    payload = {
        "sub": promoter_id,
        "exp": expires,
        "type": "refresh",
        "jti": str(uuid.uuid4()),
    }
    token = jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
    return token, expires


def create_reset_token(email: str) -> str:
    expires = datetime.utcnow() + timedelta(minutes=5)
    payload = {
        "sub": email,
        "exp": expires,
        "type": "reset",
    }
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(
            token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM]
        )
    except JWTError:
        return None
