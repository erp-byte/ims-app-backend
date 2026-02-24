from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError

from shared.config_loader import settings

_bearer = HTTPBearer()


def verify_token(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> dict:
    """Decode and validate an IMS JWT. Returns {"user_id": ..., "email": ...}."""
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.IMS_JWT_SECRET,
            algorithms=[settings.IMS_JWT_ALGORITHM],
        )
        return {
            "user_id": payload.get("user_id"),
            "email": payload.get("email"),
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired",
        )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )
