from datetime import datetime, timedelta

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError

from shared.config_loader import settings

_bearer = HTTPBearer()

IPQC_JWT_EXPIRE_HOURS = 24


def create_ipqc_token(user: dict) -> str:
    """Create a JWT token for an IPQC user. Expires in 24 hours."""
    expires = datetime.utcnow() + timedelta(hours=IPQC_JWT_EXPIRE_HOURS)
    payload = {
        "sub": str(user["id"]),
        "username": user["username"],
        "display_name": user["display_name"],
        "is_admin": user["is_admin"],
        "exp": expires,
        "type": "ipqc",
    }
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def get_current_ipqc_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> dict:
    """FastAPI dependency — decode and validate an IPQC JWT.
    Returns {"id", "username", "display_name", "is_admin"}."""
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM],
        )
        if payload.get("type") != "ipqc":
            raise JWTError("Wrong token type")
        return {
            "id": int(payload["sub"]),
            "username": payload["username"],
            "display_name": payload["display_name"],
            "is_admin": payload["is_admin"],
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired — please log in again",
        )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )
