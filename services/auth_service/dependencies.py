from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from shared.database import get_db
from shared.models import Promoter
from services.auth_service.token_manager import decode_token

_bearer = HTTPBearer()


def get_current_promoter(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
    db: Session = Depends(get_db),
) -> Promoter:
    """Extract and validate the access token, return the Promoter ORM object."""
    payload = decode_token(credentials.credentials)

    if not payload or payload.get("type") != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )

    promoter = db.get(Promoter, payload["sub"])
    if not promoter:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Promoter not found",
        )

    return promoter
