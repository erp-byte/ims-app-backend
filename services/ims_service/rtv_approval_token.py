"""
Signed token helpers for RTV magic-link approval buttons.

Each Approve / Reject / Hold button in the action email carries a JWT that
encodes the RTV identity, the assigned business head email, and the action.
The /rtv/email-action endpoint verifies the signature and applies the change.
"""

from __future__ import annotations

import time
from typing import Literal
from urllib.parse import urlencode

from jose import jwt, JWTError

from shared.config_loader import settings

ALG = "HS256"
ActionType = Literal["approve", "reject", "hold"]


def _secret() -> str:
    return settings.IMS_JWT_SECRET or settings.JWT_SECRET_KEY


def make_action_token(
    rtv_id: str,
    rtv_db_id: int,
    company: str,
    head_email: str,
    action: ActionType,
) -> str:
    payload = {
        "rtv": rtv_id,
        "rid": rtv_db_id,
        "co": company,
        "he": (head_email or "").lower(),
        "act": action,
        "exp": int(time.time()) + settings.RTV_ACTION_TOKEN_TTL_DAYS * 86400,
    }
    return jwt.encode(payload, _secret(), algorithm=ALG)


def verify_action_token(token: str) -> dict:
    """Decode the token. Raises jose.JWTError on bad signature or expiry."""
    return jwt.decode(token, _secret(), algorithms=[ALG])


def action_url(
    rtv_id: str,
    rtv_db_id: int,
    company: str,
    head_email: str,
    action: ActionType,
) -> str:
    token = make_action_token(rtv_id, rtv_db_id, company, head_email, action)
    base = settings.APP_BASE_URL.rstrip("/")
    return f"{base}/rtv/email-action?{urlencode({'token': token})}"


__all__ = ["make_action_token", "verify_action_token", "action_url", "JWTError"]
