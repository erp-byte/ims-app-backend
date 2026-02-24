import os
import json
import base64

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import HTTPException, status

from shared.config_loader import settings
from shared.logger import get_logger
from services.crypto_service.models import EncryptedResponse

logger = get_logger("crypto.tools")

_key = bytes.fromhex(settings.AES_SECRET_KEY)
_aesgcm = AESGCM(_key)
_NONCE_SIZE = 12

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


@mcp_tool(name="decrypt_request", description="Decrypt AES-256-GCM encrypted request payload")
def decrypt_request(payload: str) -> dict:
    """Decrypt a base64-encoded AES-256-GCM payload to dict."""
    try:
        raw = base64.b64decode(payload)
        nonce = raw[:_NONCE_SIZE]
        ciphertext = raw[_NONCE_SIZE:]
        plaintext = _aesgcm.decrypt(nonce, ciphertext, None)
        return json.loads(plaintext.decode("utf-8"))
    except Exception:
        logger.warning("Failed to decrypt incoming payload")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or corrupted payload",
        )


@mcp_tool(name="encrypt_response", description="Encrypt response dict to AES-256-GCM payload")
def encrypt_response(data: dict) -> EncryptedResponse:
    """Encrypt a dict and wrap it in EncryptedResponse."""
    plaintext = json.dumps(data).encode("utf-8")
    nonce = os.urandom(_NONCE_SIZE)
    ciphertext = _aesgcm.encrypt(nonce, plaintext, None)
    payload = base64.b64encode(nonce + ciphertext).decode("utf-8")
    return EncryptedResponse(payload=payload)
