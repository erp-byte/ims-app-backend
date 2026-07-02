"""AES-256-GCM symmetric encryption for packing "batch tokens".

The QR / frontend holds an opaque, authenticated ciphertext that encodes a
plaintext ``batch_code``; the backend decrypts it with a server-only key and
never exposes the key. AES-GCM is authenticated, so a tampered / wrong-key
token fails closed (``InvalidTag`` -> :class:`InvalidBatchToken`).

Token layout::  urlsafe_b64( nonce[12] || ciphertext-with-tag )

Key: ``settings.PACKING_TOKEN_KEY`` if set, else the existing
``settings.AES_SECRET_KEY``. Accepts either a 64-char hex string (32 bytes,
the AES_SECRET_KEY convention) or urlsafe-base64 of 16/24/32 bytes.
"""
from __future__ import annotations

import base64
import os
from functools import lru_cache

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from shared.config_loader import settings

_NONCE_BYTES = 12  # 96-bit nonce is the AES-GCM standard size


class InvalidBatchToken(Exception):
    """A batch token could not be decoded/decrypted (malformed, tampered, or
    produced under a different key)."""


def _decode_key(raw: str) -> bytes:
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError(
            "No packing token key configured — set PACKING_TOKEN_KEY or AES_SECRET_KEY."
        )
    # 64 hex chars -> 32 bytes (matches the AES_SECRET_KEY convention).
    if len(raw) == 64 and all(c in "0123456789abcdefABCDEF" for c in raw):
        return bytes.fromhex(raw)
    # Otherwise treat as urlsafe-base64 of 16/24/32 bytes.
    try:
        return base64.urlsafe_b64decode(raw)
    except (ValueError, TypeError) as exc:
        raise RuntimeError(
            "Packing token key is neither 64-char hex nor urlsafe-base64."
        ) from exc


@lru_cache(maxsize=1)
def _cipher() -> AESGCM:
    key = _decode_key(settings.PACKING_TOKEN_KEY or settings.AES_SECRET_KEY)
    if len(key) not in (16, 24, 32):
        raise RuntimeError(f"Packing token key must be 16/24/32 bytes (got {len(key)}).")
    return AESGCM(key)


def encrypt_batch_code(batch_code: str) -> str:
    """Encrypt a batch_code into an opaque, URL-safe batch token."""
    nonce = os.urandom(_NONCE_BYTES)
    ct = _cipher().encrypt(nonce, batch_code.encode("utf-8"), None)
    return base64.urlsafe_b64encode(nonce + ct).decode("ascii")


def decrypt_batch_code(token: str) -> str:
    """Decrypt a batch token back to its plaintext batch_code.

    Raises :class:`InvalidBatchToken` on any malformed / tampered / wrong-key
    input so callers can map it to a 400.
    """
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
    except (ValueError, TypeError, UnicodeEncodeError) as exc:
        raise InvalidBatchToken("token is not valid base64") from exc
    if len(raw) <= _NONCE_BYTES:
        raise InvalidBatchToken("token too short to contain a nonce + ciphertext")
    nonce, ct = raw[:_NONCE_BYTES], raw[_NONCE_BYTES:]
    try:
        plaintext = _cipher().decrypt(nonce, ct, None)
    except InvalidTag as exc:
        raise InvalidBatchToken("token failed authentication") from exc
    try:
        return plaintext.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidBatchToken("decrypted payload is not valid UTF-8") from exc
