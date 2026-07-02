"""Packing Details router — CRUD + AES-256-GCM encrypted batch tokens + a
PUBLIC (no-auth) scan endpoint for the candorfoods.in QR page.

Kept at /api/v1/packing-details so the existing web frontend + Wix Velo page
(coded against that path) work unchanged against this backend.
"""
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from shared.database import get_db
from services.ims_service.dependencies import verify_token
from services.packing_service import crypto, tools
from services.packing_service.crypto import InvalidBatchToken


router = APIRouter(prefix="/api/v1/packing-details", tags=["packing-details"])


def _created_by(user: dict) -> str:
    user = user or {}
    return user.get("email") or str(user.get("user_id") or "")


# ── request bodies ─────────────────────────────────────────────────────────
class PackingDetailCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    batch_code: str = Field(min_length=1)
    article_name: str = Field(min_length=1)
    details: dict[str, Any] = Field(default_factory=dict)


class PackingDetailUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    batch_code: Optional[str] = Field(default=None, min_length=1)
    article_name: Optional[str] = Field(default=None, min_length=1)
    details: Optional[dict[str, Any]] = None


class BatchTokenMintRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    batch_code: str = Field(min_length=1)


class EncryptedBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    batch_token: str = Field(min_length=1)


def _invalid_token() -> HTTPException:
    return HTTPException(
        400, detail={"error": "invalid_batch_token",
                     "message": "The batch token could not be decrypted"},
    )


def _not_found(packing_id: int) -> HTTPException:
    return HTTPException(
        404, detail={"error": "packing_detail_not_found", "packing_id": packing_id},
    )


# ── create ─────────────────────────────────────────────────────────────────
@router.post("", status_code=201)
def create_packing_detail(
    body: PackingDetailCreate,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_token),
):
    return tools.create_packing_detail(
        db, batch_code=body.batch_code, article_name=body.article_name,
        details=body.details, created_by=_created_by(user),
    )


# ── encrypted batch-token access ───────────────────────────────────────────
@router.post("/batch-token")
def mint_batch_token(
    body: BatchTokenMintRequest,
    user: dict = Depends(verify_token),
):
    """Encrypt a plaintext batch_code into a token the QR/frontend can store."""
    return {"batch_token": crypto.encrypt_batch_code(body.batch_code)}


@router.post("/by-encrypted-batch")
def fetch_by_encrypted_batch(
    body: EncryptedBatchRequest,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_token),
):
    try:
        batch_code = crypto.decrypt_batch_code(body.batch_token)
    except InvalidBatchToken:
        raise _invalid_token()
    return tools.list_packing_details(db, batch_code=batch_code, limit=500, offset=0)


@router.get("/public/scan")
def public_scan(
    t: str = Query(..., min_length=1, description="AES-GCM batch token from a scanned QR"),
    db: Session = Depends(get_db),
):
    """PUBLIC (no auth) — decrypt a scanned QR's batch token and return the
    packing block details for that batch. The token is the capability; only a
    token minted server-side decrypts. Used by the candorfoods.in Wix page."""
    try:
        batch_code = crypto.decrypt_batch_code(t)
    except InvalidBatchToken:
        raise _invalid_token()
    rows = tools.list_packing_details(db, batch_code=batch_code, limit=500, offset=0)
    return {
        "batch_code": batch_code,
        "count": len(rows),
        "records": [
            {"id": r["id"], "article_name": r["article_name"],
             "details": r["details"], "created_at": r["created_at"]}
            for r in rows
        ],
    }


# ── read ───────────────────────────────────────────────────────────────────
@router.get("")
def list_packing_details(
    batch_code: Optional[str] = Query(None),
    article_name: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    user: dict = Depends(verify_token),
):
    return tools.list_packing_details(
        db, batch_code=batch_code, article_name=article_name, limit=limit, offset=offset,
    )


@router.get("/{packing_id}")
def get_packing_detail(
    packing_id: int,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_token),
):
    row = tools.get_packing_detail(db, packing_id)
    if row is None:
        raise _not_found(packing_id)
    return row


# ── update / delete ────────────────────────────────────────────────────────
@router.patch("/{packing_id}")
def update_packing_detail(
    packing_id: int,
    body: PackingDetailUpdate,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_token),
):
    provided = body.model_dump(exclude_unset=True)
    if not provided:
        raise HTTPException(
            400, detail={"error": "empty_update", "message": "Provide at least one field to update"},
        )
    row = tools.update_packing_detail(
        db, packing_id, batch_code=body.batch_code, article_name=body.article_name,
        details=body.details, details_provided="details" in provided,
    )
    if row is None:
        raise _not_found(packing_id)
    return row


@router.delete("/{packing_id}", status_code=204)
def delete_packing_detail(
    packing_id: int,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_token),
):
    if not tools.delete_packing_detail(db, packing_id):
        raise _not_found(packing_id)
