"""Lot Search router — strict email allowlist."""

from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy.orm import Session

from shared.database import get_db
from services.lot_search_service.tools import search_lot


router = APIRouter(prefix="/lot-search", tags=["lot-search"])


LOT_SEARCH_ALLOWED_EMAILS = {"yash@candorfoods.in", "b.hrithik@candorfoods.in"}


def _require_allowed(email: Optional[str]) -> str:
    if not email or email.strip().lower() not in LOT_SEARCH_ALLOWED_EMAILS:
        raise HTTPException(status_code=403, detail="Not authorized to use Lot Search")
    return email.strip().lower()


@router.get("")
def lot_search_endpoint(
    lot_number: Optional[str] = Query(None),
    box_id: Optional[str] = Query(None),
    transaction_no: Optional[str] = Query(None),
    x_user_email: Optional[str] = Header(None, alias="X-User-Email"),
    db: Session = Depends(get_db),
):
    _require_allowed(x_user_email)

    lot = (lot_number or "").strip() or None
    box = (box_id or "").strip() or None
    txn = (transaction_no or "").strip() or None
    if not (lot or box or txn):
        raise HTTPException(status_code=422, detail="Provide at least one of lot_number, box_id, transaction_no")

    return search_lot(db, lot, box, txn)
