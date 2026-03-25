from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from shared.database import get_db
from services.bulk_entry_service.models import (
    Company,
    BulkEntryPayload,
    BulkEntryFullUpdate,
    BulkEntryResponse,
    BulkEntryDetailResponse,
    BulkEntryListResponse,
    BulkEntryDeleteResponse,
    BoxUpsertRequest,
    BoxResponse,
    BoxListResponse,
    BoxUpsertResponse,
    TransactionResponse,
)
from services.bulk_entry_service.tools import (
    create_bulk_entry,
    list_bulk_entries,
    get_bulk_entry,
    update_bulk_entry,
    delete_bulk_entry,
    list_boxes,
    lookup_box,
    upsert_box,
)

router = APIRouter(prefix="/bulk-entry", tags=["bulk-entry"])


# ── Create ────────────────────────────────────


@router.post("", response_model=BulkEntryResponse, status_code=201)
def create_endpoint(payload: BulkEntryPayload, db: Session = Depends(get_db)):
    """Create a bulk entry with transaction, articles, and auto-generated boxes."""
    return create_bulk_entry(payload, db)


# ── List ──────────────────────────────────────


@router.get("/{company}", response_model=BulkEntryListResponse)
def list_endpoint(
    company: Company,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=500),
    status: Optional[str] = Query(None),
    vendor: Optional[str] = Query(None),
    source_location: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    sort_by: str = Query("created_at"),
    sort_order: str = Query("desc"),
    db: Session = Depends(get_db),
):
    return list_bulk_entries(
        company=company,
        page=page,
        per_page=per_page,
        status=status,
        vendor=vendor,
        source_location=source_location,
        search=search,
        from_date=from_date,
        to_date=to_date,
        sort_by=sort_by,
        sort_order=sort_order,
        db=db,
    )


# ── Box lookup (before /{company}/{transaction_no} to avoid path conflict) ──


@router.get("/{company}/box/{box_id}", response_model=BoxResponse)
def box_lookup_endpoint(company: Company, box_id: str, db: Session = Depends(get_db)):
    return lookup_box(company, box_id, db)


# ── Single entry detail ──────────────────────


@router.get("/{company}/{transaction_no}", response_model=BulkEntryDetailResponse)
def get_endpoint(company: Company, transaction_no: str, db: Session = Depends(get_db)):
    return get_bulk_entry(company, transaction_no, db)


# ── Update transaction ───────────────────────


@router.put("/{company}/{transaction_no}", response_model=TransactionResponse)
def update_endpoint(
    company: Company,
    transaction_no: str,
    data: BulkEntryFullUpdate,
    db: Session = Depends(get_db),
):
    return update_bulk_entry(company, transaction_no, data, db)


# ── Delete ───────────────────────────────────


@router.delete("/{company}/{transaction_no}", response_model=BulkEntryDeleteResponse)
def delete_endpoint(company: Company, transaction_no: str, db: Session = Depends(get_db)):
    return delete_bulk_entry(company, transaction_no, db)


# ── Box endpoints ────────────────────────────


@router.get("/{company}/{transaction_no}/boxes", response_model=BoxListResponse)
def list_boxes_endpoint(company: Company, transaction_no: str, db: Session = Depends(get_db)):
    return list_boxes(company, transaction_no, db)


@router.put("/{company}/{transaction_no}/box", response_model=BoxUpsertResponse)
def upsert_box_endpoint(
    company: Company,
    transaction_no: str,
    data: BoxUpsertRequest,
    db: Session = Depends(get_db),
):
    return upsert_box(company, transaction_no, data.model_dump(exclude_none=True), db)
