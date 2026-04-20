from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from shared.database import get_db
from services.cold_storage_service.models import (
    ColdStorageCreate,
    ColdStorageUpdate,
    ColdStorageBulkCreate,
    ColdStorageResponse,
    ColdStorageListResponse,
    ColdStorageDeleteResponse,
    ColdStorageSummaryResponse,
    BulkCreateResponse,
    ColdStorageBoxUpsertRequest,
    ColdStorageBoxResponse,
    ColdStorageBoxListResponse,
    ColdStorageBoxUpsertResponse,
    ColdStorageApprovalRequest,
    ColdStorageApprovalResponse,
)
from services.cold_storage_service.tools import (
    list_cold_storage,
    get_cold_storage,
    create_cold_storage,
    update_cold_storage,
    delete_cold_storage,
    bulk_create_cold_storage,
    bulk_delete_cold_storage,
    get_cold_storage_summary,
    approve_cold_storage,
    list_boxes,
    upsert_box,
    lookup_box,
)

router = APIRouter(prefix="/cold-storage", tags=["cold-storage"])


# ── Summary (before /{record_id} to avoid path conflict) ─────────


@router.get("/summary", response_model=ColdStorageSummaryResponse)
def summary_endpoint(
    group_name: Optional[str] = Query(None),
    storage_location: Optional[str] = Query(None),
    exporter: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    return get_cold_storage_summary(group_name, storage_location, exporter, db)


# ── List ─────────────────────────────────────


@router.get("", response_model=ColdStorageListResponse)
def list_endpoint(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=500),
    group_name: Optional[str] = Query(None),
    storage_location: Optional[str] = Query(None),
    exporter: Optional[str] = Query(None),
    item_mark: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    sort_by: str = Query("id"),
    sort_order: str = Query("desc"),
    db: Session = Depends(get_db),
):
    return list_cold_storage(
        page=page,
        per_page=per_page,
        group_name=group_name,
        storage_location=storage_location,
        exporter=exporter,
        item_mark=item_mark,
        search=search,
        from_date=from_date,
        to_date=to_date,
        sort_by=sort_by,
        sort_order=sort_order,
        db=db,
    )


# ── CRUD ─────────────────────────────────────


@router.post("", response_model=ColdStorageResponse, status_code=201)
def create_endpoint(data: ColdStorageCreate, db: Session = Depends(get_db)):
    return create_cold_storage(data.model_dump(), db)


@router.post("/bulk", response_model=BulkCreateResponse, status_code=201)
def bulk_create_endpoint(data: ColdStorageBulkCreate, db: Session = Depends(get_db)):
    records = [r.model_dump() for r in data.records]
    return bulk_create_cold_storage(records, db)


@router.post("/bulk-delete")
def bulk_delete_endpoint(
    record_ids: list[int],
    db: Session = Depends(get_db),
):
    """Delete multiple cold storage records by IDs."""
    return bulk_delete_cold_storage(record_ids, db)


# ── Box endpoints (before /{record_id} to avoid path conflict) ──


@router.get("/box/{box_id}", response_model=ColdStorageBoxResponse)
def box_lookup_endpoint(box_id: str, db: Session = Depends(get_db)):
    return lookup_box(box_id, db)


@router.get("/{record_id}/boxes", response_model=ColdStorageBoxListResponse)
def list_boxes_endpoint(record_id: int, db: Session = Depends(get_db)):
    return list_boxes(record_id, db)


@router.put("/{record_id}/box", response_model=ColdStorageBoxUpsertResponse)
def upsert_box_endpoint(
    record_id: int,
    data: ColdStorageBoxUpsertRequest,
    db: Session = Depends(get_db),
):
    return upsert_box(record_id, data.model_dump(exclude_none=True), db)


# ── Approve ──────────────────────────────────


@router.put("/{record_id}/approve", response_model=ColdStorageApprovalResponse)
def approve_endpoint(
    record_id: int,
    data: ColdStorageApprovalRequest,
    db: Session = Depends(get_db),
):
    """Approve a cold storage record."""
    return approve_cold_storage(record_id, data.approved_by, db)


# ── Single record CRUD ──────────────────────


@router.get("/{record_id}", response_model=ColdStorageResponse)
def get_endpoint(record_id: int, db: Session = Depends(get_db)):
    return get_cold_storage(record_id, db)


@router.put("/{record_id}", response_model=ColdStorageResponse)
def update_endpoint(
    record_id: int,
    data: ColdStorageUpdate,
    db: Session = Depends(get_db),
):
    return update_cold_storage(record_id, data.model_dump(exclude_none=True), db)


@router.delete("/{record_id}", response_model=ColdStorageDeleteResponse)
def delete_endpoint(record_id: int, db: Session = Depends(get_db)):
    return delete_cold_storage(record_id, db)
