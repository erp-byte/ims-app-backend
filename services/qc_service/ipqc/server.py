from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from shared.database import get_db
from services.qc_service.ipqc.models import (
    IPQCCreateRequest,
    IPQCUpdateRequest,
    IPQCApprovalRequest,
    IPQCAdminActionRequest,
    IPQCResponse,
    IPQCListResponse,
    IPQCDeleteResponse,
    IPQCSKULookupRequest,
    IPQCSKULookupResponse,
    IPQCSKUSearchResponse,
)
from services.qc_service.ipqc.tools import (
    create_ipqc,
    list_ipqc,
    get_ipqc,
    update_ipqc,
    delete_ipqc,
    approve_ipqc,
    lookup_ipqc_sku,
    search_ipqc_sku,
)

router = APIRouter(prefix="/qc/ipqc", tags=["qc-ipqc"])


# ── SKU Lookup ───────────────────────────────


@router.post("/sku-lookup", response_model=IPQCSKULookupResponse)
def sku_lookup_endpoint(body: IPQCSKULookupRequest, db: Session = Depends(get_db)):
    """Lookup SKU from unified ipqc_sku table — no company param needed."""
    result = lookup_ipqc_sku(body.item_description, db)
    if result is None:
        return IPQCSKULookupResponse(item_description=body.item_description)
    return IPQCSKULookupResponse(**result)


@router.get("/sku-search", response_model=IPQCSKUSearchResponse)
def sku_search_endpoint(
    search: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
):
    """Search SKUs by partial item description match."""
    return search_ipqc_sku(search, db)


# ── Create ────────────────────────────────────


@router.post("", response_model=IPQCResponse, status_code=201)
def create_endpoint(payload: IPQCCreateRequest, db: Session = Depends(get_db)):
    return create_ipqc(payload, db)


# ── List ──────────────────────────────────────


@router.get("", response_model=IPQCListResponse)
def list_endpoint(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    customer: Optional[str] = Query(None),
    verdict: Optional[str] = Query(None),
    factory_code: Optional[str] = Query(None),
    floor: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    sort_by: str = Query("created_at"),
    sort_order: str = Query("desc"),
    db: Session = Depends(get_db),
):
    return list_ipqc(
        page=page,
        per_page=per_page,
        customer=customer,
        verdict=verdict,
        factory_code=factory_code,
        floor=floor,
        from_date=from_date,
        to_date=to_date,
        search=search,
        sort_by=sort_by,
        sort_order=sort_order,
        db=db,
    )


# ── Get single ───────────────────────────────


@router.get("/{ipqc_no}", response_model=IPQCResponse)
def get_endpoint(ipqc_no: str, db: Session = Depends(get_db)):
    return get_ipqc(ipqc_no, db)


# ── Update ────────────────────────────────────


@router.put("/{ipqc_no}", response_model=IPQCResponse)
def update_endpoint(
    ipqc_no: str,
    data: IPQCUpdateRequest,
    username: str = Query(..., description="Admin email"),
    password: str = Query(..., description="Admin password"),
    db: Session = Depends(get_db),
):
    """Update an IPQC record. Admin only."""
    return update_ipqc(ipqc_no, data.model_dump(exclude_none=True), username, password, db)


# ── Delete ────────────────────────────────────


@router.delete("/{ipqc_no}", response_model=IPQCDeleteResponse)
def delete_endpoint(
    ipqc_no: str,
    username: str = Query(..., description="Admin email"),
    password: str = Query(..., description="Admin password"),
    db: Session = Depends(get_db),
):
    """Delete an IPQC record. Admin only."""
    return delete_ipqc(ipqc_no, username, password, db)


# ── Approve ───────────────────────────────────


@router.post("/{ipqc_no}/approve", response_model=IPQCResponse)
def approve_endpoint(
    ipqc_no: str,
    data: IPQCApprovalRequest,
    db: Session = Depends(get_db),
):
    return approve_ipqc(ipqc_no, data.username, data.password, db)
