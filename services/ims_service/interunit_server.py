from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from shared.database import get_db
from services.ims_service.interunit_models import (
    RequestCreate,
    RequestUpdate,
    RequestWithLines,
    RequestResponse,
    WarehouseSiteResponse,
    DeleteResponse,
    TransferCreate,
    TransferWithLines,
    TransferListResponse,
    TransferDeleteResponse,
    TransferInCreate,
    TransferInDetail,
    TransferInListResponse,
)
from services.ims_service.interunit_tools import (
    get_warehouse_sites,
    create_request,
    list_requests,
    get_request,
    update_request,
    delete_request,
    create_transfer,
    list_transfers,
    get_transfer,
    delete_transfer,
    create_transfer_in,
    list_transfer_ins,
    get_transfer_in,
)

router = APIRouter(prefix="/interunit", tags=["interunit"])


@router.get("/dropdowns/warehouse-sites", response_model=List[WarehouseSiteResponse])
def get_warehouse_sites_endpoint(
    active_only: bool = Query(True),
    db: Session = Depends(get_db),
):
    return get_warehouse_sites(active_only, db)


@router.post("/requests", response_model=RequestWithLines, status_code=201)
def create_request_endpoint(
    request_data: RequestCreate,
    created_by: str = Query("user@example.com"),
    db: Session = Depends(get_db),
):
    return create_request(request_data, created_by, db)


@router.get("/requests", response_model=List[RequestWithLines])
def list_requests_endpoint(
    status: Optional[str] = Query(None),
    from_warehouse: Optional[str] = Query(None),
    to_warehouse: Optional[str] = Query(None),
    created_by: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    return list_requests(status, from_warehouse, to_warehouse, created_by, db)


@router.get("/requests/{request_id}", response_model=RequestWithLines)
def get_request_endpoint(
    request_id: int,
    db: Session = Depends(get_db),
):
    return get_request(request_id, db)


@router.put("/requests/{request_id}", response_model=RequestResponse)
def update_request_endpoint(
    request_id: int,
    update_data: RequestUpdate,
    db: Session = Depends(get_db),
):
    return update_request(request_id, update_data, db)


@router.delete("/requests/{request_id}", response_model=DeleteResponse)
def delete_request_endpoint(
    request_id: int,
    db: Session = Depends(get_db),
):
    return delete_request(request_id, db)


# ── Transfer endpoints (Phase B) ──


@router.post("/transfers", status_code=201)
def create_transfer_endpoint(
    transfer_data: TransferCreate,
    created_by: str = Query("user@example.com"),
    db: Session = Depends(get_db),
):
    return create_transfer(transfer_data, created_by, db)


@router.get("/transfers", response_model=TransferListResponse)
def list_transfers_endpoint(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    status: Optional[str] = Query(None),
    from_site: Optional[str] = Query(None),
    to_site: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    challan_no: Optional[str] = Query(None),
    sort_by: str = Query("created_ts"),
    sort_order: str = Query("desc"),
    db: Session = Depends(get_db),
):
    return list_transfers(
        page, per_page, status, from_site, to_site,
        from_date, to_date, challan_no, sort_by, sort_order, db,
    )


@router.get("/transfers/{transfer_id}", response_model=TransferWithLines)
def get_transfer_endpoint(
    transfer_id: int,
    db: Session = Depends(get_db),
):
    return get_transfer(transfer_id, db)


@router.delete("/transfers/{transfer_id}", response_model=TransferDeleteResponse)
def delete_transfer_endpoint(
    transfer_id: int,
    db: Session = Depends(get_db),
):
    return delete_transfer(transfer_id, db)


# ── Transfer IN endpoints (Phase C) ──


@router.post("/transfer-in", status_code=201)
def create_transfer_in_endpoint(
    transfer_in_data: TransferInCreate,
    db: Session = Depends(get_db),
):
    return create_transfer_in(transfer_in_data, db)


@router.get("/transfer-in", response_model=TransferInListResponse)
def list_transfer_ins_endpoint(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    receiving_warehouse: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    sort_by: str = Query("created_at"),
    sort_order: str = Query("desc"),
    db: Session = Depends(get_db),
):
    return list_transfer_ins(
        page, per_page, receiving_warehouse,
        from_date, to_date, sort_by, sort_order, db,
    )


@router.get("/transfer-in/{transfer_in_id}", response_model=TransferInDetail)
def get_transfer_in_endpoint(
    transfer_in_id: int,
    db: Session = Depends(get_db),
):
    return get_transfer_in(transfer_in_id, db)
