from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from shared.database import get_db
from services.ims_service.inward_models import Company
from services.ims_service.inward_tools import get_box_by_number, get_box_by_box_id
from services.ims_service.interunit_models import (
    RequestCreate,
    RequestUpdate,
    RequestWithLines,
    RequestResponse,
    RequestListResponse,
    WarehouseSiteResponse,
    DeleteResponse,
    TransferCreate,
    TransferWithLines,
    TransferListResponse,
    TransferDeleteResponse,
    TransferInCreate,
    TransferInDetail,
    TransferInListResponse,
    PendingTransferInCreate,
    PendingBoxAcknowledge,
    FinalizeTransferIn,
    CategorialSearchResponse,
    CategorialDropdownResponse,
)
from services.ims_service.interunit_tools import (
    get_warehouse_sites,
    create_request,
    list_requests,
    get_request,
    update_request,
    delete_request,
    create_transfer,
    update_transfer,
    list_transfers,
    get_transfer,
    delete_transfer,
    create_transfer_in,
    list_transfer_ins,
    get_transfer_in,
    create_pending_transfer_in,
    acknowledge_pending_box,
    unacknowledge_pending_box,
    acknowledge_pending_boxes_batch,
    finalize_transfer_in,
    get_pending_by_transfer_out,
    categorial_global_search,
    categorial_dropdown,
    get_bulk_entry_box,
)

router = APIRouter(prefix="/interunit", tags=["interunit"])

AUTHORIZED_DELETE_EMAILS = {"yash@candorfoods.in"}


def _check_delete_permission(user_email: str):
    if user_email.lower() not in AUTHORIZED_DELETE_EMAILS:
        raise HTTPException(403, "You are not authorized to delete records")


@router.get("/dropdowns/warehouse-sites", response_model=List[WarehouseSiteResponse])
def get_warehouse_sites_endpoint(
    active_only: bool = Query(True),
    db: Session = Depends(get_db),
):
    return get_warehouse_sites(active_only, db)


@router.get("/box-lookup/{company}")
def box_lookup_endpoint(
    company: Company,
    box_number: int = Query(..., description="Box number"),
    transaction_no: str = Query(..., description="Transaction number"),
    db: Session = Depends(get_db),
):
    """Look up a single box by box_number and transaction_no from company boxes table."""
    return get_box_by_number(company, box_number, transaction_no, db)


@router.get("/box-lookup-by-id/{company}")
def box_lookup_by_id_endpoint(
    company: Company,
    box_id: str = Query(..., description="Box ID (e.g. 34888867-2)"),
    transaction_no: str = Query(..., description="Transaction number (e.g. TR-20260224173440)"),
    db: Session = Depends(get_db),
):
    """Look up a single box by box_id and transaction_no from company boxes table."""
    return get_box_by_box_id(company, box_id, transaction_no, db)


@router.get("/bulk-entry-box-lookup/{company}")
def bulk_entry_box_lookup_endpoint(
    company: Company,
    box_id: str = Query(..., description="Box ID from QR code"),
    transaction_no: str = Query(..., description="Transaction number starting with BE-"),
    db: Session = Depends(get_db),
):
    """Look up a single box from bulk entry boxes table (for BE- prefix transactions)."""
    return get_bulk_entry_box(company, box_id, transaction_no, db)


@router.post("/requests", response_model=RequestWithLines, status_code=201)
def create_request_endpoint(
    request_data: RequestCreate,
    created_by: str = Query("user@example.com"),
    db: Session = Depends(get_db),
):
    return create_request(request_data, created_by, db)


@router.get("/requests", response_model=RequestListResponse)
def list_requests_endpoint(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    status: Optional[str] = Query(None),
    from_warehouse: Optional[str] = Query(None),
    to_warehouse: Optional[str] = Query(None),
    created_by: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    return list_requests(page, per_page, status, from_warehouse, to_warehouse, created_by, db)


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
    user_email: str = Query(..., description="Email of user performing delete"),
    db: Session = Depends(get_db),
):
    _check_delete_permission(user_email)
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


@router.put("/transfers/{transfer_id}")
def update_transfer_endpoint(
    transfer_id: int,
    transfer_data: TransferCreate,
    db: Session = Depends(get_db),
):
    return update_transfer(transfer_id, transfer_data, db)


@router.delete("/transfers/{transfer_id}", response_model=TransferDeleteResponse)
def delete_transfer_endpoint(
    transfer_id: int,
    user_email: str = Query(..., description="Email of user performing delete"),
    db: Session = Depends(get_db),
):
    _check_delete_permission(user_email)
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


# ── Pending Transfer IN (real-time acknowledge) ──


@router.post("/transfer-in/pending", status_code=201)
def create_pending_transfer_in_endpoint(
    data: PendingTransferInCreate,
    db: Session = Depends(get_db),
):
    return create_pending_transfer_in(data, db)


@router.get("/transfer-in/pending/by-transfer-out/{transfer_out_id}")
def get_pending_by_transfer_out_endpoint(
    transfer_out_id: int,
    db: Session = Depends(get_db),
):
    return get_pending_by_transfer_out(transfer_out_id, db)


@router.post("/transfer-in/{header_id}/acknowledge")
def acknowledge_box_endpoint(
    header_id: int,
    data: PendingBoxAcknowledge,
    db: Session = Depends(get_db),
):
    return acknowledge_pending_box(header_id, data, db)


@router.post("/transfer-in/{header_id}/acknowledge-batch")
def acknowledge_batch_endpoint(
    header_id: int,
    boxes: List[PendingBoxAcknowledge],
    db: Session = Depends(get_db),
):
    return acknowledge_pending_boxes_batch(header_id, boxes, db)


@router.delete("/transfer-in/{header_id}/acknowledge/{box_id:path}")
def unacknowledge_box_endpoint(
    header_id: int,
    box_id: str,
    db: Session = Depends(get_db),
):
    return unacknowledge_pending_box(header_id, box_id, db)


@router.post("/transfer-in/{header_id}/finalize")
def finalize_transfer_in_endpoint(
    header_id: int,
    data: FinalizeTransferIn,
    db: Session = Depends(get_db),
):
    return finalize_transfer_in(header_id, data, db)


# ── Transfer IN detail/delete (must come after /pending and /{id}/acknowledge routes) ──


@router.get("/transfer-in/{transfer_in_id}", response_model=TransferInDetail)
def get_transfer_in_endpoint(
    transfer_in_id: int,
    db: Session = Depends(get_db),
):
    return get_transfer_in(transfer_in_id, db)


@router.delete("/transfer-in/{transfer_in_id}")
def delete_transfer_in_endpoint(
    transfer_in_id: int,
    user_email: str = Query(..., description="Email of the user performing the delete"),
    db: Session = Depends(get_db),
):
    from services.ims_service.interunit_tools import delete_transfer_in
    return delete_transfer_in(transfer_in_id, user_email, db)


# ── All SKU lookup endpoints ──


@router.get("/categorial-search", response_model=CategorialSearchResponse)
def categorial_search_endpoint(
    search: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """Global search on all_sku.particulars for transfer/request article lookup."""
    return categorial_global_search(search, limit, offset, db)


@router.get("/categorial-dropdown", response_model=CategorialDropdownResponse)
def categorial_dropdown_endpoint(
    material_type: Optional[str] = Query(None),
    item_category: Optional[str] = Query(None),
    sub_category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """Cascading dropdown on all_sku for transfer/request article lookup."""
    return categorial_dropdown(
        material_type, item_category, sub_category,
        search, limit, offset, db,
    )
