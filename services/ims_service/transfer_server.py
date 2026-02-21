from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from shared.database import get_db
from services.ims_service.transfer_models import (
    WarehouseMasterResponse,
    TransferRequestCreate,
    TransferRequestListResponse,
    TransferRequestDetailResponse,
    TransferCompleteCreate,
    DCDataResponse,
    ScannerInput,
    ScannerResponse,
    StandardResponse,
)
from services.ims_service.transfer_tools import (
    get_warehouses,
    create_transfer_request,
    get_transfer_requests,
    get_transfer_request_detail,
    submit_transfer,
    resolve_scanner_input,
    get_dc_data,
    get_status_options,
    get_material_types,
)

router = APIRouter(prefix="/transfer", tags=["Transfer Module"])


# ============================================
# WAREHOUSE MASTER ENDPOINTS
# ============================================


@router.get("/warehouses", response_model=List[WarehouseMasterResponse])
def get_warehouses_endpoint(
    is_active: bool = Query(True, description="Filter by active status"),
    db: Session = Depends(get_db),
):
    """Get all warehouses for dropdowns"""
    return get_warehouses(is_active, db)


# ============================================
# TRANSFER REQUEST ENDPOINTS
# ============================================


@router.post("/request", response_model=StandardResponse)
def create_transfer_request_endpoint(
    request_data: TransferRequestCreate,
    db: Session = Depends(get_db),
):
    """Create a new transfer request"""
    return create_transfer_request(request_data, db)


@router.get("/requests", response_model=TransferRequestListResponse)
def get_transfer_requests_endpoint(
    request_status: Optional[str] = Query(None, description="Filter by status", alias="status"),
    from_warehouse: Optional[str] = Query(None, description="Filter by from warehouse"),
    to_warehouse: Optional[str] = Query(None, description="Filter by to warehouse"),
    request_date_from: Optional[date] = Query(None, description="Filter from date"),
    request_date_to: Optional[date] = Query(None, description="Filter to date"),
    created_by: Optional[str] = Query(None, description="Filter by creator"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    db: Session = Depends(get_db),
):
    """Get transfer requests list with filtering and pagination"""
    return get_transfer_requests(
        request_status, from_warehouse, to_warehouse,
        request_date_from, request_date_to, created_by,
        page, per_page, db,
    )


@router.get("/requests/{request_id}", response_model=TransferRequestDetailResponse)
def get_transfer_request_detail_endpoint(
    request_id: int,
    db: Session = Depends(get_db),
):
    """Get transfer request details by ID (used in transfer form)"""
    return get_transfer_request_detail(request_id, db)


# ============================================
# TRANSFER FORM ENDPOINTS
# ============================================


@router.post("/submit", response_model=StandardResponse)
def submit_transfer_endpoint(
    transfer_data: TransferCompleteCreate,
    db: Session = Depends(get_db),
):
    """Submit complete transfer with scanned boxes and transport details"""
    return submit_transfer(transfer_data, db)


# ============================================
# SCANNER ENDPOINTS
# ============================================


@router.post("/scanner/resolve", response_model=ScannerResponse)
def resolve_scanner_input_endpoint(
    scanner_input: ScannerInput,
    db: Session = Depends(get_db),
):
    """Resolve scanned box/lot/batch information"""
    return resolve_scanner_input(scanner_input, db)


# ============================================
# DC GENERATION ENDPOINTS
# ============================================


@router.get("/{company}/{transfer_no}/dc-data", response_model=DCDataResponse)
def get_dc_data_endpoint(
    company: str,
    transfer_no: str,
    db: Session = Depends(get_db),
):
    """Get delivery challan data for DC generation"""
    return get_dc_data(company, transfer_no, db)


# ============================================
# UTILITY ENDPOINTS
# ============================================


@router.get("/status-options")
def get_status_options_endpoint():
    """Get available status options for transfer requests"""
    return get_status_options()


@router.get("/material-types")
def get_material_types_endpoint():
    """Get available material types"""
    return get_material_types()


# ============================================
# INTERUNIT COMPATIBILITY ENDPOINTS
# ============================================


@router.get("/interunit/requests/{request_id}", response_model=TransferRequestDetailResponse)
def get_interunit_request_endpoint(
    request_id: int,
    db: Session = Depends(get_db),
):
    """Get transfer request for interunit compatibility (used in transfer form)"""
    return get_transfer_request_detail(request_id, db)


@router.post("/interunit/{company}", response_model=StandardResponse)
def submit_interunit_transfer_endpoint(
    company: str,
    transfer_data: TransferCompleteCreate,
    db: Session = Depends(get_db),
):
    """Submit transfer for interunit compatibility"""
    return submit_transfer(transfer_data, db)
