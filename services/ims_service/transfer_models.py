from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any

from pydantic import BaseModel, Field, field_validator


# ============================================
# BASE MODELS
# ============================================

class WarehouseMasterBase(BaseModel):
    warehouse_code: str = Field(..., description="Warehouse Code")
    warehouse_name: str = Field(..., description="Warehouse Name")
    address: str = Field(..., description="Warehouse Address")
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None
    gstin: Optional[str] = None
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_email: Optional[str] = None
    is_active: bool = Field(default=True)


class WarehouseMasterCreate(WarehouseMasterBase):
    pass


class WarehouseMasterResponse(WarehouseMasterBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ============================================
# TRANSFER REQUEST MODELS
# ============================================

class TransferRequestItemBase(BaseModel):
    line_number: int = Field(..., description="Line Number")
    material_type: Optional[str] = Field(None, description="Material Type: RM, PM, FG, SFG")
    item_category: str = Field(..., description="Item Category")
    sub_category: Optional[str] = None
    item_description: str = Field(..., description="Item Description")
    sku_id: Optional[str] = None
    quantity: Decimal = Field(..., description="Quantity")
    uom: str = Field(..., description="Unit of Measure")
    pack_size: Decimal = Field(default=0, description="Pack Size")
    package_size: Optional[str] = None
    net_weight: Decimal = Field(default=0, description="Net Weight")

    @field_validator('material_type')
    @classmethod
    def validate_material_type(cls, v):
        if v is not None and v not in ['RM', 'PM', 'FG', 'SFG']:
            raise ValueError('Material type must be RM, PM, FG, or SFG')
        return v.upper() if v else v


class TransferRequestItemCreate(TransferRequestItemBase):
    pass


class TransferRequestItemResponse(TransferRequestItemBase):
    id: int
    transfer_id: int
    created_at: datetime

    class Config:
        from_attributes = True


class TransferRequestBase(BaseModel):
    request_date: date = Field(..., description="Request Date")
    from_warehouse: str = Field(..., description="From Warehouse Code")
    to_warehouse: str = Field(..., description="To Warehouse Code")
    reason: Optional[str] = None
    reason_description: str = Field(..., description="Reason Description")
    created_by: Optional[str] = None


class TransferRequestCreate(TransferRequestBase):
    request_no: Optional[str] = Field(None, description="Request Number (optional, will be generated if not provided)")
    items: List[TransferRequestItemCreate] = Field(..., description="Transfer Request Items")


class TransferRequestResponse(TransferRequestBase):
    id: int
    request_no: str
    transfer_no: Optional[str] = None
    status: str = Field(default="Pending", description="Status: Pending, Approved, Rejected, In Transit, Completed")
    created_at: datetime
    updated_at: datetime
    items: List[TransferRequestItemResponse] = []

    class Config:
        from_attributes = True


# ============================================
# TRANSFER SCANNED BOXES MODELS
# ============================================

class TransferScannedBoxBase(BaseModel):
    box_id: int = Field(..., description="Box ID")
    transaction_no: str = Field(..., description="Transaction Number")
    sku_id: str = Field(..., description="SKU ID")
    box_number_in_array: int = Field(..., description="Box Number in Array")
    box_number: int = Field(..., description="Box Number")
    item_description: Optional[str] = None
    net_weight: Decimal = Field(default=0, description="Net Weight")
    gross_weight: Decimal = Field(default=0, description="Gross Weight")
    qr_data: Optional[Dict[str, Any]] = None


class TransferScannedBoxCreate(TransferScannedBoxBase):
    pass


class TransferScannedBoxResponse(TransferScannedBoxBase):
    id: int
    transfer_id: int
    scan_timestamp: datetime

    class Config:
        from_attributes = True


# ============================================
# TRANSFER INFO MODELS (Transport Details)
# ============================================

class TransferInfoBase(BaseModel):
    vehicle_number: str = Field(..., description="Vehicle Number")
    vehicle_number_other: Optional[str] = None
    driver_name: str = Field(..., description="Driver Name")
    driver_name_other: Optional[str] = None
    driver_phone: Optional[str] = None
    approval_authority: str = Field(..., description="Approval Authority")


class TransferInfoCreate(TransferInfoBase):
    pass


class TransferInfoResponse(TransferInfoBase):
    id: int
    transfer_id: int
    created_at: datetime

    class Config:
        from_attributes = True


# ============================================
# COMPLETE TRANSFER MODELS
# ============================================

class TransferCompleteCreate(BaseModel):
    """Complete transfer data for submission"""
    request_no: str = Field(..., description="Request Number")
    request_date: date = Field(..., description="Request Date")
    from_warehouse: str = Field(..., description="From Warehouse")
    to_warehouse: str = Field(..., description="To Warehouse")
    reason_description: str = Field(..., description="Reason Description")
    items: List[TransferRequestItemCreate] = Field(..., description="Transfer Items")
    scanned_boxes: List[TransferScannedBoxCreate] = Field(..., description="Scanned Boxes")
    transport_info: TransferInfoCreate = Field(..., description="Transport Information")


class TransferCompleteResponse(BaseModel):
    """Complete transfer response with all details"""
    id: int
    request_no: str
    transfer_no: str
    request_date: date
    from_warehouse: str
    to_warehouse: str
    reason: Optional[str] = None
    reason_description: str
    status: str
    created_by: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    items: List[TransferRequestItemResponse] = []
    scanned_boxes: List[TransferScannedBoxResponse] = []
    transport_info: Optional[TransferInfoResponse] = None

    class Config:
        from_attributes = True


# ============================================
# DC GENERATION MODELS
# ============================================

class WarehouseAddress(BaseModel):
    code: str
    name: str
    address: str
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None
    gstin: Optional[str] = None
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_email: Optional[str] = None


class DCItem(BaseModel):
    line_number: int
    material_type: Optional[str] = None
    item_category: str
    sub_category: Optional[str] = None
    item_description: str
    sku_id: Optional[str] = None
    quantity: Decimal
    uom: str
    pack_size: Decimal
    package_size: Optional[str] = None
    net_weight: Decimal


class DCScannedBox(BaseModel):
    box_id: int
    transaction_no: str
    sku_id: str
    box_number: int
    item_description: Optional[str] = None
    net_weight: Decimal
    gross_weight: Decimal


class DCTransportInfo(BaseModel):
    vehicle_number: str
    vehicle_number_other: Optional[str] = None
    driver_name: str
    driver_name_other: Optional[str] = None
    driver_phone: Optional[str] = None
    approval_authority: str


class DCDataResponse(BaseModel):
    """Delivery Challan data response"""
    transfer_no: str
    request_no: str
    request_date: date
    from_warehouse: WarehouseAddress
    to_warehouse: WarehouseAddress
    items: List[DCItem]
    scanned_boxes: List[DCScannedBox]
    transport_info: DCTransportInfo


# ============================================
# REQUEST LIST MODELS
# ============================================

class TransferRequestListItem(BaseModel):
    id: int
    request_no: str
    transfer_no: Optional[str] = None
    request_date: date
    from_warehouse: str
    to_warehouse: str
    reason_description: str
    status: str
    item_count: int = Field(..., description="Number of items in request")
    created_by: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class TransferRequestListResponse(BaseModel):
    success: bool = Field(..., description="Success status")
    message: str = Field(..., description="Response message")
    data: List[TransferRequestListItem] = Field(..., description="Transfer requests list")
    total: int = Field(..., description="Total count")
    page: int = Field(..., description="Current page")
    per_page: int = Field(..., description="Items per page")
    pages: int = Field(..., description="Total pages")


# ============================================
# SCANNER MODELS
# ============================================

class ScannerInput(BaseModel):
    scan_value: str = Field(..., description="Scanned value")
    warehouse: Optional[str] = None


class ScannerResponse(BaseModel):
    success: bool = Field(..., description="Success status")
    message: str = Field(..., description="Response message")
    data: Optional[Dict[str, Any]] = None


class BoxScanData(BaseModel):
    scan_value: str
    resolved_box: Optional[str] = None
    resolved_lot: Optional[str] = None
    resolved_batch: Optional[str] = None
    sku_id: Optional[str] = None
    sku_name: Optional[str] = None
    material_type: Optional[str] = None
    uom: Optional[str] = None
    available_qty: Optional[Decimal] = None
    expiry_date: Optional[date] = None
    fefo_priority: Optional[int] = None


# ============================================
# FILTER MODELS
# ============================================

class TransferRequestFilter(BaseModel):
    status: Optional[str] = Field(None, description="Filter by status")
    from_warehouse: Optional[str] = Field(None, description="Filter by from warehouse")
    to_warehouse: Optional[str] = Field(None, description="Filter by to warehouse")
    request_date_from: Optional[date] = Field(None, description="Filter from date")
    request_date_to: Optional[date] = Field(None, description="Filter to date")
    created_by: Optional[str] = Field(None, description="Filter by creator")
    page: int = Field(default=1, ge=1, description="Page number")
    per_page: int = Field(default=20, ge=1, le=100, description="Items per page")


# ============================================
# RESPONSE MODELS
# ============================================

class StandardResponse(BaseModel):
    success: bool = Field(..., description="Success status")
    message: str = Field(..., description="Response message")
    data: Optional[Dict[str, Any]] = None


class TransferRequestDetailResponse(BaseModel):
    success: bool = Field(..., description="Success status")
    message: str = Field(..., description="Response message")
    data: Optional[Dict[str, Any]] = None
