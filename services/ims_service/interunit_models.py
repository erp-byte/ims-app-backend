from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ── Request input schemas ──


class FormDataBase(BaseModel):
    request_date: str = Field(..., description="DD-MM-YYYY")
    from_warehouse: str = Field(..., description="Source warehouse site code")
    to_warehouse: str = Field(..., description="Destination warehouse site code")
    reason_description: str = Field(..., description="Reason for the transfer")

    @field_validator("reason_description")
    @classmethod
    def uppercase_reason(cls, v: str) -> str:
        return v.upper() if v else v

    @model_validator(mode="after")
    def warehouses_must_differ(self):
        if self.from_warehouse and self.to_warehouse and self.from_warehouse == self.to_warehouse:
            raise ValueError("From warehouse and to warehouse must be different")
        return self


class ArticleDataCreate(BaseModel):
    material_type: str
    item_category: str
    sub_category: str
    item_description: str
    quantity: str = "0"
    uom: str
    pack_size: str = "0.00"
    package_size: Optional[str] = "0"
    batch_number: Optional[str] = None
    lot_number: Optional[str] = None

    @field_validator("material_type", "uom")
    @classmethod
    def uppercase_codes(cls, v: str) -> str:
        return v.upper() if v else v

    @field_validator("item_category", "sub_category", "item_description")
    @classmethod
    def uppercase_text(cls, v: str) -> str:
        return v.upper() if v else v

    @field_validator("package_size")
    @classmethod
    def validate_package_size_for_fg(cls, v: Optional[str], info) -> Optional[str]:
        mt = info.data.get("material_type", "")
        if mt == "FG" and (not v or v == "0"):
            raise ValueError("Package size is required when material type is FG")
        return v


class ComputedFields(BaseModel):
    request_no: Optional[str] = None


class ValidationRules(BaseModel):
    from_warehouse_required: bool = True
    from_warehouse_not_equal_to_warehouse: bool = True
    to_warehouse_required: bool = True
    to_warehouse_not_equal_from_warehouse: bool = True
    material_type_required: bool = True
    material_type_enum: List[str] = ["RM", "PM", "FG", "RTV"]
    package_size_required: bool = True
    package_size_conditional: str = "Only when materialType === 'FG'"


class RequestCreate(BaseModel):
    form_data: FormDataBase
    article_data: List[ArticleDataCreate] = Field(..., min_length=1)
    computed_fields: Optional[ComputedFields] = None
    validation_rules: Optional[ValidationRules] = None


class RequestUpdate(BaseModel):
    status: Optional[str] = None
    reject_reason: Optional[str] = None
    rejected_ts: Optional[datetime] = None

    @field_validator("reject_reason")
    @classmethod
    def uppercase_reject(cls, v: Optional[str]) -> Optional[str]:
        return v.upper() if v else v


# ── Response schemas ──


class RequestLineResponse(BaseModel):
    id: int
    request_id: int
    material_type: str
    item_category: str
    sub_category: str
    item_description: str
    quantity: str
    uom: str
    pack_size: str
    package_size: Optional[str] = None
    net_weight: str
    batch_number: Optional[str] = None
    lot_number: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class RequestResponse(BaseModel):
    id: int
    request_no: str
    request_date: str
    from_warehouse: str
    to_warehouse: str
    reason_description: str
    status: str
    reject_reason: Optional[str] = None
    created_by: Optional[str] = None
    created_ts: Optional[datetime] = None
    rejected_ts: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class RequestWithLines(RequestResponse):
    lines: List[RequestLineResponse] = []


class WarehouseSiteResponse(BaseModel):
    id: int
    site_code: str
    site_name: str
    is_active: bool


class DeleteResponse(BaseModel):
    success: bool
    message: str


# ── Transfer (Phase B) input schemas ──


class TransferHeaderCreate(BaseModel):
    challan_no: Optional[str] = None
    stock_trf_date: str = Field(..., description="DD-MM-YYYY")
    from_warehouse: str
    to_warehouse: str
    vehicle_no: str
    driver_name: Optional[str] = None
    approved_by: Optional[str] = None
    remark: Optional[str] = None
    reason_code: Optional[str] = None

    @model_validator(mode="after")
    def warehouses_must_differ(self):
        if self.from_warehouse and self.to_warehouse and self.from_warehouse == self.to_warehouse:
            raise ValueError("From warehouse and to warehouse must be different")
        return self


class TransferLineCreate(BaseModel):
    material_type: str
    item_category: str
    sub_category: str
    item_description: str
    quantity: str = "0"
    uom: str
    pack_size: str = "0.00"
    package_size: Optional[str] = "0"
    batch_number: Optional[str] = None
    lot_number: Optional[str] = None

    @field_validator("material_type", "uom")
    @classmethod
    def uppercase_codes(cls, v: str) -> str:
        return v.upper() if v else v

    @field_validator("item_category", "sub_category", "item_description")
    @classmethod
    def uppercase_text(cls, v: str) -> str:
        return v.upper() if v else v


class BoxCreate(BaseModel):
    box_number: int
    article: str
    lot_number: Optional[str] = None
    batch_number: Optional[str] = None
    transaction_no: Optional[str] = None
    net_weight: str = "0.00"
    gross_weight: str = "0.00"


class TransferCreate(BaseModel):
    header: TransferHeaderCreate
    lines: List[TransferLineCreate] = Field(..., min_length=1)
    boxes: Optional[List[BoxCreate]] = None
    request_id: Optional[int] = None


# ── Transfer (Phase B) response schemas ──


class TransferHeaderResponse(BaseModel):
    id: int
    challan_no: str
    stock_trf_date: str
    from_warehouse: str
    to_warehouse: str
    vehicle_no: str
    driver_name: Optional[str] = None
    approved_by: Optional[str] = None
    remark: Optional[str] = None
    reason_code: Optional[str] = None
    status: str
    request_id: Optional[int] = None
    request_no: Optional[str] = None
    created_by: Optional[str] = None
    created_ts: Optional[datetime] = None
    approved_ts: Optional[datetime] = None
    has_variance: bool = False


class TransferLineResponse(BaseModel):
    id: int
    header_id: int
    material_type: str
    item_category: str
    sub_category: str
    item_description: str
    quantity: str
    uom: str
    pack_size: str
    package_size: Optional[str] = None
    net_weight: str
    total_weight: str
    batch_number: Optional[str] = None
    lot_number: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class BoxResponse(BaseModel):
    id: int
    header_id: int
    transfer_line_id: Optional[int] = None
    box_number: int
    article: str
    lot_number: Optional[str] = None
    batch_number: Optional[str] = None
    transaction_no: Optional[str] = None
    net_weight: str
    gross_weight: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class TransferWithLines(TransferHeaderResponse):
    lines: List[TransferLineResponse] = []
    boxes: List[BoxResponse] = []


class TransferListItem(TransferHeaderResponse):
    items_count: int = 0
    boxes_count: int = 0
    pending_items: int = 0


class TransferListResponse(BaseModel):
    records: List[TransferListItem] = []
    total: int = 0
    page: int = 1
    per_page: int = 10
    total_pages: int = 0


class TransferDeleteResponse(BaseModel):
    success: bool
    message: str
    transfer_id: Optional[int] = None
    challan_no: Optional[str] = None


# ── Transfer IN (Phase C) input schemas ──


class TransferInBoxCreate(BaseModel):
    box_number: str
    transfer_out_box_id: Optional[int] = None
    article: Optional[str] = None
    batch_number: Optional[str] = None
    lot_number: Optional[str] = None
    transaction_no: Optional[str] = None
    net_weight: Optional[float] = None
    gross_weight: Optional[float] = None
    is_matched: bool = True


class TransferInCreate(BaseModel):
    transfer_out_id: int
    grn_number: str
    receiving_warehouse: str
    received_by: str
    box_condition: Optional[str] = "Good"
    condition_remarks: Optional[str] = None
    scanned_boxes: List[TransferInBoxCreate] = Field(..., min_length=1)

    @field_validator("receiving_warehouse", "received_by")
    @classmethod
    def uppercase_fields(cls, v: str) -> str:
        return v.upper() if v else v


# ── Transfer IN (Phase C) response schemas ──


class TransferInBoxResponse(BaseModel):
    id: int
    header_id: int
    box_number: str
    transfer_out_box_id: Optional[int] = None
    article: Optional[str] = None
    batch_number: Optional[str] = None
    lot_number: Optional[str] = None
    transaction_no: Optional[str] = None
    net_weight: Optional[float] = None
    gross_weight: Optional[float] = None
    scanned_at: Optional[datetime] = None
    is_matched: bool = True


class TransferInHeaderResponse(BaseModel):
    id: int
    transfer_out_id: int
    transfer_out_no: str
    grn_number: str
    grn_date: Optional[datetime] = None
    receiving_warehouse: str
    received_by: str
    received_at: Optional[datetime] = None
    box_condition: Optional[str] = None
    condition_remarks: Optional[str] = None
    status: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class TransferInDetail(TransferInHeaderResponse):
    boxes: List[TransferInBoxResponse] = []
    total_boxes_scanned: int = 0


class TransferInListItem(TransferInHeaderResponse):
    total_boxes_scanned: int = 0


class TransferInListResponse(BaseModel):
    records: List[TransferInListItem] = []
    total: int = 0
    page: int = 1
    per_page: int = 10
    total_pages: int = 0
