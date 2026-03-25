from datetime import datetime
from decimal import Decimal
from typing import Annotated, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from services.ims_service.inward_models import Company

Decimal18_2 = Annotated[Decimal, Field(max_digits=18, decimal_places=2)]
Decimal18_3 = Annotated[Decimal, Field(max_digits=18, decimal_places=3)]


# ══════════════════════════════════════════════
#  Request / Input schemas
# ══════════════════════════════════════════════


class RTVHeaderCreate(BaseModel):
    factory_unit: str
    customer: str
    invoice_number: Optional[str] = None
    challan_no: Optional[str] = None
    dn_no: Optional[str] = None
    conversion: Optional[str] = "0"
    sales_poc: Optional[str] = None
    remark: Optional[str] = None


class RTVLineCreate(BaseModel):
    material_type: str
    item_category: str
    sub_category: str
    item_description: str
    uom: str
    qty: str = "0"
    rate: str = "0"
    value: str = "0"
    net_weight: Optional[str] = "0"
    carton_weight: Optional[str] = "0"

    @field_validator("material_type", "uom")
    @classmethod
    def uppercase_codes(cls, v: str) -> str:
        return v.upper() if v else v


class RTVCreate(BaseModel):
    company: Company
    header: RTVHeaderCreate
    lines: List[RTVLineCreate] = Field(..., min_length=1)


class RTVHeaderUpdate(BaseModel):
    factory_unit: Optional[str] = None
    customer: Optional[str] = None
    invoice_number: Optional[str] = None
    challan_no: Optional[str] = None
    dn_no: Optional[str] = None
    conversion: Optional[str] = None
    sales_poc: Optional[str] = None
    remark: Optional[str] = None
    status: Optional[str] = None


class RTVBoxUpsertRequest(BaseModel):
    article_description: str
    box_number: int = Field(..., ge=1)
    uom: Optional[str] = None
    conversion: Optional[str] = None
    net_weight: Optional[Decimal18_3] = None
    gross_weight: Optional[Decimal18_3] = None
    lot_number: Optional[str] = None
    count: Optional[int] = None


class RTVLinesUpdateRequest(BaseModel):
    lines: List["RTVLineCreate"] = Field(..., min_length=1)


# ── Approval request schemas ─────────────────


class RTVApprovalHeaderFields(BaseModel):
    factory_unit: Optional[str] = None
    customer: Optional[str] = None
    invoice_number: Optional[str] = None
    challan_no: Optional[str] = None
    dn_no: Optional[str] = None
    conversion: Optional[str] = None
    sales_poc: Optional[str] = None
    remark: Optional[str] = None


class RTVApprovalLineFields(BaseModel):
    item_description: str
    qty: Optional[str] = None
    rate: Optional[str] = None
    value: Optional[str] = None
    net_weight: Optional[str] = None
    carton_weight: Optional[str] = None
    uom: Optional[str] = None
    material_type: Optional[str] = None
    item_category: Optional[str] = None
    sub_category: Optional[str] = None


class RTVApprovalBoxFields(BaseModel):
    article_description: str
    box_number: int = Field(..., ge=1)
    uom: Optional[str] = None
    conversion: Optional[str] = None
    net_weight: Optional[Decimal18_3] = None
    gross_weight: Optional[Decimal18_3] = None
    count: Optional[int] = None


class RTVApprovalRequest(BaseModel):
    approved_by: str
    header: Optional[RTVApprovalHeaderFields] = None
    lines: Optional[List[RTVApprovalLineFields]] = None
    boxes: Optional[List[RTVApprovalBoxFields]] = None


# ── Box edit log schemas ─────────────────────


class RTVBoxEditLogEntry(BaseModel):
    field_name: str
    old_value: Optional[str] = None
    new_value: Optional[str] = None


class RTVBoxEditLogRequest(BaseModel):
    email_id: str
    box_id: str
    rtv_id: str
    changes: List[RTVBoxEditLogEntry]


# ══════════════════════════════════════════════
#  Response schemas
# ══════════════════════════════════════════════


class RTVLineResponse(BaseModel):
    id: int
    header_id: int
    material_type: str
    item_category: str
    sub_category: str
    item_description: str
    uom: str
    qty: str
    rate: str
    value: str
    net_weight: str
    carton_weight: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class RTVBoxResponse(BaseModel):
    id: int
    header_id: int
    rtv_line_id: Optional[int] = None
    box_number: int
    box_id: Optional[str] = None
    article_description: str
    uom: Optional[str] = None
    conversion: Optional[str] = None
    lot_number: Optional[str] = None
    net_weight: str
    gross_weight: str
    count: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class RTVBoxUpsertResponse(BaseModel):
    status: str
    box_id: str
    rtv_id: str
    article_description: str
    box_number: int


class RTVHeaderResponse(BaseModel):
    id: int
    rtv_id: str
    rtv_date: Optional[datetime] = None
    factory_unit: str
    customer: str
    invoice_number: Optional[str] = None
    challan_no: Optional[str] = None
    dn_no: Optional[str] = None
    conversion: Optional[str] = None
    sales_poc: Optional[str] = None
    remark: Optional[str] = None
    status: str
    created_by: Optional[str] = None
    created_ts: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class RTVWithDetails(RTVHeaderResponse):
    lines: List[RTVLineResponse] = []
    boxes: List[RTVBoxResponse] = []


class RTVListItem(RTVHeaderResponse):
    items_count: int = 0
    boxes_count: int = 0
    total_qty: int = 0


class RTVListResponse(BaseModel):
    records: List[RTVListItem] = []
    total: int = 0
    page: int = 1
    per_page: int = 10
    total_pages: int = 0


class RTVDeleteResponse(BaseModel):
    success: bool
    message: str
    rtv_id: Optional[str] = None


class RTVLinesUpdateResponse(BaseModel):
    status: str
    rtv_id: str
    lines_count: int


class RTVApprovalResponse(BaseModel):
    status: str
    rtv_id: str
    company: str
    approved_by: str
    approved_at: str
