from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, List, Optional

from pydantic import BaseModel, Field

Decimal12_2 = Annotated[Decimal, Field(max_digits=12, decimal_places=2)]
Decimal12_3 = Annotated[Decimal, Field(max_digits=12, decimal_places=3)]
Decimal14_2 = Annotated[Decimal, Field(max_digits=14, decimal_places=2)]


# ── Request schemas ──────────────────────────


class ColdStorageCreate(BaseModel):
    inward_dt: Optional[date] = None
    unit: Optional[str] = None
    inward_no: Optional[str] = None
    item_mark: Optional[str] = None
    vakkal: Optional[str] = None
    lot_no: Optional[str] = None
    no_of_cartons: Optional[Decimal12_2] = None
    weight_kg: Optional[Decimal12_3] = None
    total_inventory_kgs: Optional[Decimal14_2] = None
    group_name: Optional[str] = None
    item_description: Optional[str] = None
    storage_location: Optional[str] = None
    exporter: Optional[str] = None
    last_purchase_rate: Optional[Decimal12_2] = None
    value: Optional[Decimal14_2] = None


class ColdStorageUpdate(BaseModel):
    inward_dt: Optional[date] = None
    unit: Optional[str] = None
    inward_no: Optional[str] = None
    item_mark: Optional[str] = None
    vakkal: Optional[str] = None
    lot_no: Optional[str] = None
    no_of_cartons: Optional[Decimal12_2] = None
    weight_kg: Optional[Decimal12_3] = None
    total_inventory_kgs: Optional[Decimal14_2] = None
    group_name: Optional[str] = None
    item_description: Optional[str] = None
    storage_location: Optional[str] = None
    exporter: Optional[str] = None
    last_purchase_rate: Optional[Decimal12_2] = None
    value: Optional[Decimal14_2] = None


class ColdStorageBulkCreate(BaseModel):
    records: List[ColdStorageCreate]


# ── Response schemas ─────────────────────────


class ColdStorageResponse(BaseModel):
    id: int
    inward_dt: Optional[date] = None
    unit: Optional[str] = None
    inward_no: Optional[str] = None
    item_mark: Optional[str] = None
    vakkal: Optional[str] = None
    lot_no: Optional[str] = None
    no_of_cartons: Optional[float] = None
    weight_kg: Optional[float] = None
    total_inventory_kgs: Optional[float] = None
    group_name: Optional[str] = None
    item_description: Optional[str] = None
    storage_location: Optional[str] = None
    exporter: Optional[str] = None
    last_purchase_rate: Optional[float] = None
    value: Optional[float] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ColdStorageListResponse(BaseModel):
    records: List[ColdStorageResponse] = []
    total: int = 0
    page: int = 1
    per_page: int = 20
    total_pages: int = 0


class ColdStorageDeleteResponse(BaseModel):
    success: bool
    message: str
    id: Optional[int] = None


class GroupSummaryItem(BaseModel):
    group_name: str
    total_records: int
    total_cartons: float
    total_inventory_kgs: float
    total_value: float


class ColdStorageSummaryResponse(BaseModel):
    summary: List[GroupSummaryItem] = []
    grand_total_records: int = 0
    grand_total_inventory_kgs: float = 0
    grand_total_value: float = 0


class BulkCreateResponse(BaseModel):
    status: str
    records_created: int


# ── Box schemas ─────────────────────────────


class ColdStorageBoxUpsertRequest(BaseModel):
    box_number: int = Field(..., ge=1)
    weight_kg: Optional[Decimal12_3] = None
    status: Optional[str] = None


class ColdStorageBoxResponse(BaseModel):
    id: int
    stock_id: int
    box_number: int
    box_id: Optional[str] = None
    item_description: Optional[str] = None
    lot_no: Optional[str] = None
    weight_kg: Optional[float] = None
    status: str = "available"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ColdStorageBoxListResponse(BaseModel):
    boxes: List[ColdStorageBoxResponse] = []
    total: int = 0


class ColdStorageBoxUpsertResponse(BaseModel):
    status: str
    box_id: str
    stock_id: int
    box_number: int


# ── Approval schemas ─────────────────────────


class ColdStorageApprovalRequest(BaseModel):
    approved_by: str


class ColdStorageApprovalResponse(BaseModel):
    status: str
    id: int
    approved_by: str
    approved_at: Optional[str] = None
