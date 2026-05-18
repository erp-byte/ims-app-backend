from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any, List, Literal, Optional

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


# ── Direct Out schemas ───────────────────────


class DirectOutLine(BaseModel):
    stock_id: int
    item_description: Optional[str] = None
    lot_no: Optional[str] = None
    inward_no: Optional[str] = None
    item_mark: Optional[str] = None
    issue_qty: float
    uom: Optional[str] = None
    unit: Optional[str] = None  # from cold_stocks.unit (e.g. D-39, D-514)
    warehouse: Optional[str] = None
    box_id: Optional[str] = None
    transaction_no: Optional[str] = None
    weight_kg_per_box: Optional[float] = None


class DirectOutCreate(BaseModel):
    transaction_type: Literal["DIRECT_OUT"] = "DIRECT_OUT"
    company: Literal["CFPL", "CDPL"]
    entry_date: str
    authority_person: str
    to_customer: str
    warehouse: Optional[str] = None  # made optional — per-line warehouse comes from each stock row
    vehicle_no: Optional[str] = None
    invoice_no: Optional[str] = None
    remarks: Optional[str] = None
    lines: List[DirectOutLine]
    created_by: Optional[str] = None


class DirectOutRecord(BaseModel):
    id: int
    transaction_no: str
    transaction_type: str
    company: Optional[str] = None
    entry_date: Optional[date] = None
    authority_person: Optional[str] = None
    to_customer: Optional[str] = None
    warehouse: Optional[str] = None
    vehicle_no: Optional[str] = None
    invoice_no: Optional[str] = None
    remarks: Optional[str] = None
    lines: List[Any] = []
    removed_stock_snapshot: List[Any] = []
    line_count: Optional[int] = None
    total_issue_qty: Optional[float] = None
    status: Optional[str] = None
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class DirectOutUpdate(BaseModel):
    """Header-only patch. Article entries are NOT editable here."""
    entry_date: Optional[str] = None
    authority_person: Optional[str] = None
    to_customer: Optional[str] = None
    warehouse: Optional[str] = None
    vehicle_no: Optional[str] = None
    invoice_no: Optional[str] = None
    remarks: Optional[str] = None


class DirectOutListResponse(BaseModel):
    records: List[DirectOutRecord] = []
    total: int = 0
    page: int = 1
    per_page: int = 20
