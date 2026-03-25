from decimal import Decimal
from typing import Annotated, List, Literal, Optional

from pydantic import BaseModel, Field

Company = Literal["CFPL", "CDPL"]

Decimal18_2 = Annotated[Decimal, Field(max_digits=18, decimal_places=2)]
Decimal18_3 = Annotated[Decimal, Field(max_digits=18, decimal_places=3)]
PositiveInt = Annotated[int, Field(strict=True, ge=1)]
NonNegativeInt = Annotated[int, Field(strict=True, ge=0)]


# ── Request schemas ──────────────────────────


class TransactionIn(BaseModel):
    transaction_no: str
    entry_date: str
    vehicle_number: Optional[str] = None
    transporter_name: Optional[str] = None
    lr_number: Optional[str] = None
    vendor_supplier_name: Optional[str] = None
    customer_party_name: Optional[str] = None
    source_location: Optional[str] = None
    destination_location: Optional[str] = None
    challan_number: Optional[str] = None
    invoice_number: Optional[str] = None
    po_number: Optional[str] = None
    grn_number: Optional[str] = None
    grn_quantity: Optional[Decimal18_3] = None
    system_grn_date: Optional[str] = None
    purchased_by: Optional[str] = None
    service_invoice_number: Optional[str] = None
    dn_number: Optional[str] = None
    approval_authority: Optional[str] = None
    total_amount: Optional[Decimal18_2] = None
    tax_amount: Optional[Decimal18_2] = None
    discount_amount: Optional[Decimal18_2] = None
    po_quantity: Optional[Decimal18_3] = None
    remark: Optional[str] = None
    currency: Optional[str] = "INR"
    warehouse: Optional[str] = None


class ArticleIn(BaseModel):
    transaction_no: str
    sku_id: Optional[int] = None
    item_description: str
    item_category: Optional[str] = None
    sub_category: Optional[str] = None
    material_type: Optional[str] = None
    quality_grade: Optional[str] = None
    uom: Optional[str] = None
    po_quantity: Optional[Decimal18_3] = None
    units: Optional[str] = None
    quantity_units: Optional[Decimal18_3] = None
    net_weight: Optional[Decimal18_3] = None
    total_weight: Optional[Decimal18_3] = None
    po_weight: Optional[Decimal18_3] = None
    lot_number: Optional[str] = None
    manufacturing_date: Optional[str] = None
    expiry_date: Optional[str] = None
    unit_rate: Optional[Decimal18_2] = None
    total_amount: Optional[Decimal18_2] = None
    carton_weight: Optional[Decimal18_3] = None
    box_count: PositiveInt
    box_net_weight: Optional[Decimal18_3] = None
    box_gross_weight: Optional[Decimal18_3] = None


class BulkEntryPayload(BaseModel):
    company: Company
    transaction: TransactionIn
    articles: List[ArticleIn]


class BulkEntryUpdate(BaseModel):
    vehicle_number: Optional[str] = None
    transporter_name: Optional[str] = None
    lr_number: Optional[str] = None
    vendor_supplier_name: Optional[str] = None
    customer_party_name: Optional[str] = None
    source_location: Optional[str] = None
    destination_location: Optional[str] = None
    challan_number: Optional[str] = None
    invoice_number: Optional[str] = None
    po_number: Optional[str] = None
    grn_number: Optional[str] = None
    grn_quantity: Optional[Decimal18_3] = None
    system_grn_date: Optional[str] = None
    purchased_by: Optional[str] = None
    service_invoice_number: Optional[str] = None
    dn_number: Optional[str] = None
    approval_authority: Optional[str] = None
    total_amount: Optional[Decimal18_2] = None
    tax_amount: Optional[Decimal18_2] = None
    discount_amount: Optional[Decimal18_2] = None
    po_quantity: Optional[Decimal18_3] = None
    remark: Optional[str] = None
    currency: Optional[str] = None
    warehouse: Optional[str] = None


class ArticleUpdateIn(BaseModel):
    """Article fields for update — matched by item_description."""
    transaction_no: str
    item_description: str
    sku_id: Optional[int] = None
    item_category: Optional[str] = None
    sub_category: Optional[str] = None
    material_type: Optional[str] = None
    quality_grade: Optional[str] = None
    uom: Optional[str] = None
    po_quantity: Optional[Decimal18_3] = None
    units: Optional[str] = None
    quantity_units: Optional[Decimal18_3] = None
    net_weight: Optional[Decimal18_3] = None
    total_weight: Optional[Decimal18_3] = None
    po_weight: Optional[Decimal18_3] = None
    lot_number: Optional[str] = None
    manufacturing_date: Optional[str] = None
    expiry_date: Optional[str] = None
    unit_rate: Optional[Decimal18_2] = None
    total_amount: Optional[Decimal18_2] = None
    carton_weight: Optional[Decimal18_3] = None
    box_count: Optional[PositiveInt] = None
    box_net_weight: Optional[Decimal18_3] = None
    box_gross_weight: Optional[Decimal18_3] = None


class BoxUpdateIn(BaseModel):
    """Box fields for update — matched by (article_description, box_number)."""
    article_description: str
    box_number: PositiveInt
    net_weight: Optional[Decimal18_3] = None
    gross_weight: Optional[Decimal18_3] = None
    lot_number: Optional[str] = None
    status: Optional[str] = None


class BulkEntryFullUpdate(BaseModel):
    """Full update payload — only provided fields are changed, nothing is deleted."""
    transaction: Optional[BulkEntryUpdate] = None
    articles: Optional[List[ArticleUpdateIn]] = None
    boxes: Optional[List[BoxUpdateIn]] = None


class BoxUpsertRequest(BaseModel):
    article_description: str
    box_number: PositiveInt
    net_weight: Optional[Decimal18_3] = None
    gross_weight: Optional[Decimal18_3] = None
    lot_number: Optional[str] = None
    status: Optional[str] = None


# ── Response schemas ─────────────────────────


class GeneratedBoxInfo(BaseModel):
    box_number: int
    box_id: str
    article_description: str
    net_weight: Optional[float] = None
    gross_weight: Optional[float] = None
    lot_number: Optional[str] = None


class ArticleBoxGroup(BaseModel):
    article_description: str
    box_ids: List[str]
    boxes: List[GeneratedBoxInfo]


class BulkEntryResponse(BaseModel):
    status: str
    transaction_no: str
    company: str
    articles_count: int
    total_boxes_created: int
    articles_with_boxes: List[ArticleBoxGroup]


class TransactionResponse(BaseModel):
    transaction_no: str
    entry_date: Optional[str] = None
    vehicle_number: Optional[str] = None
    transporter_name: Optional[str] = None
    lr_number: Optional[str] = None
    vendor_supplier_name: Optional[str] = None
    customer_party_name: Optional[str] = None
    source_location: Optional[str] = None
    destination_location: Optional[str] = None
    challan_number: Optional[str] = None
    invoice_number: Optional[str] = None
    po_number: Optional[str] = None
    grn_number: Optional[str] = None
    grn_quantity: Optional[float] = None
    system_grn_date: Optional[str] = None
    purchased_by: Optional[str] = None
    service_invoice_number: Optional[str] = None
    dn_number: Optional[str] = None
    approval_authority: Optional[str] = None
    total_amount: Optional[float] = None
    tax_amount: Optional[float] = None
    discount_amount: Optional[float] = None
    po_quantity: Optional[float] = None
    remark: Optional[str] = None
    currency: Optional[str] = None
    warehouse: Optional[str] = None
    status: Optional[str] = None
    approved_by: Optional[str] = None
    approved_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ArticleResponse(BaseModel):
    id: int
    transaction_no: str
    sku_id: Optional[int] = None
    item_description: Optional[str] = None
    item_category: Optional[str] = None
    sub_category: Optional[str] = None
    material_type: Optional[str] = None
    quality_grade: Optional[str] = None
    uom: Optional[str] = None
    units: Optional[str] = None
    po_quantity: Optional[float] = None
    quantity_units: Optional[float] = None
    net_weight: Optional[float] = None
    total_weight: Optional[float] = None
    po_weight: Optional[float] = None
    lot_number: Optional[str] = None
    manufacturing_date: Optional[str] = None
    expiry_date: Optional[str] = None
    unit_rate: Optional[float] = None
    total_amount: Optional[float] = None
    carton_weight: Optional[float] = None
    box_count: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class BoxResponse(BaseModel):
    id: int
    transaction_no: str
    article_description: Optional[str] = None
    box_number: int
    box_id: Optional[str] = None
    net_weight: Optional[float] = None
    gross_weight: Optional[float] = None
    lot_number: Optional[str] = None
    count: Optional[int] = None
    status: str = "available"
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class BulkEntryDetailResponse(BaseModel):
    transaction: TransactionResponse
    articles: List[ArticleResponse] = []
    boxes: List[BoxResponse] = []


class BulkEntryListResponse(BaseModel):
    records: List[TransactionResponse] = []
    total: int = 0
    page: int = 1
    per_page: int = 20
    total_pages: int = 0


class BulkEntryDeleteResponse(BaseModel):
    success: bool
    message: str
    transaction_no: Optional[str] = None


class BoxUpsertResponse(BaseModel):
    status: str
    box_id: str
    transaction_no: str
    article_description: str
    box_number: int


class BoxListResponse(BaseModel):
    boxes: List[BoxResponse] = []
    total: int = 0
