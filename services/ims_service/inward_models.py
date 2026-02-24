from decimal import Decimal
from typing import Annotated, List, Literal, Optional

from pydantic import BaseModel, Field

Company = Literal["CFPL", "CDPL"]

# Reusable constrained types (replaces condecimal/conint for Pylance compat)
Decimal18_2 = Annotated[Decimal, Field(max_digits=18, decimal_places=2)]
Decimal18_3 = Annotated[Decimal, Field(max_digits=18, decimal_places=3)]
PositiveInt = Annotated[int, Field(strict=True, ge=1)]
NonNegativeInt = Annotated[int, Field(strict=True, ge=0)]


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


class BoxIn(BaseModel):
    transaction_no: str
    article_description: str
    box_number: PositiveInt
    net_weight: Optional[Decimal18_3] = None
    gross_weight: Optional[Decimal18_3] = None
    lot_number: Optional[str] = None
    count: Optional[NonNegativeInt] = None


class InwardPayloadFlexible(BaseModel):
    """Flexible payload to handle both frontend and backend formats."""

    company: Company
    transaction: TransactionIn

    # Legacy format
    articles: Optional[List[ArticleIn]] = None
    boxes: Optional[List[BoxIn]] = None

    # Frontend format
    article_details: Optional[dict] = None
    ledger_details: Optional[dict] = None

    def model_post_init(self, __context) -> None:
        if self.article_details is not None and self.ledger_details is not None:
            if self.articles is None and self.boxes is None:
                self.articles, self.boxes = self._create_articles_and_boxes()

        if not self.articles or not self.boxes:
            raise ValueError("articles and boxes are required")

    def _create_articles_and_boxes(self) -> tuple[List[ArticleIn], List[BoxIn]]:
        if not (self.article_details and self.ledger_details):
            raise ValueError("article_details and ledger_details are required")

        sku_id = self.article_details.get("sku_id")

        item_description = self.article_details.get("item_description")
        if not item_description:
            raise ValueError("item_description is required and must be provided by the user")

        article = ArticleIn(
            transaction_no=self.transaction.transaction_no,
            sku_id=sku_id,
            item_description=item_description,
            item_category=self.article_details.get("item_category"),
            sub_category=self.article_details.get("sub_group_cd"),
            material_type=self.article_details.get("material_type"),
            quantity_units=self.ledger_details.get("received_quantity"),
            net_weight=self.ledger_details.get("net_weight"),
            total_weight=self.ledger_details.get("gross_weight"),
            lot_number=self.ledger_details.get("lot_number"),
            manufacturing_date=self.ledger_details.get("manufacturing_date"),
            expiry_date=self.ledger_details.get("expiry_date"),
            unit_rate=self.ledger_details.get("supplier_rate") or self.ledger_details.get("inward_rate"),
            total_amount=0.0,
        )

        box = BoxIn(
            transaction_no=self.transaction.transaction_no,
            article_description=item_description,
            box_number=1,
            net_weight=self.ledger_details.get("net_weight"),
            gross_weight=self.ledger_details.get("gross_weight"),
            lot_number=self.ledger_details.get("lot_number"),
            count=self.ledger_details.get("count"),
        )

        return [article], [box]


class InwardListItem(BaseModel):
    transaction_no: str
    entry_date: str
    status: Optional[str] = "pending"
    invoice_number: Optional[str] = None
    po_number: Optional[str] = None
    vendor_supplier_name: Optional[str] = None
    customer_party_name: Optional[str] = None
    total_amount: Optional[float] = None
    item_descriptions: List[str]
    quantities_and_uoms: List[str]
    has_edits: bool = False


class InwardListResponse(BaseModel):
    records: List[InwardListItem]
    total: int
    page: int
    per_page: int


# ---------- PO extraction models ----------


class POArticleExtracted(BaseModel):
    item_description: str
    po_weight: Optional[float] = None
    unit_rate: Optional[float] = None
    total_amount: Optional[float] = None


class POExtractResponse(BaseModel):
    supplier_name: Optional[str] = None
    source_location: Optional[str] = None
    customer_name: Optional[str] = None
    destination_location: Optional[str] = None
    po_number: Optional[str] = None
    purchased_by: Optional[str] = None
    total_amount: Optional[float] = None
    tax_amount: Optional[float] = None
    discount_amount: Optional[float] = None
    po_quantity: Optional[float] = None
    currency: Optional[str] = None
    articles: List[POArticleExtracted] = []


class MultiPOExtractResponse(BaseModel):
    purchase_orders: List[POExtractResponse]


class POUploadResponse(BaseModel):
    """Returned by POST /inward/extract-po/upload"""
    job_id: str
    total_pages: int


class PageExtractResponse(BaseModel):
    """Returned by POST /inward/extract-po/{job_id}/page/{page_num}"""
    job_id: str
    page_num: int
    total_pages: int
    purchase_orders: List[POExtractResponse]


# ---------- SKU lookup models ----------


class SKULookupRequest(BaseModel):
    item_description: str


class SKULookupResponse(BaseModel):
    sku_id: Optional[int] = None
    item_description: str
    material_type: Optional[str] = None
    item_category: Optional[str] = None
    sub_category: Optional[str] = None


# ---------- Approval models ----------

class ApprovalTransactionFields(BaseModel):
    """Optional transaction fields that can be filled at approval time."""
    warehouse: Optional[str] = None
    vehicle_number: Optional[str] = None
    transporter_name: Optional[str] = None
    lr_number: Optional[str] = None
    challan_number: Optional[str] = None
    invoice_number: Optional[str] = None
    grn_number: Optional[str] = None
    grn_quantity: Optional[Decimal18_3] = None
    system_grn_date: Optional[str] = None
    service_invoice_number: Optional[str] = None
    dn_number: Optional[str] = None
    approval_authority: Optional[str] = None
    remark: Optional[str] = None
    service: Optional[bool] = False
    rtv: Optional[bool] = False


class ApprovalArticleFields(BaseModel):
    """Article fields filled at approval. Matched by item_description."""
    item_description: str
    quality_grade: Optional[str] = None
    uom: Optional[str] = None
    po_quantity: Optional[Decimal18_3] = None
    units: Optional[str] = None
    quantity_units: Optional[Decimal18_3] = None
    net_weight: Optional[Decimal18_3] = None
    total_weight: Optional[Decimal18_3] = None
    lot_number: Optional[str] = None
    manufacturing_date: Optional[str] = None
    expiry_date: Optional[str] = None
    unit_rate: Optional[Decimal18_2] = None
    total_amount: Optional[Decimal18_2] = None
    carton_weight: Optional[Decimal18_3] = None


class ApprovalBoxFields(BaseModel):
    """Box fields filled at approval."""
    article_description: str
    box_number: PositiveInt
    net_weight: Optional[Decimal18_3] = None
    gross_weight: Optional[Decimal18_3] = None
    lot_number: Optional[str] = None
    count: Optional[NonNegativeInt] = None


class ApprovalRequest(BaseModel):
    approved_by: str
    transaction: Optional[ApprovalTransactionFields] = None
    articles: Optional[List[ApprovalArticleFields]] = None
    boxes: Optional[List[ApprovalBoxFields]] = None


# ---------- Box upsert + edit log models ----------


class BoxUpsertRequest(BaseModel):
    """Single box upsert — called when the Print button is clicked."""
    article_description: str
    box_number: PositiveInt
    net_weight: Optional[Decimal18_3] = None
    gross_weight: Optional[Decimal18_3] = None
    lot_number: Optional[str] = None
    count: Optional[NonNegativeInt] = None


class BoxUpsertResponse(BaseModel):
    status: str  # "inserted" or "updated"
    box_id: str
    transaction_no: str
    article_description: str
    box_number: int


class BoxEditLogEntry(BaseModel):
    field_name: str
    old_value: Optional[str] = None
    new_value: Optional[str] = None


class BoxEditLogRequest(BaseModel):
    email_id: str
    box_id: str
    transaction_no: str
    changes: List[BoxEditLogEntry]


# ---------- SKU dropdown models ----------


class SKUDropdownSelectedState(BaseModel):
    material_type: Optional[str] = None
    item_description: Optional[str] = None
    item_category: Optional[str] = None
    sub_category: Optional[str] = None


class SKUResolvedFromItem(BaseModel):
    material_type: Optional[str] = None
    item_category: Optional[str] = None
    sub_category: Optional[str] = None


class SKUDropdownOptions(BaseModel):
    material_types: List[str] = []
    item_categories: List[str] = []
    sub_categories: List[str] = []
    item_descriptions: List[str] = []
    item_ids: List[int] = []


class SKUDropdownMeta(BaseModel):
    total_material_types: int = 0
    total_item_descriptions: int = 0
    total_categories: int = 0
    total_sub_categories: int = 0
    limit: int = 200
    offset: int = 0
    sort: str = "alpha"
    search: Optional[str] = None


class SKUDropdownResponse(BaseModel):
    company: str
    selected: SKUDropdownSelectedState
    auto_selection: dict
    options: SKUDropdownOptions
    meta: SKUDropdownMeta


class SKUGlobalSearchItem(BaseModel):
    id: int
    item_description: str
    material_type: Optional[str] = None
    group: Optional[str] = None
    sub_group: Optional[str] = None


class SKUGlobalSearchResponse(BaseModel):
    company: str
    items: List[SKUGlobalSearchItem]
    meta: dict


class SKUIdResponse(BaseModel):
    sku_id: Optional[int] = None
    id: Optional[int] = None
    item_description: str
    material_type: Optional[str] = None
    group: Optional[str] = None
    sub_group: Optional[str] = None
    item_category: Optional[str] = None
    sub_category: Optional[str] = None
    company: str
