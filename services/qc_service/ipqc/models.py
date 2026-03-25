from datetime import date, datetime
from typing import List, Literal, Optional

from pydantic import BaseModel


# ── Checklist item schemas ───────────────────


class SensoryItem(BaseModel):
    parameter: Literal[
        "appearance_texture", "taste", "flavour", "odour", "other"
    ]
    checked: bool = False
    remark: Optional[str] = None


class PhysicalItem(BaseModel):
    parameter: str
    checked: bool = False
    value: Optional[str] = None
    remark: Optional[str] = None


class LabelItem(BaseModel):
    parameter: Literal[
        "product_name", "net_weight", "fssai_no", "storage_condition",
        "country_of_origin", "batch_no", "pkg_date", "exp_date",
        "packed_by_address", "marketed_by_address", "imported_by_address",
        "allergen_information", "mrp_usp", "barcode_article_no",
    ]
    checked: bool = False
    remark: Optional[str] = None


PhysicalCategory = Literal["dates", "seeds", "other"]
Verdict = Literal["accept", "reject"]


# ── Article schema ──────────────────────────


class ArticleItem(BaseModel):
    item_description: Optional[str] = None
    customer: Optional[str] = None
    batch_number: Optional[str] = None
    physical_category: PhysicalCategory = "other"
    sensory_evaluation: List[SensoryItem] = []
    physical_parameters: List[PhysicalItem] = []
    label_check: List[LabelItem] = []
    seal_check: bool = False
    verdict: Verdict = "accept"
    overall_remark: Optional[str] = None


# ── Request schemas ──────────────────────────


class IPQCCreateRequest(BaseModel):
    check_date: Optional[date] = None
    factory_code: Optional[str] = None
    floor: Optional[str] = None
    articles: List[ArticleItem] = []
    checked_by: Optional[str] = None

    # Flat fields kept for backward compatibility
    item_description: Optional[str] = None
    customer: Optional[str] = None
    batch_number: Optional[str] = None
    sensory_evaluation: List[SensoryItem] = []
    physical_category: PhysicalCategory = "other"
    physical_parameters: List[PhysicalItem] = []
    label_check: List[LabelItem] = []
    seal_check: bool = False
    verdict: Verdict = "accept"
    overall_remark: Optional[str] = None


class IPQCUpdateRequest(BaseModel):
    check_date: Optional[date] = None
    factory_code: Optional[str] = None
    floor: Optional[str] = None
    articles: Optional[List[ArticleItem]] = None

    # Flat fields kept for backward compatibility
    item_description: Optional[str] = None
    customer: Optional[str] = None
    batch_number: Optional[str] = None
    sensory_evaluation: Optional[List[SensoryItem]] = None
    physical_category: Optional[PhysicalCategory] = None
    physical_parameters: Optional[List[PhysicalItem]] = None
    label_check: Optional[List[LabelItem]] = None
    seal_check: Optional[bool] = None
    verdict: Optional[Verdict] = None
    overall_remark: Optional[str] = None


class IPQCApprovalRequest(BaseModel):
    username: str       # email — verified against ipqc_users
    password: str       # plain text — verified on server


class IPQCAdminActionRequest(BaseModel):
    username: str       # email — must be an admin in ipqc_users
    password: str       # plain text — verified on server


# ── Response schemas ─────────────────────────


class IPQCResponse(BaseModel):
    id: int
    ipqc_no: str
    check_date: Optional[str] = None
    item_description: Optional[str] = None
    customer: Optional[str] = None
    batch_number: Optional[str] = None
    factory_code: Optional[str] = None
    floor: Optional[str] = None

    sensory_evaluation: list = []
    physical_category: Optional[str] = None
    physical_parameters: list = []
    label_check: list = []

    seal_check: Optional[bool] = None
    verdict: Optional[str] = None
    overall_remark: Optional[str] = None

    articles: list = []

    checked_by: Optional[str] = None
    approved_by: Optional[str] = None
    approved_at: Optional[str] = None

    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class IPQCListResponse(BaseModel):
    records: List[IPQCResponse] = []
    total: int = 0
    page: int = 1
    per_page: int = 20
    total_pages: int = 0


class IPQCDeleteResponse(BaseModel):
    success: bool
    message: str
    ipqc_no: Optional[str] = None


# ── SKU lookup schemas ──────────────────────


class IPQCSKULookupRequest(BaseModel):
    item_description: str


class IPQCSKULookupResponse(BaseModel):
    sku_id: Optional[int] = None
    item_description: str
    material_type: Optional[str] = None
    item_category: Optional[str] = None
    sub_category: Optional[str] = None
    sale_group: Optional[str] = None
    source_company: Optional[str] = None


class IPQCSKUSearchResponse(BaseModel):
    items: List[IPQCSKULookupResponse] = []
    total: int = 0
