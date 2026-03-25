from decimal import Decimal
from typing import Annotated, List, Literal, Optional
from datetime import date, datetime

from pydantic import BaseModel, Field


# ── Literal types ────────────────────────────────────────────────

ShelfPosition = Literal["eye_level", "top", "middle", "bottom"]
StockAvailability = Literal["in_stock", "low_stock", "out_of_stock"]
PromotionType = Literal["discount", "bogo", "combo", "cashback", "gift"]
ShelfShareChoice = Literal["very_low", "low", "about_same", "high", "very_high"]
FootfallChoice = Literal["less_than_100", "100_to_300", "300_to_500", "above_500"]

Decimal10_2 = Annotated[Decimal, Field(max_digits=10, decimal_places=2)]
Decimal8_3 = Annotated[Decimal, Field(max_digits=8, decimal_places=3)]
Decimal5_2 = Annotated[Decimal, Field(max_digits=5, decimal_places=2)]


# ── Competitor Master ────────────────────────────────────────────

class CompetitorCreate(BaseModel):
    name: str
    category: Optional[str] = None
    logo_url: Optional[str] = None
    website: Optional[str] = None
    is_active: bool = True


class CompetitorUpdate(BaseModel):
    name: Optional[str] = None
    category: Optional[str] = None
    logo_url: Optional[str] = None
    website: Optional[str] = None
    is_active: Optional[bool] = None


class CompetitorResponse(BaseModel):
    id: str
    name: str
    category: Optional[str] = None
    logo_url: Optional[str] = None
    website: Optional[str] = None
    is_active: bool
    is_verified: bool
    created_by: str
    created_by_role: str
    created_at: str
    updated_at: str


class CompetitorListResponse(BaseModel):
    records: List[CompetitorResponse] = []
    total: int = 0
    page: int = 1
    per_page: int = 20
    total_pages: int = 0


# ── Competitor Products ──────────────────────────────────────────

class ProductCreate(BaseModel):
    competitor_id: str
    product_name: str
    ean: Optional[str] = None
    category: Optional[str] = None
    sub_category: Optional[str] = None
    size_kg: Optional[Decimal8_3] = None
    mrp: Optional[Decimal10_2] = None
    selling_price: Optional[Decimal10_2] = None
    our_equivalent_ean: Optional[str] = None


class ProductUpdate(BaseModel):
    product_name: Optional[str] = None
    ean: Optional[str] = None
    category: Optional[str] = None
    sub_category: Optional[str] = None
    size_kg: Optional[Decimal8_3] = None
    mrp: Optional[Decimal10_2] = None
    selling_price: Optional[Decimal10_2] = None
    our_equivalent_ean: Optional[str] = None


class ProductResponse(BaseModel):
    id: str
    competitor_id: str
    competitor_name: Optional[str] = None
    product_name: str
    ean: Optional[str] = None
    category: Optional[str] = None
    sub_category: Optional[str] = None
    size_kg: Optional[Decimal8_3] = None
    mrp: Optional[Decimal10_2] = None
    selling_price: Optional[Decimal10_2] = None
    our_equivalent_ean: Optional[str] = None
    is_verified: bool
    created_by: str
    created_at: str
    updated_at: str


class ProductListResponse(BaseModel):
    records: List[ProductResponse] = []
    total: int = 0
    page: int = 1
    per_page: int = 20
    total_pages: int = 0


# ── Auto-Suggest ─────────────────────────────────────────────────

class SuggestItem(BaseModel):
    id: str
    name: str
    is_verified: bool


class SuggestResponse(BaseModel):
    suggestions: List[SuggestItem] = []


class CategorySuggestItem(BaseModel):
    category: str


class CategorySuggestResponse(BaseModel):
    suggestions: List[CategorySuggestItem] = []


# ── Price Tracking ───────────────────────────────────────────────

class PriceTrackingCreate(BaseModel):
    competitor_name: str
    product_name: str
    observed_mrp: float
    observed_selling_price: float
    offer_description: Optional[str] = None
    shelf_position: ShelfPosition
    facing_count: int = Field(ge=0, default=0)
    stock_availability: StockAvailability
    photo_url: Optional[str] = None


class PriceTrackingCreateResponse(BaseModel):
    id: str
    message: str


class PriceTrackingItem(BaseModel):
    competitor_name: str
    product_name: str
    store_name: str
    observed_mrp: Decimal10_2
    observed_selling_price: Decimal10_2
    discount_percentage: Optional[Decimal5_2] = None
    offer_description: Optional[str] = None
    shelf_position: str
    facing_count: int
    stock_availability: str
    observed_at: str


class PriceTrackingListResponse(BaseModel):
    items: List[PriceTrackingItem] = []


# ── Promotions ───────────────────────────────────────────────────

class PromotionCreate(BaseModel):
    competitor_name: str
    promotion_type: PromotionType
    description: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    photo_url: Optional[str] = None


class PromotionUpdate(BaseModel):
    description: Optional[str] = None
    promotion_type: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    is_active: Optional[bool] = None
    photo_url: Optional[str] = None


class PromotionCreateResponse(BaseModel):
    id: str
    message: str


class PromotionResponse(BaseModel):
    id: str
    competitor_id: str
    competitor_name: str
    promotion_type: str
    description: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    store_name: str
    promoter_id: str
    is_active: bool
    photo_url: Optional[str] = None
    created_at: str


class PromotionListResponse(BaseModel):
    records: List[PromotionResponse] = []
    total: int = 0
    page: int = 1
    per_page: int = 20
    total_pages: int = 0


# ── Market Share ─────────────────────────────────────────────────

class MarketShareCreate(BaseModel):
    competitor_name: str
    category: str
    our_shelf_share: ShelfShareChoice
    competitor_shelf_share: ShelfShareChoice
    estimated_footfall: Optional[FootfallChoice] = None


class MarketShareCreateResponse(BaseModel):
    id: str
    message: str


class MarketShareItem(BaseModel):
    competitor_name: str
    category: str
    store_name: str
    our_shelf_share_pct: Decimal5_2
    competitor_shelf_share_pct: Decimal5_2
    estimated_footfall: Optional[int] = None
    observed_at: str


class MarketShareListResponse(BaseModel):
    items: List[MarketShareItem] = []


# ── Admin Review ─────────────────────────────────────────────────

class PendingReviewItem(BaseModel):
    id: str
    item_type: str
    name: str
    created_by: str
    created_at: str


class PendingReviewResponse(BaseModel):
    records: List[PendingReviewItem] = []
    total: int = 0
    page: int = 1
    per_page: int = 20
    total_pages: int = 0


class MergeRequest(BaseModel):
    keep_id: str
    merge_ids: List[str]


class MergeResponse(BaseModel):
    message: str
    merged_count: int


# ── Generic ──────────────────────────────────────────────────────

class MessageResponse(BaseModel):
    message: str
