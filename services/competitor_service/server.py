from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session

from shared.logger import get_logger

logger = get_logger("competitor.server")

from shared.database import get_db
from shared.models import Promoter
from services.auth_service.dependencies import get_current_promoter
from services.ims_service.dependencies import verify_token
from services.competitor_service.models import (
    CompetitorCreate,
    CompetitorUpdate,
    CompetitorResponse,
    CompetitorListResponse,
    ProductCreate,
    ProductUpdate,
    ProductResponse,
    ProductListResponse,
    SuggestResponse,
    CategorySuggestResponse,
    PriceTrackingCreate,
    PriceTrackingCreateResponse,
    PriceTrackingListResponse,
    PromotionCreate,
    PromotionCreateResponse,
    PromotionUpdate,
    PromotionResponse,
    PromotionListResponse,
    MarketShareCreate,
    MarketShareCreateResponse,
    MarketShareListResponse,
    PendingReviewResponse,
    MergeRequest,
    MergeResponse,
    MessageResponse,
)
from services.competitor_service.tools import (
    create_competitor,
    list_competitors,
    get_competitor,
    update_competitor,
    delete_competitor,
    create_product,
    list_products,
    update_product,
    delete_product,
    suggest_competitors,
    suggest_products,
    suggest_categories,
    create_price_tracking,
    list_price_tracking,
    create_promotion,
    list_promotions,
    update_promotion,
    delete_promotion,
    create_market_share,
    list_market_share,
    list_pending_reviews,
    verify_item,
    merge_competitors,
)

router = APIRouter(prefix="/competitor", tags=["competitor"])


# ── Auto-Suggest (Promoter auth) ─────────────────────────────────


@router.get("/suggest/competitors", response_model=SuggestResponse)
def suggest_competitors_endpoint(
    q: str = Query(..., min_length=2),
    promoter: Promoter = Depends(get_current_promoter),
    db: Session = Depends(get_db),
):
    return suggest_competitors(q, db)


@router.get("/suggest/products", response_model=SuggestResponse)
def suggest_products_endpoint(
    q: str = Query(..., min_length=2),
    competitor_id: Optional[str] = Query(None),
    promoter: Promoter = Depends(get_current_promoter),
    db: Session = Depends(get_db),
):
    return suggest_products(q, competitor_id, db)


@router.get("/suggest/categories", response_model=CategorySuggestResponse)
def suggest_categories_endpoint(
    q: str = Query(..., min_length=2),
    promoter: Promoter = Depends(get_current_promoter),
    db: Session = Depends(get_db),
):
    return suggest_categories(q, db)


# ── Products CRUD (Admin auth) ───────────────────────────────────


@router.post("/products", response_model=ProductResponse, status_code=201)
def create_product_endpoint(
    data: ProductCreate,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return create_product(data, user, db)


@router.get("/products", response_model=ProductListResponse)
def list_products_endpoint(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=500),
    search: Optional[str] = Query(None),
    competitor_id: Optional[str] = Query(None),
    is_verified: Optional[bool] = Query(None),
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return list_products(page, per_page, search, competitor_id, is_verified, db)


@router.put("/products/{product_id}", response_model=ProductResponse)
def update_product_endpoint(
    product_id: str,
    data: ProductUpdate,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return update_product(product_id, data.model_dump(exclude_none=True), db)


@router.delete("/products/{product_id}", response_model=MessageResponse)
def delete_product_endpoint(
    product_id: str,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return delete_product(product_id, db)


# ── Price Tracking ───────────────────────────────────────────────


@router.post("/price-tracking/debug")
async def debug_price_tracking(request: Request):
    """Temporary debug endpoint — logs raw request body."""
    body = await request.body()
    logger.info("RAW BODY: %s", body.decode())
    return {"raw": body.decode()}


@router.post("/price-tracking", response_model=PriceTrackingCreateResponse, status_code=201)
def create_price_tracking_endpoint(
    data: PriceTrackingCreate,
    promoter: Promoter = Depends(get_current_promoter),
    db: Session = Depends(get_db),
):
    return create_price_tracking(data, promoter, db)


@router.get("/price-tracking", response_model=PriceTrackingListResponse)
def list_price_tracking_endpoint(
    promoter: Promoter = Depends(get_current_promoter),
    db: Session = Depends(get_db),
):
    return list_price_tracking(str(promoter.id), db)


# ── Promotions ───────────────────────────────────────────────────


@router.post("/promotions", response_model=PromotionCreateResponse, status_code=201)
def create_promotion_endpoint(
    data: PromotionCreate,
    promoter: Promoter = Depends(get_current_promoter),
    db: Session = Depends(get_db),
):
    return create_promotion(data, promoter, db)


@router.get("/promotions", response_model=PromotionListResponse)
def list_promotions_endpoint(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=500),
    competitor_id: Optional[str] = Query(None),
    promotion_type: Optional[str] = Query(None),
    is_active: Optional[bool] = Query(None),
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return list_promotions(page, per_page, competitor_id, promotion_type, is_active, db)


@router.put("/promotions/{promo_id}", response_model=PromotionResponse)
def update_promotion_endpoint(
    promo_id: str,
    data: PromotionUpdate,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return update_promotion(promo_id, data.model_dump(exclude_none=True), db)


@router.delete("/promotions/{promo_id}", response_model=MessageResponse)
def delete_promotion_endpoint(
    promo_id: str,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return delete_promotion(promo_id, db)


# ── Market Share ─────────────────────────────────────────────────


@router.post("/market-share", response_model=MarketShareCreateResponse, status_code=201)
def create_market_share_endpoint(
    data: MarketShareCreate,
    promoter: Promoter = Depends(get_current_promoter),
    db: Session = Depends(get_db),
):
    return create_market_share(data, promoter, db)


@router.get("/market-share", response_model=MarketShareListResponse)
def list_market_share_endpoint(
    promoter: Promoter = Depends(get_current_promoter),
    db: Session = Depends(get_db),
):
    return list_market_share(str(promoter.id), db)


# ── Admin Review ─────────────────────────────────────────────────


@router.get("/review/pending", response_model=PendingReviewResponse)
def list_pending_reviews_endpoint(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=500),
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return list_pending_reviews(page, per_page, db)


@router.put("/review/{item_id}/verify", response_model=MessageResponse)
def verify_item_endpoint(
    item_id: str,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return verify_item(item_id, db)


@router.post("/review/merge", response_model=MergeResponse)
def merge_competitors_endpoint(
    data: MergeRequest,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return merge_competitors(data.keep_id, data.merge_ids, db)


# ── Competitor Master CRUD (Admin auth) — LAST to avoid path conflicts


@router.post("/", response_model=CompetitorResponse, status_code=201)
def create_competitor_endpoint(
    data: CompetitorCreate,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return create_competitor(data, user, db)


@router.get("/", response_model=CompetitorListResponse)
def list_competitors_endpoint(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=500),
    search: Optional[str] = Query(None),
    is_verified: Optional[bool] = Query(None),
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return list_competitors(page, per_page, search, is_verified, db)


@router.get("/{competitor_id}", response_model=CompetitorResponse)
def get_competitor_endpoint(
    competitor_id: str,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return get_competitor(competitor_id, db)


@router.put("/{competitor_id}", response_model=CompetitorResponse)
def update_competitor_endpoint(
    competitor_id: str,
    data: CompetitorUpdate,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return update_competitor(competitor_id, data.model_dump(exclude_none=True), db)


@router.delete("/{competitor_id}", response_model=MessageResponse)
def delete_competitor_endpoint(
    competitor_id: str,
    user: dict = Depends(verify_token),
    db: Session = Depends(get_db),
):
    return delete_competitor(competitor_id, db)
