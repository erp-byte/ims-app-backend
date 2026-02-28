import base64
import json
import re
import time
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict

import anthropic
from fastapi import HTTPException
from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session

from shared.config_loader import settings
from shared.logger import get_logger
from services.ims_service.inward_models import (
    Company,
    InwardPayloadFlexible,
    InwardListItem,
    InwardListResponse,
    ApprovalRequest,
    BoxUpsertRequest,
    BoxUpsertResponse,
    BoxEditLogRequest,
    SKUDropdownSelectedState,
    SKUResolvedFromItem,
    SKUDropdownOptions,
    SKUDropdownMeta,
    SKUDropdownResponse,
    SKUGlobalSearchItem,
    SKUGlobalSearchResponse,
    SKUIdResponse,
)

logger = get_logger("ims.inward")

# Global lock to prevent concurrent PO extractions (each extraction fires
# many sequential Claude API calls; concurrent extractions overwhelm rate limits)
_extraction_lock = threading.Lock()

# ── Per-page extraction cache ──────────────────────────────────────────────


@dataclass
class _PageCacheEntry:
    pages: list[bytes]       # single-page PDF bytes
    total_pages: int
    created_at: float        # time.time()


_page_cache: Dict[str, _PageCacheEntry] = {}
_page_cache_lock = threading.Lock()
_CACHE_TTL_SECONDS = 600  # 10 minutes


def _cleanup_expired_cache():
    """Remove cache entries older than TTL."""
    now = time.time()
    with _page_cache_lock:
        expired = [k for k, v in _page_cache.items() if now - v.created_at > _CACHE_TTL_SECONDS]
        for k in expired:
            del _page_cache[k]
            logger.info(f"Cleaned up expired page cache: {k}")


def upload_po_pdf(file_bytes: bytes) -> dict:
    """Split PDF into pages, store in cache, return job_id + total_pages."""
    _cleanup_expired_cache()

    pages = _split_pdf_pages(file_bytes)
    if not pages:
        raise HTTPException(422, "PDF contains no pages")

    job_id = str(uuid.uuid4())
    entry = _PageCacheEntry(
        pages=pages,
        total_pages=len(pages),
        created_at=time.time(),
    )

    with _page_cache_lock:
        _page_cache[job_id] = entry

    logger.info(f"Uploaded PDF: job_id={job_id}, pages={len(pages)}")
    return {"job_id": job_id, "total_pages": len(pages)}


def extract_single_page(job_id: str, page_num: int) -> dict:
    """Extract POs from a single cached page. page_num is 1-indexed."""
    with _page_cache_lock:
        entry = _page_cache.get(job_id)

    if not entry:
        raise HTTPException(404, "Job not found or expired. Please re-upload the PDF.")

    if page_num < 1 or page_num > entry.total_pages:
        raise HTTPException(400, f"Invalid page_num {page_num}. Must be 1-{entry.total_pages}.")

    page_bytes = entry.pages[page_num - 1]
    page_b64 = base64.standard_b64encode(page_bytes).decode("utf-8")

    client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
    result = _call_claude_extract(client, page_b64)

    page_pos = result.get("purchase_orders", [])

    # Apply item_description cleanup
    for po in page_pos:
        for article in po.get("articles", []):
            desc = article.get("item_description", "")
            if desc:
                cleaned = " ".join(desc.split())
                cleaned = re.split(r"\s*desc\s*:\s*", cleaned, maxsplit=1, flags=re.IGNORECASE)[0].strip()
                if cleaned:
                    article["item_description"] = cleaned

    logger.info(f"Extracted page {page_num}/{entry.total_pages} for job {job_id}: {len(page_pos)} PO(s)")

    return {
        "job_id": job_id,
        "page_num": page_num,
        "total_pages": entry.total_pages,
        "purchase_orders": page_pos,
    }


# ---------- Helpers ----------


def table_names(company: Company) -> dict:
    prefix = "cfpl" if company == "CFPL" else "cdpl"
    return {
        "tx": f"{prefix}_transactions_v2",
        "art": f"{prefix}_articles_v2",
        "box": f"{prefix}_boxes_v2",
        "sku": f"{prefix}sku",
    }


def generate_box_ids(boxes: List[dict]) -> List[dict]:
    """Assign epoch-based box_id to each box. Last 8 digits of epoch ms + counter."""
    base = str(int(time.time() * 1000))[-8:]
    result = []
    for i, box in enumerate(boxes, start=1):
        box["box_id"] = f"{base}-{i}"
        result.append(box)
    return result


def clean_date_fields(data: dict) -> dict:
    """Convert empty strings to None for date fields."""
    date_fields = ["system_grn_date", "manufacturing_date", "expiry_date"]
    cleaned = data.copy()
    for field in date_fields:
        if field in cleaned and cleaned[field] == "":
            cleaned[field] = None
    return cleaned


def format_date_for_frontend(date_value) -> Optional[str]:
    """Format date values for frontend consumption (YYYY-MM-DD)."""
    if date_value is None:
        return None

    try:
        if isinstance(date_value, str):
            cleaned = date_value.strip()

            if re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}", cleaned):
                try:
                    return cleaned.split(" ")[0]
                except ValueError:
                    pass

            try:
                if "+" in cleaned and len(cleaned.split("+")[1]) == 2:
                    cleaned = cleaned.replace("+00", "+0000", 1)

                timestamp_formats = [
                    "%Y-%m-%d %H:%M:%S%z",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%dT%H:%M:%SZ",
                ]
                for fmt in timestamp_formats:
                    try:
                        return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
                    except ValueError:
                        continue
            except Exception:
                pass

            if " " in cleaned and re.match(r"\d{4}-\d{2}-\d{2}", cleaned):
                return cleaned.split(" ")[0]

            return cleaned

        elif hasattr(date_value, "strftime"):
            return date_value.strftime("%Y-%m-%d")
        else:
            return format_date_for_frontend(str(date_value))

    except Exception:
        if isinstance(date_value, str) and " " in date_value:
            try:
                return date_value.split(" ")[0]
            except Exception:
                pass
        return str(date_value) if date_value else None


def format_record_dates(record_dict: dict) -> dict:
    """Format all date fields in a record for frontend consumption."""
    date_fields = ["entry_date", "system_grn_date", "manufacturing_date", "expiry_date"]
    formatted = record_dict.copy()
    for field in date_fields:
        if field in formatted:
            formatted[field] = format_date_for_frontend(formatted[field])
    return formatted


def validate_and_normalize_dates(
    from_date: Optional[str], to_date: Optional[str]
) -> tuple[Optional[str], Optional[str]]:
    """Validate and normalize date inputs, ensuring correct order."""
    if not from_date and not to_date:
        return None, None

    try:
        from_dt = datetime.strptime(from_date, "%Y-%m-%d").date() if from_date else None
        to_dt = datetime.strptime(to_date, "%Y-%m-%d").date() if to_date else None

        if from_dt and to_dt and from_dt > to_dt:
            from_dt, to_dt = to_dt, from_dt

        return (
            from_dt.strftime("%Y-%m-%d") if from_dt else None,
            to_dt.strftime("%Y-%m-%d") if to_dt else None,
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD format.")


def build_search_conditions(
    tables: dict, search: Optional[str], from_date: Optional[str], to_date: Optional[str]
) -> tuple[str, dict]:
    """Build comprehensive WHERE clause for search across all fields."""
    where_clauses = ["1=1"]
    params: dict = {}

    if search and search.strip():
        search_term = f"%{search.strip()}%"

        search_fields = [
            # Transaction fields
            "t.transaction_no", "t.vehicle_number", "t.transporter_name",
            "t.lr_number", "t.vendor_supplier_name", "t.customer_party_name",
            "t.source_location", "t.destination_location", "t.challan_number",
            "t.invoice_number", "t.po_number", "t.grn_number", "t.purchased_by",
            "t.service_invoice_number", "t.dn_number", "t.approval_authority",
            "t.warehouse", "t.remark", "t.currency",
            # Article fields
            "a.item_description", "a.item_category", "a.sub_category",
            "a.material_type", "a.quality_grade", "a.uom", "a.units",
            "a.lot_number",
            # Box fields
            "b.article_description", "b.lot_number", "b.box_id",
        ]

        search_conditions = [f"COALESCE({f}, '') ILIKE :search" for f in search_fields]

        numeric_search_conditions = [
            "CAST(COALESCE(t.grn_quantity, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(t.total_amount, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(t.tax_amount, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(t.discount_amount, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(t.po_quantity, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(a.sku_id, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(a.po_weight, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(a.po_quantity, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(a.quantity_units, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(a.net_weight, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(a.total_weight, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(a.unit_rate, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(a.total_amount, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(a.carton_weight, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(b.box_number, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(b.net_weight, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(b.gross_weight, 0) AS TEXT) ILIKE :search",
            "CAST(COALESCE(b.count, 0) AS TEXT) ILIKE :search",
        ]

        all_conditions = search_conditions + numeric_search_conditions
        where_clauses.append(f"({' OR '.join(all_conditions)})")
        params["search"] = search_term

    # Date filtering — uses entry_date (falls back to system_grn_date)
    if from_date or to_date:
        date_expr = "CAST(COALESCE(t.entry_date, t.system_grn_date) AS DATE)"
        if from_date and to_date:
            if from_date == to_date:
                where_clauses.append(f"{date_expr} = CAST(:target_date AS DATE)")
                params["target_date"] = from_date
            else:
                where_clauses.append(f"{date_expr} BETWEEN CAST(:from_date AS DATE) AND CAST(:to_date AS DATE)")
                params["from_date"] = from_date
                params["to_date"] = to_date
        elif from_date:
            where_clauses.append(f"{date_expr} >= CAST(:from_date AS DATE)")
            params["from_date"] = from_date
        elif to_date:
            where_clauses.append(f"{date_expr} <= CAST(:to_date AS DATE)")
            params["to_date"] = to_date

    return " AND ".join(where_clauses), params


# ---------- CRUD ----------


def list_inward_records(
    company: Company,
    page: int,
    per_page: int,
    search: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    sort_by: Optional[str],
    sort_order: Optional[str],
    db: Session,
    status: Optional[str] = None,
    grn_status: Optional[str] = None,
) -> InwardListResponse:
    tables = table_names(company)

    normalized_from, normalized_to = validate_and_normalize_dates(from_date, to_date)
    where_sql, params = build_search_conditions(tables, search, normalized_from, normalized_to)

    if status:
        valid_statuses = ["pending", "approved"]
        if status not in valid_statuses:
            raise HTTPException(400, f"Invalid status. Allowed: {valid_statuses}")
        where_sql += " AND t.status = :status"
        params["status"] = status

    if grn_status:
        if grn_status == "completed":
            where_sql += " AND t.grn_number IS NOT NULL AND TRIM(t.grn_number) != ''"
        elif grn_status == "pending":
            where_sql += " AND (t.grn_number IS NULL OR TRIM(t.grn_number) = '')"
        else:
            raise HTTPException(400, "Invalid grn_status. Allowed: [completed, pending]")

    valid_sort_fields = ["entry_date", "transaction_no", "invoice_number", "po_number"]
    valid_sort_orders = ["asc", "desc"]

    if sort_by and sort_by not in valid_sort_fields:
        raise HTTPException(status_code=400, detail=f"Invalid sort field. Allowed: {valid_sort_fields}")
    if sort_order and sort_order not in valid_sort_orders:
        raise HTTPException(status_code=400, detail=f"Invalid sort order. Allowed: {valid_sort_orders}")

    sort_field = "COALESCE(entry_date, system_grn_date)" if (not sort_by or sort_by == "entry_date") else sort_by
    sort_direction = sort_order or "desc"

    total = db.execute(
        text(f"""
            SELECT COUNT(DISTINCT t.transaction_no)
            FROM {tables['tx']} t
            LEFT JOIN {tables['art']} a ON t.transaction_no = a.transaction_no
            LEFT JOIN {tables['box']} b ON t.transaction_no = b.transaction_no
            WHERE {where_sql}
        """),
        params,
    ).scalar_one()

    offset = (page - 1) * per_page
    order_clause = f"{sort_field} {sort_direction.upper()} NULLS LAST, transaction_no DESC"

    records = db.execute(
        text(f"""
            WITH filtered_transactions AS (
                SELECT DISTINCT t.transaction_no
                FROM {tables['tx']} t
                LEFT JOIN {tables['art']} a ON t.transaction_no = a.transaction_no
                LEFT JOIN {tables['box']} b ON t.transaction_no = b.transaction_no
                WHERE {where_sql}
            ),
            transaction_data AS (
                SELECT
                    t.transaction_no,
                    t.entry_date,
                    t.system_grn_date,
                    t.status,
                    t.invoice_number,
                    t.po_number,
                    t.vendor_supplier_name,
                    t.customer_party_name,
                    t.total_amount,
                    STRING_AGG(DISTINCT a.item_description, ', ' ORDER BY a.item_description) AS article_descriptions,
                    STRING_AGG(DISTINCT
                        CASE
                            WHEN a.quantity_units IS NOT NULL AND a.uom IS NOT NULL
                            THEN CONCAT(a.quantity_units::text, ' ', a.uom)
                            WHEN a.quantity_units IS NOT NULL
                            THEN a.quantity_units::text
                            ELSE NULL
                        END, ', '
                        ORDER BY CASE
                            WHEN a.quantity_units IS NOT NULL AND a.uom IS NOT NULL
                            THEN CONCAT(a.quantity_units::text, ' ', a.uom)
                            WHEN a.quantity_units IS NOT NULL
                            THEN a.quantity_units::text
                            ELSE NULL
                        END
                    ) FILTER (WHERE a.quantity_units IS NOT NULL) AS article_quantities,
                    COUNT(DISTINCT b.box_number) AS box_count,
                    STRING_AGG(DISTINCT b.article_description, ', ' ORDER BY b.article_description) AS box_descriptions
                FROM {tables['tx']} t
                INNER JOIN filtered_transactions ft ON t.transaction_no = ft.transaction_no
                LEFT JOIN {tables['art']} a ON t.transaction_no = a.transaction_no
                LEFT JOIN {tables['box']} b ON t.transaction_no = b.transaction_no
                GROUP BY t.transaction_no, t.entry_date, t.system_grn_date, t.status, t.invoice_number, t.po_number, t.vendor_supplier_name, t.customer_party_name, t.total_amount
            )
            SELECT
                td.transaction_no,
                COALESCE(td.entry_date, td.system_grn_date) AS entry_date,
                td.status,
                td.invoice_number,
                td.po_number,
                td.vendor_supplier_name,
                td.customer_party_name,
                td.total_amount,
                COALESCE(td.article_descriptions, td.box_descriptions) AS item_descriptions_text,
                CASE
                    WHEN td.article_quantities IS NOT NULL THEN td.article_quantities
                    WHEN td.box_count > 0 THEN CONCAT(td.box_count::text, ' BOX')
                    ELSE NULL
                END AS quantities_and_uoms_text,
                EXISTS (
                    SELECT 1 FROM box_edit_logs el WHERE el.transaction_no = td.transaction_no
                ) AS has_edits
            FROM transaction_data td
            ORDER BY {order_clause}
            LIMIT :limit OFFSET :offset
        """),
        {**params, "limit": per_page, "offset": offset},
    ).fetchall()

    formatted = []
    for record in records:
        item_descriptions = []
        if record.item_descriptions_text and record.item_descriptions_text.strip():
            item_descriptions = [d.strip() for d in record.item_descriptions_text.split(",") if d.strip()]

        quantities_and_uoms = []
        if record.quantities_and_uoms_text and record.quantities_and_uoms_text.strip():
            quantities_and_uoms = [q.strip() for q in record.quantities_and_uoms_text.split(",") if q.strip()]

        formatted.append(
            InwardListItem(
                transaction_no=record.transaction_no or "",
                entry_date=format_date_for_frontend(record.entry_date) or "",
                status=record.status or "pending",
                invoice_number=record.invoice_number,
                po_number=record.po_number,
                vendor_supplier_name=record.vendor_supplier_name,
                customer_party_name=record.customer_party_name,
                total_amount=float(record.total_amount) if record.total_amount is not None else None,
                item_descriptions=item_descriptions,
                quantities_and_uoms=quantities_and_uoms,
                has_edits=record.has_edits,
            )
        )

    return InwardListResponse(records=formatted, total=total, page=page, per_page=per_page)


def export_inward_records(
    company: Company,
    search: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    sort_by: Optional[str],
    sort_order: Optional[str],
    db: Session,
    status: Optional[str] = None,
    grn_status: Optional[str] = None,
) -> list[dict]:
    """Return all filtered inward records (all 3 tables) without pagination for Excel export."""
    tables = table_names(company)

    normalized_from, normalized_to = validate_and_normalize_dates(from_date, to_date)
    where_sql, params = build_search_conditions(tables, search, normalized_from, normalized_to)

    if status:
        valid_statuses = ["pending", "approved"]
        if status not in valid_statuses:
            raise HTTPException(400, f"Invalid status. Allowed: {valid_statuses}")
        where_sql += " AND t.status = :status"
        params["status"] = status

    if grn_status:
        if grn_status == "completed":
            where_sql += " AND t.grn_number IS NOT NULL AND TRIM(t.grn_number) != ''"
        elif grn_status == "pending":
            where_sql += " AND (t.grn_number IS NULL OR TRIM(t.grn_number) = '')"
        else:
            raise HTTPException(400, "Invalid grn_status. Allowed: [completed, pending]")

    valid_sort_fields = ["entry_date", "transaction_no", "invoice_number", "po_number"]
    valid_sort_orders = ["asc", "desc"]

    if sort_by and sort_by not in valid_sort_fields:
        raise HTTPException(status_code=400, detail=f"Invalid sort field. Allowed: {valid_sort_fields}")
    if sort_order and sort_order not in valid_sort_orders:
        raise HTTPException(status_code=400, detail=f"Invalid sort order. Allowed: {valid_sort_orders}")

    sort_field = "COALESCE(t.entry_date, t.system_grn_date)" if (not sort_by or sort_by == "entry_date") else f"t.{sort_by}"
    sort_direction = sort_order or "desc"
    order_clause = f"{sort_field} {sort_direction.upper()} NULLS LAST, t.transaction_no DESC"

    records = db.execute(
        text(f"""
            WITH filtered_transactions AS (
                SELECT DISTINCT t.transaction_no
                FROM {tables['tx']} t
                LEFT JOIN {tables['art']} a ON t.transaction_no = a.transaction_no
                LEFT JOIN {tables['box']} b ON t.transaction_no = b.transaction_no
                WHERE {where_sql}
            )
            SELECT
                -- Transaction fields
                t.transaction_no,
                COALESCE(t.entry_date, t.system_grn_date) AS entry_date,
                t.status,
                t.vehicle_number,
                t.transporter_name,
                t.lr_number,
                t.vendor_supplier_name,
                t.customer_party_name,
                t.source_location,
                t.destination_location,
                t.challan_number,
                t.invoice_number,
                t.po_number,
                t.grn_number,
                t.grn_quantity,
                t.system_grn_date,
                t.purchased_by,
                t.service_invoice_number,
                t.dn_number,
                t.approval_authority,
                t.total_amount AS tx_total_amount,
                t.tax_amount,
                t.discount_amount,
                t.po_quantity AS tx_po_quantity,
                t.remark,
                t.currency,
                -- Article fields
                a.sku_id,
                a.item_description,
                a.item_category,
                a.sub_category,
                a.material_type,
                a.quality_grade,
                a.uom,
                a.po_quantity AS art_po_quantity,
                a.units,
                a.quantity_units,
                a.net_weight AS art_net_weight,
                a.total_weight,
                a.po_weight,
                a.lot_number AS art_lot_number,
                a.manufacturing_date,
                a.expiry_date,
                a.unit_rate,
                a.total_amount AS art_total_amount,
                a.carton_weight,
                -- Box fields
                b.box_id,
                b.article_description AS box_article,
                b.box_number,
                b.net_weight AS box_net_weight,
                b.gross_weight AS box_gross_weight,
                b.lot_number AS box_lot_number,
                b.count AS box_count
            FROM {tables['tx']} t
            INNER JOIN filtered_transactions ft ON t.transaction_no = ft.transaction_no
            LEFT JOIN {tables['art']} a ON t.transaction_no = a.transaction_no
            LEFT JOIN {tables['box']} b ON t.transaction_no = b.transaction_no
            ORDER BY {order_clause}, a.item_description, b.box_number
        """),
        params,
    ).fetchall()

    rows = []
    for r in records:
        rows.append({
            # Transaction
            "Transaction No": r.transaction_no or "",
            "Entry Date": format_date_for_frontend(r.entry_date) or "",
            "Status": (r.status or "pending").capitalize(),
            "Vehicle Number": r.vehicle_number or "",
            "Transporter": r.transporter_name or "",
            "LR Number": r.lr_number or "",
            "Vendor / Supplier": r.vendor_supplier_name or "",
            "Customer / Party": r.customer_party_name or "",
            "Source Location": r.source_location or "",
            "Destination": r.destination_location or "",
            "Challan Number": r.challan_number or "",
            "Invoice Number": r.invoice_number or "",
            "PO Number": r.po_number or "",
            "GRN Number": r.grn_number or "",
            "GRN Quantity": float(r.grn_quantity) if r.grn_quantity is not None else "",
            "System GRN Date": format_date_for_frontend(r.system_grn_date) or "",
            "Purchased By": r.purchased_by or "",
            "Service Invoice": r.service_invoice_number or "",
            "DN Number": r.dn_number or "",
            "Approval Authority": r.approval_authority or "",
            "Total Amount": float(r.tx_total_amount) if r.tx_total_amount is not None else "",
            "Tax Amount": float(r.tax_amount) if r.tax_amount is not None else "",
            "Discount Amount": float(r.discount_amount) if r.discount_amount is not None else "",
            "PO Quantity": float(r.tx_po_quantity) if r.tx_po_quantity is not None else "",
            "Remark": r.remark or "",
            "Currency": r.currency or "",
            # Article
            "SKU ID": r.sku_id or "",
            "Item Description": r.item_description or "",
            "Item Category": r.item_category or "",
            "Sub Category": r.sub_category or "",
            "Material Type": r.material_type or "",
            "Quality Grade": r.quality_grade or "",
            "UOM": r.uom or "",
            "Art PO Qty": float(r.art_po_quantity) if r.art_po_quantity is not None else "",
            "Units": r.units or "",
            "Quantity Units": float(r.quantity_units) if r.quantity_units is not None else "",
            "Art Net Weight": float(r.art_net_weight) if r.art_net_weight is not None else "",
            "Total Weight": float(r.total_weight) if r.total_weight is not None else "",
            "PO Weight": float(r.po_weight) if r.po_weight is not None else "",
            "Art Lot Number": r.art_lot_number or "",
            "Mfg Date": format_date_for_frontend(r.manufacturing_date) or "",
            "Expiry Date": format_date_for_frontend(r.expiry_date) or "",
            "Unit Rate": float(r.unit_rate) if r.unit_rate is not None else "",
            "Art Total Amount": float(r.art_total_amount) if r.art_total_amount is not None else "",
            "Carton Weight": float(r.carton_weight) if r.carton_weight is not None else "",
            # Box
            "Box ID": r.box_id or "",
            "Box Article": r.box_article or "",
            "Box Number": r.box_number or "",
            "Box Net Weight": float(r.box_net_weight) if r.box_net_weight is not None else "",
            "Box Gross Weight": float(r.box_gross_weight) if r.box_gross_weight is not None else "",
            "Box Lot Number": r.box_lot_number or "",
            "Box Count": int(r.box_count) if r.box_count is not None else "",
        })
    return rows


# Article column list used for INSERT
_ARTICLE_COLUMNS = (
    "transaction_no, sku_id, item_description, item_category, sub_category, "
    "material_type, quality_grade, uom, po_quantity, units, quantity_units, "
    "net_weight, total_weight, po_weight, lot_number, manufacturing_date, expiry_date, "
    "unit_rate, total_amount, carton_weight"
)

_ARTICLE_PARAMS = (
    ":transaction_no, :sku_id, :item_description, :item_category, :sub_category, "
    ":material_type, :quality_grade, :uom, :po_quantity, :units, :quantity_units, "
    ":net_weight, :total_weight, :po_weight, :lot_number, :manufacturing_date, :expiry_date, "
    ":unit_rate, :total_amount, :carton_weight"
)


def create_inward(payload: InwardPayloadFlexible, db: Session) -> dict:
    t = payload.transaction
    tables = table_names(payload.company)
    txno = t.transaction_no

    if not txno:
        raise HTTPException(400, "transaction.transaction_no is required")

    for a in payload.articles:
        if a.transaction_no != txno:
            raise HTTPException(400, f"Article '{a.item_description}' has mismatched transaction_no")
    for b in payload.boxes:
        if b.transaction_no != txno:
            raise HTTPException(400, f"Box {b.box_number} has mismatched transaction_no")

    article_names = {a.item_description for a in payload.articles}
    unknown_refs = {b.article_description for b in payload.boxes if b.article_description not in article_names}
    if unknown_refs:
        raise HTTPException(400, f"Boxes reference unknown article(s): {sorted(list(unknown_refs))}")

    _ensure_skus(payload.articles, tables, db)

    # 1) Insert transaction
    tx_data = clean_date_fields(t.model_dump())
    result = db.execute(
        text(f"""
            INSERT INTO {tables['tx']} (
                transaction_no, entry_date, vehicle_number, transporter_name, lr_number,
                vendor_supplier_name, customer_party_name, source_location, destination_location,
                challan_number, invoice_number, po_number, grn_number, grn_quantity, system_grn_date,
                purchased_by, service_invoice_number, dn_number, approval_authority,
                total_amount, tax_amount, discount_amount, po_quantity, remark, currency, status
            ) VALUES (
                :transaction_no, :entry_date, :vehicle_number, :transporter_name, :lr_number,
                :vendor_supplier_name, :customer_party_name, :source_location, :destination_location,
                :challan_number, :invoice_number, :po_number, :grn_number, :grn_quantity, :system_grn_date,
                :purchased_by, :service_invoice_number, :dn_number, :approval_authority,
                :total_amount, :tax_amount, :discount_amount, :po_quantity, :remark, :currency, 'pending'
            )
            ON CONFLICT (transaction_no) DO NOTHING
        """),
        tx_data,
    )
    if result.rowcount == 0:
        raise HTTPException(409, f"transaction_no '{txno}' already exists")

    # 2) Bulk insert articles
    if payload.articles:
        articles_data = [clean_date_fields(a.model_dump()) for a in payload.articles]
        db.execute(
            text(f"""
                INSERT INTO {tables['art']} ({_ARTICLE_COLUMNS})
                VALUES ({_ARTICLE_PARAMS})
                ON CONFLICT (transaction_no, item_description) DO NOTHING
            """),
            articles_data,
        )

    # 3) Bulk insert boxes (without box_id — assigned later when approver prints)
    if payload.boxes:
        boxes_data = [b.model_dump() for b in payload.boxes]
        db.execute(
            text(f"""
                INSERT INTO {tables['box']} (
                    transaction_no, article_description, box_number, net_weight, gross_weight, lot_number, count
                ) VALUES (
                    :transaction_no, :article_description, :box_number, :net_weight, :gross_weight, :lot_number, :count
                )
                ON CONFLICT (transaction_no, article_description, box_number) DO NOTHING
            """),
            boxes_data,
        )

    db.commit()

    return {"status": "ok", "transaction_no": txno, "company": payload.company}


def get_inward(company: Company, transaction_no: str, db: Session) -> dict:
    tables = table_names(company)

    tx_res = db.execute(
        text(f"SELECT * FROM {tables['tx']} WHERE transaction_no = :txno"),
        {"txno": transaction_no},
    ).fetchone()
    if not tx_res:
        raise HTTPException(404, f"transaction_no '{transaction_no}' not found for {company}")

    transaction = format_record_dates(dict(tx_res._mapping))

    arts = db.execute(
        text(f"""
            SELECT a.*, s.material_type AS sku_material_type
            FROM {tables['art']} a
            LEFT JOIN {tables['sku']} s ON a.sku_id = s.id
            WHERE a.transaction_no = :txno
            ORDER BY a.id ASC
        """),
        {"txno": transaction_no},
    ).fetchall()
    articles = [format_record_dates(dict(r._mapping)) for r in arts]

    boxes_res = db.execute(
        text(f"""
            SELECT * FROM {tables['box']}
            WHERE transaction_no = :txno
            ORDER BY article_description ASC, box_number ASC
        """),
        {"txno": transaction_no},
    ).fetchall()
    boxes = [dict(r._mapping) for r in boxes_res]

    # Synthesize articles from boxes if none exist
    if not articles and boxes:
        article_groups: dict = {}
        for box in boxes:
            desc = box["article_description"]
            if desc not in article_groups:
                article_groups[desc] = {
                    "transaction_no": transaction_no,
                    "sku_id": 0,
                    "item_description": desc,
                    "item_category": None,
                    "sub_category": None,
                    "material_type": None,
                    "quality_grade": None,
                    "uom": "BOX",
                    "po_quantity": None,
                    "units": None,
                    "quantity_units": 0,
                    "net_weight": 0,
                    "total_weight": 0,
                    "po_weight": None,
                    "lot_number": None,
                    "manufacturing_date": None,
                    "expiry_date": None,
                    "unit_rate": 0,
                    "total_amount": 0,
                    "carton_weight": None,
                    "box_count": 0,
                    "total_net_weight": 0,
                    "total_gross_weight": 0,
                }
            article_groups[desc]["box_count"] += 1
            if box["net_weight"]:
                article_groups[desc]["total_net_weight"] += float(box["net_weight"])
            if box["gross_weight"]:
                article_groups[desc]["total_gross_weight"] += float(box["gross_weight"])

        articles = list(article_groups.values())
        for article in articles:
            article["quantity_units"] = article["box_count"]
            article["net_weight"] = article["total_net_weight"]
            article["total_weight"] = article["total_gross_weight"]

    # Fetch edit logs for this transaction
    edit_logs_res = db.execute(
        text("""
            SELECT box_id, field_name, old_value, new_value, email_id, edited_at
            FROM box_edit_logs
            WHERE transaction_no = :txno
            ORDER BY edited_at DESC
        """),
        {"txno": transaction_no},
    ).fetchall()
    edit_logs = [dict(r._mapping) for r in edit_logs_res]

    return {
        "company": company,
        "transaction": transaction,
        "articles": articles,
        "boxes": boxes,
        "edit_logs": edit_logs,
    }


def update_inward(
    company: Company, transaction_no: str, payload: InwardPayloadFlexible, db: Session
) -> dict:
    tables = table_names(company)

    existing = db.execute(
        text(f"SELECT transaction_no, status FROM {tables['tx']} WHERE transaction_no = :txno"),
        {"txno": transaction_no},
    ).fetchone()
    if not existing:
        raise HTTPException(404, f"Transaction '{transaction_no}' not found")

    if payload.transaction.transaction_no != transaction_no:
        raise HTTPException(400, "Transaction number in payload must match URL parameter")

    # 1) Update transaction
    tx_data = clean_date_fields(payload.transaction.model_dump())
    tx_update_fields = []
    tx_params = {"txno": transaction_no}
    for field, value in tx_data.items():
        if field != "transaction_no":
            tx_update_fields.append(f"{field} = :{field}")
            tx_params[field] = value

    if tx_update_fields:
        db.execute(
            text(f"""
                UPDATE {tables['tx']}
                SET {', '.join(tx_update_fields)}
                WHERE transaction_no = :txno
            """),
            tx_params,
        )

    # 2) Replace articles
    db.execute(
        text(f"DELETE FROM {tables['art']} WHERE transaction_no = :txno"),
        {"txno": transaction_no},
    )
    if payload.articles:
        _ensure_skus(payload.articles, tables, db)
        articles_data = [clean_date_fields(a.model_dump()) for a in payload.articles]
        db.execute(
            text(f"INSERT INTO {tables['art']} ({_ARTICLE_COLUMNS}) VALUES ({_ARTICLE_PARAMS})"),
            articles_data,
        )

    # 3) Replace boxes — preserve existing box_id for already-printed boxes
    existing_box_ids = {}
    if payload.boxes:
        rows = db.execute(
            text(f"SELECT article_description, box_number, box_id FROM {tables['box']} WHERE transaction_no = :txno AND box_id IS NOT NULL"),
            {"txno": transaction_no},
        ).fetchall()
        existing_box_ids = {(r.article_description, r.box_number): r.box_id for r in rows}

    db.execute(
        text(f"DELETE FROM {tables['box']} WHERE transaction_no = :txno"),
        {"txno": transaction_no},
    )
    if payload.boxes:
        boxes_data = []
        for b in payload.boxes:
            d = b.model_dump()
            d["box_id"] = existing_box_ids.get((d["article_description"], d["box_number"]))
            boxes_data.append(d)
        db.execute(
            text(f"""
                INSERT INTO {tables['box']} (
                    transaction_no, article_description, box_number, net_weight, gross_weight, lot_number, count, box_id
                ) VALUES (
                    :transaction_no, :article_description, :box_number, :net_weight, :gross_weight, :lot_number, :count, :box_id
                )
            """),
            boxes_data,
        )

    db.commit()

    return {
        "status": "updated",
        "transaction_no": transaction_no,
        "company": company,
        "articles_count": len(payload.articles),
        "boxes_count": len(payload.boxes),
    }


def delete_inward(company: Company, transaction_no: str, db: Session) -> dict:
    tables = table_names(company)

    existing = db.execute(
        text(f"SELECT transaction_no FROM {tables['tx']} WHERE transaction_no = :txno"),
        {"txno": transaction_no},
    ).fetchone()
    if not existing:
        raise HTTPException(404, f"Transaction '{transaction_no}' not found")

    boxes_deleted = db.execute(
        text(f"DELETE FROM {tables['box']} WHERE transaction_no = :txno"),
        {"txno": transaction_no},
    ).rowcount

    articles_deleted = db.execute(
        text(f"DELETE FROM {tables['art']} WHERE transaction_no = :txno"),
        {"txno": transaction_no},
    ).rowcount

    transaction_deleted = db.execute(
        text(f"DELETE FROM {tables['tx']} WHERE transaction_no = :txno"),
        {"txno": transaction_no},
    ).rowcount

    db.commit()

    return {
        "status": "deleted",
        "transaction_no": transaction_no,
        "company": company,
        "deleted_counts": {
            "transaction": transaction_deleted,
            "articles": articles_deleted,
            "boxes": boxes_deleted,
        },
    }


# ---------- Approval ----------


def approve_inward(
    company: Company, transaction_no: str, payload: ApprovalRequest, db: Session
) -> dict:
    tables = table_names(company)

    existing = db.execute(
        text(f"SELECT transaction_no, status FROM {tables['tx']} WHERE transaction_no = :txno"),
        {"txno": transaction_no},
    ).fetchone()
    if not existing:
        raise HTTPException(404, f"Transaction '{transaction_no}' not found")

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # ---------- APPROVE ----------

    # 1) Update transaction fields if provided
    tx_update_parts = [
        "status = 'approved'",
        "approved_by = :approved_by",
        "approved_at = :approved_at",
    ]
    tx_params = {
        "txno": transaction_no,
        "approved_by": payload.approved_by,
        "approved_at": now,
    }

    if payload.transaction:
        tx_data = clean_date_fields(payload.transaction.model_dump(exclude_none=True))
        for field, value in tx_data.items():
            tx_update_parts.append(f"{field} = :{field}")
            tx_params[field] = value

    db.execute(
        text(f"""
            UPDATE {tables['tx']}
            SET {', '.join(tx_update_parts)}
            WHERE transaction_no = :txno
        """),
        tx_params,
    )

    # 2) Update articles if provided (merge by item_description)
    if payload.articles:
        for art in payload.articles:
            art_data = clean_date_fields(art.model_dump(exclude_none=True))
            item_desc = art_data.pop("item_description")

            if art_data:
                set_parts = [f"{k} = :{k}" for k in art_data]
                art_data["txno"] = transaction_no
                art_data["item_desc"] = item_desc
                db.execute(
                    text(f"""
                        UPDATE {tables['art']}
                        SET {', '.join(set_parts)}
                        WHERE transaction_no = :txno AND item_description = :item_desc
                    """),
                    art_data,
                )

    # 3) Upsert boxes if provided (preserve existing box_ids from printed boxes)
    if payload.boxes:
        for b in payload.boxes:
            box_params = {
                "txno": transaction_no,
                "art_desc": b.article_description,
                "box_num": b.box_number,
                "net_weight": b.net_weight,
                "gross_weight": b.gross_weight,
                "lot_number": b.lot_number,
                "count": b.count,
            }
            existing = db.execute(
                text(f"""
                    SELECT box_id FROM {tables['box']}
                    WHERE transaction_no = :txno
                      AND article_description = :art_desc AND box_number = :box_num
                """),
                box_params,
            ).fetchone()

            if existing:
                # Update without overwriting box_id
                db.execute(
                    text(f"""
                        UPDATE {tables['box']}
                        SET net_weight = :net_weight, gross_weight = :gross_weight,
                            lot_number = :lot_number, count = :count
                        WHERE transaction_no = :txno
                          AND article_description = :art_desc AND box_number = :box_num
                    """),
                    box_params,
                )
            else:
                # New box — no box_id yet (assigned when approver prints)
                db.execute(
                    text(f"""
                        INSERT INTO {tables['box']} (
                            transaction_no, article_description, box_number,
                            net_weight, gross_weight, lot_number, count
                        ) VALUES (
                            :txno, :art_desc, :box_num,
                            :net_weight, :gross_weight, :lot_number, :count
                        )
                    """),
                    box_params,
                )

    db.commit()

    return {
        "status": "approved",
        "transaction_no": transaction_no,
        "company": company,
        "approved_by": payload.approved_by,
        "approved_at": now,
    }


# ---------- Box Upsert + Edit Logging ----------


def upsert_box(
    company: Company, transaction_no: str, payload: BoxUpsertRequest, db: Session
) -> BoxUpsertResponse:
    tables = table_names(company)

    # Verify transaction exists
    existing_tx = db.execute(
        text(f"SELECT transaction_no FROM {tables['tx']} WHERE transaction_no = :txno"),
        {"txno": transaction_no},
    ).fetchone()
    if not existing_tx:
        raise HTTPException(404, f"Transaction '{transaction_no}' not found")

    # Check if box already exists
    existing_box = db.execute(
        text(f"""
            SELECT id, box_id FROM {tables['box']}
            WHERE transaction_no = :txno
              AND article_description = :art_desc
              AND box_number = :box_num
        """),
        {"txno": transaction_no, "art_desc": payload.article_description, "box_num": payload.box_number},
    ).fetchone()

    params = {
        "txno": transaction_no,
        "art_desc": payload.article_description,
        "box_num": payload.box_number,
        "net_weight": payload.net_weight,
        "gross_weight": payload.gross_weight,
        "lot_number": payload.lot_number,
        "count": payload.count,
    }

    if existing_box and existing_box.box_id:
        # UPDATE existing row, preserve box_id
        db.execute(
            text(f"""
                UPDATE {tables['box']}
                SET net_weight = :net_weight, gross_weight = :gross_weight,
                    lot_number = :lot_number, count = :count
                WHERE transaction_no = :txno
                  AND article_description = :art_desc AND box_number = :box_num
            """),
            params,
        )
        box_id = existing_box.box_id
        status = "updated"
    else:
        # Generate new box_id
        base = str(int(time.time() * 1000))[-8:]
        box_id = f"{base}-{payload.box_number}"
        params["box_id"] = box_id

        if existing_box:
            # Row exists without box_id — update it
            db.execute(
                text(f"""
                    UPDATE {tables['box']}
                    SET net_weight = :net_weight, gross_weight = :gross_weight,
                        lot_number = :lot_number, count = :count, box_id = :box_id
                    WHERE transaction_no = :txno
                      AND article_description = :art_desc AND box_number = :box_num
                """),
                params,
            )
        else:
            # Fresh insert
            db.execute(
                text(f"""
                    INSERT INTO {tables['box']} (
                        transaction_no, article_description, box_number,
                        net_weight, gross_weight, lot_number, count, box_id
                    ) VALUES (
                        :txno, :art_desc, :box_num,
                        :net_weight, :gross_weight, :lot_number, :count, :box_id
                    )
                """),
                params,
            )
        status = "inserted"

    db.commit()

    return BoxUpsertResponse(
        status=status,
        box_id=box_id,
        transaction_no=transaction_no,
        article_description=payload.article_description,
        box_number=payload.box_number,
    )


def log_box_edits(payload: BoxEditLogRequest, db: Session) -> dict:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    for change in payload.changes:
        description = f"Changed {change.field_name} from '{change.old_value}' to '{change.new_value}'"
        db.execute(
            text("""
                INSERT INTO box_edit_logs (email_id, description, transaction_no, box_id, field_name, old_value, new_value, edited_at)
                VALUES (:email_id, :description, :txno, :box_id, :field_name, :old_value, :new_value, :edited_at)
            """),
            {
                "email_id": payload.email_id,
                "description": description,
                "txno": payload.transaction_no,
                "box_id": payload.box_id,
                "field_name": change.field_name,
                "old_value": change.old_value,
                "new_value": change.new_value,
                "edited_at": now,
            },
        )
    db.commit()
    return {"status": "logged", "entries": len(payload.changes)}


# ---------- PO PDF Extraction ----------


def _split_pdf_pages(file_bytes: bytes) -> list[bytes]:
    """Split a PDF into single-page PDFs using PyMuPDF."""
    import fitz  # pymupdf

    doc = fitz.open(stream=file_bytes, filetype="pdf")
    pages = []
    for page_num in range(len(doc)):
        single = fitz.open()
        single.insert_pdf(doc, from_page=page_num, to_page=page_num)
        pages.append(single.tobytes())
        single.close()
    doc.close()
    return pages


_PO_EXTRACT_PROMPT = (
    "This page is from a Purchase Order PDF. "
    "Extract ALL Purchase Orders found and return ONLY valid JSON "
    "(no markdown, no code fences, no explanation).\n\n"
    "Return this exact structure:\n"
    "{\n"
    '  "purchase_orders": [\n'
    "    {\n"
    '      "supplier_name": "vendor/supplier name",\n'
    '      "source_location": "supplier address or city",\n'
    '      "customer_name": "buyer/customer name",\n'
    '      "destination_location": "delivery address or city",\n'
    '      "po_number": "PO number",\n'
    '      "purchased_by": "indentor or purchaser name",\n'
    '      "total_amount": numeric or null,\n'
    '      "tax_amount": numeric or null,\n'
    '      "discount_amount": numeric or null,\n'
    '      "po_quantity": total quantity in kgs (numeric or null),\n'
    '      "currency": "INR" or other currency code,\n'
    '      "articles": [\n'
    "        {\n"
    '          "item_description": "product/item name ONLY",\n'
    '          "po_weight": weight in kgs (numeric or null),\n'
    '          "unit_rate": rate per unit (numeric or null),\n'
    '          "total_amount": line total amount (numeric or null)\n'
    "        }\n"
    "      ]\n"
    "    }\n"
    "  ]\n"
    "}\n\n"
    "IMPORTANT RULES:\n"
    "- Each distinct PO number must be a separate entry in the purchase_orders array.\n"
    "- If only one PO is found, still wrap it in the purchase_orders array.\n"
    "- Group articles under their respective PO.\n"
    "- If a field is not found, set it to null.\n"
    "- Return ONLY the JSON object.\n"
    "- A single PO may span multiple pages. If this page is a CONTINUATION of a PO "
    "(e.g. more line items from the same PO, or the totals/footer of a PO), "
    "still extract it with the same po_number and any articles found on this page.\n"
    "- If the page contains ONLY Terms & Conditions, Annexures, signatures, or other "
    "non-PO content with NO line items and NO PO header, return: "
    '{\"purchase_orders\": []}\n\n'
    "CRITICAL RULES FOR ARTICLES:\n"
    "- Each line item / row in the PO table MUST be a SEPARATE article object. "
    "NEVER combine multiple items into one article.\n"
    '- "item_description" must contain the EXACT product/item name as written in the Description column, '
    "including any numbers, grades, pack sizes, or variant codes that are part of the name "
    "(e.g. \"Cashew 210\", \"SOYA NUGGETS 570\", \"Afghan Black Raisins Seedless 1*2\", "
    "\"American Almonds Running 25-27 count\", \"AL BARAKAH BARHI DATES 10KG\", "
    "\"Soy Isolate Protein (Supro 620)\"). "
    "Do NOT include the separate column values (Qty, UOM, Unit Price, Taxable Value, GST%, HSN Code) "
    "in item_description — those belong in their own fields (po_weight, unit_rate, total_amount).\n"
    "- Examples of CORRECT item_description: \"California Pista Inshell\", \"Cashew 210\", "
    "\"Afghan Black Raisins Seedless 1*2\", \"SOYA NUGGETS 570\"\n"
    "- Examples of WRONG item_description: \"California Pista Inshell 43000 LB, RATE $4.29\", "
    "\"Cashew 210 W210\", \"PVC Pipe 100 meters @ Rs 250\"\n"
    "- Extract EVERY line item from the PO — do not skip or merge any rows.\n"
    "- NEVER use newline characters in item_description. If the product name or pack size "
    "(e.g. \"1*2\", \"10KG\", \"25-27 count\") appears on a separate line in the PDF, "
    "join it with the main description using a space.\n"
    "- If a single row has a long description with sub-items separated by commas or line breaks, "
    "split them into separate article objects.\n"
    "- IMPORTANT: In these POs, each line item has a MAIN description in the Description column, "
    "and sometimes an additional sub-description / variant / notes line BELOW the HSN code row "
    "(e.g. the main description is \"AL BARAKAH FARD DATES STANDARD\" and below the HSN row "
    "there is \"FARD C DATES\"; or the main description is \"Cashew 210\" and below the HSN "
    "row there is \"W210\"; or \"Peri Peri Liquid\" with \"PERI PERI FLAVOUR LIQUID MF2007\" below; "
    "or \"California Inshell Almonds\" with \"03781 NONP INSHELL ALMONDS 50 LB BAG\" below; "
    "or \"Utility Wipes\" with \"Kitchen Roll 2Kg\" below). "
    "For item_description, use ONLY the main description line. "
    "Do NOT include the sub-description, variant code, or notes line. "
    "Ignore any extra text that appears below the HSN code for each line item."
)


def _call_claude_extract(client, pdf_b64: str) -> dict:
    """Call Claude API with retry for a single PDF page."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            message = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=4096,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "document",
                                "source": {
                                    "type": "base64",
                                    "media_type": "application/pdf",
                                    "data": pdf_b64,
                                },
                            },
                            {"type": "text", "text": _PO_EXTRACT_PROMPT},
                        ],
                    }
                ],
            )
            break
        except anthropic.RateLimitError:
            if attempt < max_retries - 1:
                wait = (attempt + 1) * 10
                logger.warning(f"Rate limited, retrying in {wait}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)
            else:
                raise HTTPException(429, "API rate limit exceeded. Please try again in a minute.")

    raw_text = message.content[0].text.strip()

    if raw_text.startswith("```"):
        raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text)
        raw_text = re.sub(r"\s*```$", "", raw_text)

    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        logger.error(f"Claude returned invalid JSON: {raw_text}")
        raise HTTPException(422, "Failed to parse extraction result from Claude")

    if "purchase_orders" not in parsed:
        parsed = {"purchase_orders": [parsed]}

    return parsed


def extract_po_from_pdf(file_bytes: bytes) -> dict:
    """Split PDF by page, send each page to Claude, merge all POs."""
    if not _extraction_lock.acquire(blocking=False):
        raise HTTPException(
            429,
            "Another PO extraction is already in progress. Please wait and try again.",
        )
    try:
        return _extract_po_from_pdf_inner(file_bytes)
    finally:
        _extraction_lock.release()


def _extract_po_from_pdf_inner(file_bytes: bytes) -> dict:
    """Internal extraction logic (called under lock)."""
    client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

    pages = _split_pdf_pages(file_bytes)
    logger.info(f"PDF has {len(pages)} page(s), extracting POs per page")

    all_pos: list[dict] = []
    seen_po_numbers: dict[str, int] = {}  # po_number -> index in all_pos

    # Pacing: after every 120s of continuous API calls, pause 5s to avoid rate limits
    batch_start = time.time()
    PACE_WINDOW = 120  # seconds
    PACE_PAUSE = 5     # seconds

    for i, page_bytes in enumerate(pages):
        # Check if we've been running for 120s — if so, cool down
        elapsed = time.time() - batch_start
        if elapsed >= PACE_WINDOW:
            logger.info(f"Pacing: {elapsed:.0f}s elapsed, pausing {PACE_PAUSE}s before page {i + 1}")
            time.sleep(PACE_PAUSE)
            batch_start = time.time()

        page_b64 = base64.standard_b64encode(page_bytes).decode("utf-8")

        logger.info(f"Extracting page {i + 1}/{len(pages)}")
        result = _call_claude_extract(client, page_b64)

        page_pos = result.get("purchase_orders", [])

        # Skip empty results (T&C pages, annexures, signatures, etc.)
        if not page_pos:
            logger.info(f"Page {i + 1}: no POs found (likely T&C / annexure page), skipping")
            continue

        for po in page_pos:
            po_num = (po.get("po_number") or "").strip() or None
            articles = po.get("articles") or []

            # Skip noise entries: no PO number AND no articles
            if not po_num and not articles:
                logger.info(f"Page {i + 1}: skipping entry with no PO number and no articles")
                continue

            if po_num and po_num in seen_po_numbers:
                # Same PO number seen before — merge articles & fill missing fields
                existing = all_pos[seen_po_numbers[po_num]]
                existing.setdefault("articles", []).extend(articles)
                # Fill in any fields that were null in the first page but present in continuation
                for field in ("supplier_name", "customer_name", "source_location",
                              "destination_location", "purchased_by", "currency",
                              "total_amount", "tax_amount", "discount_amount", "po_quantity"):
                    if not existing.get(field) and po.get(field):
                        existing[field] = po[field]
                logger.info(f"Page {i + 1}: merged {len(articles)} article(s) into existing PO {po_num}")
            elif po_num:
                # New PO with a PO number
                seen_po_numbers[po_num] = len(all_pos)
                all_pos.append(po)
            else:
                # No PO number — this is likely a continuation page of the previous PO
                if all_pos:
                    prev = all_pos[-1]
                    prev.setdefault("articles", []).extend(articles)
                    for field in ("supplier_name", "customer_name", "source_location",
                                  "destination_location", "purchased_by", "currency",
                                  "total_amount", "tax_amount", "discount_amount", "po_quantity"):
                        if not prev.get(field) and po.get(field):
                            prev[field] = po[field]
                    logger.info(f"Page {i + 1}: no PO number, appended {len(articles)} article(s) to previous PO")
                else:
                    # No previous PO to attach to — add as standalone
                    all_pos.append(po)
                    logger.info(f"Page {i + 1}: no PO number and no previous PO, added as new entry")

    if not all_pos:
        raise HTTPException(422, "No purchase orders could be extracted from the PDF")

    # Clean up item_description: join multi-line names and strip "desc:" suffixes.
    # Claude may split pack sizes like "1*2" onto a second line — join with space
    # so "Afghan Black Raisins Seedless\n1*2" becomes "Afghan Black Raisins Seedless 1*2".
    # Also strip "desc:" or "Desc:" suffixes if Claude appended a sub-description.
    for po in all_pos:
        for article in po.get("articles", []):
            desc = article.get("item_description", "")
            if desc:
                # Replace newlines with spaces to preserve pack sizes (e.g. "1*2")
                cleaned = " ".join(desc.split())
                # Strip "desc:" or "Desc:" suffixes if Claude formatted it that way
                cleaned = re.split(r"\s*desc\s*:\s*", cleaned, maxsplit=1, flags=re.IGNORECASE)[0].strip()
                if cleaned:
                    article["item_description"] = cleaned

    return {"purchase_orders": all_pos}


# ---------- SKU Lookup ----------


def lookup_sku(item_description: str, company: Company, db: Session) -> dict | None:
    """Lookup SKU by item_description and return sku_id, material_type, item_category, sub_category."""
    tables = table_names(company)

    row = db.execute(
        text(f"""
            SELECT id, item_description, material_type, item_category, sub_category
            FROM {tables['sku']}
            WHERE item_description ILIKE :desc
            LIMIT 1
        """),
        {"desc": item_description},
    ).mappings().first()

    if not row:
        return None

    return {
        "sku_id": row["id"],
        "item_description": row["item_description"],
        "material_type": row["material_type"],
        "item_category": row["item_category"],
        "sub_category": row["sub_category"],
    }


# ---------- Internal helpers ----------


def _ensure_skus(articles, tables: dict, db: Session) -> None:
    """Auto-create missing SKUs in the sku table."""
    sku_ids = {a.sku_id for a in articles if a.sku_id is not None}
    if not sku_ids:
        return

    sku_sql = text(f"SELECT id FROM {tables['sku']} WHERE id IN :ids").bindparams(
        bindparam("ids", expanding=True)
    )
    found = {row[0] for row in db.execute(sku_sql, {"ids": list(sku_ids)})}
    missing = sku_ids - found

    for missing_id in missing:
        article = next(a for a in articles if a.sku_id == missing_id)
        db.execute(
            text(f"""
                INSERT INTO {tables['sku']} (id, item_description, item_category, sub_category)
                VALUES (:id, :item_description, :item_category, :sub_category)
                ON CONFLICT (id) DO UPDATE SET
                    item_description = EXCLUDED.item_description,
                    item_category = EXCLUDED.item_category,
                    sub_category = EXCLUDED.sub_category
            """),
            {
                "id": missing_id,
                "item_description": article.item_description,
                "item_category": article.item_category or "",
                "sub_category": article.sub_category or "",
            },
        )
        logger.info(f"Auto-created SKU {missing_id} for item: {article.item_description}")


# ---------- SKU Dropdown / Search / ID ----------


def get_vendors(search: Optional[str], db: Session) -> list[dict]:
    """Return vendors from the vendors table, optionally filtered by name."""
    where = "WHERE vendor_name IS NOT NULL"
    params: dict = {}
    if search and search.strip():
        where += " AND vendor_name ILIKE :search"
        params["search"] = f"%{search.strip()}%"

    rows = db.execute(
        text(f"""
            SELECT id, vendor_name, location
            FROM vendors
            {where}
            ORDER BY vendor_name ASC
            LIMIT 1500
        """),
        params,
    ).fetchall()

    return [
        {"id": r[0], "vendor_name": r[1], "location": r[2]}
        for r in rows
    ]


def sku_dropdown(
    company: Company,
    material_type: Optional[str],
    item_category: Optional[str],
    sub_category: Optional[str],
    item_description: Optional[str],
    search: Optional[str],
    limit: int,
    offset: int,
    db: Session,
) -> SKUDropdownResponse:
    """Cascading SKU dropdown: material_type -> item_category -> sub_category -> item_description."""
    tbl = table_names(company)["sku"]

    material_type = material_type.strip() if material_type else None
    item_category = item_category.strip() if item_category else None
    sub_category = sub_category.strip() if sub_category else None
    item_description = item_description.strip() if item_description else None
    search = search.strip() if search else None

    # 1) All material types (unfiltered)
    material_types = db.execute(
        text(f"""
            SELECT DISTINCT material_type FROM {tbl}
            WHERE material_type IS NOT NULL
            ORDER BY material_type ASC
        """)
    ).scalars().all()

    # 2) Item categories (filtered by material_type)
    item_categories = []
    if material_type:
        item_categories = db.execute(
            text(f"""
                SELECT DISTINCT item_category FROM {tbl}
                WHERE UPPER(material_type) = UPPER(:mt) AND item_category IS NOT NULL
                ORDER BY item_category ASC
            """),
            {"mt": material_type},
        ).scalars().all()

    # 3) Sub categories (filtered by material_type + item_category)
    sub_categories = []
    if material_type and item_category:
        sub_categories = db.execute(
            text(f"""
                SELECT DISTINCT sub_category FROM {tbl}
                WHERE UPPER(material_type) = UPPER(:mt)
                  AND UPPER(item_category) = UPPER(:ic)
                  AND sub_category IS NOT NULL
                ORDER BY sub_category ASC
            """),
            {"mt": material_type, "ic": item_category},
        ).scalars().all()

    # 4) Item descriptions + IDs (filtered by full hierarchy)
    item_descs: list[str] = []
    item_ids: list[int] = []
    total_item_descriptions = 0

    if material_type and item_category and sub_category:
        where = [
            "UPPER(material_type) = UPPER(:mt)",
            "UPPER(item_category) = UPPER(:ic)",
            "UPPER(sub_category) = UPPER(:sc)",
        ]
        params: dict = {"mt": material_type, "ic": item_category, "sc": sub_category}

        if search:
            where.append("LOWER(item_description) LIKE :search")
            params["search"] = f"%{search.lower()}%"

        where_sql = " AND ".join(where)

        total_item_descriptions = db.execute(
            text(f"SELECT COUNT(DISTINCT item_description) FROM {tbl} WHERE {where_sql}"),
            params,
        ).scalar_one()

        rows = db.execute(
            text(f"""
                SELECT DISTINCT id, item_description FROM {tbl}
                WHERE {where_sql} AND item_description IS NOT NULL
                ORDER BY item_description ASC
                LIMIT :limit OFFSET :offset
            """),
            {**params, "limit": limit, "offset": offset},
        ).fetchall()

        item_ids = [r[0] for r in rows]
        item_descs = [r[1] for r in rows]

    # 5) Auto-resolve from item_description
    resolved = SKUResolvedFromItem()
    if item_description:
        row = db.execute(
            text(f"""
                SELECT material_type, item_category, sub_category FROM {tbl}
                WHERE item_description = :desc
                LIMIT 1
            """),
            {"desc": item_description},
        ).fetchone()
        if row:
            resolved = SKUResolvedFromItem(
                material_type=row[0], item_category=row[1], sub_category=row[2]
            )

    return SKUDropdownResponse(
        company=company,
        selected=SKUDropdownSelectedState(
            material_type=material_type,
            item_description=item_description,
            item_category=item_category,
            sub_category=sub_category,
        ),
        auto_selection={"resolved_from_item": resolved.model_dump()},
        options=SKUDropdownOptions(
            material_types=material_types,
            item_categories=item_categories,
            sub_categories=sub_categories,
            item_descriptions=item_descs,
            item_ids=item_ids,
        ),
        meta=SKUDropdownMeta(
            total_material_types=len(material_types),
            total_item_descriptions=total_item_descriptions,
            total_categories=len(item_categories),
            total_sub_categories=len(sub_categories),
            limit=limit,
            offset=offset,
            sort="alpha",
            search=search,
        ),
    )


def sku_global_search(
    company: Company,
    search: Optional[str],
    limit: int,
    offset: int,
    db: Session,
) -> SKUGlobalSearchResponse:
    """Global item description search — bypasses hierarchy."""
    tbl = table_names(company)["sku"]
    search_term = search.strip() if search else None

    where_clauses = ["1=1"]
    params: dict = {}

    if search_term:
        where_clauses.append("LOWER(item_description) LIKE :search")
        params["search"] = f"%{search_term.lower()}%"

    where_sql = " AND ".join(where_clauses)

    total_items = db.execute(
        text(f"SELECT COUNT(DISTINCT item_description) FROM {tbl} WHERE {where_sql}"),
        params,
    ).scalar_one()

    rows = db.execute(
        text(f"""
            SELECT DISTINCT id, item_description, material_type, item_category, sub_category
            FROM {tbl}
            WHERE {where_sql}
            ORDER BY item_description ASC
            LIMIT :limit OFFSET :offset
        """),
        {**params, "limit": limit, "offset": offset},
    ).fetchall()

    items = [
        SKUGlobalSearchItem(
            id=r[0],
            item_description=r[1],
            material_type=r[2],
            group=r[3],
            sub_group=r[4],
        )
        for r in rows
    ]

    return SKUGlobalSearchResponse(
        company=company,
        items=items,
        meta={
            "total_items": total_items,
            "limit": limit,
            "offset": offset,
            "search": search_term,
            "has_more": (offset + limit) < total_items,
        },
    )


def sku_id_lookup(
    company: Company,
    item_description: str,
    item_category: Optional[str],
    sub_category: Optional[str],
    material_type: Optional[str],
    db: Session,
) -> SKUIdResponse:
    """Get SKU ID for a specific item description (case-insensitive)."""
    tbl = table_names(company)["sku"]

    # Handle "other" in any field — return null sku_id
    fields = [item_description, item_category, sub_category, material_type]
    if any(f and f.strip().lower() == "other" for f in fields):
        return SKUIdResponse(
            sku_id=None,
            id=None,
            item_description=item_description,
            material_type=material_type,
            group=item_category,
            sub_group=sub_category,
            item_category=item_category,
            sub_category=sub_category,
            company=company,
        )

    where_clauses = ["UPPER(item_description) = UPPER(:desc)"]
    params: dict = {"desc": item_description}

    if material_type:
        where_clauses.append("UPPER(material_type) = UPPER(:mt)")
        params["mt"] = material_type
    if item_category:
        where_clauses.append("UPPER(item_category) = UPPER(:ic)")
        params["ic"] = item_category
    if sub_category:
        where_clauses.append("UPPER(sub_category) = UPPER(:sc)")
        params["sc"] = sub_category

    where_sql = " AND ".join(where_clauses)

    row = db.execute(
        text(f"""
            SELECT id, item_description, material_type, item_category, sub_category
            FROM {tbl}
            WHERE {where_sql}
            LIMIT 1
        """),
        params,
    ).fetchone()

    if not row:
        raise HTTPException(404, f"SKU not found for item_description: {item_description}")

    return SKUIdResponse(
        sku_id=row[0],
        id=row[0],
        item_description=row[1],
        material_type=row[2],
        group=row[3],
        sub_group=row[4],
        item_category=row[3],
        sub_category=row[4],
        company=company,
    )
