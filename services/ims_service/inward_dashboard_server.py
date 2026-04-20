"""
Inward Dashboard API v2 — All data loaded once, client-side filtering.
Uses warehouse field (not destination_location) for warehouse grouping.
Only transactions with articles (actual entries) are included.
"""

from typing import Optional
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.database import get_db
from shared.logger import get_logger

logger = get_logger("inward_dashboard")

router = APIRouter(prefix="/inward-dashboard", tags=["Inward Dashboard"])


def _tbl(company: str):
    p = company.strip().lower()
    if p not in ("cfpl", "cdpl"):
        raise HTTPException(400, f"Unknown company: {company}")
    return {
        "tx": f"{p}_transactions_v2",
        "art": f"{p}_articles_v2",
    }


def _f(v):
    return round(float(v), 2) if v else 0.0


# Only include article rows with actual entry data (skip PO-only uploads)
ENTRY_FILTER = """(
    COALESCE(a.total_weight, 0) > 0
    OR COALESCE(a.net_weight, 0) > 0
    OR COALESCE(a.quantity_units, 0) > 0
    OR COALESCE(a.total_amount, 0) > 0
)"""


# ═══════════════════════════════════════════════════════════════════
# Main data endpoint — loads ALL inward records with articles joined
# Client does filtering/grouping/KPIs from this data
# ═══════════════════════════════════════════════════════════════════

@router.get("/all-data")
async def get_all_data(
    company: str = Query(...),
    db: Session = Depends(get_db),
):
    """
    Returns all inward transactions joined with articles.
    Only transactions that have at least one article entry are included.
    Warehouse = t.warehouse field (W202, A185, A68 etc.), NOT destination_location.
    """
    tbl = _tbl(company)

    try:
        sql = text(f"""
            SELECT
                t.transaction_no,
                COALESCE(t.entry_date, t.system_grn_date)::text AS entry_date,
                TO_CHAR(COALESCE(t.entry_date, t.system_grn_date), 'YYYY-MM') AS entry_month,
                COALESCE(t.warehouse, '') AS warehouse,
                COALESCE(t.vendor_supplier_name, '') AS vendor,
                COALESCE(t.customer_party_name, '') AS customer,
                COALESCE(t.status, 'pending') AS status,
                COALESCE(t.invoice_number, '') AS invoice_number,
                COALESCE(t.po_number, '') AS po_number,
                COALESCE(t.purchased_by, '') AS purchased_by,
                COALESCE(t.grn_number, '') AS grn_number,
                COALESCE(a.item_description, '') AS item_description,
                a.sku_id,
                COALESCE(a.item_category, '') AS item_category,
                COALESCE(a.sub_category, '') AS sub_category,
                COALESCE(a.material_type, '') AS material_type,
                COALESCE(a.quality_grade, '') AS quality_grade,
                COALESCE(a.uom, '') AS uom,
                COALESCE(a.lot_number, '') AS lot_number,
                COALESCE(a.quantity_units, 0) AS qty,
                COALESCE(a.net_weight, 0) AS net_weight,
                COALESCE(a.total_weight, 0) AS total_weight,
                COALESCE(a.unit_rate, 0) AS unit_rate,
                COALESCE(a.total_amount, 0) AS total_amount
            FROM {tbl['tx']} t
            INNER JOIN {tbl['art']} a ON t.transaction_no = a.transaction_no
            WHERE {ENTRY_FILTER}
            ORDER BY COALESCE(t.entry_date, t.system_grn_date) DESC NULLS LAST
        """)
        rows = db.execute(sql).fetchall()
        cols = rows[0]._fields if rows else []

        NUMERIC_COLS = {"qty", "net_weight", "total_weight", "unit_rate", "total_amount"}
        INT_COLS = {"sku_id"}

        records = []
        for r in rows:
            rec = {}
            for c in cols:
                v = getattr(r, c)
                if v is None:
                    rec[c] = 0 if c in NUMERIC_COLS or c in INT_COLS else ""
                elif c in NUMERIC_COLS:
                    rec[c] = round(float(v), 2)
                elif c in INT_COLS:
                    rec[c] = int(v)
                else:
                    rec[c] = str(v)
            records.append(rec)

        return {"records": records, "total": len(records), "as_of_date": date.today().isoformat()}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("All data error: %s", e)
        raise HTTPException(500, str(e))


# ═══════════════════════════════════════════════════════════════════
# Filter options — distinct values for dropdowns/chips
# ═══════════════════════════════════════════════════════════════════

@router.get("/filter-options")
async def get_filter_options(
    company: str = Query(...),
    db: Session = Depends(get_db),
):
    tbl = _tbl(company)
    try:
        result: dict = {}

        # All filter queries use ENTRY_FILTER to skip PO-only rows
        wh = db.execute(text(f"""
            SELECT DISTINCT t.warehouse AS wh, COUNT(DISTINCT t.transaction_no) AS cnt
            FROM {tbl['tx']} t INNER JOIN {tbl['art']} a ON t.transaction_no = a.transaction_no
            WHERE t.warehouse IS NOT NULL AND t.warehouse != '' AND {ENTRY_FILTER}
            GROUP BY t.warehouse ORDER BY cnt DESC
        """)).fetchall()
        result["warehouses"] = [{"name": r.wh, "count": int(r.cnt)} for r in wh]

        vnd = db.execute(text(f"""
            SELECT DISTINCT t.vendor_supplier_name AS v, COUNT(DISTINCT t.transaction_no) AS cnt
            FROM {tbl['tx']} t INNER JOIN {tbl['art']} a ON t.transaction_no = a.transaction_no
            WHERE t.vendor_supplier_name IS NOT NULL AND t.vendor_supplier_name != '' AND {ENTRY_FILTER}
            GROUP BY t.vendor_supplier_name ORDER BY cnt DESC
        """)).fetchall()
        result["vendors"] = [{"name": r.v, "count": int(r.cnt)} for r in vnd]

        cst = db.execute(text(f"""
            SELECT DISTINCT t.customer_party_name AS c, COUNT(DISTINCT t.transaction_no) AS cnt
            FROM {tbl['tx']} t INNER JOIN {tbl['art']} a ON t.transaction_no = a.transaction_no
            WHERE t.customer_party_name IS NOT NULL AND t.customer_party_name != '' AND {ENTRY_FILTER}
            GROUP BY t.customer_party_name ORDER BY cnt DESC
        """)).fetchall()
        result["customers"] = [{"name": r.c, "count": int(r.cnt)} for r in cst]

        result["item_categories"] = [r[0] for r in db.execute(text(f"""
            SELECT DISTINCT a.item_category FROM {tbl['art']} a
            WHERE a.item_category IS NOT NULL AND a.item_category != '' AND {ENTRY_FILTER}
            ORDER BY a.item_category
        """)).fetchall()]

        result["sub_categories"] = [r[0] for r in db.execute(text(f"""
            SELECT DISTINCT a.sub_category FROM {tbl['art']} a
            WHERE a.sub_category IS NOT NULL AND a.sub_category != '' AND {ENTRY_FILTER}
            ORDER BY a.sub_category
        """)).fetchall()]

        result["material_types"] = [r[0] for r in db.execute(text(f"""
            SELECT DISTINCT a.material_type FROM {tbl['art']} a
            WHERE a.material_type IS NOT NULL AND a.material_type != '' AND {ENTRY_FILTER}
            ORDER BY a.material_type
        """)).fetchall()]

        result["statuses"] = [r[0] for r in db.execute(text(f"""
            SELECT DISTINCT t.status FROM {tbl['tx']} t
            INNER JOIN {tbl['art']} a ON t.transaction_no = a.transaction_no
            WHERE t.status IS NOT NULL AND {ENTRY_FILTER}
            ORDER BY t.status
        """)).fetchall()]

        result["purchased_by"] = [r[0] for r in db.execute(text(f"""
            SELECT DISTINCT t.purchased_by FROM {tbl['tx']} t
            INNER JOIN {tbl['art']} a ON t.transaction_no = a.transaction_no
            WHERE t.purchased_by IS NOT NULL AND t.purchased_by != '' AND {ENTRY_FILTER}
            ORDER BY t.purchased_by
        """)).fetchall()]

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Filter options error: %s", e)
        raise HTTPException(500, str(e))


# ═══════════════════════════════════════════════════════════════════
# Item History — full history ignoring filters
# ═══════════════════════════════════════════════════════════════════

@router.get("/item-history")
async def get_item_history(
    company: str = Query(...),
    item_description: str = Query(...),
    db: Session = Depends(get_db),
):
    tbl = _tbl(company)
    try:
        inward_sql = text(f"""
            SELECT t.transaction_no, COALESCE(t.entry_date, t.system_grn_date)::text AS entry_date,
                t.vendor_supplier_name AS vendor, a.lot_number,
                COALESCE(a.quantity_units, 0) AS qty, COALESCE(a.total_weight, 0) AS weight,
                COALESCE(a.unit_rate, 0) AS rate,
                COALESCE(t.warehouse, '') AS warehouse, t.status
            FROM {tbl['tx']} t
            JOIN {tbl['art']} a ON t.transaction_no = a.transaction_no
            WHERE a.item_description = :item
            ORDER BY COALESCE(t.entry_date, t.system_grn_date) ASC
        """)
        rows = db.execute(inward_sql, {"item": item_description}).fetchall()

        vendor_sql = text(f"""
            SELECT t.vendor_supplier_name AS vendor,
                COUNT(DISTINCT t.transaction_no) AS inward_count,
                SUM(COALESCE(a.quantity_units, 0)) AS total_qty,
                SUM(COALESCE(a.total_weight, 0)) AS total_weight,
                SUM(COALESCE(a.total_amount, 0)) AS total_value,
                MAX(COALESCE(t.entry_date, t.system_grn_date))::text AS last_supply
            FROM {tbl['tx']} t
            JOIN {tbl['art']} a ON t.transaction_no = a.transaction_no
            WHERE a.item_description = :item AND t.vendor_supplier_name IS NOT NULL
            GROUP BY t.vendor_supplier_name ORDER BY SUM(COALESCE(a.total_amount, 0)) DESC
        """)
        vendor_rows = db.execute(vendor_sql, {"item": item_description}).fetchall()

        return {
            "item_description": item_description,
            "total_inwards": len(rows),
            "total_qty": round(sum(float(r.qty or 0) for r in rows), 2),
            "total_weight": round(sum(float(r.weight or 0) for r in rows), 2),
            "first_date": rows[0].entry_date if rows else None,
            "last_date": rows[-1].entry_date if rows else None,
            "inward_timeline": [{
                "transaction_no": r.transaction_no,
                "entry_date": r.entry_date,
                "vendor": r.vendor or "", "lot_number": r.lot_number or "",
                "qty": _f(r.qty), "weight": _f(r.weight), "rate": _f(r.rate),
                "warehouse": r.warehouse, "status": r.status or "",
            } for r in rows],
            "vendor_history": [{
                "vendor": r.vendor, "inward_count": int(r.inward_count or 0),
                "total_qty": _f(r.total_qty), "total_weight": _f(r.total_weight),
                "total_value": _f(r.total_value),
                "avg_rate": round(_f(r.total_value) / _f(r.total_weight), 2) if _f(r.total_weight) > 0 else 0,
                "last_supply": r.last_supply,
            } for r in vendor_rows],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Item history error: %s", e)
        raise HTTPException(500, str(e))


# ═══════════════════════════════════════════════════════════════════
# Vendor History
# ═══════════════════════════════════════════════════════════════════

@router.get("/vendor-history")
async def get_vendor_history(
    company: str = Query(...),
    vendor_name: str = Query(...),
    db: Session = Depends(get_db),
):
    tbl = _tbl(company)
    try:
        item_sql = text(f"""
            SELECT a.item_description,
                COUNT(DISTINCT t.transaction_no) AS inward_count,
                SUM(COALESCE(a.quantity_units, 0)) AS total_qty,
                SUM(COALESCE(a.total_weight, 0)) AS total_weight,
                SUM(COALESCE(a.total_amount, 0)) AS total_value,
                MAX(COALESCE(t.entry_date, t.system_grn_date))::text AS last_inward
            FROM {tbl['tx']} t JOIN {tbl['art']} a ON t.transaction_no = a.transaction_no
            WHERE t.vendor_supplier_name = :vendor
            GROUP BY a.item_description ORDER BY SUM(COALESCE(a.total_amount, 0)) DESC
        """)
        item_rows = db.execute(item_sql, {"vendor": vendor_name}).fetchall()

        monthly_sql = text(f"""
            SELECT TO_CHAR(COALESCE(t.entry_date, t.system_grn_date), 'YYYY-MM') AS month,
                TO_CHAR(COALESCE(t.entry_date, t.system_grn_date), 'Mon YYYY') AS month_label,
                COUNT(DISTINCT t.transaction_no) AS inward_count,
                SUM(COALESCE(a.total_weight, 0)) AS total_weight,
                SUM(COALESCE(a.total_amount, 0)) AS total_value
            FROM {tbl['tx']} t JOIN {tbl['art']} a ON t.transaction_no = a.transaction_no
            WHERE t.vendor_supplier_name = :vendor
            GROUP BY 1, 2 ORDER BY 1
        """)
        monthly_rows = db.execute(monthly_sql, {"vendor": vendor_name}).fetchall()

        total_value = sum(float(r.total_value or 0) for r in item_rows)

        return {
            "vendor_name": vendor_name,
            "total_transactions": sum(int(r.inward_count or 0) for r in item_rows),
            "total_value": round(total_value, 2),
            "item_summary": [{
                "item_description": r.item_description,
                "inward_count": int(r.inward_count or 0),
                "total_qty": _f(r.total_qty), "total_weight": _f(r.total_weight),
                "total_value": _f(r.total_value),
                "avg_rate": round(_f(r.total_value) / _f(r.total_weight), 2) if _f(r.total_weight) > 0 else 0,
                "last_inward": r.last_inward,
            } for r in item_rows],
            "monthly_pattern": [{
                "month": r.month, "month_label": r.month_label,
                "inward_count": int(r.inward_count or 0),
                "total_weight": _f(r.total_weight), "total_value": _f(r.total_value),
            } for r in monthly_rows],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Vendor history error: %s", e)
        raise HTTPException(500, str(e))
