"""
Cold Storage Dashboard API v3 — Stock Summary, Ageing Summary, Concentration & Risk
Reads from company-specific tables: cdpl_cold_stocks / cfpl_cold_stocks
Supports multi-company (All = UNION of both tables)

Layer hierarchy:
  Layer 1: storage_location + group_name
  Layer 2: item_subgroup  (NOT item_description)
  Layer 3: item_mark
  Layer 4: lot_no (FIFO by inward_dt) — lazy-loaded
"""

from typing import Optional, List
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.database import get_db
from shared.logger import get_logger

logger = get_logger("cold_storage_dashboard")

router = APIRouter(
    prefix="/cold-storage/dashboard",
    tags=["Cold Storage Dashboard"],
)

COMPANY_TABLE_MAP = {
    "cfpl": "cfpl_cold_stocks",
    "cdpl": "cdpl_cold_stocks",
}


def _resolve_tables(company: str) -> List[str]:
    c = company.strip().lower()
    if c == "all":
        return list(COMPANY_TABLE_MAP.values())
    table = COMPANY_TABLE_MAP.get(c)
    if not table:
        raise HTTPException(400, f"Unknown company: {company}. Use 'cfpl', 'cdpl', or 'all'.")
    return [table]


_COMMON_COLS = (
    "storage_location, group_name, item_subgroup, item_mark, item_description, "
    "lot_no, inward_dt, inward_no, unit, no_of_cartons, weight_kg, "
    "total_inventory_kgs, last_purchase_rate, "
    "vakkal, exporter, spl_remarks"
)


def _union_source(tables: List[str]) -> str:
    if len(tables) == 1:
        return tables[0]
    parts = " UNION ALL ".join(f"SELECT {_COMMON_COLS} FROM {t}" for t in tables)
    return f"({parts}) AS cs"


def _f(v):
    return round(float(v), 2) if v else 0.0


def _avg(kgs, val):
    return round(val / kgs, 2) if kgs and kgs > 0 else 0.0


# ═══════════════════════════════════════════════════════════════════
# Tab 1: Stock Summary
# ═══════════════════════════════════════════════════════════════════

@router.get("/stock-summary")
async def get_stock_summary(
    company: str = Query("all"),
    storage_location: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    tables = _resolve_tables(company)
    src = _union_source(tables)

    try:
        loc_filter = ""
        params: dict = {}
        if storage_location:
            loc_filter = "AND storage_location = :loc"
            params["loc"] = storage_location

        l3_sql = text(f"""
            SELECT
                COALESCE(storage_location, 'Unassigned')   AS storage_location,
                COALESCE(group_name, 'Ungrouped')          AS group_name,
                COALESCE(item_subgroup, 'General')         AS item_subgroup,
                COALESCE(item_mark, 'No Mark')             AS item_mark,
                COALESCE(SUM(COALESCE(total_inventory_kgs, 0)), 0)  AS total_kgs,
                COALESCE(SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))), 0)       AS total_value,
                COUNT(*)                                    AS lot_count,
                SUM(CASE WHEN inward_dt IS NOT NULL AND (CURRENT_DATE - inward_dt) < 183
                    THEN COALESCE(total_inventory_kgs, 0) ELSE 0 END) AS age_0_6,
                SUM(CASE WHEN inward_dt IS NOT NULL AND (CURRENT_DATE - inward_dt) >= 183
                    AND (CURRENT_DATE - inward_dt) < 365
                    THEN COALESCE(total_inventory_kgs, 0) ELSE 0 END) AS age_6_12,
                SUM(CASE WHEN inward_dt IS NOT NULL AND (CURRENT_DATE - inward_dt) >= 365
                    AND (CURRENT_DATE - inward_dt) < 548
                    THEN COALESCE(total_inventory_kgs, 0) ELSE 0 END) AS age_12_18,
                SUM(CASE WHEN inward_dt IS NOT NULL AND (CURRENT_DATE - inward_dt) >= 548
                    AND (CURRENT_DATE - inward_dt) < 730
                    THEN COALESCE(total_inventory_kgs, 0) ELSE 0 END) AS age_18_24,
                SUM(CASE WHEN inward_dt IS NOT NULL AND (CURRENT_DATE - inward_dt) >= 730
                    THEN COALESCE(total_inventory_kgs, 0) ELSE 0 END) AS age_24_plus
            FROM {src}
            WHERE 1=1 {loc_filter}
            GROUP BY storage_location, group_name, item_subgroup, item_mark
            ORDER BY storage_location, group_name, item_subgroup, item_mark
        """)
        l3_rows = db.execute(l3_sql, params).fetchall()

        l3_index: dict = {}
        l2_agg: dict = {}
        l1_agg: dict = {}

        for r in l3_rows:
            kgs, val, cnt = _f(r.total_kgs), _f(r.total_value), int(r.lot_count)
            age_profile = {
                "age_0_6": _f(r.age_0_6), "age_6_12": _f(r.age_6_12),
                "age_12_18": _f(r.age_12_18), "age_18_24": _f(r.age_18_24),
                "age_24_plus": _f(r.age_24_plus),
            }
            entry = {
                "item_mark": r.item_mark,
                "total_kgs": kgs, "total_value": val,
                "avg_rate": _avg(kgs, val), "lot_count": cnt,
                "age_profile": age_profile,
            }
            key3 = (r.storage_location, r.group_name, r.item_subgroup)
            l3_index.setdefault(key3, []).append(entry)

            prev = l2_agg.get(key3, {"total_kgs": 0, "total_value": 0, "lot_count": 0})
            l2_agg[key3] = {"total_kgs": round(prev["total_kgs"] + kgs, 2), "total_value": round(prev["total_value"] + val, 2), "lot_count": prev["lot_count"] + cnt}

            key1 = (r.storage_location, r.group_name)
            prev1 = l1_agg.get(key1, {"total_kgs": 0, "total_value": 0, "lot_count": 0})
            l1_agg[key1] = {"total_kgs": round(prev1["total_kgs"] + kgs, 2), "total_value": round(prev1["total_value"] + val, 2), "lot_count": prev1["lot_count"] + cnt}

        l2_index: dict = {}
        for (loc, grp, sg), agg in l2_agg.items():
            l2_index.setdefault((loc, grp), []).append({
                "item_subgroup": sg, **agg,
                "avg_rate": _avg(agg["total_kgs"], agg["total_value"]),
                "children": l3_index.get((loc, grp, sg), []),
            })
        for k in l2_index:
            l2_index[k].sort(key=lambda x: x["item_subgroup"])

        result = []
        gk = gv = gl = 0
        for (loc, grp) in sorted(l1_agg.keys()):
            a = l1_agg[(loc, grp)]
            gk += a["total_kgs"]; gv += a["total_value"]; gl += a["lot_count"]
            result.append({
                "storage_location": loc, "group_name": grp,
                "total_kgs": a["total_kgs"], "total_value": a["total_value"],
                "avg_rate": _avg(a["total_kgs"], a["total_value"]),
                "lot_count": a["lot_count"],
                "children": l2_index.get((loc, grp), []),
            })

        return {
            "as_of_date": date.today().isoformat(), "company": company.upper(),
            "data": result,
            "grand_total": {"total_kgs": round(gk, 2), "total_value": round(gv, 2), "avg_rate": _avg(gk, gv), "lot_count": gl},
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Stock summary error: %s", e)
        raise HTTPException(500, f"Stock summary error: {str(e)}")


# ═══════════════════════════════════════════════════════════════════
# Tab 2: Ageing Summary (Kgs + Value per bracket)
# ═══════════════════════════════════════════════════════════════════

@router.get("/ageing-summary")
async def get_ageing_summary(
    company: str = Query("all"),
    storage_location: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    tables = _resolve_tables(company)
    src = _union_source(tables)

    try:
        loc_filter = ""
        params: dict = {}
        if storage_location:
            loc_filter = "AND storage_location = :loc"
            params["loc"] = storage_location

        sql = text(f"""
            SELECT
                COALESCE(storage_location, 'Unassigned') AS storage_location,
                COALESCE(group_name, 'Ungrouped')        AS group_name,
                COALESCE(item_subgroup, 'General')       AS item_subgroup,
                COALESCE(item_mark, 'No Mark')           AS item_mark,
                SUM(CASE WHEN (CURRENT_DATE - inward_dt) < 183 THEN COALESCE(total_inventory_kgs,0) ELSE 0 END) AS kgs_0_6,
                SUM(CASE WHEN (CURRENT_DATE - inward_dt) >= 183 AND (CURRENT_DATE - inward_dt) < 365 THEN COALESCE(total_inventory_kgs,0) ELSE 0 END) AS kgs_6_12,
                SUM(CASE WHEN (CURRENT_DATE - inward_dt) >= 365 AND (CURRENT_DATE - inward_dt) < 548 THEN COALESCE(total_inventory_kgs,0) ELSE 0 END) AS kgs_12_18,
                SUM(CASE WHEN (CURRENT_DATE - inward_dt) >= 548 AND (CURRENT_DATE - inward_dt) < 730 THEN COALESCE(total_inventory_kgs,0) ELSE 0 END) AS kgs_18_24,
                SUM(CASE WHEN (CURRENT_DATE - inward_dt) >= 730 THEN COALESCE(total_inventory_kgs,0) ELSE 0 END) AS kgs_24_plus,
                SUM(CASE WHEN (CURRENT_DATE - inward_dt) < 183 THEN (COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0)) ELSE 0 END) AS val_0_6,
                SUM(CASE WHEN (CURRENT_DATE - inward_dt) >= 183 AND (CURRENT_DATE - inward_dt) < 365 THEN (COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0)) ELSE 0 END) AS val_6_12,
                SUM(CASE WHEN (CURRENT_DATE - inward_dt) >= 365 AND (CURRENT_DATE - inward_dt) < 548 THEN (COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0)) ELSE 0 END) AS val_12_18,
                SUM(CASE WHEN (CURRENT_DATE - inward_dt) >= 548 AND (CURRENT_DATE - inward_dt) < 730 THEN (COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0)) ELSE 0 END) AS val_18_24,
                SUM(CASE WHEN (CURRENT_DATE - inward_dt) >= 730 THEN (COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0)) ELSE 0 END) AS val_24_plus,
                SUM(COALESCE(total_inventory_kgs,0)) AS grand_total_kgs,
                SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) AS grand_total_value
            FROM {src}
            WHERE inward_dt IS NOT NULL {loc_filter}
            GROUP BY storage_location, group_name, item_subgroup, item_mark
            ORDER BY storage_location, group_name, item_subgroup, item_mark
        """)
        rows = db.execute(sql, params).fetchall()

        ZERO_B = {
            "kgs_0_6": 0, "kgs_6_12": 0, "kgs_12_18": 0, "kgs_18_24": 0, "kgs_24_plus": 0,
            "val_0_6": 0, "val_6_12": 0, "val_12_18": 0, "val_18_24": 0, "val_24_plus": 0,
            "grand_total_kgs": 0, "grand_total_value": 0,
        }

        def _add(a, b):
            return {k: round(a[k] + b[k], 2) for k in ZERO_B}

        l3_idx: dict = {}; l2_agg: dict = {}; l1_agg: dict = {}

        for r in rows:
            entry = {
                "kgs_0_6": _f(r.kgs_0_6), "kgs_6_12": _f(r.kgs_6_12), "kgs_12_18": _f(r.kgs_12_18),
                "kgs_18_24": _f(r.kgs_18_24), "kgs_24_plus": _f(r.kgs_24_plus),
                "val_0_6": _f(r.val_0_6), "val_6_12": _f(r.val_6_12), "val_12_18": _f(r.val_12_18),
                "val_18_24": _f(r.val_18_24), "val_24_plus": _f(r.val_24_plus),
                "grand_total_kgs": _f(r.grand_total_kgs), "grand_total_value": _f(r.grand_total_value),
            }
            k3 = (r.storage_location, r.group_name, r.item_subgroup)
            l3_idx.setdefault(k3, []).append({"item_mark": r.item_mark, **entry})
            l2_agg[k3] = _add(l2_agg.get(k3, dict(ZERO_B)), entry)
            k1 = (r.storage_location, r.group_name)
            l1_agg[k1] = _add(l1_agg.get(k1, dict(ZERO_B)), entry)

        l2_idx: dict = {}
        for (loc, grp, sg), agg in l2_agg.items():
            l2_idx.setdefault((loc, grp), []).append({"item_subgroup": sg, **agg, "children": l3_idx.get((loc, grp, sg), [])})
        for k in l2_idx:
            l2_idx[k].sort(key=lambda x: x["item_subgroup"])

        result = []
        grand = dict(ZERO_B)
        for (loc, grp) in sorted(l1_agg.keys()):
            a = l1_agg[(loc, grp)]
            grand = _add(grand, a)
            result.append({"storage_location": loc, "group_name": grp, **a, "children": l2_idx.get((loc, grp), [])})

        return {"as_of_date": date.today().isoformat(), "company": company.upper(), "data": result, "grand_total": grand}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Ageing summary error: %s", e)
        raise HTTPException(500, f"Ageing summary error: {str(e)}")


# ═══════════════════════════════════════════════════════════════════
# Layer 4: Lot Details (Lazy-loaded, FIFO)
# ═══════════════════════════════════════════════════════════════════

@router.get("/lot-details")
async def get_lot_details(
    company: str = Query("all"),
    storage_location: str = Query(...),
    group_name: str = Query(...),
    item_subgroup: str = Query(...),
    item_mark: str = Query(...),
    db: Session = Depends(get_db),
):
    tables = _resolve_tables(company)
    src = _union_source(tables)

    try:
        avg_sql = text(f"""
            SELECT CASE WHEN SUM(COALESCE(total_inventory_kgs,0)) > 0
                THEN SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) / SUM(COALESCE(total_inventory_kgs,0)) ELSE 0 END AS subgroup_avg_rate
            FROM {src}
            WHERE COALESCE(storage_location,'Unassigned') = :loc
              AND COALESCE(group_name,'Ungrouped') = :grp
              AND COALESCE(item_subgroup,'General') = :sg
        """)
        avg_row = db.execute(avg_sql, {"loc": storage_location, "grp": group_name, "sg": item_subgroup}).fetchone()
        subgroup_avg = float(avg_row.subgroup_avg_rate) if avg_row and avg_row.subgroup_avg_rate else 0.0

        # Consolidated at LOT level — GROUP BY lot_no to merge box-level rows
        sql = text(f"""
            SELECT
                lot_no,
                MIN(inward_dt) AS inward_dt,
                MIN(inward_no) AS inward_no,
                MIN(unit) AS unit,
                MIN(item_description) AS item_description,
                SUM(COALESCE(no_of_cartons,0)) AS no_of_cartons,
                MIN(COALESCE(weight_kg,0)) AS weight_kg,
                SUM(COALESCE(total_inventory_kgs,0)) AS total_kgs,
                CASE WHEN SUM(COALESCE(total_inventory_kgs,0)) > 0
                     THEN SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) / SUM(COALESCE(total_inventory_kgs,0))
                     ELSE 0 END AS last_purchase_rate,
                SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) AS value,
                MIN(vakkal) AS vakkal,
                MIN(exporter) AS exporter,
                STRING_AGG(DISTINCT COALESCE(spl_remarks,''), '; ') FILTER (WHERE spl_remarks IS NOT NULL AND spl_remarks != '') AS spl_remarks,
                COUNT(*) AS box_count,
                CASE WHEN MIN(inward_dt) IS NOT NULL THEN (CURRENT_DATE - MIN(inward_dt)) ELSE NULL END AS ageing_days,
                CASE
                    WHEN MIN(inward_dt) IS NULL THEN 'Unknown'
                    WHEN (CURRENT_DATE - MIN(inward_dt)) < 183 THEN '< 6 Months'
                    WHEN (CURRENT_DATE - MIN(inward_dt)) < 365 THEN '6-12 Months'
                    WHEN (CURRENT_DATE - MIN(inward_dt)) < 548 THEN '12-18 Months'
                    WHEN (CURRENT_DATE - MIN(inward_dt)) < 730 THEN '18-24 Months'
                    ELSE '> 24 Months'
                END AS ageing_bracket
            FROM {src}
            WHERE COALESCE(storage_location,'Unassigned') = :loc
              AND COALESCE(group_name,'Ungrouped') = :grp
              AND COALESCE(item_subgroup,'General') = :sg
              AND COALESCE(item_mark,'No Mark') = :mark
            GROUP BY lot_no
            ORDER BY MIN(inward_dt) ASC NULLS LAST, lot_no ASC
        """)
        rows = db.execute(sql, {"loc": storage_location, "grp": group_name, "sg": item_subgroup, "mark": item_mark}).fetchall()

        lots = []
        for r in rows:
            kgs = float(r.total_kgs); val = float(r.value); rate = float(r.last_purchase_rate)
            deviation_pct = round(((rate - subgroup_avg) / subgroup_avg) * 100, 1) if subgroup_avg > 0 and rate > 0 else 0.0
            deviation_level = "normal" if abs(deviation_pct) <= 15 else ("review" if abs(deviation_pct) <= 50 else "anomaly")
            lots.append({
                "lot_no": str(r.lot_no) if r.lot_no else "", "inward_dt": str(r.inward_dt) if r.inward_dt else None,
                "inward_no": r.inward_no or "", "unit": r.unit or "", "item_description": r.item_description or "",
                "no_of_cartons": float(r.no_of_cartons), "weight_kg": float(r.weight_kg),
                "total_kgs": round(kgs, 2), "last_purchase_rate": round(rate, 2), "value": round(val, 2),
                "avg_rate": round(val / kgs, 2) if kgs > 0 else 0.0,
                "vakkal": r.vakkal or "", "exporter": r.exporter or "", "spl_remarks": r.spl_remarks or "",
                "box_count": int(r.box_count),
                "ageing_days": int(r.ageing_days) if r.ageing_days is not None else None,
                "ageing_bracket": r.ageing_bracket,
                "deviation_pct": deviation_pct, "deviation_level": deviation_level,
            })
        return {"lots": lots, "total": len(lots), "subgroup_avg_rate": round(subgroup_avg, 2)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Lot details error: %s", e)
        raise HTTPException(500, f"Lot details error: {str(e)}")


# ═══════════════════════════════════════════════════════════════════
# Tab 3: Concentration & Risk
# ═══════════════════════════════════════════════════════════════════

@router.get("/concentration")
async def get_concentration(
    company: str = Query("all"),
    storage_location: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    tables = _resolve_tables(company)
    src = _union_source(tables)

    try:
        loc_filter = ""; params: dict = {}
        if storage_location:
            loc_filter = "AND storage_location = :loc"; params["loc"] = storage_location

        sql = text(f"""
            SELECT COALESCE(group_name,'Ungrouped') AS group_name,
                COALESCE(item_subgroup,'General') AS item_subgroup,
                SUM(COALESCE(total_inventory_kgs,0)) AS total_kgs,
                SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) AS total_value, COUNT(*) AS lot_count,
                SUM(CASE WHEN inward_dt IS NOT NULL AND (CURRENT_DATE - inward_dt) >= 548
                    THEN COALESCE(total_inventory_kgs,0) ELSE 0 END) AS aged_18plus_kgs,
                SUM(CASE WHEN inward_dt IS NOT NULL AND (CURRENT_DATE - inward_dt) >= 548
                    THEN (COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0)) ELSE 0 END) AS aged_18plus_value
            FROM {src} WHERE 1=1 {loc_filter}
            GROUP BY group_name, item_subgroup ORDER BY SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) DESC
        """)
        rows = db.execute(sql, params).fetchall()

        grand_kgs = sum(float(r.total_kgs or 0) for r in rows)
        grand_value = sum(float(r.total_value or 0) for r in rows)
        grand_lots = sum(int(r.lot_count or 0) for r in rows)
        aged_kgs = sum(float(r.aged_18plus_kgs or 0) for r in rows)
        aged_value = sum(float(r.aged_18plus_value or 0) for r in rows)

        items = []
        for i, r in enumerate(rows):
            kgs = float(r.total_kgs or 0); val = float(r.total_value or 0); lots = int(r.lot_count or 0)
            pct = round((val / grand_value) * 100, 1) if grand_value > 0 else 0
            frag = "high" if lots > 25 else ("medium" if lots >= 10 else "normal")
            items.append({"rank": i+1, "group_name": r.group_name, "item_subgroup": r.item_subgroup,
                "total_kgs": round(kgs, 2), "total_value": round(val, 2), "portfolio_pct": pct,
                "avg_rate": round(val/kgs, 2) if kgs > 0 else 0, "lot_count": lots, "fragmentation": frag})

        top3_pct = round(sum(it["portfolio_pct"] for it in items[:3]), 1) if len(items) >= 3 else 0
        alerts = [it for it in items if it["portfolio_pct"] > 10]

        return {
            "as_of_date": date.today().isoformat(), "company": company.upper(), "items": items,
            "portfolio": {
                "total_kgs": round(grand_kgs, 2), "total_value": round(grand_value, 2),
                "avg_rate": round(grand_value/grand_kgs, 2) if grand_kgs > 0 else 0,
                "total_lots": grand_lots, "top3_pct": top3_pct,
                "aged_18plus_kgs": round(aged_kgs, 2), "aged_18plus_value": round(aged_value, 2),
                "aged_18plus_pct": round((aged_value/grand_value)*100, 1) if grand_value > 0 else 0,
            },
            "alerts": alerts,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Concentration error: %s", e)
        raise HTTPException(500, f"Concentration error: {str(e)}")


# ═══════════════════════════════════════════════════════════════════
# Inward Trend (last 12 months)
# ═══════════════════════════════════════════════════════════════════

@router.get("/inward-trend")
async def get_inward_trend(
    company: str = Query("all"),
    storage_location: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Monthly inward trend with rich insights — by inward_dt from DB."""
    tables = _resolve_tables(company)
    src = _union_source(tables)

    try:
        loc_filter = ""; params: dict = {}
        if storage_location:
            loc_filter = "AND storage_location = :loc"; params["loc"] = storage_location

        # Monthly trend (last 12 months)
        sql = text(f"""
            SELECT TO_CHAR(inward_dt, 'YYYY-MM') AS month, TO_CHAR(inward_dt, 'Mon YYYY') AS month_label,
                SUM(COALESCE(total_inventory_kgs,0)) AS total_kgs, SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) AS total_value, COUNT(*) AS lot_count
            FROM {src}
            WHERE inward_dt IS NOT NULL AND inward_dt >= (CURRENT_DATE - INTERVAL '12 months') {loc_filter}
            GROUP BY TO_CHAR(inward_dt, 'YYYY-MM'), TO_CHAR(inward_dt, 'Mon YYYY')
            ORDER BY TO_CHAR(inward_dt, 'YYYY-MM')
        """)
        rows = db.execute(sql, params).fetchall()

        months = [{"month": r.month, "month_label": r.month_label, "total_kgs": _f(r.total_kgs),
                    "total_value": _f(r.total_value), "lot_count": int(r.lot_count or 0)} for r in rows]

        current_kgs = months[-1]["total_kgs"] if months else 0
        prev_kgs = months[-2]["total_kgs"] if len(months) >= 2 else 0
        mom = round(((current_kgs - prev_kgs) / prev_kgs) * 100, 1) if prev_kgs > 0 else 0

        # Total open lots
        lot_sql = text(f"SELECT COUNT(*) AS total_lots FROM {src} WHERE 1=1 {loc_filter}")
        total_lots = db.execute(lot_sql, params).scalar() or 0

        # Additional insights
        # Total stock & value
        totals_sql = text(f"""
            SELECT SUM(COALESCE(total_inventory_kgs,0)) AS total_stock_kgs,
                   SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) AS total_stock_value,
                   COUNT(DISTINCT group_name) AS group_count,
                   COUNT(DISTINCT storage_location) AS location_count,
                   MIN(inward_dt) AS earliest_inward,
                   MAX(inward_dt) AS latest_inward
            FROM {src} WHERE 1=1 {loc_filter}
        """)
        totals = db.execute(totals_sql, params).fetchone()

        # Top 5 groups by Kgs
        top_groups_sql = text(f"""
            SELECT COALESCE(group_name, 'Ungrouped') AS group_name,
                SUM(COALESCE(total_inventory_kgs,0)) AS total_kgs,
                SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) AS total_value,
                COUNT(*) AS lot_count
            FROM {src} WHERE 1=1 {loc_filter}
            GROUP BY group_name ORDER BY SUM(COALESCE(total_inventory_kgs,0)) DESC LIMIT 5
        """)
        top_groups = db.execute(top_groups_sql, params).fetchall()

        # Top 5 inward dates (most stock added on which dates)
        top_dates_sql = text(f"""
            SELECT inward_dt, SUM(COALESCE(total_inventory_kgs,0)) AS total_kgs,
                SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) AS total_value, COUNT(*) AS lot_count
            FROM {src} WHERE inward_dt IS NOT NULL {loc_filter}
            GROUP BY inward_dt ORDER BY SUM(COALESCE(total_inventory_kgs,0)) DESC LIMIT 5
        """)
        top_dates = db.execute(top_dates_sql, params).fetchall()

        # Avg monthly inward (from trend data)
        avg_monthly_kgs = round(sum(m["total_kgs"] for m in months) / len(months), 2) if months else 0
        peak_month = max(months, key=lambda m: m["total_kgs"]) if months else None

        # Last 3 months % of total stock
        last3_kgs = sum(m["total_kgs"] for m in months[-3:]) if len(months) >= 3 else sum(m["total_kgs"] for m in months)
        total_stock = float(totals.total_stock_kgs or 0) if totals else 0
        last3_pct = round((last3_kgs / total_stock) * 100, 1) if total_stock > 0 else 0

        return {
            "months": months,
            "current_month_kgs": current_kgs,
            "current_month_value": months[-1]["total_value"] if months else 0,
            "current_month_lots": months[-1]["lot_count"] if months else 0,
            "mom_change_pct": mom,
            "total_open_lots": int(total_lots),
            "total_stock_kgs": _f(totals.total_stock_kgs) if totals else 0,
            "total_stock_value": _f(totals.total_stock_value) if totals else 0,
            "group_count": int(totals.group_count or 0) if totals else 0,
            "location_count": int(totals.location_count or 0) if totals else 0,
            "earliest_inward": str(totals.earliest_inward) if totals and totals.earliest_inward else None,
            "latest_inward": str(totals.latest_inward) if totals and totals.latest_inward else None,
            "avg_monthly_kgs": avg_monthly_kgs,
            "peak_month": peak_month,
            "last3_months_pct": last3_pct,
            "top_groups": [{
                "group_name": r.group_name, "total_kgs": _f(r.total_kgs),
                "total_value": _f(r.total_value), "lot_count": int(r.lot_count or 0),
            } for r in top_groups],
            "top_inward_dates": [{
                "date": str(r.inward_dt), "total_kgs": _f(r.total_kgs),
                "total_value": _f(r.total_value), "lot_count": int(r.lot_count or 0),
            } for r in top_dates],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Inward trend error: %s", e)
        raise HTTPException(500, f"Inward trend error: {str(e)}")


# ═══════════════════════════════════════════════════════════════════
# Storage Locations dropdown
# ═══════════════════════════════════════════════════════════════════

@router.get("/storage-locations")
async def get_dashboard_storage_locations(
    company: str = Query("all"),
    db: Session = Depends(get_db),
):
    tables = _resolve_tables(company)
    src = _union_source(tables)

    try:
        rows = db.execute(text(f"""
            SELECT DISTINCT storage_location FROM {src}
            WHERE storage_location IS NOT NULL AND storage_location != ''
            ORDER BY storage_location
        """)).fetchall()
        return {"locations": [r.storage_location for r in rows]}
    except Exception as e:
        raise HTTPException(500, f"Locations error: {str(e)}")


# ═══════════════════════════════════════════════════════════════════
# §2 — Attention Flags (bracket crossing, rate anomaly, stale lots)
# ═══════════════════════════════════════════════════════════════════

@router.get("/attention-flags")
async def get_attention_flags(
    company: str = Query("all"),
    storage_location: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Compute attention flags from lot data. No outward tracking yet — uses inward_dt only."""
    tables = _resolve_tables(company)
    src = _union_source(tables)

    try:
        loc_filter = ""; params: dict = {}
        if storage_location:
            loc_filter = "AND storage_location = :loc"; params["loc"] = storage_location

        # Consolidated lot-level data
        sql = text(f"""
            SELECT
                lot_no, MIN(inward_dt) AS inward_dt, MIN(inward_no) AS inward_no,
                COALESCE(MIN(storage_location), 'Unassigned') AS storage_location,
                COALESCE(MIN(group_name), 'Ungrouped') AS group_name,
                COALESCE(MIN(item_subgroup), 'General') AS item_subgroup,
                COALESCE(MIN(item_mark), 'No Mark') AS item_mark,
                SUM(COALESCE(total_inventory_kgs, 0)) AS total_kgs,
                SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) AS total_value,
                CASE WHEN SUM(COALESCE(total_inventory_kgs,0)) > 0
                     THEN SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) / SUM(COALESCE(total_inventory_kgs,0))
                     ELSE 0 END AS rate,
                CASE WHEN MIN(inward_dt) IS NOT NULL THEN (CURRENT_DATE - MIN(inward_dt)) ELSE NULL END AS ageing_days
            FROM {src}
            WHERE 1=1 {loc_filter}
            GROUP BY lot_no
            HAVING SUM(COALESCE(total_inventory_kgs, 0)) > 0
            ORDER BY MIN(inward_dt) ASC NULLS LAST
        """)
        rows = db.execute(sql, params).fetchall()

        # Compute sub-group avg rates
        sg_avg_sql = text(f"""
            SELECT COALESCE(item_subgroup, 'General') AS sg,
                CASE WHEN SUM(COALESCE(total_inventory_kgs,0)) > 0
                    THEN SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) / SUM(COALESCE(total_inventory_kgs,0))
                    ELSE 0 END AS avg_rate
            FROM {src} WHERE 1=1 {loc_filter}
            GROUP BY item_subgroup
        """)
        sg_avgs = {r.sg: float(r.avg_rate or 0) for r in db.execute(sg_avg_sql, params).fetchall()}

        flags = []
        bracket_thresholds = [(183, "< 6 Months", "6-12 Months"), (365, "6-12 Months", "12-18 Months"),
                              (548, "12-18 Months", "18-24 Months"), (730, "18-24 Months", "> 24 Months")]

        for r in rows:
            kgs = float(r.total_kgs or 0)
            val = float(r.total_value or 0)
            rate = float(r.rate or 0)
            days = int(r.ageing_days) if r.ageing_days is not None else None
            lot = str(r.lot_no) if r.lot_no else ""

            base = {
                "lot_no": lot, "inward_dt": str(r.inward_dt) if r.inward_dt else None,
                "inward_no": r.inward_no or "", "storage_location": r.storage_location,
                "group_name": r.group_name, "item_subgroup": r.item_subgroup,
                "item_mark": r.item_mark, "total_kgs": round(kgs, 2),
                "total_value": round(val, 2), "rate": round(rate, 2),
                "ageing_days": days,
            }

            # Bracket crossing imminent (within 30 days of next bracket)
            if days is not None:
                for threshold, current_bracket, next_bracket in bracket_thresholds:
                    days_to_cross = threshold - days
                    if 0 < days_to_cross <= 30:
                        flags.append({
                            **base, "flag_type": "bracket_crossing", "severity": "critical",
                            "current_bracket": current_bracket, "next_bracket": next_bracket,
                            "days_to_cross": days_to_cross,
                            "message": f"Crosses to {next_bracket} in {days_to_cross} days",
                        })
                        break

            # Rate anomaly (>50% deviation from sub-group avg)
            sg_avg = sg_avgs.get(r.item_subgroup, 0)
            if sg_avg > 0 and rate > 0:
                dev = abs((rate - sg_avg) / sg_avg) * 100
                if dev > 50:
                    flags.append({
                        **base, "flag_type": "rate_anomaly", "severity": "info",
                        "subgroup_avg_rate": round(sg_avg, 2),
                        "deviation_pct": round(((rate - sg_avg) / sg_avg) * 100, 1),
                        "message": f"Rate ₹{round(rate,0)}/Kg deviates {round(dev,0)}% from avg ₹{round(sg_avg,0)}/Kg",
                    })

            # Stale lot (60+ days from inward with no movement data — proxy using ageing)
            if days is not None and days >= 60:
                flags.append({
                    **base, "flag_type": "stale_lot", "severity": "warning",
                    "days_stale": days,
                    "message": f"No recorded movement for {days} days since inward",
                })

        # Sort: critical first, then by value desc
        sev_order = {"critical": 0, "warning": 1, "info": 2}
        flags.sort(key=lambda f: (sev_order.get(f["severity"], 3), -f["total_value"]))

        # Summary counts
        summary = {}
        for f in flags:
            ft = f["flag_type"]
            summary[ft] = summary.get(ft, 0) + 1

        return {"flags": flags, "summary": summary, "total": len(flags)}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Attention flags error: %s", e)
        raise HTTPException(500, f"Attention flags error: {str(e)}")


# ═══════════════════════════════════════════════════════════════════
# §3 — Slow & Non-Moving Tracker
# ═══════════════════════════════════════════════════════════════════

@router.get("/slow-moving")
async def get_slow_moving(
    company: str = Query("all"),
    storage_location: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Classify lots by movement status based on inward_dt age."""
    tables = _resolve_tables(company)
    src = _union_source(tables)

    try:
        loc_filter = ""; params: dict = {}
        if storage_location:
            loc_filter = "AND storage_location = :loc"; params["loc"] = storage_location

        sql = text(f"""
            SELECT
                lot_no, MIN(inward_dt) AS inward_dt,
                COALESCE(MIN(storage_location), 'Unassigned') AS storage_location,
                COALESCE(MIN(group_name), 'Ungrouped') AS group_name,
                COALESCE(MIN(item_subgroup), 'General') AS item_subgroup,
                COALESCE(MIN(item_mark), 'No Mark') AS item_mark,
                SUM(COALESCE(total_inventory_kgs, 0)) AS total_kgs,
                SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) AS total_value,
                CASE WHEN MIN(inward_dt) IS NOT NULL THEN (CURRENT_DATE - MIN(inward_dt)) ELSE NULL END AS ageing_days,
                CASE
                    WHEN MIN(inward_dt) IS NULL THEN 'Unknown'
                    WHEN (CURRENT_DATE - MIN(inward_dt)) < 183 THEN '< 6 Months'
                    WHEN (CURRENT_DATE - MIN(inward_dt)) < 365 THEN '6-12 Months'
                    WHEN (CURRENT_DATE - MIN(inward_dt)) < 548 THEN '12-18 Months'
                    WHEN (CURRENT_DATE - MIN(inward_dt)) < 730 THEN '18-24 Months'
                    ELSE '> 24 Months'
                END AS ageing_bracket
            FROM {src}
            WHERE 1=1 {loc_filter}
            GROUP BY lot_no
            HAVING SUM(COALESCE(total_inventory_kgs, 0)) > 0
            ORDER BY MIN(inward_dt) ASC NULLS LAST
        """)
        rows = db.execute(sql, params).fetchall()

        items = []
        counts = {"active": 0, "slow_moving": 0, "non_moving": 0, "dead_stock": 0}
        kgs_totals = {"active": 0.0, "slow_moving": 0.0, "non_moving": 0.0, "dead_stock": 0.0}

        for r in rows:
            days = int(r.ageing_days) if r.ageing_days is not None else 0
            kgs = float(r.total_kgs or 0)
            val = float(r.total_value or 0)
            bracket = r.ageing_bracket or "Unknown"

            if days >= 180 and bracket in ("18-24 Months", "> 24 Months"):
                status = "dead_stock"
            elif days >= 60:
                status = "non_moving"
            elif days >= 31:
                status = "slow_moving"
            else:
                status = "active"

            counts[status] += 1
            kgs_totals[status] += kgs

            items.append({
                "lot_no": str(r.lot_no) if r.lot_no else "",
                "inward_dt": str(r.inward_dt) if r.inward_dt else None,
                "storage_location": r.storage_location,
                "group_name": r.group_name,
                "item_subgroup": r.item_subgroup,
                "item_mark": r.item_mark,
                "total_kgs": round(kgs, 2),
                "total_value": round(val, 2),
                "ageing_bracket": bracket,
                "ageing_days": days,
                "movement_status": status,
            })

        total_kgs = sum(kgs_totals.values())

        return {
            "items": items,
            "counts": counts,
            "kgs_totals": {k: round(v, 2) for k, v in kgs_totals.items()},
            "pct_totals": {k: round((v / total_kgs) * 100, 1) if total_kgs > 0 else 0 for k, v in kgs_totals.items()},
            "total": len(items),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Slow moving error: %s", e)
        raise HTTPException(500, f"Slow moving error: {str(e)}")


# ═══════════════════════════════════════════════════════════════════
# §4 — Activity Rundown
# ═══════════════════════════════════════════════════════════════════

@router.get("/activity-rundown")
async def get_activity_rundown(
    company: str = Query("all"),
    storage_location: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Location-wise, company-wise, group-wise, exporter-wise breakdowns."""
    tables = _resolve_tables(company)
    src = _union_source(tables)

    try:
        loc_filter = ""; params: dict = {}
        if storage_location:
            loc_filter = "AND storage_location = :loc"; params["loc"] = storage_location

        # §4A Location wise
        loc_sql = text(f"""
            SELECT COALESCE(storage_location, 'Unassigned') AS location,
                SUM(COALESCE(total_inventory_kgs, 0)) AS total_kgs,
                SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) AS total_value,
                COUNT(*) AS lot_count,
                COUNT(DISTINCT group_name) AS group_count
            FROM {src} WHERE 1=1 {loc_filter}
            GROUP BY storage_location ORDER BY SUM(COALESCE(total_inventory_kgs, 0)) DESC
        """)
        locations = [{
            "location": r.location, "total_kgs": _f(r.total_kgs),
            "total_value": _f(r.total_value), "lot_count": int(r.lot_count or 0),
            "group_count": int(r.group_count or 0),
        } for r in db.execute(loc_sql, params).fetchall()]

        # §4B Company wise (only when "all" — show both)
        company_breakdown = []
        if company.lower() == "all":
            for prefix, label in [("cfpl", "CFPL"), ("cdpl", "CDPL")]:
                tbl = COMPANY_TABLE_MAP.get(prefix)
                if not tbl:
                    continue
                cmp_sql = text(f"""
                    SELECT SUM(COALESCE(total_inventory_kgs, 0)) AS total_kgs,
                        SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) AS total_value,
                        COUNT(*) AS lot_count,
                        COUNT(DISTINCT storage_location) AS location_count
                    FROM {tbl} WHERE 1=1 {loc_filter}
                """)
                r = db.execute(cmp_sql, params).fetchone()
                if r:
                    kgs = _f(r.total_kgs)
                    val = _f(r.total_value)
                    company_breakdown.append({
                        "company": label, "total_kgs": kgs, "total_value": val,
                        "avg_rate": round(val / kgs, 2) if kgs > 0 else 0,
                        "lot_count": int(r.lot_count or 0),
                        "location_count": int(r.location_count or 0),
                    })

        # §4C Group/SubGroup wise
        grp_sql = text(f"""
            SELECT COALESCE(group_name, 'Ungrouped') AS group_name,
                COALESCE(item_subgroup, 'General') AS item_subgroup,
                SUM(COALESCE(total_inventory_kgs, 0)) AS total_kgs,
                SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) AS total_value,
                COUNT(*) AS lot_count
            FROM {src} WHERE 1=1 {loc_filter}
            GROUP BY group_name, item_subgroup
            ORDER BY SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) DESC
        """)
        groups = [{
            "group_name": r.group_name, "item_subgroup": r.item_subgroup,
            "total_kgs": _f(r.total_kgs), "total_value": _f(r.total_value),
            "lot_count": int(r.lot_count or 0),
        } for r in db.execute(grp_sql, params).fetchall()]

        # §4D Exporter wise
        exp_sql = text(f"""
            SELECT COALESCE(MIN(exporter), 'Unknown') AS exporter,
                COUNT(*) AS lot_count,
                SUM(COALESCE(total_inventory_kgs, 0)) AS total_kgs,
                SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) AS total_value,
                MAX(inward_dt)::text AS last_inward
            FROM {src} WHERE exporter IS NOT NULL AND exporter != '' {loc_filter}
            GROUP BY exporter
            ORDER BY SUM((COALESCE(last_purchase_rate,0)*COALESCE(total_inventory_kgs,0))) DESC
        """)
        exporters = [{
            "exporter": r.exporter, "lot_count": int(r.lot_count or 0),
            "total_kgs": _f(r.total_kgs), "total_value": _f(r.total_value),
            "avg_rate": round(_f(r.total_value) / _f(r.total_kgs), 2) if _f(r.total_kgs) > 0 else 0,
            "last_inward": r.last_inward,
        } for r in db.execute(exp_sql, params).fetchall()]

        return {
            "locations": locations,
            "company_breakdown": company_breakdown,
            "groups": groups,
            "exporters": exporters,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Activity rundown error: %s", e)
        raise HTTPException(500, f"Activity rundown error: {str(e)}")
