"""
Transfer Dashboard API — All data loaded once, client-side filtering.
Tables: interunit_transfers_header, interunit_transfers_lines, interunit_transfer_boxes,
        interunit_transfer_in_header
"""

from typing import Optional
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.database import get_db
from shared.logger import get_logger

logger = get_logger("transfer_dashboard")

router = APIRouter(prefix="/transfer-dashboard", tags=["Transfer Dashboard"])


def _f(v):
    return round(float(v), 2) if v else 0.0


# Only include transfers with actual line items
LINE_FILTER = "COALESCE(l.net_weight, 0) > 0 OR COALESCE(l.total_weight, 0) > 0 OR COALESCE(l.qty, 0) > 0"


@router.get("/all-data")
async def get_all_data(db: Session = Depends(get_db)):
    """All transfer records joined with lines. Only transfers with actual items."""
    try:
        sql = text(f"""
            SELECT
                h.id AS transfer_id,
                COALESCE(h.challan_no, '') AS challan_no,
                h.stock_trf_date::text AS transfer_date,
                TO_CHAR(h.stock_trf_date, 'YYYY-MM') AS transfer_month,
                COALESCE(h.from_site, '') AS from_warehouse,
                COALESCE(h.to_site, '') AS to_warehouse,
                COALESCE(h.vehicle_no, '') AS vehicle_no,
                COALESCE(h.driver_name, '') AS driver_name,
                COALESCE(h.status, '') AS status,
                COALESCE(h.created_by, '') AS created_by,
                COALESCE(h.remark, '') AS remark,
                COALESCE(l.item_desc_raw, '') AS item_description,
                COALESCE(l.item_category, '') AS item_category,
                COALESCE(l.sub_category, '') AS sub_category,
                COALESCE(l.rm_pm_fg_type, '') AS material_type,
                COALESCE(l.lot_number, '') AS lot_number,
                COALESCE(l.qty, 0) AS qty,
                COALESCE(l.uom, '') AS uom,
                COALESCE(l.pack_size, 0) AS pack_size,
                ROUND(COALESCE(l.net_weight, 0)::numeric, 2) AS net_weight,
                ROUND(COALESCE(l.total_weight, 0)::numeric, 2) AS total_weight
            FROM interunit_transfers_header h
            INNER JOIN interunit_transfers_lines l ON h.id = l.header_id
            WHERE ({LINE_FILTER})
            ORDER BY h.stock_trf_date DESC NULLS LAST
        """)
        rows = db.execute(sql).fetchall()
        cols = rows[0]._fields if rows else []

        NUMERIC = {"qty", "pack_size", "net_weight", "total_weight"}
        INT = {"transfer_id"}

        records = []
        for r in rows:
            rec = {}
            for c in cols:
                v = getattr(r, c)
                if v is None:
                    rec[c] = 0 if c in NUMERIC or c in INT else ""
                elif c in NUMERIC:
                    rec[c] = round(float(v), 2)
                elif c in INT:
                    rec[c] = int(v)
                else:
                    rec[c] = str(v)
            records.append(rec)

        # Box counts per transfer
        box_sql = text("""
            SELECT header_id, COUNT(*) AS box_count
            FROM interunit_transfer_boxes
            GROUP BY header_id
        """)
        box_counts = {int(r.header_id): int(r.box_count) for r in db.execute(box_sql).fetchall()}
        for rec in records:
            rec["box_count"] = box_counts.get(rec["transfer_id"], 0)

        # Transfer-in status per transfer
        tin_sql = text("""
            SELECT transfer_out_id, status AS tin_status
            FROM interunit_transfer_in_header
        """)
        tin_map = {int(r.transfer_out_id): r.tin_status for r in db.execute(tin_sql).fetchall()}
        for rec in records:
            rec["received_status"] = tin_map.get(rec["transfer_id"], "Not Received")

        # Issue details per transfer (from interunit_transfer_in_boxes)
        issue_sql = text("""
            SELECT tih.transfer_out_id,
                   tib.article,
                   tib.issue->>'remarks' AS issue_remarks,
                   tib.issue->>'actual_qty' AS actual_qty,
                   tib.issue->>'actual_total_weight' AS actual_total_weight,
                   COALESCE(tib.net_weight, 0) AS net_weight
            FROM interunit_transfer_in_boxes tib
            JOIN interunit_transfer_in_header tih ON tib.header_id = tih.id
            WHERE tib.issue IS NOT NULL
              AND tib.issue::text != 'null'
              AND tib.issue::text != '{}'
            ORDER BY tih.transfer_out_id, tib.article
        """)
        # Group issues by transfer_out_id
        issue_map: dict = {}
        for r in db.execute(issue_sql).fetchall():
            tid = int(r.transfer_out_id)
            if tid not in issue_map:
                issue_map[tid] = {"issue_count": 0, "issue_items": [], "issue_weight": 0, "issue_details": []}
            entry = issue_map[tid]
            entry["issue_count"] += 1
            entry["issue_weight"] += float(r.net_weight or 0)
            if r.article and r.article not in entry["issue_items"]:
                entry["issue_items"].append(r.article)
            entry["issue_details"].append({
                "article": r.article or "",
                "remarks": r.issue_remarks or "",
                "actual_qty": r.actual_qty or "",
                "actual_total_weight": r.actual_total_weight or "",
            })

        for rec in records:
            iss = issue_map.get(rec["transfer_id"], {})
            rec["issue_count"] = iss.get("issue_count", 0)
            rec["issue_items"] = ", ".join(iss.get("issue_items", []))
            rec["issue_weight"] = round(iss.get("issue_weight", 0), 2)
            rec["issue_details"] = iss.get("issue_details", [])
            rec["has_issue"] = rec["issue_count"] > 0

        return {"records": records, "total": len(records), "as_of_date": date.today().isoformat()}

    except Exception as e:
        logger.error("All data error: %s", e)
        raise HTTPException(500, str(e))


@router.get("/filter-options")
async def get_filter_options(db: Session = Depends(get_db)):
    """Distinct values for filter chips."""
    try:
        result: dict = {}

        result["from_warehouses"] = [r[0] for r in db.execute(text("""
            SELECT DISTINCT h.from_site FROM interunit_transfers_header h
            INNER JOIN interunit_transfers_lines l ON h.id = l.header_id
            WHERE h.from_site IS NOT NULL AND h.from_site != '' AND ({LINE_FILTER})
            ORDER BY h.from_site
        """.format(LINE_FILTER=LINE_FILTER))).fetchall()]

        result["to_warehouses"] = [r[0] for r in db.execute(text("""
            SELECT DISTINCT h.to_site FROM interunit_transfers_header h
            INNER JOIN interunit_transfers_lines l ON h.id = l.header_id
            WHERE h.to_site IS NOT NULL AND h.to_site != '' AND ({LINE_FILTER})
            ORDER BY h.to_site
        """.format(LINE_FILTER=LINE_FILTER))).fetchall()]

        result["statuses"] = [r[0] for r in db.execute(text("""
            SELECT DISTINCT status FROM interunit_transfers_header
            WHERE status IS NOT NULL ORDER BY status
        """)).fetchall()]

        result["item_categories"] = [r[0] for r in db.execute(text(f"""
            SELECT DISTINCT l.item_category FROM interunit_transfers_lines l
            WHERE l.item_category IS NOT NULL AND l.item_category != '' AND ({LINE_FILTER})
            ORDER BY l.item_category
        """)).fetchall()]

        result["material_types"] = [r[0] for r in db.execute(text(f"""
            SELECT DISTINCT l.rm_pm_fg_type FROM interunit_transfers_lines l
            WHERE l.rm_pm_fg_type IS NOT NULL AND l.rm_pm_fg_type != '' AND ({LINE_FILTER})
            ORDER BY l.rm_pm_fg_type
        """)).fetchall()]

        result["created_by"] = [r[0] for r in db.execute(text("""
            SELECT DISTINCT created_by FROM interunit_transfers_header
            WHERE created_by IS NOT NULL AND created_by != '' ORDER BY created_by
        """)).fetchall()]

        return result

    except Exception as e:
        logger.error("Filter options error: %s", e)
        raise HTTPException(500, str(e))
