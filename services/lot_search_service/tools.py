"""Lot Search — cross-table lookup by lot_number / box_id / transaction_no.

Each helper runs a focused indexed lookup and returns a normalized row shape:
    {table, company, lot, box_id, transaction_no, item, weight, extra}

Column-name variance across tables (lot_number vs lot_no, article vs
article_description vs item_description) is normalized via SELECT aliases.
"""

from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


# Per-category row cap. We still cap the *rows* returned for display, but report
# the exact total count (so the UI shows e.g. "247" instead of "100+").
PER_TABLE_LIMIT = 100


def _count(db: Session, from_sql: str, where: str, params: dict) -> int:
    """Exact COUNT(*) for a table+filter — only invoked when a table is truncated."""
    return int(db.execute(text(f"SELECT COUNT(*) FROM {from_sql} WHERE {where}"), params).scalar() or 0)


def _and_clauses(lot_col: str, box_col: str, txn_col: str,
                 lot: Optional[str], box: Optional[str], txn: Optional[str]) -> tuple[str, dict]:
    """Build WHERE clauses for the 3 inputs against the named columns."""
    parts: list[str] = []
    params: dict = {}
    if lot:
        parts.append(f"{lot_col} = :lot")
        params["lot"] = lot
    if box:
        parts.append(f"{box_col} = :box")
        params["box"] = box
    if txn:
        parts.append(f"{txn_col} = :txn")
        params["txn"] = txn
    where = " AND ".join(parts) if parts else "FALSE"
    return where, params


def _search_v2_boxes(db: Session, lot: Optional[str], box: Optional[str], txn: Optional[str]) -> list[dict]:
    results: list[dict] = []
    for company in ("cfpl", "cdpl"):
        table = f"{company}_boxes_v2"
        where, params = _and_clauses("lot_number", "box_id", "transaction_no", lot, box, txn)
        rows = db.execute(text(f"""
            SELECT lot_number AS lot, box_id, transaction_no, article_description AS item,
                   net_weight AS weight, gross_weight, box_number
            FROM {table}
            WHERE {where}
            ORDER BY id DESC
            LIMIT {PER_TABLE_LIMIT + 1}
        """), params).fetchall()
        for r in rows[:PER_TABLE_LIMIT]:
            results.append({
                "table": table, "company": company.upper(),
                "lot": r.lot, "box_id": r.box_id, "transaction_no": r.transaction_no,
                "item": r.item, "weight": float(r.weight or 0),
                "extra": {"box_number": r.box_number, "gross_weight": float(r.gross_weight or 0)},
            })
        if len(rows) > PER_TABLE_LIMIT:
            results.append({"table": table, "_truncated": True,
                            "_total": _count(db, table, where, params)})
    return results


def _search_bulk_entry_boxes(db: Session, lot: Optional[str], box: Optional[str], txn: Optional[str]) -> list[dict]:
    results: list[dict] = []
    for company in ("cfpl", "cdpl"):
        table = f"{company}_bulk_entry_boxes"
        where, params = _and_clauses("lot_number", "box_id", "transaction_no", lot, box, txn)
        rows = db.execute(text(f"""
            SELECT lot_number AS lot, box_id, transaction_no, article_description AS item,
                   net_weight AS weight, gross_weight, box_number
            FROM {table}
            WHERE {where}
            ORDER BY id DESC
            LIMIT {PER_TABLE_LIMIT + 1}
        """), params).fetchall()
        for r in rows[:PER_TABLE_LIMIT]:
            results.append({
                "table": table, "company": company.upper(),
                "lot": r.lot, "box_id": r.box_id, "transaction_no": r.transaction_no,
                "item": r.item, "weight": float(r.weight or 0),
                "extra": {"box_number": r.box_number, "gross_weight": float(r.gross_weight or 0)},
            })
        if len(rows) > PER_TABLE_LIMIT:
            results.append({"table": table, "_truncated": True,
                            "_total": _count(db, table, where, params)})
    return results


def _search_transfer_out(db: Session, lot: Optional[str], box: Optional[str], txn: Optional[str]) -> list[dict]:
    where, params = _and_clauses("itb.lot_number", "itb.box_id", "itb.transaction_no", lot, box, txn)
    rows = db.execute(text(f"""
        SELECT itb.id, itb.lot_number AS lot, itb.box_id, itb.transaction_no,
               itb.article AS item, itb.net_weight AS weight, itb.gross_weight,
               itb.box_number, itb.header_id,
               h.challan_no, h.from_site, h.to_site, h.status, h.stock_trf_date
        FROM interunit_transfer_boxes itb
        LEFT JOIN interunit_transfers_header h ON h.id = itb.header_id
        WHERE {where}
        ORDER BY itb.id DESC
        LIMIT {PER_TABLE_LIMIT + 1}
    """), params).fetchall()
    out: list[dict] = []
    for r in rows[:PER_TABLE_LIMIT]:
        out.append({
            "table": "interunit_transfer_boxes",
            "company": None,
            "lot": r.lot, "box_id": r.box_id, "transaction_no": r.transaction_no,
            "item": r.item, "weight": float(r.weight or 0),
            "extra": {
                "challan_no": r.challan_no,
                "from_site": r.from_site,
                "to_site": r.to_site,
                "status": r.status,
                "stock_trf_date": str(r.stock_trf_date) if r.stock_trf_date else None,
                "header_id": r.header_id,
                "box_number": r.box_number,
                "gross_weight": float(r.gross_weight or 0),
            },
        })
    if len(rows) > PER_TABLE_LIMIT:
        out.append({"table": "interunit_transfer_boxes", "_truncated": True,
                    "_total": _count(db, "interunit_transfer_boxes itb", where, params)})
    return out


def _search_transfer_in(db: Session, lot: Optional[str], box: Optional[str], txn: Optional[str]) -> list[dict]:
    where, params = _and_clauses("itb.lot_number", "itb.box_id", "itb.transaction_no", lot, box, txn)
    # Transfer-IN header has no `from_site` — source warehouse lives on the
    # parent Transfer-OUT header, reached via h.transfer_out_id.
    rows = db.execute(text(f"""
        SELECT itb.id, itb.lot_number AS lot, itb.box_id, itb.transaction_no,
               itb.article AS item, itb.net_weight AS weight, itb.gross_weight,
               itb.header_id, itb.is_matched, itb.scanned_at,
               h.grn_number, h.receiving_warehouse, h.status, h.received_at,
               h.transfer_out_id, h.transfer_out_no,
               oh.from_site AS source_from_site
        FROM interunit_transfer_in_boxes itb
        LEFT JOIN interunit_transfer_in_header h ON h.id = itb.header_id
        LEFT JOIN interunit_transfers_header oh ON oh.id = h.transfer_out_id
        WHERE {where}
        ORDER BY itb.id DESC
        LIMIT {PER_TABLE_LIMIT + 1}
    """), params).fetchall()
    out: list[dict] = []
    for r in rows[:PER_TABLE_LIMIT]:
        out.append({
            "table": "interunit_transfer_in_boxes",
            "company": None,
            "lot": r.lot, "box_id": r.box_id, "transaction_no": r.transaction_no,
            "item": r.item, "weight": float(r.weight or 0),
            "extra": {
                "grn_number": r.grn_number,
                "from_site": r.source_from_site,
                "receiving_warehouse": r.receiving_warehouse,
                "status": r.status,
                "received_at": str(r.received_at) if r.received_at else None,
                "transfer_out_id": r.transfer_out_id,
                "transfer_out_no": r.transfer_out_no,
                "header_id": r.header_id,
                "is_matched": r.is_matched,
                "gross_weight": float(r.gross_weight or 0),
            },
        })
    if len(rows) > PER_TABLE_LIMIT:
        out.append({"table": "interunit_transfer_in_boxes", "_truncated": True,
                    "_total": _count(db, "interunit_transfer_in_boxes itb", where, params)})
    return out


def _search_jb_materialout(db: Session, lot: Optional[str], box: Optional[str], txn: Optional[str]) -> list[dict]:
    where, params = _and_clauses("l.lot_number", "l.box_id", "l.transaction_no", lot, box, txn)
    rows = db.execute(text(f"""
        SELECT l.id, l.lot_number AS lot, l.box_id, l.transaction_no,
               l.item_description AS item, l.quantity_kgs AS weight,
               l.header_id, l.material_type, l.cold_unit, l.batch_number
        FROM jb_materialout_lines l
        WHERE {where}
        ORDER BY l.id DESC
        LIMIT {PER_TABLE_LIMIT + 1}
    """), params).fetchall()
    out: list[dict] = []
    for r in rows[:PER_TABLE_LIMIT]:
        out.append({
            "table": "jb_materialout_lines",
            "company": None,
            "lot": r.lot, "box_id": r.box_id, "transaction_no": r.transaction_no,
            "item": r.item, "weight": float(r.weight or 0),
            "extra": {
                "header_id": r.header_id,
                "material_type": r.material_type,
                "cold_unit": r.cold_unit,
                "batch_number": r.batch_number,
            },
        })
    if len(rows) > PER_TABLE_LIMIT:
        out.append({"table": "jb_materialout_lines", "_truncated": True,
                    "_total": _count(db, "jb_materialout_lines l", where, params)})
    return out


def _search_jb_inward_boxes(db: Session, lot: Optional[str], box: Optional[str], txn: Optional[str]) -> list[dict]:
    where, params = _and_clauses("lot_no", "box_id", "transaction_no", lot, box, txn)
    rows = db.execute(text(f"""
        SELECT id, lot_no AS lot, box_id, transaction_no,
               item_description AS item, net_weight AS weight, gross_weight,
               inward_warehouse, box_number, inward_receipt_id, box_type, item_mark
        FROM jb_inward_boxes
        WHERE {where}
        ORDER BY id DESC
        LIMIT {PER_TABLE_LIMIT + 1}
    """), params).fetchall()
    out: list[dict] = []
    for r in rows[:PER_TABLE_LIMIT]:
        out.append({
            "table": "jb_inward_boxes",
            "company": None,
            "lot": r.lot, "box_id": r.box_id, "transaction_no": r.transaction_no,
            "item": r.item, "weight": float(r.weight or 0),
            "extra": {
                "inward_warehouse": r.inward_warehouse,
                "inward_receipt_id": r.inward_receipt_id,
                "box_type": r.box_type,
                "item_mark": r.item_mark,
                "box_number": r.box_number,
                "gross_weight": float(r.gross_weight or 0),
            },
        })
    if len(rows) > PER_TABLE_LIMIT:
        out.append({"table": "jb_inward_boxes", "_truncated": True,
                    "_total": _count(db, "jb_inward_boxes", where, params)})
    return out


def _search_cold_stocks(db: Session, lot: Optional[str], box: Optional[str], txn: Optional[str]) -> list[dict]:
    results: list[dict] = []
    for company in ("cfpl", "cdpl"):
        table = f"{company}_cold_stocks"
        where, params = _and_clauses("lot_no", "box_id", "transaction_no", lot, box, txn)
        rows = db.execute(text(f"""
            SELECT lot_no AS lot, box_id, transaction_no, item_description AS item,
                   weight_kg AS weight, no_of_cartons, unit, storage_location,
                   group_name, canonical_warehouse, inward_transaction_no
            FROM {table}
            WHERE {where}
            ORDER BY id DESC
            LIMIT {PER_TABLE_LIMIT + 1}
        """), params).fetchall()
        for r in rows[:PER_TABLE_LIMIT]:
            results.append({
                "table": table, "company": company.upper(),
                "lot": r.lot, "box_id": r.box_id, "transaction_no": r.transaction_no,
                "item": r.item, "weight": float(r.weight or 0),
                "extra": {
                    "unit": r.unit,
                    "storage_location": r.storage_location,
                    "canonical_warehouse": r.canonical_warehouse,
                    "group_name": r.group_name,
                    "no_of_cartons": r.no_of_cartons,
                    "inward_transaction_no": r.inward_transaction_no,
                },
            })
        if len(rows) > PER_TABLE_LIMIT:
            results.append({"table": table, "_truncated": True,
                            "_total": _count(db, table, where, params)})
    return results


def _search_cold_transfer_in(db: Session, lot: Optional[str], box: Optional[str], txn: Optional[str]) -> list[dict]:
    where, params = _and_clauses("ctb.lot_no", "ctb.box_id", "ctb.transaction_no", lot, box, txn)
    rows = db.execute(text(f"""
        SELECT ctb.id, ctb.lot_no AS lot, ctb.box_id, ctb.transaction_no,
               ctb.item_description AS item, ctb.weight_kg AS weight,
               ctb.no_of_cartons, ctb.unit, ctb.header_id,
               h.grn_number, h.from_site, h.to_site, h.to_company, h.status, h.received_at,
               h.transfer_out_no
        FROM cold_transfer_inboxes ctb
        LEFT JOIN cold_transfer_in_headers h ON h.id = ctb.header_id
        WHERE {where}
        ORDER BY ctb.id DESC
        LIMIT {PER_TABLE_LIMIT + 1}
    """), params).fetchall()
    out: list[dict] = []
    for r in rows[:PER_TABLE_LIMIT]:
        out.append({
            "table": "cold_transfer_inboxes",
            "company": r.to_company,
            "lot": r.lot, "box_id": r.box_id, "transaction_no": r.transaction_no,
            "item": r.item, "weight": float(r.weight or 0),
            "extra": {
                "grn_number": r.grn_number,
                "from_site": r.from_site,
                "to_site": r.to_site,
                "status": r.status,
                "received_at": str(r.received_at) if r.received_at else None,
                "transfer_out_no": r.transfer_out_no,
                "header_id": r.header_id,
                "unit": r.unit,
                "no_of_cartons": r.no_of_cartons,
            },
        })
    if len(rows) > PER_TABLE_LIMIT:
        out.append({"table": "cold_transfer_inboxes", "_truncated": True,
                    "_total": _count(db, "cold_transfer_inboxes ctb", where, params)})
    return out


def _search_disposition(db: Session, lot: Optional[str], box: Optional[str], txn: Optional[str]) -> list[dict]:
    where, params = _and_clauses("box_id", "box_id", "transaction_no", lot, box, txn)
    # Build proper WHERE for disposition (uses lot_no, not lot_number)
    parts: list[str] = []
    params = {}
    if lot:
        parts.append("lot_no = :lot")
        params["lot"] = lot
    if box:
        parts.append("box_id = :box")
        params["box"] = box
    if txn:
        # Match either the original txn (when the box was inwarded) OR the
        # disposition's outgoing reference (DO-... / transfer challan etc.)
        parts.append("(transaction_no = :txn OR disposition_ref_no = :txn)")
        params["txn"] = txn
    where = " AND ".join(parts) if parts else "FALSE"

    rows = db.execute(text(f"""
        SELECT id, box_id, transaction_no, lot_no AS lot, item_description AS item,
               from_company, unit, from_site, source_table,
               disposition_type, disposition_ref_table, disposition_ref_no,
               disposed_at, disposed_by, reverted, reverted_at
        FROM cold_stock_disposition
        WHERE {where}
        ORDER BY id DESC
        LIMIT {PER_TABLE_LIMIT + 1}
    """), params).fetchall()
    out: list[dict] = []
    for r in rows[:PER_TABLE_LIMIT]:
        out.append({
            "table": "cold_stock_disposition",
            "company": r.from_company,
            "lot": r.lot, "box_id": r.box_id, "transaction_no": r.transaction_no,
            "item": r.item, "weight": 0.0,
            "extra": {
                "from_site": r.from_site,
                "unit": r.unit,
                "source_table": r.source_table,
                "disposition_type": r.disposition_type,
                "disposition_ref_table": r.disposition_ref_table,
                "disposition_ref_no": r.disposition_ref_no,
                "disposed_at": str(r.disposed_at) if r.disposed_at else None,
                "disposed_by": r.disposed_by,
                "reverted": bool(r.reverted),
                "reverted_at": str(r.reverted_at) if r.reverted_at else None,
            },
        })
    if len(rows) > PER_TABLE_LIMIT:
        out.append({"table": "cold_stock_disposition", "_truncated": True,
                    "_total": _count(db, "cold_stock_disposition", where, params)})
    return out


def _search_direct_out_headers(db: Session, txn: Optional[str]) -> list[dict]:
    """Match a DO-... transaction_no directly to the direct-out header table."""
    if not txn:
        return []
    out: list[dict] = []
    for company in ("cfpl", "cdpl"):
        table = f"{company}_cold_storage_direct_out"
        rows = db.execute(text(f"""
            SELECT id, transaction_no, entry_date, authority_person, to_customer,
                   warehouse, vehicle_no, invoice_no, line_count, total_issue_qty,
                   status, created_by, created_at
            FROM {table}
            WHERE transaction_no = :txn
            ORDER BY id DESC
            LIMIT {PER_TABLE_LIMIT + 1}
        """), {"txn": txn}).fetchall()
        for r in rows[:PER_TABLE_LIMIT]:
            out.append({
                "table": table, "company": company.upper(),
                "lot": None, "box_id": None, "transaction_no": r.transaction_no,
                "item": None, "weight": float(r.total_issue_qty or 0),
                "extra": {
                    "entry_date": str(r.entry_date) if r.entry_date else None,
                    "authority_person": r.authority_person,
                    "to_customer": r.to_customer,
                    "warehouse": r.warehouse,
                    "vehicle_no": r.vehicle_no,
                    "invoice_no": r.invoice_no,
                    "line_count": r.line_count,
                    "total_issue_qty": float(r.total_issue_qty or 0),
                    "status": r.status,
                    "created_by": r.created_by,
                    "created_at": str(r.created_at) if r.created_at else None,
                },
            })
        if len(rows) > PER_TABLE_LIMIT:
            out.append({"table": table, "_truncated": True,
                        "_total": _count(db, table, "transaction_no = :txn", {"txn": txn})})
    return out


# ── Public ────────────────────────────────────────────────────────────────

CATEGORIES = (
    ("inward", "Inward (v2)", _search_v2_boxes),
    ("bulk_entry", "Inward — Bulk Entry", _search_bulk_entry_boxes),
    ("transfer_out", "Interunit Transfer-Out", _search_transfer_out),
    ("transfer_in", "Interunit Transfer-In", _search_transfer_in),
    ("job_work_out", "Job Work — Material Out", _search_jb_materialout),
    ("job_work_in", "Job Work — Inward Boxes", _search_jb_inward_boxes),
    ("cold_stocks", "Cold Stocks (current)", _search_cold_stocks),
    ("cold_transfer_in", "Cold Transfer-In", _search_cold_transfer_in),
    ("direct_out_disposition", "Direct Out / Disposition (per-box)", _search_disposition),
)


def _category_payload(label: str, rows: list[dict]) -> dict:
    """Build a category result: capped `rows` for display, but an EXACT `count`.

    A category can span multiple tables (e.g. cfpl+cdpl). For a truncated table we
    use its `_total` (exact COUNT); for a non-truncated table we use the number of
    rows it actually returned (which is its full count, ≤ PER_TABLE_LIMIT).
    """
    non_marker = [r for r in rows if not r.get("_truncated")]
    markers = [r for r in rows if r.get("_truncated")]
    truncated_totals = {m["table"]: int(m.get("_total") or 0) for m in markers}
    returned_by_table: dict = {}
    for r in non_marker:
        returned_by_table[r["table"]] = returned_by_table.get(r["table"], 0) + 1
    all_tables = set(returned_by_table) | set(truncated_totals)
    exact = sum(truncated_totals.get(t, returned_by_table.get(t, 0)) for t in all_tables)
    return {
        "label": label,
        "count": exact,
        "rows": non_marker,
        "truncated": bool(markers),
    }


def search_lot(
    db: Session,
    lot_number: Optional[str],
    box_id: Optional[str],
    transaction_no: Optional[str],
) -> dict:
    lot = (lot_number or "").strip() or None
    box = (box_id or "").strip() or None
    txn = (transaction_no or "").strip() or None

    categories: dict = {}
    grand_total = 0
    for key, label, fn in CATEGORIES:
        payload = _category_payload(label, fn(db, lot, box, txn))
        categories[key] = payload
        grand_total += payload["count"]

    # Direct-out header lookup (only meaningful with txn = DO-...)
    do_rows = _search_direct_out_headers(db, txn)
    if do_rows:
        payload = _category_payload("Direct Out — Transaction", do_rows)
        categories["direct_out_header"] = payload
        grand_total += payload["count"]

    return {
        "query": {"lot_number": lot, "box_id": box, "transaction_no": txn},
        "grand_total": grand_total,
        "categories": categories,
    }
