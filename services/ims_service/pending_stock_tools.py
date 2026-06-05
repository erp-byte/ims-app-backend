"""Pending Transfer Stock helpers.

Middleware between Transfer Out (dispatch) and Transfer In (receive).

Flow:
    create_transfer       → park_in_pending     (deduct source, insert pending)
    finalize_transfer_in  → pick_from_pending   (insert destination, delete pending)
    create_transfer_in    → pick_from_pending   (one-shot variant)
    delete_transfer_in    → unpick_to_pending   (delete destination, restore pending)
    delete_transfer       → restore_to_source   (restore source, delete pending)

Read APIs (for frontend):
    list_pending_transfers   — grouped per transfer_out for the Pending modal
    pending_by_lot           — totals per (lot_no, item_description) for inventory display
"""
import json
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.canonicalize import WAREHOUSE_ALIASES
from shared.logger import get_logger

logger = get_logger("ims.pending_stock")


def _normalize_site(raw: Optional[str]) -> str:
    """Collapse warehouse name variants to canonical codes (e.g. 'Warehouse A68' → 'A68')."""
    if not raw:
        return raw or ""
    key = raw.strip().lower().replace("_", " ")
    return WAREHOUSE_ALIASES.get(key, raw.strip())


COLD_STORAGE_SITE_NAMES = {
    "cold storage",
    "rishi cold",
    "savla d-39 cold",
    "savla d-514 cold",
}


def _is_cold_site(site: Optional[str]) -> bool:
    return (site or "").strip().lower() in COLD_STORAGE_SITE_NAMES


def _table_exists(db: Session, table: str) -> bool:
    return bool(
        db.execute(
            text("SELECT to_regclass(:t)"),
            {"t": f"public.{table}"},
        ).scalar()
    )


def _find_in_cold_stocks(db: Session, box_id: str, transaction_no: str, lot_no: Optional[str] = None):
    """Look up a cold-stock row by (box_id, transaction_no), disambiguated by lot.

    `box_id` is unique only *within* a transaction_no, and some legacy txns carry the
    same box_id for two different lots/items. So we match on (box_id, transaction_no);
    if that resolves to multiple rows, the one whose lot matches `lot_no` is chosen.
    The old box_id-only (no transaction_no) fallback is removed — it grabbed a
    same-labelled box from a DIFFERENT batch (the root cause of wrong-item parking).
    Returns (table, row) or (None, None).
    """
    lot = (lot_no or "").strip().upper()
    candidates = []  # [(table, row)] across BOTH companies for (box_id, transaction_no)
    for table in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
        if not _table_exists(db, table):
            continue
        rows = db.execute(
            text(f"SELECT * FROM {table} WHERE box_id = :bid AND transaction_no = :tno"),
            {"bid": box_id, "tno": transaction_no},
        ).fetchall()
        candidates.extend((table, r) for r in rows)
    if not candidates:
        return None, None
    if len(candidates) == 1:
        return candidates[0]
    # Multiple (box_id, transaction_no) rows (incl. the same id in both companies)
    # → disambiguate by lot; never guess when lot can't resolve it (prevents the
    # wrong-batch grab that mis-parked items, e.g. raisins booked as wet-dates).
    if lot:
        for table, r in candidates:
            if ((getattr(r, "lot_no", "") or "").strip().upper()) == lot:
                return table, r
    return None, None


def _find_available_cold_by_lot(db: Session, company: str, lot_no: str,
                                item_description: Optional[str], limit: int):
    """FIFO-pick up to `limit` available cold_stocks rows for (company, lot_no,
    item_description). Used to rescue the shortfall between ordered qty and parked
    boxes BY LOT NUMBER when strict box_id matching failed (re-inwarded stock,
    box-id drift). Returns [(table, row), ...]."""
    if limit <= 0:
        return []
    table = f"{company}_cold_stocks"
    if not _table_exists(db, table):
        return []
    item_clause = "AND item_description = :item" if item_description else ""
    rows = db.execute(
        text(f"""
            SELECT * FROM {table}
            WHERE lot_no = :lot {item_clause}
            ORDER BY inward_dt ASC NULLS LAST, id ASC
            LIMIT :n
        """),
        {"lot": lot_no, "item": item_description, "n": limit},
    ).fetchall()
    return [(table, r) for r in rows]


def _find_in_bulk_entry(db: Session, box_id: str, transaction_no: str, lot_no: Optional[str] = None):
    """Look up a warehouse-source box row by (box_id, transaction_no), disambiguated by lot.

    Search order (per user spec May 2026): *_boxes_v2 first (current inward target),
    then legacy *_bulk_entry_boxes. Match on (box_id, transaction_no); if a table
    returns multiple rows, the one whose `lot_number` matches `lot_no` is chosen.
    Returns (table, row) or (None, None).
    """
    lot = (lot_no or "").strip().upper()
    candidates = []  # [(table, row)] in search order (boxes_v2 first, then legacy)
    for table in ("cfpl_boxes_v2", "cdpl_boxes_v2", "cfpl_bulk_entry_boxes", "cdpl_bulk_entry_boxes"):
        if not _table_exists(db, table):
            continue
        rows = db.execute(
            text(f"SELECT * FROM {table} WHERE box_id = :bid AND transaction_no = :tno"),
            {"bid": box_id, "tno": transaction_no},
        ).fetchall()
        candidates.extend((table, r) for r in rows)
    if not candidates:
        return None, None
    if len(candidates) == 1:
        return candidates[0]
    if lot:
        for table, r in candidates:
            if ((getattr(r, "lot_number", "") or "").strip().upper()) == lot:
                return table, r
    # No lot disambiguation possible — preserve original behaviour (first match,
    # boxes_v2 before legacy). Warehouse box_ids rarely collide within a txn.
    return candidates[0]


def _company_from_table(table: str) -> str:
    return "cfpl" if table.startswith("cfpl") else "cdpl"


def _destination_table(to_storage_type: str, to_company: str) -> str:
    """Destination INSERT table for finalized boxes.

    Cold destinations land in *_cold_stocks. Warehouse destinations land
    in *_boxes_v2 (the current target). The legacy bulk_entry_boxes
    family is read-only via _find_in_bulk_entry — new rows always go to v2.
    """
    if to_storage_type == "cold":
        return f"{to_company}_cold_stocks"
    return f"{to_company}_boxes_v2"


def _cold_row_to_json(row) -> dict:
    """Extract cold_stocks columns into a JSON-serializable dict for pending_transfer_stock.cold_storage_data."""
    def g(name, default=None):
        v = getattr(row, name, default)
        if isinstance(v, datetime):
            return v.isoformat()
        if hasattr(v, "isoformat"):
            return v.isoformat()
        if v is None:
            return None
        try:
            float(v)
            return float(v) if not isinstance(v, (int, str, bool)) else v
        except (TypeError, ValueError):
            return v

    return {
        "inward_dt": g("inward_dt"),
        "unit": g("unit"),
        "inward_no": g("inward_no"),
        "item_mark": g("item_mark"),
        "vakkal": g("vakkal"),
        "group_name": g("group_name"),
        "item_subgroup": g("item_subgroup"),
        "storage_location": g("storage_location"),
        "exporter": g("exporter"),
        "last_purchase_rate": g("last_purchase_rate"),
        "value": g("value"),
        "total_inventory_kgs": g("total_inventory_kgs"),
        "spl_remarks": g("spl_remarks"),
        "inward_transaction_no": g("inward_transaction_no"),
    }


# ============================================================================
#  STBR (Scan-Time Box ID Reconciliation) — May 2026
#  Spec: docs/conventions.md#pending-transfer-stock-middleware
#
#  Problem solved: at Transfer Out, IMS dispatches its first 20 box_ids (FIFO
#  from cold_stocks/boxes_v2). The warehouse staff physically picks a
#  different range (e.g. boxes 30-49). At Transfer In scan time, QR is the
#  ground truth — the pending row's box_id is rewritten to the scanned
#  value, the wrongly-picked box is restored to source inventory, the
#  actually-shipped box is re-deducted.
#
#  Also: if a series offset is detected (e.g. all 20 boxes drifted by +29),
#  the remaining unreconciled siblings in the SAME (transfer_out, txn, lot)
#  are propagated optimistically — subsequent scans verify the prediction.
# ============================================================================

_RECONCILIATION_INIT_DONE = False


def _ensure_reconciliation_schema(db: Session) -> None:
    """Idempotent self-heal: create transfer_box_reconciliation table + audit
    columns on interunit_transfer_in_boxes and pending_transfer_stock if
    missing. Runs once per process (guarded by _RECONCILIATION_INIT_DONE).
    """
    global _RECONCILIATION_INIT_DONE
    if _RECONCILIATION_INIT_DONE:
        return
    try:
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS transfer_box_reconciliation (
                id BIGSERIAL PRIMARY KEY,
                transfer_in_id BIGINT,
                transfer_out_id BIGINT,
                lot_no VARCHAR(50),
                transaction_no VARCHAR(50) NOT NULL,
                original_box_id VARCHAR(100),
                actual_box_id VARCHAR(100) NOT NULL,
                reconciliation_status VARCHAR(20) NOT NULL,
                conflict_reason TEXT,
                scan_source VARCHAR(20) DEFAULT 'manual',
                scanned_by VARCHAR(100),
                scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                from_company VARCHAR(10),
                to_company VARCHAR(10),
                from_site VARCHAR(100),
                to_site VARCHAR(100),
                propagated_from_id BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.execute(text(
            "CREATE INDEX IF NOT EXISTS tbr_transfer_in_idx "
            "ON transfer_box_reconciliation(transfer_in_id)"
        ))
        db.execute(text(
            "CREATE INDEX IF NOT EXISTS tbr_txn_lot_idx "
            "ON transfer_box_reconciliation(transaction_no, lot_no)"
        ))
        db.execute(text(
            "CREATE INDEX IF NOT EXISTS tbr_actual_box_idx "
            "ON transfer_box_reconciliation(actual_box_id, transaction_no)"
        ))

        # ── Disposition ledger: append-only log of every reason a box left
        #    source inventory (Transfer Out, Direct Out, Job Work Out, etc).
        #    Used by STBR for the audit trail and by the box-history endpoint.
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS cold_stock_disposition (
                id BIGSERIAL PRIMARY KEY,
                box_id VARCHAR(100) NOT NULL,
                transaction_no VARCHAR(50) NOT NULL,
                lot_no VARCHAR(50),
                item_description VARCHAR(255),
                from_company VARCHAR(10),
                unit VARCHAR(50),
                from_site VARCHAR(100),
                source_table VARCHAR(50),
                disposition_type VARCHAR(30) NOT NULL,
                    -- 'transfer_out_pending' | 'direct_out' | 'job_work_out'
                    -- | 'consumption' | 'outward' | 'manual_correction'
                disposition_ref_table VARCHAR(50),
                disposition_ref_id BIGINT,
                disposition_ref_no VARCHAR(100),
                disposed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                disposed_by VARCHAR(100),
                reverted BOOLEAN DEFAULT FALSE,
                reverted_at TIMESTAMP,
                reverted_reason TEXT,
                snapshot_data JSONB,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.execute(text(
            "CREATE INDEX IF NOT EXISTS csd_box_txn_idx "
            "ON cold_stock_disposition(box_id, transaction_no)"
        ))
        db.execute(text(
            "CREATE INDEX IF NOT EXISTS csd_txn_lot_idx "
            "ON cold_stock_disposition(transaction_no, lot_no)"
        ))
        db.execute(text(
            "CREATE INDEX IF NOT EXISTS csd_active_idx "
            "ON cold_stock_disposition(disposition_type, reverted)"
        ))
        db.execute(text(
            "CREATE INDEX IF NOT EXISTS csd_ref_idx "
            "ON cold_stock_disposition(disposition_ref_table, disposition_ref_id)"
        ))

        # Audit columns on existing tables
        for col_sql in (
            "ALTER TABLE interunit_transfer_in_boxes ADD COLUMN IF NOT EXISTS original_box_id VARCHAR(100)",
            "ALTER TABLE interunit_transfer_in_boxes ADD COLUMN IF NOT EXISTS reconciled BOOLEAN DEFAULT FALSE",
            "ALTER TABLE interunit_transfer_in_boxes ADD COLUMN IF NOT EXISTS reconciliation_id BIGINT",
            "ALTER TABLE interunit_transfer_in_boxes ADD COLUMN IF NOT EXISTS scan_source VARCHAR(20) DEFAULT 'manual'",
            "ALTER TABLE pending_transfer_stock ADD COLUMN IF NOT EXISTS original_box_id VARCHAR(100)",
            "ALTER TABLE pending_transfer_stock ADD COLUMN IF NOT EXISTS reconciled BOOLEAN DEFAULT FALSE",
            "ALTER TABLE pending_transfer_stock ADD COLUMN IF NOT EXISTS item_description VARCHAR(255)",
            "ALTER TABLE interunit_transfers_header ADD COLUMN IF NOT EXISTS unallocated_boxes INTEGER DEFAULT 0",
            "ALTER TABLE interunit_transfers_header ADD COLUMN IF NOT EXISTS updated_ts TIMESTAMP",
            # edited_at = a GENUINE user-edit marker. updated_ts is auto-managed
            # (column default CURRENT_TIMESTAMP + a BEFORE-UPDATE trigger), so it moves
            # on every create-reconcile / receive / sync and CANNOT mark a real edit.
            # edited_at is written ONLY by update_transfer, so the "Edited" badge is honest.
            "ALTER TABLE interunit_transfers_header ADD COLUMN IF NOT EXISTS edited_at TIMESTAMP",
        ):
            try:
                db.execute(text(col_sql))
            except Exception as e:
                logger.warning("STBR schema patch skipped: %s — %s", col_sql, e)
        db.commit()
        _RECONCILIATION_INIT_DONE = True
        logger.info("STBR: reconciliation + disposition schema ensured.")
    except Exception as e:
        db.rollback()
        logger.warning("STBR auto-init skipped: %s", e)


def _split_box_id(box_id: str):
    """Split a box_id like '65628000-30' into (prefix, suffix_int) or (full, None).

    Tries the rightmost '-' as the separator, then any trailing numeric run.
    Returns (prefix:str, suffix:int|None).
    """
    if not box_id:
        return ("", None)
    s = box_id.strip()
    # Try last '-' as separator
    if "-" in s:
        head, tail = s.rsplit("-", 1)
        if tail.isdigit():
            return (head + "-", int(tail))
    # Trailing-digit run with no separator
    i = len(s)
    while i > 0 and s[i - 1].isdigit():
        i -= 1
    if i < len(s) and i > 0:
        return (s[:i], int(s[i:]))
    return (s, None)


def _series_offset(placeholder_box_id: str, scanned_box_id: str):
    """If placeholder and scanned share a prefix and have numeric suffixes,
    return the integer offset (scanned − placeholder). Otherwise return None
    (no propagation possible).
    """
    p_prefix, p_suffix = _split_box_id(placeholder_box_id)
    s_prefix, s_suffix = _split_box_id(scanned_box_id)
    if p_prefix != s_prefix or p_suffix is None or s_suffix is None:
        return None
    if p_suffix == s_suffix:
        return 0
    return s_suffix - p_suffix


def _apply_offset(placeholder_box_id: str, offset: int) -> Optional[str]:
    """Apply an integer offset to the numeric suffix of a box_id. Returns
    the new box_id, or None if the placeholder has no numeric suffix.
    """
    prefix, suffix = _split_box_id(placeholder_box_id)
    if suffix is None:
        return None
    new_suffix = suffix + offset
    if new_suffix < 0:
        return None
    # Preserve zero-padding width if the original used it
    orig_tail = placeholder_box_id.rsplit("-", 1)[-1] if "-" in placeholder_box_id else None
    if orig_tail and orig_tail.isdigit() and len(orig_tail) > len(str(suffix)):
        pad_width = len(orig_tail)
    else:
        pad_width = len(str(suffix))
    return f"{prefix}{str(new_suffix).zfill(pad_width)}"


def _already_acknowledged(db: Session, box_id: str, transaction_no: str) -> Optional[dict]:
    """Check whether a (box_id, transaction_no) is already locked into a
    completed flow.

    NOTE: boxes_v2 is intentionally NOT checked. Transfer-In does NOT save received
    stock into cfpl/cdpl_boxes_v2 — for warehouse destinations the receipt lives ONLY
    in interunit_transfer_in_boxes. boxes_v2 is the warehouse's own source/inward
    stock, so a box sitting there is NOT "already received via a transfer" — checking
    it caused false 409s ("already received into cfpl_boxes_v2"). Only the cold
    destination (cold→cold) is a genuine stock-table landing spot.

    Returns:
      None if available, else {"location": "destination_cold", ...}.
    """
    # Already in destination cold_stocks? (genuine landing spot for cold→cold)
    for tbl in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
        if not _table_exists(db, tbl):
            continue
        hit = db.execute(
            text(f"SELECT id, unit FROM {tbl} WHERE box_id = :b AND transaction_no = :t LIMIT 1"),
            {"b": box_id, "t": transaction_no},
        ).fetchone()
        if hit:
            return {"location": "destination_cold", "table": tbl, "row_id": hit.id, "unit": hit.unit}
    return None


def _find_active_pending_row(db: Session, box_id: str, transaction_no: str):
    """Find an 'In Transit' pending_transfer_stock row by (box_id, transaction_no).
    Returns the row or None.
    """
    row = db.execute(
        text("""
            SELECT * FROM pending_transfer_stock
            WHERE box_id = :b AND transaction_no = :t AND status = 'In Transit'
            LIMIT 1
        """),
        {"b": box_id, "t": transaction_no},
    ).fetchone()
    return row


def _find_unreconciled_placeholder(
    db: Session,
    transfer_out_id: int,
    transaction_no: str,
):
    """Pick the first unreconciled pending row for this transfer_out + txn
    (FIFO by id). Returns the row or None.
    """
    return db.execute(
        text("""
            SELECT * FROM pending_transfer_stock
            WHERE transfer_out_id = :tid
              AND transaction_no = :t
              AND status = 'In Transit'
              AND COALESCE(reconciled, FALSE) = FALSE
            ORDER BY id ASC
            LIMIT 1
        """),
        {"tid": transfer_out_id, "t": transaction_no},
    ).fetchone()


def _write_disposition(
    db: Session,
    *,
    box_id: str,
    transaction_no: str,
    lot_no: Optional[str],
    item_description: Optional[str],
    from_company: Optional[str],
    unit: Optional[str],
    from_site: Optional[str],
    source_table: Optional[str],
    disposition_type: str,
    disposition_ref_table: Optional[str] = None,
    disposition_ref_id: Optional[int] = None,
    disposition_ref_no: Optional[str] = None,
    disposed_by: Optional[str] = None,
    snapshot_data: Optional[dict] = None,
    notes: Optional[str] = None,
) -> Optional[int]:
    """Append a row to cold_stock_disposition recording why a box left source.

    `disposition_type` is one of:
      'transfer_out_pending' | 'direct_out' | 'job_work_out'
      | 'consumption' | 'outward' | 'manual_correction'
    """
    try:
        rec_id = db.execute(
            text("""
                INSERT INTO cold_stock_disposition
                    (box_id, transaction_no, lot_no, item_description,
                     from_company, unit, from_site, source_table,
                     disposition_type, disposition_ref_table,
                     disposition_ref_id, disposition_ref_no,
                     disposed_by, snapshot_data, notes)
                VALUES
                    (:bid, :txn, :lot, :item,
                     :fc, :unit, :fs, :st,
                     :dtype, :dref_tbl,
                     :dref_id, :dref_no,
                     :dby, CAST(:snap AS JSONB), :notes)
                RETURNING id
            """),
            {
                "bid": box_id, "txn": transaction_no, "lot": lot_no, "item": item_description,
                "fc": from_company, "unit": unit, "fs": from_site, "st": source_table,
                "dtype": disposition_type,
                "dref_tbl": disposition_ref_table, "dref_id": disposition_ref_id,
                "dref_no": disposition_ref_no,
                "dby": disposed_by,
                "snap": json.dumps(snapshot_data) if snapshot_data else None,
                "notes": notes,
            },
        ).scalar()
        return int(rec_id) if rec_id is not None else None
    except Exception as e:
        # Disposition is audit-only — never block the source deduction it accompanies.
        logger.warning(
            "Disposition write skipped (box=%s, txn=%s, type=%s): %s",
            box_id, transaction_no, disposition_type, e,
        )
        return None


def _revert_disposition(
    db: Session,
    *,
    box_id: str,
    transaction_no: str,
    disposition_type: str,
    reverted_reason: Optional[str] = None,
) -> int:
    """Mark the active (reverted=false) disposition row(s) for this
    (box_id, transaction_no, disposition_type) as reverted. Returns
    the number of rows updated. No-op + 0 if no active row exists.
    """
    try:
        result = db.execute(
            text("""
                UPDATE cold_stock_disposition
                SET reverted = TRUE,
                    reverted_at = CURRENT_TIMESTAMP,
                    reverted_reason = :reason
                WHERE box_id = :bid
                  AND transaction_no = :txn
                  AND disposition_type = :dtype
                  AND COALESCE(reverted, FALSE) = FALSE
            """),
            {"bid": box_id, "txn": transaction_no, "dtype": disposition_type,
             "reason": reverted_reason},
        )
        return int(result.rowcount or 0)
    except Exception as e:
        logger.warning("Disposition revert skipped: %s", e)
        return 0


def _restore_box_to_source(
    db: Session,
    source_table: str,
    box_id: str,
    transaction_no: str,
    cold_storage_data: Optional[dict],
    gross_weight: Optional[float],
    net_weight: Optional[float],
    article: Optional[str],
    item_description: str,
    lot_no: Optional[str],
    weight_kg: float,
    no_of_cartons: int,
    from_site: str,
) -> None:
    """INSERT a previously-deducted box back into its source table.
    Used by STBR when the IMS-picked box wasn't actually shipped.
    """
    if not source_table or not _table_exists(db, source_table):
        logger.warning("STBR restore: source table %s missing for box %s", source_table, box_id)
        return
    if source_table.endswith("_cold_stocks"):
        cd = cold_storage_data or {}
        db.execute(
            text(f"""
                INSERT INTO {source_table}
                    (inward_dt, unit, inward_no, item_description, item_mark,
                     vakkal, lot_no, no_of_cartons, weight_kg, total_inventory_kgs,
                     group_name, item_subgroup, storage_location, exporter,
                     last_purchase_rate, value, box_id, transaction_no, spl_remarks)
                VALUES
                    (:inward_dt, :unit, :inward_no, :item_description, :item_mark,
                     :vakkal, :lot_no, :no_of_cartons, :weight_kg, :total_inventory_kgs,
                     :group_name, :item_subgroup, :storage_location, :exporter,
                     :last_purchase_rate, :value, :box_id, :transaction_no, :spl_remarks)
            """),
            {
                "inward_dt": cd.get("inward_dt"),
                "unit": cd.get("unit") or from_site,
                "inward_no": cd.get("inward_no"),
                "item_description": item_description,
                "item_mark": cd.get("item_mark"),
                "vakkal": cd.get("vakkal"),
                "lot_no": lot_no,
                "no_of_cartons": no_of_cartons or 1,
                "weight_kg": weight_kg,
                "total_inventory_kgs": cd.get("total_inventory_kgs") or float(weight_kg or 0),
                "group_name": cd.get("group_name"),
                "item_subgroup": cd.get("item_subgroup"),
                "storage_location": cd.get("storage_location") or from_site,
                "exporter": cd.get("exporter"),
                "last_purchase_rate": cd.get("last_purchase_rate"),
                "value": cd.get("value"),
                "box_id": box_id,
                "transaction_no": transaction_no,
                "spl_remarks": cd.get("spl_remarks"),
            },
        )
    else:  # boxes_v2 / bulk_entry_boxes
        db.execute(
            text(f"""
                INSERT INTO {source_table}
                    (box_id, transaction_no, article_description, lot_number,
                     net_weight, gross_weight)
                VALUES (:box_id, :transaction_no, :article, :lot_no, :net_weight, :gross_weight)
            """),
            {
                "box_id": box_id,
                "transaction_no": transaction_no,
                "article": article or item_description,
                "lot_no": lot_no,
                "net_weight": net_weight if net_weight is not None else weight_kg,
                "gross_weight": gross_weight,
            },
        )


def _swap_pending_row(
    db: Session,
    pending_row,
    new_box_id: str,
    transfer_in_id: Optional[int],
    scanned_by: Optional[str],
    scan_source: str,
    propagated_from_id: Optional[int] = None,
) -> Optional[int]:
    """Core single-box ledger flip:

      1. Restore the OLD box_id back to source (un-deduct).
      2. Find the NEW (scanned) box in source, snapshot it, then delete it.
      3. UPDATE pending_transfer_stock SET box_id=new, original_box_id=old, reconciled=TRUE.
      4. INSERT transfer_box_reconciliation row.

    Returns the reconciliation row id, or None on a soft skip (no-op same-id).
    """
    old_box_id = pending_row.box_id
    if old_box_id == new_box_id:
        # 'matched' — no swap needed; just log + flag reconciled
        rec_id = db.execute(
            text("""
                INSERT INTO transfer_box_reconciliation
                    (transfer_in_id, transfer_out_id, lot_no, transaction_no,
                     original_box_id, actual_box_id, reconciliation_status,
                     scan_source, scanned_by,
                     from_company, to_company, from_site, to_site,
                     propagated_from_id)
                VALUES
                    (:ti, :to, :lot, :txn, :orig, :act, 'matched',
                     :src, :who, :fc, :tc, :fs, :ts, :pf)
                RETURNING id
            """),
            {
                "ti": transfer_in_id, "to": pending_row.transfer_out_id,
                "lot": pending_row.lot_no, "txn": pending_row.transaction_no,
                "orig": old_box_id, "act": new_box_id,
                "src": scan_source, "who": scanned_by,
                "fc": pending_row.from_company, "tc": pending_row.to_company,
                "fs": pending_row.from_site, "ts": pending_row.to_site,
                "pf": propagated_from_id,
            },
        ).scalar()
        db.execute(
            text("""
                UPDATE pending_transfer_stock
                SET reconciled = TRUE,
                    original_box_id = COALESCE(original_box_id, :old)
                WHERE id = :id
            """),
            {"id": pending_row.id, "old": old_box_id},
        )
        return rec_id

    # Decode cold_storage_data once (used by restore + fungibility audit).
    cold_data = pending_row.cold_storage_data or None
    if isinstance(cold_data, str):
        try:
            cold_data = json.loads(cold_data)
        except Exception:
            cold_data = None

    # 1. Restore OLD box back to source — DEFERRED until we know if this is a
    #    fungible swap. We pre-compute the source-presence of the new box first,
    #    then decide: literal swap (restore old + delete new) vs fungible (skip both).
    #    We snapshot the values we'd need to restore so step 5 can do it if needed.

    # 2. Find NEW box in source, snapshot it, then delete from source.
    #    If not in source, check FUNGIBILITY against the placeholder's pool —
    #    same (transaction_no, lot_no) means the IMS box_ids are arbitrary
    #    labels for the same physical inward batch. Per user spec (May 2026):
    #    "in 1900 boxes, if direct out is done for 1200 boxes, and transfer
    #     is then made on later date and the box received has box id of the
    #     1200 direct out range then the swapping should not conflict"
    new_source_table = None
    new_cold_data = cold_data  # default: reuse old snapshot
    new_gross = pending_row.gross_weight
    new_net = pending_row.net_weight
    new_article = pending_row.article
    new_weight_kg = pending_row.weight_kg
    new_no_of_cartons = pending_row.no_of_cartons
    new_item_desc = pending_row.item_description
    new_lot_no = pending_row.lot_no
    disposition_hint: Optional[dict] = None

    if pending_row.from_storage_type == "cold":
        tbl, row = _find_in_cold_stocks(db, new_box_id, pending_row.transaction_no, pending_row.lot_no)
        if row is not None:
            new_source_table = tbl
            new_cold_data = _cold_row_to_json(row)
            new_item_desc = getattr(row, "item_description", None) or new_item_desc
            new_lot_no = getattr(row, "lot_no", None) or new_lot_no
            new_weight_kg = float(getattr(row, "weight_kg", 0) or new_weight_kg or 0)
            new_no_of_cartons = int(getattr(row, "no_of_cartons", 1) or 1)
    else:
        tbl, row = _find_in_bulk_entry(db, new_box_id, pending_row.transaction_no, pending_row.lot_no)
        if row is not None:
            new_source_table = tbl
            new_gross = float(getattr(row, "gross_weight", 0) or 0)
            new_net = float(getattr(row, "net_weight", 0) or 0)
            new_article = getattr(row, "article_description", None) or new_article

    # FUNGIBILITY CHECK — only fires when the new box wasn't found in source.
    # Within a (transaction_no, lot_no) inward batch, box_ids are arbitrary
    # labels for fungible physical boxes (same lot, same item, same vakkal).
    # If the scanned box was previously claimed by a Direct Out / Job Work
    # disposition from the same pool, this is a legitimate relabel — not a
    # source-deduction. We skip the source ops and just update the pending
    # row, recording the disposition note for full audit traceability.
    is_fungible_swap = False
    if new_source_table is None:
        disposition_hint = db.execute(
            text("""
                SELECT id, disposition_type, disposition_ref_no, disposed_at, lot_no
                FROM cold_stock_disposition
                WHERE box_id = :b
                  AND transaction_no = :t
                  AND COALESCE(reverted, FALSE) = FALSE
                ORDER BY disposed_at DESC
                LIMIT 1
            """),
            {"b": new_box_id, "t": pending_row.transaction_no},
        ).fetchone()
        # Fungibility: same (txn, lot, from_company, from_site) as placeholder.
        # txn is already equal (we matched _find_unreconciled_placeholder on it).
        # If the scanned box's disposition row exists, lot from there is
        # authoritative; otherwise we trust pool-by-txn (every row of TR-X is
        # in the same pool by construction).
        scanned_lot = disposition_hint.lot_no if disposition_hint else pending_row.lot_no
        if (not pending_row.lot_no) or (scanned_lot or "") == (pending_row.lot_no or ""):
            is_fungible_swap = True

    if new_source_table:
        # Literal swap path: deduct the scanned box from source and restore
        # the IMS placeholder back to source (it was never really shipped).
        db.execute(
            text(f"DELETE FROM {new_source_table} WHERE box_id = :b AND transaction_no = :t"),
            {"b": new_box_id, "t": pending_row.transaction_no},
        )
        _restore_box_to_source(
            db,
            source_table=pending_row.source_table,
            box_id=old_box_id,
            transaction_no=pending_row.transaction_no,
            cold_storage_data=cold_data,
            gross_weight=float(pending_row.gross_weight) if pending_row.gross_weight is not None else None,
            net_weight=float(pending_row.net_weight) if pending_row.net_weight is not None else None,
            article=pending_row.article,
            item_description=pending_row.item_description,
            lot_no=pending_row.lot_no,
            weight_kg=float(pending_row.weight_kg or 0),
            no_of_cartons=int(pending_row.no_of_cartons or 1),
            from_site=pending_row.from_site,
        )
        # The transfer_out_pending disposition still applies — same pool, just
        # different label. Re-key the existing disposition row to the new box_id
        # so the audit chain is preserved without orphaning the old label.
        try:
            db.execute(
                text("""
                    UPDATE cold_stock_disposition
                    SET box_id = :new_b,
                        notes = COALESCE(notes, '') ||
                                ' | relabel ' || :old_b || ' -> ' || :new_b
                    WHERE box_id = :old_b
                      AND transaction_no = :txn
                      AND disposition_type = 'transfer_out_pending'
                      AND COALESCE(reverted, FALSE) = FALSE
                """),
                {"old_b": old_box_id, "new_b": new_box_id, "txn": pending_row.transaction_no},
            )
        except Exception as e:
            logger.warning("Disposition relabel skipped: %s", e)

    elif is_fungible_swap:
        # Fungible swap path: no source-side ops.
        # The IMS placeholder was a bookkeeping label for ONE unit of count
        # from this inward pool. The scanned box was ALREADY consumed (per
        # disposition_hint, or just deducted by IMS FIFO at dispatch time).
        # Both share the same pool, so the pool's running count is unchanged.
        #
        # Record this relabel against the existing disposition row so a
        # query for the new box_id's history shows the original disposition
        # chain (e.g. "direct_out DO-7 ... then transfer_in TI-9").
        try:
            db.execute(
                text("""
                    INSERT INTO cold_stock_disposition
                        (box_id, transaction_no, lot_no, item_description,
                         from_company, from_site, source_table,
                         disposition_type, disposition_ref_table,
                         disposition_ref_no, disposed_by, notes)
                    VALUES
                        (:b, :t, :lot, :item, :fc, :fs, :st,
                         'transfer_out_pending', 'pending_transfer_stock',
                         :ref_no, :who,
                         'fungible_relabel from ' || :old_b
                         || COALESCE(' (prior: ' || :prior || ')', ''))
                """),
                {
                    "b": new_box_id, "t": pending_row.transaction_no,
                    "lot": pending_row.lot_no, "item": pending_row.item_description,
                    "fc": pending_row.from_company, "fs": pending_row.from_site,
                    "st": pending_row.source_table,
                    "ref_no": pending_row.transfer_out_challan_no,
                    "who": scanned_by,
                    "old_b": old_box_id,
                    "prior": disposition_hint.disposition_type if disposition_hint else None,
                },
            )
        except Exception as e:
            logger.warning("Fungible disposition write skipped: %s", e)
    # else: neither found nor fungible — overridden_no_source path
    # (no source ops; reconciliation row will flag it as conflict-like).

    # 3. UPDATE pending row to point at the new physical box
    db.execute(
        text("""
            UPDATE pending_transfer_stock
            SET box_id = :new_b,
                original_box_id = COALESCE(original_box_id, :old_b),
                reconciled = TRUE,
                source_table = COALESCE(:new_st, source_table),
                cold_storage_data = CAST(:csd AS JSONB),
                gross_weight = COALESCE(:gw, gross_weight),
                net_weight = COALESCE(:nw, net_weight),
                article = COALESCE(:art, article),
                item_description = :item_desc,
                lot_no = COALESCE(:lot, lot_no),
                weight_kg = :wkg,
                no_of_cartons = :noc
            WHERE id = :id
        """),
        {
            "id": pending_row.id,
            "new_b": new_box_id,
            "old_b": old_box_id,
            "new_st": new_source_table,
            "csd": json.dumps(new_cold_data) if new_cold_data else None,
            "gw": new_gross,
            "nw": new_net,
            "art": new_article,
            "item_desc": new_item_desc,
            "lot": new_lot_no,
            "wkg": new_weight_kg,
            "noc": new_no_of_cartons,
        },
    )

    # 4. INSERT reconciliation audit row
    if new_source_table:
        status = "overridden"
        reason = None
    elif is_fungible_swap:
        status = "fungible_swap"
        prior_disp = (
            f"prior disposition: {disposition_hint.disposition_type} "
            f"(ref={disposition_hint.disposition_ref_no})"
            if disposition_hint else "no prior disposition"
        )
        reason = (
            f"Fungible relabel within pool (txn={pending_row.transaction_no}, "
            f"lot={pending_row.lot_no}). {prior_disp}. No source restore/deduct needed — "
            f"box_ids within an inward batch are arbitrary labels."
        )
    else:
        status = "overridden_no_source"
        reason = "Scanned box not found in any source table and pool fungibility check failed"

    rec_id = db.execute(
        text("""
            INSERT INTO transfer_box_reconciliation
                (transfer_in_id, transfer_out_id, lot_no, transaction_no,
                 original_box_id, actual_box_id, reconciliation_status,
                 conflict_reason, scan_source, scanned_by,
                 from_company, to_company, from_site, to_site,
                 propagated_from_id)
            VALUES
                (:ti, :to, :lot, :txn, :orig, :act, :status,
                 :reason, :src, :who, :fc, :tc, :fs, :ts, :pf)
            RETURNING id
        """),
        {
            "ti": transfer_in_id, "to": pending_row.transfer_out_id,
            "lot": new_lot_no, "txn": pending_row.transaction_no,
            "orig": old_box_id, "act": new_box_id,
            "status": status,
            "reason": reason,
            "src": scan_source, "who": scanned_by,
            "fc": pending_row.from_company, "tc": pending_row.to_company,
            "fs": pending_row.from_site, "ts": pending_row.to_site,
            "pf": propagated_from_id,
        },
    ).scalar()
    return rec_id


def reconcile_box_in_pending(
    db: Session,
    scanned_box_id: str,
    scanned_transaction_no: str,
    transfer_in_header_id: int,
    transfer_out_id: int,
    scan_source: str = "qr_scan",
    scanned_by: Optional[str] = None,
) -> dict:
    """STBR (Scan-Time Box ID Reconciliation) entry point.

    Called BEFORE the existing acknowledge UPSERT. Validates the scanned
    box, swaps the placeholder pending row to point at the actual box,
    restores the IMS-picked box back to source, and — when a clean
    arithmetic offset is detected — propagates the same swap to all
    unreconciled siblings in the same (transfer_out_id, transaction_no,
    lot_no) batch.

    Returns:
        {
          "status":            "matched" | "overridden" | "propagated"
                               | "conflict" | "duplicate",
          "original_box_id":   str | None,
          "actual_box_id":     str,
          "transfer_out_id":   int | None,
          "lot_no":            str | None,
          "propagated_count":  int,
          "reason":            str | None,
          "reconciliation_id": int | None,
          "siblings":          [ {old: ..., new: ...}, ... ],
        }
    """
    _ensure_reconciliation_schema(db)

    scanned_box_id = (scanned_box_id or "").strip()
    scanned_transaction_no = (scanned_transaction_no or "").strip()
    if not scanned_box_id or not scanned_transaction_no:
        return {
            "status": "conflict",
            "actual_box_id": scanned_box_id, "original_box_id": None,
            "transfer_out_id": transfer_out_id, "lot_no": None,
            "propagated_count": 0,
            "reason": "Empty box_id or transaction_no",
            "reconciliation_id": None, "siblings": [],
        }

    # STEP 1 — Already locked into a destination or another transfer?
    lock = _already_acknowledged(db, scanned_box_id, scanned_transaction_no)
    if lock and lock.get("location", "").startswith("destination_"):
        return {
            "status": "duplicate",
            "actual_box_id": scanned_box_id, "original_box_id": None,
            "transfer_out_id": transfer_out_id, "lot_no": None,
            "propagated_count": 0,
            "reason": f"Box already received into {lock.get('table')}",
            "reconciliation_id": None, "siblings": [],
        }

    # STEP 2 — Is the scanned box already in THIS active pending batch?
    existing = _find_active_pending_row(db, scanned_box_id, scanned_transaction_no)
    if existing is not None:
        if existing.transfer_out_id == transfer_out_id:
            # Already in our pending — likely an exact-match scan. Log + flag.
            rec_id = _swap_pending_row(
                db, existing, scanned_box_id, transfer_in_header_id,
                scanned_by, scan_source, propagated_from_id=None,
            )
            return {
                "status": "matched",
                "actual_box_id": scanned_box_id,
                "original_box_id": existing.box_id,
                "transfer_out_id": transfer_out_id,
                "lot_no": existing.lot_no, "propagated_count": 0,
                "reason": None, "reconciliation_id": rec_id, "siblings": [],
            }
        else:
            return {
                "status": "duplicate",
                "actual_box_id": scanned_box_id, "original_box_id": None,
                "transfer_out_id": transfer_out_id, "lot_no": None,
                "propagated_count": 0,
                "reason": f"Box belongs to a different active transfer (transfer_out_id={existing.transfer_out_id})",
                "reconciliation_id": None, "siblings": [],
            }

    # STEP 3 — Find an unreconciled placeholder slot in THIS batch
    placeholder = _find_unreconciled_placeholder(db, transfer_out_id, scanned_transaction_no)
    if placeholder is None:
        return {
            "status": "conflict",
            "actual_box_id": scanned_box_id, "original_box_id": None,
            "transfer_out_id": transfer_out_id, "lot_no": None,
            "propagated_count": 0,
            "reason": "No unreconciled slot available for this transaction_no in this transfer (over-scan or wrong txn)",
            "reconciliation_id": None, "siblings": [],
        }

    # STEP 4 — Swap this single box (restore old, deduct new, update pending, audit)
    primary_rec_id = _swap_pending_row(
        db, placeholder, scanned_box_id, transfer_in_header_id,
        scanned_by, scan_source, propagated_from_id=None,
    )

    # STEP 5 — Try arithmetic series propagation to remaining siblings.
    # Narrow scope (user decision): (transfer_out_id, transaction_no, lot_no).
    offset = _series_offset(placeholder.box_id, scanned_box_id)
    siblings_remapped: list = []
    if offset is not None and offset != 0:
        sibling_rows = db.execute(
            text("""
                SELECT * FROM pending_transfer_stock
                WHERE transfer_out_id = :tid
                  AND transaction_no = :txn
                  AND COALESCE(lot_no, '') = COALESCE(:lot, '')
                  AND status = 'In Transit'
                  AND COALESCE(reconciled, FALSE) = FALSE
                ORDER BY id ASC
            """),
            {"tid": transfer_out_id, "txn": scanned_transaction_no,
             "lot": placeholder.lot_no},
        ).fetchall()
        for sib in sibling_rows:
            predicted = _apply_offset(sib.box_id, offset)
            if not predicted:
                continue
            # Verify the predicted box actually exists in source — otherwise skip
            if sib.from_storage_type == "cold":
                _, src_row = _find_in_cold_stocks(db, predicted, sib.transaction_no, sib.lot_no)
            else:
                _, src_row = _find_in_bulk_entry(db, predicted, sib.transaction_no, sib.lot_no)
            if src_row is None:
                continue
            # Skip if predicted box is already locked elsewhere
            if _already_acknowledged(db, predicted, sib.transaction_no):
                continue
            try:
                _swap_pending_row(
                    db, sib, predicted, transfer_in_header_id,
                    scanned_by, scan_source="auto_match",
                    propagated_from_id=primary_rec_id,
                )
                siblings_remapped.append({"old": sib.box_id, "new": predicted})
            except Exception as e:
                logger.warning("STBR propagation skipped for sibling %s -> %s: %s",
                               sib.box_id, predicted, e)

    return {
        "status": "propagated" if siblings_remapped else "overridden",
        "actual_box_id": scanned_box_id,
        "original_box_id": placeholder.box_id,
        "transfer_out_id": transfer_out_id,
        "lot_no": placeholder.lot_no,
        "propagated_count": len(siblings_remapped),
        "reason": None,
        "reconciliation_id": primary_rec_id,
        "siblings": siblings_remapped,
    }


# ----------------------------------------------------------------------------
#  park_in_pending — called from create_transfer
# ----------------------------------------------------------------------------
def park_in_pending(
    transfer_out_id: int,
    challan_no: str,
    from_site: str,
    to_site: str,
    boxes: list,
    dispatched_by: str,
    db: Session,
    transfer_type: str = "INTERUNIT",
) -> int:
    """Deduct each box from its source table and insert a corresponding row in
    pending_transfer_stock (status='In Transit'). Returns count parked."""
    from_storage_type = "cold" if _is_cold_site(from_site) else "warehouse"
    to_storage_type = "cold" if _is_cold_site(to_site) else "warehouse"
    parked = 0
    now = datetime.now()
    # Stamp rows with the transfer-out's actual initiation date (header
    # stock_trf_date), not the park/sync time, so the stored ledger is faithful.
    _hdr_row = db.execute(
        text("SELECT stock_trf_date, created_ts FROM interunit_transfers_header WHERE id = :id"),
        {"id": transfer_out_id},
    ).fetchone()
    dispatched_at = (_hdr_row[0] or _hdr_row[1] or now) if _hdr_row else now

    for box in boxes:
        box_id = (getattr(box, "box_id", "") or "").strip()
        transaction_no = (getattr(box, "transaction_no", "") or "").strip()
        if not box_id or not transaction_no or transaction_no == "DIRECT":
            logger.info("PARK_PENDING: skip box without id/txn (box_id=%r, tno=%r)", box_id, transaction_no)
            continue

        source_table, source_row = (None, None)
        cold_data = None
        warehouse_data = {}

        if from_storage_type == "cold":
            source_table, source_row = _find_in_cold_stocks(db, box_id, transaction_no, getattr(box, "lot_number", None))
            if source_row is None:
                logger.warning("PARK_PENDING: no cold_stocks match for box_id=%s tno=%s", box_id, transaction_no)
                continue
            cold_data = _cold_row_to_json(source_row)
            item_description = getattr(source_row, "item_description", None) or getattr(box, "article", "")
            lot_no = getattr(source_row, "lot_no", None) or getattr(box, "lot_number", None)
            weight_kg = float(getattr(source_row, "weight_kg", 0) or getattr(box, "net_weight", 0) or 0)
            no_of_cartons = int(getattr(source_row, "no_of_cartons", 1) or 1)
            # Cold-source rows previously left these flat columns NULL — carry them
            # so the pending row stands alone without parsing cold_storage_data.
            art = item_description
            net_w = weight_kg or None
            gross_w = float(getattr(source_row, "gross_weight", 0) or 0) or None
            batch_no = getattr(source_row, "batch_number", None) or getattr(box, "batch_number", None)
        else:
            source_table, source_row = _find_in_bulk_entry(db, box_id, transaction_no, getattr(box, "lot_number", None))
            if source_row is None:
                logger.warning("PARK_PENDING: no bulk_entry_boxes match for box_id=%s tno=%s", box_id, transaction_no)
                continue
            item_description = getattr(source_row, "article_description", None) or getattr(box, "article", "")
            lot_no = getattr(source_row, "lot_number", None) or getattr(box, "lot_number", None)
            weight_kg = float(getattr(source_row, "net_weight", 0) or getattr(box, "net_weight", 0) or 0)
            no_of_cartons = 1
            warehouse_data = {
                "gross_weight": float(getattr(source_row, "gross_weight", 0) or 0),
                "net_weight": weight_kg,
                "article": getattr(source_row, "article_description", None),
            }
            art = warehouse_data["article"]
            net_w = warehouse_data["net_weight"]
            gross_w = warehouse_data["gross_weight"]
            batch_no = getattr(source_row, "batch_number", None) or getattr(box, "batch_number", None)

        from_company = _company_from_table(source_table)
        to_company = from_company  # default; may be overridden by destination table later
        destination_table = _destination_table(to_storage_type, to_company)

        db.execute(
            text("""
                INSERT INTO pending_transfer_stock
                    (transfer_type, transfer_out_id, transfer_out_challan_no,
                     box_id, transaction_no,
                     from_company, to_company, from_site, to_site,
                     from_storage_type, to_storage_type,
                     source_table, source_row_id, destination_table,
                     item_description, lot_no, batch_number, weight_kg, no_of_cartons,
                     cold_storage_data,
                     gross_weight, net_weight, article,
                     status, dispatched_at, dispatched_by)
                VALUES
                    (:transfer_type, :transfer_out_id, :challan_no,
                     :box_id, :transaction_no,
                     :from_company, :to_company, :from_site, :to_site,
                     :from_storage_type, :to_storage_type,
                     :source_table, :source_row_id, :destination_table,
                     :item_description, :lot_no, :batch_number, :weight_kg, :no_of_cartons,
                     CAST(:cold_storage_data AS JSONB),
                     :gross_weight, :net_weight, :article,
                     'In Transit', :dispatched_at, :dispatched_by)
                ON CONFLICT (box_id, transaction_no) DO NOTHING
            """),
            {
                "transfer_type": transfer_type,
                "transfer_out_id": transfer_out_id,
                "challan_no": challan_no,
                "box_id": box_id,
                "transaction_no": transaction_no,
                "from_company": from_company,
                "to_company": to_company,
                "from_site": from_site,
                "to_site": to_site,
                "from_storage_type": from_storage_type,
                "to_storage_type": to_storage_type,
                "source_table": source_table,
                "source_row_id": getattr(source_row, "id", None),
                "destination_table": destination_table,
                "item_description": item_description,
                "lot_no": lot_no,
                "batch_number": batch_no,
                "weight_kg": weight_kg,
                "no_of_cartons": no_of_cartons,
                "cold_storage_data": json.dumps(cold_data) if cold_data else None,
                "gross_weight": gross_w,
                "net_weight": net_w,
                "article": art,
                "dispatched_at": dispatched_at,
                "dispatched_by": dispatched_by,
            },
        )

        # Delete from source table (atomic with pending insert via outer transaction)
        db.execute(
            text(f"DELETE FROM {source_table} WHERE id = :rid"),
            {"rid": getattr(source_row, "id")},
        )

        # Disposition ledger — audit trail of why this box left source.
        # `reverted=true` later by restore_to_source on cancel.
        _ensure_reconciliation_schema(db)
        _write_disposition(
            db,
            box_id=box_id,
            transaction_no=transaction_no,
            lot_no=lot_no,
            item_description=item_description,
            from_company=from_company,
            unit=getattr(source_row, "unit", None) if from_storage_type == "cold" else None,
            from_site=from_site,
            source_table=source_table,
            disposition_type="transfer_out_pending",
            disposition_ref_table="pending_transfer_stock",
            disposition_ref_id=None,
            disposition_ref_no=challan_no,
            disposed_by=dispatched_by,
            snapshot_data=cold_data if cold_data else (warehouse_data if warehouse_data else None),
            notes=f"transfer_out_id={transfer_out_id}",
        )

        parked += 1
        logger.info(
            "PARK_PENDING: box_id=%s tno=%s source=%s → pending (transfer_out_id=%s)",
            box_id, transaction_no, source_table, transfer_out_id,
        )

    logger.info("PARK_PENDING: parked %d/%d boxes for transfer_out_id=%s", parked, len(boxes), transfer_out_id)
    return parked


# ----------------------------------------------------------------------------
#  park_lines_in_pending — fallback for box-less (quantity/article-only) transfers
#
#  Some transfers carry no per-box rows: warehouse "article-only" dispatches where
#  the operator types quantities and the stock is not box-tracked (so Plan C box
#  derivation finds nothing). Those used to leave NO trace in pending_transfer_stock,
#  so they were absent from the pending ledger (only surfaced via the header-fallback
#  UNION in list_pending_transfers). This parks one In-Transit row PER UNIT from the
#  transfer lines so every transfer is represented in the pending table.
#
#  These rows are TRACKING-ONLY: source_table/destination_table are empty sentinels,
#  so they deduct nothing on dispatch (restore_to_source no-ops on the missing table)
#  and insert nothing on receive (pick_from_pending just deletes them, keyed by
#  transfer_out_id). They therefore do NOT change inventory math — they only make the
#  in-transit ledger complete. box_id is synthetic ("LINE-<line_id>-<n>") and unique,
#  so the uq_in_transit_box (box_id, transaction_no) constraint is satisfied.
# ----------------------------------------------------------------------------
def park_lines_in_pending(
    transfer_out_id: int,
    challan_no: str,
    from_site: str,
    to_site: str,
    lines: list,
    dispatched_by: str,
    db: Session,
    transfer_type: str = "INTERUNIT",
) -> int:
    """Insert one 'In Transit' pending row per unit for a box-less transfer.
    Returns count parked. Tracking-only: no source/destination inventory change."""
    from_storage_type = "cold" if _is_cold_site(from_site) else "warehouse"
    to_storage_type = "cold" if _is_cold_site(to_site) else "warehouse"
    now = datetime.now()
    # Use the transfer-out's actual initiation date (header stock_trf_date),
    # not the park/sync time, so the stored ledger is faithful.
    _hdr_row = db.execute(
        text("SELECT stock_trf_date, created_ts FROM interunit_transfers_header WHERE id = :id"),
        {"id": transfer_out_id},
    ).fetchone()
    dispatched_at = (_hdr_row[0] or _hdr_row[1] or now) if _hdr_row else now
    parked = 0

    for line in lines:
        article = (getattr(line, "item_desc_raw", "") or "").strip()
        qty = int(getattr(line, "qty", 0) or 0)
        if not article or qty <= 0:
            continue

        line_id = getattr(line, "id", None)
        total_net = float(getattr(line, "net_weight", 0) or 0)
        total_gross = float(getattr(line, "total_weight", 0) or 0) or total_net
        per_unit_net = round(total_net / qty, 3) if qty else total_net
        per_unit_gross = round(total_gross / qty, 3) if qty else total_gross
        lot_no = (getattr(line, "lot_number", "") or "") or None
        batch_no = (getattr(line, "batch_number", "") or "") or None

        for n in range(1, qty + 1):
            db.execute(
                text("""
                    INSERT INTO pending_transfer_stock
                        (transfer_type, transfer_out_id, transfer_out_challan_no,
                         box_id, transaction_no,
                         from_company, to_company, from_site, to_site,
                         from_storage_type, to_storage_type,
                         source_table, source_row_id, destination_table,
                         item_description, lot_no, batch_number, weight_kg, no_of_cartons,
                         rm_pm_fg_type, item_category, sub_category,
                         pack_size, unit_pack_size, qty, uom,
                         net_weight, gross_weight, total_weight, article,
                         status, dispatched_at, dispatched_by)
                    VALUES
                        (:transfer_type, :transfer_out_id, :challan_no,
                         :box_id, :transaction_no,
                         '', '', :from_site, :to_site,
                         :from_storage_type, :to_storage_type,
                         '', NULL, '',
                         :item_description, :lot_no, :batch_number, :weight_kg, 1,
                         :rm_pm_fg_type, :item_category, :sub_category,
                         :pack_size, :unit_pack_size, 1, :uom,
                         :net_weight, :gross_weight, :total_weight, :article,
                         'In Transit', :dispatched_at, :dispatched_by)
                    ON CONFLICT (box_id, transaction_no) DO NOTHING
                """),
                {
                    "transfer_type": transfer_type,
                    "transfer_out_id": transfer_out_id,
                    "challan_no": challan_no,
                    "box_id": f"LINE-{line_id}-{n}",
                    "transaction_no": challan_no,
                    "from_site": from_site,
                    "to_site": to_site,
                    "from_storage_type": from_storage_type,
                    "to_storage_type": to_storage_type,
                    "item_description": article,
                    "lot_no": lot_no,
                    "batch_number": batch_no,
                    "weight_kg": per_unit_net,
                    "rm_pm_fg_type": getattr(line, "rm_pm_fg_type", None),
                    "item_category": getattr(line, "item_category", None),
                    "sub_category": getattr(line, "sub_category", None),
                    "pack_size": float(getattr(line, "pack_size", 0) or 0) or None,
                    "unit_pack_size": float(getattr(line, "unit_pack_size", 0) or 0) or None,
                    "uom": getattr(line, "uom", None),
                    "net_weight": per_unit_net,
                    "gross_weight": per_unit_gross,
                    "total_weight": per_unit_net,
                    "article": article,
                    "dispatched_at": dispatched_at,
                    "dispatched_by": dispatched_by,
                },
            )
            parked += 1

    logger.info(
        "PARK_LINES_PENDING: parked %d unit-rows from %d line(s) for transfer_out_id=%s",
        parked, len(lines), transfer_out_id,
    )
    return parked


def _guess_company_from_site(site: Optional[str]) -> str:
    """Same heuristic as backfill: Rishi/CDPL sites → cdpl, else cfpl."""
    s = (site or "").strip().lower()
    return "cdpl" if ("rishi" in s or "cdpl" in s) else "cfpl"


def _park_cold_row(db: Session, hdr, transfer_out_id: int, source_table: str, src,
                   box_id: str, from_storage_type: str, to_storage_type: str, now) -> None:
    """Insert one In-Transit pending row for a by-lot-matched cold_stocks row and
    deduct (DELETE) it from source. Mirrors park_in_pending's cold-source path."""
    cold_data = _cold_row_to_json(src)
    from_company = _company_from_table(source_table)
    to_company = from_company
    db.execute(
        text("""
            INSERT INTO pending_transfer_stock
                (transfer_type, transfer_out_id, transfer_out_challan_no, box_id, transaction_no,
                 from_company, to_company, from_site, to_site, from_storage_type, to_storage_type,
                 source_table, source_row_id, destination_table, item_description, lot_no,
                 batch_number, weight_kg, no_of_cartons, net_weight, article,
                 cold_storage_data, status, dispatched_at, dispatched_by)
            VALUES
                (:tt, :toid, :chal, :bid, :tno, :fc, :tc, :fs, :ts, :fst, :tst,
                 :src, :srid, :dst, :item, :lot, :batch, :wt, :noc, :netw, :art,
                 CAST(:cd AS JSONB), 'In Transit', :da, :db_)
            ON CONFLICT (box_id, transaction_no) DO NOTHING
        """),
        {
            "tt": "INTERUNIT", "toid": transfer_out_id, "chal": hdr.challan_no,
            "bid": box_id, "tno": hdr.challan_no, "fc": from_company, "tc": to_company,
            "fs": hdr.from_site, "ts": hdr.to_site, "fst": from_storage_type,
            "tst": to_storage_type, "src": source_table,
            "srid": getattr(src, "id", None),
            "dst": _destination_table(to_storage_type, to_company),
            "item": getattr(src, "item_description", None) or "",
            "lot": getattr(src, "lot_no", None),
            "batch": getattr(src, "batch_number", None),
            "wt": float(getattr(src, "weight_kg", 0) or 0),
            "noc": int(getattr(src, "no_of_cartons", 1) or 1),
            "netw": float(getattr(src, "weight_kg", 0) or 0) or None,
            "art": getattr(src, "item_description", None) or None,
            "cd": json.dumps(cold_data) if cold_data else None,
            "da": getattr(hdr, "stock_trf_date", None) or getattr(hdr, "created_ts", None) or now,
            "db_": getattr(hdr, "created_by", None) or "reconcile",
        },
    )
    db.execute(text(f"DELETE FROM {source_table} WHERE id = :rid"),
               {"rid": getattr(src, "id")})

    # Audit trail (parity with park_in_pending) so the deduction is traceable and
    # restore_to_source can close the disposition loop on cancel/edit.
    _ensure_reconciliation_schema(db)
    _write_disposition(
        db,
        box_id=box_id,
        transaction_no=hdr.challan_no,
        lot_no=getattr(src, "lot_no", None),
        item_description=getattr(src, "item_description", None) or "",
        from_company=from_company,
        unit=getattr(src, "unit", None),
        from_site=hdr.from_site,
        source_table=source_table,
        disposition_type="transfer_out_pending",
        disposition_ref_table="pending_transfer_stock",
        disposition_ref_id=None,
        disposition_ref_no=hdr.challan_no,
        disposed_by=getattr(hdr, "created_by", None) or "reconcile",
        snapshot_data=cold_data,
        notes=f"transfer_out_id={transfer_out_id} (reconcile by-lot)",
    )


def _restore_pending_row(db: Session, p, dry_run: bool = False) -> None:
    """Restore ONE pending_transfer_stock row to its source cold_stocks and delete the
    pending row. Used by reconcile to undo wrong-lot / excess parks (cold sources).
    Mirrors restore_to_source's cold branch for a single row."""
    src = getattr(p, "source_table", None)
    if dry_run:
        return
    if not src or not _table_exists(db, src):
        db.execute(text("DELETE FROM pending_transfer_stock WHERE id = :id"), {"id": p.id})
        return
    if src.endswith("_cold_stocks"):
        cold_json = getattr(p, "cold_storage_data", None) or {}
        db.execute(
            text(f"""
                INSERT INTO {src}
                    (inward_dt, unit, inward_no, item_description, item_mark, vakkal, lot_no,
                     no_of_cartons, weight_kg, total_inventory_kgs, group_name, item_subgroup,
                     storage_location, exporter, last_purchase_rate, value,
                     box_id, transaction_no, spl_remarks)
                VALUES
                    (:inward_dt, :unit, :inward_no, :item_description, :item_mark, :vakkal, :lot_no,
                     :no_of_cartons, :weight_kg, :total_inventory_kgs, :group_name, :item_subgroup,
                     :storage_location, :exporter, :last_purchase_rate, :value,
                     :box_id, :transaction_no, :spl_remarks)
                ON CONFLICT DO NOTHING
            """),
            {
                "inward_dt": cold_json.get("inward_dt"), "unit": cold_json.get("unit") or p.from_site,
                "inward_no": cold_json.get("inward_no"), "item_description": p.item_description,
                "item_mark": cold_json.get("item_mark"), "vakkal": cold_json.get("vakkal"),
                "lot_no": p.lot_no, "no_of_cartons": p.no_of_cartons or 1, "weight_kg": p.weight_kg,
                "total_inventory_kgs": cold_json.get("total_inventory_kgs") or float(p.weight_kg or 0),
                "group_name": cold_json.get("group_name"), "item_subgroup": cold_json.get("item_subgroup"),
                "storage_location": cold_json.get("storage_location") or p.from_site,
                "exporter": cold_json.get("exporter"), "last_purchase_rate": cold_json.get("last_purchase_rate"),
                "value": cold_json.get("value"), "box_id": p.box_id,
                "transaction_no": p.transaction_no, "spl_remarks": cold_json.get("spl_remarks"),
            },
        )
    _revert_disposition(db, box_id=p.box_id, transaction_no=p.transaction_no,
                        disposition_type="transfer_out_pending",
                        reverted_reason="reconcile: wrong-lot/excess restore to source")
    db.execute(text("DELETE FROM pending_transfer_stock WHERE id = :id"), {"id": p.id})


def reconcile_transfer_to_order(transfer_out_id: int, db: Session,
                                dry_run: bool = False) -> dict:
    """FLAG-ONLY accounting reconcile for one transfer.

    Clean-accounting policy (per ops decision): the parked rows in pending_transfer_stock
    ARE the physical in-transit truth. We do NOT move/pull cold_stocks to make the count
    match the order — that would mark physically-present boxes as shipped and corrupt the
    available count. Instead we compare the order (lines) to what actually shipped (parked)
    and record the net gap on the header (unallocated_boxes) for review. Future over-orders
    are blocked at dispatch (see the over-order guard in create/update_transfer).

    Returns: {transfer_out_id, allocated(=0), unallocated(=net gap), total_ordered,
              total_parked, groups:[{lot, item, ordered, parked, shortfall}]}.
    Writes nothing except the unallocated_boxes flag (skipped when dry_run=True)."""
    report = {"transfer_out_id": transfer_out_id, "allocated": 0,
              "unallocated": 0, "groups": []}
    if not _table_exists(db, "pending_transfer_stock"):
        return report

    hdr = db.execute(
        text("""SELECT id, challan_no, from_site, to_site, created_by, created_ts
                FROM interunit_transfers_header WHERE id = :tid"""),
        {"tid": transfer_out_id},
    ).fetchone()
    if not hdr:
        return report

    # Receiving-aware safety: once any box has been received (GRN started), the parked
    # In-Transit count legitimately drops below ordered as boxes move to destination.
    # Topping up here would re-deduct already-received stock (double count), so skip the
    # by-lot fill for transfers whose receipt has begun. The receive path keeps its own
    # tables in sync via pick_from_pending; reconcile only owns the pre-receipt invariant.
    received = db.execute(
        text("""SELECT COUNT(tib.id)
                FROM interunit_transfer_in_header tih
                JOIN interunit_transfer_in_boxes tib ON tib.header_id = tih.id
                WHERE tih.transfer_out_id = :tid"""),
        {"tid": transfer_out_id},
    ).scalar() or 0
    if received:
        report["skipped_receiving_in_progress"] = True
        report["received"] = int(received)
        logger.info("RECONCILE: transfer_out_id=%s skipped — %s box(es) already received",
                    transfer_out_id, received)
        return report

    from_storage_type = "cold" if _is_cold_site(hdr.from_site) else "warehouse"
    to_storage_type = "cold" if _is_cold_site(hdr.to_site) else "warehouse"

    # Ordered lots/qtys — the authoritative truth (cold->warehouse: ordered == shipped).
    ordered_rows = db.execute(
        text("""SELECT lot_number AS lot_no, MIN(item_desc_raw) AS item_description,
                       COALESCE(SUM(qty),0) AS ordered
                FROM interunit_transfers_lines WHERE header_id = :tid
                GROUP BY lot_number"""),
        {"tid": transfer_out_id},
    ).fetchall()
    ordered = {(o.lot_no or ""): int(o.ordered or 0) for o in ordered_rows if int(o.ordered or 0) > 0}
    item_by_lot = {(o.lot_no or ""): (o.item_description or "") for o in ordered_rows}

    # Current parked rows (full rows so wrong-lot / excess ones can be restored).
    parked_all = db.execute(
        text("""SELECT * FROM pending_transfer_stock
                WHERE transfer_out_id = :tid AND status = 'In Transit'"""),
        {"tid": transfer_out_id},
    ).fetchall()
    parked_by_lot: dict = {}
    for p in parked_all:
        parked_by_lot.setdefault((p.lot_no or ""), []).append(p)

    total_ordered = sum(ordered.values())
    total_parked = len(parked_all)
    report["total_ordered"] = total_ordered
    report["total_parked"] = total_parked
    report["restored_wrong_lot"] = 0
    report["pulled_ordered"] = 0
    report["trimmed_excess"] = 0

    # WAREHOUSE source: each order LINE is one box and `qty` is the PACK count (units per
    # box), so SUM(qty) is NOT the box count (that caused false shortages, e.g. 75 boxes
    # parked vs SUM(qty)=1963 → bogus 1888 short). The reliable "expected boxes" is the
    # number of box rows the dispatch recorded. Flag the genuine gap only; no stock moved
    # (warehouse is box-tracked, not lot-pulled).
    if from_storage_type != "cold":
        box_count = db.execute(
            text("SELECT COUNT(*) FROM interunit_transfer_boxes WHERE header_id = :tid"),
            {"tid": transfer_out_id},
        ).scalar() or 0
        report["total_ordered"] = box_count
        report["groups"].append({"lot": "", "item": "(warehouse: 1 line = 1 box; qty=pack)",
                                  "ordered": box_count, "parked": total_parked,
                                  "shortfall": max(box_count - total_parked, 0)})
        report["allocated"] = 0
        report["unallocated"] = max(box_count - total_parked, 0)
        if not dry_run:
            _ensure_reconciliation_schema(db)
            db.execute(text("UPDATE interunit_transfers_header SET unallocated_boxes = :u WHERE id = :tid"),
                       {"u": report["unallocated"], "tid": transfer_out_id})
        logger.info("RECONCILE(warehouse,flag): tid=%s box_count=%s parked=%s short=%s dry_run=%s",
                    transfer_out_id, box_count, total_parked, report["unallocated"], dry_run)
        return report

    # COLD source: CORRECT pending to the order. The ORDER LOT is truth (no real scanning).
    now = datetime.now()
    shortage = 0

    # 1) Restore parked rows whose lot is NOT in the order (wrong-lot corruption).
    for lot, rows in list(parked_by_lot.items()):
        if lot not in ordered:
            for p in rows:
                _restore_pending_row(db, p, dry_run=dry_run)
                report["restored_wrong_lot"] += 1
            parked_by_lot[lot] = []

    # 2) Per ordered lot: make parked count == ordered qty (top up from THAT lot, or trim).
    guessed = _guess_company_from_site(hdr.from_site)
    for lot, qty in ordered.items():
        rows = parked_by_lot.get(lot, [])
        have = len(rows)
        pulled = 0
        if have < qty:
            need = qty - have
            for company in (guessed, "cdpl" if guessed == "cfpl" else "cfpl"):
                if need <= 0:
                    break
                for table, src in _find_available_cold_by_lot(db, company, lot, None, need):
                    box_id = f"RC-{transfer_out_id}-{getattr(src, 'id')}"
                    if not dry_run:
                        _park_cold_row(db, hdr, transfer_out_id, table, src, box_id,
                                       from_storage_type, to_storage_type, now)
                    pulled += 1
                    need -= 1
            report["pulled_ordered"] += pulled
            shortage += need  # ordered lot genuinely not available in the sheet
        elif have > qty:
            for p in rows[:have - qty]:
                _restore_pending_row(db, p, dry_run=dry_run)
                report["trimmed_excess"] += 1
        report["groups"].append({"lot": lot, "item": item_by_lot.get(lot, ""),
                                 "ordered": qty, "parked": have, "pulled": pulled,
                                 "shortfall": max(qty - have - pulled, 0)})

    report["allocated"] = report["pulled_ordered"]
    report["unallocated"] = shortage

    if not dry_run:
        _ensure_reconciliation_schema(db)
        db.execute(text("UPDATE interunit_transfers_header SET unallocated_boxes = :u WHERE id = :tid"),
                   {"u": shortage, "tid": transfer_out_id})
    logger.info("RECONCILE(cold,correct): tid=%s ordered=%s parked0=%s restored_wrong=%s "
                "pulled=%s trimmed=%s short=%s dry_run=%s",
                transfer_out_id, total_ordered, total_parked, report["restored_wrong_lot"],
                report["pulled_ordered"], report["trimmed_excess"], shortage, dry_run)
    return report


# ----------------------------------------------------------------------------
#  pick_from_pending — called from create_transfer_in / finalize_transfer_in
# ----------------------------------------------------------------------------
def pick_from_pending(transfer_out_id: int, db: Session, challan_no_for_inward: Optional[str] = None,
                      acknowledged_keys: Optional[set] = None) -> int:
    """Move 'In Transit' rows tied to this transfer_out into their destination table,
    then delete the pending row. Returns count picked.

    acknowledged_keys: set of (box_id, transaction_no) actually received. When given,
    only those real boxes are picked (the rest stay In Transit so a partial receipt
    can't leak them); 'LINE-%' tracking rows are always picked. None = legacy full
    pick (every in-transit row), kept for callers that intend a complete receipt.
    """
    pending_rows = db.execute(
        text("""
            SELECT * FROM pending_transfer_stock
            WHERE transfer_out_id = :tid AND status = 'In Transit'
        """),
        {"tid": transfer_out_id},
    ).fetchall()

    ack = None
    if acknowledged_keys is not None:
        ack = {((b or "").strip(), (t or "").strip()) for (b, t) in acknowledged_keys}

    picked = 0
    for p in pending_rows:
        # Scope to acknowledged boxes when provided: only pick boxes actually received
        # (by (box_id, transaction_no)); 'LINE-%' tracking rows are always picked.
        if ack is not None:
            _bid = (p.box_id or "").strip()
            if not _bid.startswith("LINE-") and (_bid, (p.transaction_no or "").strip()) not in ack:
                continue
        dest = p.destination_table

        # Transfer-in only stores in interunit_transfer_in tables.
        # For cold-destination transfers (cold→cold), insert into destination cold_stocks.
        # For warehouse-destination transfers (cold→warehouse), do NOT insert into
        # bulk_entry_boxes — the interunit_transfer_in_boxes records are the final state.
        if dest.endswith("_cold_stocks"):
            if not _table_exists(db, dest):
                logger.warning("PICK_PENDING: destination table %s missing, skip box_id=%s", dest, p.box_id)
                continue

            cold_json = p.cold_storage_data or {}
            db.execute(
                text(f"""
                    INSERT INTO {dest}
                        (inward_dt, unit, inward_no, item_description, item_mark,
                         vakkal, lot_no, no_of_cartons, weight_kg,
                         total_inventory_kgs, group_name, item_subgroup, storage_location,
                         exporter, last_purchase_rate, value,
                         box_id, transaction_no, spl_remarks)
                    VALUES
                        (:inward_dt, :unit, :inward_no, :item_description, :item_mark,
                         :vakkal, :lot_no, :no_of_cartons, :weight_kg,
                         :total_inventory_kgs, :group_name, :item_subgroup, :storage_location,
                         :exporter, :last_purchase_rate, :value,
                         :box_id, :transaction_no, :spl_remarks)
                    ON CONFLICT DO NOTHING
                """),
                {
                    "inward_dt": cold_json.get("inward_dt"),
                    "unit": cold_json.get("unit") or p.to_site,
                    "inward_no": challan_no_for_inward or cold_json.get("inward_no") or p.transfer_out_challan_no,
                    "item_description": p.item_description,
                    "item_mark": cold_json.get("item_mark"),
                    "vakkal": cold_json.get("vakkal"),
                    "lot_no": p.lot_no,
                    "no_of_cartons": p.no_of_cartons or 1,
                    "weight_kg": p.weight_kg,
                    "total_inventory_kgs": cold_json.get("total_inventory_kgs") or float(p.weight_kg or 0),
                    "group_name": cold_json.get("group_name"),
                    "item_subgroup": cold_json.get("item_subgroup"),
                    "storage_location": cold_json.get("storage_location") or p.to_site,
                    "exporter": cold_json.get("exporter"),
                    "last_purchase_rate": cold_json.get("last_purchase_rate"),
                    "value": cold_json.get("value"),
                    "box_id": p.box_id,
                    "transaction_no": p.transaction_no,
                    "spl_remarks": cold_json.get("spl_remarks"),
                },
            )

        db.execute(
            text("DELETE FROM pending_transfer_stock WHERE id = :id"),
            {"id": p.id},
        )
        picked += 1
        logger.info("PICK_PENDING: box_id=%s tno=%s -> %s (transfer_out_id=%s)",
                    p.box_id, p.transaction_no, dest, transfer_out_id)

    logger.info("PICK_PENDING: picked %d rows for transfer_out_id=%s", picked, transfer_out_id)
    return picked


def count_remaining_in_transit(transfer_out_id: int, db: Session) -> int:
    """Count REAL (non-'LINE-%') boxes still 'In Transit' for this transfer_out.

    This is the completion gate for the bridge invariant: a transfer may only flip to
    'Received' when this returns 0. It counts pending rows (unique on (box_id, txn)),
    so it is immune to duplicate OUT-box rows.
    """
    return db.execute(
        text("""
            SELECT COUNT(*) FROM pending_transfer_stock
            WHERE transfer_out_id = :tid AND status = 'In Transit'
              AND COALESCE(box_id, '') NOT LIKE 'LINE-%'
        """),
        {"tid": transfer_out_id},
    ).scalar() or 0


# ----------------------------------------------------------------------------
#  unpick_to_pending — called from delete_transfer_in
# ----------------------------------------------------------------------------
def unpick_to_pending(transfer_in_id: int, transfer_out_id: int, db: Session) -> int:
    """Reverse a Transfer In: delete the boxes from destination table and
    re-insert them into pending_transfer_stock (status='In Transit'). Returns count restored."""
    transfer_out = db.execute(
        text("SELECT id, challan_no, from_site, to_site, created_by FROM interunit_transfers_header WHERE id = :tid"),
        {"tid": transfer_out_id},
    ).fetchone()
    if not transfer_out:
        logger.warning("UNPICK_PENDING: transfer_out %s not found, nothing to restore", transfer_out_id)
        return 0

    from_site = transfer_out.from_site or ""
    to_site = transfer_out.to_site or ""
    from_storage_type = "cold" if _is_cold_site(from_site) else "warehouse"
    to_storage_type = "cold" if _is_cold_site(to_site) else "warehouse"

    in_boxes = db.execute(
        text("""
            SELECT box_id, transaction_no, article, lot_number, batch_number,
                   net_weight, gross_weight
            FROM interunit_transfer_in_boxes WHERE header_id = :hid
        """),
        {"hid": transfer_in_id},
    ).fetchall()

    restored = 0
    now = datetime.now()

    for b in in_boxes:
        if not b.box_id or not b.transaction_no:
            continue

        # Determine destination table (where the box currently lives)
        dest_table = None
        if to_storage_type == "cold":
            for t in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
                if _table_exists(db, t):
                    found = db.execute(
                        text(f"SELECT id FROM {t} WHERE box_id = :bid AND transaction_no = :tno LIMIT 1"),
                        {"bid": b.box_id, "tno": b.transaction_no},
                    ).fetchone()
                    if found:
                        dest_table = t
                        break
        else:
            for t in ("cfpl_bulk_entry_boxes", "cdpl_bulk_entry_boxes"):
                if _table_exists(db, t):
                    found = db.execute(
                        text(f"SELECT id FROM {t} WHERE box_id = :bid AND transaction_no = :tno LIMIT 1"),
                        {"bid": b.box_id, "tno": b.transaction_no},
                    ).fetchone()
                    if found:
                        dest_table = t
                        break

        # Capture row snapshot before delete so we can rebuild cold_storage_data
        cold_data = None
        item_description = b.article or ""
        lot_no = b.lot_number or None
        weight_kg = float(b.net_weight or 0)
        no_of_cartons = 1

        if dest_table and dest_table.endswith("_cold_stocks"):
            row = db.execute(
                text(f"SELECT * FROM {dest_table} WHERE box_id = :bid AND transaction_no = :tno LIMIT 1"),
                {"bid": b.box_id, "tno": b.transaction_no},
            ).fetchone()
            if row is not None:
                cold_data = _cold_row_to_json(row)
                item_description = getattr(row, "item_description", None) or item_description
                lot_no = getattr(row, "lot_no", None) or lot_no
                weight_kg = float(getattr(row, "weight_kg", 0) or weight_kg)
                no_of_cartons = int(getattr(row, "no_of_cartons", 1) or 1)

        # Delete from destination
        if dest_table:
            db.execute(
                text(f"DELETE FROM {dest_table} WHERE box_id = :bid AND transaction_no = :tno"),
                {"bid": b.box_id, "tno": b.transaction_no},
            )

        # Re-insert into pending_transfer_stock
        from_company = "cfpl"
        to_company = "cfpl"
        if dest_table and dest_table.startswith("cdpl"):
            to_company = "cdpl"
        source_table_guess = (
            "cfpl_cold_stocks" if from_storage_type == "cold" and to_company == "cfpl"
            else "cdpl_cold_stocks" if from_storage_type == "cold"
            else "cfpl_bulk_entry_boxes" if to_company == "cfpl"
            else "cdpl_bulk_entry_boxes"
        )
        destination_table_keep = dest_table or _destination_table(to_storage_type, to_company)

        db.execute(
            text("""
                INSERT INTO pending_transfer_stock
                    (transfer_type, transfer_out_id, transfer_out_challan_no,
                     box_id, transaction_no,
                     from_company, to_company, from_site, to_site,
                     from_storage_type, to_storage_type,
                     source_table, source_row_id, destination_table,
                     item_description, lot_no, batch_number, weight_kg, no_of_cartons,
                     cold_storage_data,
                     gross_weight, net_weight, article,
                     status, dispatched_at, dispatched_by)
                VALUES
                    ('INTERUNIT', :transfer_out_id, :challan_no,
                     :box_id, :transaction_no,
                     :from_company, :to_company, :from_site, :to_site,
                     :from_storage_type, :to_storage_type,
                     :source_table, NULL, :destination_table,
                     :item_description, :lot_no, :batch_number, :weight_kg, :no_of_cartons,
                     CAST(:cold_storage_data AS JSONB),
                     :gross_weight, :net_weight, :article,
                     'In Transit', :dispatched_at, :dispatched_by)
                ON CONFLICT (box_id, transaction_no) DO NOTHING
            """),
            {
                "transfer_out_id": transfer_out_id,
                "challan_no": transfer_out.challan_no,
                "box_id": b.box_id,
                "transaction_no": b.transaction_no,
                "from_company": from_company,
                "to_company": to_company,
                "from_site": from_site,
                "to_site": to_site,
                "from_storage_type": from_storage_type,
                "to_storage_type": to_storage_type,
                "source_table": source_table_guess,
                "destination_table": destination_table_keep,
                "item_description": item_description,
                "lot_no": lot_no,
                "batch_number": getattr(b, "batch_number", None),
                "weight_kg": weight_kg,
                "no_of_cartons": no_of_cartons,
                "cold_storage_data": json.dumps(cold_data) if cold_data else None,
                "gross_weight": float(b.gross_weight) if b.gross_weight is not None else None,
                "net_weight": float(b.net_weight) if b.net_weight is not None else None,
                "article": b.article,
                # Preserve the ORIGINAL transfer-out date on re-park (not the re-open
                # time), so an unpicked box keeps its true dispatch date.
                "dispatched_at": getattr(transfer_out, "stock_trf_date", None)
                                 or getattr(transfer_out, "created_ts", None) or now,
                "dispatched_by": transfer_out.created_by or "system",
            },
        )
        restored += 1
        logger.info("UNPICK_PENDING: box_id=%s tno=%s removed from %s -> back to pending",
                    b.box_id, b.transaction_no, dest_table)

    logger.info("UNPICK_PENDING: restored %d boxes to pending for transfer_in_id=%s", restored, transfer_in_id)
    return restored


# ----------------------------------------------------------------------------
#  restore_to_source — called from delete_transfer
# ----------------------------------------------------------------------------
def restore_to_source(transfer_out_id: int, db: Session) -> int:
    """Restore every pending row (any status) tied to this transfer_out back to
    its source table, then delete the pending row. Returns count restored."""
    pending_rows = db.execute(
        text("""
            SELECT * FROM pending_transfer_stock
            WHERE transfer_out_id = :tid
        """),
        {"tid": transfer_out_id},
    ).fetchall()

    restored = 0
    box_number_counters: dict = {}

    for p in pending_rows:
        src = p.source_table
        if not _table_exists(db, src):
            logger.warning("RESTORE_SOURCE: source table %s missing, skip box_id=%s", src, p.box_id)
            db.execute(text("DELETE FROM pending_transfer_stock WHERE id = :id"), {"id": p.id})
            continue

        cold_json = p.cold_storage_data or {}

        if src.endswith("_cold_stocks"):
            db.execute(
                text(f"""
                    INSERT INTO {src}
                        (inward_dt, unit, inward_no, item_description, item_mark,
                         vakkal, lot_no, no_of_cartons, weight_kg,
                         total_inventory_kgs, group_name, item_subgroup, storage_location,
                         exporter, last_purchase_rate, value,
                         box_id, transaction_no, spl_remarks)
                    VALUES
                        (:inward_dt, :unit, :inward_no, :item_description, :item_mark,
                         :vakkal, :lot_no, :no_of_cartons, :weight_kg,
                         :total_inventory_kgs, :group_name, :item_subgroup, :storage_location,
                         :exporter, :last_purchase_rate, :value,
                         :box_id, :transaction_no, :spl_remarks)
                    ON CONFLICT DO NOTHING
                """),
                {
                    "inward_dt": cold_json.get("inward_dt"),
                    "unit": cold_json.get("unit") or p.from_site,
                    "inward_no": cold_json.get("inward_no"),
                    "item_description": p.item_description,
                    "item_mark": cold_json.get("item_mark"),
                    "vakkal": cold_json.get("vakkal"),
                    "lot_no": p.lot_no,
                    "no_of_cartons": p.no_of_cartons or 1,
                    "weight_kg": p.weight_kg,
                    "total_inventory_kgs": cold_json.get("total_inventory_kgs") or float(p.weight_kg or 0),
                    "group_name": cold_json.get("group_name"),
                    "item_subgroup": cold_json.get("item_subgroup"),
                    "storage_location": cold_json.get("storage_location") or p.from_site,
                    "exporter": cold_json.get("exporter"),
                    "last_purchase_rate": cold_json.get("last_purchase_rate"),
                    "value": cold_json.get("value"),
                    "box_id": p.box_id,
                    "transaction_no": p.transaction_no,
                    "spl_remarks": cold_json.get("spl_remarks"),
                },
            )
        else:
            # Warehouse-source restore. The bulk_entry/boxes tables have FK constraints
            # on transaction_no → *_transactions(_v2). Legacy *_bulk_entry_boxes is being
            # phased out in favour of *_boxes_v2 and many legacy parents have been
            # cascade-deleted. Strategy:
            #   1. If src's parent transaction still exists → INSERT to src as-is.
            #   2. Else if src is legacy and the v2 parent exists → redirect to *_boxes_v2.
            #   3. Else (no parent anywhere) → log warning + skip the source INSERT;
            #      still clear the pending row so the delete can complete. The box is
            #      already orphaned upstream — blocking the user's delete won't recover it.
            company = src.split("_")[0] if src else ""
            parent_tbl_for_src = (
                f"{company}_transactions_v2" if src.endswith("_boxes_v2")
                else (f"{company}_bulk_entry_transactions" if src.endswith("_bulk_entry_boxes") else None)
            )
            target_table = src
            if parent_tbl_for_src:
                has_parent = db.execute(
                    text(f"SELECT 1 FROM {parent_tbl_for_src} WHERE transaction_no = :t"),
                    {"t": p.transaction_no},
                ).scalar()
                if not has_parent:
                    v2_table = f"{company}_boxes_v2"
                    v2_parent_tbl = f"{company}_transactions_v2"
                    redirected = False
                    if src != v2_table and _table_exists(db, v2_table):
                        v2_parent = db.execute(
                            text(f"SELECT 1 FROM {v2_parent_tbl} WHERE transaction_no = :t"),
                            {"t": p.transaction_no},
                        ).scalar()
                        if v2_parent:
                            target_table = v2_table
                            redirected = True
                    if not redirected:
                        logger.warning(
                            "RESTORE_SOURCE: parent txn %s missing for source=%s (and v2 fallback "
                            "unavailable) for box_id=%s — skipping source INSERT, pending row cleared.",
                            p.transaction_no, src, p.box_id,
                        )
                        db.execute(text("DELETE FROM pending_transfer_stock WHERE id = :id"), {"id": p.id})
                        restored += 1
                        continue

            article_key = (p.transaction_no or "", p.article or p.item_description or "")
            box_number_counters[article_key] = box_number_counters.get(article_key, 0) + 1
            box_num = box_number_counters[article_key]

            db.execute(
                text(f"""
                    INSERT INTO {target_table}
                        (box_id, transaction_no, article_description, lot_number,
                         net_weight, gross_weight, box_number, count)
                    VALUES
                        (:box_id, :transaction_no, :article, :lot_no,
                         :net_weight, :gross_weight, :box_number, :count)
                    ON CONFLICT DO NOTHING
                """),
                {
                    "box_id": p.box_id,
                    "transaction_no": p.transaction_no,
                    "article": p.article or p.item_description,
                    "lot_no": p.lot_no,
                    "net_weight": p.net_weight or p.weight_kg,
                    "gross_weight": p.gross_weight,
                    "box_number": box_num,
                    "count": p.no_of_cartons or 1,
                },
            )

        # Mark the disposition row as reverted (transfer was cancelled/deleted).
        # Use both the current box_id and original_box_id (if STBR remapped it)
        # so the audit closes the loop regardless of which label was deducted.
        _revert_disposition(
            db,
            box_id=p.box_id,
            transaction_no=p.transaction_no,
            disposition_type="transfer_out_pending",
            reverted_reason=f"transfer_out_id={transfer_out_id} cancelled/deleted",
        )
        if getattr(p, "original_box_id", None) and p.original_box_id != p.box_id:
            _revert_disposition(
                db,
                box_id=p.original_box_id,
                transaction_no=p.transaction_no,
                disposition_type="transfer_out_pending",
                reverted_reason=f"transfer_out_id={transfer_out_id} cancelled (pre-reconcile label)",
            )

        db.execute(text("DELETE FROM pending_transfer_stock WHERE id = :id"), {"id": p.id})
        restored += 1
        logger.info("RESTORE_SOURCE: box_id=%s tno=%s -> %s (transfer_out_id=%s)",
                    p.box_id, p.transaction_no, src, transfer_out_id)

    logger.info("RESTORE_SOURCE: restored %d rows for transfer_out_id=%s", restored, transfer_out_id)
    return restored


# ----------------------------------------------------------------------------
#  Read APIs for frontend
# ----------------------------------------------------------------------------
def list_pending_transfers(
    db: Session,
    from_site: Optional[str] = None,
    to_site: Optional[str] = None,
    company: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    search: Optional[str] = None,
) -> dict:
    """Group rows in pending_transfer_stock by transfer_out_id for the
    Pending Transfer Status modal. Returns one row per dispatch.

    Two sources are combined:
    1. pending_transfer_stock rows (In Transit, excluding Received headers)
    2. interunit_transfers_header rows with Dispatch/Partial status that have
       no pending_transfer_stock rows (legacy or article-only transfers)
    """
    if not _table_exists(db, "pending_transfer_stock"):
        return {"records": [], "total": 0, "filter_options": {"from_sites": [], "to_sites": []}}

    # Ensure the unallocated_boxes column exists before we select it (idempotent,
    # globally short-circuited after first call) so the shortfall indicator is safe.
    _ensure_reconciliation_schema(db)

    # Outer filters applied to the combined CTE (no table prefix)
    outer_clauses: list = []
    params: dict = {}

    if from_site:
        outer_clauses.append("from_site = :from_site")
        params["from_site"] = from_site
    if to_site:
        outer_clauses.append("to_site = :to_site")
        params["to_site"] = to_site
    # Company scoping intentionally DISABLED: the Pending Transfer Status section is a
    # single unified view of every in-transit transfer (CFPL + CDPL) regardless of the
    # navbar company. Filtering by company previously hid cross-company transfers — e.g.
    # CDPL cold→warehouse dispatches were invisible under CFPL (TRANS202605281739). The
    # `company` param is still accepted for API compatibility but is no longer applied.
    _ = company
    if from_date:
        outer_clauses.append("dispatched_at::date >= :from_date::date")
        params["from_date"] = from_date
    if to_date:
        outer_clauses.append("dispatched_at::date <= :to_date::date")
        params["to_date"] = to_date
    if search:
        outer_clauses.append("transfer_out_challan_no ILIKE :s")
        params["s"] = f"%{search}%"

    outer_where = ("WHERE " + " AND ".join(outer_clauses)) if outer_clauses else ""

    rows = db.execute(
        text(f"""
            WITH combined AS (
                -- Tracked transfers: have pending_transfer_stock rows.
                -- Exclude transfers whose header has already been Received.
                SELECT
                    pts.transfer_out_id,
                    pts.transfer_out_challan_no,
                    -- DATE = the transfer-out's actual initiation date from the live
                    -- header (stock_trf_date), NOT pts.dispatched_at which stores the
                    -- park/sync time. Fall back to created_ts if the date is unset.
                    MIN(COALESCE(ith.stock_trf_date, ith.created_ts)) AS dispatched_at,
                    MIN(pts.from_site)                    AS from_site,
                    MIN(pts.to_site)                      AS to_site,
                    -- A single dispatch can pull lots from BOTH cold companies, which
                    -- previously split one challan into two rows. Group by the dispatch
                    -- only (transfer_out_id + challan) and surface a representative or
                    -- 'mixed' company so each dispatch is exactly one row.
                    CASE WHEN COUNT(DISTINCT pts.from_company) > 1 THEN 'mixed'
                         ELSE MIN(pts.from_company) END   AS from_company,
                    CASE WHEN COUNT(DISTINCT pts.to_company) > 1 THEN 'mixed'
                         ELSE MIN(pts.to_company) END     AS to_company,
                    MIN(pts.from_storage_type)            AS from_storage_type,
                    MIN(pts.to_storage_type)              AS to_storage_type,
                    COUNT(*)                              AS total_boxes,
                    COALESCE(SUM(pts.no_of_cartons), 0)   AS total_cartons,
                    COALESCE(SUM(pts.weight_kg), 0)       AS total_kg,
                    -- Read the creator from the live header (single source of truth)
                    -- so a corrected created_by shows immediately without re-parking,
                    -- and a stale/placeholder parked snapshot can't mismatch it.
                    MIN(ith.created_by)                   AS dispatched_by,
                    MIN(ith.status)                       AS header_status,
                    -- LIVE shortfall (never stale): ordered boxes not accounted for
                    -- = ordered - parked(In-Transit, real) - received. Computing it live
                    -- (instead of the stored unallocated_boxes flag) means a corrected
                    -- pending set self-heals the badge, and it nets out received boxes.
                    GREATEST(
                        (CASE WHEN (SELECT COUNT(*) FROM interunit_transfer_boxes b WHERE b.header_id = pts.transfer_out_id) > 0
                              THEN (SELECT COUNT(*) FROM interunit_transfer_boxes b WHERE b.header_id = pts.transfer_out_id)
                              ELSE COALESCE((SELECT SUM(l.qty)::int FROM interunit_transfers_lines l WHERE l.header_id = pts.transfer_out_id), 0) END)
                        - COUNT(*) FILTER (WHERE COALESCE(pts.box_id, '') NOT LIKE 'LINE-%')
                        - COALESCE((SELECT COUNT(*) FROM interunit_transfer_in_boxes tib
                                    JOIN interunit_transfer_in_header tih ON tih.id = tib.header_id
                                    WHERE tih.transfer_out_id = pts.transfer_out_id), 0),
                        0
                    ) AS unallocated_boxes,
                    -- "Edited" badge source: the genuine edit marker (edited_at),
                    -- NOT updated_ts (auto-bumped by trigger on every row change).
                    MAX(ith.edited_at)                    AS updated_ts
                FROM pending_transfer_stock pts
                JOIN interunit_transfers_header ith
                    ON ith.id = pts.transfer_out_id AND ith.status != 'Received'
                WHERE pts.status = 'In Transit'
                GROUP BY pts.transfer_out_id, pts.transfer_out_challan_no

                UNION ALL

                -- Orphaned/legacy transfers: Dispatch or Partial status in the header
                -- but no rows in pending_transfer_stock (e.g. article-only, pre-feature).
                -- Box/weight totals fall back to interunit_transfers_lines when there
                -- are no rows in interunit_transfer_boxes (typical for warehouse-source
                -- transfers submitted before Plan C auto-derive).
                SELECT
                    h.id                                  AS transfer_out_id,
                    h.challan_no                          AS transfer_out_challan_no,
                    COALESCE(h.stock_trf_date, h.created_ts) AS dispatched_at,
                    h.from_site,
                    h.to_site,
                    NULL                                  AS from_company,
                    NULL                                  AS to_company,
                    CASE WHEN LOWER(COALESCE(h.from_site,'')) SIMILAR TO
                         '%(cold|rishi|savla|supreme)%' THEN 'cold' ELSE 'warehouse' END
                                                          AS from_storage_type,
                    CASE WHEN LOWER(COALESCE(h.to_site,'')) SIMILAR TO
                         '%(cold|rishi|savla|supreme)%' THEN 'cold' ELSE 'warehouse' END
                                                          AS to_storage_type,
                    CASE WHEN (SELECT COUNT(*) FROM interunit_transfer_boxes b WHERE b.header_id = h.id) > 0
                         THEN (SELECT COUNT(*) FROM interunit_transfer_boxes b WHERE b.header_id = h.id)
                         ELSE COALESCE((SELECT SUM(l.qty)::int FROM interunit_transfers_lines l WHERE l.header_id = h.id), 0)
                    END                                   AS total_boxes,
                    CASE WHEN (SELECT COUNT(*) FROM interunit_transfer_boxes b WHERE b.header_id = h.id) > 0
                         THEN (SELECT COUNT(*) FROM interunit_transfer_boxes b WHERE b.header_id = h.id)
                         ELSE COALESCE((SELECT SUM(l.qty)::int FROM interunit_transfers_lines l WHERE l.header_id = h.id), 0)
                    END                                   AS total_cartons,
                    CASE WHEN (SELECT COUNT(*) FROM interunit_transfer_boxes b WHERE b.header_id = h.id) > 0
                         THEN COALESCE((SELECT SUM(CAST(b.net_weight AS NUMERIC)) FROM interunit_transfer_boxes b WHERE b.header_id = h.id), 0)
                         ELSE COALESCE((SELECT SUM(CAST(l.net_weight AS NUMERIC)) FROM interunit_transfers_lines l WHERE l.header_id = h.id), 0)
                    END                                   AS total_kg,
                    COALESCE(h.created_by, '')            AS dispatched_by,
                    h.status                              AS header_status,
                    -- LIVE shortfall (orphan branch: no pending rows, so parked = 0).
                    GREATEST(
                        (CASE WHEN (SELECT COUNT(*) FROM interunit_transfer_boxes b WHERE b.header_id = h.id) > 0
                              THEN (SELECT COUNT(*) FROM interunit_transfer_boxes b WHERE b.header_id = h.id)
                              ELSE COALESCE((SELECT SUM(l.qty)::int FROM interunit_transfers_lines l WHERE l.header_id = h.id), 0) END)
                        - COALESCE((SELECT COUNT(*) FROM interunit_transfer_in_boxes tib
                                    JOIN interunit_transfer_in_header tih ON tih.id = tib.header_id
                                    WHERE tih.transfer_out_id = h.id), 0),
                        0
                    )                                     AS unallocated_boxes,
                    h.edited_at                           AS updated_ts
                FROM interunit_transfers_header h
                WHERE h.status IN ('Dispatch', 'Partial')
                  AND NOT EXISTS (
                      SELECT 1 FROM pending_transfer_stock p
                      WHERE p.transfer_out_id = h.id AND p.status = 'In Transit'
                  )
            )
            SELECT * FROM combined
            {outer_where}
            ORDER BY dispatched_at DESC
        """),
        params,
    ).fetchall()

    records = [
        {
            "transfer_out_id": r.transfer_out_id,
            "transfer_out_challan_no": r.transfer_out_challan_no,
            "dispatched_at": r.dispatched_at.isoformat() if r.dispatched_at else None,
            "from_site": _normalize_site(r.from_site),
            "to_site": _normalize_site(r.to_site),
            "from_company": r.from_company,
            "to_company": r.to_company,
            "from_storage_type": r.from_storage_type,
            "to_storage_type": r.to_storage_type,
            "total_boxes": int(r.total_boxes or 0),
            "total_cartons": float(r.total_cartons or 0),
            "total_kg": float(r.total_kg or 0),
            "dispatched_by": r.dispatched_by or "",
            "status": "In Transit",
            "header_status": getattr(r, "header_status", None) or "Dispatch",
            "unallocated_boxes": int(getattr(r, "unallocated_boxes", 0) or 0),
            "updated_ts": r.updated_ts.isoformat() if getattr(r, "updated_ts", None) else None,
        }
        for r in rows
    ]

    # Filter option chips — union of (a) distinct sites from combined in-transit data,
    # and (b) all active warehouse_sites — so chips are always visible even when
    # pending is empty. Both sources are normalized so "Warehouse A68" → "A68".
    # Chip counts: number of distinct TRANSFERS per (from_site, to_site), summed
    # across the in-transit + orphaned populations. Counting per-box rows here
    # would inflate to-chip counts (e.g. one cold→W202 dispatch with 300 boxes
    # would contribute 300, not 1, to W202's to-count).
    pending_site_rows = db.execute(
        text("""
            SELECT pts.from_site, pts.to_site, COUNT(DISTINCT pts.transfer_out_id) AS n
            FROM pending_transfer_stock pts
            JOIN interunit_transfers_header ith ON ith.id = pts.transfer_out_id AND ith.status != 'Received'
            WHERE pts.status = 'In Transit'
            GROUP BY pts.from_site, pts.to_site
            UNION ALL
            SELECT h.from_site, h.to_site, COUNT(DISTINCT h.id) AS n
            FROM interunit_transfers_header h
            WHERE h.status IN ('Dispatch', 'Partial')
              AND NOT EXISTS (
                  SELECT 1 FROM pending_transfer_stock p WHERE p.transfer_out_id = h.id AND p.status = 'In Transit'
              )
            GROUP BY h.from_site, h.to_site
        """),
    ).fetchall()
    from_counts: dict = {}
    to_counts: dict = {}
    for r in pending_site_rows:
        if r.from_site:
            key = _normalize_site(r.from_site)
            from_counts[key] = from_counts.get(key, 0) + int(r.n or 0)
        if r.to_site:
            key = _normalize_site(r.to_site)
            to_counts[key] = to_counts.get(key, 0) + int(r.n or 0)

    all_sites: list = []
    if _table_exists(db, "warehouse_sites"):
        try:
            ws_rows = db.execute(
                text("SELECT site_name FROM warehouse_sites WHERE COALESCE(is_active, true) = true ORDER BY site_name")
            ).fetchall()
            all_sites = [_normalize_site(r.site_name) for r in ws_rows if r.site_name]
        except Exception:
            all_sites = []

    from_chips = sorted({*from_counts.keys(), *all_sites})
    to_chips = sorted({*to_counts.keys(), *all_sites})

    return {
        "records": records,
        "total": len(records),
        "filter_options": {
            "from_sites": from_chips,
            "to_sites": to_chips,
            "from_site_counts": from_counts,
            "to_site_counts": to_counts,
        },
    }


def pending_by_lot(
    db: Session,
    lot_no: Optional[str] = None,
    item_description: Optional[str] = None,
    from_site: Optional[str] = None,
    from_company: Optional[str] = None,
) -> dict:
    """Sum pending cartons and weight for a given lot+item (used by inventory
    UI to subtract from displayed available quantity). Also returns the list
    of pending transactions joined with the transfer header for richer context."""
    if not _table_exists(db, "pending_transfer_stock"):
        return {"pending_cartons": 0, "pending_kg": 0, "transfers": [], "boxes": []}

    _ensure_reconciliation_schema(db)  # guarantees updated_ts exists before we select it

    clauses = ["pts.status = 'In Transit'"]
    params: dict = {}

    if lot_no:
        clauses.append("pts.lot_no = :lot_no")
        params["lot_no"] = lot_no
    if item_description:
        clauses.append("pts.item_description = :item_description")
        params["item_description"] = item_description
    if from_site:
        clauses.append("pts.from_site = :from_site")
        params["from_site"] = from_site
    if from_company:
        clauses.append("pts.from_company = :from_company")
        params["from_company"] = from_company.lower()

    where = " AND ".join(clauses)

    total = db.execute(
        text(f"""
            SELECT
                COALESCE(SUM(pts.no_of_cartons), 0)  AS pending_cartons,
                COALESCE(SUM(pts.weight_kg), 0)      AS pending_kg,
                COUNT(*)                              AS box_count
            FROM pending_transfer_stock pts
            WHERE {where}
        """),
        params,
    ).fetchone()

    # Aggregate per transfer — one row per challan, summed over all boxes.
    # JOIN header so we can surface vehicle/driver/approved_by/reason/remark/status.
    header_join_ok = _table_exists(db, "interunit_transfers_header")
    join_clause = (
        "LEFT JOIN interunit_transfers_header h ON h.id = pts.transfer_out_id"
        if header_join_ok else ""
    )
    header_cols = (
        ",\n                   MIN(h.vehicle_no)    AS vehicle_no,"
        "\n                   MIN(h.driver_name)   AS driver_name,"
        "\n                   MIN(h.approved_by)   AS approved_by,"
        "\n                   MIN(h.remark)        AS remark,"
        "\n                   MIN(h.reason_code)   AS reason_code,"
        "\n                   MIN(h.status)        AS transfer_status,"
        "\n                   BOOL_OR(COALESCE(h.has_variance, false)) AS has_variance,"
        "\n                   MIN(h.updated_ts)    AS updated_ts"
    ) if header_join_ok else (
        ",\n                   NULL::text AS vehicle_no,"
        "\n                   NULL::text AS driver_name,"
        "\n                   NULL::text AS approved_by,"
        "\n                   NULL::text AS remark,"
        "\n                   NULL::text AS reason_code,"
        "\n                   NULL::text AS transfer_status,"
        "\n                   false      AS has_variance,"
        "\n                   NULL::timestamp AS updated_ts"
    )

    transfer_rows = db.execute(
        text(f"""
            SELECT pts.transfer_out_id,
                   pts.transfer_out_challan_no,
                   MIN(pts.dispatched_at)            AS dispatched_at,
                   pts.from_site,
                   pts.to_site,
                   pts.from_storage_type,
                   pts.to_storage_type,
                   COUNT(*)                           AS box_count,
                   COALESCE(SUM(pts.no_of_cartons),0) AS cartons,
                   COALESCE(SUM(pts.weight_kg), 0)    AS weight_kg,
                   MIN(pts.dispatched_by)            AS dispatched_by
                   {header_cols}
            FROM pending_transfer_stock pts
            {join_clause}
            WHERE {where}
            GROUP BY pts.transfer_out_id, pts.transfer_out_challan_no,
                     pts.from_site, pts.to_site,
                     pts.from_storage_type, pts.to_storage_type
            ORDER BY MIN(pts.dispatched_at) DESC
        """),
        params,
    ).fetchall()

    return {
        "pending_cartons": float(total.pending_cartons or 0),
        "pending_kg": float(total.pending_kg or 0),
        "box_count": int(total.box_count or 0),
        "transfers": [
            {
                "transfer_out_id": r.transfer_out_id,
                "challan_no": r.transfer_out_challan_no,
                "dispatched_at": r.dispatched_at.isoformat() if r.dispatched_at else None,
                "from_site": r.from_site,
                "to_site": r.to_site,
                "from_storage_type": r.from_storage_type,
                "to_storage_type": r.to_storage_type,
                "box_count": int(r.box_count or 0),
                "cartons": float(r.cartons or 0),
                "weight_kg": float(r.weight_kg or 0),
                "dispatched_by": r.dispatched_by or "",
                "vehicle_no": getattr(r, "vehicle_no", None) or "",
                "driver_name": getattr(r, "driver_name", None) or "",
                "approved_by": getattr(r, "approved_by", None) or "",
                "remark": getattr(r, "remark", None) or "",
                "reason_code": getattr(r, "reason_code", None) or "",
                "transfer_status": getattr(r, "transfer_status", None) or "",
                "has_variance": bool(getattr(r, "has_variance", False)),
                "updated_ts": r.updated_ts.isoformat() if getattr(r, "updated_ts", None) else None,
            }
            for r in transfer_rows
        ],
        # legacy alias retained briefly for any callers expecting `boxes`
        "boxes": [],
    }


def in_transit_by_lot(db: Session, company: Optional[str] = None) -> dict:
    """One batched query: In-Transit pending cartons/kg/boxes per lot, for dashboard
    overlays (so the cold-storage dashboard can show an 'in transit' context badge per
    lot without N per-lot calls). Returns {lot_no: {cartons, kg, box_count}}.

    NOTE: these boxes are already deducted from cold_stocks at dispatch — this map is
    for DISPLAY context only; do NOT subtract it from displayed stock (double count)."""
    if not _table_exists(db, "pending_transfer_stock"):
        return {}
    clauses = ["status = 'In Transit'", "lot_no IS NOT NULL", "lot_no <> ''"]
    params: dict = {}
    if company:
        clauses.append("LOWER(from_company) = :co")
        params["co"] = company.lower()
    where = " AND ".join(clauses)
    rows = db.execute(
        text(f"""
            SELECT lot_no,
                   COALESCE(SUM(no_of_cartons), 0) AS cartons,
                   COALESCE(SUM(weight_kg), 0)     AS kg,
                   COUNT(*)                         AS box_count
            FROM pending_transfer_stock
            WHERE {where}
            GROUP BY lot_no
        """),
        params,
    ).fetchall()
    return {
        r.lot_no: {
            "cartons": float(r.cartons or 0),
            "kg": float(r.kg or 0),
            "box_count": int(r.box_count or 0),
        }
        for r in rows if r.lot_no
    }


# ----------------------------------------------------------------------------
#  backfill_pending_from_existing_transfers — one-time migration
#
#  Parks every box from every in-transit Transfer Out (status Dispatch/Partial,
#  no 'Received' Transfer In) into pending_transfer_stock, deducting source
#  stock the way create_transfer would have if pending_transfer_stock had
#  existed at dispatch time. Safe to re-run — already-parked transfers are
#  skipped via the unique (box_id, transaction_no) constraint.
# ----------------------------------------------------------------------------
def backfill_pending_from_existing_transfers(db: Session, dry_run: bool = False) -> dict:
    if not _table_exists(db, "pending_transfer_stock"):
        return {"error": "pending_transfer_stock table missing"}

    candidates = db.execute(
        text("""
            SELECT h.id, h.challan_no, h.from_site, h.to_site,
                   h.status, h.created_by, h.created_ts, h.stock_trf_date
            FROM interunit_transfers_header h
            WHERE LOWER(COALESCE(h.status, '')) IN ('dispatch', 'partial', 'completed', 'in transit')
              AND NOT EXISTS (
                SELECT 1 FROM interunit_transfer_in_header ti
                WHERE ti.transfer_out_id = h.id
                  AND LOWER(COALESCE(ti.status, '')) = 'received'
              )
            ORDER BY h.created_ts ASC
        """)
    ).fetchall()

    summary = {
        "transfers_scanned": len(candidates),
        "transfers_with_existing_pending": 0,
        "boxes_parked_from_cold": 0,
        "boxes_parked_from_warehouse": 0,
        "boxes_parked_without_source": 0,
        "boxes_skipped_already_parked": 0,
        "boxes_with_missing_id": 0,
        "boxes_topped_up_by_lot": 0,
        "boxes_unallocatable": 0,
        "reconciled": [],
    }

    # Write router: in dry-run, every write is a no-op so the function is read-only
    # and can preview what an apply would do. Reads still go through db.execute.
    _w = (lambda *a, **k: None) if dry_run else db.execute

    for t in candidates:
        # Count (no longer SKIP) transfers that already have pending rows: we now
        # reconcile EVERY in-transit transfer up to its ordered qty. The per-box dup
        # check below and ON CONFLICT prevent double-parking; reconcile_transfer_to_order
        # then fills the remaining shortfall BY LOT from the main stock sheet.
        existing = db.execute(
            text("SELECT COUNT(*) FROM pending_transfer_stock WHERE transfer_out_id = :tid"),
            {"tid": t.id},
        ).scalar()
        if existing and existing > 0:
            summary["transfers_with_existing_pending"] += 1

        boxes = db.execute(
            text("""
                SELECT box_id, transaction_no, article, lot_number, batch_number,
                       net_weight, gross_weight, box_number
                FROM interunit_transfer_boxes
                WHERE header_id = :tid
            """),
            {"tid": t.id},
        ).fetchall()

        from_storage_type = "cold" if _is_cold_site(t.from_site) else "warehouse"
        to_storage_type = "cold" if _is_cold_site(t.to_site) else "warehouse"
        dispatched_at = getattr(t, "stock_trf_date", None) or getattr(t, "created_ts", None) or datetime.now()
        dispatched_by = t.created_by or "backfill"

        for b in boxes:
            box_id = (b.box_id or "").strip()
            tno = (b.transaction_no or "").strip()
            if not box_id or not tno or tno == "DIRECT":
                summary["boxes_with_missing_id"] += 1
                continue

            # Already in pending from a parallel run? skip.
            dup = db.execute(
                text("SELECT 1 FROM pending_transfer_stock WHERE box_id = :bid AND transaction_no = :tno LIMIT 1"),
                {"bid": box_id, "tno": tno},
            ).fetchone()
            if dup:
                summary["boxes_skipped_already_parked"] += 1
                continue

            # Find source row
            source_table = None
            source_row = None
            cold_data = None
            warehouse_data = {}

            if from_storage_type == "cold":
                source_table, source_row = _find_in_cold_stocks(db, box_id, tno, getattr(b, "lot_number", None))
                if source_row is not None:
                    cold_data = _cold_row_to_json(source_row)

            if source_row is None:
                # Try warehouse tables (whether or not from_storage_type said warehouse)
                wh_table, wh_row = _find_in_bulk_entry(db, box_id, tno, getattr(b, "lot_number", None))
                if wh_row is not None:
                    source_table = wh_table
                    source_row = wh_row
                    warehouse_data = {
                        "gross_weight": float(getattr(wh_row, "gross_weight", 0) or 0),
                        "net_weight": float(getattr(wh_row, "net_weight", 0) or b.net_weight or 0),
                        "article": getattr(wh_row, "article_description", None),
                    }

            # Resolve descriptor fields
            if cold_data is not None and source_row is not None:
                item_description = getattr(source_row, "item_description", None) or b.article or ""
                lot_no = getattr(source_row, "lot_no", None) or b.lot_number or None
                weight_kg = float(getattr(source_row, "weight_kg", 0) or b.net_weight or 0)
                no_of_cartons = int(getattr(source_row, "no_of_cartons", 1) or 1)
            elif warehouse_data:
                item_description = warehouse_data.get("article") or b.article or ""
                lot_no = b.lot_number or None
                weight_kg = float(warehouse_data.get("net_weight") or b.net_weight or 0)
                no_of_cartons = 1
            else:
                # No source row anywhere — park with whatever transfer_box has.
                # Pick a sensible default source_table from site naming (Rishi → cdpl, Savla → cfpl).
                item_description = b.article or ""
                lot_no = b.lot_number or None
                weight_kg = float(b.net_weight or 0)
                no_of_cartons = 1
                site_lower = (t.from_site or "").strip().lower()
                guessed_company = "cdpl" if ("rishi" in site_lower or "cdpl" in site_lower) else "cfpl"
                source_table = (
                    f"{guessed_company}_cold_stocks"
                    if from_storage_type == "cold"
                    else f"{guessed_company}_bulk_entry_boxes"
                )

            from_company = "cfpl" if (source_table or "").startswith("cfpl") else "cdpl"
            to_company = from_company
            destination_table = _destination_table(to_storage_type, to_company)

            _w(
                text("""
                    INSERT INTO pending_transfer_stock
                        (transfer_type, transfer_out_id, transfer_out_challan_no,
                         box_id, transaction_no,
                         from_company, to_company, from_site, to_site,
                         from_storage_type, to_storage_type,
                         source_table, source_row_id, destination_table,
                         item_description, lot_no, batch_number, weight_kg, no_of_cartons,
                         cold_storage_data,
                         gross_weight, net_weight, article,
                         status, dispatched_at, dispatched_by)
                    VALUES
                        ('INTERUNIT', :transfer_out_id, :challan_no,
                         :box_id, :transaction_no,
                         :from_company, :to_company, :from_site, :to_site,
                         :from_storage_type, :to_storage_type,
                         :source_table, :source_row_id, :destination_table,
                         :item_description, :lot_no, :batch_number, :weight_kg, :no_of_cartons,
                         CAST(:cold_storage_data AS JSONB),
                         :gross_weight, :net_weight, :article,
                         'In Transit', :dispatched_at, :dispatched_by)
                    ON CONFLICT (box_id, transaction_no) DO NOTHING
                """),
                {
                    "transfer_out_id": t.id,
                    "challan_no": t.challan_no,
                    "box_id": box_id,
                    "transaction_no": tno,
                    "from_company": from_company,
                    "to_company": to_company,
                    "from_site": t.from_site,
                    "to_site": t.to_site,
                    "from_storage_type": from_storage_type,
                    "to_storage_type": to_storage_type,
                    "source_table": source_table,
                    "source_row_id": getattr(source_row, "id", None) if source_row is not None else None,
                    "destination_table": destination_table,
                    "item_description": item_description,
                    "lot_no": lot_no,
                    "batch_number": getattr(source_row, "batch_number", None) if source_row is not None else None,
                    "weight_kg": weight_kg,
                    "no_of_cartons": no_of_cartons,
                    "cold_storage_data": json.dumps(cold_data) if cold_data else None,
                    "gross_weight": warehouse_data.get("gross_weight") if warehouse_data else (float(b.gross_weight) if b.gross_weight is not None else None),
                    "net_weight": warehouse_data.get("net_weight") if warehouse_data else (float(b.net_weight) if b.net_weight is not None else None),
                    "article": warehouse_data.get("article") or b.article,
                    "dispatched_at": dispatched_at,
                    "dispatched_by": dispatched_by,
                },
            )

            # Delete source row (only if it actually exists — keeps idempotency)
            if source_row is not None and getattr(source_row, "id", None) is not None and source_table:
                _w(
                    text(f"DELETE FROM {source_table} WHERE id = :rid"),
                    {"rid": source_row.id},
                )

            if cold_data is not None:
                summary["boxes_parked_from_cold"] += 1
            elif warehouse_data:
                summary["boxes_parked_from_warehouse"] += 1
            else:
                summary["boxes_parked_without_source"] += 1

        # Reconcile this transfer up to its ordered qty: fills any remaining shortfall
        # BY LOT from the main sheet (the fix for 600-ordered-but-407-parked), and
        # flags genuinely unallocatable units on the header.
        rec = reconcile_transfer_to_order(t.id, db, dry_run=dry_run)
        summary["boxes_topped_up_by_lot"] += rec["allocated"]
        summary["boxes_unallocatable"] += rec["unallocated"]
        summary["reconciled"].append(rec)

    if not dry_run:
        db.commit()
    logger.info("BACKFILL_PENDING(dry_run=%s): %s", dry_run,
                {k: v for k, v in summary.items() if k != "reconciled"})
    return summary
