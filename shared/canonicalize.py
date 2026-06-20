"""Canonical name + transaction resolver helpers.

Single source of truth for normalizing inventory dimensions across all
dashboards. Dashboard queries should use the materialized canonical columns
(populated via backfill + trigger), but ad-hoc callers can use these helpers
directly.

Companion DB migration: ``backend/services/cold_storage_service/migrations/
20260525_canonical_cold_stock_columns.sql`` adds and maintains the columns.
"""
from __future__ import annotations

import re
from typing import Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session


# ---------------------------------------------------------------------------
# Warehouse canonicalization
# ---------------------------------------------------------------------------

# Canonical enum — anything not mappable to one of these is treated as "Other".
CANONICAL_WAREHOUSES = {
    "Savla D-39",
    "Savla D-514",
    "Rishi",
    "Supreme",
    "W202",
    "A101",
    "A185",
    "A68",
    "F53",
    "Dev Int",
}

# Lowercased + underscore-normalized aliases → canonical name.
WAREHOUSE_ALIASES: dict[str, str] = {
    # Savla D-39
    "savla d-39": "Savla D-39",
    "savla d39": "Savla D-39",
    "d-39": "Savla D-39",
    "d39": "Savla D-39",
    "savla bond": "Savla D-39",
    "old savla": "Savla D-39",
    "savla d-39 cold": "Savla D-39",
    "savla d39 cold": "Savla D-39",
    # Savla D-514
    "savla d-514": "Savla D-514",
    "savla d514": "Savla D-514",
    "d-514": "Savla D-514",
    "d514": "Savla D-514",
    "new savla": "Savla D-514",
    "savla d-514 cold": "Savla D-514",
    # Rishi
    "rishi": "Rishi",
    "rishi cold": "Rishi",
    "rishi cold storage": "Rishi",
    # Supreme
    "supreme": "Supreme",
    "supreme cold": "Supreme",
    "supreme cold storage": "Supreme",
    # Regular warehouses (case variants)
    "w202": "W202",
    "warehouse w202": "W202",
    "a101": "A101",
    "warehouse a101": "A101",
    "a185": "A185",
    "warehouse a185": "A185",
    "a68": "A68",
    "warehouse a68": "A68",
    "f53": "F53",
    "warehouse f53": "F53",
    "dev int": "Dev Int",
    "dev_int": "Dev Int",
}


def canonical_warehouse(unit: Optional[str], storage_location: Optional[str]) -> Optional[str]:
    """Return the canonical warehouse name or None for unrecognized values.

    Cold-stocks rows have BOTH ``unit`` (e.g. 'D-39') and
    ``storage_location`` (e.g. 'SAVLA', 'OLD SAVLA'). The unit alone
    disambiguates Savla D-39 vs Savla D-514, so it takes priority.
    """
    if unit:
        key = unit.strip().lower().replace("_", " ")
        if key in WAREHOUSE_ALIASES:
            return WAREHOUSE_ALIASES[key]
    if storage_location:
        key = storage_location.strip().lower().replace("_", " ")
        if key in WAREHOUSE_ALIASES:
            return WAREHOUSE_ALIASES[key]
        # Direct case-insensitive match against canonical names
        for cw in CANONICAL_WAREHOUSES:
            if key == cw.lower():
                return cw
    return None  # caller decides what to do (typically: bucket as "Other")


# ---------------------------------------------------------------------------
# Group / sub-group canonicalization (looks up all_sku)
# ---------------------------------------------------------------------------

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def _fold(s: Optional[str]) -> str:
    """Lowercase + strip non-alphanumeric for case-/punctuation-insensitive compare."""
    if not s:
        return ""
    return _NON_ALNUM.sub("", s.lower())


def canonical_group_subgroup(
    item_description: Optional[str],
    fallback_group: Optional[str] = None,
    fallback_subgroup: Optional[str] = None,
    sku_index: Optional[dict] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """Look up canonical (item_group, sub_group) for an item_description from
    ``all_sku``. Falls back to a case-folded version of the input strings if
    no match is found, so "Fard" / "FARD" / "fard" all collapse to "Fard".

    sku_index: optional pre-built {fold(particulars): (item_group, sub_group)}
    for batch backfill; if None, this function is a no-op (relies on DB index
    only when ``build_sku_index`` is called explicitly).
    """
    if sku_index and item_description:
        hit = sku_index.get(_fold(item_description))
        if hit:
            return hit
    # Fallback: case-fold the inputs to merge "DATES" / "dates" / "Dates"
    return (
        _title(fallback_group),
        _title(fallback_subgroup),
    )


def _title(s: Optional[str]) -> Optional[str]:
    """Title-case a free-text label so 'DATES', 'dates', 'Dates' collapse."""
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    # Acronyms stay upper; otherwise Title Case each word
    return " ".join(
        w.upper() if (len(w) <= 3 and w.isalpha() and w.isupper()) else w.capitalize()
        for w in s.split()
    )


def build_sku_index(db: Session) -> dict:
    """Pre-load ``all_sku`` into an in-memory dict for batch backfill.

    Returns {folded_particulars: (item_group_title, sub_group_title)}.
    Empty dict if the table is missing.
    """
    exists = db.execute(text("SELECT to_regclass('public.all_sku')")).scalar()
    if not exists:
        return {}
    rows = db.execute(
        text("SELECT particulars, item_group, sub_group FROM all_sku WHERE particulars IS NOT NULL")
    ).fetchall()
    idx: dict = {}
    for r in rows:
        key = _fold(r.particulars)
        if not key:
            continue
        idx[key] = (_title(r.item_group), _title(r.sub_group))
    return idx


# ---------------------------------------------------------------------------
# Transaction resolver
# ---------------------------------------------------------------------------

def resolve_transaction(db: Session, transaction_no: str) -> dict:
    """Find which module a transaction number belongs to.

    Returns a dict::
        {
          "transaction_no": "...",
          "type": "inward" | "transfer" | "bulk_entry" | "outward" | "rtv" | None,
          "url_path": "/inward/{txn}" | "/transfer/{id}" | ... | None,
          "company": "cfpl" | "cdpl" | None,
          "exists_in_ims": bool,
        }

    Used by the dashboard "Open Transaction" buttons. If ``exists_in_ims``
    is False, the button should be disabled (legacy data with no IMS record)
    and a Rectify CTA shown instead.
    """
    txn = (transaction_no or "").strip()
    if not txn:
        return {"transaction_no": "", "type": None, "url_path": None, "company": None, "exists_in_ims": False}

    upper = txn.upper()

    # 1. Transfer (TRANS prefix → interunit_transfers_header.challan_no)
    if upper.startswith("TRANS"):
        row = db.execute(
            text("SELECT id FROM interunit_transfers_header WHERE challan_no = :c LIMIT 1"),
            {"c": txn},
        ).fetchone()
        if row:
            return {
                "transaction_no": txn, "type": "transfer",
                "url_path": f"/transfer/view/{row.id}",
                "company": None, "exists_in_ims": True,
            }

    # 2. Inward (TR-... prefix → company transaction tables)
    if upper.startswith("TR-") or upper.startswith("TR2"):
        for prefix in ("cfpl", "cdpl"):
            tbl = f"{prefix}_transactions"
            exists = db.execute(text("SELECT to_regclass(:t)"), {"t": f"public.{tbl}"}).scalar()
            if not exists:
                continue
            row = db.execute(
                text(f"SELECT 1 FROM {tbl} WHERE transaction_no = :t LIMIT 1"),
                {"t": txn},
            ).fetchone()
            if row:
                return {
                    "transaction_no": txn, "type": "inward",
                    "url_path": f"/inward/{txn}",
                    "company": prefix, "exists_in_ims": True,
                }

    # 3. Bulk Entry (BE- prefix → company bulk entry tables)
    if upper.startswith("BE-"):
        for prefix in ("cfpl", "cdpl"):
            tbl = f"{prefix}_bulk_entry_transactions"
            exists = db.execute(text("SELECT to_regclass(:t)"), {"t": f"public.{tbl}"}).scalar()
            if not exists:
                continue
            row = db.execute(
                text(f"SELECT 1 FROM {tbl} WHERE transaction_no = :t LIMIT 1"),
                {"t": txn},
            ).fetchone()
            if row:
                return {
                    "transaction_no": txn, "type": "bulk_entry",
                    "url_path": f"/inward/{txn}",
                    "company": prefix, "exists_in_ims": True,
                }

    # 4. Outward
    out_table_exists = db.execute(text("SELECT to_regclass('public.outward_consignments')")).scalar()
    if out_table_exists:
        row = db.execute(
            text("SELECT id FROM outward_consignments WHERE consignment_no = :t LIMIT 1"),
            {"t": txn},
        ).fetchone()
        if row:
            return {
                "transaction_no": txn, "type": "outward",
                "url_path": f"/outward/{row.id}",
                "company": None, "exists_in_ims": True,
            }

    # 5. Not found anywhere — legacy / pre-IMS data
    return {
        "transaction_no": txn,
        "type": None,
        "url_path": None,
        "company": None,
        "exists_in_ims": False,
    }
