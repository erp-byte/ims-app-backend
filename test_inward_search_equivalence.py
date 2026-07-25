"""Search rewrite guard: the EXISTS-based predicate must match the transaction set
the old LEFT JOIN + DISTINCT predicate matched, for text, numeric and mixed terms.

Run: python test_inward_search_equivalence.py
"""
import sys

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from services.ims_service.inward_tools import (
    build_search_conditions,
    table_names,
    union_source_ctes,
)

# The pre-rewrite predicate, kept verbatim as the reference implementation.
_OLD_TEXT = [
    "t.transaction_no", "t.vehicle_number", "t.transporter_name", "t.lr_number",
    "t.vendor_supplier_name", "t.customer_party_name", "t.source_location",
    "t.destination_location", "t.challan_number", "t.invoice_number", "t.po_number",
    "t.grn_number", "t.purchased_by", "t.service_invoice_number", "t.dn_number",
    "t.approval_authority", "t.warehouse", "t.remark", "t.currency",
    "a.item_description", "a.item_category", "a.sub_category", "a.material_type",
    "a.quality_grade", "a.uom", "a.units", "a.lot_number",
    "b.article_description", "b.lot_number", "b.box_id",
]
_OLD_NUM = [
    "t.grn_quantity", "t.total_amount", "t.tax_amount", "t.discount_amount",
    "t.po_quantity", "a.sku_id", "a.po_weight", "a.po_quantity", "a.quantity_units",
    "a.net_weight", "a.total_weight", "a.unit_rate", "a.total_amount",
    "a.carton_weight", "b.box_number", "b.net_weight", "b.gross_weight", "b.count",
]


def old_matches(db, company, term):
    conds = [f"COALESCE({f}, '') ILIKE :search" for f in _OLD_TEXT]
    conds += [f"CAST(COALESCE({f}, 0) AS TEXT) ILIKE :search" for f in _OLD_NUM]
    return set(
        db.execute(
            text(f"""
                WITH {union_source_ctes(company)}
                SELECT DISTINCT t.transaction_no, t._source
                FROM all_tx t
                LEFT JOIN all_art a
                    ON t.transaction_no = a.transaction_no AND t._source = a._source
                LEFT JOIN all_box b
                    ON t.transaction_no = b.transaction_no AND t._source = b._source
                WHERE ({' OR '.join(conds)})
            """),
            {"search": f"%{term}%"},
        ).fetchall()
    )


def new_matches(db, company, term):
    where_sql, params = build_search_conditions(table_names(company), term, None, None)
    return set(
        db.execute(
            text(f"""
                WITH {union_source_ctes(company)}
                SELECT t.transaction_no, t._source FROM all_tx t WHERE {where_sql}
            """),
            params,
        ).fetchall()
    )


def demo():
    url = next(
        l.split("=", 1)[1].strip() for l in open(".env") if l.startswith("DATABASE_URL")
    )
    db = sessionmaker(bind=create_engine(url, connect_args={"connect_timeout": 10}))()
    terms = ["SAVLA", "BOX", "CFPL", "chicken", "123", "2026", "TXN", "0.5", "IN-"]
    try:
        for company in ("CFPL", "CDPL"):
            for t in terms:
                old, new = old_matches(db, company, t), new_matches(db, company, t)
                assert old == new, (
                    f"{company} {t!r}: only-old={sorted(old - new)[:5]} "
                    f"only-new={sorted(new - old)[:5]}"
                )
                print(f"  ok {company} {t!r:10s} -> {len(new)} transactions")
    finally:
        db.close()
    print("all search terms match the pre-rewrite result set")


if __name__ == "__main__":
    sys.exit(demo())
