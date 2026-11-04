"""Dependency-free regression tests: editing an inward that lives in the *_bulk_entry_*
fallback tables must not touch ``line_number`` — those tables have no such column.

Seen in prod 2026-11-03 on CFPL TR-20261103164557 (bulk inward, 2411 boxes): the edit
page returned "AttributeError: Could not locate column in row for column 'line_number'"
because update_inward read ``r.line_number`` from cfpl_bulk_entry_articles rows.

No database required:
    PYTHONPATH=. python -m pytest <this file> -q -p no:cacheprovider
"""
from services.ims_service.inward_models import ArticleIn, InwardUpdatePayload, TransactionUpdateIn
from services.ims_service.inward_tools import update_inward

BULK_ART_COLS = [
    "id", "transaction_no", "sku_id", "item_description", "item_category", "sub_category",
    "material_type", "quality_grade", "uom", "units", "po_quantity", "quantity_units",
    "net_weight", "total_weight", "po_weight", "lot_number", "manufacturing_date",
    "expiry_date", "unit_rate", "total_amount", "carton_weight", "box_count", "created_at",
    "updated_at", "item_mark", "spl_remarks", "vakkal",
]


class FakeRow:
    """Behaves like a SQLAlchemy Row: attribute access, ``_mapping`` and index access.
    A column the SELECT did not return raises exactly as SQLAlchemy does."""

    def __init__(self, **cols):
        object.__setattr__(self, "_cols", cols)

    def __getattr__(self, name):
        try:
            return self._cols[name]
        except KeyError:
            raise AttributeError(f"Could not locate column in row for column '{name}'") from None

    def __getitem__(self, i):
        return list(self._cols.values())[i]

    @property
    def _mapping(self):
        return dict(self._cols)


class Res:
    def __init__(self, rows=None, rowcount=1):
        self.rows, self.rowcount = rows or [], rowcount

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows

    def scalar(self):
        return 0


class BulkEntryDB:
    """Routes the reads update_inward performs for a transaction that exists only in the
    cfpl_bulk_entry_* tables; records every statement."""

    def __init__(self, existing_articles):
        self.existing_articles = existing_articles
        self.calls = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.calls.append((sql, params))
        if "FROM cfpl_transactions_v2" in sql:
            return Res([])                                   # not a v2 inward -> fallback
        if "FROM cfpl_bulk_entry_transactions" in sql:
            return Res([FakeRow(transaction_no="TR-1", entry_date="2026-11-03", warehouse="W202",
                                vendor_supplier_name="V", status="approved")])
        if "information_schema.columns" in sql:
            return Res([FakeRow(column_name=c) for c in BULK_ART_COLS])
        if "SELECT * FROM cfpl_bulk_entry_articles" in sql:
            return Res(self.existing_articles)
        return Res([])

    def commit(self):
        pass

    def find(self, needle):
        return [(sql, params) for sql, params in self.calls if needle in sql]


def _payload(article):
    return InwardUpdatePayload(
        company="CFPL",
        transaction=TransactionUpdateIn(transaction_no="TR-1", entry_date="2026-11-03"),
        articles=[article],
        boxes=[],
    )


def _bulk_article_row(**over):
    row = {c: None for c in BULK_ART_COLS}          # bulk_entry rows carry NO line_number key
    row.update(id=100, transaction_no="TR-1", item_description="fardh dates",
               quality_grade="FARD AB WET DATES", unit_rate=130, lot_number="30325")
    row.update(over)
    return FakeRow(**row)


def test_edit_existing_bulk_entry_article_updates_by_description():
    db = BulkEntryDB([_bulk_article_row()])
    art = ArticleIn(transaction_no="TR-1", line_number=1, item_description="fardh dates",
                    quality_grade="FARD AB WET DATES", unit_rate=135, lot_number="30325")

    out = update_inward("CFPL", "TR-1", _payload(art), db)

    assert out["status"] == "updated" and out["articles_count"] == 1, out
    upd = db.find("UPDATE cfpl_bulk_entry_articles")
    assert len(upd) == 1, [c[0] for c in db.calls]
    sql, params = upd[0]
    assert "item_description = :art_key" in sql and "line_number" not in sql, sql
    assert params["art_key"] == "fardh dates" and params["unit_rate"] == 135, params


def test_new_bulk_entry_article_insert_omits_line_number():
    db = BulkEntryDB([_bulk_article_row()])
    art = ArticleIn(transaction_no="TR-1", line_number=2, item_description="medjool dates",
                    unit_rate=200)

    update_inward("CFPL", "TR-1", _payload(art), db)

    ins = db.find("INSERT INTO cfpl_bulk_entry_articles")
    assert len(ins) == 1, [c[0] for c in db.calls]
    sql, params = ins[0]
    assert "line_number" not in sql, sql
    assert params[0]["item_description"] == "medjool dates", params
