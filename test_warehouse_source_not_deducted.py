"""A warehouse transfer must never delete the box from the inward record.

THE DESIGN (owner, 2026-08-06)
    NORMAL transfer: the inward tables — {cfpl,cdpl}_boxes_v2, *_articles_v2,
        *_bulk_entry_boxes — are REFERENCE ONLY. They are read to pick the item
        and its detail when the operator scans. Dispatch writes a REPLICA into
        pending_transfer_stock; receipt removes the pending row. The source row
        is never touched.
    COLD transfer: the box MOVES. It is deducted from *_cold_stocks into
        pending_transfer_stock, and on receipt written to the destination table.

WHY THE DIFFERENCE IS CORRECT
    *_cold_stocks is a live inventory ledger — one row per box actually in the
    store. *_boxes_v2 is the goods-received record and has NO location column
    at all (id, transaction_no, article_description, box_number, net_weight,
    gross_weight, lot_number, count, created_at, box_id, line_number). A table
    that cannot say where a box is cannot be decremented to move it; deleting
    from it only erases the receipt.

WHAT THE BUG DID
    park_in_pending treated both the same and ran
        DELETE FROM {source_table} WHERE id = :rid
    for warehouse sources too. On received warehouse transfers, 1,216 boxes were
    dispatched and 1,152 now exist in NO stock table: deleted from inward at
    dispatch, pending row deleted at receipt, and never inserted at the
    destination (_do_pick computes destination_table and only logs it). Inward
    TR-20260310175034 dropped from 200 boxes to 180 this way.

Dependency-free:  python test_warehouse_source_not_deducted.py
"""
from services.ims_service.pending_stock_tools import (
    _delete_source_row, _deducts_source, _is_inward_table, _refuse_inward_write,
)


def test_inward_tables_are_recognised():
    """The tables that record GRNs. Transfer reads them and nothing else."""
    for t in ("cfpl_boxes_v2", "cdpl_boxes_v2", "cfpl_articles_v2", "cdpl_articles_v2",
              "cfpl_bulk_entry_boxes", "cdpl_bulk_entry_boxes",
              "cfpl_bulk_entry_articles", "cfpl_boxes", "cfpl_transactions_v2"):
        assert _is_inward_table(t) is True, t
    for t in ("cfpl_cold_stocks", "cdpl_cold_stocks", "pending_transfer_stock",
              "interunit_transfer_boxes", None, ""):
        assert _is_inward_table(t) is False, t


def test_every_kind_of_inward_write_is_refused():
    """Delete, restore, swap and un-receive all funnel through one refusal."""
    for t in ("cfpl_boxes_v2", "cdpl_bulk_entry_boxes", "cfpl_articles_v2"):
        assert _refuse_inward_write(t, "delete from", "BOX-1") is True, t
    for t in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
        assert _refuse_inward_write(t, "delete from", "BOX-1") is False, t


class FakeDB:
    """Records the SQL it is asked to run so the test can assert on it."""
    def __init__(self):
        self.sql = []

    def execute(self, stmt, params=None):
        self.sql.append((str(stmt), params or {}))
        return None


def test_cold_source_is_deducted():
    for table in ("cfpl_cold_stocks", "cdpl_cold_stocks"):
        db = FakeDB()
        assert _delete_source_row(db, table, 42) is True, table
        assert len(db.sql) == 1, db.sql
        assert "DELETE FROM " + table in db.sql[0][0]
        assert db.sql[0][1] == {"rid": 42}


def test_warehouse_source_is_never_deducted():
    """The whole point: inward is a reference, not a stock ledger."""
    for table in ("cfpl_boxes_v2", "cdpl_boxes_v2",
                  "cfpl_bulk_entry_boxes", "cdpl_bulk_entry_boxes"):
        db = FakeDB()
        assert _delete_source_row(db, table, 42) is False, table
        assert db.sql == [], f"{table} must issue NO sql, got {db.sql}"


def test_unknown_or_missing_table_is_left_alone():
    """Fail closed. An unrecognised source is not deducted — losing stock is
    worse than leaving a stale row for a human to look at."""
    for table in (None, "", "some_other_table"):
        db = FakeDB()
        assert _delete_source_row(db, table, 1) is False
        assert db.sql == []


def test_the_predicate_matches_the_design():
    assert _deducts_source("cfpl_cold_stocks") is True
    assert _deducts_source("cdpl_cold_stocks") is True
    assert _deducts_source("cfpl_boxes_v2") is False
    assert _deducts_source("cfpl_bulk_entry_boxes") is False
    assert _deducts_source(None) is False


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
    print("\nWarehouse source is reference-only; cold still moves.")
