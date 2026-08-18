"""Dependency-free unit tests for unpick_to_pending's source attribution.

Bug: re-opening a received transfer (unpick_to_pending) re-parks each box into
pending_transfer_stock with a GUESSED source_table and a NULL source_row_id:

    source_table_guess = ("cfpl_cold_stocks" if from_storage_type == "cold" and to_company == "cfpl"
                          else "cdpl_cold_stocks" if from_storage_type == "cold"
                          else "cfpl_bulk_entry_boxes" if to_company == "cfpl"
                          else "cdpl_bulk_entry_boxes")

The company comes from the DESTINATION table, not the source, so a cross-company
cold transfer is attributed to the wrong ledger. That matters because
restore_to_source (cancel / delete a transfer) INSERTs straight into whatever
source_table says, guarded only by an untargeted ON CONFLICT DO NOTHING and with
no presence check at all -- so a mis-attributed box is recreated in the other
company's cold_stocks, and a quantity-only bookkeeping line whose original park
carried an EMPTY source_table is recreated as real cold stock that never existed.

cold_stock_disposition already records the truth: it is append-only, written at
park time, and carries source_table, from_company and a snapshot of the source
row. Unpick should read it back rather than guess.

No database required:  python test_unpick_source_attribution.py
"""
from types import SimpleNamespace

from services.ims_service import pending_stock_tools as P


class Res:
    def __init__(self, rows=None, scalar=None):
        self._rows, self._scalar = rows or [], scalar
    def fetchall(self): return self._rows
    def fetchone(self): return self._rows[0] if self._rows else None
    def scalar(self): return self._scalar


def _box(box_id="B1", txn="TR-1", article="Deri Dates", lot="L1",
         batch=None, net=10.0, gross=11.0, out_box_id=None):
    return SimpleNamespace(box_id=box_id, transaction_no=txn, article=article,
                           lot_number=lot, batch_number=batch,
                           net_weight=net, gross_weight=gross,
                           transfer_out_box_id=out_box_id)


def _db(boxes, *, from_site="Cold Storage", to_site="W202",
        dest_hit=None, disposition=None):
    """Mock for unpick_to_pending.

    dest_hit    : table name where the box is found at the destination, or None
    disposition : dict(source_table=..., from_company=..., snapshot_id=...) or None
    """
    inserts = []

    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt); p = params or {}
            up = sql.upper()
            if "FROM interunit_transfers_header WHERE id" in sql:
                return Res(rows=[SimpleNamespace(
                    id=42, challan_no="TRANS-1", from_site=from_site, to_site=to_site,
                    created_by="op", stock_trf_date=None, created_ts=None)])
            if "FROM interunit_transfer_in_boxes WHERE header_id" in sql:
                return Res(rows=boxes)
            if "to_regclass" in sql:
                return Res(scalar="present")
            if "FROM cold_stock_disposition" in sql:
                if not disposition:
                    return Res(rows=[])
                return Res(rows=[SimpleNamespace(
                    source_table=disposition.get("source_table"),
                    from_company=disposition.get("from_company"),
                    snapshot_data={"id": disposition.get("snapshot_id")}
                    if disposition.get("snapshot_id") else None)])
            if up.startswith("INSERT INTO PENDING_TRANSFER_STOCK") or \
               "INSERT INTO pending_transfer_stock" in sql:
                inserts.append(p)
                return Res()
            # destination probe: SELECT id FROM <tbl> WHERE box_id = ...
            if sql.strip().upper().startswith("SELECT ID FROM") and "box_id" in sql:
                tbl = sql.split("FROM")[1].split()[0].strip()
                return Res(rows=[SimpleNamespace(id=1)] if tbl == dest_hit else [])
            if sql.strip().upper().startswith("SELECT * FROM") and "box_id" in sql:
                return Res(rows=[])
            return Res()
    return DB(), inserts


def test_unpick_uses_the_disposition_ledger_source_not_the_guess():
    """Cold->cold across companies. The box came from cdpl_cold_stocks; the guess
    derives its company from the DESTINATION and would say cfpl_cold_stocks, so a
    later cancel recreates the carton in the wrong company's ledger."""
    db, inserts = _db([_box()], from_site="Cold Storage", to_site="Cold Storage",
                      dest_hit="cfpl_cold_stocks",
                      disposition={"source_table": "cdpl_cold_stocks",
                                   "from_company": "cdpl", "snapshot_id": 777})
    P.unpick_to_pending(9, 42, db)
    assert len(inserts) == 1, inserts
    assert inserts[0]["source_table"] == "cdpl_cold_stocks", inserts[0]
    print("PASS test_unpick_uses_the_disposition_ledger_source_not_the_guess")


def test_unpick_restores_source_row_id_from_the_ledger_snapshot():
    """source_row_id was hardcoded NULL, discarding the link to the source row."""
    db, inserts = _db([_box()], dest_hit="cfpl_cold_stocks",
                      disposition={"source_table": "cfpl_cold_stocks",
                                   "from_company": "cfpl", "snapshot_id": 777})
    P.unpick_to_pending(9, 42, db)
    assert inserts[0]["source_row_id"] == 777, inserts[0]
    print("PASS test_unpick_restores_source_row_id_from_the_ledger_snapshot")


def test_unpick_carries_the_ledger_from_company():
    """from_company was hardcoded 'cfpl' regardless of where the box came from."""
    db, inserts = _db([_box()], dest_hit="cfpl_cold_stocks",
                      disposition={"source_table": "cdpl_cold_stocks",
                                   "from_company": "cdpl", "snapshot_id": 1})
    P.unpick_to_pending(9, 42, db)
    assert inserts[0]["from_company"] == "cdpl", inserts[0]
    print("PASS test_unpick_carries_the_ledger_from_company")


def test_unpick_preserves_empty_source_for_a_bookkeeping_line():
    """A quantity-only line was parked by park_lines_in_pending with an EMPTY
    source_table so restore_to_source would no-op it. It has no ledger row and no
    dispatch box. Substituting a real cold table turns a placeholder into stock
    the next cancel would INSERT into cold_stocks."""
    db, inserts = _db([_box(box_id="MINTED-1", out_box_id=None)],
                      dest_hit=None, disposition=None)
    P.unpick_to_pending(9, 42, db)
    assert inserts[0]["source_table"] == "", (
        f"expected the empty sentinel, got {inserts[0]['source_table']!r}")
    print("PASS test_unpick_preserves_empty_source_for_a_bookkeeping_line")


def test_unpick_keeps_the_guess_for_a_dispatched_box_with_no_ledger_row():
    """A real scanned box whose ledger write failed still gets today's guess --
    changing that would strand a genuinely cold-sourced carton on cancel. It is a
    guess, so it is logged, but behaviour is unchanged."""
    db, inserts = _db([_box(box_id="B1", out_box_id=900)],
                      dest_hit="cfpl_cold_stocks", disposition=None)
    P.unpick_to_pending(9, 42, db)
    assert inserts[0]["source_table"] == "cfpl_cold_stocks", inserts[0]
    print("PASS test_unpick_keeps_the_guess_for_a_dispatched_box_with_no_ledger_row")


ALL = [
    test_unpick_uses_the_disposition_ledger_source_not_the_guess,
    test_unpick_restores_source_row_id_from_the_ledger_snapshot,
    test_unpick_carries_the_ledger_from_company,
    test_unpick_preserves_empty_source_for_a_bookkeeping_line,
    test_unpick_keeps_the_guess_for_a_dispatched_box_with_no_ledger_row,
]

if __name__ == "__main__":
    for t in ALL:
        t()
    print(f"\nALL {len(ALL)} TESTS PASSED")
