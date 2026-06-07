"""Unit tests for the fungible lot-fallback in pick_from_pending (no DB). """
import io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from types import SimpleNamespace
import services.ims_service.pending_stock_tools as P


class Res:
    def __init__(self, rows=None, scalar=None):
        self._rows = rows or []; self._scalar = scalar
    def fetchall(self): return self._rows
    def fetchone(self): return self._rows[0] if self._rows else None
    def scalar(self): return self._scalar


def _prow(pid, box_id, txn, lot, dest="cfpl_boxes_v2"):
    return SimpleNamespace(id=pid, box_id=box_id, transaction_no=txn, destination_table=dest,
                           cold_storage_data=None, item_description="X", no_of_cartons=1,
                           weight_kg=5.0, lot_no=lot, to_site="W202", transfer_out_challan_no="TR-X")


def _pick_db(rows, deleted):
    class DB:
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql: return Res(scalar="x")
            if "SELECT * FROM pending_transfer_stock" in sql and "In Transit" in sql:
                return Res(rows=rows)
            if sql.strip().upper().startswith("DELETE FROM PENDING_TRANSFER_STOCK"):
                deleted.append(params["id"]); return Res()
            return Res()
    return DB()


def test_fungible_relabeled_nulltxn_still_picks():
    # pending has real box_ids+txn; acknowledged boxes are RELABELED + txn=None but same lots
    rows = [_prow(1, "REAL-1", "TR1", "CF0403226"), _prow(2, "REAL-2", "TR1", "CF0403226"),
            _prow(3, "REAL-3", "TR2", "CF100326")]
    deleted = []
    ack = [{"box_id": "X1", "transaction_no": None, "lot_number": "CF0403226"},
           {"box_id": "X2", "transaction_no": None, "lot_number": "CF0403226"},
           {"box_id": "X3", "transaction_no": None, "lot_number": "CF100326"}]
    n = P.pick_from_pending(42, _pick_db(rows, deleted), acknowledged_boxes=ack)
    assert n == 3 and sorted(deleted) == [1, 2, 3], (n, deleted)
    print("PASS test_fungible_relabeled_nulltxn_still_picks")


def test_fungible_bounded_by_ack_count_per_lot():
    # 3 pending of lot L, but only 2 acknowledged for L -> only 2 picked (partial receipt safe)
    rows = [_prow(1, "R1", "T", "L"), _prow(2, "R2", "T", "L"), _prow(3, "R3", "T", "L")]
    deleted = []
    ack = [{"box_id": "X1", "transaction_no": None, "lot_number": "L"},
           {"box_id": "X2", "transaction_no": None, "lot_number": "L"}]
    n = P.pick_from_pending(42, _pick_db(rows, deleted), acknowledged_boxes=ack)
    assert n == 2 and len(deleted) == 2, (n, deleted)
    print("PASS test_fungible_bounded_by_ack_count_per_lot")


def test_exact_match_still_preferred_then_fallback():
    rows = [_prow(1, "A", "T", "L1"), _prow(2, "R2", "T", "L2")]
    deleted = []
    ack = [{"box_id": "A", "transaction_no": "T", "lot_number": "L1"},   # exact
           {"box_id": "ZZ", "transaction_no": None, "lot_number": "L2"}]  # fallback by lot
    n = P.pick_from_pending(42, _pick_db(rows, deleted), acknowledged_boxes=ack)
    assert n == 2 and sorted(deleted) == [1, 2], (n, deleted)
    print("PASS test_exact_match_still_preferred_then_fallback")


def test_wrong_lot_not_picked():
    # acknowledged lot doesn't exist in pending -> nothing picked (no over-pick)
    rows = [_prow(1, "R1", "T", "L")]
    deleted = []
    ack = [{"box_id": "X", "transaction_no": None, "lot_number": "OTHER"}]
    n = P.pick_from_pending(42, _pick_db(rows, deleted), acknowledged_boxes=ack)
    assert n == 0 and deleted == [], (n, deleted)
    print("PASS test_wrong_lot_not_picked")


def test_line_rows_always_picked_with_ack_boxes():
    rows = [_prow(1, "LINE-1", "T", "L"), _prow(2, "R2", "T", "L")]
    deleted = []
    ack = [{"box_id": "X", "transaction_no": None, "lot_number": "L"}]
    n = P.pick_from_pending(42, _pick_db(rows, deleted), acknowledged_boxes=ack)
    # LINE-1 always picked + 1 fallback for lot L -> 2
    assert n == 2 and sorted(deleted) == [1, 2], (n, deleted)
    print("PASS test_line_rows_always_picked_with_ack_boxes")


for fn in [test_fungible_relabeled_nulltxn_still_picks, test_fungible_bounded_by_ack_count_per_lot,
           test_exact_match_still_preferred_then_fallback, test_wrong_lot_not_picked,
           test_line_rows_always_picked_with_ack_boxes]:
    fn()
print("\nALL PICKFIX TESTS PASSED")
