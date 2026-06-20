"""Dependency-free: RTV models must retain the cold per-article fields.
Run: python test_rtv_cold_fields.py
"""
from services.ims_service import rtv_tools
from services.ims_service.rtv_models import (
    RTVLineCreate, RTVBoxUpsertRequest, RTVApprovalLineFields,
    RTVApprovalBoxFields, RTVBulkBoxItem, RTVBulkBoxUpdateRequest,
    RTVCreate, RTVHeaderCreate, RTVApprovalRequest,
)


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)

    def __getattr__(self, name):
        # Any column the mapper reads that we didn't explicitly set is None.
        return None


def test_line_create_keeps_cold_fields():
    line = RTVLineCreate(
        material_type="rm", item_category="DATES", sub_category="KHALAS",
        item_description="al barakah khalas dates", uom="10",
        lot_number="7648", item_mark="MARK-1", spl_remarks="handle cold", vakkal="VK-99",
    )
    d = line.model_dump(exclude_none=True)
    for f in ("lot_number", "item_mark", "spl_remarks", "vakkal"):
        assert d[f], f"{f} dropped by RTVLineCreate"
    print("test_line_create_keeps_cold_fields: PASS")


def test_box_upsert_keeps_cold_fields():
    box = RTVBoxUpsertRequest(
        article_description="al barakah khalas dates", box_number=1,
        lot_number="7648", item_mark="MARK-1", spl_remarks="cold", vakkal="VK-99",
    )
    d = box.model_dump(exclude_none=True)
    for f in ("lot_number", "item_mark", "spl_remarks", "vakkal"):
        assert d[f], f"{f} dropped by RTVBoxUpsertRequest"
    print("test_box_upsert_keeps_cold_fields: PASS")


def test_approval_models_keep_cold_fields():
    line = RTVApprovalLineFields(item_description="x", lot_number="1", item_mark="m", spl_remarks="r", vakkal="v")
    box = RTVApprovalBoxFields(article_description="x", box_number=1, lot_number="1", item_mark="m", spl_remarks="r", vakkal="v")
    for f in ("lot_number", "item_mark", "spl_remarks", "vakkal"):
        assert line.model_dump(exclude_none=True)[f]
        assert box.model_dump(exclude_none=True)[f]
    print("test_approval_models_keep_cold_fields: PASS")


def test_bulk_box_request_keeps_cold_fields():
    req = RTVBulkBoxUpdateRequest(boxes=[RTVBulkBoxItem(
        article_description="x", box_number=1, lot_number="1",
        item_mark="m", spl_remarks="r", vakkal="v", net_weight="1.0", gross_weight="1.2",
    )])
    d = req.boxes[0].model_dump(exclude_none=True)
    for f in ("lot_number", "item_mark", "spl_remarks", "vakkal"):
        assert d[f], f"{f} dropped by RTVBulkBoxItem"
    print("test_bulk_box_request_keeps_cold_fields: PASS")


def test_create_rtv_persists_cold_line_fields():
    captured = []

    class _Result:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

    class CaptureDB:
        def execute(self, clause, params=None):
            sql = str(clause)
            params = params or {}
            captured.append((sql, params))
            if "INTO" in sql and "rtv_header" in sql:
                return _Result(_Row(id=1, **params))
            if "INTO" in sql and "rtv_lines" in sql:
                return _Result(_Row(
                    id=2, created_at=None, updated_at=None, **params
                ))
            return _Result(None)

        def commit(self):
            pass

    data = RTVCreate(
        company="CFPL",
        header=RTVHeaderCreate(factory_unit="Savla D-39", customer="c"),
        lines=[RTVLineCreate(
            material_type="rm", item_category="DATES", sub_category="KHALAS",
            item_description="al barakah khalas dates", uom="10",
            lot_number="7648", item_mark="MARK-1", spl_remarks="handle cold", vakkal="VK-99",
        )],
    )
    rtv_tools.create_rtv(data, "tester", CaptureDB())

    line_inserts = [
        (sql, p) for sql, p in captured
        if "INTO" in sql and "rtv_lines" in sql
    ]
    assert line_inserts, "no rtv_lines INSERT captured"
    sql, p = line_inserts[0]
    assert p["lot_number"] == "7648"
    assert p["item_mark"] == "MARK-1"
    assert p["spl_remarks"] == "handle cold"
    assert p["vakkal"] == "VK-99"
    assert "lot_number" in sql, "lot_number column missing from rtv_lines INSERT SQL"
    print("test_create_rtv_persists_cold_line_fields: PASS")


def test_upsert_box_persists_cold_fields():
    class _Result:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

    captured = []

    class FakeDB:
        def execute(self, clause, params=None):
            sql = str(clause)
            params = params or {}
            captured.append((sql, params))
            if "FROM" in sql and "rtv_header" in sql:
                return _Result(_Row(id=1, rtv_id="RTV-X"))
            if "FROM" in sql and "rtv_lines" in sql:
                return _Result(_Row(id=10))
            if "FROM" in sql and "rtv_boxes" in sql and "SELECT" in sql:
                return _Result(None)
            return _Result(None)

        def commit(self):
            pass

    payload = RTVBoxUpsertRequest(
        article_description="al barakah khalas dates", box_number=1,
        lot_number="7648", item_mark="MARK-1", spl_remarks="cold", vakkal="VK-99",
        net_weight="1.0", gross_weight="1.2", count=2,
    )
    rtv_tools.upsert_rtv_box("CFPL", 1, payload, FakeDB())

    box_inserts = [
        (sql, p) for sql, p in captured
        if "INTO" in sql and "rtv_boxes" in sql
    ]
    assert box_inserts, "no rtv_boxes INSERT captured"
    sql, p = box_inserts[0]
    for col in ("item_mark", "spl_remarks", "vakkal", "lot_number"):
        assert col in sql, f"{col} column missing from rtv_boxes INSERT SQL"
    assert p["item_mark"] == "MARK-1"
    assert p["spl_remarks"] == "cold"
    assert p["vakkal"] == "VK-99"
    assert p["lot_number"] == "7648"
    print("test_upsert_box_persists_cold_fields: PASS")


def test_approve_rtv_persists_cold_box_fields():
    class _Result:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

    captured = []

    class ApproveDB:
        def execute(self, clause, params=None):
            sql = str(clause)
            params = params or {}
            captured.append((sql, params))
            if "FROM" in sql and "rtv_header" in sql and "SELECT" in sql:
                return _Result(_Row(id=1, rtv_id="RTV-X", status="Pending"))
            if "FROM" in sql and "rtv_boxes" in sql and "SELECT" in sql:
                return _Result(None)
            return _Result(None)

        def commit(self):
            pass

    payload = RTVApprovalRequest(
        approved_by="t",
        boxes=[RTVApprovalBoxFields(
            article_description="x", box_number=1,
            lot_number="7648", item_mark="MARK-1", spl_remarks="cold", vakkal="VK-99",
            net_weight="1.0", gross_weight="1.2",
        )],
    )
    rtv_tools.approve_rtv("CFPL", 1, payload, ApproveDB())

    box_inserts = [
        (sql, p) for sql, p in captured
        if "INTO" in sql and "rtv_boxes" in sql
    ]
    assert box_inserts, "no rtv_boxes INSERT captured"
    sql, p = box_inserts[0]
    for col in ("lot_number", "item_mark", "spl_remarks", "vakkal"):
        assert col in sql, f"{col} column missing from approve_rtv box INSERT SQL"
    assert p["lot_number"] == "7648"
    assert p["item_mark"] == "MARK-1"
    assert p["spl_remarks"] == "cold"
    assert p["vakkal"] == "VK-99"
    # Approve-created cold boxes must carry a box_id so they mirror into
    # cold_stocks (which filters box_id IS NOT NULL).
    assert "box_id" in sql, "box_id column missing from approve_rtv box INSERT SQL"
    assert p.get("box_id"), "approve_rtv box INSERT params missing non-empty box_id"
    print("test_approve_rtv_persists_cold_box_fields: PASS")


if __name__ == "__main__":
    test_line_create_keeps_cold_fields()
    test_box_upsert_keeps_cold_fields()
    test_approval_models_keep_cold_fields()
    test_bulk_box_request_keeps_cold_fields()
    test_create_rtv_persists_cold_line_fields()
    test_upsert_box_persists_cold_fields()
    test_approve_rtv_persists_cold_box_fields()
    print("ALL PASS")
