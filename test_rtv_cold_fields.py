"""Dependency-free: RTV models must retain the cold per-article fields.
Run: python test_rtv_cold_fields.py
"""
from services.ims_service.rtv_models import (
    RTVLineCreate, RTVBoxUpsertRequest, RTVApprovalLineFields,
    RTVApprovalBoxFields, RTVBulkBoxItem, RTVBulkBoxUpdateRequest,
)


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


if __name__ == "__main__":
    test_line_create_keeps_cold_fields()
    test_box_upsert_keeps_cold_fields()
    test_approval_models_keep_cold_fields()
    test_bulk_box_request_keeps_cold_fields()
    print("ALL PASS")
