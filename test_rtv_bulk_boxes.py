"""Dependency-free: bulk_save_boxes inserts new boxes (with cold fields) and
deletes boxes no longer present. Run: python test_rtv_bulk_boxes.py
"""
from services.ims_service import rtv_tools
from services.ims_service.rtv_models import RTVBulkBoxUpdateRequest, RTVBulkBoxItem


class _Row:
    def __init__(self, **kw): self.__dict__.update(kw)


class BulkDB:
    def __init__(self, existing):
        self.existing = existing
        self.calls = []
    def execute(self, clause, params=None):
        sql = str(clause); self.calls.append((sql, params or {}))
        class R:
            def __init__(self, rows): self._rows = rows
            def fetchone(self): return self._rows[0] if self._rows else None
            def fetchall(self): return self._rows
        if "SELECT id, rtv_id FROM" in sql:
            return R([_Row(id=1, rtv_id="RTV-X")])
        if "FROM" in sql and "rtv_boxes" in sql and "SELECT" in sql:
            return R(self.existing)
        if "SELECT id FROM" in sql:
            return R([_Row(id=10)])
        return R([])
    def commit(self): pass


def test_bulk_save_inserts_and_deletes():
    existing = [_Row(box_number=1, box_id="b1", article_description="x"),
                _Row(box_number=2, box_id="b2", article_description="x")]
    db = BulkDB(existing)
    req = RTVBulkBoxUpdateRequest(boxes=[
        RTVBulkBoxItem(article_description="x", box_number=1, lot_number="L1",
                       item_mark="M", spl_remarks="R", vakkal="V", net_weight="1.0", gross_weight="1.2"),
        RTVBulkBoxItem(article_description="x", box_number=3, lot_number="L3",
                       item_mark="M3", net_weight="2.0", gross_weight="2.3"),
    ])
    res = rtv_tools.bulk_save_boxes("CFPL", 1, req, db, notify_discrepancy=False)
    assert res["inserted"] >= 1
    assert res["deleted"] >= 1
    ins = [(s, p) for s, p in db.calls if "INTO" in s and "rtv_boxes" in s]
    assert any("item_mark" in s for s, _ in ins), "cold fields not in bulk INSERT"
    print("test_bulk_save_inserts_and_deletes: PASS")


def test_bulk_insert_box_ids_unique_across_articles():
    db = BulkDB(existing=[])  # nothing existing -> both take INSERT branch
    req = RTVBulkBoxUpdateRequest(boxes=[
        RTVBulkBoxItem(article_description="A", box_number=1, net_weight="1"),
        RTVBulkBoxItem(article_description="B", box_number=1, net_weight="1"),
    ])
    rtv_tools.bulk_save_boxes("CFPL", 1, req, db, notify_discrepancy=False)
    box_ids = [p.get("box_id") for s, p in db.calls if "INTO" in s and "rtv_boxes" in s]
    assert len(box_ids) == 2, f"expected 2 inserts, got {box_ids}"
    assert len(set(box_ids)) == 2, f"box_id collision across articles: {box_ids}"
    print("test_bulk_insert_box_ids_unique_across_articles: PASS")


if __name__ == "__main__":
    test_bulk_save_inserts_and_deletes()
    test_bulk_insert_box_ids_unique_across_articles()
    print("ALL PASS")
