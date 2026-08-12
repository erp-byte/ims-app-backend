"""Dependency-free tests for the same-name / different-grade article fix.

Root cause: articles were identified by item_description (the name), so two "cashew 320"
articles (grades M320 vs W320) collapsed — the 2nd article and its boxes were dropped by
ON CONFLICT, and the box lookup for article 2 returned article 1's box_id.

Fix: a stable per-transaction line_number identifies each article; boxes carry it too.
These tests assert the identity helpers and that create_inward now writes both articles
and both box sets keyed by line_number.

No database required:  python test_inward_line_number.py
"""
from services.ims_service.inward_models import (
    InwardPayloadFlexible,
    TransactionIn,
    ArticleIn,
    BoxIn,
)
from services.ims_service.inward_tools import (
    _assign_line_numbers,
    _resolve_box_line_numbers,
    _backfill_read_line_numbers,
    _is_v2_tables,
    _surface_orphan_box_articles,
    create_inward,
)


def _abox(line, desc, n, net=20.0, gross=22.2):
    return {"line_number": line, "article_description": desc, "box_number": n,
            "net_weight": net, "gross_weight": gross}


class MockRes:
    def __init__(self, rowcount=1, rows=None):
        self.rowcount = rowcount
        self._rows = rows or []
    def fetchone(self): return self._rows[0] if self._rows else None
    def fetchall(self): return self._rows
    def scalar(self): return 0


class CaptureDB:
    """Records every execute(sql, params); returns rowcount=1 so the tx insert 'succeeds'."""
    def __init__(self):
        self.calls = []
    def execute(self, stmt, params=None):
        self.calls.append((str(stmt), params))
        return MockRes(rowcount=1)
    def commit(self):
        pass

    def find(self, needle):
        return [(sql, params) for sql, params in self.calls if needle in sql]


# ── identity helpers ────────────────────────────────────────────────────────────
def test_assign_line_numbers_same_name():
    arts = [
        ArticleIn(transaction_no="TR-1", item_description="cashew 320", quality_grade="M320"),
        ArticleIn(transaction_no="TR-1", item_description="cashew 320", quality_grade="W320"),
    ]
    _assign_line_numbers(arts)
    assert [a.line_number for a in arts] == [1, 2], [a.line_number for a in arts]
    print("PASS test_assign_line_numbers_same_name")


def test_assign_line_numbers_preserves_supplied():
    arts = [
        ArticleIn(transaction_no="TR-1", line_number=5, item_description="A"),
        ArticleIn(transaction_no="TR-1", item_description="B"),  # gets next free (1)
    ]
    _assign_line_numbers(arts)
    assert arts[0].line_number == 5, arts[0].line_number
    assert arts[1].line_number == 1, arts[1].line_number
    print("PASS test_assign_line_numbers_preserves_supplied")


def test_resolve_box_line_numbers_uses_supplied():
    arts = [
        ArticleIn(transaction_no="TR-1", line_number=1, item_description="cashew 320"),
        ArticleIn(transaction_no="TR-1", line_number=2, item_description="cashew 320"),
    ]
    boxes = [
        BoxIn(transaction_no="TR-1", line_number=1, article_description="cashew 320", box_number=1),
        BoxIn(transaction_no="TR-1", line_number=2, article_description="cashew 320", box_number=1),
    ]
    _resolve_box_line_numbers(arts, boxes)
    # already supplied → untouched (this is the real-world path; the two boxes stay separate)
    assert [b.line_number for b in boxes] == [1, 2], [b.line_number for b in boxes]
    print("PASS test_resolve_box_line_numbers_uses_supplied")


def test_is_v2_tables():
    assert _is_v2_tables({"box": "cfpl_boxes_v2"}) is True
    assert _is_v2_tables({"box": "cfpl_bulk_entry_boxes"}) is False
    print("PASS test_is_v2_tables")


# ── create_inward end-to-end (mock DB) ───────────────────────────────────────────
def _make_payload():
    return InwardPayloadFlexible(
        company="CFPL",
        transaction=TransactionIn(transaction_no="TR-1", entry_date="2026-07-31"),
        articles=[
            # no sku_id → _ensure_skus is a no-op (no DB dependency)
            ArticleIn(transaction_no="TR-1", line_number=1, item_description="cashew 320", quality_grade="M320", unit_rate=700),
            ArticleIn(transaction_no="TR-1", line_number=2, item_description="cashew 320", quality_grade="W320", unit_rate=715),
        ],
        boxes=[
            BoxIn(transaction_no="TR-1", line_number=1, article_description="cashew 320", box_number=1),
            BoxIn(transaction_no="TR-1", line_number=2, article_description="cashew 320", box_number=1),
        ],
    )


def test_create_inward_writes_both_same_name_articles():
    db = CaptureDB()
    out = create_inward(_make_payload(), db)
    assert out["status"] == "ok", out

    art_inserts = db.find("cfpl_articles_v2")
    assert len(art_inserts) == 1, "one bulk article insert expected"
    sql, params = art_inserts[0]
    assert "ON CONFLICT (transaction_no, line_number)" in sql, sql
    # both same-name articles are in the insert, with distinct line_numbers
    assert isinstance(params, list) and len(params) == 2, params
    assert sorted(p["line_number"] for p in params) == [1, 2], params
    assert all(p["item_description"] == "cashew 320" for p in params), params
    print("PASS test_create_inward_writes_both_same_name_articles")


def test_create_inward_boxes_keyed_by_line_number():
    db = CaptureDB()
    create_inward(_make_payload(), db)

    box_inserts = db.find("cfpl_boxes_v2")
    assert len(box_inserts) == 1, "one bulk box insert expected"
    sql, params = box_inserts[0]
    assert "ON CONFLICT (transaction_no, line_number, box_number)" in sql, sql
    assert "line_number" in sql, sql
    # two boxes, both box_number 1 but distinct line_numbers → no collision
    assert isinstance(params, list) and len(params) == 2, params
    assert sorted(p["line_number"] for p in params) == [1, 2], params
    assert all(p["box_number"] == 1 for p in params), params
    print("PASS test_create_inward_boxes_keyed_by_line_number")


# ── orphan-box surfacing (the hover/view/review mismatch fix) ─────────────────────
def test_surface_orphan_by_line_number():
    # One real article (line 1); boxes exist for line 1 AND line 2 — line 2's article row
    # is missing (the "cashew 300" case). The line-2 boxes must surface as one article.
    articles = [{"line_number": 1, "item_description": "cashew 320"}]
    boxes = [_abox(1, "cashew 320", 1), _abox(1, "cashew 320", 2),
             _abox(2, "cashew 300", 1), _abox(2, "cashew 300", 2), _abox(2, "cashew 300", 3)]
    orphans = _surface_orphan_box_articles("TR-1", articles, boxes)
    assert len(orphans) == 1, orphans
    o = orphans[0]
    assert o["item_description"] == "cashew 300" and o["line_number"] == 2, o
    assert o["quantity_units"] == 3 and o["box_count"] == 3, o
    assert round(o["net_weight"], 1) == 60.0 and round(o["total_weight"], 1) == 66.6, o
    assert o["is_orphan_surfaced"] is True
    print("PASS test_surface_orphan_by_line_number")


def test_surface_orphan_by_description_pre_migration():
    # Pre-migration data: no line_number anywhere. Orphan is detected by article_description.
    articles = [{"line_number": None, "item_description": "cashew 320"}]
    boxes = [_abox(None, "cashew 320", 1), _abox(None, "cashew 300", 1)]
    orphans = _surface_orphan_box_articles("TR-1", articles, boxes)
    assert [o["item_description"] for o in orphans] == ["cashew 300"], orphans
    print("PASS test_surface_orphan_by_description_pre_migration")


def test_surface_no_orphans_returns_empty():
    articles = [{"line_number": 1, "item_description": "cashew 320"}]
    boxes = [_abox(1, "cashew 320", 1), _abox(1, "cashew 320", 2)]
    assert _surface_orphan_box_articles("TR-1", articles, boxes) == []
    print("PASS test_surface_no_orphans_returns_empty")


def test_surface_all_when_no_article_rows():
    # subsumes the old "synthesize from boxes when empty" behaviour
    boxes = [_abox(1, "A", 1), _abox(2, "B", 1)]
    orphans = _surface_orphan_box_articles("TR-1", [], boxes)
    assert sorted(o["item_description"] for o in orphans) == ["A", "B"], orphans
    print("PASS test_surface_all_when_no_article_rows")


# ── read-time line_number backfill (boxes-not-loading regression) ───────────────
# Reads can return rows with no line_number: it is nullable on *_v2 (legacy insert
# paths) and absent entirely on *_bulk_entry_* . Consumers then filled the gap
# inconsistently (article -> idx+1, box -> 0), so no box matched its article and the
# UI showed correct box-derived totals above an empty box list.
def test_backfill_bulk_entry_boxes_have_no_line_column():
    # *_bulk_entry_boxes rows literally have no line_number key at all.
    articles = [{"item_description": "indian green raisins"}]
    boxes = [{"article_description": "indian green raisins", "box_number": n} for n in (1, 2, 3)]
    _backfill_read_line_numbers(articles, boxes)
    assert articles[0]["line_number"] == 1, articles[0]
    assert [b["line_number"] for b in boxes] == [1, 1, 1], boxes
    print("PASS test_backfill_bulk_entry_boxes_have_no_line_column")


def test_backfill_null_line_numbers_on_v2():
    articles = [{"item_description": "raisins", "line_number": None}]
    boxes = [{"article_description": "raisins", "box_number": 1, "line_number": None}]
    _backfill_read_line_numbers(articles, boxes)
    assert articles[0]["line_number"] == 1
    assert boxes[0]["line_number"] == 1
    print("PASS test_backfill_null_line_numbers_on_v2")


def test_backfill_matches_boxes_to_their_article_by_description():
    articles = [{"item_description": "A"}, {"item_description": "B"}]
    boxes = [
        {"article_description": "B", "box_number": 1},
        {"article_description": "A", "box_number": 1},
    ]
    _backfill_read_line_numbers(articles, boxes)
    assert [a["line_number"] for a in articles] == [1, 2]
    assert [b["line_number"] for b in boxes] == [2, 1], boxes
    print("PASS test_backfill_matches_boxes_to_their_article_by_description")


def test_backfill_preserves_supplied_lines():
    articles = [{"item_description": "A", "line_number": None},
                {"item_description": "B", "line_number": 1}]
    boxes = [{"article_description": "B", "box_number": 1, "line_number": 1},
             {"article_description": "A", "box_number": 1, "line_number": None}]
    _backfill_read_line_numbers(articles, boxes)
    # 1 is taken by B, so A takes the next free slot; supplied box lines are untouched.
    assert [a["line_number"] for a in articles] == [2, 1], articles
    assert [b["line_number"] for b in boxes] == [1, 2], boxes
    print("PASS test_backfill_preserves_supplied_lines")


def test_backfill_blank_box_description_falls_back_to_sole_article():
    articles = [{"item_description": "raisins"}]
    boxes = [{"article_description": "", "box_number": 1}]
    _backfill_read_line_numbers(articles, boxes)
    assert boxes[0]["line_number"] == 1, boxes
    print("PASS test_backfill_blank_box_description_falls_back_to_sole_article")


def test_backfill_orphan_boxes_get_lines_past_the_real_articles():
    # >1 article, so no sole-article fallback: "ghost" matches no article row.
    articles = [{"item_description": "A"}, {"item_description": "B"}]
    boxes = [{"article_description": "ghost", "box_number": 1},
             {"article_description": "ghost", "box_number": 2},
             {"article_description": "A", "box_number": 1}]
    _backfill_read_line_numbers(articles, boxes)
    assert [b["line_number"] for b in boxes] == [3, 3, 1], boxes
    # and they still surface as their own article group rather than colliding
    orphans = _surface_orphan_box_articles("TR-1", articles, boxes)
    assert [o["item_description"] for o in orphans] == ["ghost"], orphans
    assert orphans[0]["quantity_units"] == 2, orphans
    print("PASS test_backfill_orphan_boxes_get_lines_past_the_real_articles")


def test_backfill_treats_stored_zero_as_unset():
    # The UI normalises a missing box line to 0 and posts it back; upsert_box /
    # approve_inward only skip the column on `is None`, so 0 lands in the row and
    # permanently un-matches the box from its 1-based article. Real lines start at 1.
    articles = [{"item_description": "A", "line_number": 1},
                {"item_description": "B", "line_number": 2}]
    boxes = [{"article_description": "A", "box_number": 1, "line_number": 0},
             {"article_description": "B", "box_number": 1, "line_number": 0}]
    _backfill_read_line_numbers(articles, boxes)
    assert [b["line_number"] for b in boxes] == [1, 2], boxes
    print("PASS test_backfill_treats_stored_zero_as_unset")


def test_backfill_zero_article_line_is_reassigned():
    articles = [{"item_description": "A", "line_number": 0}]
    boxes = [{"article_description": "A", "box_number": 1, "line_number": 0}]
    _backfill_read_line_numbers(articles, boxes)
    assert articles[0]["line_number"] == 1, articles
    assert boxes[0]["line_number"] == 1, boxes
    print("PASS test_backfill_zero_article_line_is_reassigned")


def test_backfill_zero_boxes_still_surface_as_orphans_when_unmatched():
    # 0-line boxes whose description matches no article must not collapse onto line 1.
    articles = [{"item_description": "A"}, {"item_description": "B"}]
    boxes = [{"article_description": "ghost", "box_number": 1, "line_number": 0}]
    _backfill_read_line_numbers(articles, boxes)
    assert boxes[0]["line_number"] == 3, boxes
    orphans = _surface_orphan_box_articles("TR-1", articles, boxes)
    assert [o["item_description"] for o in orphans] == ["ghost"], orphans
    print("PASS test_backfill_zero_boxes_still_surface_as_orphans_when_unmatched")


def test_backfill_returns_desc_to_line_map_for_sum_rekeying():
    articles = [{"item_description": "A"}, {"item_description": "B"}]
    boxes = [{"article_description": "ghost", "box_number": 1}]
    by_desc = _backfill_read_line_numbers(articles, boxes)
    assert by_desc == {"A": 1, "B": 2, "ghost": 3}, by_desc
    print("PASS test_backfill_returns_desc_to_line_map_for_sum_rekeying")


ALL = [
    test_assign_line_numbers_same_name,
    test_assign_line_numbers_preserves_supplied,
    test_resolve_box_line_numbers_uses_supplied,
    test_backfill_bulk_entry_boxes_have_no_line_column,
    test_backfill_null_line_numbers_on_v2,
    test_backfill_matches_boxes_to_their_article_by_description,
    test_backfill_preserves_supplied_lines,
    test_backfill_blank_box_description_falls_back_to_sole_article,
    test_backfill_orphan_boxes_get_lines_past_the_real_articles,
    test_backfill_treats_stored_zero_as_unset,
    test_backfill_zero_article_line_is_reassigned,
    test_backfill_zero_boxes_still_surface_as_orphans_when_unmatched,
    test_backfill_returns_desc_to_line_map_for_sum_rekeying,
    test_is_v2_tables,
    test_create_inward_writes_both_same_name_articles,
    test_create_inward_boxes_keyed_by_line_number,
    test_surface_orphan_by_line_number,
    test_surface_orphan_by_description_pre_migration,
    test_surface_no_orphans_returns_empty,
    test_surface_all_when_no_article_rows,
]

if __name__ == "__main__":
    for t in ALL:
        t()
    print(f"\nALL {len(ALL)} TESTS PASSED")
