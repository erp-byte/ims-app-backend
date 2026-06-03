"""Dependency-free unit tests for Item 1B: compute-on-read overlay.

Safety net so the UI shows box-derived aggregates immediately, even before the one-time backfill
runs (and regardless of whether a non-UI consumer fixed the stored row). The DB grouping happens
in get_inward; this pure helper does the overlay: for every article that has boxes, overwrite its
quantity_units / net_weight / total_weight from the box sums. Articles with no boxes are left
untouched (don't zero out a legacy boxless article on read).

Note the column split: articles key on `item_description`; the box-sums map is keyed on the box
`article_description` value (the same string).

No database required:  python test_overlay_box_aggregates.py
"""
from services.ims_service.inward_tools import overlay_box_derived_aggregates


def test_overlay_overwrites_articles_that_have_boxes():
    # the stuck 1000/1720 case: stored 1000 but 1720 boxes exist
    articles = [{"item_description": "Onion 50kg", "quantity_units": 1000,
                 "net_weight": 11340, "total_weight": 11900}]
    box_sums = {"Onion 50kg": {"cnt": 1720, "net": 19500.5, "gross": 20460.0}}
    out = overlay_box_derived_aggregates(articles, box_sums)
    assert out[0]["quantity_units"] == 1720, out
    assert out[0]["net_weight"] == 19500.5, out
    assert out[0]["total_weight"] == 20460.0, out
    print("PASS test_overlay_overwrites_articles_that_have_boxes")


def test_overlay_leaves_boxless_articles_untouched():
    # an article with no box rows must keep its stored values (not get zeroed on read)
    articles = [{"item_description": "Loose grain", "quantity_units": 5,
                 "net_weight": 250, "total_weight": 260}]
    out = overlay_box_derived_aggregates(articles, {})
    assert out[0]["quantity_units"] == 5 and out[0]["net_weight"] == 250, out
    print("PASS test_overlay_leaves_boxless_articles_untouched")


def test_overlay_matches_item_description_to_box_article_description():
    articles = [{"item_description": "A"}, {"item_description": "B"}]
    box_sums = {"A": {"cnt": 3, "net": 30, "gross": 33}}   # only A has boxes
    out = overlay_box_derived_aggregates(articles, box_sums)
    assert out[0]["quantity_units"] == 3, out
    assert "quantity_units" not in out[1], "B had no boxes -> untouched"
    print("PASS test_overlay_matches_item_description_to_box_article_description")


ALL = [
    test_overlay_overwrites_articles_that_have_boxes,
    test_overlay_leaves_boxless_articles_untouched,
    test_overlay_matches_item_description_to_box_article_description,
]

if __name__ == "__main__":
    for t in ALL:
        t()
    print(f"\nALL {len(ALL)} TESTS PASSED")
