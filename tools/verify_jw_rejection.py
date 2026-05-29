"""Standalone verification for Task 4 logic — runs in-memory, no DB.
Mirrors the server-side aggregation/caps logic in `submit_material_in()` so we can
sanity-check it without standing up a request.

Run with: python backend/tools/verify_jw_rejection.py
"""


def recompute_counts(boxes):
    fg, rej = {}, {}
    for b in boxes:
        d = (b.get("item_description") or "").strip()
        if not d:
            continue
        bt = (b.get("box_type") or "FG").upper()
        if bt == "REJECTION":
            rej[d] = rej.get(d, 0) + 1
        else:
            fg[d] = fg.get(d, 0) + 1
    return fg, rej


def kg_caps_check(items, boxes):
    sent_fg = {(it.get("description") or "").strip(): float(it.get("finished_goods_kgs") or 0) for it in items}
    sent_rej = {(it.get("description") or "").strip(): float(it.get("rejection_kgs") or 0) for it in items}
    box_fg, box_rej = {}, {}
    for b in boxes:
        d = (b.get("item_description") or "").strip()
        w = float(b.get("net_weight") or 0)
        if (b.get("box_type") or "FG").upper() == "REJECTION":
            box_rej[d] = box_rej.get(d, 0.0) + w
        else:
            box_fg[d] = box_fg.get(d, 0.0) + w
    errors = []
    for d, w in box_fg.items():
        if w > sent_fg.get(d, 0.0) + 0.01:
            errors.append(("FG", d, w, sent_fg.get(d, 0.0)))
    for d, w in box_rej.items():
        if w > sent_rej.get(d, 0.0) + 0.01:
            errors.append(("REJECTION", d, w, sent_rej.get(d, 0.0)))
    return errors


def main():
    items = [{"description": "Wet Dates Khalas", "finished_goods_kgs": 10.0, "rejection_kgs": 2.0}]
    boxes = [
        {"item_description": "Wet Dates Khalas", "net_weight": 5.0, "box_type": "FG"},
        {"item_description": "Wet Dates Khalas", "net_weight": 5.0, "box_type": "FG"},
        {"item_description": "Wet Dates Khalas", "net_weight": 2.0, "box_type": "REJECTION"},
    ]
    fg, rej = recompute_counts(boxes)
    assert fg == {"Wet Dates Khalas": 2}, fg
    assert rej == {"Wet Dates Khalas": 1}, rej
    assert kg_caps_check(items, boxes) == [], "case 1 should pass caps"

    boxes2 = boxes + [{"item_description": "Wet Dates Khalas", "net_weight": 0.5, "box_type": "FG"}]
    errs = kg_caps_check(items, boxes2)
    assert errs and errs[0][0] == "FG", errs

    boxes3 = boxes + [{"item_description": "Wet Dates Khalas", "net_weight": 0.5, "box_type": "REJECTION"}]
    errs = kg_caps_check(items, boxes3)
    assert errs and errs[0][0] == "REJECTION", errs

    boxes4 = [{"item_description": "X", "net_weight": 1.0}]
    fg4, rej4 = recompute_counts(boxes4)
    assert fg4 == {"X": 1} and rej4 == {}, (fg4, rej4)

    items5 = [{"description": "Mixed Item", "finished_goods_kgs": 10.0, "rejection_kgs": 2.0}]
    boxes5 = [
        {"item_description": "Mixed Item", "net_weight": 10.0, "box_type": "FG"},
        {"item_description": "Mixed Item", "net_weight": 2.0, "box_type": "REJECTION"},
    ]
    assert kg_caps_check(items5, boxes5) == [], "exact-match case should pass"

    items6 = [{"description": "Edge", "finished_goods_kgs": 1.0, "rejection_kgs": 0.0}]
    boxes6 = [{"item_description": "Edge", "net_weight": 1.005, "box_type": "FG"}]
    assert kg_caps_check(items6, boxes6) == [], "1.005kg vs 1.0kg cap should pass within 0.01 tolerance"

    items7 = [{"description": "Edge", "finished_goods_kgs": 1.0, "rejection_kgs": 0.0}]
    boxes7 = [{"item_description": "Edge", "net_weight": 1.05, "box_type": "FG"}]
    assert kg_caps_check(items7, boxes7), "1.05kg vs 1.0kg cap should fail (over tolerance)"

    print("OK - all verification cases passed.")


if __name__ == "__main__":
    main()
