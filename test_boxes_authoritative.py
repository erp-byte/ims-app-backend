"""
Dependency-free unit test for _boxes_authoritative / _uncovered_lines.

Regression guard for the doubled "View DC" on a scanned Direct Out: the form sends
one line per article-list row (scanned AND manually typed), so the same physical
stock arrives twice. The scanned boxes must win — qty = box count, net = summed box
weights, duplicate lines dropped — on create AND on edit. No database required.
"""
from types import SimpleNamespace

from services.ims_service.interunit_tools import _boxes_authoritative, _uncovered_lines


def line(_id, desc, qty, net, gross=None, ups=0, lot=""):
    return SimpleNamespace(
        id=_id, header_id=1, item_desc_raw=desc, qty=qty, net_weight=net,
        total_weight=gross if gross is not None else net, unit_pack_size=ups,
        rm_pm_fg_type="PM", item_category="PACKAGING", sub_category="", pack_size=0,
        uom="", batch_number="", lot_number=lot, vakkal="", created_at=None, updated_at=None,
    )


def box(article, net, gross, lot=""):
    return SimpleNamespace(article=article, net_weight=net, gross_weight=gross,
                           lot_number=lot, box_id="B", transaction_no="TR-1")


class FakeDB:
    """Applies the helper's UPDATE/DELETE to an in-memory line list so the closing
    SELECT returns what a real DB would."""

    def __init__(self, lines, box_line_ids=None):
        self.lines = {l.id: l for l in lines}
        self.box_line_ids = box_line_ids or []
        self.sql = []

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())
        params = params or {}
        self.sql.append((sql, params))
        if sql.startswith("UPDATE interunit_transfer_boxes SET transfer_line_id"):
            self.box_line_ids = [params["lid"]] * len(self.box_line_ids)
        elif sql.startswith("UPDATE interunit_transfers_lines SET qty"):
            l = self.lines[params["id"]]
            l.qty, l.net_weight, l.total_weight = params["q"], params["net"], params["gross"]
            l.unit_pack_size = params["ups"]
            l.uom = l.uom or "BOX"
        elif sql.startswith("DELETE FROM interunit_transfers_lines"):
            for i in params["ids"]:
                self.lines.pop(i, None)
        elif "SELECT DISTINCT transfer_line_id" in sql:
            return [(i,) for i in sorted(set(self.box_line_ids))]
        elif sql.startswith("SELECT id, header_id"):
            return SimpleNamespace(fetchall=lambda: [self.lines[k] for k in sorted(self.lines)])
        return SimpleNamespace(fetchall=lambda: [], fetchone=lambda: None, scalar=lambda: None)


def run():
    ART = "PM24-D MART HEALTHY CHOICE ROASTED PUMPKIN SEEDS 100G"

    # 1. TRANS202607271640: 9 scanned boxes + 9 manually typed entries, same article.
    scanned = [box(ART.title(), 19.84 + i / 100, 20.56 + i / 100) for i in range(9)]
    lines = ([line(100 + i, ART, 1, 19.84 + i / 100, ups=5000) for i in range(9)]
             + [line(200 + i, ART, 1, 19.79, ups=5000) for i in range(9)])
    db = FakeDB(lines, box_line_ids=[217] * 9)   # boxes hung off the LAST line
    out = _boxes_authoritative(db, 1, "A68", scanned, lines)

    assert len(out) == 1, f"expected the 18 lines to collapse to 1, got {len(out)}"
    kept = out[0]
    assert kept.id == 100, f"the first line survives, got {kept.id}"
    assert kept.qty == 9, f"DC qty must be the 9 physical boxes, got {kept.qty}"
    expect_net = round(sum(b.net_weight for b in scanned), 3)
    assert abs(float(kept.net_weight) - expect_net) < 1e-9, \
        f"DC net must be the box sum {expect_net}, got {kept.net_weight}"
    assert float(kept.net_weight) < 200, "net is doubled — the manual lines leaked in"
    assert kept.unit_pack_size == 5000 and kept.uom == "BOX"
    # boxes were reattached to the surviving line → it counts as covered, so its stock
    # is NOT parked a second time as a box-less line.
    assert _uncovered_lines(db, 1, out) == []

    # 2. PM boxes carry weight in gross only (net 0) → never 0.000 kg on the DC.
    pm_lines = [line(300, "PM24-21 GM DATE BITE BLISTER", 1, 3.06, 3.60)]
    db2 = FakeDB(pm_lines, box_line_ids=[300, 300])
    out2 = _boxes_authoritative(
        db2, 2, "A68",
        [box("PM24-21 GM DATE BITE BLISTER", 0, 3.60),
         box("PM24-21 GM DATE BITE BLISTER", 0, 3.16)],
        pm_lines,
    )
    assert out2[0].qty == 2 and abs(float(out2[0].net_weight) - 6.76) < 1e-9, \
        f"gross fallback expected 6.76, got {out2[0].net_weight}"

    # 3. Cold source / no boxes → untouched (lines are the order there).
    cold = [line(400, "WET DATES", 12, 150.0)]
    db3 = FakeDB(cold)
    assert _boxes_authoritative(db3, 3, "Cold Storage", [box("WET DATES", 12.5, 12.5)], cold) is cold
    assert db3.sql == [], "cold source must not be rewritten"
    assert _boxes_authoritative(db3, 3, "A68", [], cold) is cold

    # 4. A manual-only article (no boxes) keeps its own line and IS parked box-less.
    mixed = [line(500, ART, 1, 19.84), line(600, "3 PLY CARTON", 49, 289.1)]
    db4 = FakeDB(mixed, box_line_ids=[500])
    out4 = _boxes_authoritative(db4, 4, "A68", [box(ART, 19.84, 20.56)], mixed)
    assert {l.id for l in out4} == {500, 600}
    assert [l.id for l in _uncovered_lines(db4, 4, out4)] == [600]

    print("ALL ASSERTIONS PASSED — scanned boxes win on create+edit, no doubling, "
          "gross fallback, cold untouched, manual lines preserved.")


if __name__ == "__main__":
    run()
