"""Every row /cold-storage/stocks/search returns must be pickable on its own.

Two cold piles of the same lot (different inward date / unit / site / item mark) used
to share one pick key (item, lot, inward_no): the second pile re-picked the FIRST
pile's boxes, so the operator got "already added" / duplicate-box_id and the challan
would not save. And a pile with a NULL lot_no was unpickable at all.

Read-only against the live DB. Run: python test_cold_pile_selector.py
"""
from fastapi.testclient import TestClient

import main

# No `with` — that would run lifespan (startup migrations) against the real DB.
client = TestClient(main.app)


def _search(**kw):
    r = client.get("/cold-storage/stocks/search", params={"limit": 500, **kw})
    assert r.status_code == 200, r.text
    return r.json()["results"]


def _pick(pile, qty, **override):
    params = {"company": pile["company"], "item_description": pile["item_description"],
              "lot_no": pile["lot_no"], "inward_no": pile["inward_no"],
              "pile_key": pile["pile_key"], "qty": qty, **override}
    r = client.get("/cold-storage/stocks/pick-boxes", params=params)
    assert r.status_code == 200, r.text
    return r.json()["boxes"]


def test_every_search_row_has_a_unique_pile_key():
    rows = _search(q="dates")
    keys = [r["pile_key"] for r in rows]
    assert all(keys), "a search row came back without a pile_key"
    assert len(set(keys)) == len(keys), "two search rows share a pile_key"


def test_same_lot_different_pile_picks_different_boxes():
    """The bug, reproduced as a test: find any lot that spans >1 pile and prove the
    piles no longer hand out the same boxes."""
    by_lot = {}
    for r in _search(q=""):  # q='' -> no filter -> broad sample
        by_lot.setdefault((r["company"], r["item_description"], r["lot_no"]), []).append(r)
    multi = [v for v in by_lot.values() if len(v) > 1]
    if not multi:
        print("  (no multi-pile lot in the sample — key uniqueness covered above)")
        return
    for piles in multi:
        picked = []
        for p in piles:
            boxes = _pick(p, 5)
            assert boxes, f"pile {p['pile_key']} picked 0 boxes (unselectable)"
            picked.append({b["id"] for b in boxes})
        for i in range(len(picked)):
            for j in range(i + 1, len(picked)):
                assert not (picked[i] & picked[j]), (
                    f"piles of lot {piles[0]['lot_no']} returned overlapping rows "
                    f"{picked[i] & picked[j]}")
        print(f"  lot {piles[0]['lot_no']!r}: {len(piles)} piles, disjoint box sets OK")


def test_lotless_pile_is_pickable():
    """Piles recovered from disposition / arrived via transfer-in carry no lot_no.
    `CAST(lot_no AS TEXT) = ''` is NULL in SQL, so these used to pick nothing."""
    lotless = [r for r in _search(q="") if not r["lot_no"]]
    if not lotless:
        print("  (no lot-less pile in the sample)")
        return
    for p in lotless[:3]:
        boxes = _pick(p, 3, lot_no="")
        assert boxes, f"lot-less pile {p['item_description']!r} picked 0 boxes"
        print(f"  lot-less {p['item_description'][:34]!r}: picked {len(boxes)} boxes OK")


def test_null_txn_row_is_found_by_the_dispatch_lookup():
    """A pile re-inserted with transaction_no = NULL must still be found when the
    dispatch passes the '' the form sends — otherwise park_in_pending skips the
    cold_stocks deduction and the box is counted twice (cold + in transit)."""
    from sqlalchemy import text

    from services.ims_service.pending_stock_tools import _find_in_cold_stocks
    from shared.database import SessionLocal

    db = SessionLocal()
    try:
        row = db.execute(text("""
            SELECT box_id, CAST(lot_no AS TEXT) AS lot FROM cfpl_cold_stocks
            WHERE transaction_no IS NULL AND box_id IS NOT NULL LIMIT 1
        """)).fetchone()
        if row is None:
            print("  (no NULL-transaction_no cold row left — nothing to check)")
            return
        for tno in ("", None):
            tbl, found = _find_in_cold_stocks(db, row.box_id, tno, lot_no=row.lot)
            assert found is not None, f"box {row.box_id} with NULL txn not found for tno={tno!r}"
        print(f"  NULL-txn box {row.box_id} resolves for both '' and None OK")
    finally:
        db.close()


if __name__ == "__main__":
    test_every_search_row_has_a_unique_pile_key()
    test_same_lot_different_pile_picks_different_boxes()
    test_lotless_pile_is_pickable()
    test_null_txn_row_is_found_by_the_dispatch_lookup()
    print("OK")
