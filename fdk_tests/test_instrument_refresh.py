"""Daily refresh of Shoonya tokens and names from the exchange master file.

Shoonya's NSE_symbols.txt has no ISIN, so stored instruments are matched on token.
Only a series move (ABC-EQ -> ABC-BE) is renamed automatically; anything else is
reported for the operator to decide. A file that looks wrong changes nothing.
"""
import io
import zipfile
from datetime import date

import pytest

from fdk_fakes import FakeConn, FakeCursor
from services.fdk_sector_service import instrument_refresh as ir

HEADER = "Exchange,Token,LotSize,Symbol,TradingSymbol,Instrument,TickSize,"


def _master_text(rows):
    lines = [HEADER] + [f"NSE,{token},1,{sym},{tsym},{inst},0.05," for token, sym, tsym, inst in rows]
    return "\n".join(lines) + "\n"


def _master(rows):
    return ir.parse_master(_master_text(rows))


def _stored(*rows):
    return [{"isin": isin, "shoonya_symbol": sym, "shoonya_token": token, "series": sym.rpartition("-")[2]}
            for isin, sym, token in rows]


# ----------------------------------------------------------------- parsing
def test_parse_master_maps_token_to_trading_symbol_and_series():
    master = _master([("11536", "TCS", "TCS-EQ", "EQ"), ("2031", "M&M", "M&M-EQ", "EQ"),
                      ("26000", "Nifty 50", "NIFTY INDEX", "INDEX")])
    assert master["11536"] == {"symbol": "TCS", "trading_symbol": "TCS-EQ", "series": "EQ"}
    assert master["2031"]["trading_symbol"] == "M&M-EQ"
    assert master["26000"]["series"] == "INDEX"


def test_parse_master_rejects_a_file_with_the_wrong_columns():
    with pytest.raises(ValueError, match="columns"):
        ir.parse_master("Token,Name\n1,TCS\n")


def test_parse_master_skips_short_rows():
    master = ir.parse_master(HEADER + "\nNSE,11536,1,TCS,TCS-EQ,EQ,0.05,\nNSE,2031\n\n")
    assert list(master) == ["11536"]


def test_download_master_unzips_and_parses(monkeypatch):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("NSE_symbols.txt", _master_text([("11536", "TCS", "TCS-EQ", "EQ")]))
    calls = []

    class Resp:
        status_code = 200
        content = buf.getvalue()

        def raise_for_status(self):
            pass

    def _get(url, timeout=None, **kw):
        calls.append((url, timeout))
        return Resp()

    monkeypatch.setattr(ir.requests, "get", _get)
    master = ir.download_master()
    assert master["11536"]["trading_symbol"] == "TCS-EQ"
    assert calls[0][0] == "https://api.shoonya.com/NSE_symbols.txt.zip" and calls[0][1]


# ----------------------------------------------------------------- planning
def _plan(stored, rows, taken=(), **kw):
    kw.setdefault("min_rows", 1)
    return ir.plan_refresh(stored, _master(rows), taken_symbols=set(taken), **kw)


def test_unchanged_instruments_are_verified_and_nothing_else():
    plan = _plan(_stored(("INE1", "TCS-EQ", "11536")), [("11536", "TCS", "TCS-EQ", "EQ")])
    assert plan.verified == ["INE1"]
    assert plan.renames == [] and plan.decisions == [] and plan.missing == []
    assert plan.aborted is None and plan.changed is False


def test_a_series_move_is_renamed_automatically():
    plan = _plan(_stored(("INE1", "ABC-EQ", "501")), [("501", "ABC", "ABC-BE", "BE")])
    assert plan.renames == [("INE1", "ABC-EQ", "ABC-BE", "BE")]
    assert plan.verified == ["INE1"]
    assert plan.changed is True


def test_a_name_change_is_reported_not_applied():
    plan = _plan(_stored(("INE1", "OLDCO-EQ", "501")), [("501", "NEWCO", "NEWCO-EQ", "EQ")])
    assert plan.renames == []
    assert plan.decisions == [("INE1", "OLDCO-EQ", "NEWCO-EQ", "name changed")]


def test_a_series_move_onto_a_symbol_already_in_use_is_reported_not_applied():
    plan = _plan(_stored(("INE1", "ABC-EQ", "501")), [("501", "ABC", "ABC-BE", "BE")],
                 taken=["ABC-BE"])
    assert plan.renames == []
    assert plan.decisions == [("INE1", "ABC-EQ", "ABC-BE", "name clash")]


def test_a_token_missing_from_the_file_is_reported():
    stored = _stored(*[(f"INE{n}", f"S{n}-EQ", str(n)) for n in range(99)], ("INE2", "GONE-EQ", "999"))
    rows = [(str(n), f"S{n}", f"S{n}-EQ", "EQ") for n in range(99)]          # 1 of 100 missing
    plan = _plan(stored, rows)
    assert plan.missing == [("INE2", "GONE-EQ", "999")]
    assert len(plan.verified) == 99
    assert plan.aborted is None


def test_too_many_missing_tokens_aborts_the_refresh():
    stored = _stored(*[(f"INE{n}", f"S{n}-EQ", str(n)) for n in range(20)])
    rows = [(str(n), f"S{n}", f"S{n}-EQ", "EQ") for n in range(18)]        # 2 of 20 missing = 10%
    plan = _plan(stored, rows)
    assert plan.aborted and "missing" in plan.aborted
    assert plan.renames == [] and plan.verified == []


def test_a_file_with_too_few_rows_aborts_the_refresh():
    plan = ir.plan_refresh(_stored(("INE1", "TCS-EQ", "11536")),
                           _master([("11536", "TCS", "TCS-EQ", "EQ")]), taken_symbols=set())
    assert plan.aborted and "rows" in plan.aborted


# ----------------------------------------------------------------- applying
class _Cursor(FakeCursor):
    stored_rows = []
    taken_rows = []

    def fetchall(self):
        sql = self.last_sql.lower()
        if "from instruments" in sql:
            return [tuple(r.values()) for r in _Cursor.stored_rows]
        return [(s,) for s in _Cursor.taken_rows]


class _Conn(FakeConn):
    def __init__(self):
        self.cur = _Cursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_apply_renames_the_three_tables_and_stamps_verified_on_in_one_transaction():
    conn = _Conn()
    plan = ir.RefreshPlan(verified=["INE1", "INE2"], renames=[("INE2", "ABC-EQ", "ABC-BE", "BE")])

    ir.apply_plan(conn, plan, date(2026, 11, 3))

    sql = [" ".join(s.split()) for s, _ in conn.cur.executed]
    params = [p for _, p in conn.cur.executed]
    assert any("UPDATE instruments SET verified_on" in s for s in sql)
    assert any("UPDATE instruments SET shoonya_symbol" in s for s in sql)
    assert any("UPDATE stock_symbols SET symbol" in s for s in sql)
    assert any("UPDATE sector_mappings SET symbol" in s for s in sql)
    assert ("ABC-BE", "ABC-EQ") in params                    # new name replaces the old one
    assert conn.commits == 1 and conn.rollbacks == 0


def test_apply_writes_nothing_for_an_aborted_plan():
    conn = _Conn()
    ir.apply_plan(conn, ir.RefreshPlan(aborted="file looks wrong"), date(2026, 11, 3))
    assert conn.cur.executed == [] and conn.commits == 0


def test_apply_rolls_back_when_a_statement_fails():
    conn = _Conn()

    def boom(sql, params=None):
        raise RuntimeError("db down")

    conn.cur.execute = boom
    with pytest.raises(RuntimeError):
        ir.apply_plan(conn, ir.RefreshPlan(verified=["INE1"]), date(2026, 11, 3))
    assert conn.rollbacks == 1 and conn.commits == 0


# ----------------------------------------------------------------- end to end
def test_refresher_loads_stored_rows_plans_and_applies(monkeypatch):
    _Cursor.stored_rows = [{"isin": "INE1", "shoonya_symbol": "ABC-EQ", "shoonya_token": "501", "series": "EQ"}]
    _Cursor.taken_rows = ["ABC-EQ", "TCS-EQ"]
    conns = []

    def connect():
        conns.append(_Conn())
        return conns[-1]

    refresher = ir.InstrumentRefresher(
        connect, download=lambda: _master([("501", "ABC", "ABC-BE", "BE")]),
        min_rows=1, today=lambda: date(2026, 11, 3))

    plan = refresher.run()

    assert plan.renames == [("INE1", "ABC-EQ", "ABC-BE", "BE")]
    assert any(c.commits == 1 for c in conns)


def test_refresh_message_lists_renames_decisions_and_missing():
    from services.fdk_telegram_service.message_template import instrument_refresh_message
    plan = ir.RefreshPlan(verified=["INE1", "INE2", "INE3"],
                          renames=[("INE1", "ABC-EQ", "ABC-BE", "BE")],
                          decisions=[("INE2", "OLDCO-EQ", "NEWCO-EQ", "name changed")],
                          missing=[("INE3", "GONE-EQ", "999")])
    msg = instrument_refresh_message(plan)
    assert "ABC-EQ" in msg and "ABC-BE" in msg
    assert "OLDCO-EQ" in msg and "NEWCO-EQ" in msg
    assert "GONE-EQ" in msg
    assert msg.count("&") == msg.count("&amp;")


def test_refresh_message_for_an_aborted_refresh_says_why():
    from services.fdk_telegram_service.message_template import instrument_refresh_message
    msg = instrument_refresh_message(ir.RefreshPlan(aborted="12 of 100 tokens missing (over 5%)"))
    assert "12 of 100" in msg and "nothing was changed" in msg.lower()
