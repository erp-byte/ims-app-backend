"""Orchestrator broker wiring: which authenticator it builds and what it does after login.

Offline: psycopg2 and requests are faked, the Shoonya HTTP login is patched.
"""
import asyncio
import logging
import pathlib
import time
from types import SimpleNamespace

import psycopg2
import pytest
import requests

from fdk_fakes import FakeConn, FakeCursor, FakeResponse
from shared.fdk_config_loader import load_config
from test_fdk_config_loader import ALL_KEYS, COMMON, SHOONYA


@pytest.fixture
def offline(monkeypatch):
    for key in ALL_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in {**COMMON, **SHOONYA}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("FDK_SCHEDULING_ENABLED", "false")   # run() must not wait for 09:13 IST
    monkeypatch.setattr(psycopg2, "connect", lambda *a, **k: FakeConn())
    monkeypatch.setattr(requests, "post", lambda *a, **k: FakeResponse())
    return monkeypatch


def _orchestrator():
    from services.fdk_supervisor_service.ochestrator import Orchestrator
    return Orchestrator(load_config())          # no AuthState: the Fyers webhook login is gone


def test_builds_the_shoonya_authenticator_and_its_token_table(offline):
    from services.fdk_auth_service.shoonya_authenticator import ShoonyaAuthenticator

    orch = _orchestrator()

    assert orch.broker == "shoonya"
    assert isinstance(orch.authenticator, ShoonyaAuthenticator)
    assert orch.token_manager.table == "shoonya_tokens"


def test_login_state_tells_waiting_from_failing_from_ok(offline):
    from services.fdk_supervisor_service import ochestrator as orch_mod

    orch = _orchestrator()
    auth = orch.authenticator

    orch.config.scheduling_enabled = True
    offline.setattr(orch_mod, "is_market_hours", lambda: False)
    assert orch.login_state() == "waiting_for_market_hours"

    offline.setattr(orch_mod, "is_market_hours", lambda: True)
    assert orch.login_state() == "in_progress"

    auth.last_error = "Invalid Input : Wrong Password"
    assert orch.login_state() == "failed"

    auth.awaiting_manual_login = True              # link sent, waiting for the pasted URL
    assert orch.login_state() == "awaiting_manual_login"
    auth.awaiting_manual_login = False

    auth.is_authenticated = True
    assert orch.login_state() == "ok"

    orch.on_hold = True
    assert orch.login_state() == "on_hold"


def test_login_state_reads_parked_after_a_failed_day(offline):
    from services.fdk_supervisor_service import ochestrator as orch_mod

    orch = _orchestrator()
    orch.config.scheduling_enabled = True
    orch.authenticator.last_error = "no authorization code within 120s"   # the day's browser failure
    offline.setattr(orch_mod, "is_market_hours", lambda: False)
    assert orch.login_state() == "waiting_for_market_hours"   # parked overnight, not "failed"
    offline.setattr(orch_mod, "is_market_hours", lambda: True)
    assert orch.login_state() == "failed"


def test_manual_login_wait_follows_market_hours(offline):
    from services.fdk_supervisor_service import ochestrator as orch_mod

    orch = _orchestrator()
    keep_waiting = orch.authenticator.keep_waiting

    orch.config.scheduling_enabled = False
    assert keep_waiting() is True

    orch.config.scheduling_enabled = True
    offline.setattr(orch_mod, "is_market_hours", lambda: False)
    assert keep_waiting() is False                   # market closed: stop resending the link
    offline.setattr(orch_mod, "is_market_hours", lambda: True)
    assert keep_waiting() is True


def test_run_retries_the_initial_supabase_loads_until_they_succeed(offline, caplog):
    from services.fdk_supervisor_service import ochestrator as orch_mod
    from services.fdk_auth_service import shoonya_authenticator as sa
    offline.setattr(sa.shoonya_login, "validate_session", lambda user, tok: (False, "No token"))
    offline.setattr(sa.shoonya_login, "auto_login_available", lambda: True)
    offline.setattr(sa.shoonya_login, "auto_login_code", lambda *a, **k: {"code": "C1"})
    offline.setattr(sa.shoonya_login, "exchange_code",
                    lambda *a, **k: {"access_token": "sess-1", "account_id": "FA12345"})
    offline.setattr(orch_mod, "SUPERVISOR_RETRY_DELAY", 0.01)
    offline.setattr(orch_mod, "TickDispatcher", FakeDispatcher)
    orch = _orchestrator()
    offline.setattr(orch.fyers, "build", lambda *a, **k: None)
    offline.setattr(orch.penny, "build", lambda *a, **k: None)
    offline.setattr(orch, "_connect_dispatcher", lambda: None)
    calls = {"n": 0}
    real_load = orch.symbol_manager.load_symbols

    def flaky(detector):
        calls["n"] += 1
        if calls["n"] == 1:
            raise psycopg2.OperationalError("supabase down")
        return real_load(detector)

    offline.setattr(orch.symbol_manager, "load_symbols", flaky)

    async def drive():
        task = asyncio.create_task(orch.run())
        for _ in range(100):
            if orch.authenticator.is_authenticated:
                break
            await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    with caplog.at_level(logging.ERROR, logger="fdk.orchestrator"):
        asyncio.run(drive())

    assert calls["n"] >= 2                       # failed once, then succeeded
    assert orch.authenticator.is_authenticated   # the supervisor survived the transient error
    assert "supabase down" in " ".join(r.getMessage() for r in caplog.records)


class FakeDispatcher:
    """Stands in for TickDispatcher: records how it was built, answers health checks."""

    made: list["FakeDispatcher"] = []
    rebuild_needed = False
    up = True
    rejection = None
    reports: list[dict] = []

    @classmethod
    def reset(cls):
        cls.made, cls.rebuild_needed, cls.up, cls.rejection, cls.reports = [], False, True, None, []

    def __init__(self, uid, actid, access_token, detectors, instrument_tokens):
        self.uid, self.actid, self.access_token = uid, actid, access_token
        self.detectors, self.instrument_tokens = detectors, instrument_tokens
        self.connected = self.closed = False
        self.rejection_reason = None
        self.no_token = []
        FakeDispatcher.made.append(self)

    def connect(self):
        self.connected = True
        if FakeDispatcher.rejection:
            self.rejection_reason = FakeDispatcher.rejection
            for det in self.detectors:
                det.token_expired = True

    def close(self):
        self.closed = True

    def needs_rebuild(self):
        return FakeDispatcher.rebuild_needed

    def is_up(self):
        return FakeDispatcher.up

    def monitored(self, detector):
        return len(detector.config.symbols)

    def take_coverage_report(self):
        return FakeDispatcher.reports.pop(0) if FakeDispatcher.reports else None

    def status(self):
        return {"connected": FakeDispatcher.up, "acknowledged": 3, "requested": 3}


def _detector(symbols):
    return SimpleNamespace(config=SimpleNamespace(symbols=symbols), token_expired=False,
                           access_token=None)


@pytest.fixture
def feed(offline):
    """An orchestrator past login, with symbol data and detectors faked."""
    from services.fdk_auth_service import shoonya_authenticator as sa
    from services.fdk_supervisor_service import ochestrator as orch_mod

    FakeDispatcher.reset()
    offline.setattr(orch_mod, "TickDispatcher", FakeDispatcher)
    offline.setattr(orch_mod, "SUPERVISOR_POLL", 0.005)
    offline.setattr(sa.shoonya_login, "validate_session", lambda user, tok: (False, "No token"))
    offline.setattr(sa.shoonya_login, "auto_login_available", lambda: True)
    offline.setattr(sa.shoonya_login, "auto_login_code", lambda *a, **k: {"code": "C1"})
    offline.setattr(sa.shoonya_login, "exchange_code",
                    lambda *a, **k: {"access_token": "sess-1", "account_id": "ACT-1"})
    orch = _orchestrator()
    orch.loaded = []
    orch.messages = []

    def load_symbols(detector):
        orch.loaded.append(detector)
        return {"shoonya": ["TCS-EQ", "INFY-EQ"], "penny": ["IDEA-EQ"]}.get(detector, [])

    def build(service):
        def _build(token, symbols, sectors):
            service._detector = _detector(symbols)
        return _build

    async def record_message(text, **kwargs):
        orch.messages.append(text)
        return True

    offline.setattr(orch.symbol_manager, "load_symbols", load_symbols)
    offline.setattr(orch.symbol_manager, "load_instrument_tokens",
                    lambda: {"TCS-EQ": "11536", "INFY-EQ": "1594", "IDEA-EQ": "14366"})
    offline.setattr(orch.symbol_manager, "load_sector_mapping", lambda: {"TCS-EQ": "IT"})
    offline.setattr(orch.fyers, "build", build(orch.fyers))
    offline.setattr(orch.penny, "build", build(orch.penny))
    offline.setattr(orch.login_sender, "send_async", record_message)
    return orch


def _drive(orch, *steps, timeout=5.0, settle=0.05):
    """Run the supervisor through (condition, action) steps, polling instead of sleeping."""
    async def drive():
        task = asyncio.create_task(orch.run())
        for condition, action in steps:
            deadline = time.monotonic() + timeout
            while not condition():
                assert time.monotonic() < deadline, f"never reached: {condition}"
                assert not task.done(), task.exception()
                await asyncio.sleep(0.002)
            if action:
                action()
        await asyncio.sleep(settle)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(drive())


def _connected():
    return bool(FakeDispatcher.made) and FakeDispatcher.made[-1].connected


def _sent(orch, text):
    return [m for m in orch.messages if text in m]


def _set(name, value):
    return lambda: setattr(FakeDispatcher, name, value)


def _after(seconds):
    deadline = []

    def elapsed():
        if not deadline:
            deadline.append(time.monotonic() + seconds)
        return time.monotonic() > deadline[0]

    return elapsed


def test_after_login_the_shoonya_feed_starts_with_user_id_and_tokens(feed):
    _drive(feed, (_connected, None))

    assert feed.authenticator.access_token == "sess-1"
    assert "shoonya" in feed.loaded and "penny" in feed.loaded and "fyers" not in feed.loaded
    dispatcher = FakeDispatcher.made[0]
    # The same frame on every login path, as the vendor SDK sends: actid is the user id.
    assert (dispatcher.uid, dispatcher.actid, dispatcher.access_token) == \
           ("FA12345", "FA12345", "sess-1")
    assert dispatcher.instrument_tokens == {"TCS-EQ": "11536", "INFY-EQ": "1594", "IDEA-EQ": "14366"}
    assert [d.config.symbols for d in dispatcher.detectors] == [["TCS-EQ", "INFY-EQ"], ["IDEA-EQ"]]


def test_monitoring_started_names_shoonya_and_the_counts(feed):
    _drive(feed, (lambda: _sent(feed, "Monitoring Started"), None))
    started = _sent(feed, "Monitoring Started")
    assert len(started) == 1
    assert "Shoonya: 2 symbols" in started[0] and "Penny: 1 symbols" in started[0]
    assert "Fyers" not in started[0]


def test_a_feed_that_is_not_up_says_so_once_and_announces_when_it_recovers(feed):
    FakeDispatcher.up = False
    _drive(feed,
           (lambda: _sent(feed, "feed is not up"), None),
           (_after(0.05), _set("up", True)),         # several polls, still one notice
           (lambda: _sent(feed, "Monitoring Started"), None))
    assert len(_sent(feed, "feed is not up")) == 1
    assert len(_sent(feed, "Monitoring Started")) == 1
    assert len(FakeDispatcher.made) == 1, "the socket recovered by itself, no rebuild needed"


def test_a_long_outage_alerts_once_not_on_every_rebuild(feed, monkeypatch):
    from services.fdk_supervisor_service import ochestrator as orch_mod
    monkeypatch.setattr(orch_mod, "should_rebuild_socket", lambda needs, last, now: needs)

    def go_down():
        FakeDispatcher.up = False
        FakeDispatcher.rebuild_needed = True

    def come_back():
        FakeDispatcher.up = True
        FakeDispatcher.rebuild_needed = False

    _drive(feed,
           (lambda: _sent(feed, "Monitoring Started"), go_down),
           (lambda: len(FakeDispatcher.made) >= 6, come_back),
           (lambda: len(_sent(feed, "Monitoring Started")) == 2, None))
    assert len(_sent(feed, "WebSocket Rebuild")) == 1
    assert _sent(feed, "feed is not up") == []
    assert all(d.closed for d in FakeDispatcher.made[:-1])


def _report(missing, acknowledged=2, requested=3):
    return {"acknowledged": acknowledged, "requested": requested,
            "missing": missing, "missing_by_detector": [1, 0]}


def test_symbols_the_gateway_did_not_acknowledge_are_named_with_their_detector(feed):
    FakeDispatcher.reports = [_report(["M&M-EQ"])]
    _drive(feed, (lambda: _sent(feed, "Feed coverage"), None))
    warning = _sent(feed, "Feed coverage")[0]
    assert "2 of 3" in warning
    assert "Shoonya 1" in warning and "Penny 0" in warning
    assert "M&amp;M-EQ" in warning, "names must be escaped for Telegram's HTML mode"


def test_the_same_gap_is_reported_once_and_a_new_one_again(feed):
    FakeDispatcher.reports = [_report(["TCS-EQ"]), _report(["TCS-EQ"]), _report(["TCS-EQ", "INFY-EQ"], 1)]
    _drive(feed, (lambda: not FakeDispatcher.reports, None))
    assert len(_sent(feed, "Feed coverage")) == 2


def test_full_coverage_sends_no_warning(feed):
    _drive(feed, (lambda: _sent(feed, "Monitoring Started"), None), (_after(0.03), None))
    assert _connected() and _sent(feed, "Feed coverage") == []


def test_symbols_without_a_token_are_not_counted_as_monitored(feed, monkeypatch):
    monkeypatch.setattr(FakeDispatcher, "monitored", lambda self, det: len(det.config.symbols) - 1)
    _drive(feed, (lambda: _sent(feed, "Monitoring Started"), None))
    assert "Shoonya: 1 symbols" in _sent(feed, "Monitoring Started")[0]


# --- a token the socket refuses --------------------------------------

@pytest.fixture
def stored_session(feed, offline):
    """A token store with a session REST still accepts, as after a morning login."""
    from services.fdk_auth_service import shoonya_authenticator as sa
    store = {"token": "CACHED", "cleared": 0}

    def clear():
        store["token"] = None
        store["cleared"] += 1

    offline.setattr(sa.shoonya_login, "validate_session", lambda user, tok: (True, "Valid"))
    offline.setattr(feed.token_manager, "is_token_valid_by_time", lambda: (store["token"] is not None, ""))
    offline.setattr(feed.token_manager, "load_token", lambda: (store["token"], 0.0, "", None))
    offline.setattr(feed.token_manager, "clear_tokens", clear)
    offline.setattr(feed.token_manager, "save_token",
                    lambda token, **k: store.update(token=token))
    return store


def test_a_socket_rejection_discards_the_session_and_logs_in_fresh(feed, stored_session):
    FakeDispatcher.rejection = "Invalid session"

    def recover():
        FakeDispatcher.rejection = None

    _drive(feed,
           (lambda: _sent(feed, "rejected"), recover),
           (lambda: len(FakeDispatcher.made) == 2 and _connected(), None))

    assert [d.access_token for d in FakeDispatcher.made] == ["CACHED", "sess-1"]
    assert stored_session["cleared"] == 1
    assert "Invalid session" in _sent(feed, "rejected")[0]
    assert len(_sent(feed, "rejected")) == 1


def test_a_fresh_session_also_rejected_stops_retrying_and_says_why(feed, stored_session):
    FakeDispatcher.rejection = "Invalid session"
    _drive(feed, (lambda: len(_sent(feed, "rejected")) == 2, None), settle=0.1)

    assert len(FakeDispatcher.made) == 2, "two tries, then it waits for /rst"
    assert all(d.closed for d in FakeDispatcher.made)
    assert len(_sent(feed, "Authentication Successful")) == 2      # not one a minute all day
    assert "/rst" in _sent(feed, "rejected")[-1]
    assert feed.login_state() == "failed"
    assert "Invalid session" in feed.authenticator.last_error


def test_rst_gets_a_parked_feed_going_again(feed, stored_session):
    FakeDispatcher.rejection = "Invalid session"

    def operator_fixes_it_and_sends_rst():
        FakeDispatcher.rejection = None
        feed.request_restart()

    _drive(feed,
           (lambda: len(_sent(feed, "rejected")) == 2, operator_fixes_it_and_sends_rst),
           (lambda: _sent(feed, "Monitoring Started"), None))
    assert len(FakeDispatcher.made) == 3


def test_a_healthy_feed_earns_a_fresh_rejection_budget(feed, stored_session):
    def reject():
        FakeDispatcher.rejection = "Invalid session"
        FakeDispatcher.made[-1].rejection_reason = "Invalid session"
        for det in FakeDispatcher.made[-1].detectors:
            det.token_expired = True

    def recover():
        FakeDispatcher.rejection = None

    _drive(feed,
           (lambda: _sent(feed, "Monitoring Started"), reject),
           (lambda: len(_sent(feed, "rejected")) == 1, recover),
           (lambda: len(_sent(feed, "Monitoring Started")) == 2, reject),
           (lambda: len(_sent(feed, "rejected")) == 2, recover),
           (lambda: len(_sent(feed, "Monitoring Started")) == 3, None))
    assert not feed._feed_parked


def test_a_database_blip_during_a_rejection_does_not_use_up_the_retry(feed, stored_session, monkeypatch):
    """One failed write must not park the feed for the day without trying a fresh login."""
    from services.fdk_supervisor_service import ochestrator as orch_mod
    monkeypatch.setattr(orch_mod, "SUPERVISOR_ERROR_DELAY", 0.005)
    real_clear, calls = feed.token_manager.clear_tokens, []

    def flaky_clear():
        calls.append(1)
        if len(calls) == 1:
            raise psycopg2.OperationalError("server closed the connection unexpectedly")
        real_clear()

    monkeypatch.setattr(feed.token_manager, "clear_tokens", flaky_clear)
    FakeDispatcher.rejection = "Invalid session"

    def recover():
        FakeDispatcher.rejection = None

    _drive(feed,
           (lambda: _sent(feed, "rejected"), recover),
           (lambda: len(FakeDispatcher.made) == 2 and _connected(), None))

    assert not feed._feed_parked
    assert FakeDispatcher.made[1].access_token == "sess-1"
    assert len(_sent(feed, "rejected")) == 1


def _longest_stall(coroutine_factory):
    """Run a coroutine beside a heartbeat and return the longest the loop went unresponsive."""
    async def main():
        gaps = []
        done = asyncio.Event()

        async def heartbeat():
            last = time.monotonic()
            while not done.is_set():
                await asyncio.sleep(0.005)
                now = time.monotonic()
                gaps.append(now - last)
                last = now

        beat = asyncio.create_task(heartbeat())
        await asyncio.sleep(0.02)    # let it start, or a block at once goes unseen
        await coroutine_factory()
        done.set()
        await beat
        return max(gaps)
    return asyncio.run(main())


@pytest.mark.parametrize("path", ["re_authenticate", "daily_reset", "parking"])
def test_stopping_the_detectors_does_not_freeze_the_event_loop(feed, monkeypatch, path):
    """stop() can wait 10 s per detector on a Sheets call, and this loop is the backend's."""
    monkeypatch.setattr(feed.fyers, "stop", lambda: time.sleep(0.25))
    monkeypatch.setattr(feed.penny, "stop", lambda: time.sleep(0.25))

    async def no_login():
        return False

    monkeypatch.setattr(feed, "_authenticate_with_checks", no_login)
    feed._feed_rejections = 1
    feed._dispatcher = FakeDispatcher("u", "a", "t", [], {})
    run = {"re_authenticate": feed._re_authenticate,
           "daily_reset": lambda: feed._daily_token_reset("today"),
           "parking": feed._on_feed_rejected}[path]
    assert _longest_stall(run) < 0.2


def test_the_supervisor_loop_stops_detectors_off_the_event_loop():
    import inspect
    from services.fdk_supervisor_service.ochestrator import Orchestrator
    assert "self.fyers.stop()" not in inspect.getsource(Orchestrator.run)
    assert "self.penny.stop()" not in inspect.getsource(Orchestrator.run)


def test_hold_during_the_close_stops_the_relogin(feed, monkeypatch):
    logins = []
    monkeypatch.setattr(feed.authenticator, "authenticate", lambda: logins.append(1))

    async def close_while_hld_arrives():
        feed.on_hold = True

    monkeypatch.setattr(feed, "_close_dispatcher_async", close_while_hld_arrives)
    asyncio.run(feed._re_authenticate())
    assert logins == []


def test_market_close_closes_the_feed_even_if_the_detectors_are_not_running(feed, monkeypatch):
    from services.fdk_supervisor_service import ochestrator as orch_mod
    feed.config.scheduling_enabled = True
    monkeypatch.setattr(orch_mod, "is_market_hours", lambda: False)
    dispatcher = FakeDispatcher("u", "a", "t", [], {})
    feed._dispatcher = dispatcher
    _drive(feed, (lambda: dispatcher.closed, None))
    assert feed._dispatcher is None


def test_feed_status_with_and_without_a_feed(feed):
    assert feed.feed_status()["connected"] is False
    feed._dispatcher = FakeDispatcher("u", "a", "t", [], {})
    assert feed.feed_status()["connected"] is True
    assert feed.feed_status()["acknowledged"] == 3


def test_summaries_are_titled_with_the_database_label(feed):
    assert feed.fyers_summary.generator.name == "shoonya"


def test_the_large_cap_detector_config_is_labelled_shoonya(offline, monkeypatch):
    from services.fdk_fyers_service import service as fyers_service
    monkeypatch.setattr(fyers_service, "GoogleSheetsManager", lambda *a, **k: None)
    service = _orchestrator().fyers
    service.build("sess-1", ["TCS-EQ"], {})
    assert service.detector.config.name == "shoonya"


def test_token_manager_defaults_to_the_only_token_table(offline):
    from services.fdk_auth_service.token_manager import TokenManager
    assert TokenManager(load_config()).table == "shoonya_tokens"


def test_load_instrument_tokens_maps_active_symbols_through_isin(offline):
    from services.fdk_sector_service.symbol_manager import SymbolManager
    executed = []

    class Cursor(FakeCursor):
        def execute(self, sql, params=None):
            executed.append(" ".join(sql.split()))
            super().execute(sql, params)

        def fetchall(self):
            return [("TCS-EQ", "11536"), ("M&M-EQ", "2031")]

    class Conn(FakeConn):
        def cursor(self):
            return Cursor()

    offline.setattr(psycopg2, "connect", lambda *a, **k: Conn())
    tokens = SymbolManager("dsn").load_instrument_tokens()

    assert tokens == {"TCS-EQ": "11536", "M&M-EQ": "2031"}
    query = executed[-1]
    assert "shoonya_token" in query and "JOIN instruments USING (isin)" in query.replace(" i ", " ")
    assert "active = TRUE" in query


# --- MCP tools -------------------------------------------------------

class _FakeMCP:
    def __init__(self):
        self.tools = {}

    def tool(self):
        def register(fn):
            self.tools[fn.__name__] = fn
            return fn
        return register


@pytest.fixture
def mcp_tools():
    from services.fdk_mcp_service.fyers_tools import register_fyers_tools

    orchestrator = SimpleNamespace(
        symbol_manager=SimpleNamespace(load_symbols=lambda d: {"shoonya": ["TCS-EQ"]}.get(d, [])))
    mcp = _FakeMCP()
    register_fyers_tools(mcp, orchestrator)
    return mcp.tools


def test_detector_status_shows_the_feed(offline):
    from services.fdk_mcp_service.fyers_tools import register_fyers_tools
    mcp = _FakeMCP()
    register_fyers_tools(mcp, _orchestrator())
    status = asyncio.run(mcp.tools["get_detector_status"]())
    assert status["feed"]["connected"] is False


def test_listing_symbols_defaults_to_the_shoonya_label(mcp_tools):
    result = asyncio.run(mcp_tools["list_monitored_symbols"]())
    assert result == {"detector": "shoonya", "count": 1, "symbols": ["TCS-EQ"]}


def test_the_fyers_label_is_no_longer_accepted(mcp_tools):
    assert "error" in asyncio.run(mcp_tools["list_monitored_symbols"]("fyers"))


def test_operator_messages_do_not_name_the_old_broker():
    from services.fdk_telegram_service.message_template import websocket_rebuild_message
    assert "Fyers" not in websocket_rebuild_message()
    assert "Shoonya" in websocket_rebuild_message()


# --- the Fyers SDK is gone -------------------------------------------

def test_nothing_imports_the_fyers_sdk():
    root = pathlib.Path(__file__).resolve().parents[1]
    sources = [*root.glob("services/**/*.py"), *root.glob("shared/*.py"), root / "fdk_main.py"]
    offenders = [p.name for p in sources if "fyers_apiv3" in p.read_text(encoding="utf-8")]
    assert offenders == []


def test_the_fyers_sdk_is_not_installed_by_the_image():
    root = pathlib.Path(__file__).resolve().parents[1]
    for name in ("requirements.txt", "fdk_requirements.txt", "Dockerfile", "fdk_main.py"):
        text = (root / name).read_text(encoding="utf-8")
        assert "fyers-apiv3" not in text and "nodeps" not in text, name
    assert not (root / "fdk_requirements_nodeps.txt").exists()
    assert "websocket-client" in (root / "fdk_requirements.txt").read_text(encoding="utf-8")
