"""Wiring tests for fdk inside the Candor backend.

Covers the three states the backend can be in:
  * FDK_ENABLED unset  -> fdk is inert (routes answer, nothing starts, MCP 503)
  * FDK_ENABLED=true   -> detector builds, Telegram webhooks registered, /fdk/* and MCP serve
  * start-up failure   -> error is contained, backend keeps running, MCP news tools still serve

Nothing here touches the network, Supabase, Telegram or Fyers: psycopg2 and
requests are faked, and the orchestrator loop is replaced with an idle task.
"""
import asyncio
import logging
import pathlib

import pytest
import requests
from fastapi import FastAPI
from fastapi.testclient import TestClient

import fdk_main
from fdk_fakes import FakeConn, FakeResponse
from services.fdk_auth_service import server as fdk_auth_server

ROOT = pathlib.Path(__file__).resolve().parents[1]

FDK_ENV = {
    "FDK_ENABLED": "true",
    "FDK_SHOONYA_USERNAME": "FA12345",
    "FDK_SHOONYA_PASSWORD": "Pass@123",
    "FDK_SHOONYA_TOTP_SECRET": "JBSWY3DPEHPK3PXP",
    "FDK_SHOONYA_CLIENT_ID": "FA12345_U",
    "FDK_SHOONYA_SECRET_CODE": "s" * 64,
    "FDK_LOGIN_BOT_TOKEN": "tok-login", "FDK_LOGIN_CHAT_ID": "1",
    "FDK_FYERS_TRADE_BOT_TOKEN": "tok-ft", "FDK_FYERS_TRADE_CHAT_ID": "2",
    "FDK_FYERS_SUMMARY_BOT_TOKEN": "tok-fs", "FDK_FYERS_SUMMARY_CHAT_ID": "3",
    "FDK_PENNY_TRADE_BOT_TOKEN": "tok-pt", "FDK_PENNY_TRADE_CHAT_ID": "4",
    "FDK_PENNY_SUMMARY_BOT_TOKEN": "tok-ps", "FDK_PENNY_SUMMARY_CHAT_ID": "5",
    "FDK_SUPABASE_DSN": "postgresql://fake:fake@localhost/fake",
}

MCP_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}


# ----------------------------------------------------------------- fixtures
@pytest.fixture
def fresh_state(monkeypatch):
    """Reset the module-level fdk state and isolate from any FDK_* already in the environment."""
    for optional in ("FDK_MCP_PATH_SECRET", "FDK_NEWS_BOT_TOKEN", "FDK_NEWS_CHAT_ID",
                     "FDK_SCHEDULING_ENABLED", "FDK_FYERS_CLIENT_ID",
                     "FDK_SHOONYA_IMEI", "FDK_SHOONYA_REDIRECT_URI"):
        monkeypatch.delenv(optional, raising=False)
    monkeypatch.setattr(fdk_main, "FDK_SELF_PING", False)   # evaluated at import time
    fdk_main.fdk_state.__init__()
    fdk_auth_server._orchestrator = fdk_auth_server._login_sender = None
    fdk_auth_server._pending_summary_date = False
    yield
    fdk_main.fdk_state.__init__()
    fdk_auth_server._orchestrator = fdk_auth_server._login_sender = None
    fdk_auth_server._pending_summary_date = False


@pytest.fixture
def posted(monkeypatch):
    """Fake requests.post/get and record every call (Telegram API, self-ping)."""
    calls = []

    def _post(url, *args, **kwargs):
        calls.append((url, kwargs))
        return FakeResponse()

    monkeypatch.setattr(requests, "post", _post)
    # getUpdates (polling mode) answers "no updates"; everything else is a plain OK
    monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResponse({"ok": True, "result": []}))
    return calls


@pytest.fixture
def fake_supabase(monkeypatch):
    import psycopg2
    monkeypatch.setattr(psycopg2, "connect", lambda *a, **k: FakeConn())


@pytest.fixture
def idle_orchestrator(monkeypatch):
    from services.fdk_supervisor_service.ochestrator import Orchestrator

    async def _idle(self):
        await asyncio.Event().wait()

    monkeypatch.setattr(Orchestrator, "run", _idle)


def _make_app() -> FastAPI:
    app = FastAPI(lifespan=lambda _app: fdk_main.fdk_lifespan())
    fdk_main.fdk_mount(app)
    return app


def _mcp(client, method, _id=1, **params):
    body = {"jsonrpc": "2.0", "id": _id, "method": method, "params": params}
    resp = client.post("/fdk/mcp", json=body, headers=MCP_HEADERS)
    assert resp.status_code == 200, resp.text
    return resp.json()


# ----------------------------------------------------------------- tests
def test_fdk_logger_is_namespaced():
    from shared.fdk_logger import get_logger
    fdk_log = get_logger("main")
    assert fdk_log.name == "fdk.main"
    assert fdk_log is not logging.getLogger("main")


def test_disabled_is_inert(fresh_state, monkeypatch):
    from starlette.routing import Route

    monkeypatch.setenv("FDK_ENABLED", "false")
    app = _make_app()
    # the Fyers browser-redirect login route is gone with the Fyers login
    assert "/fdk/auth/callback" not in {r.path for r in app.routes if isinstance(r, Route)}
    with TestClient(app) as client:
        assert client.get("/fdk/health").json()["status"] == "disabled"
        assert client.post("/fdk/mcp", json={}, headers=MCP_HEADERS).status_code == 503
        # anything else under /fdk falls through to the (not yet built) MCP mount
        assert client.get("/fdk/auth/callback").status_code == 503
        resp = client.post("/fdk/webhook/telegram", json={"message": {"text": "/hld"}})
        assert resp.json()["status"] == "error"
    assert fdk_main.fdk_state.enabled is False
    assert fdk_main.fdk_state.orchestrator is None
    assert fdk_main.fdk_state.task is None


def test_enabled_builds_registers_and_serves(fresh_state, monkeypatch, posted,
                                             fake_supabase, idle_orchestrator):
    for key, value in FDK_ENV.items():
        monkeypatch.setenv(key, value)
    # BASE_URL is read at import time; point both consumers at the test host.
    monkeypatch.setattr(fdk_main, "BASE_URL", "http://fdk.test")
    from services.fdk_auth_service import tools as fdk_auth_tools
    monkeypatch.setattr(fdk_auth_tools, "BASE_URL", "http://fdk.test")

    with TestClient(_make_app()) as client:
        state = fdk_main.fdk_state
        assert state.error is None
        assert state.orchestrator is not None
        assert state.task is not None and not state.task.done()

        # Telegram: webhook on all 5 bots, command menu on the login bot; no poller
        webhooks = [kw["json"]["url"] for url, kw in posted if url.endswith("/setWebhook")]
        assert webhooks == ["http://fdk.test/fdk/webhook/telegram"] * 5
        assert sum(1 for url, _ in posted if url.endswith("/setMyCommands")) == 1
        assert any("tok-login" in url for url, _ in posted if url.endswith("/setMyCommands"))
        assert state.poller_task is None

        health = client.get("/fdk/health").json()
        assert health["status"] == "healthy"
        assert health["fyers_running"] is False and health["penny_running"] is False
        assert health["broker"] == "shoonya"
        assert health["authenticated"] is False
        assert health["last_login_at"] is None
        assert health["login_method"] is None
        assert health["telegram_mode"] == "webhook"
        # no login attempted yet (the run loop is idled in this test)
        assert health["login_state"] in ("waiting_for_market_hours", "in_progress")
        assert isinstance(health["market_hours"], bool)
        assert health["scheduling_enabled"] is True

        # A pasted login URL is ignored unless the authenticator is waiting for one...
        pasted = {"message": {"text": "https://trade.shoonya.com/?code=abc123"}}
        assert client.post("/fdk/webhook/telegram", json=pasted).json() == {"status": "ok"}
        # ...and handed to it when it is
        state.orchestrator.authenticator.awaiting_manual_login = True
        assert client.post("/fdk/webhook/telegram", json=pasted).json() == {"status": "code_received"}
        assert state.orchestrator.authenticator._pending_code == "abc123"
        assert client.get("/fdk/health").json()["login_state"] == "awaiting_manual_login"
        state.orchestrator.authenticator.awaiting_manual_login = False

        # Telegram command path -> orchestrator
        resp = client.post("/fdk/webhook/telegram", json={"message": {"text": "/hld holiday"}})
        assert resp.json() == {"status": "hold_activated"}
        assert client.get("/fdk/health").json()["status"] == "on_hold"

        # A pasted Fyers redirect URL is just an ordinary message now
        resp = client.post("/fdk/webhook/telegram",
                           json={"message": {"text": "https://x/?auth_code=abc123"}})
        assert resp.json() == {"status": "ok"}

        # MCP mounted under /fdk and answering (stateless, JSON responses)
        init = _mcp(client, "initialize", protocolVersion="2025-06-18", capabilities={},
                    clientInfo={"name": "fdk-test", "version": "0"})
        assert init["result"]["serverInfo"]["name"] == "fyersdk"
        tools = {t["name"] for t in _mcp(client, "tools/list", _id=2)["result"]["tools"]}
        assert {"get_detector_status", "get_volume_summary",
                "list_monitored_symbols", "fetch_news_headlines"} <= tools
        status = _mcp(client, "tools/call", _id=3, name="get_detector_status", arguments={})
        assert '"broker": "shoonya"' in str(status["result"]).replace("'", '"')

    assert fdk_main.fdk_state.task.cancelled() or fdk_main.fdk_state.task.done()
    assert fdk_main.fdk_state.mcp_app is None
    # Shutdown must also cancel a login that may be sleeping in a worker thread,
    # otherwise the interpreter waits for it (up to ~5 min) before exiting.
    assert fdk_main.fdk_state.orchestrator.authenticator._cancel_event.is_set()


def test_polling_mode_when_no_public_url(fresh_state, monkeypatch, posted,
                                         fake_supabase, idle_orchestrator):
    for key, value in FDK_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(fdk_main, "BASE_URL", "")            # no HTTPS host -> long polling

    with TestClient(_make_app()) as client:
        state = fdk_main.fdk_state
        assert state.orchestrator is not None
        assert state.poller_task is not None and not state.poller_task.done()
        assert not [u for u, _ in posted if u.endswith("/setWebhook")]
        assert any(u.endswith("/bottok-login/deleteWebhook") for u, _ in posted)
        assert sum(1 for u, _ in posted if u.endswith("/setMyCommands")) == 1
        assert client.get("/fdk/health").json()["telegram_mode"] == "polling"

    assert fdk_main.fdk_state.poller_task.cancelled() or fdk_main.fdk_state.poller_task.done()


def test_startup_failure_is_contained(fresh_state, monkeypatch, posted):
    import psycopg2

    for key, value in FDK_ENV.items():
        monkeypatch.setenv(key, value)

    def _down(*a, **k):
        raise psycopg2.OperationalError("supabase unreachable")

    monkeypatch.setattr(psycopg2, "connect", _down)

    with TestClient(_make_app()) as client:
        state = fdk_main.fdk_state
        assert state.orchestrator is None
        assert "OperationalError" in state.error
        health = client.get("/fdk/health").json()
        assert health["status"] == "unavailable"
        assert "supabase unreachable" in health["error"]
        # No detector -> no webhook registration attempted
        assert not [u for u, _ in posted if u.endswith("/setWebhook")]
        # MCP still serves the news tools; detector tools report unavailable
        tools = {t["name"] for t in _mcp(client, "tools/list")["result"]["tools"]}
        assert "fetch_news_headlines" in tools and "get_detector_status" in tools
        call = _mcp(client, "tools/call", _id=2, name="get_detector_status", arguments={})
        assert "unavailable" in str(call["result"]).lower()


def test_supervisor_failure_is_read_from_the_task():
    async def run():
        async def boom():
            raise RuntimeError("supabase down")

        async def forever():
            await asyncio.Event().wait()

        failed = asyncio.create_task(boom())
        running = asyncio.create_task(forever())
        cancelled = asyncio.create_task(forever())
        await asyncio.sleep(0)
        cancelled.cancel()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        try:
            return (fdk_main.supervisor_failure(failed), fdk_main.supervisor_failure(running),
                    fdk_main.supervisor_failure(cancelled), fdk_main.supervisor_failure(None))
        finally:
            running.cancel()

    failed_msg, running_msg, cancelled_msg, none_msg = asyncio.run(run())
    assert "supabase down" in failed_msg
    assert running_msg is None and none_msg is None
    assert cancelled_msg == "cancelled"


def test_health_reports_a_dead_supervisor(fresh_state):
    from types import SimpleNamespace

    async def run():
        async def boom():
            raise RuntimeError("supabase down")

        task = asyncio.create_task(boom())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        fdk_main.fdk_state.enabled = True
        fdk_main.fdk_state.orchestrator = SimpleNamespace(broker="shoonya")
        fdk_main.fdk_state.task = task
        return await fdk_main.fdk_health_check()

    health = asyncio.run(run())
    assert health["status"] == "stopped"
    assert "supabase down" in health["error"]
    assert health["broker"] == "shoonya"


def test_missing_env_is_contained(fresh_state, monkeypatch, posted):
    monkeypatch.setenv("FDK_ENABLED", "true")
    for key in FDK_ENV:
        if key != "FDK_ENABLED":
            monkeypatch.delenv(key, raising=False)
    with TestClient(_make_app()) as client:
        assert fdk_main.fdk_state.orchestrator is None
        assert "KeyError" in fdk_main.fdk_state.error
        assert client.get("/fdk/health").json()["status"] == "unavailable"


@pytest.mark.skipif(not (ROOT / ".env").exists(), reason="backend .env needed to import main")
def test_backend_main_is_wired(fresh_state, monkeypatch):
    monkeypatch.setenv("FDK_ENABLED", "false")
    import main as backend_main
    from starlette.routing import Mount, Route

    paths = {r.path for r in backend_main.app.routes if isinstance(r, Route)}
    assert {"/fdk/health", "/fdk/webhook/telegram"} <= paths
    assert "/fdk/auth/callback" not in paths
    mounts = [r for r in backend_main.app.routes if isinstance(r, Mount) and r.path == "/fdk"]
    assert len(mounts) == 1 and mounts[0].name == "fdk_mcp"
    # The mount is last so it never shadows the explicit /fdk routes.
    assert backend_main.app.routes[-1] is mounts[0]
