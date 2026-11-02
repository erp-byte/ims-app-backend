"""Orchestrator broker wiring: which authenticator it builds and what it does after login.

Offline: psycopg2 and requests are faked, the Shoonya HTTP login is patched.
"""
import asyncio
import logging

import psycopg2
import pytest
import requests

from fdk_fakes import FakeConn, FakeResponse
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


def test_run_retries_the_initial_supabase_loads_until_they_succeed(offline, caplog):
    from services.fdk_supervisor_service import ochestrator as orch_mod
    from services.fdk_auth_service import shoonya_authenticator as sa
    offline.setattr(sa.shoonya_login, "validate_session", lambda user, tok: (False, "No token"))
    offline.setattr(sa.shoonya_login, "auto_login_available", lambda: True)
    offline.setattr(sa.shoonya_login, "auto_login_code", lambda *a, **k: {"code": "C1"})
    offline.setattr(sa.shoonya_login, "exchange_code",
                    lambda *a, **k: {"access_token": "sess-1", "account_id": "FA12345"})
    offline.setattr(orch_mod, "SUPERVISOR_RETRY_DELAY", 0.01)
    orch = _orchestrator()
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


def test_shoonya_logs_in_then_idles_without_touching_the_fyers_feed(offline, caplog):
    from services.fdk_auth_service import shoonya_authenticator as sa
    offline.setattr(sa.shoonya_login, "validate_session", lambda user, tok: (False, "No token"))
    offline.setattr(sa.shoonya_login, "auto_login_available", lambda: True)
    offline.setattr(sa.shoonya_login, "auto_login_code", lambda *a, **k: {"code": "C1"})
    offline.setattr(sa.shoonya_login, "exchange_code",
                    lambda *a, **k: {"access_token": "sess-1", "account_id": "FA12345"})
    orch = _orchestrator()
    touched = []
    offline.setattr(orch.fyers, "build", lambda *a, **k: touched.append("fyers.build"))
    offline.setattr(orch.penny, "build", lambda *a, **k: touched.append("penny.build"))
    offline.setattr(orch, "_connect_dispatcher", lambda: touched.append("connect"))

    async def drive():
        task = asyncio.create_task(orch.run())
        for _ in range(50):
            if orch.authenticator.is_authenticated:
                break
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.3)                 # give the loop a chance to (wrongly) start things
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    with caplog.at_level(logging.WARNING, logger="fdk.orchestrator"):
        asyncio.run(drive())

    assert orch.authenticator.is_authenticated
    assert orch.authenticator.access_token == "sess-1"
    assert touched == []
    assert orch._dispatcher is None
    assert "tick feed" in " ".join(r.getMessage() for r in caplog.records)
