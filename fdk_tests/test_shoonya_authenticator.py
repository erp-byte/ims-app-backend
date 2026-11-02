"""Unit tests for ShoonyaAuthenticator — the OAuth ladder the orchestrator drives.

cached session -> automated browser login -> manual Telegram link (paste the URL).
Supabase and Telegram are in-memory fakes; the HTTP/browser functions of the
login module are patched so nothing touches the network.
"""
import asyncio
import time
from types import SimpleNamespace

import pytest

from services.fdk_auth_service import shoonya_authenticator as sa
from shared.fdk_config_loader import ShoonyaConfig


class FakeTokenManager:
    def __init__(self, token=None, age_seconds=0):
        self.token = token
        self.ts = time.time() - age_seconds if token else 0.0
        self.saved = []
        self.cleared = 0

    def is_token_valid_by_time(self):
        if not self.token:
            return False, "No token available"
        if time.time() - self.ts < 8 * 3600:
            return True, "Token is valid"
        return False, "Token expired"

    def load_token(self):
        return self.token, self.ts, "", None

    def save_token(self, access_token, ts=None, created_at=None, refresh_token=None):
        self.saved.append(access_token)
        self.token, self.ts = access_token, time.time()

    def clear_tokens(self):
        self.cleared += 1
        self.token = None


class FakeSender:
    def __init__(self):
        self.messages = []

    async def send_async(self, text, parse_mode="HTML"):
        self.messages.append(text)
        return True


def _config(auto_login=True):
    return SimpleNamespace(shoonya=ShoonyaConfig(
        username="FA12345", password="Pass@123", totp_secret="JBSWY3DPEHPK3PXP",
        client_id="FA12345_U", secret_code="s" * 64, auto_login=auto_login,
    ))


def _auth(tm, sender, monkeypatch, *, validate=(False, "No token"), auto=None,
          exchange=None, auto_login=True, browser=True):
    monkeypatch.setattr(sa.shoonya_login, "validate_session", lambda user, tok: validate)
    monkeypatch.setattr(sa.shoonya_login, "auto_login_available", lambda: browser)
    auto_calls, exchange_calls = [], []

    def _auto(client_id, username, password, totp_secret, **kw):
        auto_calls.append(username)
        return auto if auto is not None else {"error": "browser timeout"}

    def _exchange(username, client_id, secret_code, code):
        exchange_calls.append(code)
        return exchange if exchange is not None else {
            "access_token": f"acc-{code}", "account_id": username, "refresh_token": None}

    monkeypatch.setattr(sa.shoonya_login, "auto_login_code", _auto)
    monkeypatch.setattr(sa.shoonya_login, "exchange_code", _exchange)
    auth = sa.ShoonyaAuthenticator(_config(auto_login), tm, sender)
    auth.auto_attempts = 1            # no real retry delays inside tests
    auth.auto_retry_delay = 0
    auth.resend_interval = 0.05
    return auth, auto_calls, exchange_calls


def test_rejects_a_malformed_or_empty_totp_secret_at_construction():
    for bad in ("not base32 0189", ""):
        config = _config()
        config.shoonya.totp_secret = bad
        with pytest.raises(ValueError, match="FDK_SHOONYA_TOTP_SECRET"):
            sa.ShoonyaAuthenticator(config, FakeTokenManager(), FakeSender())


def test_reuses_a_cached_session_that_the_broker_still_accepts(monkeypatch):
    tm, sender = FakeTokenManager(token="acc-cached"), FakeSender()
    auth, auto_calls, exchange_calls = _auth(tm, sender, monkeypatch, validate=(True, "Valid"))

    assert asyncio.run(auth.authenticate()) is True
    assert auth.is_authenticated and auth.access_token == "acc-cached"
    assert auto_calls == [] and exchange_calls == []
    assert any("Shoonya" in m and "Successful" in m for m in sender.messages)


def test_automated_login_exchanges_the_captured_code(monkeypatch):
    tm, sender = FakeTokenManager(), FakeSender()
    auth, auto_calls, exchange_calls = _auth(tm, sender, monkeypatch, auto={"code": "C1"})

    assert asyncio.run(auth.authenticate()) is True
    assert auto_calls == ["FA12345"] and exchange_calls == ["C1"]
    assert auth.access_token == "acc-C1" and auth.account_id == "FA12345"
    assert tm.saved == ["acc-C1"]
    assert auth.last_login_at is not None and auth.last_login_at.tzinfo is not None
    assert auth.login_method == "auto"


def test_falls_back_to_the_manual_link_when_the_browser_login_fails(monkeypatch):
    tm, sender = FakeTokenManager(), FakeSender()
    auth, auto_calls, exchange_calls = _auth(tm, sender, monkeypatch,
                                             auto={"error": "browser timeout"})

    async def run():
        task = asyncio.create_task(auth.authenticate())
        for _ in range(100):
            if auth.awaiting_manual_login:
                break
            await asyncio.sleep(0.01)
        assert auth.awaiting_manual_login
        assert any("client_id=FA12345_U" in m for m in sender.messages)   # the OAuth link
        # the operator logs in and pastes the URL they land on
        assert auth.submit_code("https://trade.shoonya.com/?code=PASTED1") is True
        return await asyncio.wait_for(task, timeout=5)

    assert asyncio.run(run()) is True
    assert exchange_calls == ["PASTED1"]
    assert auth.access_token == "acc-PASTED1"
    assert auth.awaiting_manual_login is False
    assert auth.login_method == "manual"
    assert "browser timeout" not in (auth.last_error or "")


def test_manual_link_is_used_directly_when_auto_login_is_off(monkeypatch):
    tm, sender = FakeTokenManager(), FakeSender()
    auth, auto_calls, _ = _auth(tm, sender, monkeypatch, auto_login=False)

    async def run():
        task = asyncio.create_task(auth.authenticate())
        for _ in range(100):
            if auth.awaiting_manual_login:
                break
            await asyncio.sleep(0.01)
        auth.submit_code("?code=M1")
        return await asyncio.wait_for(task, timeout=5)

    assert asyncio.run(run()) is True
    assert auto_calls == []


def test_manual_link_is_used_directly_when_no_browser_is_installed(monkeypatch):
    tm, sender = FakeTokenManager(), FakeSender()
    auth, auto_calls, _ = _auth(tm, sender, monkeypatch, browser=False)

    async def run():
        task = asyncio.create_task(auth.authenticate())
        for _ in range(100):
            if auth.awaiting_manual_login:
                break
            await asyncio.sleep(0.01)
        auth.submit_code("?code=M2")
        return await asyncio.wait_for(task, timeout=5)

    assert asyncio.run(run()) is True
    assert auto_calls == []


def test_manual_link_is_resent_while_waiting(monkeypatch):
    tm, sender = FakeTokenManager(), FakeSender()
    auth, _, _ = _auth(tm, sender, monkeypatch, auto_login=False)

    async def run():
        task = asyncio.create_task(auth.authenticate())
        await asyncio.sleep(0.2)                      # several resend intervals
        auth.cancel_auth()
        return await asyncio.wait_for(task, timeout=5)

    assert asyncio.run(run()) is False
    assert sum("client_id=FA12345_U" in m for m in sender.messages) >= 2


def test_bad_pasted_code_is_reported_and_the_wait_continues(monkeypatch):
    tm, sender = FakeTokenManager(), FakeSender()
    auth, _, exchange_calls = _auth(tm, sender, monkeypatch, auto_login=False,
                                    exchange={"error": "Invalid Input : Invalid checksum"})

    async def run():
        task = asyncio.create_task(auth.authenticate())
        for _ in range(100):
            if auth.awaiting_manual_login:
                break
            await asyncio.sleep(0.01)
        auth.submit_code("?code=BAD")
        await asyncio.sleep(0.1)
        assert auth.awaiting_manual_login            # still waiting for a good paste
        assert any("Invalid checksum" in m for m in sender.messages)
        assert auth.submit_code("not a code at all") is False
        auth.cancel_auth()
        return await asyncio.wait_for(task, timeout=5)

    assert asyncio.run(run()) is False
    assert exchange_calls == ["BAD"]
    assert auth.is_authenticated is False


def test_cancel_cuts_the_manual_wait_short(monkeypatch):
    tm, sender = FakeTokenManager(), FakeSender()
    auth, _, _ = _auth(tm, sender, monkeypatch, auto_login=False)
    auth.resend_interval = 300

    async def run():
        task = asyncio.create_task(auth.authenticate())
        await asyncio.sleep(0.05)
        auth.cancel_auth()                      # what /hld and /rst do
        return await asyncio.wait_for(task, timeout=5)

    assert asyncio.run(run()) is False
    assert auth.awaiting_manual_login is False


def test_reset_clears_the_session_so_the_next_login_is_fresh(monkeypatch):
    tm, sender = FakeTokenManager(token="acc-cached"), FakeSender()
    auth, _, _ = _auth(tm, sender, monkeypatch, validate=(True, "Valid"))
    asyncio.run(auth.authenticate())

    auth.reset()

    assert auth.is_authenticated is False
    assert auth.access_token is None and auth.account_id is None


def test_reset_cancel_allows_a_new_attempt_after_cancel(monkeypatch):
    tm, sender = FakeTokenManager(token="acc-cached"), FakeSender()
    auth, _, _ = _auth(tm, sender, monkeypatch, validate=(True, "Valid"))
    auth.cancel_auth()
    auth.reset_cancel()

    assert asyncio.run(auth.authenticate()) is True
