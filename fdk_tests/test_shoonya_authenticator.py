"""Unit tests for ShoonyaAuthenticator — the auth ladder the orchestrator drives.

Supabase and Telegram are replaced by in-memory fakes; the HTTP login module is
patched at the function level so no network is touched.
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


def _config():
    return SimpleNamespace(shoonya=ShoonyaConfig(
        username="FA12345", password="Pass@123", totp_secret="JBSWY3DPEHPK3PXP",
        vendor_code="FA12345_U", api_key="k" * 64, imei="abc1234",
    ))


def _auth(token_manager, sender, monkeypatch, validate=(False, "No token"), login=None):
    monkeypatch.setattr(sa.shoonya_login, "validate_session", lambda user, tok: validate)
    calls = []

    def _login_with_retry(creds, max_attempts, delay, should_cancel=None):
        calls.append(creds)
        return login if login is not None else {"error": "Invalid Input : Wrong Password"}

    monkeypatch.setattr(sa.shoonya_login, "login_with_retry", _login_with_retry)
    auth = sa.ShoonyaAuthenticator(_config(), token_manager, sender)
    auth.cooldown = 0
    return auth, calls


def test_rejects_a_malformed_totp_secret_at_construction():
    config = _config()
    config.shoonya.totp_secret = "not base32 0189"

    with pytest.raises(ValueError, match="FDK_SHOONYA_TOTP_SECRET"):
        sa.ShoonyaAuthenticator(config, FakeTokenManager(), FakeSender())


def test_rejects_an_empty_totp_secret_at_construction():
    config = _config()
    config.shoonya.totp_secret = ""

    with pytest.raises(ValueError, match="FDK_SHOONYA_TOTP_SECRET"):
        sa.ShoonyaAuthenticator(config, FakeTokenManager(), FakeSender())


def test_reuses_a_cached_token_that_the_broker_still_accepts(monkeypatch):
    tm, sender = FakeTokenManager(token="sess-cached"), FakeSender()
    auth, login_calls = _auth(tm, sender, monkeypatch, validate=(True, "Valid"))

    assert asyncio.run(auth.authenticate()) is True
    assert auth.is_authenticated and auth.access_token == "sess-cached"
    assert login_calls == []
    assert any("Shoonya" in m and "Successful" in m for m in sender.messages)


def test_logs_in_fresh_when_the_cached_token_is_rejected(monkeypatch):
    tm, sender = FakeTokenManager(token="sess-stale"), FakeSender()
    auth, login_calls = _auth(tm, sender, monkeypatch, validate=(False, "Session Expired"),
                              login={"access_token": "sess-new", "account_id": "FA12345",
                                     "user_name": "Test"})

    assert asyncio.run(auth.authenticate()) is True
    assert auth.access_token == "sess-new"
    assert auth.account_id == "FA12345"
    assert tm.saved == ["sess-new"]
    assert login_calls and login_calls[0]["username"] == "FA12345"
    assert login_calls[0]["imei"] == "abc1234"
    assert auth.last_login_at is not None and auth.last_login_at.tzinfo is not None


def test_failed_login_notifies_telegram_and_returns_false(monkeypatch):
    tm, sender = FakeTokenManager(), FakeSender()
    auth, _ = _auth(tm, sender, monkeypatch, login={"error": "Invalid Input : Wrong Password"})

    assert asyncio.run(auth.authenticate()) is False
    assert auth.is_authenticated is False
    assert auth.last_error == "Invalid Input : Wrong Password"
    assert any("Wrong Password" in m for m in sender.messages)
    assert tm.saved == []


def test_cancel_cuts_the_post_failure_cooldown_short(monkeypatch):
    tm, sender = FakeTokenManager(), FakeSender()
    auth, _ = _auth(tm, sender, monkeypatch, login={"error": "Invalid Input : Invalid OTP"})
    auth.cooldown = 300

    async def run():
        task = asyncio.create_task(auth.authenticate())
        await asyncio.sleep(0.05)
        auth.cancel_auth()                      # what /hld and /rst do
        return await asyncio.wait_for(task, timeout=5)

    assert asyncio.run(run()) is False


def test_reset_clears_the_session_so_the_next_login_is_fresh(monkeypatch):
    tm, sender = FakeTokenManager(token="sess-cached"), FakeSender()
    auth, _ = _auth(tm, sender, monkeypatch, validate=(True, "Valid"))
    asyncio.run(auth.authenticate())

    auth.reset()

    assert auth.is_authenticated is False
    assert auth.access_token is None
    assert auth.account_id is None


def test_reset_cancel_allows_a_new_attempt_after_cancel(monkeypatch):
    tm, sender = FakeTokenManager(token="sess-cached"), FakeSender()
    auth, _ = _auth(tm, sender, monkeypatch, validate=(True, "Valid"))
    auth.cancel_auth()
    auth.reset_cancel()

    assert asyncio.run(auth.authenticate()) is True
