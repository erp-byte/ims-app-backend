"""Unit tests for the headless Shoonya login module (QuickAuth + UserDetails over HTTP).

Loaded via importlib file-load so the package __init__ (which pulls in psycopg2 via
the token manager) is bypassed. Only requests + pyotp are needed.
"""
import hashlib
import importlib.util
import json
import pathlib

import pytest

MODULE_PATH = (
    pathlib.Path(__file__).resolve().parents[1]
    / "services" / "fdk_auth_service" / "shoonya_login.py"
)


def _load():
    spec = importlib.util.spec_from_file_location("shoonya_login_under_test", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


shoonya = _load()

CREDS = {
    "username": "FA12345",
    "password": "Pass@123",
    "totp_secret": "JBSWY3DPEHPK3PXP",
    "vendor_code": "FA12345_U",
    "api_key": "k" * 64,
    "imei": "abc1234",
}


class _FakeResp:
    def __init__(self, payload):
        self.text = json.dumps(payload)
        self.status_code = 200

    def json(self):
        return json.loads(self.text)


def _parse_form(data: str) -> tuple[dict, str | None]:
    """Split the broker's 'jData=<json>[&jKey=<token>]' form body."""
    assert data.startswith("jData=")
    body = data[len("jData="):]
    jkey = None
    if "&jKey=" in body:
        body, jkey = body.split("&jKey=", 1)
    return json.loads(body), jkey


def test_login_posts_hashed_credentials_and_returns_session(monkeypatch):
    calls = []

    def _post(url, data=None, timeout=None, **kwargs):
        calls.append((url, data, timeout))
        return _FakeResp({"stat": "Ok", "susertoken": "sess-1", "actid": "FA12345",
                          "uname": "Test User", "exarr": ["NSE", "BSE"]})

    monkeypatch.setattr(shoonya.requests, "post", _post)
    monkeypatch.setattr(shoonya, "_totp_now", lambda secret: "123456")

    result = shoonya.login(CREDS)

    assert result["access_token"] == "sess-1"
    assert result["account_id"] == "FA12345"
    assert result["user_name"] == "Test User"
    url, data, timeout = calls[0]
    assert url == "https://api.shoonya.com/NorenWClientTP/QuickAuth"
    assert timeout and timeout > 0
    payload, jkey = _parse_form(data)
    assert jkey is None
    assert payload["uid"] == "FA12345"
    assert payload["pwd"] == hashlib.sha256(b"Pass@123").hexdigest()
    assert payload["appkey"] == hashlib.sha256(f"FA12345|{'k' * 64}".encode()).hexdigest()
    assert payload["factor2"] == "123456"
    assert payload["vc"] == "FA12345_U"
    assert payload["imei"] == "abc1234"
    assert payload["source"] == "API" and payload["apkversion"] == "1.0.0"


def test_login_reports_the_brokers_error_message(monkeypatch):
    monkeypatch.setattr(shoonya.requests, "post",
                        lambda *a, **k: _FakeResp({"stat": "Not_Ok",
                                                   "emsg": "Invalid Input : Wrong Password"}))
    monkeypatch.setattr(shoonya, "_totp_now", lambda secret: "123456")

    result = shoonya.login(CREDS)

    assert "access_token" not in result
    assert "Invalid Input : Wrong Password" in result["error"]


def test_login_reports_network_failure_instead_of_raising(monkeypatch):
    def _boom(*a, **k):
        raise shoonya.requests.ConnectionError("dns down")

    monkeypatch.setattr(shoonya.requests, "post", _boom)
    monkeypatch.setattr(shoonya, "_totp_now", lambda secret: "123456")

    result = shoonya.login(CREDS)

    assert "access_token" not in result
    assert "dns down" in result["error"]


def test_totp_code_is_six_digits_from_the_secret():
    code = shoonya._totp_now("JBSWY3DPEHPK3PXP")
    assert len(code) == 6 and code.isdigit()


def test_login_with_a_malformed_totp_secret_reports_an_error_instead_of_raising(monkeypatch):
    posted = []
    monkeypatch.setattr(shoonya.requests, "post", lambda *a, **k: posted.append(1))

    result = shoonya.login({**CREDS, "totp_secret": "not base32 0189"})

    assert "access_token" not in result
    assert "totp" in result["error"].lower() or "base32" in result["error"].lower()
    assert posted == []                      # nothing was sent to the broker


def test_validate_session_sends_the_token_as_jkey(monkeypatch):
    calls = []

    def _post(url, data=None, timeout=None, **kwargs):
        calls.append((url, data))
        return _FakeResp({"stat": "Ok", "uname": "Test User"})

    monkeypatch.setattr(shoonya.requests, "post", _post)

    ok, detail = shoonya.validate_session("FA12345", "sess-1")

    assert ok is True
    url, data = calls[0]
    assert url == "https://api.shoonya.com/NorenWClientTP/UserDetails"
    payload, jkey = _parse_form(data)
    assert payload["uid"] == "FA12345"
    assert jkey == "sess-1"


def test_validate_session_rejects_an_expired_token(monkeypatch):
    monkeypatch.setattr(shoonya.requests, "post",
                        lambda *a, **k: _FakeResp({"stat": "Not_Ok",
                                                   "emsg": "Session Expired : Invalid Session Key"}))

    ok, detail = shoonya.validate_session("FA12345", "stale")

    assert ok is False
    assert "Session Expired" in detail


def test_validate_session_treats_network_errors_as_invalid(monkeypatch):
    def _boom(*a, **k):
        raise shoonya.requests.Timeout("slow")

    monkeypatch.setattr(shoonya.requests, "post", _boom)

    ok, detail = shoonya.validate_session("FA12345", "sess-1")

    assert ok is False and "slow" in detail


def test_login_with_retry_retries_until_success(monkeypatch):
    attempts = []
    sleeps = []

    def _login(creds):
        attempts.append(1)
        if len(attempts) < 3:
            return {"error": "Invalid Input : Invalid OTP"}
        return {"access_token": "sess-3", "account_id": "FA12345", "user_name": "T"}

    monkeypatch.setattr(shoonya, "login", _login)
    monkeypatch.setattr(shoonya.time, "sleep", lambda s: sleeps.append(s))

    result = shoonya.login_with_retry(CREDS, max_attempts=5, delay=60)

    assert result["access_token"] == "sess-3"
    assert len(attempts) == 3
    assert sum(sleeps) == 2 * 60


def test_login_with_retry_gives_up_after_max_attempts(monkeypatch):
    attempts = []
    monkeypatch.setattr(shoonya, "login",
                        lambda creds: attempts.append(1) or {"error": "Invalid Input : Wrong Password"})
    monkeypatch.setattr(shoonya.time, "sleep", lambda s: None)

    result = shoonya.login_with_retry(CREDS, max_attempts=3, delay=1)

    assert "access_token" not in result
    assert "Wrong Password" in result["error"]
    assert len(attempts) == 3


def test_login_with_retry_stops_when_cancelled(monkeypatch):
    attempts = []
    monkeypatch.setattr(shoonya, "login",
                        lambda creds: attempts.append(1) or {"error": "Invalid Input : Invalid OTP"})
    monkeypatch.setattr(shoonya.time, "sleep", lambda s: None)
    state = {"cancel": False}

    def _should_cancel():
        state["cancel"] = True          # cancel as soon as the first wait begins
        return state["cancel"]

    result = shoonya.login_with_retry(CREDS, max_attempts=5, delay=60, should_cancel=_should_cancel)

    assert "access_token" not in result
    assert "cancel" in result["error"].lower()
    assert len(attempts) <= 1
