"""Unit tests for the Shoonya OAuth login module (post-April-2026 API).

Loaded via importlib file-load so the package __init__ (which pulls in psycopg2 via
the token manager) is bypassed. Only requests + pyotp are needed; selenium is
replaced by an injected fake driver.
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

USER, CLIENT_ID, SECRET = "FA12345", "FA12345_U", "s" * 64


class _JsonResp:
    def __init__(self, payload, status_code=200):
        self.text = json.dumps(payload)
        self.status_code = status_code


class _HtmlResp:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text


def _parse_form(data: str) -> dict:
    assert data.startswith("jData=")
    assert "&jKey=" not in data            # the new API never sends jKey
    return json.loads(data[len("jData="):])


# ----------------------------------------------------------------- URLs & helpers
def test_oauth_login_url_carries_the_client_id():
    assert shoonya.oauth_login_url(CLIENT_ID) == \
        "https://api.shoonya.com/OAuthlogin/authorize/oauth?client_id=FA12345_U"


def test_extract_code_from_a_pasted_redirect_url():
    url = "https://trade.shoonya.com/OAuthlogin/?code=AbC123xyz&state=1"
    assert shoonya.extract_code(url) == "AbC123xyz"
    assert shoonya.extract_code("please login first, then paste " + url) == "AbC123xyz"
    assert shoonya.extract_code("/hld holiday") is None
    assert shoonya.extract_code("") is None


def test_checksum_is_sha256_of_client_id_secret_and_code():
    assert shoonya.checksum(CLIENT_ID, SECRET, "code1") == \
        hashlib.sha256(f"{CLIENT_ID}{SECRET}code1".encode()).hexdigest()


# ----------------------------------------------------------------- token exchange
def test_exchange_code_posts_gen_acs_tok_and_returns_the_session(monkeypatch):
    calls = []

    def _post(url, data=None, headers=None, timeout=None, **kw):
        calls.append((url, data, headers, timeout))
        return _JsonResp({"stat": "Ok", "access_token": "acc-1", "susertoken": "sus-1",
                          "USERID": USER, "actid": USER, "refresh_token": "ref-1"})

    monkeypatch.setattr(shoonya.requests, "post", _post)

    result = shoonya.exchange_code(USER, CLIENT_ID, SECRET, "code1")

    assert result["access_token"] == "acc-1"
    assert result["account_id"] == USER
    assert result["refresh_token"] == "ref-1"
    url, data, headers, timeout = calls[0]
    assert url == "https://api.shoonya.com/NorenWClientAPI/GenAcsTok"
    assert timeout and timeout > 0
    payload = _parse_form(data)
    assert payload == {"code": "code1", "uid": USER,
                       "checksum": shoonya.checksum(CLIENT_ID, SECRET, "code1")}
    assert not headers                                   # no Bearer yet, as in the vendor SDK


def test_exchange_code_reports_the_brokers_error_message(monkeypatch):
    monkeypatch.setattr(shoonya.requests, "post",
                        lambda *a, **k: _JsonResp({"stat": "Not_Ok",
                                                   "emsg": "Invalid Input : Invalid checksum"}))

    result = shoonya.exchange_code(USER, CLIENT_ID, SECRET, "code1")

    assert "access_token" not in result
    assert "Invalid checksum" in result["error"]


def test_exchange_code_reports_http_status_when_the_reply_is_not_json(monkeypatch):
    monkeypatch.setattr(shoonya.requests, "post",
                        lambda *a, **k: _HtmlResp(502, "<html>502 Bad Gateway</html>"))

    result = shoonya.exchange_code(USER, CLIENT_ID, SECRET, "code1")

    assert "HTTP 502" in result["error"] and "Expecting value" not in result["error"]


def test_exchange_code_reports_network_failure_instead_of_raising(monkeypatch):
    def _boom(*a, **k):
        raise shoonya.requests.ConnectionError("dns down")

    monkeypatch.setattr(shoonya.requests, "post", _boom)

    assert "dns down" in shoonya.exchange_code(USER, CLIENT_ID, SECRET, "code1")["error"]


# ----------------------------------------------------------------- session check
def test_validate_session_uses_the_bearer_header(monkeypatch):
    calls = []

    def _post(url, data=None, headers=None, timeout=None, **kw):
        calls.append((url, data, headers))
        return _JsonResp({"stat": "Ok", "uname": "Test User"})

    monkeypatch.setattr(shoonya.requests, "post", _post)

    ok, detail = shoonya.validate_session(USER, "acc-1")

    assert ok is True
    url, data, headers = calls[0]
    assert url == "https://api.shoonya.com/NorenWClientAPI/UserDetails"
    assert _parse_form(data) == {"uid": USER}
    assert headers["Authorization"] == "Bearer acc-1"
    assert headers["Content-Type"].startswith("application/json")


def test_validate_session_rejects_an_expired_token(monkeypatch):
    monkeypatch.setattr(shoonya.requests, "post",
                        lambda *a, **k: _JsonResp({"stat": "Not_Ok",
                                                   "emsg": "Session Expired : Invalid Session Key"},
                                                  status_code=401))

    ok, detail = shoonya.validate_session(USER, "stale")

    assert ok is False and "Session Expired" in detail


def test_validate_session_treats_non_json_and_network_errors_as_invalid(monkeypatch):
    monkeypatch.setattr(shoonya.requests, "post", lambda *a, **k: _HtmlResp(503, ""))
    ok, detail = shoonya.validate_session(USER, "acc-1")
    assert ok is False and "HTTP 503" in detail

    def _boom(*a, **k):
        raise shoonya.requests.Timeout("slow")

    monkeypatch.setattr(shoonya.requests, "post", _boom)
    ok, detail = shoonya.validate_session(USER, "acc-1")
    assert ok is False and "slow" in detail


# ----------------------------------------------------------------- automated login (browser)
class _FakeElement:
    def __init__(self, displayed=True):
        self.displayed = displayed
        self.value = None
        self.clicks = 0

    def is_displayed(self):
        return self.displayed

    def click(self):
        self.clicks += 1

    def clear(self):
        self.value = ""

    def send_keys(self, value):
        self.value = (self.value or "") + value


class _FakeDriver:
    """Mimics the slice of the Selenium WebDriver the login uses."""

    def __init__(self, code="ABC123", code_after_polls=2, inputs=3, password_field_delay=0,
                 redirect="https://trade.shoonya.com/landing?code={code}&route_to=x",
                 rerender_after_login=False, body_text="", current_url=""):
        self.urls = []
        self.inputs = [_FakeElement() for _ in range(inputs)]
        self.hidden = _FakeElement(displayed=False)
        self.login_button = _FakeElement()
        self.code = code
        self.code_after_polls = code_after_polls
        self.polls = 0
        self.password_field_delay = password_field_delay
        self.redirect = redirect
        self.rerender_after_login = rerender_after_login
        self.generations = [self.inputs]
        self.body = _FakeElement()
        self.body.text = body_text
        self.current_url = current_url
        self.quit_called = False

    def get(self, url):
        self.urls.append(url)

    def _current_inputs(self):
        if self.rerender_after_login and self.login_button.clicks >= 1 and len(self.generations) == 1:
            # the SPA re-rendered after LOGIN: old elements go stale, new ones appear
            for old in self.inputs:
                old.stale = True
            self.inputs = [_FakeElement() for _ in self.inputs]
            self.generations.append(self.inputs)
        return self.inputs

    def find_elements(self, by, selector):
        inputs = self._current_inputs()
        if selector == "input[type='password']":
            if self.password_field_delay > 0:
                self.password_field_delay -= 1
                return []
            return [inputs[1]] if len(inputs) > 1 else []
        return [self.hidden] + inputs

    def find_element(self, by, selector):
        if by == "tag name" and selector == "body":
            return self.body
        assert "LOGIN" in selector
        return self.login_button

    def get_log(self, kind):
        self.polls += 1
        if self.code and self.polls >= self.code_after_polls:
            url = self.redirect.format(code=self.code)
            msg = {"message": {"method": "Network.requestWillBeSent",
                               "params": {"request": {"url": url}}}}
            return [{"message": json.dumps(msg)}]
        return []

    def quit(self):
        self.quit_called = True


def _stale_guard(method):
    def wrapper(self, *args, **kwargs):
        if getattr(self, "stale", False):
            raise RuntimeError("stale element reference: stale element not found")
        return method(self, *args, **kwargs)
    return wrapper


_FakeElement.click = _stale_guard(_FakeElement.click)
_FakeElement.clear = _stale_guard(_FakeElement.clear)
_FakeElement.send_keys = _stale_guard(_FakeElement.send_keys)


def _auto(driver, **kw):
    kw.setdefault("sleep", lambda s: None)
    kw.setdefault("totp_now", lambda secret: "123456")
    return shoonya.auto_login_code(CLIENT_ID, USER, "Pass@123", "JBSWY3DPEHPK3PXP",
                                   driver_factory=lambda: driver, **kw)


def test_auto_login_drives_the_official_login_page_and_captures_the_code():
    driver = _FakeDriver(code="XYZ789", password_field_delay=2)

    result = _auto(driver)

    assert result == {"code": "XYZ789"}
    assert driver.urls == ["https://api.shoonya.com/OAuthlogin/investor-entry-level/login"
                           "?api_key=FA12345_U&route_to=FA12345"]
    assert [i.value for i in driver.inputs] == [USER, "Pass@123", "123456"]
    assert driver.hidden.value is None                   # hidden inputs are skipped
    assert driver.login_button.clicks == 1
    assert driver.quit_called


def test_auto_login_resubmits_exactly_once_then_gives_up():
    driver = _FakeDriver(code=None)
    clock = {"t": 0.0}
    generated = []

    def _totp(secret):                       # a real TOTP is different every time we look
        generated.append(f"{len(generated):06d}")
        return generated[-1]

    def _sleep(s):
        clock["t"] += 30            # every poll moves the clock 30 s -> hits the 60 s window fast

    result = _auto(driver, sleep=_sleep, clock=lambda: clock["t"], totp_now=_totp)

    assert "error" in result and "code" not in result
    assert driver.login_button.clicks == 2               # first submit + one resubmit, never more
    assert driver.inputs[2].value == generated[1]
    assert driver.quit_called


def test_auto_login_stops_when_cancelled():
    driver = _FakeDriver(code=None)
    polls = {"n": 0}

    def _cancelled():
        polls["n"] += 1
        return polls["n"] > 3

    result = _auto(driver, cancelled=_cancelled)

    assert result == {"error": "cancelled"}
    assert driver.quit_called


def test_auto_login_caps_the_page_load_time():
    driver = _FakeDriver()
    driver.page_load_timeout = None
    driver.set_page_load_timeout = lambda seconds: setattr(driver, "page_load_timeout", seconds)

    _auto(driver, page_wait=12)

    assert driver.page_load_timeout == 12


def test_auto_login_accepts_a_long_code_on_a_non_shoonya_redirect():
    long_code = "A" * 32
    driver = _FakeDriver(code=long_code, redirect="https://my-app.example.com/cb?code={code}")

    assert _auto(driver) == {"code": long_code}


def test_auto_login_accepts_the_registered_redirect_even_with_a_short_code():
    driver = _FakeDriver(code="short1", redirect="https://my-app.example.com/cb?code={code}")

    assert _auto(driver, redirect_hint="https://my-app.example.com/cb") == {"code": "short1"}


def test_auto_login_ignores_short_code_params_on_foreign_urls():
    driver = _FakeDriver(code="en", redirect="https://cdn.example.net/app.js?code={code}")
    clock = {"t": 0.0}

    def _sleep(s):
        clock["t"] += 30

    result = _auto(driver, sleep=_sleep, clock=lambda: clock["t"], totp_now=lambda s: "123456")

    assert "error" in result


def test_totp_resubmit_requeries_the_inputs_after_the_page_rerenders():
    driver = _FakeDriver(code="LATE1", code_after_polls=10 ** 6, rerender_after_login=True)
    clock = {"t": 0.0}
    codes = iter(["111111", "222222"])

    def _sleep(s):
        clock["t"] += 30
        if driver.login_button.clicks >= 2:        # second submit done: let the code appear
            driver.code_after_polls = 0

    result = _auto(driver, sleep=_sleep, clock=lambda: clock["t"], totp_now=lambda s: next(codes))

    assert result == {"code": "LATE1"}
    # fresh elements, not the stale ones — and ALL refilled: re-created inputs start blank
    assert [e.value for e in driver.generations[1]] == [USER, "Pass@123", "222222"]
    assert driver.login_button.clicks == 2


def test_failure_reports_the_page_url_and_visible_text():
    driver = _FakeDriver(code=None, body_text="  Invalid OTP.\n Please try again.  ",
                         current_url="https://api.shoonya.com/OAuthlogin/investor-entry-level/login?x=1")
    clock = {"t": 0.0}

    def _sleep(s):
        clock["t"] += 30

    result = _auto(driver, sleep=_sleep, clock=lambda: clock["t"], totp_now=lambda s: "123456")

    assert "Invalid OTP. Please try again." in result["error"]
    assert "investor-entry-level/login" in result["error"]


def test_failure_redacts_the_code_and_points_at_the_redirect_setting():
    # the login itself worked, but the redirect went to an unknown host with a short code:
    # the diagnostics must explain that without leaking the one-time code into logs/health
    driver = _FakeDriver(code="short1", redirect="https://my-app.example.com/cb?code={code}",
                         current_url="https://my-app.example.com/cb?code=short1",
                         body_text="Logged in. code=short1")
    clock = {"t": 0.0}

    def _sleep(s):
        clock["t"] += 30

    result = _auto(driver, sleep=_sleep, clock=lambda: clock["t"], totp_now=lambda s: "123456")

    assert "short1" not in result["error"]
    assert "code=***" in result["error"]
    assert "FDK_SHOONYA_REDIRECT_URI" in result["error"]


def test_auto_login_reports_a_changed_page_layout():
    driver = _FakeDriver(inputs=2)                       # only two visible inputs

    result = _auto(driver)

    assert "error" in result and "login page" in result["error"].lower()
    assert driver.quit_called


def test_auto_login_reports_a_missing_browser():
    def _no_browser():
        raise RuntimeError("chromedriver not found")

    result = shoonya.auto_login_code(CLIENT_ID, USER, "p", "JBSWY3DPEHPK3PXP",
                                     driver_factory=_no_browser, sleep=lambda s: None)

    assert "chromedriver not found" in result["error"]
    assert result["fatal"] is True            # no point retrying: go straight to the Telegram link
