"""Shoonya (Finvasia) OAuth login — the API in force since 1 April 2026.

The old QuickAuth/vendor-code login and the NorenWClientTP base were retired
(they answer 502 for everyone). What exists now, mirrored from the vendor's
NorenRestApiOAuth SDK (0.0.41) and the official docs:

  1. The user logs in on Shoonya's hosted page (user id, password, OTP/TOTP):
         https://api.shoonya.com/OAuthlogin/authorize/oauth?client_id=<client id>
     and lands on a Shoonya URL that carries ?code=<authorization code>.
     Headless alternative (official "Python Authentication (Selenium)" page):
     drive the login page with a browser and sniff the code — auto_login_code().
  2. Exchange the code from the WHITELISTED static IP:
         POST {REST_BASE}/GenAcsTok   jData={"code", "checksum", "uid"}
         checksum = sha256(client_id + secret_code + code)
     -> {"access_token", "susertoken", "USERID", "actid", "refresh_token"}
  3. Every later call carries  Authorization: Bearer <access_token>.
     The token is valid for the trading day; there is no refresh endpoint.

Standalone module (stdlib + requests + pyotp; selenium only when the browser
login is used) so tests can load it by path without the package __init__.
"""
import hashlib
import importlib.util
import json
import logging
import os
import re
import time
from typing import Callable, Optional
from urllib.parse import parse_qs, urlparse

import pyotp
import requests

log = logging.getLogger("fdk.shoonya_login")

REST_BASE = "https://api.shoonya.com/NorenWClientAPI"
WS_URL = "wss://api.shoonya.com/NorenWSAPI/"
AUTHORIZE_URL = "https://api.shoonya.com/OAuthlogin/authorize/oauth"
LOGIN_PAGE_URL = "https://api.shoonya.com/OAuthlogin/investor-entry-level/login"
GEN_ACS_TOK_URL = f"{REST_BASE}/GenAcsTok"
USER_DETAILS_URL = f"{REST_BASE}/UserDetails"
LOGOUT_URL = f"{REST_BASE}/Logout"
HTTP_TIMEOUT = 15

_CODE_RE = re.compile(r"[?&#]code=([^&\s#]+)")


# ----------------------------------------------------------------- small helpers
def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _totp_now(secret: str) -> str:
    return pyotp.TOTP(secret).now()


def _snippet(text: str) -> str:
    """First words of a non-JSON reply (HTML tags stripped) for the error message."""
    plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text or "")).strip()
    return plain[:120] if plain else "(empty body)"


def _post(url: str, jdata: dict, headers: Optional[dict] = None) -> dict:
    """The broker's form convention: jData=<json>, token (if any) in the headers."""
    resp = requests.post(url, data="jData=" + json.dumps(jdata), headers=headers,
                         timeout=HTTP_TIMEOUT)
    try:
        return json.loads(resp.text)
    except ValueError:
        # A WAF block, a dead upstream (502/503) or a wrong URL answers with HTML
        # or nothing; say so instead of leaking a JSON-decoder message.
        raise RuntimeError(
            f"HTTP {resp.status_code} non-JSON reply from the broker: {_snippet(resp.text)}"
        ) from None


def bearer_headers(access_token: str) -> dict:
    """Exactly what the vendor SDK's injectOAuthHeader() attaches."""
    return {"Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=utf-8"}


# ----------------------------------------------------------------- OAuth pieces
def oauth_login_url(client_id: str) -> str:
    """The page the user (or the headless browser) logs in on."""
    return f"{AUTHORIZE_URL}?client_id={client_id}"


def extract_code(text: str) -> Optional[str]:
    """Authorization code from a pasted redirect URL (or any text containing code=...)."""
    match = _CODE_RE.search(text or "")
    return match.group(1) if match else None


def checksum(client_id: str, secret_code: str, code: str) -> str:
    return _sha256(f"{client_id}{secret_code}{code}")


def exchange_code(username: str, client_id: str, secret_code: str, code: str) -> dict:
    """Code -> access token (GenAcsTok). Never raises.

    Returns {access_token, account_id, user_id, refresh_token, susertoken} or {error}.
    Must be called from the static IP whitelisted on the Shoonya account.
    """
    jdata = {"code": code, "checksum": checksum(client_id, secret_code, code), "uid": username}
    try:
        res = _post(GEN_ACS_TOK_URL, jdata)
    except Exception as exc:
        log.error(f"Shoonya token exchange request failed: {exc}")
        return {"error": f"request failed: {exc}"}
    if not res.get("access_token"):
        emsg = res.get("emsg") or f"unexpected response: {res}"
        log.error(f"Shoonya token exchange rejected: {emsg}")
        return {"error": emsg}
    return {
        "access_token": res["access_token"],
        "account_id": res.get("actid") or username,
        "user_id": res.get("USERID") or username,
        "refresh_token": res.get("refresh_token"),
        "susertoken": res.get("susertoken"),
    }


def validate_session(username: str, access_token: str) -> tuple[bool, str]:
    """Ask the broker whether a stored access token is still live."""
    try:
        res = _post(USER_DETAILS_URL, {"uid": username}, headers=bearer_headers(access_token))
    except Exception as exc:
        return False, f"request failed: {exc}"
    if res.get("stat") == "Ok":
        return True, "Valid"
    return False, res.get("emsg") or f"unexpected response: {res}"


# ----------------------------------------------------------------- headless browser login
PAGE_INPUTS = "input:not([type='hidden']):not([type='checkbox']):not([type='radio'])"
PASSWORD_INPUT = "input[type='password']"
LOGIN_BUTTON = "//button[normalize-space()='LOGIN']"


def auto_login_available() -> bool:
    """True when selenium is importable (the browser itself is checked at use time)."""
    return importlib.util.find_spec("selenium") is not None


def _chrome_driver():
    """Headless Chrome exactly as Shoonya's documented script configures it."""
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    binary = os.getenv("FDK_CHROME_BINARY", "").strip()
    if not binary and os.path.exists("/usr/bin/chromium"):
        binary = "/usr/bin/chromium"           # python:3.11-slim + apt chromium (Dockerfile)
    if binary:
        options.binary_location = binary
    driver_path = os.getenv("FDK_CHROMEDRIVER", "").strip()
    if not driver_path and os.path.exists("/usr/bin/chromedriver"):
        driver_path = "/usr/bin/chromedriver"
    service = Service(driver_path) if driver_path else None
    return webdriver.Chrome(options=options, service=service)


def _scan_network_for_code(driver, redirect_hint: str = "") -> Optional[str]:
    """The authorization code from the post-login navigation in Chrome's network log.

    Accepts the redirect when it goes to a Shoonya host (the official recipe), to
    the redirect URL registered on the account (redirect_hint), or when the code
    is long enough to be a real OAuth code (so a custom redirect host still works
    while `?code=en`-style asset parameters are ignored).
    """
    try:
        entries = driver.get_log("performance")
    except Exception:
        return None
    for entry in entries:
        try:
            message = json.loads(entry["message"])["message"]
        except Exception:
            continue
        if message.get("method") != "Network.requestWillBeSent":
            continue
        url = message.get("params", {}).get("request", {}).get("url", "")
        if "code=" not in url:
            continue
        code = parse_qs(urlparse(url).query).get("code", [None])[0]
        if not code:
            continue
        if ("shoonya" in url.lower()
                or (redirect_hint and url.startswith(redirect_hint))
                or len(code) >= 20):
            return code
    return None


def _fill(element, value: str) -> None:
    element.click()
    element.clear()
    element.send_keys(value)


def _visible_inputs(driver) -> list:
    """Re-queried every time: the page is a SPA and re-renders after LOGIN."""
    return [el for el in driver.find_elements("css selector", PAGE_INPUTS) if el.is_displayed()]


_CODE_MASK_RE = re.compile(r"(code=)[^&\s#]+")


def _page_diagnostics(driver) -> str:
    """Where the browser ended up and what the page says — for the error message.

    The message reaches the log and /fdk/health, so any code= in it (a one-time
    credential, when the redirect was one the scanner did not accept) is masked;
    the hint then names the setting that makes that redirect acceptable.
    """
    parts = []
    hint = ""
    try:
        url = getattr(driver, "current_url", "") or ""
        if url:
            masked_url = _CODE_MASK_RE.sub(r"\1***", url)
            parts.append(f"page: {masked_url}")
            if masked_url != url:
                hint = ("; the page did redirect with a code the scanner did not accept — "
                        "set FDK_SHOONYA_REDIRECT_URI to that URL's prefix")
    except Exception:
        pass
    try:
        text = re.sub(r"\s+", " ", driver.find_element("tag name", "body").text or "").strip()
        if text:
            masked_text = _CODE_MASK_RE.sub(r"\1***", text[:200])
            parts.append(f"text: {masked_text}")
    except Exception:
        pass
    return f" ({'; '.join(parts)}{hint})" if parts else ""


def auto_login_code(client_id: str, username: str, password: str, totp_secret: str, *,
                    driver_factory: Optional[Callable] = None, timeout: float = 60.0,
                    page_wait: float = 30.0, poll: float = 0.5,
                    totp_now: Optional[Callable[[str], str]] = None,
                    cancelled: Optional[Callable[[], bool]] = None,
                    redirect_hint: str = "",
                    clock: Callable[[], float] = time.monotonic,
                    sleep: Callable[[float], None] = time.sleep) -> dict:
    """Drive Shoonya's login page headlessly and capture the OAuth code.

    Mirrors the official "Python Authentication (Selenium)" script: fill the first
    three visible inputs (user id, password, TOTP), click LOGIN, watch the network
    log for the redirect carrying code=, and resubmit exactly once with a fresh
    TOTP if nothing arrived within `timeout`. Bounded to about 2 x timeout plus the
    page load; `cancelled()` (polled every tick) aborts early. Returns {"code": ...}
    or {"error": ...} — with "fatal": True when there is no browser to drive, so the
    caller should not retry. Never raises.
    """
    totp_now = totp_now or _totp_now
    cancelled = cancelled or (lambda: False)
    try:
        driver = (driver_factory or _chrome_driver)()
    except Exception as exc:
        log.error(f"Shoonya browser login unavailable: {exc}")
        return {"error": f"browser unavailable: {exc}", "fatal": True}
    try:
        set_timeout = getattr(driver, "set_page_load_timeout", None)
        if set_timeout:
            try:
                set_timeout(page_wait)      # Selenium's default is 300 s
            except Exception:
                pass
        driver.get(f"{LOGIN_PAGE_URL}?api_key={client_id}&route_to={username}")

        start = clock()
        while not driver.find_elements("css selector", PASSWORD_INPUT):
            if cancelled():
                return {"error": "cancelled"}
            if clock() - start > page_wait:
                return {"error": "login page did not render a password field"
                                 + _page_diagnostics(driver)}
            sleep(poll)

        inputs = _visible_inputs(driver)
        if len(inputs) < 3:
            return {"error": f"login page layout changed ({len(inputs)} visible inputs, "
                             "expected user id / password / OTP)" + _page_diagnostics(driver)}
        _fill(inputs[0], username)
        _fill(inputs[1], password)
        _fill(inputs[2], totp_now(totp_secret))
        driver.find_element("xpath", LOGIN_BUTTON).click()

        resubmitted = False
        start = clock()
        while True:
            if cancelled():
                return {"error": "cancelled"}
            code = _scan_network_for_code(driver, redirect_hint)
            if code:
                return {"code": code}
            if clock() - start > timeout:
                if resubmitted:
                    return {"error": f"no authorization code within {int(2 * timeout)}s "
                                     "(check user id / password / TOTP secret)"
                                     + _page_diagnostics(driver)}
                inputs = _visible_inputs(driver)        # the page re-rendered: fresh elements
                if len(inputs) < 3:
                    return {"error": "login page moved on without an authorization code"
                                     + _page_diagnostics(driver)}
                log.info("Shoonya browser login: no code yet, resubmitting once with a fresh TOTP")
                _fill(inputs[0], username)        # re-created inputs may be blank: refill all
                _fill(inputs[1], password)
                _fill(inputs[2], totp_now(totp_secret))
                driver.find_element("xpath", LOGIN_BUTTON).click()
                resubmitted = True
                start = clock()
            sleep(poll)
    except Exception as exc:
        log.error(f"Shoonya browser login failed: {exc}")
        return {"error": f"browser login failed: {exc}" + _page_diagnostics(driver)}
    finally:
        try:
            driver.quit()
        except Exception:
            pass
