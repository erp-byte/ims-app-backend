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


def _scan_network_for_code(driver) -> Optional[str]:
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
        if "code=" in url and "shoonya" in url.lower():
            code = parse_qs(urlparse(url).query).get("code", [None])[0]
            if code:
                return code
    return None


def _fill(element, value: str) -> None:
    element.click()
    element.clear()
    element.send_keys(value)


def auto_login_code(client_id: str, username: str, password: str, totp_secret: str, *,
                    driver_factory: Optional[Callable] = None, timeout: float = 60.0,
                    page_wait: float = 30.0, poll: float = 0.5,
                    totp_now: Optional[Callable[[str], str]] = None,
                    clock: Callable[[], float] = time.monotonic,
                    sleep: Callable[[float], None] = time.sleep) -> dict:
    """Drive Shoonya's login page headlessly and capture the OAuth code.

    Mirrors the official "Python Authentication (Selenium)" script: fill the first
    three visible inputs (user id, password, TOTP), click LOGIN, watch the network
    log for the redirect carrying code=, and resubmit once with a fresh TOTP if the
    30 s window rolled over. Returns {"code": ...} or {"error": ...}; never raises.
    """
    totp_now = totp_now or _totp_now
    try:
        driver = (driver_factory or _chrome_driver)()
    except Exception as exc:
        log.error(f"Shoonya browser login unavailable: {exc}")
        return {"error": f"browser unavailable: {exc}"}
    try:
        driver.get(f"{LOGIN_PAGE_URL}?api_key={client_id}&route_to={username}")

        start = clock()
        while not driver.find_elements("css selector", PASSWORD_INPUT):
            if clock() - start > page_wait:
                return {"error": "login page did not render a password field"}
            sleep(poll)

        inputs = [el for el in driver.find_elements("css selector", PAGE_INPUTS)
                  if el.is_displayed()]
        if len(inputs) < 3:
            return {"error": f"login page layout changed ({len(inputs)} visible inputs, "
                             "expected user id / password / OTP)"}
        _fill(inputs[0], username)
        _fill(inputs[1], password)
        otp = totp_now(totp_secret)
        _fill(inputs[2], otp)
        driver.find_element("xpath", LOGIN_BUTTON).click()

        start = clock()
        while True:
            code = _scan_network_for_code(driver)
            if code:
                return {"code": code}
            if clock() - start > timeout:
                fresh = totp_now(totp_secret)
                if fresh == otp:
                    return {"error": f"no authorization code within {int(timeout)}s "
                                     "(check user id / password / TOTP secret)"}
                log.info("Shoonya browser login: TOTP window rolled over, resubmitting once")
                _fill(inputs[2], fresh)
                driver.find_element("xpath", LOGIN_BUTTON).click()
                otp = fresh
                start = clock()
            sleep(poll)
    except Exception as exc:
        log.error(f"Shoonya browser login failed: {exc}")
        return {"error": f"browser login failed: {exc}"}
    finally:
        try:
            driver.quit()
        except Exception:
            pass
