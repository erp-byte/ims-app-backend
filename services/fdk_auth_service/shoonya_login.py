"""Headless Shoonya (Finvasia) login over the broker's REST API.

Two form posts: QuickAuth (built exactly as the vendor's NorenRestApiPy client
builds it) to log in, and UserDetails (from the Shoonya REST docs; the vendor
client has no such call) to validate a stored session — with explicit timeouts
and the broker's error message surfaced instead of swallowed. There is
no OAuth/redirect step: user id + password + TOTP + vendor code + API key + IMEI
go straight to the broker and a day session token comes back.

Standalone module (stdlib + requests + pyotp only) so tests can load it by path
without importing the package __init__ (which pulls in psycopg2 via the token manager).
"""
import hashlib
import json
import logging
import re
import time
from typing import Callable, Optional

import pyotp
import requests

log = logging.getLogger("fdk.shoonya_login")

SHOONYA_HOST = "https://api.shoonya.com/NorenWClientTP"
LOGIN_URL = f"{SHOONYA_HOST}/QuickAuth"
USER_DETAILS_URL = f"{SHOONYA_HOST}/UserDetails"
HTTP_TIMEOUT = 15


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _totp_now(secret: str) -> str:
    return pyotp.TOTP(secret).now()


def _snippet(text: str) -> str:
    """First words of a non-JSON reply (HTML tags stripped) for the error message."""
    plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text or "")).strip()
    return plain[:120] if plain else "(empty body)"


def _post(url: str, jdata: dict, jkey: Optional[str] = None) -> dict:
    """The broker's form convention: jData=<json>[&jKey=<session token>]."""
    payload = "jData=" + json.dumps(jdata)
    if jkey:
        payload += f"&jKey={jkey}"
    resp = requests.post(url, data=payload, timeout=HTTP_TIMEOUT)
    try:
        return json.loads(resp.text)
    except ValueError:
        # A WAF block, a dead upstream (502/503) or a wrong URL answers with HTML
        # or nothing; say so instead of leaking a JSON-decoder message.
        raise RuntimeError(
            f"HTTP {resp.status_code} non-JSON reply from the broker: {_snippet(resp.text)}"
        ) from None


def login(creds: dict) -> dict:
    """One login attempt.

    creds: username, password, totp_secret, vendor_code, api_key, imei.
    Returns {access_token, account_id, user_name, exchanges} on success, else {error}.
    Never raises.
    """
    uid = creds["username"]
    try:
        jdata = {
            "source": "API",
            "apkversion": "1.0.0",
            "uid": uid,
            "pwd": _sha256(creds["password"]),
            "factor2": _totp_now(creds["totp_secret"]),
            "vc": creds["vendor_code"],
            "appkey": _sha256(f"{uid}|{creds['api_key']}"),
            "imei": creds.get("imei") or "abc1234",
        }
    except Exception as exc:                      # malformed TOTP secret (not base32), missing key
        log.error(f"Shoonya login could not build the request: {exc}")
        return {"error": f"TOTP/credentials error: {exc}"}
    try:
        res = _post(LOGIN_URL, jdata)
    except Exception as exc:                      # network, timeout, non-JSON body
        log.error(f"Shoonya login request failed: {exc}")
        return {"error": f"request failed: {exc}"}
    if res.get("stat") != "Ok" or not res.get("susertoken"):
        emsg = res.get("emsg") or f"unexpected response: {res}"
        log.error(f"Shoonya login rejected: {emsg}")
        return {"error": emsg}
    return {
        "access_token": res["susertoken"],
        "account_id": res.get("actid") or uid,
        "user_name": res.get("uname", ""),
        "exchanges": res.get("exarr", []),
    }


def validate_session(username: str, token: str) -> tuple[bool, str]:
    """Ask the broker whether a stored session token is still live."""
    try:
        res = _post(USER_DETAILS_URL, {"uid": username}, jkey=token)
    except Exception as exc:
        return False, f"request failed: {exc}"
    if res.get("stat") == "Ok":
        return True, "Valid"
    return False, res.get("emsg") or f"unexpected response: {res}"


def login_with_retry(creds: dict, max_attempts: int = 5, delay: int = 60,
                     should_cancel: Optional[Callable[[], bool]] = None) -> dict:
    """Re-run login() up to max_attempts, sleeping `delay`s between tries.

    should_cancel() is polled before each attempt and every second of the
    inter-attempt wait (what /hld and /rst use). Returns the last login() result,
    or {"error": "cancelled"}.
    """
    max_attempts = max(1, int(max_attempts))
    last: dict = {"error": "no attempt made"}
    for attempt in range(1, max_attempts + 1):
        if should_cancel and should_cancel():
            log.info("Shoonya login cancelled before attempt %d", attempt)
            return {"error": "cancelled"}
        last = login(creds)
        if last.get("access_token"):
            if attempt > 1:
                log.info(f"Shoonya login succeeded on attempt {attempt}/{max_attempts}")
            return last
        if attempt < max_attempts:
            log.warning(f"Shoonya login attempt {attempt}/{max_attempts} failed "
                        f"({last.get('error')}); retrying in {delay}s")
            for _ in range(int(delay)):
                if should_cancel and should_cancel():
                    log.info("Shoonya login cancelled during retry wait")
                    return {"error": "cancelled"}
                time.sleep(1)
    log.error(f"Shoonya login failed after {max_attempts} attempt(s): {last.get('error')}")
    return last
