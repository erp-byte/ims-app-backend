"""Telegram long polling on the login bot — inbound commands without a public HTTPS URL."""
import asyncio
import json

import pytest
import requests

from services.fdk_telegram_service.poller import TelegramPoller


class _Resp:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


def _drive(poller, until, limit=200):
    async def run():
        task = asyncio.create_task(poller.run())
        for _ in range(limit):
            if until():
                break
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())


def test_poller_removes_the_webhook_then_dispatches_messages_in_order(monkeypatch):
    gets, posts, handled = [], [], []
    batches = [[{"update_id": 10, "message": {"text": "/hld holiday"}},
                {"update_id": 11, "message": {"text": "https://x/?code=ABC"}},
                {"update_id": 12, "edited_message": {"text": "ignored"}}]]

    def _get(url, params=None, timeout=None, **kw):
        gets.append((url, dict(params or {})))
        return _Resp({"ok": True, "result": batches.pop(0) if batches else []})

    def _post(url, *a, **kw):
        posts.append(url)
        return _Resp({"ok": True, "result": True})

    monkeypatch.setattr(requests, "get", _get)
    monkeypatch.setattr(requests, "post", _post)

    async def handler(text):
        handled.append(text)

    poller = TelegramPoller("tok-login", handler, timeout=0, idle_pause=0)
    _drive(poller, lambda: len(gets) >= 3)

    assert posts[0].endswith("/bottok-login/deleteWebhook")      # polling and webhooks are exclusive
    assert handled == ["/hld holiday", "https://x/?code=ABC"]    # edited_message skipped
    assert gets[0][0].endswith("/bottok-login/getUpdates")
    assert "offset" not in gets[0][1]
    assert gets[1][1]["offset"] == 13                            # last update_id + 1
    assert gets[1][1]["timeout"] == 0


def test_poller_only_listens_to_the_login_chat(monkeypatch, caplog):
    import logging
    handled, gets = [], []
    batches = [[{"update_id": 1, "message": {"chat": {"id": 111}, "text": "/hld from the login chat"}},
                {"update_id": 2, "message": {"chat": {"id": 999}, "text": "/hld from a stranger"}},
                {"update_id": 3, "message": {"chat": {"id": 111},
                                             "text": "https://x/?code=SECRETCODE&x=1"}}]]

    def _get(url, params=None, timeout=None, **kw):
        gets.append(1)
        return _Resp({"ok": True, "result": batches.pop(0) if batches else []})

    monkeypatch.setattr(requests, "get", _get)
    monkeypatch.setattr(requests, "post", lambda *a, **k: _Resp({"ok": True}))

    async def handler(text):
        handled.append(text)
        if "code=" in text:
            raise RuntimeError("boom")

    poller = TelegramPoller("tok", handler, chat_id="111", timeout=0, idle_pause=0)
    with caplog.at_level(logging.ERROR, logger="fdk.telegram_poller"):
        _drive(poller, lambda: len(gets) >= 3)

    assert handled == ["/hld from the login chat", "https://x/?code=SECRETCODE&x=1"]
    logged = " ".join(r.getMessage() for r in caplog.records)
    assert "SECRETCODE" not in logged and "code=***" in logged     # one-time code never logged


def test_poller_survives_network_errors_and_handler_errors(monkeypatch):
    calls = {"get": 0, "handled": 0}

    def _get(url, params=None, timeout=None, **kw):
        calls["get"] += 1
        if calls["get"] == 1:
            raise requests.ConnectionError("telegram down")
        if calls["get"] == 2:
            return _Resp({"ok": True, "result": [{"update_id": 1, "message": {"text": "/snd"}},
                                                  {"update_id": 2, "message": {"text": "/rst"}}]})
        return _Resp({"ok": True, "result": []})

    monkeypatch.setattr(requests, "get", _get)
    monkeypatch.setattr(requests, "post", lambda *a, **k: _Resp({"ok": True}))

    async def handler(text):
        calls["handled"] += 1
        if text == "/snd":
            raise RuntimeError("summary exploded")

    poller = TelegramPoller("tok", handler, timeout=0, idle_pause=0, error_backoff=0)
    _drive(poller, lambda: calls["get"] >= 4)

    assert calls["handled"] == 2            # the raising handler did not stop the second message
    assert calls["get"] >= 4                # polling continued after the network error
