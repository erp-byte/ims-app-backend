"""Unit tests for WebSocketManager subscription + liveness lifecycle.

Covers the mid-morning data stop and the four residual failure modes that
could reproduce it by other routes:

1. subscription lost on reconnect (SDK __on_close wipes scrips_per_channel
   and symbol_token; its reconnect path never re-subscribes)
2. the __on_open queue-rebind race (self.message = [] discards anything
   queued before the socket is actually open)
3. retry exhaustion / dead socket — is_connected() still reports True, so
   liveness must be taken from sock.connected
4. auth failure mid-session, and the zombie-socket leak on close
"""
import logging

import pytest

import services.fdk_detector_service.websocket_manager as wm


class _Inner:
    def __init__(self, connected=True):
        self.connected = connected


class _WsObj:
    def __init__(self, connected=True):
        self.sock = _Inner(connected)


class FakeSocket:
    """Stands in for FyersDataSocket, mirroring connect() -> OnOpen."""

    _instance = None
    last: "FakeSocket | None" = None
    open_after_polls = 0
    never_opens = False

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.subscribed: list[list[str]] = []
        self.poll_count = 0
        self.restart_flag = True
        self.close_calls = 0
        self._ws_present = True
        setattr(self, "_FyersDataSocket__ws_object", _WsObj(True))
        FakeSocket.last = self

    # --- SDK surface -------------------------------------------------
    def is_connected(self):
        if FakeSocket.never_opens:
            return False
        self.poll_count += 1
        return self.poll_count > FakeSocket.open_after_polls

    def connect(self):
        cb = self.kwargs.get("on_connect")
        if cb:
            cb()

    def subscribe(self, symbols, data_type="SymbolUpdate"):
        self.subscribed.append(list(symbols))

    def close_connection(self):
        # mirrors the SDK: guarded on __ws_object, so it no-ops mid-reconnect
        if self._ws_present:
            self.close_calls += 1

    # --- test helpers ------------------------------------------------
    def kill_socket(self):
        getattr(self, "_FyersDataSocket__ws_object").sock.connected = False

    def enter_reconnect_window(self):
        self._ws_present = False


SYMBOLS = ["NSE:RELIANCE-EQ", "NSE:BSE-EQ", "NSE:IDEA-EQ"]


@pytest.fixture
def manager(monkeypatch):
    monkeypatch.setattr(wm.data_ws, "FyersDataSocket", FakeSocket)
    monkeypatch.setattr(wm.time, "sleep", lambda *_a, **_k: None)
    FakeSocket.last = None
    FakeSocket.open_after_polls = 0
    FakeSocket.never_opens = False
    return wm.WebSocketManager("CID", "TOKEN", lambda *a: None, list(SYMBOLS))


# --- 1. subscription lifecycle ---------------------------------------

def test_initial_connect_subscribes_all_symbols(manager):
    manager.connect()
    assert FakeSocket.last.subscribed == [SYMBOLS]


def test_connect_registers_all_callbacks(manager):
    manager.connect()
    kw = FakeSocket.last.kwargs
    assert kw.get("on_connect") is not None
    assert kw.get("on_error") is not None
    assert kw.get("on_close") is not None


def test_reconnect_resubscribes_every_symbol(manager):
    manager.connect()
    FakeSocket.last.kwargs["on_connect"]()   # SDK reconnects after a drop
    assert FakeSocket.last.subscribed == [SYMBOLS, SYMBOLS]


# --- 2. the __on_open rebind race ------------------------------------

def test_waits_for_socket_to_open_before_subscribing(manager):
    FakeSocket.open_after_polls = 4
    manager.connect()
    assert FakeSocket.last.subscribed == [SYMBOLS]
    assert FakeSocket.last.poll_count > 4


def test_does_not_subscribe_if_socket_never_opens(manager, monkeypatch, caplog):
    monkeypatch.setattr(wm, "SOCKET_OPEN_TIMEOUT", 0.01)
    monkeypatch.setattr(wm, "SOCKET_POLL_INTERVAL", 0.001)
    FakeSocket.never_opens = True
    with caplog.at_level(logging.ERROR, logger="fdk.websocket"):
        manager.connect()
    assert FakeSocket.last.subscribed == []
    assert "NOT subscribed" in " ".join(r.getMessage() for r in caplog.records)


# --- 3. liveness / retry exhaustion ----------------------------------

def test_is_alive_true_after_successful_connect(manager):
    manager.connect()
    assert manager.is_alive() is True
    assert manager.needs_rebuild() is False


def test_is_alive_false_when_underlying_socket_dropped(manager):
    """is_connected() still says True here — sock.connected is the truth."""
    manager.connect()
    FakeSocket.last.kill_socket()
    assert FakeSocket.last.is_connected() is True   # SDK's misleading answer
    assert manager.is_alive() is False
    assert manager.needs_rebuild() is True


def test_needs_rebuild_when_subscription_never_landed(manager, monkeypatch):
    monkeypatch.setattr(wm, "SOCKET_OPEN_TIMEOUT", 0.01)
    monkeypatch.setattr(wm, "SOCKET_POLL_INTERVAL", 0.001)
    FakeSocket.never_opens = True
    manager.connect()
    assert manager.needs_rebuild() is True


def test_needs_rebuild_when_never_connected(manager):
    assert manager.needs_rebuild() is True


# --- 4. auth errors and the zombie-socket leak -----------------------

def test_auth_error_invokes_callback(manager):
    seen = []
    manager.on_auth_error = seen.append
    manager.connect()
    manager._on_error({"type": "cn", "code": -99, "message": "Token is expired"})
    assert len(seen) == 1


def test_non_auth_error_does_not_invoke_callback(manager):
    seen = []
    manager.on_auth_error = seen.append
    manager.connect()
    manager._on_error({"code": -42, "message": "some transient blip"})
    assert seen == []


def test_close_stops_reconnect_loop_even_when_close_connection_noops(manager):
    """The zombie leak: close_connection() is guarded on __ws_object, so
    closing mid-reconnect left restart_flag True and the old instance
    reconnecting forever with stale detector callbacks."""
    manager.connect()
    sock = FakeSocket.last
    sock.enter_reconnect_window()      # __ws_object is None right now
    manager.close()
    assert sock.close_calls == 0, "SDK guard means close_connection did nothing"
    assert sock.restart_flag is False, "reconnect loop must still be stopped"
    assert manager.ws is None
