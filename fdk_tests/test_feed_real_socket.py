"""The feed over a real socket: the real websocket-client against a local server.

The server speaks the protocol from Shoonya's websocket-overview and subscribe-market-feed
pages, so this checks our use of the library (callback signatures, run_forever arguments,
threads, close) rather than Shoonya itself. Nothing here leaves the machine.
"""
import json
import socket
import threading
import time
from types import SimpleNamespace

import pytest
import websocket
from websockets.sync.server import serve

import services.fdk_detector_service.websocket_manager as wm

# conftest replaces WebSocketApp for each test, so keep the real one from import time.
REAL_APP = websocket.WebSocketApp


class LocalGateway:
    def __init__(self):
        self.received: list[list[dict]] = []    # frames, one list per connection
        self.connections = []
        self.ack_status = "OK"
        self.lock = threading.Lock()
        self.server = serve(self._handle, "127.0.0.1", 0)
        self.url = f"ws://127.0.0.1:{self.server.socket.getsockname()[1]}/"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def _handle(self, conn):
        frames: list[dict] = []
        with self.lock:
            self.received.append(frames)
            self.connections.append(conn)
        for raw in conn:
            frame = json.loads(raw)
            frames.append(frame)
            if frame["t"] == "a":
                conn.send(json.dumps({"t": "ak", "uid": frame["uid"], "s": self.ack_status}))
            elif frame["t"] == "t":
                for scrip in frame["k"].split("#"):
                    conn.send(json.dumps({"t": "tk", "e": "NSE", "tk": scrip.split("|")[1],
                                          "lp": "100.00", "v": "1000"}))

    def push(self, **frame):
        self.connections[-1].send(json.dumps(frame))

    def drop_client(self):
        """An abrupt loss, with no closing handshake, which is how most real drops look."""
        self.connections[-1].socket.shutdown(socket.SHUT_RDWR)

    def stop(self):
        self.server.shutdown()
        self.thread.join(timeout=2)


def wait_for(condition, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.005)
    return condition()


class Detector:
    def __init__(self, *symbols):
        self.config = SimpleNamespace(symbols=list(symbols))
        self.ticks: list[dict] = []
        self.token_expired = False

    def on_tick(self, message):
        self.ticks.append(message)


TOKENS = {f"SYM{n}-EQ": str(n) for n in range(1, 251)}


@pytest.fixture
def gateway(monkeypatch):
    server = LocalGateway()
    monkeypatch.setattr(wm.websocket, "WebSocketApp", REAL_APP)
    monkeypatch.setattr(wm, "WS_URL", server.url)
    monkeypatch.setattr(wm, "SUBSCRIBE_BATCH_DELAY", 0.001)
    monkeypatch.setattr(wm, "RECONNECT_BASE_DELAY", 0.01)
    monkeypatch.setattr(wm, "RECONNECT_MAX_DELAY", 0.02)
    yield server
    server.stop()


@pytest.fixture
def feed(gateway):
    detector = Detector(*TOKENS)
    dispatcher = wm.TickDispatcher("FA12345", "FA12345", "TOKEN", [detector], TOKENS)
    dispatcher.detector = detector
    yield dispatcher
    dispatcher.close()


def test_handshake_batches_and_ticks_over_a_real_socket(gateway, feed):
    feed.connect()

    frames = gateway.received[0]
    assert frames[0] == {"t": "a", "uid": "FA12345", "actid": "FA12345",
                         "source": "API", "accesstoken": "TOKEN"}
    subscribes = [f for f in frames if f["t"] == "t"]
    assert [len(f["k"].split("#")) for f in subscribes] == [100, 100, 50]
    assert feed.coverage() == (250, 250)
    assert feed.is_alive() is True
    assert feed.needs_rebuild() is False

    gateway.push(t="tf", e="NSE", tk="7", lp="101.50")          # no volume: not queued
    gateway.push(t="tf", e="NSE", tk="7", v="5000")
    assert wait_for(lambda: any(t["v"] == "5000" for t in feed.detector.ticks))
    tick = next(t for t in feed.detector.ticks if t["v"] == "5000")
    assert (tick["symbol"], tick["lp"], tick["t"]) == ("SYM7-EQ", "101.50", "tf")


def test_a_real_drop_reconnects_and_resubscribes_everything(gateway, feed):
    feed.connect()
    gateway.drop_client()
    assert wait_for(lambda: len(gateway.received) == 2
                    and len([f for f in gateway.received[1] if f["t"] == "t"]) == 3)
    assert gateway.received[1][0]["t"] == "a"
    assert wait_for(lambda: feed.coverage() == (250, 250))
    assert feed.needs_rebuild() is False


def test_an_idle_socket_outlives_the_socket_timeout(gateway, feed, monkeypatch):
    """The timeout bounds dials and stuck frames; a quiet market must not count as a failure."""
    monkeypatch.setattr(wm, "SOCKET_TIMEOUT", 0.2)
    feed.connect()
    time.sleep(1.0)
    assert feed.is_alive() is True
    assert len(gateway.received) == 1


def test_a_gateway_that_accepts_and_never_answers_is_retried_then_handed_over(monkeypatch):
    """Without a socket timeout this dial blocks for good and nothing ever notices."""
    listener = socket.create_server(("127.0.0.1", 0))
    accepted = []

    def accept_and_stay_silent():
        try:
            while True:
                accepted.append(listener.accept()[0])
        except OSError:
            pass

    threading.Thread(target=accept_and_stay_silent, daemon=True).start()
    monkeypatch.setattr(wm.websocket, "WebSocketApp", REAL_APP)
    monkeypatch.setattr(wm, "WS_URL", f"ws://127.0.0.1:{listener.getsockname()[1]}/")
    monkeypatch.setattr(wm, "SOCKET_TIMEOUT", 0.1)
    monkeypatch.setattr(wm, "SOCKET_OPEN_TIMEOUT", 0.05)
    monkeypatch.setattr(wm, "RECONNECT_RETRY", 2)
    monkeypatch.setattr(wm, "RECONNECT_BASE_DELAY", 0.01)
    monkeypatch.setattr(wm, "RECONNECT_MAX_DELAY", 0.01)
    manager = wm.WebSocketManager("FA12345", "FA12345", "TOKEN", lambda f: None, ["NSE|1"])
    try:
        manager.connect()
        assert manager.is_up() is False
        assert wait_for(manager.needs_rebuild)
        assert len(accepted) == 3
        started = time.monotonic()
        manager.close()
        assert time.monotonic() - started < 2
    finally:
        listener.close()
        for conn in accepted:
            conn.close()


def test_a_real_rejection_flags_the_token_and_stops_dialling(gateway, feed):
    gateway.ack_status = "Not_Ok"
    feed.connect()
    assert feed.detector.token_expired is True
    assert feed.needs_rebuild() is True
    time.sleep(0.1)
    assert len(gateway.received) == 1


def test_close_ends_the_real_socket_and_threads(gateway, feed):
    feed.connect()
    thread = feed._ws._thread
    feed.close()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert feed.is_alive() is False
    time.sleep(0.05)
    assert len(gateway.received) == 1, "close must not leave a reconnect loop behind"
