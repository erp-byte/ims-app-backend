"""Unit tests for the Shoonya market feed: handshake, batched subscribe, reconnect, tick merge.

FakeApp stands in for websocket.WebSocketApp and plays the gateway's side of the protocol
documented at shoonya.com/api-documentation/websocket-overview and /subscribe-market-feed,
so the frames asserted here are the frames those pages specify.
"""
import json
import logging
import queue
import threading
import time
from types import SimpleNamespace

import pytest
import websocket

import services.fdk_detector_service.websocket_manager as wm
from services.fdk_auth_service.shoonya_login import WS_URL
from services.fdk_detector_service.detector import VolumeSpikeDetector
from shared.fdk_constants import SUBSCRIBE_BATCH_DELAY, SUBSCRIBE_BATCH_SIZE
from shared.fdk_models import DetectorConfig


class FakeApp:
    """The gateway: acks the connect frame, then one tk per subscribed scrip."""

    instances: list["FakeApp"] = []
    ack_type = "ak"
    ack_status = "OK"      # the vendor SDK compares with "OK"; the docs print "Ok"
    silent = False         # never answers the connect frame
    refuse_next = 0        # the next N attempts die before the socket opens
    max_acks = None        # stops acknowledging scrips after this many, per connection

    @classmethod
    def reset(cls):
        cls.instances = []
        cls.ack_type = "ak"
        cls.ack_status = "OK"
        cls.silent = False
        cls.refuse_next = 0
        cls.max_acks = None

    def __init__(self, url, on_open=None, on_message=None, on_error=None, on_close=None):
        self.url = url
        self.on_open = on_open
        self.on_message = on_message
        self.on_error = on_error
        self.on_close = on_close
        self.sent: list[dict] = []
        self.inbox: queue.Queue = queue.Queue()
        self.run_kwargs = None
        self.sock = SimpleNamespace(connected=False)
        self._closed = threading.Event()
        self._acks = 0
        FakeApp.instances.append(self)

    def run_forever(self, **kwargs):
        self.run_kwargs = kwargs
        if FakeApp.refuse_next > 0:
            FakeApp.refuse_next -= 1
            self.on_error(self, ConnectionError("refused"))
            self.on_close(self, None, None)
            return True
        self.sock.connected = True
        self.on_open(self)
        while not self._closed.is_set():
            try:
                frame = self.inbox.get(timeout=0.002)
            except queue.Empty:
                continue
            self.on_message(self, frame)
        self.sock = None       # as the real app does at teardown
        self.on_close(self, None, None)
        return False

    def send(self, payload):
        if self._closed.is_set():
            raise websocket.WebSocketConnectionClosedException("socket is already closed.")
        frame = json.loads(payload)
        self.sent.append(frame)
        if frame["t"] == "a" and not FakeApp.silent:
            self.push(t=FakeApp.ack_type, uid=frame["uid"], s=FakeApp.ack_status)
        elif frame["t"] == "t":
            for scrip in frame["k"].split("#"):
                if FakeApp.max_acks is not None and self._acks >= FakeApp.max_acks:
                    break
                self._acks += 1
                self.push(t="tk", e="NSE", tk=scrip.split("|")[1], lp="100.00", v="1000")

    def close(self):
        self._closed.set()

    # --- test helpers ------------------------------------------------
    def push(self, **frame):
        self.inbox.put(json.dumps(frame))

    def drop(self):
        self._closed.set()

    @property
    def subscribe_frames(self):
        return [f for f in self.sent if f["t"] == "t"]


def wait_for(condition, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.002)
    return condition()


SCRIPS = [f"NSE|{n}" for n in range(1, 251)]


@pytest.fixture(autouse=True)
def gateway(monkeypatch):
    FakeApp.reset()
    monkeypatch.setattr(wm.websocket, "WebSocketApp", FakeApp)
    monkeypatch.setattr(wm, "SUBSCRIBE_BATCH_DELAY", 0.001)
    monkeypatch.setattr(wm, "RECONNECT_BASE_DELAY", 0.001)
    monkeypatch.setattr(wm, "RECONNECT_MAX_DELAY", 0.002)
    monkeypatch.setattr(wm, "SOCKET_OPEN_TIMEOUT", 2)
    monkeypatch.setattr(wm, "ACK_WAIT", 2)
    return FakeApp


@pytest.fixture
def build():
    made = []

    def _make(scrips=None, on_tick=None, on_auth_error=None, on_coverage=None):
        manager = wm.WebSocketManager("FA12345", "FA12345", "TOKEN", on_tick or (lambda f: None),
                                      list(SCRIPS if scrips is None else scrips),
                                      on_auth_error=on_auth_error, on_coverage=on_coverage)
        made.append(manager)
        return manager

    yield _make
    for manager in made:
        manager.close()


# --- handshake -------------------------------------------------------

def test_connect_frame_is_the_documented_one_and_goes_first(build):
    build().connect()
    app = FakeApp.instances[0]
    assert app.url == WS_URL
    assert app.sent[0] == {"t": "a", "uid": "FA12345", "actid": "FA12345",
                           "source": "API", "accesstoken": "TOKEN"}


def test_heartbeat_matches_the_vendor_sdk(build):
    build().connect()
    assert FakeApp.instances[0].run_kwargs == {"ping_interval": 3, "ping_payload": '{"t":"h"}'}


def test_nothing_is_subscribed_before_the_connect_ack(build, monkeypatch, caplog):
    monkeypatch.setattr(wm, "SOCKET_OPEN_TIMEOUT", 0.05)
    FakeApp.silent = True
    manager = build()
    with caplog.at_level(logging.ERROR, logger="fdk.websocket"):
        manager.connect()
    assert FakeApp.instances[0].subscribe_frames == []
    assert "NOT subscribed" in " ".join(r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("ack_type,status", [("ak", "OK"), ("ak", "Ok"), ("ck", "OK")])
def test_every_documented_spelling_of_the_ack_counts(build, ack_type, status):
    """The overview says ak/Ok, the code-examples page ck/OK, the vendor SDK ak/OK."""
    FakeApp.ack_type, FakeApp.ack_status = ack_type, status
    build().connect()
    assert FakeApp.instances[0].subscribe_frames


def test_rejected_connect_flags_auth_and_stops_retrying(build):
    FakeApp.ack_status = "Not_Ok"
    seen = []
    manager = build(on_auth_error=seen.append)
    manager.connect()
    assert wait_for(lambda: seen)
    assert seen[0]["s"] == "Not_Ok"
    assert manager.rejection_reason == "Not_Ok"
    assert FakeApp.instances[0].subscribe_frames == []
    assert manager.needs_rebuild() is True
    time.sleep(0.03)
    assert len(FakeApp.instances) == 1, "a rejected token must not be retried on a loop"


# --- batched subscribe -----------------------------------------------

def test_scrips_go_out_in_batches_joined_with_hash(build):
    build().connect()
    frames = FakeApp.instances[0].subscribe_frames
    assert [len(f["k"].split("#")) for f in frames] == [100, 100, 50]
    assert "#".join(f["k"] for f in frames) == "#".join(SCRIPS)


def test_batch_limits_are_the_agreed_ones():
    assert 50 <= SUBSCRIBE_BATCH_SIZE <= 100
    assert 0.05 <= SUBSCRIBE_BATCH_DELAY <= 0.1


def test_batches_pause_between_each_other_but_not_after_the_last(build, monkeypatch):
    delays = []
    monkeypatch.setattr(wm, "_pause", delays.append)
    build().connect()
    assert delays == [wm.SUBSCRIBE_BATCH_DELAY] * 2


def test_connect_returns_only_after_every_scrip_is_acknowledged(build):
    manager = build()
    manager.connect()
    assert manager.coverage() == (250, 250)
    assert manager.needs_rebuild() is False


def test_partial_acknowledgement_is_reported_not_retried(build, monkeypatch, caplog):
    """The changelog mentions a 500 tokens/connection limit: a rebuild cannot fix that."""
    monkeypatch.setattr(wm, "ACK_WAIT", 0.05)
    FakeApp.max_acks = 200
    reports = []
    manager = build(on_coverage=lambda *a: reports.append((a[0], a[1], len(a[2]))))
    with caplog.at_level(logging.ERROR, logger="fdk.websocket"):
        manager.connect()
    assert manager.coverage() == (200, 250)
    assert manager.needs_rebuild() is False
    assert reports == [(200, 250, 50)]
    assert "200 of 250" in " ".join(r.getMessage() for r in caplog.records)
    assert len(FakeApp.instances) == 1


def test_full_acknowledgement_reports_no_gap(build):
    reports = []
    build(on_coverage=lambda *a: reports.append(a)).connect()
    assert reports == []


def test_coverage_is_rechecked_after_every_reconnect(build, monkeypatch):
    monkeypatch.setattr(wm, "ACK_WAIT", 0.05)
    reports = []
    manager = build(on_coverage=lambda *a: reports.append((a[0], a[1], len(a[2]))))
    manager.connect()
    assert reports == []
    FakeApp.max_acks = 100
    FakeApp.instances[0].drop()
    assert wait_for(lambda: reports == [(100, 250, 150)])


def test_no_acknowledgement_at_all_needs_a_rebuild(build, monkeypatch):
    monkeypatch.setattr(wm, "ACK_WAIT", 0.02)
    monkeypatch.setattr(wm, "SOCKET_OPEN_TIMEOUT", 0.02)
    FakeApp.max_acks = 0
    manager = build()
    manager.connect()
    assert wait_for(manager.needs_rebuild)


# --- reconnect -------------------------------------------------------

def test_a_drop_reconnects_and_replays_connect_and_every_batch(build):
    build().connect()
    FakeApp.instances[0].drop()
    assert wait_for(lambda: len(FakeApp.instances) == 2
                    and len(FakeApp.instances[1].subscribe_frames) == 3)
    second = FakeApp.instances[1]
    assert second.sent[0]["t"] == "a"
    assert "#".join(f["k"] for f in second.subscribe_frames) == "#".join(SCRIPS)


def test_backoff_doubles_up_to_the_cap(monkeypatch):
    monkeypatch.setattr(wm, "RECONNECT_BASE_DELAY", 1)
    monkeypatch.setattr(wm, "RECONNECT_MAX_DELAY", 60)
    assert [wm._backoff_delay(n) for n in (1, 2, 3, 4, 7, 8)] == [1, 2, 4, 8, 60, 60]


def test_retrying_is_not_yet_a_rebuild_but_giving_up_is(build, monkeypatch):
    monkeypatch.setattr(wm, "RECONNECT_BASE_DELAY", 30)
    monkeypatch.setattr(wm, "RECONNECT_MAX_DELAY", 30)
    manager = build()
    manager.connect()
    FakeApp.instances[0].drop()
    assert wait_for(lambda: not manager.is_alive())
    assert manager.needs_rebuild() is False      # inside the backoff wait
    manager.close()

    monkeypatch.setattr(wm, "RECONNECT_BASE_DELAY", 0.001)
    monkeypatch.setattr(wm, "RECONNECT_MAX_DELAY", 0.001)
    monkeypatch.setattr(wm, "RECONNECT_RETRY", 3)
    FakeApp.instances = []
    FakeApp.refuse_next = 100
    gave_up = build()
    gave_up.connect()
    assert wait_for(gave_up.needs_rebuild)
    time.sleep(0.02)
    assert len(FakeApp.instances) == 4           # the first attempt plus 3 retries


def test_a_stable_connection_earns_a_fresh_retry_budget(build, monkeypatch):
    monkeypatch.setattr(wm, "RECONNECT_RETRY", 2)
    monkeypatch.setattr(wm, "STABLE_CONNECTION_SECONDS", 0)
    manager = build()
    manager.connect()
    for n in range(4):
        FakeApp.instances[n].drop()
        assert wait_for(lambda: len(FakeApp.instances) == n + 2
                        and FakeApp.instances[n + 1].subscribe_frames)
    assert manager.needs_rebuild() is False


def test_attempts_that_never_opened_earn_no_fresh_budget(build, monkeypatch):
    """A black-holed dial can take minutes to fail, longer than any 'stable' threshold."""
    monkeypatch.setattr(wm, "RECONNECT_RETRY", 2)
    monkeypatch.setattr(wm, "STABLE_CONNECTION_SECONDS", 0)
    FakeApp.refuse_next = 100
    manager = build()
    manager.connect()
    assert wait_for(manager.needs_rebuild)
    time.sleep(0.02)
    assert len(FakeApp.instances) == 3


def test_close_wakes_a_connect_that_is_still_waiting(build, monkeypatch, caplog):
    monkeypatch.setattr(wm, "SOCKET_OPEN_TIMEOUT", 30)
    FakeApp.silent = True
    manager = build()
    returned = threading.Event()
    threading.Thread(target=lambda: (manager.connect(), returned.set()), daemon=True).start()
    assert wait_for(lambda: FakeApp.instances and FakeApp.instances[0].sent)
    with caplog.at_level(logging.ERROR, logger="fdk.websocket"):
        manager.close()
        assert returned.wait(2), "connect() kept blocking after close()"
    assert "NOT subscribed" not in " ".join(r.getMessage() for r in caplog.records)


def test_close_during_a_backoff_wait_ends_the_loop(build, monkeypatch):
    monkeypatch.setattr(wm, "RECONNECT_BASE_DELAY", 30)
    monkeypatch.setattr(wm, "RECONNECT_MAX_DELAY", 30)
    manager = build()
    manager.connect()
    thread = manager._thread
    FakeApp.instances[0].drop()
    assert wait_for(lambda: not manager.is_alive())
    manager.close()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert len(FakeApp.instances) == 1


def test_close_closes_the_open_socket(build):
    manager = build()
    manager.connect()
    app = FakeApp.instances[0]
    manager.close()
    assert wait_for(lambda: app.sock is None)
    assert manager.is_alive() is False
    assert manager.needs_rebuild() is True


def test_a_socket_that_opens_after_close_is_closed_straight_away(build):
    """close() racing run_forever: the app's own close is undone when run_forever starts."""
    manager = build()
    manager._stop.set()
    app = FakeApp("url")
    manager._on_open(app)
    assert app._closed.is_set()
    assert app.sent == []


# --- liveness --------------------------------------------------------

def test_never_connected_needs_a_rebuild(build):
    manager = build()
    assert manager.is_alive() is False
    assert manager.needs_rebuild() is True


def test_alive_while_the_socket_is_up(build):
    manager = build()
    manager.connect()
    assert manager.is_alive() is True


def test_is_up_means_an_open_socket_with_acknowledged_scrips(build, monkeypatch):
    monkeypatch.setattr(wm, "RECONNECT_BASE_DELAY", 30)
    monkeypatch.setattr(wm, "RECONNECT_MAX_DELAY", 30)
    manager = build()
    assert manager.is_up() is False
    manager.connect()
    assert manager.is_up() is True
    FakeApp.instances[0].drop()
    assert wait_for(lambda: not manager.is_alive())
    assert manager.is_up() is False


def test_is_up_is_false_for_a_socket_that_acknowledged_nothing(build, monkeypatch):
    monkeypatch.setattr(wm, "ACK_WAIT", 0.02)
    FakeApp.max_acks = 0
    manager = build()
    manager.connect()
    assert manager.is_alive() is True
    assert manager.is_up() is False


def test_a_reconnected_socket_that_stays_quiet_counts_as_stalled(build, monkeypatch):
    monkeypatch.setattr(wm, "_ticks_expected", lambda: True)
    manager = build()
    manager.connect()
    FakeApp.instances[0].push(t="tf", e="NSE", tk="1", v="2000")
    assert wait_for(lambda: manager._last_tick_at is not None)
    FakeApp.instances[0].drop()
    assert wait_for(lambda: len(FakeApp.instances) == 2 and manager.coverage() == (250, 250))
    assert manager.needs_rebuild() is False
    manager._last_tick_at -= wm.FEED_STALL_SECONDS + 1
    assert manager.needs_rebuild() is True


def test_ticks_going_quiet_during_the_session_needs_a_rebuild(build, monkeypatch):
    """A half-open socket can stay 'connected' for minutes while delivering nothing."""
    monkeypatch.setattr(wm, "_ticks_expected", lambda: True)
    manager = build()
    manager.connect()
    FakeApp.instances[0].push(t="tf", e="NSE", tk="1", v="2000")
    assert wait_for(lambda: manager._last_tick_at is not None)
    assert manager.needs_rebuild() is False
    manager._last_tick_at -= wm.FEED_STALL_SECONDS + 1
    assert manager.needs_rebuild() is True


def test_quiet_after_the_close_is_not_a_stall(build, monkeypatch):
    monkeypatch.setattr(wm, "_ticks_expected", lambda: False)
    manager = build()
    manager.connect()
    FakeApp.instances[0].push(t="tf", e="NSE", tk="1", v="2000")
    assert wait_for(lambda: manager._last_tick_at is not None)
    manager._last_tick_at -= wm.FEED_STALL_SECONDS + 1
    assert manager.needs_rebuild() is False


def test_no_ticks_ever_is_not_a_stall(build, monkeypatch):
    """Subscribed outside trading: the snapshots arrive, no diff ever does."""
    monkeypatch.setattr(wm, "_ticks_expected", lambda: True)
    manager = build()
    manager.connect()
    assert manager.needs_rebuild() is False


def test_ticks_expected_only_until_the_close_ist():
    assert wm._ticks_expected("09:15") is True
    assert wm._ticks_expected("15:29") is True
    assert wm._ticks_expected("15:30") is False
    assert wm._ticks_expected("16:00") is False


# --- frames that are not ticks ---------------------------------------

def test_garbage_and_foreign_frames_do_not_reach_the_tick_callback(build):
    ticks = []
    manager = build(scrips=["NSE|1"], on_tick=ticks.append)
    manager.connect()
    app = FakeApp.instances[0]
    assert wait_for(lambda: len(ticks) == 1)        # the tk snapshot
    app.inbox.put("not json")
    app.push(t="om", norenordno="1")
    app.push(t="tf", e="NSE", tk="1", v="2000")
    assert wait_for(lambda: len(ticks) == 2)
    assert ticks[1]["v"] == "2000"


def test_frames_nobody_documented_are_logged_once_so_a_live_run_can_show_them(build, caplog):
    manager = build(scrips=["NSE|1"])
    manager.connect()
    app = FakeApp.instances[0]
    with caplog.at_level(logging.WARNING, logger="fdk.websocket"):
        app.push(stat="Not_Ok", emsg="Rate_Limited: too many requests")
        app.push(stat="Not_Ok", emsg="Rate_Limited: too many requests")
        app.push(t="tf", e="NSE", tk="1", v="2000")
        assert wait_for(lambda: manager._last_tick_at is not None)
    logged = [r for r in caplog.records if "Rate_Limited" in r.getMessage()]
    assert len(logged) == 1


# --- TickDispatcher --------------------------------------------------

class FakeDetector:
    def __init__(self, *symbols):
        self.config = SimpleNamespace(symbols=list(symbols))
        self.ticks: list[dict] = []
        self.token_expired = False

    def on_tick(self, message):
        self.ticks.append(message)


TOKENS = {"TCS-EQ": "11536", "INFY-EQ": "1594", "NTPC-EQ": "11630"}


@pytest.fixture
def dispatch():
    made = []

    def _make(detectors, tokens=None):
        dispatcher = wm.TickDispatcher("FA12345", "FA12345", "TOKEN", detectors, tokens or TOKENS)
        made.append(dispatcher)
        return dispatcher

    yield _make
    for dispatcher in made:
        dispatcher.close()


def test_a_symbol_two_detectors_share_is_subscribed_once(dispatch):
    dispatch([FakeDetector("TCS-EQ", "INFY-EQ"), FakeDetector("TCS-EQ", "NTPC-EQ")]).connect()
    frames = FakeApp.instances[0].subscribe_frames
    scrips = "#".join(f["k"] for f in frames).split("#")
    assert sorted(scrips) == ["NSE|11536", "NSE|11630", "NSE|1594"]


def test_ticks_reach_only_the_detectors_that_own_the_symbol(dispatch):
    shared_a, shared_b = FakeDetector("TCS-EQ", "INFY-EQ"), FakeDetector("TCS-EQ", "NTPC-EQ")
    dispatch([shared_a, shared_b]).connect()
    FakeApp.instances[0].push(t="tf", e="NSE", tk="1594", v="5000")
    assert wait_for(lambda: any(t["v"] == "5000" for t in shared_a.ticks))
    assert all(t["symbol"] != "INFY-EQ" for t in shared_b.ticks)
    assert {t["symbol"] for t in shared_a.ticks} == {"TCS-EQ", "INFY-EQ"}


def test_a_diff_is_merged_onto_the_snapshot(dispatch):
    """tf carries only what changed, so a volume-only diff still needs the last lp."""
    det = FakeDetector("NTPC-EQ")
    dispatch([det]).connect()
    app = FakeApp.instances[0]
    assert wait_for(lambda: det.ticks)
    app.push(t="tf", e="NSE", tk="11630", lp="118.60", ft="1670231260")   # no v: nothing to detect
    app.push(t="tf", e="NSE", tk="11630", v="166637")
    assert wait_for(lambda: len(det.ticks) == 2)
    assert det.ticks[0]["t"] == "tk"
    assert det.ticks[1] == {**det.ticks[1], "t": "tf", "symbol": "NTPC-EQ",
                            "lp": "118.60", "v": "166637"}


def test_a_failing_detector_does_not_cost_the_other_one_the_tick(dispatch):
    class Broken(FakeDetector):
        def on_tick(self, message):
            raise RuntimeError("sheet exploded")

    healthy = FakeDetector("TCS-EQ")
    dispatch([Broken("TCS-EQ"), healthy]).connect()
    assert wait_for(lambda: healthy.ticks)


def test_a_slow_detector_does_not_block_the_read_loop(dispatch):
    """Alerts do blocking HTTP (Sheets, Telegram); the docs ask for this off the read thread."""
    release = threading.Event()

    class Slow(FakeDetector):
        def on_tick(self, message):
            release.wait(timeout=5)
            super().on_tick(message)

    det = Slow("TCS-EQ")
    dispatcher = dispatch([det])
    dispatcher.connect()
    app = FakeApp.instances[0]
    for v in ("2000", "3000", "4000"):
        app.push(t="tf", e="NSE", tk="11536", v=v)
    assert wait_for(app.inbox.empty), "the read loop stalled behind the detector"
    assert det.ticks == []
    release.set()
    assert wait_for(lambda: len(det.ticks) == 4)
    assert det.ticks[-1]["v"] == "4000"


def test_an_overflowing_queue_drops_ticks_and_keeps_running(dispatch, monkeypatch, caplog):
    monkeypatch.setattr(wm, "TICK_QUEUE_SIZE", 1)
    release = threading.Event()

    class Slow(FakeDetector):
        def on_tick(self, message):
            release.wait(timeout=5)
            super().on_tick(message)

    det = Slow("TCS-EQ")
    dispatcher = dispatch([det])
    with caplog.at_level(logging.WARNING, logger="fdk.websocket"):
        dispatcher.connect()
        app = FakeApp.instances[0]
        for v in range(2000, 2010):
            app.push(t="tf", e="NSE", tk="11536", v=str(v))
        assert wait_for(app.inbox.empty)
    assert "queue full" in " ".join(r.getMessage() for r in caplog.records)
    release.set()
    assert wait_for(lambda: not dispatcher._queue.qsize() and det.ticks)
    app.push(t="tf", e="NSE", tk="11536", v="9999")
    assert wait_for(lambda: det.ticks[-1]["v"] == "9999")


def test_a_token_nobody_owns_is_ignored(dispatch):
    det = FakeDetector("TCS-EQ")
    dispatch([det]).connect()
    app = FakeApp.instances[0]
    app.push(t="tf", e="NSE", tk="999999", lp="1", v="1")
    app.push(t="tf", e="NSE", tk="11536", v="7777")
    assert wait_for(lambda: any(t["v"] == "7777" for t in det.ticks))
    assert {t["symbol"] for t in det.ticks} == {"TCS-EQ"}


def test_symbols_without_a_token_are_skipped_and_named(dispatch, caplog):
    with caplog.at_level(logging.ERROR, logger="fdk.websocket"):
        dispatcher = dispatch([FakeDetector("TCS-EQ", "GONE-EQ")])
    dispatcher.connect()
    assert "#".join(f["k"] for f in FakeApp.instances[0].subscribe_frames) == "NSE|11536"
    assert "GONE-EQ" in " ".join(r.getMessage() for r in caplog.records)


def test_a_snapshot_under_another_name_is_flagged(dispatch, caplog):
    """A series move (-EQ to -BE) changes the name while the stored one goes stale."""
    det = FakeDetector("TCS-EQ")
    dispatch([det]).connect()
    with caplog.at_level(logging.WARNING, logger="fdk.websocket"):
        FakeApp.instances[0].push(t="tk", e="NSE", tk="11536", ts="TCS-BE", lp="1", v="1")
        assert wait_for(lambda: "TCS-BE" in " ".join(r.getMessage() for r in caplog.records))


def test_a_rejected_token_marks_every_detector_expired(dispatch):
    FakeApp.ack_status = "Not_Ok"
    first, second = FakeDetector("TCS-EQ"), FakeDetector("INFY-EQ")
    dispatch([first, second]).connect()
    assert wait_for(lambda: first.token_expired and second.token_expired)


class _HeldDetector(VolumeSpikeDetector):
    """The real detector, with its tick handling held back until released."""

    release = None

    def on_tick(self, *args):
        self.release.wait(timeout=5)
        super().on_tick(*args)


def _held_detector(alerts):
    config = DetectorConfig(name="shoonya", threshold=1_000_000, google_sheet_id="S",
                            symbols=["TCS-EQ"], sector_mapping={})
    sender = SimpleNamespace(send=lambda text: True)
    detector = _HeldDetector(config, "TOKEN", SimpleNamespace(add_trade=alerts.append), sender, sender)
    detector.release = threading.Event()
    return detector


def test_ticks_lost_to_a_full_queue_are_not_reported_as_one_big_trade(dispatch, monkeypatch):
    """Each tick is far below the threshold; only their sum, across the gap, is above it."""
    monkeypatch.setattr(wm, "TICK_QUEUE_SIZE", 1)
    alerts = []
    detector = _held_detector(alerts)
    dispatcher = dispatch([detector])
    dispatcher.connect()
    app = FakeApp.instances[0]
    volume = 1000
    for _ in range(400):                         # 1500 shares at Rs 100: Rs 1.5 lakh a tick
        volume += 1500
        app.push(t="tf", e="NSE", tk="11536", v=str(volume))
    assert wait_for(app.inbox.empty)
    assert dispatcher.status()["dropped"] > 0
    detector.release.set()
    assert wait_for(lambda: dispatcher._queue.empty())
    for _ in range(3):                           # ordinary ticks after the gap
        volume += 1500
        app.push(t="tf", e="NSE", tk="11536", v=str(volume))
        assert wait_for(lambda: detector.previous_volumes.get("TCS-EQ") == float(volume))
    assert detector.trades_detected == 0


def test_a_snapshot_lost_to_a_full_queue_cannot_alert_the_outage_volume(dispatch, monkeypatch):
    monkeypatch.setattr(wm, "TICK_QUEUE_SIZE", 1)
    alerts = []
    detector = _held_detector(alerts)
    dispatcher = dispatch([detector])
    dispatcher.connect()
    app = FakeApp.instances[0]
    for v in ("2000", "3000", "4000"):
        app.push(t="tf", e="NSE", tk="11536", v=v)
    app.push(t="tk", e="NSE", tk="11536", lp="100.00", v="5000000")   # the reconnect snapshot, lost
    assert wait_for(app.inbox.empty)
    detector.release.set()
    assert wait_for(lambda: dispatcher._queue.empty())
    app.push(t="tf", e="NSE", tk="11536", v="5000500")
    assert wait_for(lambda: detector.previous_volumes.get("TCS-EQ") == 5000500.0)
    assert detector.trades_detected == 0


def test_a_slow_sheet_write_does_not_cost_any_ticks(dispatch):
    """Alert I/O has its own thread: the tick queue never waits on Google or Telegram."""
    release = threading.Event()
    config = DetectorConfig(name="shoonya", threshold=1_000_000, google_sheet_id="S",
                            symbols=["TCS-EQ", "INFY-EQ"], sector_mapping={})
    sender = SimpleNamespace(send=lambda text: True)
    detector = VolumeSpikeDetector(
        config, "TOKEN", SimpleNamespace(add_trade=lambda alert: release.wait(timeout=5)), sender, sender)
    thread = threading.Thread(target=detector.start, daemon=True)
    thread.start()
    dispatcher = dispatch([detector])
    dispatcher.connect()
    app = FakeApp.instances[0]
    app.push(t="tf", e="NSE", tk="11536", lp="100.00", v="501000")     # a real alert: Rs 5 Cr
    assert wait_for(lambda: detector.trades_detected == 1)
    app.push(t="tf", e="NSE", tk="1594", v="2500")
    assert wait_for(lambda: detector.previous_volumes.get("INFY-EQ") == 2500.0), \
        "ticks stopped behind a stuck sheet write"
    release.set()
    detector.stop()
    thread.join(timeout=2)


def test_missing_symbols_are_named_and_split_by_detector(dispatch, monkeypatch, caplog):
    monkeypatch.setattr(wm, "ACK_WAIT", 0.05)
    FakeApp.max_acks = 1                         # acknowledges TCS only
    dispatcher = dispatch([FakeDetector("TCS-EQ", "INFY-EQ"), FakeDetector("INFY-EQ", "NTPC-EQ")])
    with caplog.at_level(logging.ERROR, logger="fdk.websocket"):
        dispatcher.connect()
    report = dispatcher.take_coverage_report()
    assert report == {"acknowledged": 1, "requested": 3, "missing": ["INFY-EQ", "NTPC-EQ"],
                      "missing_by_detector": [1, 2]}
    assert dispatcher.take_coverage_report() is None, "each report is handed over once"
    logged = " ".join(r.getMessage() for r in caplog.records)
    assert "INFY-EQ" in logged and "NTPC-EQ" in logged


def test_monitored_counts_only_symbols_with_a_token(dispatch):
    detector = FakeDetector("TCS-EQ", "GONE-EQ")
    dispatcher = dispatch([detector])
    assert dispatcher.monitored(detector) == 1
    assert dispatcher.no_token == ["GONE-EQ"]


def test_nothing_to_subscribe_opens_no_socket(dispatch):
    dispatcher = dispatch([FakeDetector("GONE-EQ")], tokens={"OTHER-EQ": "1"})
    dispatcher.connect()
    assert FakeApp.instances == []
    assert dispatcher.is_up() is False
    assert dispatcher.no_token == ["GONE-EQ"]


def test_status_reports_the_feed(dispatch):
    dispatcher = dispatch([FakeDetector("TCS-EQ")])
    assert dispatcher.status()["connected"] is False
    dispatcher.connect()
    FakeApp.instances[0].push(t="tf", e="NSE", tk="11536", v="2000")
    assert wait_for(lambda: dispatcher.status()["seconds_since_tick"] is not None)
    status = dispatcher.status()
    assert (status["connected"], status["acknowledged"], status["requested"], status["dropped"]) == \
           (True, 1, 1, 0)


def test_a_failing_socket_close_still_stops_the_worker(dispatch, monkeypatch):
    dispatcher = dispatch([FakeDetector("TCS-EQ")])
    dispatcher.connect()
    worker, real_close = dispatcher._worker, dispatcher._ws.close

    def boom():
        raise RuntimeError("socket already gone")

    monkeypatch.setattr(dispatcher._ws, "close", boom)
    dispatcher.close()
    worker.join(timeout=2)
    assert not worker.is_alive()
    real_close()


def test_coverage_counts_scrips_not_symbols(dispatch):
    dispatcher = dispatch([FakeDetector("TCS-EQ", "INFY-EQ"), FakeDetector("TCS-EQ")])
    dispatcher.connect()
    assert dispatcher.coverage() == (2, 2)


def test_closing_the_dispatcher_stops_its_threads(dispatch):
    dispatcher = dispatch([FakeDetector("TCS-EQ")])
    dispatcher.connect()
    worker = dispatcher._worker
    dispatcher.close()
    worker.join(timeout=2)
    assert not worker.is_alive()
    dispatcher.close()                   # closing twice is harmless
