"""Shoonya market feed: one socket, batched subscribe, tick diffs merged per token.

Protocol from shoonya.com/api-documentation/websocket-overview and /subscribe-market-feed,
checked against the vendor's NorenRestApiOAuth SDK (0.0.41):
  1. Open the socket and send the connect frame (t: a).
  2. Wait for its ack (t: ak, s: OK) before subscribing; subscribing on a failed
     connect silently produces no data.
  3. Subscribe in batches (t: t, k: NSE|22#NSE|2885...). The gateway answers one
     full snapshot (t: tk) per scrip.
  4. After that it sends diffs (t: tf) carrying only the fields that changed.
Nothing survives a dropped socket, so every reconnect repeats steps 1-3.
"""
import json
import queue
import threading
import time
from datetime import datetime

import websocket

from services.fdk_auth_service.shoonya_login import WS_URL
from shared.fdk_constants import (
    FEED_EXCHANGE, IST, MARKET_CLOSE_TIME, SUBSCRIBE_BATCH_DELAY, SUBSCRIBE_BATCH_SIZE,
)
from shared.fdk_logger import get_logger

log = get_logger("websocket")

# websocket-client waits forever by default, so a gateway that accepts the connection and
# never answers would hang the feed for good. This bounds the dial, the handshake and a frame
# stuck half way. An idle socket is unaffected: reads only start once data is ready.
SOCKET_TIMEOUT = 15

SOCKET_OPEN_TIMEOUT = 30      # max wait for the socket to open and ack the connect frame
ACK_WAIT = 15                 # max wait for one tk per subscribed scrip
CLOSE_TIMEOUT = 5             # max wait for the read loop to end on close()

# What the vendor SDK passes to run_forever: a ping every 3 s carrying its heartbeat frame.
PING_INTERVAL = 3
PING_PAYLOAD = '{"t":"h"}'

# Exponential backoff, as the docs ask for: about 2 minutes of retries (4 if every dial
# times out), then needs_rebuild() hands over to the supervisor, which alerts on Telegram.
RECONNECT_BASE_DELAY = 1
RECONNECT_MAX_DELAY = 30
RECONNECT_RETRY = 8
# A socket that stayed open this long was healthy, so its drop starts a new retry budget.
STABLE_CONNECTION_SECONDS = 60

# A socket can stay "connected" for minutes after the path to the gateway has died.
# With this many symbols a live session always has ticks, so silence means trouble.
FEED_STALL_SECONDS = 120

# The overview page documents ak, its code-examples page ck; the vendor SDK reads ak.
CONNECT_ACK_TYPES = ("ak", "ck")

TICK_QUEUE_SIZE = 5000        # ticks waiting for the detectors before new ones are dropped
DROP_LOG_EVERY = 1000


def _backoff_delay(attempt: int) -> float:
    return min(RECONNECT_MAX_DELAY, RECONNECT_BASE_DELAY * 2 ** (attempt - 1))


def _pause(seconds: float) -> None:
    time.sleep(seconds)


def _ticks_expected(now_hhmm: str | None = None) -> bool:
    """False once trading has closed: the feed is quiet then, and that is not a stall."""
    now_hhmm = now_hhmm or datetime.now(IST).strftime("%H:%M")
    return now_hhmm < MARKET_CLOSE_TIME


class WebSocketManager:
    """Owns the socket: handshake, subscriptions, reconnects and health."""

    def __init__(self, uid: str, actid: str, access_token: str,
                 on_tick, scrips: list[str], on_auth_error=None, on_coverage=None,
                 expect_ticks: bool = False):
        self.uid = uid
        self.actid = actid
        self.access_token = access_token
        self.on_tick = on_tick
        self.scrips = scrips
        self.on_auth_error = on_auth_error
        self.on_coverage = on_coverage        # (acknowledged, requested, missing tokens)
        self.expect_ticks = expect_ticks      # opened mid-session: ticks are due at once
        self.rejection_reason: str | None = None
        self.ws = None
        self.subscribe_count = 0
        self.tick_count = 0                   # tf frames received in this feed's life
        self._tokens = {scrip.split("|", 1)[1] for scrip in scrips}
        self._acked: set[str] = set()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._settled = threading.Event()     # first subscribe round finished, or it never will
        self._all_acked = threading.Event()
        self._opened_at: float | None = None  # None while no socket is open
        self._last_tick_at: float | None = None
        # The stall clock starts with the first tf — or, when ticks are expected from the
        # start (a rebuild during trading), when the socket opens: a replacement socket
        # that never ticks is the outage going on, not a quiet pre-open.
        self._saw_tick = expect_ticks
        self._seen_frames: set = set()

    # -- health -------------------------------------------------------------
    def is_alive(self) -> bool:
        sock = getattr(self.ws, "sock", None)
        return bool(sock is not None and sock.connected)

    def is_up(self) -> bool:
        """An open socket the gateway has acknowledged scrips on: the feed is really running."""
        return self.is_alive() and bool(self._acked)

    def ticks_seen(self) -> bool:
        """A diff (tf) has arrived on this feed: data is actually flowing."""
        return self.tick_count > 0

    def expect_ticks_from_now(self) -> None:
        """Trading is live now: a socket opened earlier (09:13) is held to the stall rule
        from here, so a subscription that never delivers is an outage, not a pre-open."""
        self.expect_ticks = True
        self._saw_tick = True
        if self._last_tick_at is None and self._opened_at is not None:
            self._last_tick_at = time.monotonic()

    def needs_rebuild(self) -> bool:
        """True when only the supervisor can fix this socket.

        Between sockets the answer is False: the reconnect loop owns that window,
        so a brief drop neither tears everything down nor alerts anyone.
        """
        if self._stop.is_set() or self.rejection_reason is not None:
            return True
        if self._thread is None or not self._thread.is_alive():
            return True
        opened_at = self._opened_at
        if opened_at is None:
            return False
        if not self._acked and time.monotonic() - opened_at > SOCKET_OPEN_TIMEOUT:
            return True
        return self._stalled()

    def _stalled(self) -> bool:
        last_tick = self._last_tick_at
        return (last_tick is not None
                and time.monotonic() - last_tick > FEED_STALL_SECONDS
                and _ticks_expected())

    def coverage(self) -> tuple[int, int]:
        """(scrips the gateway acknowledged on this connection, scrips requested)."""
        return len(self._acked), len(self.scrips)

    def seconds_since_tick(self) -> float | None:
        last_tick = self._last_tick_at
        return None if last_tick is None else round(time.monotonic() - last_tick, 1)

    # -- lifecycle ----------------------------------------------------------
    def connect(self):
        """Block until the scrips are subscribed and acknowledged, or the timeouts expire."""
        thread = threading.Thread(target=self._run, name="shoonya-feed", daemon=True)
        thread.start()
        self._thread = thread
        if not self._settled.wait(SOCKET_OPEN_TIMEOUT + ACK_WAIT):
            log.error(f"No connect ack within {SOCKET_OPEN_TIMEOUT + ACK_WAIT}s — "
                      f"NOT subscribed; retrying, then the supervisor rebuilds")

    def close(self):
        self._stop.set()
        self._settled.set()           # wake a connect() that is still waiting
        self._all_acked.set()
        app = self.ws
        if app:
            try:
                app.close()
            except Exception:
                pass
        thread = self._thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=CLOSE_TIMEOUT)
        self.ws = None

    def _run(self):
        attempt = 0
        while not self._stop.is_set():
            websocket.setdefaulttimeout(SOCKET_TIMEOUT)
            app = websocket.WebSocketApp(
                WS_URL, on_open=self._on_open, on_message=self._on_message,
                on_error=self._on_error, on_close=self._on_close,
            )
            self.ws = app
            try:
                app.run_forever(ping_interval=PING_INTERVAL, ping_payload=PING_PAYLOAD)
            except Exception as e:
                log.warning(f"WebSocket loop ended: {e}")
            opened_at, self._opened_at = self._opened_at, None
            if self._stop.is_set() or self.rejection_reason is not None:
                return
            # Time since the socket opened, not since the dial began: a black-holed
            # dial can also last minutes, and must use up the budget.
            if opened_at is not None and time.monotonic() - opened_at >= STABLE_CONNECTION_SECONDS:
                attempt = 0
            attempt += 1
            if attempt > RECONNECT_RETRY:
                log.error(f"WebSocket still down after {RECONNECT_RETRY} retries — "
                          f"handing over to the supervisor")
                self._settled.set()
                return
            delay = _backoff_delay(attempt)
            log.warning(f"WebSocket down — retry {attempt}/{RECONNECT_RETRY} in {delay}s")
            self._stop.wait(delay)

    # -- socket callbacks ---------------------------------------------------
    def _on_open(self, app):
        if self._stop.is_set():      # close() raced this socket opening: run_forever undid it
            app.close()
            return
        self._opened_at = time.monotonic()
        # Once ticks have been flowing, a reconnected socket that stays quiet is a stall too.
        self._last_tick_at = self._opened_at if self._saw_tick else None
        self._acked = set()
        self._all_acked.clear()
        app.send(json.dumps({"t": "a", "uid": self.uid, "actid": self.actid,
                             "source": "API", "accesstoken": self.access_token}))

    def _on_message(self, app, raw):
        try:
            frame = json.loads(raw)
        except (TypeError, ValueError):
            log.warning(f"Unreadable frame from the gateway: {str(raw)[:120]}")
            return
        if not isinstance(frame, dict):
            return
        kind = frame.get("t")
        if kind == "tk" or kind == "tf":
            self._on_tick_frame(kind, frame)
        elif kind in CONNECT_ACK_TYPES:
            self._on_connect_ack(app, frame)
        else:
            self._log_unexpected(frame)

    def _log_unexpected(self, frame: dict):
        """The docs do not say how the gateway refuses a subscription, so show what it sends."""
        key = (frame.get("t"), frame.get("stat"), frame.get("emsg"))
        if key not in self._seen_frames:
            self._seen_frames.add(key)
            log.warning(f"Unexpected frame from the gateway: {str(frame)[:200]}")

    def _on_tick_frame(self, kind: str, frame: dict):
        if kind == "tf":
            self._saw_tick = True
            self.tick_count += 1
            self._last_tick_at = time.monotonic()
        else:
            token = str(frame.get("tk"))
            if token in self._tokens:
                self._acked.add(token)
                if len(self._acked) == len(self._tokens):
                    self._all_acked.set()
        try:
            self.on_tick(frame)
        except Exception as e:
            log.error(f"Tick callback failed: {e}")

    def _on_connect_ack(self, app, frame: dict):
        # Case-insensitive: the docs print "Ok", the vendor SDK compares with "OK".
        if str(frame.get("s", "")).lower() == "ok":
            # Off the read loop: the batches pause, and the acks must keep draining meanwhile.
            threading.Thread(target=self._subscribe, args=(app,),
                             name="shoonya-subscribe", daemon=True).start()
            return
        self.rejection_reason = str(frame.get("emsg") or frame.get("s") or "no reason given")
        log.error(f"Connect rejected by Shoonya: {self.rejection_reason}")
        if self.on_auth_error:
            try:
                self.on_auth_error(frame)
            except Exception as e:
                log.error(f"on_auth_error handler failed: {e}")
        self._settled.set()
        app.close()

    def _subscribe(self, app):
        """Send every scrip in batches, with a short pause so start-up does not flood the gateway."""
        total = len(self.scrips)
        for start in range(0, total, SUBSCRIBE_BATCH_SIZE):
            if self._stop.is_set() or app is not self.ws:
                return
            batch = self.scrips[start:start + SUBSCRIBE_BATCH_SIZE]
            try:
                app.send(json.dumps({"t": "t", "k": "#".join(batch)}))
            except Exception as e:
                log.warning(f"Subscribe interrupted after {start} of {total} scrips: {e}")
                return
            if start + SUBSCRIBE_BATCH_SIZE < total:
                _pause(SUBSCRIBE_BATCH_DELAY)
        self.subscribe_count += 1
        log.info(f"Sent {total} scrips (subscription #{self.subscribe_count})")
        self._all_acked.wait(ACK_WAIT)
        if self._stop.is_set() or app is not self.ws:
            return
        self._report_coverage()
        self._settled.set()

    def _report_coverage(self):
        """Runs after every subscribe round, so a reconnect that comes back short is noticed too."""
        acknowledged, requested = self.coverage()
        missing = self._tokens - self._acked
        if not missing:
            log.info(f"WebSocket connected ({requested} scrips acknowledged)")
            return
        log.error(f"Gateway acknowledged only {acknowledged} of {requested} scrips within {ACK_WAIT}s")
        if self.on_coverage:
            try:
                self.on_coverage(acknowledged, requested, missing)
            except Exception as e:
                log.error(f"on_coverage handler failed: {e}")

    def _on_error(self, app, error):
        log.error(f"WebSocket error: {error}")

    def _on_close(self, app, status_code, message):
        log.warning(f"WebSocket closed: {status_code} {message}")


class TickDispatcher:
    """Single shared feed that routes each tick to the detector(s) owning its symbol.

    The docs allow one connection per session. Diffs are merged per token on the read
    thread, then a worker thread runs the detectors, so a slow detector never blocks
    every other message on the socket.
    """

    def __init__(self, uid: str, actid: str, access_token: str, detectors: list,
                 instrument_tokens: dict[str, str], expect_ticks: bool = False):
        self._detectors = list(detectors)
        self.expect_ticks = expect_ticks
        self._symbol_map: dict[str, list] = {}
        self._token_symbol: dict[str, str] = {}
        self._state: dict[str, dict] = {}
        self._queue: queue.Queue = queue.Queue(maxsize=TICK_QUEUE_SIZE)
        self._gapped: set[str] = set()
        self._dropped = 0
        self._coverage_report: dict | None = None
        self._worker: threading.Thread | None = None
        self._worker_stop = threading.Event()
        self.no_token: list[str] = []

        for det in detectors:
            for sym in det.config.symbols:
                self._symbol_map.setdefault(sym, []).append(det)
                token = instrument_tokens.get(sym)
                if token:
                    self._token_symbol[str(token)] = sym
                elif sym not in self.no_token:
                    self.no_token.append(sym)
        if self.no_token:
            log.error(f"{len(self.no_token)} symbols have no Shoonya token and are NOT "
                      f"subscribed: {self.no_token[:20]}")
        self._subscribed_symbols = set(self._token_symbol.values())

        scrips = [f"{FEED_EXCHANGE}|{token}" for token in self._token_symbol]
        self._ws = WebSocketManager(uid, actid, access_token, self._on_frame, scrips,
                                    on_auth_error=self._on_auth_error,
                                    on_coverage=self._on_coverage, expect_ticks=expect_ticks)
        log.info(f"TickDispatcher: {len(scrips)} scrips, {len(self._detectors)} detectors")

    def _on_frame(self, frame: dict):
        """Runs on the read thread, so it only merges and queues."""
        token = str(frame.get("tk"))
        symbol = self._token_symbol.get(token)
        if not symbol:
            return
        merged = self._state.setdefault(token, {})
        merged.update(frame)
        if frame.get("t") == "tk":
            name = frame.get("ts")
            if name and name != symbol:
                log.warning(f"Token {token} trades as {name} but is stored as {symbol}")
        elif "v" not in frame:
            return                   # volume unchanged: nothing a detector could act on
        tick = {**merged, "symbol": symbol}
        # v is cumulative, so the first tick after a gap would report all the skipped
        # volume as one trade. Sending it as a snapshot makes it a new baseline instead.
        if token in self._gapped:
            tick["t"] = "tk"
        try:
            self._queue.put_nowait(tick)
        except queue.Full:
            self._gapped.add(token)
            if self._dropped % DROP_LOG_EVERY == 0:
                log.warning(f"Tick queue full, dropping ticks ({self._dropped} so far)")
            self._dropped += 1
        else:
            self._gapped.discard(token)

    def _drain(self):
        while not self._worker_stop.is_set():
            try:
                tick = self._queue.get(timeout=0.2)
            except queue.Empty:
                continue
            for det in self._symbol_map.get(tick["symbol"], []):
                try:
                    det.on_tick(tick)
                except Exception as e:
                    log.error(f"Tick handler failed for {tick['symbol']}: {e}")

    def _on_auth_error(self, message):
        """Mark every detector's token expired so the supervisor re-authenticates."""
        for det in self._detectors:
            det.token_expired = True

    def _on_coverage(self, acknowledged: int, requested: int, missing_tokens: set[str]):
        missing = sorted(self._token_symbol[token] for token in missing_tokens)
        log.error(f"NOT monitored, the gateway did not acknowledge these {len(missing)} "
                  f"symbols: {missing}")
        self._coverage_report = {
            "acknowledged": acknowledged, "requested": requested, "missing": missing,
            "missing_by_detector": [sum(1 for sym in det.config.symbols if sym in set(missing))
                                    for det in self._detectors],
        }

    def take_coverage_report(self) -> dict | None:
        """The latest gap in acknowledgements, handed over once."""
        report, self._coverage_report = self._coverage_report, None
        return report

    def monitored(self, detector) -> int:
        """How many of this detector's symbols are actually subscribed."""
        return sum(1 for sym in detector.config.symbols if sym in self._subscribed_symbols)

    @property
    def rejection_reason(self) -> str | None:
        return self._ws.rejection_reason

    def is_alive(self) -> bool:
        return self._ws.is_alive()

    def is_up(self) -> bool:
        return self._ws.is_up()

    def ticks_seen(self) -> bool:
        return self._ws.ticks_seen()

    def ready(self) -> bool:
        """Up — and, when ticks are due, actually ticking: safe to announce as monitoring."""
        return self.is_up() and (not self.expect_ticks or self.ticks_seen())

    def expect_ticks_from_now(self) -> None:
        self.expect_ticks = True
        self._ws.expect_ticks_from_now()

    def needs_rebuild(self) -> bool:
        return self._ws.needs_rebuild()

    def coverage(self) -> tuple[int, int]:
        return self._ws.coverage()

    def status(self) -> dict:
        acknowledged, requested = self._ws.coverage()
        return {"connected": self._ws.is_up(), "acknowledged": acknowledged,
                "requested": requested, "no_token": len(self.no_token),
                "ticks": self._ws.tick_count,
                "seconds_since_tick": self._ws.seconds_since_tick(), "dropped": self._dropped}

    def connect(self):
        if not self._token_symbol:
            log.error("No symbol has a Shoonya token, so there is nothing to subscribe")
            return
        self._worker = threading.Thread(target=self._drain, name="shoonya-ticks", daemon=True)
        self._worker.start()
        self._ws.connect()

    def close(self):
        try:
            self._ws.close()
        except Exception as e:
            log.error(f"Closing the socket failed: {e}")
        finally:
            self._worker_stop.set()
            worker = self._worker
            if worker and worker is not threading.current_thread():
                worker.join(timeout=CLOSE_TIMEOUT)
