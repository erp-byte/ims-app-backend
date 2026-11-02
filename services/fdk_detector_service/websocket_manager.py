"""Fyers WebSocket connection management."""
import time
from fyers_apiv3.FyersWebsocket import data_ws
from shared.fdk_logger import get_logger

log = get_logger("websocket")

# The SDK's __on_open REBINDS its outbound queue (self.message = []) and only
# then sets __ws_object. Anything queued before that moment is discarded, and
# __send_message drops sends while __ws_object is None. So we must not
# subscribe on a timer — we wait for the socket to actually be open, then let
# the SDK's own auth + mode messages settle ahead of our subscription.
SOCKET_OPEN_TIMEOUT = 30      # max wait for the socket to come up
SOCKET_POLL_INTERVAL = 0.25   # how often to check is_connected()
SUBSCRIBE_SETTLE = 2          # let auth + mode be queued/sent first

# The SDK caps reconnect attempts at min(50, reconnect_retry) and its default
# is 5 — too few to survive a rough patch on Render's network.
RECONNECT_RETRY = 50

# Auth failures the SDK reports through on_error (FyersWebsocket/defines.py).
# On these it never creates a socket at all, so the only cure is a fresh token.
AUTH_ERROR_TYPE = "cn"
AUTH_ERROR_CODES = (-99, -300)   # TOKEN_EXPIRED, INVALID_CODE


class WebSocketManager:
    def __init__(self, client_id: str, access_token: str,
                 on_message_callback, symbols: list[str],
                 on_auth_error=None):
        self.client_id = client_id
        self.access_token = access_token
        self.on_message = on_message_callback
        self.symbols = symbols
        self.on_auth_error = on_auth_error
        self.ws = None
        self.subscribe_count = 0
        self._subscribed_ok = False

    def is_alive(self) -> bool:
        """True only if the underlying socket is genuinely connected.

        The SDK's is_connected() merely checks that its __ws_object exists,
        so it keeps returning True after reconnection has been abandoned.
        The real test is the one the SDK's own ping loop uses: sock.connected.
        """
        if self.ws is None:
            return False
        try:
            ws_obj = getattr(self.ws, "_FyersDataSocket__ws_object", None)
            sock = getattr(ws_obj, "sock", None)
            return bool(sock is not None and sock.connected)
        except Exception:
            return False

    def needs_rebuild(self) -> bool:
        """True when the socket is dead, or came up without a subscription.

        Nothing in the SDK tells us it has given up: once the retry budget is
        exhausted it only prints 'Connection abandoned', and the on_close
        callback fires solely for a deliberate close. Polling this is the
        only way to notice.
        """
        return not self.is_alive() or not self._subscribed_ok

    def _wait_until_open(self) -> bool:
        """Block until the SDK reports an open socket, or the timeout expires.

        is_connected() flips True at the instant __on_open assigns
        __ws_object — which happens *after* it rebinds the outbound queue.
        Subscribing any earlier means the message is appended to the old list
        and thrown away.
        """
        deadline = time.monotonic() + SOCKET_OPEN_TIMEOUT
        while time.monotonic() < deadline:
            try:
                if self.ws is not None and self.ws.is_connected():
                    return True
            except Exception:  # SDK internals mid-reconnect
                pass
            time.sleep(SOCKET_POLL_INTERVAL)
        return False

    def _subscribe(self):
        """(Re)send the full subscription list."""
        self.ws.subscribe(symbols=self.symbols, data_type="SymbolUpdate")
        self.subscribe_count += 1
        log.info(f"Subscribed to {len(self.symbols)} symbols "
                 f"(subscription #{self.subscribe_count})")

    def _on_connect(self):
        """Subscribe on every open — including reconnects.

        This is the whole ballgame. The SDK's __on_close clears
        scrips_per_channel and symbol_token before reconnecting, and its
        reconnect path never re-subscribes. Without re-subscribing here the
        socket comes back alive and authenticated but delivers no ticks, so
        detection silently dies for the rest of the process's life.
        """
        self._subscribed_ok = False
        if not self._wait_until_open():
            log.error(f"Socket did not open within {SOCKET_OPEN_TIMEOUT}s — "
                      f"NOT subscribed; supervisor will rebuild")
            return
        time.sleep(SUBSCRIBE_SETTLE)
        self._subscribe()
        self._subscribed_ok = True

    @staticmethod
    def _is_auth_error(message) -> bool:
        if not isinstance(message, dict):
            return False
        return (message.get("type") == AUTH_ERROR_TYPE
                or message.get("code") in AUTH_ERROR_CODES)

    def _on_error(self, message):
        log.error(f"WebSocket error: {message}")
        if self._is_auth_error(message) and self.on_auth_error:
            log.error("Auth rejected by Fyers — flagging token for refresh")
            try:
                self.on_auth_error(message)
            except Exception as e:
                log.error(f"on_auth_error handler failed: {e}")

    def _on_close(self, message):
        # Only fires on a deliberate close; drops go through the SDK's
        # internal reconnect path without notifying us at all.
        log.warning(f"WebSocket closed: {message}")

    def connect(self):
        # Reset singleton so a fresh instance is created
        data_ws.FyersDataSocket._instance = None
        self._subscribed_ok = False
        self.ws = data_ws.FyersDataSocket(
            access_token=f"{self.client_id}:{self.access_token}",
            log_path="",
            litemode=False,
            write_to_file=False,
            reconnect=True,
            reconnect_retry=RECONNECT_RETRY,
            on_message=self.on_message,
            on_connect=self._on_connect,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        # connect() invokes the SDK's on_open -> our _on_connect -> subscribe,
        # on the first connection and on every reconnect alike.
        self.ws.connect()
        log.info(f"WebSocket connected ({len(self.symbols)} symbols)")

    def close(self):
        if self.ws:
            # Stop the SDK's reconnect loop FIRST. close_connection() is
            # guarded on __ws_object, so closing during a reconnect window
            # does nothing — leaving restart_flag True and the old instance
            # reconnecting forever as a zombie holding stale callbacks.
            try:
                self.ws.restart_flag = False
            except Exception:
                pass
            try:
                self.ws.close_connection()
            except Exception:
                pass
            finally:
                data_ws.FyersDataSocket._instance = None
                self.ws = None
                self._subscribed_ok = False


class TickDispatcher:
    """Single shared WebSocket that routes ticks to the correct detector(s).

    FyersDataSocket is a singleton — only one WS connection per process.
    This dispatcher subscribes to all symbols and forwards each tick
    to the detector(s) that own that symbol.
    """

    def __init__(self, client_id: str, access_token: str, detectors: list):
        self._symbol_map: dict[str, list] = {}
        self._detectors = list(detectors)
        all_symbols: list[str] = []

        for det in detectors:
            for sym in det.config.symbols:
                self._symbol_map.setdefault(sym, []).append(det)
                if sym not in all_symbols:
                    all_symbols.append(sym)

        self._ws = WebSocketManager(
            client_id, access_token, self._on_tick, all_symbols,
            on_auth_error=self._on_auth_error,
        )
        log.info(f"TickDispatcher: {len(all_symbols)} symbols, "
                 f"{len(detectors)} detectors")

    def _on_tick(self, *args):
        message = args[-1] if args else None
        if not isinstance(message, dict):
            return
        symbol = message.get("symbol")
        if symbol:
            for det in self._symbol_map.get(symbol, []):
                det.on_tick(*args)

    def _on_auth_error(self, message):
        """Mark every detector's token expired so the supervisor re-auths.

        This is what makes the documented reactive-refresh path real —
        nothing else in the system ever set token_expired.
        """
        for det in self._detectors:
            det.token_expired = True

    def is_alive(self) -> bool:
        return self._ws.is_alive()

    def needs_rebuild(self) -> bool:
        return self._ws.needs_rebuild()

    def connect(self):
        self._ws.connect()

    def close(self):
        self._ws.close()
