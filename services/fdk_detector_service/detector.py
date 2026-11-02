"""Volume Spike Detector -- parameterized for the shoonya and penny symbol lists."""
import queue
import time
import threading
from shared.fdk_models import DetectorConfig
from shared.fdk_logger import get_logger
from services.fdk_sheets_service import GoogleSheetsManager
from services.fdk_telegram_service import TelegramSender
from services.fdk_telegram_service.message_template import trade_alert_message
from .tick_handler import parse_tick
from .trade_analyzer import analyze_trade

log = get_logger("detector")

ALERT_QUEUE_SIZE = 500        # alerts waiting for Sheets and Telegram before new ones are lost


class VolumeSpikeDetector:
    def __init__(self, detector_config: DetectorConfig,
                 access_token: str,
                 sheets_manager: GoogleSheetsManager,
                 trade_sender: TelegramSender,
                 summary_sender: TelegramSender):
        self.config = detector_config
        self.access_token = access_token
        self.sheets = sheets_manager
        self.trade_sender = trade_sender
        self.summary_sender = summary_sender
        self.stop_event = threading.Event()
        self._alerts: queue.Queue = queue.Queue(maxsize=ALERT_QUEUE_SIZE)
        self.token_expired = False  # set True when the Shoonya feed rejects the token

        # Tick state
        self.previous_volumes: dict[str, float] = {}
        self.previous_ltp: dict[str, float | None] = {}
        self.last_alert_time: dict[str, float] = {}
        self.total_ticks = 0
        self.trades_detected = 0

    def on_tick(self, *args):
        """WebSocket tick callback (called by TickDispatcher)."""
        message = args[-1] if args else None
        if not isinstance(message, dict):
            return
        tick = parse_tick(message)
        if not tick:
            return
        self.total_ticks += 1
        self._process_tick(tick)

    def _process_tick(self, tick):
        symbol = tick.symbol
        prev_vol = self.previous_volumes.get(symbol, tick.vol_traded_today)
        self.previous_volumes[symbol] = tick.vol_traded_today
        self.previous_ltp[symbol] = tick.ltp

        # A snapshot arrives on subscribe and after every reconnect. It is a new
        # baseline: its jump from the last value is outage volume, not one trade.
        if tick.type == "tk":
            return

        alert = analyze_trade(
            symbol, tick.ltp, tick.vol_traded_today,
            prev_vol, self.config.threshold
        )
        if not alert:
            return

        # Throttle: 1 alert per symbol per 60s
        last = self.last_alert_time.get(symbol, 0)
        if time.time() - last <= 60:
            return
        self.last_alert_time[symbol] = time.time()
        self.trades_detected += 1

        log.info(f"[{self.config.name}] TRADE: {symbol} Rs{alert.trade_value_cr:.2f} Cr")
        # Sheets and Telegram are blocking HTTP, and every tick for every symbol comes
        # through here, so they run on this detector's own thread (see start()).
        try:
            self._alerts.put_nowait(alert)
        except queue.Full:
            log.error(f"[{self.config.name}] Alert queue full, alert LOST: "
                      f"{symbol} Rs{alert.trade_value_cr:.2f} Cr")

    def _deliver(self, alert):
        try:
            self.sheets.add_trade(alert)
            self.trade_sender.send(trade_alert_message(alert))
        except Exception as e:
            log.error(f"[{self.config.name}] Alert delivery failed for {alert.symbol}: {e}")

    def start(self):
        """Deliver alerts until stop_event is set. Ticks arrive via TickDispatcher."""
        log.info(f"[{self.config.name}] Monitoring started "
                 f"({len(self.config.symbols)} symbols, "
                 f"threshold Rs{self.config.threshold/10_000_000:.1f} Cr)")

        while not self.stop_event.is_set():
            try:
                alert = self._alerts.get(timeout=1)
            except queue.Empty:
                continue
            self._deliver(alert)

        while True:                   # anything already detected still goes out
            try:
                alert = self._alerts.get_nowait()
            except queue.Empty:
                break
            self._deliver(alert)

        log.info(f"[{self.config.name}] Monitoring stopped "
                 f"(ticks={self.total_ticks}, trades={self.trades_detected})")

    def stop(self):
        self.stop_event.set()
