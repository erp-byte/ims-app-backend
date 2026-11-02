"""Fyers large-cap detector service — isolated from penny."""
from shared.fdk_logger import get_logger
from shared.fdk_models import DetectorConfig
from shared.fdk_constants import FYERS_TRADE_THRESHOLD
from services.fdk_detector_service.detector import VolumeSpikeDetector
from services.fdk_sheets_service import GoogleSheetsManager
from services.fdk_telegram_service import TelegramSender
from services.fdk_supervisor_service.run_controller import RunController

log = get_logger("fyers_service")


class FyersService:
    """Encapsulates the fyers detector lifecycle."""

    def __init__(self, sheet_id: str,
                 google_credentials: dict,
                 trade_sender: TelegramSender,
                 summary_sender: TelegramSender):
        self.sheet_id = sheet_id
        self.google_credentials = google_credentials
        self.trade_sender = trade_sender
        self.summary_sender = summary_sender
        self._detector: VolumeSpikeDetector | None = None
        self._controller: RunController | None = None

    def build(self, access_token: str, symbols: list[str],
              sector_map: dict[str, str]):
        """Build the detector and controller. Call after auth."""
        dc = DetectorConfig(
            name="shoonya",
            threshold=FYERS_TRADE_THRESHOLD,
            google_sheet_id=self.sheet_id,
            symbols=symbols,
            sector_mapping=sector_map,
        )
        sheets = GoogleSheetsManager(self.google_credentials, self.sheet_id)
        self._detector = VolumeSpikeDetector(
            dc, access_token,
            sheets, self.trade_sender, self.summary_sender,
        )
        self._controller = RunController(self._detector)
        log.info(f"Fyers service built ({len(symbols)} symbols, "
                 f"threshold Rs{FYERS_TRADE_THRESHOLD / 10_000_000:.1f} Cr)")

    def start(self):
        if self._controller and not self._controller.is_running:
            self._controller.start()

    def stop(self):
        if self._controller and self._controller.is_running:
            self._controller.stop()

    @property
    def detector(self) -> VolumeSpikeDetector | None:
        return self._detector

    @property
    def is_running(self) -> bool:
        return self._controller.is_running if self._controller else False

    @property
    def token_expired(self) -> bool:
        return self._detector.token_expired if self._detector else False

    def update_token(self, access_token: str):
        """Update the access token after re-authentication."""
        if self._detector:
            self._detector.access_token = access_token
            self._detector.token_expired = False
