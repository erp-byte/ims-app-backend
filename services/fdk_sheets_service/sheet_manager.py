"""Google Sheets connection and write operations."""
import threading
import gspread
from google.oauth2.service_account import Credentials
from shared.fdk_logger import get_logger
from shared.fdk_constants import SHEET_HEADERS, SHEETS_TIMEOUT
from shared.fdk_models import TradeAlert
from .row_builder import build_row, row_to_list

log = get_logger("sheets")


class GoogleSheetsManager:
    def __init__(self, google_credentials: dict | None, sheet_id: str):
        self.credentials = google_credentials
        self.sheet_id = sheet_id
        self.worksheet = None
        self.lock = threading.Lock()
        self.initialized = self._initialize()

    def _initialize(self) -> bool:
        if not self.credentials:
            log.warning("No Google credentials available")
            return False
        try:
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
            creds = Credentials.from_service_account_info(
                self.credentials, scopes=scope
            )
            gc = gspread.authorize(creds)
            gc.set_timeout(SHEETS_TIMEOUT)
            sheet = gc.open_by_key(self.sheet_id)
            self.worksheet = sheet.sheet1
            headers = self.worksheet.row_values(1)
            if not headers or len(headers) < len(SHEET_HEADERS):
                self.worksheet.insert_row(SHEET_HEADERS, 1)
            log.info(f"Connected to Google Sheet: {self.sheet_id}")
            return True
        except Exception as e:
            log.error(f"Google Sheets init failed: {e}")
            return False

    def add_trade(self, alert: TradeAlert) -> bool:
        if not self.worksheet:
            return False
        try:
            row = build_row(alert)
            with self.lock:
                self.worksheet.append_row(row_to_list(row))
            log.info(f"Sheet logged: {alert.symbol} Rs{alert.trade_value_cr:.2f} Cr")
            return True
        except Exception as e:
            log.error(f"Sheet write error: {e}")
            return False
