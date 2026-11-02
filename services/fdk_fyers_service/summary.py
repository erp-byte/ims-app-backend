"""Fyers summary service — isolated from penny."""
from datetime import datetime
from shared.fdk_logger import get_logger
from services.fdk_summary_service import SummaryGenerator
from services.fdk_telegram_service import TelegramSender

log = get_logger("fyers_summary")


class FyersSummaryService:
    """Encapsulates fyers summary generation and sending."""

    def __init__(self, google_credentials: dict | None,
                 sheet_id: str, summary_sender: TelegramSender):
        self.generator = SummaryGenerator(
            "shoonya", google_credentials, sheet_id, summary_sender,
        )

    async def send_today(self) -> bool:
        """Generate and send today's summary."""
        return await self.generator.send_summary()

    async def send_for_date(self, target_date: datetime) -> bool:
        """Generate and send summary for a specific date."""
        return await self.generator.send_summary_for_date(target_date)
