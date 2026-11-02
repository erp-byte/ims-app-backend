"""Telegram service data models."""
from pydantic import BaseModel


class TelegramMessage(BaseModel):
    chat_id: str
    text: str
    parse_mode: str = "HTML"
