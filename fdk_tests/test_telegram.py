"""Unit tests for TelegramSender.

Regression cover for the silent-failure hole: a non-200 reply from the
Telegram Bot API (revoked token, wrong chat_id, bot removed from the group,
supergroup migration, HTML parse error) used to be discarded, so a summary
that Telegram rejected looked identical to one that was never generated.
"""
import logging
from types import SimpleNamespace

import pytest

from shared.fdk_config_loader import TelegramChannel
from services.fdk_telegram_service import bot_handler
from services.fdk_telegram_service.bot_handler import TelegramSender


@pytest.fixture
def sender():
    return TelegramSender(TelegramChannel(bot_token="TOKEN", chat_id="CHAT"))


def _resp(status_code: int, text: str = ""):
    return SimpleNamespace(status_code=status_code, text=text)


def test_send_returns_true_on_200(sender, monkeypatch):
    monkeypatch.setattr(bot_handler.requests, "post",
                        lambda *a, **k: _resp(200, '{"ok":true}'))
    assert sender.send("hello") is True


def test_send_returns_false_on_non_200(sender, monkeypatch):
    monkeypatch.setattr(bot_handler.requests, "post",
                        lambda *a, **k: _resp(400, "chat not found"))
    assert sender.send("hello") is False


def test_send_logs_telegram_error_body_on_non_200(sender, monkeypatch, caplog):
    """The whole point: WHY Telegram refused must reach the logs."""
    body = ('{"ok":false,"error_code":400,'
            '"description":"Bad Request: chat not found"}')
    monkeypatch.setattr(bot_handler.requests, "post",
                        lambda *a, **k: _resp(400, body))

    with caplog.at_level(logging.ERROR, logger="fdk.telegram"):
        sender.send("hello")

    logged = " ".join(r.getMessage() for r in caplog.records)
    assert "400" in logged, "status code must be logged"
    assert "chat not found" in logged, "Telegram's description must be logged"
    assert "CHAT" in logged, "the failing chat_id must be logged"


def test_send_logs_exception_with_chat_id(sender, monkeypatch, caplog):
    def _boom(*a, **k):
        raise RuntimeError("connection reset")

    monkeypatch.setattr(bot_handler.requests, "post", _boom)
    with caplog.at_level(logging.ERROR, logger="fdk.telegram"):
        assert sender.send("hello") is False
    logged = " ".join(r.getMessage() for r in caplog.records)
    assert "connection reset" in logged


# --- HTML escaping ----------------------------------------------------------
# Telegram's HTML parse mode requires a bare & to be sent as &amp;. Four symbols (M&M, M&MFIN,
# J&KBANK, COX&KINGS) and sectors such as "Oil & Gas" carry one.

def _alert(symbol="M&M-EQ", sector="Oil & Gas"):
    from datetime import datetime
    from shared.fdk_models import TradeAlert
    return TradeAlert(symbol=symbol, sector=sector, ltp=1500.5, volume_spike=20000,
                      trade_value=30_010_000, trade_value_cr=3.0, spike_type="Large Spike",
                      timestamp=datetime(2026, 11, 3, 10, 0, 0))


def test_trade_alert_escapes_symbol_and_sector_for_telegram_html():
    from services.fdk_telegram_service.message_template import trade_alert_message

    msg = trade_alert_message(_alert())

    assert "<b>Symbol:</b> M&amp;M-EQ" in msg
    assert "<b>Sector:</b> Oil &amp; Gas" in msg
    assert msg.count("&") == msg.count("&amp;")       # no bare ampersand anywhere


def test_summary_escapes_symbols_for_telegram_html(monkeypatch):
    from services.fdk_summary_service.summary_generator import SummaryGenerator

    gen = SummaryGenerator("shoonya", None, "sheet-id", SimpleNamespace())
    monkeypatch.setattr(gen, "_get_records", lambda days_back=0, target_date=None: [
        {"symbol": "M&M-EQ", "value_str": "3.10"}, {"symbol": "M&M-EQ", "value_str": "4.00"}])

    msg = gen._format_message(0, "Daily")

    assert "<b>M&amp;M-EQ</b>" in msg
    assert msg.count("&") == msg.count("&amp;")
