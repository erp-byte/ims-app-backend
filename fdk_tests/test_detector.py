"""Volume-spike detection fed with Shoonya tick messages (merged tk/tf frames)."""
import logging
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from services.fdk_detector_service import detector as detector_module
from services.fdk_detector_service.detector import VolumeSpikeDetector
from services.fdk_detector_service.tick_handler import parse_tick
from shared.fdk_constants import SHEET_HEADERS, SHEETS_TIMEOUT
from shared.fdk_models import DetectorConfig


def wait_for(condition, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.002)
    return condition()


@pytest.fixture
def alerts():
    return []


def _build(alerts, symbols=("TCS-EQ",)):
    config = DetectorConfig(name="shoonya", threshold=1_000_000, google_sheet_id="S",
                            symbols=list(symbols), sector_mapping={})
    sheets = SimpleNamespace(add_trade=alerts.append)
    sender = SimpleNamespace(send=lambda text: True)
    return VolumeSpikeDetector(config, "TOKEN", sheets, sender, sender)


@pytest.fixture
def detector(alerts):
    """A running detector: start() is the thread that delivers its alerts."""
    built = _build(alerts)
    thread = threading.Thread(target=built.start, daemon=True)
    thread.start()
    yield built
    built.stop()
    thread.join(timeout=2)


def _tick(kind, lp, v, symbol="TCS-EQ"):
    return {"t": kind, "e": "NSE", "tk": "11536", "symbol": symbol, "lp": lp, "v": v}


# --- parse_tick ------------------------------------------------------

def test_parse_reads_shoonya_fields_sent_as_strings():
    tick = parse_tick(_tick("tf", "118.45", "166637"))
    assert (tick.symbol, tick.ltp, tick.vol_traded_today, tick.type) == \
           ("TCS-EQ", 118.45, 166637.0, "tf")


@pytest.mark.parametrize("message", [
    {"t": "tf", "symbol": "TCS-EQ", "v": "10"},                 # no price yet
    {"t": "tf", "symbol": "TCS-EQ", "lp": "10"},                # no volume yet
    {"t": "tf", "lp": "10", "v": "10"},                         # no symbol
    _tick("tf", "0.00", "10"),
    _tick("tf", "10", "0"),
    _tick("tf", "n/a", "10"),
    _tick("tf", None, "10"),
])
def test_parse_rejects_what_cannot_be_a_trade(message):
    assert parse_tick(message) is None


@pytest.mark.parametrize("kind", ["ak", "uk", "om", None])
def test_parse_only_accepts_tick_frames(kind):
    assert parse_tick(_tick(kind, "10", "10")) is None


# --- detection -------------------------------------------------------

def test_the_first_snapshot_is_a_baseline_not_a_trade(detector, alerts):
    detector.on_tick(_tick("tk", "100.00", "980515"))
    assert detector.trades_detected == 0
    assert detector.previous_volumes["TCS-EQ"] == 980515.0


def test_a_diff_above_the_threshold_alerts_with_the_traded_value(detector, alerts):
    detector.on_tick(_tick("tk", "100.00", "1000"))
    detector.on_tick(_tick("tf", "100.00", "101000"))
    assert wait_for(lambda: alerts)
    assert [a.symbol for a in alerts] == ["TCS-EQ"]
    assert alerts[0].volume_spike == 100_000
    assert alerts[0].trade_value == 10_000_000.0


def test_a_diff_below_the_threshold_stays_quiet(detector, alerts):
    detector.on_tick(_tick("tk", "100.00", "1000"))
    detector.on_tick(_tick("tf", "100.00", "3000"))
    assert detector.previous_volumes["TCS-EQ"] == 3000.0
    assert detector.trades_detected == 0


def test_volume_accrued_during_an_outage_is_not_reported_as_one_trade(detector, alerts):
    """After a reconnect the gateway resends tk; the gap is hours of volume, not a spike."""
    detector.on_tick(_tick("tk", "100.00", "1000"))
    detector.on_tick(_tick("tk", "100.00", "5000000"))
    assert detector.trades_detected == 0
    detector.on_tick(_tick("tf", "100.00", "5000500"))
    assert detector.trades_detected == 0   # measured from the new baseline, not the old one


def test_the_first_diff_after_a_baseless_start_is_a_baseline_too(detector, alerts):
    """A snapshot sent before the open has no v, so parse_tick drops it."""
    detector.on_tick({"t": "tk", "symbol": "TCS-EQ", "lp": "100.00"})
    detector.on_tick(_tick("tf", "100.00", "9000000"))
    assert detector.trades_detected == 0
    assert detector.previous_volumes["TCS-EQ"] == 9000000.0


def test_alerts_are_throttled_per_symbol(detector, alerts):
    detector.on_tick(_tick("tk", "100.00", "1000"))
    detector.on_tick(_tick("tf", "100.00", "101000"))
    detector.on_tick(_tick("tf", "100.00", "201000"))
    assert detector.trades_detected == 1
    assert wait_for(lambda: len(alerts) == 1)


def test_a_snapshot_saying_nothing_has_traded_yet_is_a_baseline_of_zero(detector, alerts):
    """Otherwise the first trade of the day becomes the baseline and is never evaluated."""
    detector.on_tick(_tick("tk", "0.00", "0"))
    detector.on_tick(_tick("tf", "100.00", "100000"))
    assert wait_for(lambda: alerts)
    assert alerts[0].volume_spike == 100_000


def test_a_snapshot_with_no_volume_field_is_still_not_treated_as_zero(detector, alerts):
    """The docs say any field may be missing, so absent is not the same as none traded."""
    detector.on_tick({"t": "tk", "symbol": "TCS-EQ", "lp": "100.00"})
    detector.on_tick(_tick("tf", "100.00", "100000"))
    assert detector.trades_detected == 0


# --- alert delivery --------------------------------------------------

def test_a_hung_sheet_write_does_not_hold_up_tick_processing():
    release = threading.Event()
    config = DetectorConfig(name="shoonya", threshold=1_000_000, google_sheet_id="S",
                            symbols=["TCS-EQ"], sector_mapping={})
    sender = SimpleNamespace(send=lambda text: True)
    hung = VolumeSpikeDetector(config, "TOKEN",
                               SimpleNamespace(add_trade=lambda a: release.wait(timeout=5)), sender, sender)
    thread = threading.Thread(target=hung.start, daemon=True)
    thread.start()
    try:
        hung.on_tick(_tick("tk", "100.00", "1000"))
        hung.on_tick(_tick("tf", "100.00", "101000"))
        ticking = threading.Thread(target=hung.on_tick, args=(_tick("tf", "100.00", "101500"),), daemon=True)
        ticking.start()
        ticking.join(timeout=1)
        assert not ticking.is_alive(), "on_tick blocked behind the sheet write"
        assert hung.previous_volumes["TCS-EQ"] == 101500.0
    finally:
        release.set()
        hung.stop()
        thread.join(timeout=2)


def test_alerts_raised_before_start_go_out_once_it_runs(alerts):
    """Ticks can reach a detector while the supervisor is still waiting on connect()."""
    early = _build(alerts)
    early.on_tick(_tick("tk", "100.00", "1000"))
    early.on_tick(_tick("tf", "100.00", "101000"))
    assert alerts == []
    thread = threading.Thread(target=early.start, daemon=True)
    thread.start()
    assert wait_for(lambda: len(alerts) == 1)
    early.stop()
    thread.join(timeout=2)


def test_queued_alerts_still_go_out_when_stopping(alerts):
    stopping = _build(alerts)
    stopping.on_tick(_tick("tk", "100.00", "1000"))
    stopping.on_tick(_tick("tf", "100.00", "101000"))
    stopping.stop()
    stopping.start()                     # returns at once, after draining
    assert len(alerts) == 1


def test_a_full_alert_queue_loses_the_alert_loudly(alerts, monkeypatch, caplog):
    monkeypatch.setattr(detector_module, "ALERT_QUEUE_SIZE", 1)
    full = _build(alerts, symbols=("AAA-EQ", "BBB-EQ"))
    with caplog.at_level(logging.ERROR, logger="fdk.detector"):
        for symbol in ("AAA-EQ", "BBB-EQ"):
            full.on_tick(_tick("tk", "100.00", "1000", symbol))
            full.on_tick(_tick("tf", "100.00", "101000", symbol))
    assert "BBB-EQ" in " ".join(r.getMessage() for r in caplog.records)


def test_a_delivery_failure_does_not_end_the_alert_thread(alerts):
    config = DetectorConfig(name="shoonya", threshold=1_000_000, google_sheet_id="S",
                            symbols=["AAA-EQ", "BBB-EQ"], sector_mapping={})

    def add_trade(alert):
        if alert.symbol == "AAA-EQ":
            raise RuntimeError("sheet exploded")
        alerts.append(alert)

    sender = SimpleNamespace(send=lambda text: True)
    flaky = VolumeSpikeDetector(config, "TOKEN", SimpleNamespace(add_trade=add_trade), sender, sender)
    thread = threading.Thread(target=flaky.start, daemon=True)
    thread.start()
    for symbol in ("AAA-EQ", "BBB-EQ"):
        flaky.on_tick(_tick("tk", "100.00", "1000", symbol))
        flaky.on_tick(_tick("tf", "100.00", "101000", symbol))
    assert wait_for(lambda: [a.symbol for a in alerts] == ["BBB-EQ"])
    flaky.stop()
    thread.join(timeout=2)


def test_sheet_writes_have_a_timeout(monkeypatch):
    """gspread waits forever by default, and one stuck write would silence that detector's alerts."""
    from services.fdk_sheets_service import sheet_manager
    client = MagicMock()
    client.open_by_key.return_value.sheet1.row_values.return_value = list(SHEET_HEADERS)
    monkeypatch.setattr(sheet_manager.gspread, "authorize", lambda creds: client)
    monkeypatch.setattr(sheet_manager.Credentials, "from_service_account_info", lambda *a, **k: object())
    sheet_manager.GoogleSheetsManager({"fake": "creds"}, "SHEET")
    client.set_timeout.assert_called_once_with(SHEETS_TIMEOUT)


# --- summaries across the rename -------------------------------------

def test_summaries_count_old_and_new_names_as_one_stock():
    from datetime import datetime

    from services.fdk_summary_service.summary_generator import SummaryGenerator
    from shared.fdk_constants import IST

    today = datetime.now(IST).strftime("%d-%m-%Y")
    generator = SummaryGenerator("shoonya", None, "", SimpleNamespace(send=lambda text: True))
    generator.worksheet = SimpleNamespace(get_all_values=lambda: [
        ["Date", "Symbol", "Trd_Val_Cr"],
        [today, "NSE:TCS-EQ", "3.5"],
        [today, "TCS-EQ", "4.5"],
    ])
    records = generator._get_records(0, None)
    assert [r["symbol"] for r in records] == ["TCS-EQ", "TCS-EQ"]
