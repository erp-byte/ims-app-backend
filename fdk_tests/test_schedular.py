"""Unit tests for supervisor scheduler helpers.

schedular.py is loaded via importlib file-load because its package __init__
imports the orchestrator and everything behind it.
"""
import importlib.util
import pathlib

MODULE_PATH = (
    pathlib.Path(__file__).resolve().parents[1]
    / "services" / "fdk_supervisor_service" / "schedular.py"
)


def _load():
    spec = importlib.util.spec_from_file_location("schedular_under_test", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


sched = _load()


def test_should_reset_before_reset_time_is_false():
    assert sched.should_reset_tokens("16:59", None, "23-07-2026") is False


def test_should_reset_at_reset_time_is_true():
    assert sched.should_reset_tokens("17:00", None, "23-07-2026") is True


def test_should_reset_after_reset_time_is_true():
    assert sched.should_reset_tokens("20:30", "22-07-2026", "23-07-2026") is True


def test_should_not_reset_twice_same_day():
    assert sched.should_reset_tokens("17:05", "23-07-2026", "23-07-2026") is False


def test_should_not_reset_in_the_morning():
    assert sched.should_reset_tokens("09:13", None, "23-07-2026") is False


# --- when the feed must be delivering ticks ---------------------------

def test_ticks_are_due_between_the_first_trades_and_the_close():
    # 09:13 (the login) and 09:15 (the first trades) are too early to call a feed stalled
    assert sched.feed_should_be_live("09:13") is False
    assert sched.feed_should_be_live("09:15") is False
    assert sched.feed_should_be_live("09:16") is True
    assert sched.feed_should_be_live("12:00") is True
    assert sched.feed_should_be_live("15:29") is True
    assert sched.feed_should_be_live("15:30") is False
    assert sched.feed_should_be_live("16:00") is False


# --- websocket rebuild gate -------------------------------------------

def test_no_rebuild_when_socket_is_healthy():
    assert sched.should_rebuild_socket(False, None, 1000.0) is False


def test_rebuild_immediately_on_first_unhealthy_check():
    assert sched.should_rebuild_socket(True, None, 1000.0) is True


def test_no_rebuild_inside_cooldown():
    """A hard-down socket must not be rebuilt (and alerted) every 5s."""
    assert sched.should_rebuild_socket(True, 1000.0, 1030.0, cooldown=60) is False


def test_rebuild_again_once_cooldown_elapsed():
    assert sched.should_rebuild_socket(True, 1000.0, 1060.0, cooldown=60) is True
