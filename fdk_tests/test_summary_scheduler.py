"""Unit tests for SummaryScheduler fan-out.

The scheduler drives [fyers, penny] in order. Regression cover for two
coupling bugs: a raising generator used to abort the whole for-loop (so a
fyers failure silently suppressed the penny summary too) and left
_last_sent_date unset, re-running the survivors every 60s until midnight.
"""
import asyncio
import logging

from services.fdk_summary_service.summary_scheduler import SummaryScheduler


class _Gen:
    """Minimal stand-in for SummaryGenerator."""

    def __init__(self, name, *, raises=False, result=True):
        self.name = name
        self._raises = raises
        self._result = result

    async def send_summary(self):
        _CALLS.append(self.name)
        if self._raises:
            raise RuntimeError(f"{self.name} exploded")
        return self._result


_CALLS: list[str] = []


def _run(generators):
    _CALLS.clear()
    scheduler = SummaryScheduler(generators)
    asyncio.run(scheduler._send_all())
    return list(_CALLS)


def test_all_generators_run_when_all_succeed():
    assert _run([_Gen("fyers"), _Gen("penny")]) == ["fyers", "penny"]


def test_raising_generator_does_not_block_the_next_one():
    """A fyers explosion must not cost the penny summary."""
    assert _run([_Gen("fyers", raises=True), _Gen("penny")]) == ["fyers", "penny"]


def test_send_all_never_propagates(caplog):
    with caplog.at_level(logging.ERROR, logger="fdk.summary_scheduler"):
        _run([_Gen("fyers", raises=True), _Gen("penny")])
    logged = " ".join(r.getMessage() for r in caplog.records)
    assert "fyers" in logged and "exploded" in logged


def test_generator_returning_false_is_logged(caplog):
    with caplog.at_level(logging.WARNING, logger="fdk.summary_scheduler"):
        _run([_Gen("fyers", result=False), _Gen("penny")])
    logged = " ".join(r.getMessage() for r in caplog.records)
    assert "fyers" in logged
