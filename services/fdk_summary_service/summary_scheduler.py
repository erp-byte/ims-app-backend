"""Scheduler that triggers summary generation at 16:30 IST."""
import asyncio
from datetime import datetime
from shared.fdk_logger import get_logger
from shared.fdk_constants import IST, SUMMARY_SEND_TIME
from .summary_generator import SummaryGenerator

log = get_logger("summary_scheduler")


class SummaryScheduler:
    """Runs summary generators at the configured time (16:30 IST)."""

    def __init__(self, generators: list[SummaryGenerator]):
        self.generators = generators
        self._last_sent_date: str | None = None

    async def _send_all(self):
        """Run every generator, isolating failures.

        One detector's summary must never suppress another's: the generators
        are independent, so a raising fyers generator is logged and the
        fan-out continues to penny. Never propagates — the caller can then
        always mark the day done instead of re-running the survivors every
        60s until midnight.
        """
        for gen in self.generators:
            name = getattr(gen, "name", "?")
            try:
                if not await gen.send_summary():
                    log.warning(f"[{name}] summary reported failure — not sent")
            except Exception as e:
                log.error(f"[{name}] summary raised: {e}", exc_info=True)

    async def run(self):
        """Main loop — checks time every 30s, sends once per day at SUMMARY_SEND_TIME."""
        log.info(f"Summary scheduler started (send time: {SUMMARY_SEND_TIME} IST)")

        while True:
            try:
                now = datetime.now(IST)
                current_date = now.strftime("%d-%m-%Y")
                current_time = now.strftime("%H:%M")

                # Reset for new day
                if self._last_sent_date and self._last_sent_date != current_date:
                    self._last_sent_date = None

                # Send at or after the configured time, once per day. Using >=
                # (not ==) so a missed exact minute — process asleep or event
                # loop briefly blocked at 16:30 — still fires later that evening.
                if current_time >= SUMMARY_SEND_TIME and self._last_sent_date != current_date:
                    log.info(f"Triggering summaries at {current_time}")
                    await self._send_all()
                    self._last_sent_date = current_date

                await asyncio.sleep(30)

            except Exception as e:
                log.error(f"Summary scheduler error: {e}")
                await asyncio.sleep(60)
