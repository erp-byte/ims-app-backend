"""Run controller for a single detector instance."""
import threading
from shared.fdk_logger import get_logger
from services.fdk_detector_service.detector import VolumeSpikeDetector

log = get_logger("run_controller")


class RunController:
    def __init__(self, detector: VolumeSpikeDetector):
        self.detector = detector
        self._thread: threading.Thread | None = None
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self):
        if self._running:
            return
        self.detector.stop_event.clear()
        self._thread = threading.Thread(
            target=self._worker, daemon=True,
            name=f"detector-{self.detector.config.name}"
        )
        self._thread.start()
        self._running = True
        log.info(f"[{self.detector.config.name}] Started")

    def stop(self):
        if not self._running:
            return
        self.detector.stop()
        if self._thread:
            self._thread.join(timeout=10)
        self._running = False
        log.info(f"[{self.detector.config.name}] Stopped")

    def _worker(self):
        try:
            self.detector.start()
        except Exception as e:
            log.error(f"[{self.detector.config.name}] Worker error: {e}")
        finally:
            self._running = False
