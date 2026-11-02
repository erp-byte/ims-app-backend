"""Supervisor models."""
from enum import Enum


class DetectorState(str, Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    ERROR = "error"
