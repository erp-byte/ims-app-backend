"""Make the repo root importable so tests can import `shared` and `services`."""
import pathlib
import sys

import pytest
import websocket

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def no_real_sockets(monkeypatch):
    """A test that reaches the real feed fails here instead of dialling the gateway."""
    def refuse(*args, **kwargs):
        raise AssertionError("a test tried to open a real WebSocket")
    monkeypatch.setattr(websocket, "WebSocketApp", refuse)
