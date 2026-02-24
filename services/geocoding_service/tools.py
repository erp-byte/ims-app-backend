import requests

from shared.config_loader import settings
from shared.logger import get_logger

logger = get_logger("geocoding.tools")

_BASE_URL = "https://us1.locationiq.com/v1/reverse"

_registry = {}


def mcp_tool(name: str = None, description: str = None):
    """Decorator to register functions as discoverable tools."""
    def decorator(func):
        tool_name = name or func.__name__
        _registry[tool_name] = {
            "handler": func,
            "description": description or func.__doc__,
        }
        func._tool_name = tool_name
        return func
    return decorator


def get_tools() -> dict:
    return _registry


@mcp_tool(name="reverse_geocode", description="Convert lat/lng to address using LocationIQ API")
def reverse_geocode(latitude: float, longitude: float) -> str:
    """Call LocationIQ reverse geocoding API and return the display name."""
    try:
        response = requests.get(
            _BASE_URL,
            params={
                "key": settings.LOCATIONIQ_API_KEY,
                "lat": latitude,
                "lon": longitude,
                "format": "json",
            },
            timeout=5,
        )
        response.raise_for_status()
        data = response.json()

        display_name = data.get("display_name")
        if display_name:
            return display_name

        logger.warning(f"No display_name from reverse geocode for ({latitude}, {longitude})")
        return "Unknown location"

    except Exception as e:
        logger.error(f"Reverse geocoding failed for ({latitude}, {longitude}): {e}")
        return "Unresolved"
