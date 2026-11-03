"""MCP server (Streamable HTTP) — news tools + detector tools.

Mounted into the Candor backend under /fdk; reachable at /fdk/mcp (or
/fdk/mcp-<secret> when FDK_MCP_PATH_SECRET is set). Add it in Claude as a
custom connector: https://<backend-host>/fdk/mcp
"""
import os

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from shared.fdk_logger import get_logger
from .news_tools import register_news_tools
from .detector_tools import register_detector_tools

log = get_logger("mcp_server")


def mcp_path() -> str:
    """Path where the MCP endpoint is served."""
    secret = os.getenv("FDK_MCP_PATH_SECRET", "").strip()
    return f"/mcp-{secret}" if secret else "/mcp"


def _news_sender(orchestrator):
    """Build a TelegramSender for the news bot from config or env."""
    from services.fdk_telegram_service import TelegramSender
    from shared.fdk_config_loader import TelegramChannel

    channel = None
    if orchestrator is not None:
        channel = orchestrator.config.telegram.news
    if channel is None:
        token = os.getenv("FDK_NEWS_BOT_TOKEN", "").strip()
        chat_id = os.getenv("FDK_NEWS_CHAT_ID", "").strip()
        if token and chat_id:
            channel = TelegramChannel(bot_token=token, chat_id=chat_id)
    return TelegramSender(channel) if channel else None


def build_mcp(orchestrator=None) -> FastMCP:
    """Create the FastMCP server with all tools registered.

    stateless_http + json_response keeps it simple and compatible with
    Render free tier (no sticky sessions) and Claude custom connectors.
    """
    mcp = FastMCP(
        "fdk",
        instructions=(
            "News + Shoonya volume spike detector tools. "
            "Use fetch_news_headlines with any RSS URL (see list_news_feeds), "
            "fetch_article_content for full articles, and the get_* tools "
            "for detector status and volume spike summaries."
        ),
        stateless_http=True,
        json_response=True,
        # Public server (Render / App Runner / any non-localhost host) — disable
        # the localhost-only Host header check (DNS rebinding protection), which
        # would 421-reject the public hostname.
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=False,
        ),
    )
    mcp.settings.streamable_http_path = mcp_path()

    register_news_tools(mcp, news_sender=_news_sender(orchestrator))
    register_detector_tools(mcp, orchestrator)

    log.info(f"MCP server built, endpoint path: {mcp.settings.streamable_http_path}")
    return mcp
