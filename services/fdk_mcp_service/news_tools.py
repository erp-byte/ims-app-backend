"""News tools — fetch RSS feeds and extract article content. No API key needed."""
import asyncio
import re

import feedparser
import requests
from bs4 import BeautifulSoup

from shared.fdk_logger import get_logger

log = get_logger("mcp_news")

REQUEST_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36"),
}
TIMEOUT = 15

POPULAR_FEEDS = {
    "Times of India (Top)": "https://timesofindia.indiatimes.com/rssfeedstopstories.cms",
    "Economic Times": "https://economictimes.indiatimes.com/rssfeedsdefault.cms",
    "The Hindu (National)": "https://www.thehindu.com/news/national/feeder/default.rss",
    "NDTV (Top)": "https://feeds.feedburner.com/ndtvnews-top-stories",
    "Google News India": "https://news.google.com/rss?gl=IN&hl=en-IN&ceid=IN:en",
    "BBC World": "https://feeds.bbci.co.uk/news/world/rss.xml",
    "BBC News (Top)": "https://feeds.bbci.co.uk/news/rss.xml",
    "TechCrunch": "https://techcrunch.com/feed/",
    "Hacker News": "https://hnrss.org/frontpage",
    "Moneycontrol (Markets)": "https://www.moneycontrol.com/rss/marketreports.xml",
    "LiveMint (Markets)": "https://www.livemint.com/rss/markets",
}


def _fetch_feed_sync(url: str, limit: int) -> dict:
    resp = requests.get(url, headers=REQUEST_HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    parsed = feedparser.parse(resp.content)
    entries = []
    for e in parsed.entries[:limit]:
        summary = e.get("summary", "") or e.get("description", "")
        summary = re.sub(r"<[^>]+>", "", summary).strip()
        entries.append({
            "title": e.get("title", ""),
            "link": e.get("link", ""),
            "published": e.get("published", e.get("updated", "")),
            "summary": summary[:500],
        })
    return {
        "feed_title": parsed.feed.get("title", ""),
        "feed_link": parsed.feed.get("link", ""),
        "entry_count": len(entries),
        "entries": entries,
    }


def _extract_article_sync(url: str) -> dict:
    resp = requests.get(url, headers=REQUEST_HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    for tag in soup(["script", "style", "nav", "header", "footer",
                     "aside", "form", "iframe", "noscript"]):
        tag.decompose()

    title = soup.title.get_text(strip=True) if soup.title else ""
    container = (soup.find("article")
                 or soup.find("main")
                 or soup.find("div", attrs={"id": re.compile(r"content|article", re.I)})
                 or soup.body
                 or soup)

    blocks = []
    for el in container.find_all(["h1", "h2", "h3", "p", "li"]):
        text = el.get_text(" ", strip=True)
        if not text or len(text) < 25:
            continue
        if el.name in ("h1", "h2", "h3"):
            blocks.append(f"## {text}")
        else:
            blocks.append(text)

    content = "\n\n".join(blocks)
    return {
        "title": title,
        "url": url,
        "content_markdown": content[:15000],
        "truncated": len(content) > 15000,
    }


def _format_telegram_news(feed: dict, limit: int) -> str:
    """Format feed entries as a Telegram HTML message."""
    title = feed.get("feed_title", "News")
    lines = [f"<b>📰 {title}</b>\n"]
    for i, e in enumerate(feed.get("entries", [])[:limit], 1):
        lines.append(f'{i}. <a href="{e["link"]}">{e["title"]}</a>')
        if e.get("summary"):
            lines.append(f'   <i>{e["summary"][:150]}</i>')
        lines.append("")
    return "\n".join(lines)[:4000]  # Telegram message limit 4096


def register_news_tools(mcp, news_sender=None) -> None:
    """Attach news tools to a FastMCP instance.

    news_sender: optional TelegramSender for the news bot — enables
    send_news_to_telegram.
    """

    @mcp.tool()
    async def fetch_news_headlines(url: str, limit: int = 10) -> dict:
        """Fetch latest headlines from any RSS/Atom news feed URL.

        Args:
            url: RSS feed URL (use list_news_feeds for popular options)
            limit: Max entries to return (1-50, default 10)
        """
        limit = max(1, min(int(limit), 50))
        try:
            return await asyncio.to_thread(_fetch_feed_sync, url, limit)
        except Exception as e:
            log.error(f"Feed fetch failed for {url}: {e}")
            return {"error": f"Failed to fetch feed: {e}"}

    @mcp.tool()
    async def fetch_article_content(url: str) -> dict:
        """Extract the readable text of a news article from its URL,
        formatted as Markdown."""
        try:
            return await asyncio.to_thread(_extract_article_sync, url)
        except Exception as e:
            log.error(f"Article extraction failed for {url}: {e}")
            return {"error": f"Failed to extract article: {e}"}

    @mcp.tool()
    async def list_news_feeds() -> dict:
        """List curated popular news feed URLs (Indian + global) usable
        with fetch_news_headlines."""
        return {"feeds": POPULAR_FEEDS}

    @mcp.tool()
    async def send_news_to_telegram(url: str = "", limit: int = 5) -> dict:
        """Fetch latest headlines and send them to the configured Telegram
        news bot chat.

        Args:
            url: RSS feed URL (default: Times of India top stories)
            limit: Number of headlines to send (1-15, default 5)
        """
        if news_sender is None:
            return {"error": "News bot not configured. "
                             "Set FDK_NEWS_BOT_TOKEN and FDK_NEWS_CHAT_ID."}
        feed_url = url or POPULAR_FEEDS["Times of India (Top)"]
        limit = max(1, min(int(limit), 15))
        try:
            feed = await asyncio.to_thread(_fetch_feed_sync, feed_url, limit)
        except Exception as e:
            return {"error": f"Failed to fetch feed: {e}"}
        if not feed.get("entries"):
            return {"error": "Feed returned no entries", "url": feed_url}

        message = _format_telegram_news(feed, limit)
        ok = await news_sender.send_async(message)
        if not ok:
            return {"error": "Telegram send failed — check bot token/chat ID, "
                             "and make sure you pressed Start on the bot."}
        return {"sent": True,
                "headlines": len(feed["entries"]),
                "feed": feed.get("feed_title", feed_url)}
