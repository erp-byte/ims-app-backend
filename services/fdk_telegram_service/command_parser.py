"""Parse Telegram message commands."""
from datetime import datetime


def parse_date(text: str) -> datetime | None:
    """Parse a date from user input. Supports DD-MM-YYYY, DD/MM/YYYY, YYYY-MM-DD."""
    text = text.strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def parse_command(text: str) -> tuple[str | None, str]:
    """Parse a /command from message text. Returns (command, remaining_text)."""
    text = text.strip()
    if not text.startswith("/"):
        return None, text
    parts = text.split(maxsplit=1)
    command = parts[0].lower().split("@")[0]  # strip @botname suffix
    rest = parts[1] if len(parts) > 1 else ""
    return command, rest
