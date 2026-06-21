"""IST (Asia/Kolkata) helpers for DISPLAY and ID generation.

The database stores timestamps as naive UTC (server runs in UTC). For audit
correctness we keep storing UTC everywhere — but anything SHOWN to a user
(emails, PDFs, server-rendered HTML) or EMBEDDED in a human-readable id
(e.g. CR-YYYYMMDDHHMMSS) must read in IST.

India observes no DST, so a fixed +05:30 offset is always correct — no
ZoneInfo/tzdata dependency needed.

Do NOT use these for internal/logic timestamps that stay UTC: JWT exp/iat,
DB created_at/updated_at writes, duration math. Those must remain UTC so
historical data stays consistent.
"""
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))


def now_ist() -> datetime:
    """Current wall-clock time in IST (tz-aware)."""
    return datetime.now(IST)


def to_ist(dt):
    """Convert a datetime to IST for display. Naive datetimes are assumed UTC
    (how the DB stores them). None passes through; strings are returned as-is."""
    if dt is None or isinstance(dt, str):
        return dt
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST)


def fmt_ist(dt, fmt: str = "%d %b %Y, %I:%M %p") -> str:
    """Format a datetime in IST. Empty string for None; passes strings through."""
    d = to_ist(dt)
    if d is None:
        return ""
    if isinstance(d, str):
        return d
    return d.strftime(fmt)
