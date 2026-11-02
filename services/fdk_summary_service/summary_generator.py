"""Generates daily/weekly top-15 summaries from Google Sheets data."""
import asyncio
import re
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
from shared.fdk_logger import get_logger
from shared.fdk_constants import IST
from services.fdk_telegram_service import TelegramSender

log = get_logger("summary")


class SummaryGenerator:
    """Parameterized summary generator — one instance per detector (fyers/penny)."""

    def __init__(self, name: str, google_credentials: dict | None,
                 sheet_id: str, summary_sender: TelegramSender):
        self.name = name
        self.credentials = google_credentials
        self.sheet_id = sheet_id
        self.sender = summary_sender
        self.worksheet = None

    def _init_sheets(self) -> bool:
        if not self.credentials:
            log.warning(f"[{self.name}] No Google credentials for summary")
            return False
        try:
            scopes = ["https://www.googleapis.com/auth/spreadsheets"]
            creds = Credentials.from_service_account_info(
                self.credentials, scopes=scopes
            )
            client = gspread.authorize(creds)
            self.worksheet = client.open_by_key(self.sheet_id).sheet1
            log.info(f"[{self.name}] Sheets connected for summary")
            return True
        except Exception as e:
            log.error(f"[{self.name}] Sheets init failed: {e}")
            return False

    def _find_columns(self, headers: list[str]) -> tuple[int, int, int] | None:
        """Find Date, Symbol, and Value column indices."""
        date_idx = symbol_idx = value_idx = None
        for idx, h in enumerate(headers):
            hl = h.lower().strip()
            if hl == "date":
                date_idx = idx
            elif hl == "symbol":
                symbol_idx = idx
            elif ("trd" in hl and "val" in hl and "cr" in hl) or \
                 ("value" in hl and ("cr" in hl or "crore" in hl)):
                value_idx = idx
        if date_idx is None or symbol_idx is None or value_idx is None:
            log.error(f"[{self.name}] Required columns not found: "
                      f"date={date_idx} symbol={symbol_idx} value={value_idx}")
            return None
        return date_idx, symbol_idx, value_idx

    def _date_formats(self, dt: datetime) -> list[str]:
        return [
            dt.strftime("%d-%m-%Y"),
            dt.strftime("%Y-%m-%d"),
            dt.strftime("%d/%m/%Y"),
            dt.strftime("%m/%d/%Y"),
            dt.strftime("%d-%m-%y"),
        ]

    def _get_records(self, days_back: int = 0,
                     target_date: datetime | None = None) -> list[dict]:
        """Fetch records from the sheet for a date range.

        If target_date is given, uses it as the anchor date.
        Otherwise uses current IST time.
        """
        if not self.worksheet:
            if not self._init_sheets():
                return []
        try:
            all_values = self.worksheet.get_all_values()
            if not all_values or len(all_values) < 2:
                return []

            cols = self._find_columns(all_values[0])
            if not cols:
                return []
            date_idx, symbol_idx, value_idx = cols

            anchor = target_date if target_date else datetime.now(IST)
            target_dates = set()
            for i in range(days_back + 1):
                target_dates.update(self._date_formats(anchor - timedelta(days=i)))

            records = []
            max_idx = max(date_idx, symbol_idx, value_idx)
            for row in all_values[1:]:
                if len(row) <= max_idx:
                    continue
                date_val = row[date_idx].strip()
                if date_val not in target_dates:
                    if not any(date_val.startswith(d) for d in target_dates):
                        continue
                symbol = row[symbol_idx].strip()
                value_str = row[value_idx].strip()
                if symbol and value_str:
                    records.append({
                        "symbol": symbol,
                        "value_str": value_str,
                    })
            log.info(f"[{self.name}] Fetched {len(records)} records "
                     f"(days_back={days_back}, date={anchor.strftime('%d-%m-%Y')})")
            return records
        except Exception as e:
            log.error(f"[{self.name}] Error fetching records: {e}")
            return []

    def _aggregate(self, records: list[dict]) -> list[tuple[str, dict]]:
        """Aggregate records by symbol, return top 15 sorted by count."""
        stats: dict[str, dict] = {}
        for rec in records:
            symbol = rec["symbol"]
            value_clean = re.sub(r"[^\d.\-]", "", rec["value_str"])
            try:
                val = float(value_clean) if value_clean and value_clean != "-" else 0.0
            except ValueError:
                val = 0.0
            if symbol not in stats:
                stats[symbol] = {"count": 0, "total_cr": 0.0}
            stats[symbol]["count"] += 1
            stats[symbol]["total_cr"] += val

        sorted_symbols = sorted(
            stats.items(), key=lambda x: x[1]["count"], reverse=True
        )
        return sorted_symbols[:15]

    def _format_message(self, days_back: int, summary_type: str,
                        target_date: datetime | None = None) -> str | None:
        """Generate one summary message. Uses target_date if given, else now."""
        records = self._get_records(days_back, target_date)
        if not records:
            return None

        top_15 = self._aggregate(records)
        if not top_15:
            return None

        anchor = target_date if target_date else datetime.now(IST)
        if days_back == 0:
            date_info = anchor.strftime("%d-%m-%Y")
        else:
            start = anchor - timedelta(days=days_back)
            date_info = f"{start.strftime('%d-%m-%Y')} to {anchor.strftime('%d-%m-%Y')}"

        total_value = sum(s["total_cr"] for _, s in top_15)
        unique = len(set(r["symbol"] for r in records))

        msg = (f"<b>{self.name.upper()} {summary_type} Volume Spike Summary</b>\n"
               f"Date: {date_info}\n"
               f"Total Records: {len(records)}\n"
               f"Unique Symbols: {unique}\n"
               f"Top 15 Total Value: Rs.{total_value:,.2f} Cr\n\n"
               f"<b>TOP 15 RANKINGS (by Count):</b>\n\n")

        for idx, (symbol, s) in enumerate(top_15, 1):
            avg = s["total_cr"] / s["count"] if s["count"] > 0 else 0
            msg += (f"{idx}. <b>{symbol}</b>\n"
                    f"   Count: <b>{s['count']}</b> trades\n"
                    f"   Total Value: Rs.{s['total_cr']:,.2f} Cr\n"
                    f"   Avg per Trade: Rs.{avg:.2f} Cr\n\n")

        msg += (f"====================\n"
                f"<i>Analysis Complete for {date_info}</i>\n"
                f"<i>Ranked by highest trade count</i>")
        return msg

    def _generate_messages_for(self, target_date: datetime | None = None) -> list[str]:
        """Generate day-appropriate summary messages for a given date."""
        anchor = target_date if target_date else datetime.now(IST)
        day = anchor.strftime("%A")
        messages = []

        # Always: daily
        daily = self._format_message(0, "Daily", target_date)
        if daily:
            messages.append(daily)

        # Wednesday + Friday: 3-day
        if day in ("Wednesday", "Friday"):
            three_day = self._format_message(2, "3-Day", target_date)
            if three_day:
                messages.append(three_day)

        # Friday: weekly
        if day == "Friday":
            weekly = self._format_message(4, "Weekly", target_date)
            if weekly:
                messages.append(weekly)

        return messages

    def generate_messages(self) -> list[str]:
        """Generate day-appropriate summary messages for today."""
        return self._generate_messages_for(None)

    def generate_messages_for_date(self, target_date: datetime) -> list[str]:
        """Generate day-appropriate summary messages for a specific date."""
        return self._generate_messages_for(target_date)

    async def send_summary(self) -> bool:
        """Generate and send today's summary messages."""
        # Sheet reads run in a worker thread: the event loop is shared with the backend.
        return await self._send(await asyncio.to_thread(self.generate_messages))

    async def send_summary_for_date(self, target_date: datetime) -> bool:
        """Generate and send summary messages for a specific date."""
        return await self._send(
            await asyncio.to_thread(self.generate_messages_for_date, target_date))

    async def _send(self, messages: list[str]) -> bool:
        if not messages:
            log.warning(f"[{self.name}] No summary data to send")
            return False
        success = 0
        for msg in messages:
            if await self.sender.send_async(msg):
                success += 1
        log.info(f"[{self.name}] Sent {success}/{len(messages)} summary messages")
        return success == len(messages)
