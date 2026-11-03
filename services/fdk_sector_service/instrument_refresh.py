"""Daily refresh of Shoonya tokens and names from the exchange's symbol master.

Shoonya says to re-download NSE_symbols.txt.zip every trading day: a token or a
trading symbol can change, and a series move (ABC-EQ -> ABC-BE) renames the scrip
the feed subscribes to. The file carries no ISIN, so the stored instruments are
matched on token. Only a series move is applied automatically; a changed name or a
clash with a symbol already in use is reported for the operator to decide, and a
token that vanished from the file is reported too. A file that looks wrong (too
small, or too many stored tokens missing) changes nothing.
"""
import csv
import io
import zipfile
from dataclasses import dataclass, field
from datetime import date
from typing import Callable

import requests

from shared.fdk_constants import MAX_MISSING_TOKEN_RATIO, MIN_MASTER_ROWS, SHOONYA_MASTER_URL
from shared.fdk_logger import get_logger

log = get_logger("instrument_refresh")

HTTP_TIMEOUT = 30
MASTER_COLUMNS = ("Exchange", "Token", "LotSize", "Symbol", "TradingSymbol", "Instrument", "TickSize")


def parse_master(text: str) -> dict[str, dict]:
    """Token -> {symbol, trading_symbol, series} from the file's CSV text."""
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        raise ValueError("symbol file is empty")
    header = [cell.strip() for cell in rows[0] if cell.strip()]
    if header[:len(MASTER_COLUMNS)] != list(MASTER_COLUMNS):
        raise ValueError(f"symbol file has unexpected columns: {header}")
    idx = {name: i for i, name in enumerate(header)}
    master: dict[str, dict] = {}
    for cells in rows[1:]:
        if len(cells) < len(MASTER_COLUMNS):
            continue
        token = cells[idx["Token"]].strip()
        if not token:
            continue
        master[token] = {
            "symbol": cells[idx["Symbol"]].strip(),
            "trading_symbol": cells[idx["TradingSymbol"]].strip(),
            "series": cells[idx["Instrument"]].strip(),
        }
    return master


def download_master(url: str = SHOONYA_MASTER_URL, timeout: float = HTTP_TIMEOUT) -> dict[str, dict]:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as archive:
        names = archive.namelist()
        if not names:
            raise ValueError("symbol archive is empty")
        text = archive.read(names[0]).decode("utf-8", "replace")
    return parse_master(text)


@dataclass
class RefreshPlan:
    verified: list[str] = field(default_factory=list)               # isins seen in today's file
    renames: list[tuple[str, str, str, str]] = field(default_factory=list)    # isin, old, new, series
    decisions: list[tuple[str, str, str, str]] = field(default_factory=list)  # isin, stored, in file, why
    missing: list[tuple[str, str, str]] = field(default_factory=list)         # isin, symbol, token
    aborted: str | None = None                                      # why nothing was written

    @property
    def changed(self) -> bool:
        """Names changed in the database: the symbol universe must be reloaded."""
        return self.aborted is None and bool(self.renames)

    def summary(self) -> str:
        if self.aborted:
            return f"skipped: {self.aborted}"
        return (f"{len(self.verified)} verified, {len(self.renames)} renamed, "
                f"{len(self.decisions)} need a decision, {len(self.missing)} missing")


def plan_refresh(stored: list[dict], master: dict[str, dict], *, taken_symbols: set[str],
                 min_rows: int = MIN_MASTER_ROWS,
                 max_missing_ratio: float = MAX_MISSING_TOKEN_RATIO) -> RefreshPlan:
    """Decide what today's file means for the stored instruments. Pure: no I/O.

    stored rows carry isin, shoonya_symbol, shoonya_token and series; taken_symbols
    are the trading symbols already present in stock_symbols / sector_mappings.
    """
    plan = RefreshPlan()
    if len(master) < min_rows:
        plan.aborted = f"symbol file looks wrong: {len(master)} rows (expected at least {min_rows})"
        return plan
    claimed = set(taken_symbols)
    for row in stored:
        isin, old, token = row["isin"], row["shoonya_symbol"], str(row["shoonya_token"])
        entry = master.get(token)
        if entry is None:
            plan.missing.append((isin, old, token))
            continue
        plan.verified.append(isin)
        new = entry["trading_symbol"]
        if new == old:
            continue
        old_base, _, old_series = old.rpartition("-")
        new_base, _, new_series = new.rpartition("-")
        if old_base and old_base == new_base and old_series != new_series:
            if new in claimed:
                plan.decisions.append((isin, old, new, "name clash"))
            else:
                claimed.add(new)
                plan.renames.append((isin, old, new, new_series or entry["series"]))
        else:
            plan.decisions.append((isin, old, new, "name changed"))
    if stored and len(plan.missing) / len(stored) > max_missing_ratio:
        return RefreshPlan(aborted=f"{len(plan.missing)} of {len(stored)} tokens missing from the "
                                   f"symbol file (over {max_missing_ratio:.0%})")
    return plan


def apply_plan(conn, plan: RefreshPlan, today: date) -> None:
    """Write the plan in one transaction: the three tables move together or not at all."""
    if plan.aborted or not (plan.verified or plan.renames):
        return
    try:
        with conn.cursor() as cur:
            if plan.verified:
                cur.execute("UPDATE instruments SET verified_on = %s WHERE isin = ANY(%s)",
                            (today, plan.verified))
            for isin, old, new, series in plan.renames:
                cur.execute("UPDATE instruments SET shoonya_symbol = %s, series = %s, "
                            "verified_on = %s WHERE isin = %s", (new, series, today, isin))
                cur.execute("UPDATE stock_symbols SET symbol = %s WHERE symbol = %s", (new, old))
                cur.execute("UPDATE sector_mappings SET symbol = %s WHERE symbol = %s", (new, old))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


class InstrumentRefresher:
    """Download, plan, apply. `connect` opens a psycopg2 connection (SymbolManager's)."""

    def __init__(self, connect: Callable, download: Callable[[], dict] = download_master, *,
                 min_rows: int = MIN_MASTER_ROWS, max_missing_ratio: float = MAX_MISSING_TOKEN_RATIO,
                 today: Callable[[], date] = date.today):
        self.connect = connect
        self.download = download
        self.min_rows = min_rows
        self.max_missing_ratio = max_missing_ratio
        self.today = today

    def run(self) -> RefreshPlan:
        master = self.download()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT isin, shoonya_symbol, shoonya_token, series FROM instruments")
                stored = [dict(zip(("isin", "shoonya_symbol", "shoonya_token", "series"), row))
                          for row in cur.fetchall()]
                cur.execute("SELECT symbol FROM stock_symbols UNION SELECT symbol FROM sector_mappings")
                taken = {row[0] for row in cur.fetchall()}
        plan = plan_refresh(stored, master, taken_symbols=taken,
                            min_rows=self.min_rows, max_missing_ratio=self.max_missing_ratio)
        if plan.aborted:
            log.error(f"Instrument refresh {plan.summary()}")
            return plan
        with self.connect() as conn:
            apply_plan(conn, plan, self.today())
        log.info(f"Instrument refresh: {plan.summary()}")
        for _, old, new, _ in plan.renames:
            log.info(f"Renamed {old} -> {new}")
        for _, old, new, why in plan.decisions:
            log.warning(f"Not changed ({why}): {old} is {new} in Shoonya's file")
        for _, symbol, token in plan.missing:
            log.warning(f"Token {token} ({symbol}) is missing from Shoonya's file")
        return plan
