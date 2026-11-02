"""Fyers tools — read-only views over the volume spike detector system."""
import asyncio
from datetime import datetime

from shared.fdk_constants import IST
from shared.fdk_logger import get_logger

log = get_logger("mcp_fyers")

VALID_DETECTORS = ("fyers", "penny")


def _parse_date(date_str: str) -> datetime | None:
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str, fmt).replace(tzinfo=IST)
        except ValueError:
            continue
    return None


def register_fyers_tools(mcp, orchestrator) -> None:
    """Attach Fyers detector tools to a FastMCP instance.

    orchestrator may be None (standalone/test mode) — tools then report
    that the detector system is unavailable.
    """

    def _unavailable() -> dict:
        return {"error": "Detector system not running (orchestrator unavailable)"}

    @mcp.tool()
    async def get_detector_status() -> dict:
        """Current status of the Fyers and Penny volume spike detectors:
        running state, hold mode, market hours."""
        if orchestrator is None:
            return _unavailable()
        from services.fdk_supervisor_service.schedular import is_market_hours
        auth = orchestrator.authenticator
        last_login = getattr(auth, "last_login_at", None)
        return {
            "timestamp": datetime.now(IST).isoformat(),
            "broker": orchestrator.broker,
            "login_state": orchestrator.login_state(),
            "last_login_at": last_login.isoformat() if last_login else None,
            "last_auth_error": getattr(auth, "last_error", None),
            "on_hold": orchestrator.on_hold,
            "fyers_running": orchestrator.fyers.is_running,
            "penny_running": orchestrator.penny.is_running,
            "market_hours": is_market_hours(),
            "authenticated": orchestrator.authenticator.is_authenticated,
            "scheduling_enabled": orchestrator.config.scheduling_enabled,
        }

    @mcp.tool()
    async def get_volume_summary(detector: str = "fyers",
                                 date: str = "",
                                 days_back: int = 0) -> dict:
        """Top-15 volume spike summary (by trade count) from the alert sheet.

        Args:
            detector: 'fyers' (large-cap, Rs 3 Cr threshold) or
                'penny' (small-cap, Rs 52 Lakh threshold)
            date: Anchor date as DD-MM-YYYY (default: today IST)
            days_back: Include N days before the anchor date (0 = single day)
        """
        if orchestrator is None:
            return _unavailable()
        if detector not in VALID_DETECTORS:
            return {"error": f"detector must be one of {VALID_DETECTORS}"}

        target = None
        if date:
            target = _parse_date(date)
            if target is None:
                return {"error": "Invalid date. Use DD-MM-YYYY."}

        service = (orchestrator.fyers_summary if detector == "fyers"
                   else orchestrator.penny_summary)
        days_back = max(0, min(int(days_back), 31))
        label = {0: "Daily"}.get(days_back, f"{days_back + 1}-Day")

        msg = await asyncio.to_thread(
            service.generator._format_message, days_back, label, target
        )
        if not msg:
            return {"detector": detector,
                    "summary": None,
                    "note": "No records found for the requested date range."}
        return {"detector": detector, "summary": msg}

    @mcp.tool()
    async def list_monitored_symbols(detector: str = "fyers") -> dict:
        """List active NSE symbols monitored by a detector.

        Args:
            detector: 'fyers' (large-cap) or 'penny' (small-cap)
        """
        if orchestrator is None:
            return _unavailable()
        if detector not in VALID_DETECTORS:
            return {"error": f"detector must be one of {VALID_DETECTORS}"}
        symbols = await asyncio.to_thread(
            orchestrator.symbol_manager.load_symbols, detector
        )
        return {"detector": detector, "count": len(symbols), "symbols": symbols}

    @mcp.tool()
    async def get_sector_mapping(symbol: str = "") -> dict:
        """Get sector for an NSE symbol (e.g. 'NSE:TCS-EQ'), or counts of
        all sectors if no symbol given."""
        if orchestrator is None:
            return _unavailable()
        mapping = await asyncio.to_thread(
            orchestrator.symbol_manager.load_sector_mapping
        )
        if symbol:
            sector = mapping.get(symbol)
            if sector is None:
                # try fuzzy: allow bare symbol like 'TCS'
                matches = {s: sec for s, sec in mapping.items()
                           if symbol.upper() in s.upper()}
                if matches:
                    return {"matches": matches}
                return {"error": f"Symbol '{symbol}' not found"}
            return {"symbol": symbol, "sector": sector}
        counts: dict[str, int] = {}
        for sec in mapping.values():
            counts[sec] = counts.get(sec, 0) + 1
        return {"total_symbols": len(mapping), "sectors": counts}
