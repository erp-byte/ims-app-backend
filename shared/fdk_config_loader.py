"""Load and validate all configuration from environment variables."""
import os
import json
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Absolute path so .env is found regardless of the working directory (shared/ -> repo root).
load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def fdk_enabled() -> bool:
    """Master switch — the backend only starts the detector when FDK_ENABLED is truthy."""
    return os.getenv("FDK_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")


@dataclass
class ShoonyaConfig:
    """Shoonya (Finvasia) headless login inputs — no OAuth, no redirect step."""
    username: str
    password: str
    totp_secret: str
    vendor_code: str      # retail API users: "<username>_U"
    api_key: str          # Prism-generated API key
    imei: str = "abc1234"  # any fixed device identifier; the broker does not validate it
    redirect_uri: str = ""  # unused by Shoonya's login; accepted so an existing env line is harmless


@dataclass
class TelegramChannel:
    """A single Telegram bot+chat pair."""
    bot_token: str
    chat_id: str


@dataclass
class TelegramConfig:
    login: TelegramChannel
    fyers_trade: TelegramChannel
    fyers_summary: TelegramChannel
    penny_trade: TelegramChannel
    penny_summary: TelegramChannel
    news: TelegramChannel | None = None  # optional — news bot (MCP send)


@dataclass
class GoogleConfig:
    credentials: dict | None
    fyers_sheet_id: str
    penny_sheet_id: str


@dataclass
class SupabaseConfig:
    dsn: str


@dataclass
class AppConfig:
    shoonya: ShoonyaConfig                # the only login (the Fyers login was removed)
    telegram: TelegramConfig
    google: GoogleConfig
    supabase: SupabaseConfig
    scheduling_enabled: bool = True
    port: int = 8000
    # Fyers app id, read only by the dormant Fyers data socket (which cannot run
    # without a Fyers login any more); optional, goes away with that socket.
    fyers_client_id: str = ""


def _required(key: str) -> str:
    """KeyError when missing (as before); ValueError when present but blank."""
    value = os.environ[key].strip()
    if not value:
        raise ValueError(f"{key} is set but empty")
    return value


def _shoonya_config() -> ShoonyaConfig:
    return ShoonyaConfig(
        username=_required("FDK_SHOONYA_USERNAME"),
        password=_required("FDK_SHOONYA_PASSWORD"),
        totp_secret=_required("FDK_SHOONYA_TOTP_SECRET"),
        vendor_code=_required("FDK_SHOONYA_VENDORCODE"),
        api_key=_required("FDK_SHOONYA_API_KEY"),
        imei=os.getenv("FDK_SHOONYA_IMEI", "").strip() or "abc1234",
        redirect_uri=os.getenv("FDK_SHOONYA_REDIRECT_URI", "").strip(),
    )


def _channel(token_env: str, chat_id_env: str) -> TelegramChannel:
    return TelegramChannel(
        bot_token=os.environ[token_env],
        chat_id=os.environ[chat_id_env],
    )


def _optional_channel(token_env: str, chat_id_env: str) -> TelegramChannel | None:
    token = os.getenv(token_env, "").strip()
    chat_id = os.getenv(chat_id_env, "").strip()
    if token and chat_id:
        return TelegramChannel(bot_token=token, chat_id=chat_id)
    return None


def load_config() -> AppConfig:
    """Build AppConfig from environment variables."""
    google_creds = _load_google_credentials()

    return AppConfig(
        shoonya=_shoonya_config(),
        fyers_client_id=os.getenv("FDK_FYERS_CLIENT_ID", "").strip(),
        telegram=TelegramConfig(
            login=_channel("FDK_LOGIN_BOT_TOKEN", "FDK_LOGIN_CHAT_ID"),
            fyers_trade=_channel("FDK_FYERS_TRADE_BOT_TOKEN", "FDK_FYERS_TRADE_CHAT_ID"),
            fyers_summary=_channel("FDK_FYERS_SUMMARY_BOT_TOKEN", "FDK_FYERS_SUMMARY_CHAT_ID"),
            penny_trade=_channel("FDK_PENNY_TRADE_BOT_TOKEN", "FDK_PENNY_TRADE_CHAT_ID"),
            penny_summary=_channel("FDK_PENNY_SUMMARY_BOT_TOKEN", "FDK_PENNY_SUMMARY_CHAT_ID"),
            news=_optional_channel("FDK_NEWS_BOT_TOKEN", "FDK_NEWS_CHAT_ID"),
        ),
        google=GoogleConfig(
            credentials=google_creds,
            fyers_sheet_id=os.environ.get("FDK_FYERS_GOOGLE_SHEETS_ID", ""),
            penny_sheet_id=os.environ.get("FDK_PENNY_GOOGLE_SHEETS_ID", ""),
        ),
        supabase=SupabaseConfig(
            dsn=os.environ["FDK_SUPABASE_DSN"],
        ),
        scheduling_enabled=os.getenv("FDK_SCHEDULING_ENABLED", "true").lower() == "true",
        port=int(os.getenv("FDK_PORT", "8000")),
    )


def _load_google_credentials() -> dict | None:
    """Load Google service account credentials from env."""
    blob = os.getenv("FDK_GOOGLE_CREDENTIALS_JSON")
    if blob:
        creds = json.loads(blob)
        if "private_key" in creds:
            creds["private_key"] = creds["private_key"].replace("\\n", "\n")
        return creds

    pk = os.getenv("FDK_GOOGLE_PRIVATE_KEY", "")
    if pk:
        pk = pk.replace("\\n", "\n")

    creds = {
        "type": "service_account",
        "project_id": os.getenv("FDK_GOOGLE_PROJECT_ID", ""),
        "private_key_id": os.getenv("FDK_GOOGLE_PRIVATE_KEY_ID", ""),
        "private_key": pk,
        "client_email": os.getenv("FDK_GOOGLE_CLIENT_EMAIL", ""),
        "client_id": os.getenv("FDK_GOOGLE_CLIENT_ID", ""),
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": os.getenv("FDK_GOOGLE_CLIENT_X509_CERT_URL", ""),
        "universe_domain": "googleapis.com",
    }
    required = ["project_id", "private_key_id", "private_key", "client_email"]
    if any(not creds.get(f) for f in required):
        return None
    return creds
