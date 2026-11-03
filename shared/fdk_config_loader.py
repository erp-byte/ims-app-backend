"""Load and validate all configuration from environment variables."""
import os
import json
import re
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
    """Shoonya (Finvasia) OAuth login inputs (the API in force since 1 April 2026)."""
    username: str          # user id, e.g. FA12345
    password: str
    totp_secret: str       # Profile -> Security -> TOTP Setup
    client_id: str         # API Key Generation screen "Client ID", e.g. FA12345_U
    secret_code: str       # API Key Generation screen "Secret Code" (checksum input)
    auto_login: bool = True  # drive the login page headlessly before falling back to the Telegram link
    redirect_hint: str = ""  # redirect URL registered at Shoonya, if any: helps the browser spot the code


@dataclass
class TelegramChannel:
    """A single Telegram bot+chat pair."""
    bot_token: str
    chat_id: str


@dataclass
class TelegramConfig:
    login: TelegramChannel
    shoonya_trade: TelegramChannel        # large-cap alerts
    shoonya_summary: TelegramChannel      # large-cap daily summary
    penny_trade: TelegramChannel
    penny_summary: TelegramChannel
    news: TelegramChannel | None = None  # optional — news bot (MCP send)
    webhook_secret: str = ""             # optional — Telegram checks it back in every webhook delivery


@dataclass
class GoogleConfig:
    credentials: dict | None
    shoonya_sheet_id: str                 # large-cap alert sheet
    penny_sheet_id: str


@dataclass
class SupabaseConfig:
    dsn: str


@dataclass
class AppConfig:
    shoonya: ShoonyaConfig                # the only login
    telegram: TelegramConfig
    google: GoogleConfig
    supabase: SupabaseConfig
    scheduling_enabled: bool = True
    port: int = 8000


def _required(key: str) -> str:
    """KeyError when missing (as before); ValueError when present but blank."""
    value = os.environ[key].strip()
    if not value:
        raise ValueError(f"{key} is set but empty")
    return value


def _first_present(*keys: str) -> str:
    """First env var among `keys` that is set; blank -> ValueError; none -> KeyError(keys[0])."""
    for key in keys:
        if key in os.environ:
            return _required(key)
    raise KeyError(keys[0])


def _shoonya_config() -> ShoonyaConfig:
    username = _required("FDK_SHOONYA_USERNAME")
    # Shoonya renamed things with its 2026 OAuth API: the old "vendor code" is the
    # Client ID (retail: <username>_U) and the old "API key" is the Secret Code.
    # The client id is optional; unset OR blank means the retail default.
    client_id = (os.getenv("FDK_SHOONYA_CLIENT_ID") or os.getenv("FDK_SHOONYA_VENDORCODE")
                 or "").strip() or f"{username}_U"
    return ShoonyaConfig(
        username=username,
        password=_required("FDK_SHOONYA_PASSWORD"),
        totp_secret=_required("FDK_SHOONYA_TOTP_SECRET"),
        client_id=client_id,
        secret_code=_first_present("FDK_SHOONYA_SECRET_CODE", "FDK_SHOONYA_API_KEY"),
        auto_login=os.getenv("FDK_SHOONYA_AUTO_LOGIN", "true").strip().lower()
        in ("1", "true", "yes", "on"),
        redirect_hint=os.getenv("FDK_SHOONYA_REDIRECT_URI", "").strip(),
    )


def _channel(token_env: str, chat_id_env: str,
             legacy: tuple[str, str] | None = None) -> TelegramChannel:
    """A bot+chat pair. `legacy` names (the FDK_FYERS_* ones, renamed 2026-11-03) are
    read when the new ones are absent, for one release; a missing value names the new key."""
    old_token, old_chat = legacy or (None, None)

    def read(key, old):
        if key in os.environ:
            return os.environ[key]
        if old and old in os.environ:
            return os.environ[old]
        raise KeyError(key)

    return TelegramChannel(bot_token=read(token_env, old_token), chat_id=read(chat_id_env, old_chat))


# Telegram's setWebhook accepts secret_token only as 1-256 chars of A-Z a-z 0-9 _ -
_WEBHOOK_SECRET_RE = re.compile(r"[A-Za-z0-9_-]{1,256}")


def _webhook_secret() -> str:
    """Optional; a value Telegram would reject is an error at start-up, not a silent
    403 on every delivery (the old secret-less webhook would stay registered)."""
    secret = os.getenv("FDK_TELEGRAM_WEBHOOK_SECRET", "").strip()
    if secret and not _WEBHOOK_SECRET_RE.fullmatch(secret):
        raise ValueError("FDK_TELEGRAM_WEBHOOK_SECRET must be 1-256 characters of "
                         "A-Z a-z 0-9 _ - (Telegram rejects anything else)")
    return secret


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
        telegram=TelegramConfig(
            login=_channel("FDK_LOGIN_BOT_TOKEN", "FDK_LOGIN_CHAT_ID"),
            shoonya_trade=_channel("FDK_SHOONYA_TRADE_BOT_TOKEN", "FDK_SHOONYA_TRADE_CHAT_ID",
                                   legacy=("FDK_FYERS_TRADE_BOT_TOKEN", "FDK_FYERS_TRADE_CHAT_ID")),
            shoonya_summary=_channel("FDK_SHOONYA_SUMMARY_BOT_TOKEN", "FDK_SHOONYA_SUMMARY_CHAT_ID",
                                     legacy=("FDK_FYERS_SUMMARY_BOT_TOKEN", "FDK_FYERS_SUMMARY_CHAT_ID")),
            penny_trade=_channel("FDK_PENNY_TRADE_BOT_TOKEN", "FDK_PENNY_TRADE_CHAT_ID"),
            penny_summary=_channel("FDK_PENNY_SUMMARY_BOT_TOKEN", "FDK_PENNY_SUMMARY_CHAT_ID"),
            news=_optional_channel("FDK_NEWS_BOT_TOKEN", "FDK_NEWS_CHAT_ID"),
            webhook_secret=_webhook_secret(),
        ),
        google=GoogleConfig(
            credentials=google_creds,
            shoonya_sheet_id=(os.environ.get("FDK_SHOONYA_GOOGLE_SHEETS_ID")
                              or os.environ.get("FDK_FYERS_GOOGLE_SHEETS_ID", "")),
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
