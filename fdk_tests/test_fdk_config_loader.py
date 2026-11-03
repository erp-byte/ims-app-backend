"""Configuration: Shoonya OAuth is the only login; old names are tolerated as fallbacks."""
import pytest

from shared import fdk_config_loader as cfg

COMMON = {
    "FDK_LOGIN_BOT_TOKEN": "t1", "FDK_LOGIN_CHAT_ID": "1",
    "FDK_SHOONYA_TRADE_BOT_TOKEN": "t2", "FDK_SHOONYA_TRADE_CHAT_ID": "2",
    "FDK_SHOONYA_SUMMARY_BOT_TOKEN": "t3", "FDK_SHOONYA_SUMMARY_CHAT_ID": "3",
    "FDK_PENNY_TRADE_BOT_TOKEN": "t4", "FDK_PENNY_TRADE_CHAT_ID": "4",
    "FDK_PENNY_SUMMARY_BOT_TOKEN": "t5", "FDK_PENNY_SUMMARY_CHAT_ID": "5",
    "FDK_SUPABASE_DSN": "postgresql://fake:fake@localhost/fake",
}
SHOONYA = {
    "FDK_SHOONYA_USERNAME": "FA12345",
    "FDK_SHOONYA_PASSWORD": "Pass@123",
    "FDK_SHOONYA_TOTP_SECRET": "JBSWY3DPEHPK3PXP",
    "FDK_SHOONYA_CLIENT_ID": "FA12345_U",
    "FDK_SHOONYA_SECRET_CODE": "s" * 64,
}
LEGACY = ("FDK_SHOONYA_VENDORCODE", "FDK_SHOONYA_API_KEY", "FDK_SHOONYA_IMEI",
          "FDK_SHOONYA_REDIRECT_URI")
REMOVED_FYERS_LOGIN = ("FDK_BROKER", "FDK_FYERS_SECRET_KEY", "FDK_FYERS_REDIRECT_URI",
                       "FDK_FYERS_TOTP_SECRET", "FDK_FYERS_PIN", "FDK_FYERS_USERNAME")
# The bot and sheet settings were renamed from FDK_FYERS_* on 2026-11-03; the old names are
# read for one release.
LEGACY_BOTS = ("FDK_FYERS_TRADE_BOT_TOKEN", "FDK_FYERS_TRADE_CHAT_ID",
               "FDK_FYERS_SUMMARY_BOT_TOKEN", "FDK_FYERS_SUMMARY_CHAT_ID",
               "FDK_FYERS_GOOGLE_SHEETS_ID", "FDK_SHOONYA_GOOGLE_SHEETS_ID")
ALL_KEYS = (list(COMMON) + list(SHOONYA) + list(LEGACY) + list(REMOVED_FYERS_LOGIN) + list(LEGACY_BOTS)
            + ["FDK_FYERS_CLIENT_ID", "FDK_SHOONYA_AUTO_LOGIN", "FDK_TELEGRAM_WEBHOOK_SECRET"])


@pytest.fixture
def env(monkeypatch):
    for key in ALL_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in {**COMMON, **SHOONYA}.items():
        monkeypatch.setenv(key, value)
    return monkeypatch


def test_shoonya_oauth_inputs_are_loaded(env):
    config = cfg.load_config()

    assert config.shoonya.username == "FA12345"
    assert config.shoonya.client_id == "FA12345_U"
    assert config.shoonya.secret_code == "s" * 64
    assert config.shoonya.auto_login is True             # default: browser login first
    assert not hasattr(config.shoonya, "vendor_code")
    assert not hasattr(config.shoonya, "imei")
    assert not hasattr(config, "broker") and not hasattr(config, "fyers")


def test_legacy_names_are_accepted_as_fallbacks(env):
    env.delenv("FDK_SHOONYA_CLIENT_ID")
    env.delenv("FDK_SHOONYA_SECRET_CODE")
    env.setenv("FDK_SHOONYA_VENDORCODE", "FA12345_U")      # the old vendor code IS the client id
    env.setenv("FDK_SHOONYA_API_KEY", "k" * 64)            # the old "API key" IS the secret code

    config = cfg.load_config()

    assert config.shoonya.client_id == "FA12345_U"
    assert config.shoonya.secret_code == "k" * 64


def test_client_id_defaults_to_username_with_the_retail_suffix(env):
    env.delenv("FDK_SHOONYA_CLIENT_ID")
    assert cfg.load_config().shoonya.client_id == "FA12345_U"

    env.setenv("FDK_SHOONYA_CLIENT_ID", "   ")          # a blank line in .env means "default" too
    assert cfg.load_config().shoonya.client_id == "FA12345_U"


def test_webhook_secret_is_optional(env):
    env.delenv("FDK_TELEGRAM_WEBHOOK_SECRET", raising=False)
    assert cfg.load_config().telegram.webhook_secret == ""
    env.setenv("FDK_TELEGRAM_WEBHOOK_SECRET", " s3cret ")
    assert cfg.load_config().telegram.webhook_secret == "s3cret"


def test_webhook_secret_must_use_telegrams_charset(env):
    # Telegram's setWebhook rejects anything but 1-256 chars of A-Z a-z 0-9 _ -; a bad value
    # would leave the old secret-less webhook in force and every delivery answered 403
    env.setenv("FDK_TELEGRAM_WEBHOOK_SECRET", "fdk!2026.secret")
    with pytest.raises(ValueError, match="FDK_TELEGRAM_WEBHOOK_SECRET"):
        cfg.load_config()
    env.setenv("FDK_TELEGRAM_WEBHOOK_SECRET", "x" * 257)
    with pytest.raises(ValueError, match="FDK_TELEGRAM_WEBHOOK_SECRET"):
        cfg.load_config()
    env.setenv("FDK_TELEGRAM_WEBHOOK_SECRET", "fdk_2026-Secret")
    assert cfg.load_config().telegram.webhook_secret == "fdk_2026-Secret"


def test_bot_and_sheet_settings_use_shoonya_names(env):
    env.setenv("FDK_SHOONYA_GOOGLE_SHEETS_ID", "sheet-1")
    config = cfg.load_config()
    assert config.telegram.shoonya_trade.bot_token == "t2"
    assert config.telegram.shoonya_summary.chat_id == "3"
    assert config.google.shoonya_sheet_id == "sheet-1"
    assert not hasattr(config.telegram, "fyers_trade") and not hasattr(config.google, "fyers_sheet_id")


def test_old_fyers_bot_and_sheet_names_still_work_for_one_release(env):
    for key in ("FDK_SHOONYA_TRADE_BOT_TOKEN", "FDK_SHOONYA_TRADE_CHAT_ID",
                "FDK_SHOONYA_SUMMARY_BOT_TOKEN", "FDK_SHOONYA_SUMMARY_CHAT_ID"):
        env.delenv(key)
    env.setenv("FDK_FYERS_TRADE_BOT_TOKEN", "old-t2")
    env.setenv("FDK_FYERS_TRADE_CHAT_ID", "22")
    env.setenv("FDK_FYERS_SUMMARY_BOT_TOKEN", "old-t3")
    env.setenv("FDK_FYERS_SUMMARY_CHAT_ID", "33")
    env.setenv("FDK_FYERS_GOOGLE_SHEETS_ID", "old-sheet")
    config = cfg.load_config()
    assert (config.telegram.shoonya_trade.bot_token, config.telegram.shoonya_trade.chat_id) == ("old-t2", "22")
    assert config.telegram.shoonya_summary.bot_token == "old-t3"
    assert config.google.shoonya_sheet_id == "old-sheet"


def test_a_blank_new_bot_setting_falls_back_to_the_old_name(env):
    # a .env pasted from fdk_env.example leaves FDK_SHOONYA_TRADE_BOT_TOKEN= empty
    env.setenv("FDK_SHOONYA_TRADE_BOT_TOKEN", "")
    env.setenv("FDK_FYERS_TRADE_BOT_TOKEN", "old-t2")
    assert cfg.load_config().telegram.shoonya_trade.bot_token == "old-t2"
    env.setenv("FDK_FYERS_TRADE_BOT_TOKEN", "   ")
    with pytest.raises(KeyError, match="FDK_SHOONYA_TRADE_BOT_TOKEN"):
        cfg.load_config()


def test_a_missing_bot_setting_is_named_by_its_new_name(env):
    env.delenv("FDK_SHOONYA_TRADE_BOT_TOKEN")
    with pytest.raises(KeyError, match="FDK_SHOONYA_TRADE_BOT_TOKEN"):
        cfg.load_config()


def test_secret_code_is_required(env):
    env.delenv("FDK_SHOONYA_SECRET_CODE")

    with pytest.raises(KeyError, match="FDK_SHOONYA_SECRET_CODE"):
        cfg.load_config()


@pytest.mark.parametrize("key", ["FDK_SHOONYA_USERNAME", "FDK_SHOONYA_PASSWORD",
                                 "FDK_SHOONYA_TOTP_SECRET", "FDK_SHOONYA_SECRET_CODE"])
def test_blank_shoonya_credentials_are_rejected(env, key):
    env.setenv(key, "   ")

    with pytest.raises(ValueError, match=key):
        cfg.load_config()


def test_registered_redirect_is_kept_as_a_hint_for_the_browser_login(env):
    assert cfg.load_config().shoonya.redirect_hint == ""
    env.setenv("FDK_SHOONYA_REDIRECT_URI", "https://my-app.example.com/cb")
    assert cfg.load_config().shoonya.redirect_hint == "https://my-app.example.com/cb"


def test_auto_login_can_be_switched_off(env):
    env.setenv("FDK_SHOONYA_AUTO_LOGIN", "false")

    assert cfg.load_config().shoonya.auto_login is False


def test_fyers_client_id_went_with_the_fyers_socket(env):
    env.setenv("FDK_FYERS_CLIENT_ID", "APP-100")
    assert not hasattr(cfg.load_config(), "fyers_client_id")


def test_removed_variables_are_ignored(env):
    for key in REMOVED_FYERS_LOGIN + ("FDK_SHOONYA_IMEI", "FDK_SHOONYA_REDIRECT_URI"):
        env.setenv(key, "whatever")

    config = cfg.load_config()

    assert config.shoonya.username == "FA12345"
