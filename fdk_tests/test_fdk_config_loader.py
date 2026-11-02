"""Configuration: Shoonya OAuth is the only login; old names are tolerated as fallbacks."""
import pytest

from shared import fdk_config_loader as cfg

COMMON = {
    "FDK_LOGIN_BOT_TOKEN": "t1", "FDK_LOGIN_CHAT_ID": "1",
    "FDK_FYERS_TRADE_BOT_TOKEN": "t2", "FDK_FYERS_TRADE_CHAT_ID": "2",
    "FDK_FYERS_SUMMARY_BOT_TOKEN": "t3", "FDK_FYERS_SUMMARY_CHAT_ID": "3",
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
ALL_KEYS = (list(COMMON) + list(SHOONYA) + list(LEGACY) + list(REMOVED_FYERS_LOGIN)
            + ["FDK_FYERS_CLIENT_ID", "FDK_SHOONYA_AUTO_LOGIN"])


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


def test_auto_login_can_be_switched_off(env):
    env.setenv("FDK_SHOONYA_AUTO_LOGIN", "false")

    assert cfg.load_config().shoonya.auto_login is False


def test_fyers_client_id_is_optional_for_the_dormant_data_socket(env):
    assert cfg.load_config().fyers_client_id == ""
    env.setenv("FDK_FYERS_CLIENT_ID", "APP-100")
    assert cfg.load_config().fyers_client_id == "APP-100"


def test_removed_variables_are_ignored(env):
    for key in REMOVED_FYERS_LOGIN + ("FDK_SHOONYA_IMEI", "FDK_SHOONYA_REDIRECT_URI"):
        env.setenv(key, "whatever")

    config = cfg.load_config()

    assert config.shoonya.username == "FA12345"
