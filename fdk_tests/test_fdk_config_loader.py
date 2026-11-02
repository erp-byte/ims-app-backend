"""Configuration: Shoonya is the only login; the Fyers login variables are gone."""
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
    "FDK_SHOONYA_VENDORCODE": "FA12345_U",
    "FDK_SHOONYA_API_KEY": "k" * 64,
}
REMOVED_FYERS_LOGIN = ("FDK_BROKER", "FDK_FYERS_SECRET_KEY", "FDK_FYERS_REDIRECT_URI",
                       "FDK_FYERS_TOTP_SECRET", "FDK_FYERS_PIN", "FDK_FYERS_USERNAME")
ALL_KEYS = list(COMMON) + list(SHOONYA) + list(REMOVED_FYERS_LOGIN) + [
    "FDK_SHOONYA_IMEI", "FDK_SHOONYA_REDIRECT_URI", "FDK_FYERS_CLIENT_ID",
]


@pytest.fixture
def env(monkeypatch):
    for key in ALL_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in {**COMMON, **SHOONYA}.items():
        monkeypatch.setenv(key, value)
    return monkeypatch


def test_shoonya_is_the_only_login(env):
    config = cfg.load_config()

    assert config.shoonya.username == "FA12345"
    assert config.shoonya.vendor_code == "FA12345_U"
    assert config.shoonya.api_key == "k" * 64
    assert config.shoonya.imei == "abc1234"          # default when FDK_SHOONYA_IMEI is unset
    assert not hasattr(config, "broker")
    assert not hasattr(config, "fyers")
    assert not hasattr(cfg, "FyersConfig")


def test_shoonya_imei_can_be_overridden(env):
    env.setenv("FDK_SHOONYA_IMEI", "my-laptop-mac")

    assert cfg.load_config().shoonya.imei == "my-laptop-mac"


def test_shoonya_credentials_are_required(env):
    env.delenv("FDK_SHOONYA_PASSWORD")

    with pytest.raises(KeyError, match="FDK_SHOONYA_PASSWORD"):
        cfg.load_config()


@pytest.mark.parametrize("key", list(SHOONYA))
def test_blank_shoonya_credentials_are_rejected(env, key):
    env.setenv(key, "   ")

    with pytest.raises(ValueError, match=key):
        cfg.load_config()


def test_fyers_client_id_is_optional_for_the_dormant_data_socket(env):
    assert cfg.load_config().fyers_client_id == ""

    env.setenv("FDK_FYERS_CLIENT_ID", "APP-100")
    assert cfg.load_config().fyers_client_id == "APP-100"


def test_removed_fyers_login_variables_are_ignored(env):
    for key in REMOVED_FYERS_LOGIN:
        env.setenv(key, "whatever")

    config = cfg.load_config()           # no ValueError for FDK_BROKER, nothing read

    assert not hasattr(config, "broker")
    assert config.shoonya.username == "FA12345"
