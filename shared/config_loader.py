from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    DATABASE_URL: str
    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60
    REFRESH_TOKEN_EXPIRE_HOURS: int = 10
    AES_SECRET_KEY: str  # 64-char hex string (32 bytes)
    LOCATIONIQ_API_KEY: str
    SMTP_HOST: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_EMAIL: str = ""
    SMTP_APP_PASSWORD: str = ""
    IMAP_HOST: str = "imap.gmail.com"
    IMAP_PORT: int = 993
    RTV_EMAIL_APPROVAL_ENABLED: bool = False
    RTV_EMAIL_APPROVAL_DRY_RUN: bool = False
    RTV_EMAIL_POLL_MINUTES: int = 3
    # Comma-separated emails allowed to approve ANY RTV (admin/test override).
    RTV_EMAIL_EXTRA_APPROVERS: str = ""
    # Public base URL used for magic-link approval buttons in emails.
    APP_BASE_URL: str = "https://new-app-backend-and-ims.onrender.com"
    RTV_ACTION_TOKEN_TTL_DAYS: int = 14
    IMS_JWT_SECRET: str = ""
    IMS_JWT_ALGORITHM: str = "HS256"
    IMS_JWT_EXPIRATION_HOURS: int = 24
    ANTHROPIC_API_KEY: str = ""
    WHATSAPP_ACCESS_TOKEN: str = ""
    WHATSAPP_PHONE_NUMBER_ID: str = ""
    WHATSAPP_RECIPIENT: str = ""
    FRONTEND_URL: str = "https://ims.candorfoods.in"
    BACKEND_URL: str = "https://mmvxmfvhmq.ap-south-1.awsapprunner.com"

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
