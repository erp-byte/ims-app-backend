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
    IMS_JWT_SECRET: str = ""
    IMS_JWT_ALGORITHM: str = "HS256"
    IMS_JWT_EXPIRATION_HOURS: int = 24
    ANTHROPIC_API_KEY: str = ""

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
