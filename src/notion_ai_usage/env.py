"""Environment for Notion web session calls (browser cookie)."""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_DIR = Path(__file__).resolve().parent
_ENV_FILE = _ENV_DIR / ".env"

_COOKIE_CONFIG = SettingsConfigDict(
    env_file=_ENV_FILE if _ENV_FILE.is_file() else None,
    env_file_encoding="utf-8",
    extra="ignore",
    case_sensitive=True,
)


class NotionCookieEnv(BaseSettings):
    """Browser ``Cookie`` header for authenticated Notion web API calls."""

    model_config = _COOKIE_CONFIG

    NOTION_COOKIE: str = Field(
        min_length=1,
        description="Full Cookie header value from the browser (includes token_v2).",
    )
