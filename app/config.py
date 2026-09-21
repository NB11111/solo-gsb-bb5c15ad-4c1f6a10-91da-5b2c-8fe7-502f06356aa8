from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration, overridable via environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="CAL_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    app_name: str = Field(default="calibration-timeline-service")
    # Host/port the container listens on (documented in README and Compose).
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8080)
    # All data is kept in memory; this flag only advertises the storage mode.
    log_level: str = Field(default="info")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
