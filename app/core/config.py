from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Optional


class Settings(BaseSettings):
    app_name: str = "LLM DQ Suggestions"
    debug: bool = False
    database_url: str = Field(default="sqlite:///./test.db")

    # BFF base URL, e.g., http://api-qupid-dev.qupid.clusterdiali.me
    bff_base_url: str = Field(
        default="https://api.example.com",
        description="Base URL for BFF (without trailing path)",
    )
    # Full profiling endpoint derived from BFF + /api/profiling or /profiling
    external_api_base_url: Optional[str] = None
    external_api_key: str = Field(
        default="", description="API Key for external service"
    )
    external_api_timeout: int = Field(
        default=30, description="Request timeout in seconds"
    )

    log_level: str = "INFO"

    # PostgreSQL settings
    postgres_host: Optional[str] = None
    postgres_port: Optional[int] = None
    postgres_db: Optional[str] = None
    postgres_user: Optional[str] = None
    postgres_password: Optional[str] = None

    # Trino settings
    trino_host: Optional[str] = Field(default="localhost", description="Trino host")
    trino_port: Optional[int] = Field(default=8080, description="Trino port")
    trino_user: Optional[str] = Field(default="trino", description="Trino user")
    trino_catalog: Optional[str] = Field(default="hive", description="Default Trino catalog")
    trino_schema: Optional[str] = Field(default="default", description="Default Trino schema")

    # Accept extra env vars (ignore keys we don't explicitly model)
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()


if not settings.external_api_base_url:
    settings.external_api_base_url = (
        f"{settings.bff_base_url.rstrip('/')}/api/profiling"
    )


def get_postgres_config():
    """Get PostgreSQL configuration dictionary"""
    return {
        "host": settings.postgres_host or "localhost",
        "port": settings.postgres_port or 5432,
        "database": settings.postgres_db or "qupid",
        "user": settings.postgres_user or "postgres",
        "password": settings.postgres_password or "",
    }


def get_trino_config():
    """Get Trino configuration dictionary"""
    return {
        "host": settings.trino_host or "localhost",
        "port": settings.trino_port or 8080,
        "user": settings.trino_user or "trino",
        "catalog": settings.trino_catalog or "hive",
        "schema": settings.trino_schema or "default",
    }