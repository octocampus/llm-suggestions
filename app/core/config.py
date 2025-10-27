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
    trino_password: Optional[str] = Field(
        default=None, description="Trino password (if using BasicAuth)"
    )
    trino_catalog: Optional[str] = Field(
        default="hive", description="Default Trino catalog"
    )
    trino_schema: Optional[str] = Field(
        default="default", description="Default Trino schema"
    )
    trino_http_scheme: str = Field(
        default="http", description="HTTP scheme (http or https)"
    )
    trino_auth_type: Optional[str] = Field(
        default=None, description="Auth type: basic, jwt, oauth2, kerberos, or None"
    )

    # Keycloak settings for Trino OAuth2
    keycloak_server_url: Optional[str] = Field(
        default=None, description="Keycloak server URL"
    )
    keycloak_realm: Optional[str] = Field(default=None, description="Keycloak realm")
    keycloak_trino_client_id: Optional[str] = Field(
        default=None, description="Trino client ID"
    )
    keycloak_trino_client_secret: Optional[str] = Field(
        default=None, description="Trino client secret"
    )

    # LLM Settings
    llm_provider: str = Field(
        default="groq", description="LLM provider: groq, openai, anthropic, ollama"
    )
    llm_model: str = Field(default="openai/gpt-oss-120b", description="LLM model name")
    groq_api_key: Optional[str] = Field(default=None, description="Groq API key (free)")
    openai_api_key: Optional[str] = Field(default=None, description="OpenAI API key")
    anthropic_api_key: Optional[str] = Field(
        default=None, description="Anthropic API key"
    )

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

# Auto-enable oauth2 if Keycloak is configured
if (
    settings.keycloak_server_url
    and settings.keycloak_realm
    and settings.keycloak_trino_client_id
    and settings.keycloak_trino_client_secret
    and not settings.trino_auth_type
):
    settings.trino_auth_type = "oauth2"


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
        "password": settings.trino_password,
        "catalog": settings.trino_catalog or "system",
        "schema": settings.trino_schema or "jdbc",
        "http_scheme": settings.trino_http_scheme or "http",
        "auth_type": settings.trino_auth_type,
    }
