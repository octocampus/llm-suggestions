"""
Trino connection utility with authentication support
"""

from trino.dbapi import connect
from app.core.config import settings, get_trino_config
from app.core.logging import logger


def create_trino_cursor():
    """
    Create and return a Trino cursor with authentication support

    Supports:
    - No auth (auth_type=None)
    - OAuth2 with Keycloak using Bearer token in headers

    Returns:
        Trino cursor object
    """
    try:
        config = get_trino_config()

        # Determine HTTP scheme based on port
        port = config["port"]
        http_scheme = "https" if str(port) in ("443", "8443", "80") else "http"

        # Get Keycloak token if OAuth2 is enabled
        http_headers = {}
        auth_type = (
            config.get("auth_type", "").lower() if config.get("auth_type") else None
        )

        if auth_type == "oauth2":
            if settings.keycloak_server_url and settings.keycloak_trino_client_id:
                from app.services.keycloak_service import get_keycloak_token

                try:
                    token = get_keycloak_token()
                    http_headers["Authorization"] = f"Bearer {token}"
                    logger.info("Using OAuth2 (Keycloak) Bearer token for Trino")
                except Exception as e:
                    logger.error(f"Failed to get Keycloak token: {e}")
                    raise
            else:
                logger.error(
                    "OAuth2 auth requested but Keycloak settings not configured"
                )
                raise ValueError("Missing Keycloak configuration for OAuth2")

        # Create connection with Bearer token in headers
        # When using OAuth2, don't set user (let token determine identity)
        user = None if auth_type == "oauth2" else config["user"]

        conn = connect(
            host=config["host"],
            port=config["port"],
            user=user,
            catalog=config["catalog"],
            schema=config["schema"],
            http_scheme=http_scheme,
            http_headers=http_headers if http_headers else None,
            verify=getattr(settings, "trino_verify_ssl", False),
        )

        cursor = conn.cursor()
        logger.info(
            f"Trino cursor created successfully (host={config['host']}, port={port}, "
            f"catalog={config['catalog']}, scheme={http_scheme}, auth={auth_type or 'none'})"
        )
        return cursor

    except Exception as e:
        logger.error(f"Failed to create Trino cursor: {str(e)}")
        raise Exception(f"Trino connection failed: {str(e)}")
