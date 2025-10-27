"""
Keycloak token service for Trino authentication
"""

from datetime import datetime, timedelta
from threading import Lock
from typing import Optional
import requests
from requests.auth import HTTPBasicAuth
import jwt
from app.core.config import settings
from app.core.logging import logger


class KeycloakTokenService:
    """Manages OAuth2 tokens from Keycloak for Trino"""

    def __init__(self):
        self._lock = Lock()
        self._cached_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None

    def _is_token_valid(self, safety_margin_minutes: int = 2) -> bool:
        """Check if cached token is still valid"""
        if not self._cached_token or not self._token_expires_at:
            return False
        return datetime.now() < self._token_expires_at - timedelta(
            minutes=safety_margin_minutes
        )

    def _decode_token_expiry(self, access_token: str) -> datetime:
        """Extract expiry time from JWT token"""
        try:
            decoded = jwt.decode(access_token, options={"verify_signature": False})
            return datetime.fromtimestamp(decoded["exp"])
        except Exception as e:
            logger.warning(f"Cannot decode JWT expiry: {e}, using 15min fallback")
            return datetime.now() + timedelta(minutes=15)

    def get_service_token(self) -> str:
        """Get service account token using client credentials grant"""
        with self._lock:
            # Return cached token if still valid
            if self._is_token_valid():
                logger.debug("Using cached Keycloak token")
                return self._cached_token

            # Request new token
            try:
                # Validate settings first
                if not settings.keycloak_server_url:
                    raise ValueError("KEYCLOAK_SERVER_URL environment variable not set")
                if not settings.keycloak_realm:
                    raise ValueError("KEYCLOAK_REALM environment variable not set")
                if not settings.keycloak_trino_client_id:
                    raise ValueError(
                        "KEYCLOAK_TRINO_CLIENT_ID environment variable not set"
                    )
                if not settings.keycloak_trino_client_secret:
                    raise ValueError(
                        "KEYCLOAK_TRINO_CLIENT_SECRET environment variable not set"
                    )

                logger.debug(
                    f"Keycloak config: url={settings.keycloak_server_url}, realm={settings.keycloak_realm}"
                )

                token_url = (
                    f"{settings.keycloak_server_url}/realms/{settings.keycloak_realm}"
                    f"/protocol/openid-connect/token"
                )

                data = {
                    "grant_type": "client_credentials",
                    "client_id": settings.keycloak_trino_client_id,
                    "scope": "openid profile email",
                }

                auth = HTTPBasicAuth(
                    settings.keycloak_trino_client_id,
                    settings.keycloak_trino_client_secret,
                )

                logger.info(f"Requesting new Keycloak token from {token_url}")
                response = requests.post(token_url, data=data, auth=auth, timeout=10)
                response.raise_for_status()

                token_response = response.json()
                access_token = token_response["access_token"]

                # Cache the token
                self._cached_token = access_token
                self._token_expires_at = self._decode_token_expiry(access_token)

                logger.info(
                    f"Successfully obtained Keycloak token (expires at {self._token_expires_at})"
                )
                return access_token

            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to get Keycloak token: {e}")
                raise Exception(f"Keycloak authentication failed: {e}")
            except KeyError as e:
                logger.error(f"Malformed token response, missing key: {e}")
                raise Exception(f"Invalid Keycloak token response: {e}")

    def clear_cache(self):
        """Clear cached token (useful after 401 errors)"""
        with self._lock:
            self._cached_token = None
            self._token_expires_at = None
            logger.info("Keycloak token cache cleared")


# Global instance
keycloak_service = KeycloakTokenService()


def get_keycloak_token() -> str:
    """Get a valid Keycloak token for Trino"""
    return keycloak_service.get_service_token()
