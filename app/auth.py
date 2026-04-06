"""JWT authentication for WebSocket connections."""

import logging
from typing import Optional
from urllib.parse import parse_qs, unquote

from iot_auth.core import decode_jwt, InvalidTokenError, extract_token_from_header
from iot_auth.types import JWTPayload

from app.config import get_settings

logger = logging.getLogger(__name__)


def get_token_from_query_string(query_string: Optional[str]) -> Optional[str]:
    """
    Extract JWT token from WebSocket query string.

    WebSocket clients send token as ?token=<jwt>

    Args:
        query_string: The query string from WebSocket connection

    Returns:
        The token string if found, None otherwise
    """
    if not query_string:
        return None

    # Parse query string using urllib: "token=abc123&other=value"
    try:
        params = parse_qs(query_string)
        # parse_qs returns lists of values, get first value
        token = params.get("token", [None])[0]
        if token:
            return unquote(token)
    except Exception:
        return None

    return None


async def authenticate_websocket(
    query_string: Optional[str],
    headers: dict,
) -> JWTPayload:
    """
    Authenticate WebSocket connection using JWT token.

    Looks for token in:
    1. Query string: ?token=<jwt>
    2. Authorization header: Authorization: Bearer <jwt>

    Args:
        query_string: Query string from WebSocket connection
        headers: Headers dictionary from WebSocket connection

    Returns:
        JWTPayload: Decoded JWT payload if valid

    Raises:
        InvalidTokenError: If token is missing or invalid
    """
    settings = get_settings()

    # Try to get token from query string first (common for WebSocket)
    token = get_token_from_query_string(query_string)
    logger.debug(
        "websocket.auth.query_string",
        extra={"query_string": query_string, "token_found": bool(token)},
    )

    # Fallback to Authorization header
    if not token:
        auth_header = headers.get("authorization") or headers.get("Authorization")
        token = extract_token_from_header(auth_header)

    if not token:
        logger.warning(
            "websocket.auth.missing_token",
            extra={
                "query_string": query_string,
                "has_auth_header": bool(
                    headers.get("authorization") or headers.get("Authorization")
                ),
            },
        )
        raise InvalidTokenError("Missing JWT token")

    try:
        payload = decode_jwt(token, settings.jwt_secret_key)
        logger.info(
            "websocket.auth.success",
            extra={"user_id": payload.get("sub"), "username": payload.get("username")},
        )
        return payload
    except InvalidTokenError as e:
        logger.warning("websocket.auth.invalid_token", extra={"error": str(e)})
        raise
