"""FastAPI authentication helpers that reuse Flask session cookies."""
from __future__ import annotations

import hashlib
import logging
import os
from functools import lru_cache
from typing import Any, Dict, Optional

from fastapi import HTTPException, Request
from flask.sessions import TaggedJSONSerializer
from itsdangerous import BadData, BadSignature, URLSafeTimedSerializer

from app.models.user import User

logger = logging.getLogger(__name__)

SESSION_COOKIE_NAME = os.getenv("SESSION_COOKIE_NAME", "session")
SESSION_SECRET = os.getenv("SESSION_SECRET", "dev-secret-key-change-in-production")
SESSION_SALT = "cookie-session"


@lru_cache(maxsize=1)
def _get_session_serializer() -> URLSafeTimedSerializer:
    """Return a serializer compatible with Flask's secure cookie sessions."""

    signer_kwargs = {"key_derivation": "hmac", "digest_method": hashlib.sha1}
    serializer = URLSafeTimedSerializer(
        SESSION_SECRET,
        salt=SESSION_SALT,
        serializer=TaggedJSONSerializer(),
        signer_kwargs=signer_kwargs,
    )
    return serializer


def _decode_flask_session(cookie_value: str) -> Optional[Dict[str, Any]]:
    """Decode the Flask session cookie payload."""

    serializer = _get_session_serializer()
    try:
        return serializer.loads(cookie_value)
    except (BadSignature, BadData) as exc:
        logger.warning("Invalid Flask session cookie: %s", exc)
        return None
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Unexpected error decoding Flask session cookie: %s", exc)
        return None


def _user_from_session_payload(session_payload: Dict[str, Any]) -> Optional[User]:
    """Build a User object from decoded session payload."""

    if not isinstance(session_payload, dict):
        return None

    user_data = session_payload.get("user_data")
    if isinstance(user_data, dict):
        return User(user_data)

    supabase_session = session_payload.get("supabase_session")
    if isinstance(supabase_session, dict):
        user_info = supabase_session.get("user")
        if isinstance(user_info, dict):
            return User(user_info)

    return None


def get_current_user(request: Request) -> User:
    """Return the authenticated user derived from the Flask session cookie."""

    session_cookie = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_cookie:
        raise HTTPException(status_code=401, detail="missing_session_cookie")

    session_payload = _decode_flask_session(session_cookie)
    if not session_payload:
        raise HTTPException(status_code=401, detail="invalid_session_cookie")

    user = _user_from_session_payload(session_payload)
    if not user or not user.get_id():
        raise HTTPException(status_code=401, detail="user_not_authenticated")

    return user
