"""Utility helpers to retry Supabase calls on rate limiting."""

import logging
import time
from typing import Callable, TypeVar


logger = logging.getLogger(__name__)
T = TypeVar("T")


def _is_rate_limit_error(exc: Exception) -> bool:
    """Return True when the exception looks like a 429/TooManyRequests."""
    status = getattr(exc, "status", None) or getattr(exc, "status_code", None)
    try:
        status = int(status) if status is not None else None
    except Exception:
        status = None

    if status == 429:
        return True

    text = str(exc).lower()
    return "too many requests" in text or "429" in text


def with_supabase_retry(fn: Callable[[], T], *, max_attempts: int = 5) -> T:
    """Execute a callable with exponential backoff on 429 responses."""

    delays = [0.5, 1, 2, 4]
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - need to inspect Supabase errors
            attempt += 1
            if attempt >= max_attempts or not _is_rate_limit_error(exc):
                raise

            delay = delays[min(attempt - 1, len(delays) - 1)]
            logger.warning(
                "Supabase returned 429 (attempt %s/%s). Retrying in %.1fs...",
                attempt,
                max_attempts,
                delay,
            )
            time.sleep(delay)
