"""FastAPI endpoint dedicated to the dashboard main page aggregation."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

from fastapi import APIRouter, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool

from app.services.user_main_dashboard_service import build_user_main_dashboard_payload
from app.services.user_months_service import UserMonthsService
from app.api.auth_dependencies import get_current_user
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/main", tags=["main"])


@router.get("")
async def get_main_page(current_user: User = Depends(get_current_user)) -> Dict[str, Any]:
    """Return the aggregated main dashboard payload for the authenticated user."""

    logger.info(f"FASTAPI AUTH USER = {current_user.id}")
    user_id = current_user.get_id() or current_user.id
    if not user_id:
        raise HTTPException(status_code=401, detail="user_not_authenticated")
    user_id = str(user_id)
    payload = await run_in_threadpool(build_user_main_dashboard_payload, user_id)

    if not payload or not payload.get("meta"):
        return {
            "success": True,
            "months": [],
            "weights": [],
            "normalized_weights": [],
            "stats": {},
            "meta": {"mode": "user_main_3months"},
        }

    try:
        data = _format_main_payload(payload)
    except Exception:  # noqa: BLE001 - ensure API never propagates formatting errors
        logger.exception("Failed to format main page payload for user %s", user_id)
        raise HTTPException(status_code=500, detail="main_page_format_error")

    return {"success": True, **data}


@router.get("/debug/months")
async def debug_user_months(current_user: User = Depends(get_current_user)) -> Dict[str, Any]:
    """DEBUG ONLY endpoint exposing the raw months map for troubleshooting."""

    user_id = current_user.get_id() or current_user.id
    if not user_id:
        raise HTTPException(status_code=401, detail="user_not_authenticated")
    user_id = str(user_id)
    months_service = UserMonthsService()
    months_map = months_service.get_user_months_map(user_id)
    return {"success": True, "months": months_map}
