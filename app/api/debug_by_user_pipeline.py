from __future__ import annotations

import logging
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException

from app.api.auth_dependencies import get_current_user
from app.models.user import User
from app.services.result_storage import ResultStorageService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/debug/by-user/pipeline", tags=["debug"])


def _extract_totals(result: Dict[str, Any]) -> Dict[str, int]:
    discards = result.get("aggregated_discards") or {}
    return {
        "total_hands": int(result.get("total_hands") or 0),
        "valid_hands": int(result.get("valid_hands") or 0),
        "mystery_hands": int(discards.get("mystery") or 0),
        "short_handed_hands": int(discards.get("less_than_4_players") or 0),
    }


def _extract_interest_stats(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    combined = result.get("combined") or {}
    stats: List[Dict[str, Any]] = []

    for group_key, group_data in combined.items():
        if not isinstance(group_data, dict):
            continue

        for source_key in ("stats", "postflop_stats"):
            for stat_name, stat_payload in (group_data.get(source_key) or {}).items():
                if not isinstance(stat_payload, dict):
                    continue

                label = str(stat_payload.get("label") or stat_name)
                label_lower = label.lower()
                if "early rfi" not in label_lower and "flop cbet ip" not in label_lower:
                    continue

                stats.append(
                    {
                        "group": group_key,
                        "stat": label,
                        "attempts": int(stat_payload.get("attempts") or stat_payload.get("att") or 0),
                        "opportunities": int(
                            stat_payload.get("opportunities")
                            or stat_payload.get("opps")
                            or 0
                        ),
                    }
                )

    return stats


def _get_user_id(current_user: User) -> str:
    user_id = current_user.get_id() if hasattr(current_user, "get_id") else None
    user_id = user_id or getattr(current_user, "id", None)
    if not user_id:
        raise HTTPException(status_code=401, detail="user_not_authenticated")
    return str(user_id)


@router.get("/global")
async def debug_user_global_pipeline(
    current_user: User = Depends(get_current_user),
) -> Dict[str, Any]:
    user_id = _get_user_id(current_user)
    storage = ResultStorageService()

    try:
        result = storage.get_pipeline_result(f"user-{user_id}")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="pipeline_result_global_missing")
    except Exception as exc:  # noqa: BLE001 - surface clear error for debugging
        logger.exception("[DEBUG] Failed to load global pipeline result for %s", user_id)
        raise HTTPException(status_code=500, detail=str(exc))

    if not result:
        raise HTTPException(status_code=404, detail="pipeline_result_global_missing")

    return {
        "success": True,
        "scope": "GLOBAL",
        "totals": _extract_totals(result),
        "stats": _extract_interest_stats(result),
    }


@router.get("/month/{month}")
async def debug_user_month_pipeline(
    month: str,
    current_user: User = Depends(get_current_user),
) -> Dict[str, Any]:
    user_id = _get_user_id(current_user)
    storage = ResultStorageService()

    try:
        result = storage.get_pipeline_result(f"user-{user_id}", month=month)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="pipeline_result_month_missing")
    except Exception as exc:  # noqa: BLE001 - explicit error for debugging
        logger.exception(
            "[DEBUG] Failed to load monthly pipeline result for %s/%s", user_id, month
        )
        raise HTTPException(status_code=500, detail=str(exc))

    if not result:
        raise HTTPException(status_code=404, detail="pipeline_result_month_missing")

    return {
        "success": True,
        "scope": month,
        "totals": _extract_totals(result),
        "stats": _extract_interest_stats(result),
    }
