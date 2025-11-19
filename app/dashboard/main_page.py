"""FastAPI endpoint dedicated to the dashboard main page aggregation."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool

from app.services.user_main_dashboard_service import build_user_main_dashboard_payload
from app.services.user_months_service import UserMonthsService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/main", tags=["main"])


def _require_user_id(request: Request) -> str:
    """Extract the authenticated user identifier from request headers."""

    user_id = request.headers.get("x-user-id") or request.headers.get("x-user-email")
    if not user_id:
        raise HTTPException(status_code=401, detail="missing_user_id")
    return user_id


def _extract_months(meta: Dict[str, Any]) -> Tuple[List[str], List[float], List[float]]:
    """Return ordered months plus base/normalized weight arrays."""

    months: List[str] = []
    normalized_weights: List[float] = []

    for entry in meta.get("months", []) or []:
        month = entry.get("month")
        if not month:
            continue
        months.append(month)
        normalized_weights.append(entry.get("weight", 0.0))

    base_weights = (meta.get("weights") or [])[: len(months)]
    if not base_weights and normalized_weights:
        base_weights = normalized_weights

    return months, base_weights, normalized_weights


def _format_subgroups(subgroups: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    formatted: Dict[str, Dict[str, Any]] = {}
    for name, data in (subgroups or {}).items():
        if not isinstance(data, dict):
            continue
        formatted[name] = {"score": data.get("score")}
    return formatted


def _format_stats(stats: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    formatted: Dict[str, Dict[str, Any]] = {}

    for stat_name, stat_data in (stats or {}).items():
        if not isinstance(stat_data, dict):
            continue

        entry = {
            "score": stat_data.get("score"),
            "ideal": stat_data.get("ideal"),
            "opportunities": stat_data.get("opportunities", 0),
            "attempts": stat_data.get("attempts", 0),
        }

        frequencies = stat_data.get("frequencies_by_month") or []
        for idx in range(3):
            key = f"pct_month_{idx + 1}"
            entry[key] = frequencies[idx] if idx < len(frequencies) else None

        formatted[stat_name] = entry

    return formatted


def _format_group(group_data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(group_data, dict):
        return {}

    return {
        "label": group_data.get("label"),
        "hands_count": group_data.get("hands_count", 0),
        "overall_score": group_data.get("overall_score"),
        "subgroups": _format_subgroups(group_data.get("subgroups", {})),
        "stats": _format_stats(group_data.get("stats", {})),
    }


def _format_overall(weighted_scores: Dict[str, Any]) -> Dict[str, Any]:
    overall_entry = weighted_scores.get("overall")
    overall_score = overall_entry
    if isinstance(overall_entry, dict):
        overall_score = overall_entry.get("group_score")

    return {
        "overall_score": overall_score,
        "components": {
            "NONKO": (weighted_scores.get("nonko") or {}).get("group_score"),
            "PKO": (weighted_scores.get("pko") or {}).get("group_score"),
            "POSTFLOP": (weighted_scores.get("postflop") or {}).get("group_score"),
        },
    }


def _build_stats_tree(payload: Dict[str, Any]) -> Dict[str, Any]:
    groups = payload.get("groups") or {}
    weighted_scores = payload.get("weighted_scores") or {}

    return {
        "NONKO": {
            "9-max": _format_group(groups.get("nonko_9max")),
            "6-max": _format_group(groups.get("nonko_6max")),
        },
        "PKO": _format_group(groups.get("pko")),
        "POSTFLOP": _format_group(groups.get("postflop_all")),
        "GERAL": _format_overall(weighted_scores),
    }


def _format_main_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    meta = payload.get("meta") or {}
    months, base_weights, normalized_weights = _extract_months(meta)
    stats = _build_stats_tree(payload)

    return {
        "months": months,
        "weights": base_weights,
        "normalized_weights": normalized_weights,
        "stats": stats,
        "meta": {"mode": meta.get("mode")},
    }


@router.get("")
async def get_main_page(request: Request) -> Dict[str, Any]:
    """Return the aggregated main dashboard payload for the authenticated user."""

    user_id = _require_user_id(request)
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
async def debug_user_months(request: Request) -> Dict[str, Any]:
    """DEBUG ONLY endpoint exposing the raw months map for troubleshooting."""

    user_id = _require_user_id(request)
    months_service = UserMonthsService()
    months_map = months_service.get_user_months_map(user_id)
    return {"success": True, "months": months_map}
