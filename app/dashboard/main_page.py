# AUDITORIA (main page / leituras de payloads)
# - A pipeline pesada (run_multi_site_pipeline) é executada em background após uploads
#   via SimpleBackgroundWorker/JobService, gravando
#   work/<token>/pipeline_result_GLOBAL.json e pipeline_result_<YYYY-MM>.json, depois
#   publicados em /results/<token>/ e agregados por utilizador em /results/by_user/<id>/
#   por rebuild_user_master_results.
# - As rotas de leitura usam apenas artefactos persistidos:
#   • /api/dashboard/<token> → build_dashboard_payload → ResultStorageService.get_pipeline_result
#   • /api/dashboard/user-month e /api/main → build_user_month_dashboard_payload /
#     build_user_main_dashboard_payload → ResultStorageService (token user-<id> ou meses do utilizador)
#   • downloads de amostras (hands_by_stat) leem ficheiros TXT + metadata já gravados.
# Atualização (changelog):
#   reorganizada a leitura da main/dashboard mensal para usar caches do ResultStorageService
#   e expor subgrupos NONKO/PKO também na aba GERAL, sem reprocessar HHs.
"""FastAPI endpoint dedicated to the dashboard main page aggregation."""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from app.services.user_main_dashboard_service import build_user_main_dashboard_payload
from app.services.upload_service import UploadService
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

    upload_service = UploadService()
    try:
        payload = await run_in_threadpool(build_user_main_dashboard_payload, user_id)
    except Exception:  # noqa: BLE001 - surface controlled error to frontend
        logger.exception("Failed to build main page payload for user %s", user_id)
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": "internal_error",
                "message": "Falha ao carregar dados agregados do utilizador.",
            },
        )

    has_uploads = _user_has_completed_uploads(upload_service, user_id)

    if not payload or not payload.get("meta"):
        return _empty_main_response(has_uploads)

    try:
        data = _format_main_payload(payload)
    except Exception:  # noqa: BLE001 - ensure API never propagates formatting errors
        logger.exception("Failed to format main page payload for user %s", user_id)
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": "main_page_format_error",
                "message": "Falha ao preparar os dados da Main Page.",
            },
        )

    return {"success": True, **data, "has_uploads": has_uploads}


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

def _empty_main_response(has_uploads: bool = False) -> Dict[str, Any]:
    return {
        "success": True,
        "months": [],
        "weights": [],
        "normalized_weights": [],
        "stats": {},
        "meta": {"mode": "user_main_3months"},
        "has_data": bool(has_uploads),
    }


def _format_main_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    meta = payload.get("meta") or {}
    months_info = meta.get("months") or []
    months: List[str] = []
    normalized_weights: List[float] = []
    for entry in months_info:
        if not isinstance(entry, dict):
            continue
        month = entry.get("month")
        if not month:
            continue
        months.append(month)
        weight = entry.get("weight")
        if isinstance(weight, (int, float)):
            normalized_weights.append(float(weight))
        else:
            normalized_weights.append(0.0)

    stats = _build_stats_tree(
        payload.get("groups") or {},
        normalized_weights,
        payload.get("weighted_scores") or {},
    )

    has_uploads = bool(meta.get("has_uploads"))

    return {
        "months": months,
        "weights": meta.get("weights") or [],
        "normalized_weights": normalized_weights,
        "stats": stats,
        "meta": {"mode": meta.get("mode", "user_main_3months")},
        "has_data": _stats_tree_has_data(stats) or has_uploads,
        "token": payload.get("token"),
    }


def _user_has_completed_uploads(upload_service: UploadService, user_id: str) -> bool:
    uploads = upload_service.list_all_uploads(user_id)
    for entry in uploads:
        status = (entry.get("status") or "").lower()
        if status in {"done", "processed", "completed"}:
            return True
    return False


def _build_stats_tree(
    groups_payload: Dict[str, Any],
    normalized_weights: List[float],
    weighted_scores: Dict[str, Any],
) -> Dict[str, Any]:
    stats_tree: Dict[str, Any] = {}

    stats_tree["NONKO"] = {
        "9-max": _format_group_payload(groups_payload.get("nonko_9max"), normalized_weights),
        "6-max": _format_group_payload(groups_payload.get("nonko_6max"), normalized_weights),
    }
    stats_tree["PKO"] = _format_group_payload(groups_payload.get("pko"), normalized_weights)
    stats_tree["POSTFLOP"] = _format_group_payload(groups_payload.get("postflop_all"), normalized_weights)

    geral_stats = _build_general_stats(weighted_scores)
    if geral_stats:
        stats_tree["GERAL"] = geral_stats

    return stats_tree


def _build_general_stats(weighted_scores: Dict[str, Any]) -> Dict[str, Any]:
    def _extract_score(entry: Any) -> float | None:
        if isinstance(entry, dict):
            for key in ("group_score", "overall_score", "score"):
                if entry.get(key) is not None:
                    return entry.get(key)
            return None
        if isinstance(entry, (int, float)):
            return float(entry)
        return None

    components = {}
    for source_key, target_key in (("nonko", "NONKO"), ("pko", "PKO"), ("postflop", "POSTFLOP")):
        value = _extract_score(weighted_scores.get(source_key))
        if value is not None:
            components[target_key] = value

    overall_score = _extract_score(weighted_scores.get("overall"))

    if not components and overall_score is None:
        return {}

    return {
        "overall_score": overall_score,
        "components": components,
    }


def _format_group_payload(
    group_payload: Dict[str, Any] | None, normalized_weights: List[float]
) -> Dict[str, Any] | None:
    if not isinstance(group_payload, dict):
        return None

    stats: Dict[str, Any] = {}
    for stat_name, stat_payload in (group_payload.get("stats") or {}).items():
        stats[stat_name] = _format_stat_entry(stat_payload or {}, normalized_weights)

    return {
        "label": group_payload.get("label", ""),
        "hands_count": group_payload.get("hands_count", 0) or 0,
        "stats": stats,
        "subgroups": group_payload.get("subgroups") or {},
        "overall_score": group_payload.get("overall_score"),
    }


def _format_stat_entry(stat_payload: Dict[str, Any], normalized_weights: List[float]) -> Dict[str, Any]:
    formatted = {
        "score": stat_payload.get("score"),
        "ideal": stat_payload.get("ideal"),
        "opportunities": stat_payload.get("opportunities", 0) or 0,
        "attempts": stat_payload.get("attempts", 0) or 0,
        "sample_total": stat_payload.get("sample_total")
        or stat_payload.get("opportunities")
        or stat_payload.get("opps")
        or 0,
    }

    frequencies = stat_payload.get("frequencies_by_month") or []
    for idx, value in enumerate(frequencies):
        formatted[f"pct_month_{idx + 1}"] = value

    percentage = _compute_weighted_percentage(frequencies, normalized_weights)
    if percentage is not None:
        formatted["percentage"] = percentage

    return formatted


def _compute_weighted_percentage(values: List[Any], weights: List[float]) -> float | None:
    if not values or not weights:
        return None
    numerator = 0.0
    denominator = 0.0
    for value, weight in zip(values, weights):
        if value is None or weight is None or weight <= 0:
            continue
        numerator += float(value) * float(weight)
        denominator += float(weight)

    if denominator <= 0:
        return None
    return numerator / denominator


def _stats_tree_has_data(stats_tree: Dict[str, Any]) -> bool:
    for group in (
        (stats_tree.get("NONKO") or {}).get("9-max"),
        (stats_tree.get("NONKO") or {}).get("6-max"),
        stats_tree.get("PKO"),
        stats_tree.get("POSTFLOP"),
    ):
        if _group_has_data(group):
            return True
    return False


def _group_has_data(group: Dict[str, Any] | None) -> bool:
    if not isinstance(group, dict):
        return False
    if (group.get("hands_count") or 0) > 0:
        return True
    stats = group.get("stats") or {}
    for stat in stats.values():
        if not isinstance(stat, dict):
            continue
        if (stat.get("opportunities") or 0) > 0 or (stat.get("attempts") or 0) > 0:
            return True
        if stat.get("score") is not None:
            return True
        for key, value in stat.items():
            if key.startswith("pct_month_") and value not in (None, 0):
                return True
    return False
