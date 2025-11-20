import logging
import time
from typing import Any, Dict, List, Set, Tuple

from app.api_dashboard import build_user_month_dashboard_payload
from app.services.result_storage import ResultStorageService
from app.services.upload_service import UploadService
from app.services.user_months_service import UserMonthsService

# Debug helper used by /api/debug/user_main_state to inspect user dashboard state

logger = logging.getLogger(__name__)

from app.api_dashboard import calculate_weighted_scores_from_groups

MONTH_WEIGHTS_3 = [0.5, 0.3, 0.2]
MONTH_WEIGHTS_2 = [0.7, 0.3]
MONTH_WEIGHTS_1 = [1.0]


def _has_completed_uploads(user_id: str, upload_service: UploadService | None = None) -> bool:
    service = upload_service or UploadService()
    uploads = service.list_all_uploads(user_id)
    for entry in uploads:
        status = (entry.get("status") or "").lower()
        if status in {"done", "processed", "completed"}:
            return True
    return False


def _sorted_months_desc(months: List[str]) -> List[str]:
    def sort_key(month: str) -> Tuple[str, str]:
        if month == "unknown":
            return ("0000-00", "")
        return (month, "")

    return sorted(months, key=sort_key, reverse=True)


def _select_months(months: List[str]) -> Tuple[List[str], List[float]]:
    filtered = [m for m in months if m]
    sorted_months = _sorted_months_desc(filtered)
    selected = sorted_months[:3]

    if len(selected) == 3:
        weights = MONTH_WEIGHTS_3
    elif len(selected) == 2:
        weights = MONTH_WEIGHTS_2
    elif len(selected) == 1:
        weights = MONTH_WEIGHTS_1
    else:
        weights = []

    return selected, weights


def get_user_main_month_weights(user_id: str) -> List[Dict[str, float]]:
    """
    Return the months and normalized weights used on the user's main dashboard.

    The selection follows the same rules as the main payload aggregation: take
    the last 3 available months (excluding the current month) with base weights
    50/30/20 (or 70/30 for 2 months, 100% for a single month) and normalize the
    weights so they sum to 1.0.
    """

    months_service = UserMonthsService()
    months_map = months_service.get_user_months_map(user_id)

    selected_months, base_weights = _select_months(list(months_map.keys()))
    if not selected_months:
        return []

    total_weight = sum(base_weights[: len(selected_months)])
    normalized_weights = []
    if total_weight > 0:
        normalized_weights = [weight / total_weight for weight in base_weights[: len(selected_months)]]
    else:
        normalized_weights = [0.0 for _ in selected_months]

    return [
        {"month": month, "weight": weight}
        for month, weight in zip(selected_months, normalized_weights)
    ]


def _normalized_weights(values: List[Tuple[float, float]]) -> float:
    total_weight = sum(weight for _, weight in values if weight > 0)
    if total_weight <= 0:
        return 0.0
    return total_weight


def _weighted_average(values: List[Tuple[float, float]]) -> float | None:
    total_weight = _normalized_weights(values)
    if total_weight <= 0:
        return None
    return sum(value * weight for value, weight in values if weight > 0) / total_weight


def _aggregate_stat_for_group(
    group_name: str,
    stat_name: str,
    month_payloads: List[Tuple[str, Dict[str, Any]]],
    weights: Dict[str, float],
) -> Dict[str, Any]:
    """Aggregate a stat for a specific group across all available months."""

    opportunities_total = 0
    attempts_total = 0
    frequency_by_month: List[Any] = []
    note_weights: List[Tuple[float, float]] = []
    ideal_value = None

    for month, payload in month_payloads:
        group_data = payload.get("groups", {}).get(group_name)
        if not isinstance(group_data, dict):
            frequency_by_month.append(None)
            continue

        stat_data = (group_data.get("stats") or {}).get(stat_name)
        if not isinstance(stat_data, dict):
            frequency_by_month.append(None)
            continue

        opps_value = stat_data.get("opportunities")
        if opps_value is None:
            opps_value = stat_data.get("opps")
        attempts_value = stat_data.get("attempts")
        if attempts_value is None:
            attempts_value = stat_data.get("att")

        opportunities_total += int(opps_value or 0)
        attempts_total += int(attempts_value or 0)

        pct_value = stat_data.get("percentage")
        if pct_value is None:
            pct_value = stat_data.get("pct")
        frequency_by_month.append(pct_value if pct_value is not None else None)

        if ideal_value is None and stat_data.get("ideal") is not None:
            ideal_value = stat_data.get("ideal")

        score = stat_data.get("score")
        if score is not None:
            note_weights.append((float(score), weights.get(month, 0.0)))

    note = _weighted_average(note_weights)

    return {
        "score": note,
        "opportunities": opportunities_total,
        "attempts": attempts_total,
        "sample_total": opportunities_total,
        "frequencies_by_month": frequency_by_month,
        "ideal": ideal_value,
    }


def _aggregate_subgroup_stat(
    group_name: str,
    subgroup_name: str,
    stat_name: str,
    month_payloads: List[Tuple[str, Dict[str, Any]]],
    weights: Dict[str, float],
) -> Dict[str, Any]:
    """Aggregate stats that live inside subgroup structures (e.g., POSTFLOP tree)."""

    opportunities_total = 0
    attempts_total = 0
    frequency_by_month: List[Any] = []
    pct_weights: List[Tuple[float, float]] = []
    stat_score_weights: List[Tuple[float, float]] = []
    ideal_value = None
    stat_weight = None

    for month, payload in month_payloads:
        group_data = payload.get("groups", {}).get(group_name)
        if not isinstance(group_data, dict):
            frequency_by_month.append(None)
            continue

        subgroup_data = (group_data.get("subgroups") or {}).get(subgroup_name)
        if not isinstance(subgroup_data, dict):
            frequency_by_month.append(None)
            continue

        stats_dict = subgroup_data.get("stats")
        if not isinstance(stats_dict, dict):
            frequency_by_month.append(None)
            continue

        stat_data = stats_dict.get(stat_name)
        if not isinstance(stat_data, dict):
            frequency_by_month.append(None)
            continue

        opps_value = stat_data.get("opportunities")
        if opps_value is None:
            opps_value = stat_data.get("opps")
        attempts_value = stat_data.get("attempts")
        if attempts_value is None:
            attempts_value = stat_data.get("att")

        opportunities_total += int(opps_value or 0)
        attempts_total += int(attempts_value or 0)

        pct_value = stat_data.get("percentage")
        if pct_value is None:
            pct_value = stat_data.get("pct")
        frequency_by_month.append(pct_value if pct_value is not None else None)
        if pct_value is not None:
            pct_weights.append((float(pct_value), weights.get(month, 0.0)))

        if ideal_value is None and stat_data.get("ideal") is not None:
            ideal_value = stat_data.get("ideal")

        if stat_weight is None and stat_data.get("weight") is not None:
            stat_weight = stat_data.get("weight")

        stat_score = stat_data.get("score")
        if stat_score is not None:
            stat_score_weights.append((float(stat_score), weights.get(month, 0.0)))

    aggregated_pct = _weighted_average(pct_weights)
    aggregated_score = _weighted_average(stat_score_weights)

    return {
        "opps": opportunities_total,
        "opportunities": opportunities_total,
        "att": attempts_total,
        "attempts": attempts_total,
        "sample_total": opportunities_total,
        "pct": aggregated_pct,
        "percentage": aggregated_pct,
        "score": aggregated_score,
        "ideal": ideal_value,
        "weight": stat_weight,
        "frequencies_by_month": frequency_by_month,
    }


def _merge_group_stats(
    month_payloads: List[Tuple[str, Dict[str, Any]]], weights: Dict[str, float]
) -> Dict[str, Any]:
    aggregated_groups: Dict[str, Any] = {}

    group_keys = set()
    for _, payload in month_payloads:
        group_keys.update(payload.get("groups", {}).keys())

    for group_name in group_keys:
        stat_names: Set[str] = set()
        group_hands = 0
        label = None
        subgroup_scores: Dict[str, List[Tuple[float, float]]] = {}
        subgroup_metadata: Dict[str, Dict[str, Any]] = {}
        subgroup_stat_names: Dict[str, Set[str]] = {}
        subgroup_keys: Set[str] = set()
        group_score_values: List[Tuple[float, float]] = []

        for month, payload in month_payloads:
            group_data = payload.get("groups", {}).get(group_name)
            if not isinstance(group_data, dict):
                continue

            group_hands += int(group_data.get("hands_count", 0) or 0)

            if label is None and group_data.get("label"):
                label = group_data.get("label")

            stats_dict = group_data.get("stats")
            if isinstance(stats_dict, dict):
                stat_names.update(stats_dict.keys())

            for subgroup, data in (group_data.get("subgroups") or {}).items():
                if not isinstance(data, dict):
                    continue
                subgroup_keys.add(subgroup)
                meta = subgroup_metadata.setdefault(subgroup, {})
                if not meta:
                    for key in ("label", "weight"):
                        if data.get(key) is not None:
                            meta[key] = data.get(key)
                score = data.get("score")
                if score is None:
                    continue
                subgroup_scores.setdefault(subgroup, []).append(
                    (float(score), weights.get(month, 0.0))
                )

                stats_dict = data.get("stats")
                if isinstance(stats_dict, dict):
                    subgroup_stat_names.setdefault(subgroup, set()).update(stats_dict.keys())

            if group_data.get("overall_score") is not None:
                group_score_values.append((float(group_data.get("overall_score")), weights.get(month, 0.0)))

        group_stats: Dict[str, Any] = {}
        for stat_name in stat_names:
            group_stats[stat_name] = _aggregate_stat_for_group(
                group_name, stat_name, month_payloads, weights
            )

        merged_subgroups: Dict[str, Any] = {}
        all_subgroup_names = set(subgroup_keys) | set(subgroup_scores.keys())
        for subgroup in all_subgroup_names:
            entries = subgroup_scores.get(subgroup, [])
            merged_score = _weighted_average(entries) if entries else None
            entry = dict(subgroup_metadata.get(subgroup, {}))
            entry["score"] = merged_score

            stat_names_for_subgroup = subgroup_stat_names.get(subgroup)
            if stat_names_for_subgroup:
                stats_payload = {}
                for stat_name in stat_names_for_subgroup:
                    stats_payload[stat_name] = _aggregate_subgroup_stat(
                        group_name, subgroup, stat_name, month_payloads, weights
                    )
                entry["stats"] = stats_payload

            merged_subgroups[subgroup] = entry

        aggregated_groups[group_name] = {
            "label": label or group_name,
            "hands_count": group_hands,
            "stats": group_stats,
            "subgroups": merged_subgroups,
            "overall_score": _weighted_average(group_score_values),
        }

    logger.debug("[USER_MAIN] Aggregated groups: %s", list(aggregated_groups.keys()))
    return aggregated_groups


def get_or_build_month_dashboard_payload(
    user_id: str,
    month: str,
    *,
    result_storage: ResultStorageService | None = None,
) -> dict:
    """Load cached monthly payload or build and persist it on-demand."""

    storage = result_storage or ResultStorageService()
    start = time.monotonic()

    try:
        cached = storage.load_month_dashboard_payload(user_id, month)
        if cached:
            logger.info(
                "[USER_MONTH] Loaded cached dashboard payload for %s/%s in %.2fs",
                user_id,
                month,
                time.monotonic() - start,
            )
            return cached
    except Exception as exc:  # noqa: BLE001 - fallback to build
        logger.debug(
            "[USER_MONTH] Failed to load cached month payload for %s/%s: %s",
            user_id,
            month,
            exc,
        )

    payload = build_user_month_dashboard_payload(
        user_id, month, use_cache=False, result_storage=storage
    )

    if payload:
        try:
            storage.save_month_dashboard_payload(user_id, month, payload)
        except Exception as exc:  # noqa: BLE001 - cache best-effort
            logger.debug(
                "[USER_MONTH] Failed to persist month payload for %s/%s: %s",
                user_id,
                month,
                exc,
            )

    logger.info(
        "[USER_MONTH] Built dashboard payload for %s/%s in %.2fs",
        user_id,
        month,
        time.monotonic() - start,
    )
    return payload or {}


def get_or_build_main_dashboard_payload(
    user_id: str,
    *,
    result_storage: ResultStorageService | None = None,
    months_service: UserMonthsService | None = None,
    upload_service: UploadService | None = None,
) -> dict:
    """Return cached main payload or rebuild it by aggregating monthly caches."""

    storage = result_storage or ResultStorageService()
    months_service = months_service or UserMonthsService()
    upload_service = upload_service or UploadService()
    start = time.monotonic()

    try:
        cached = storage.load_main_dashboard_payload(user_id)
        if cached:
            logger.info(
                "[USER_MAIN] Loaded cached main payload for %s in %.2fs",
                user_id,
                time.monotonic() - start,
            )
            return cached
    except Exception as exc:  # noqa: BLE001 - rebuild on cache errors
        logger.debug(
            "[USER_MAIN] Failed to load cached main payload for %s: %s", user_id, exc
        )

    months_map = months_service.get_user_months_map(user_id)
    all_months = list(months_map.keys())

    logger.debug("[USER_MAIN] Months available for %s: %s", user_id, sorted(all_months))

    selected_months, base_weights = _select_months(all_months)
    logger.debug(
        "[USER_MAIN] Selected months for %s: %s with base weights=%s",
        user_id,
        selected_months,
        base_weights,
    )

    month_payloads: List[Tuple[str, Dict[str, Any]]] = []
    for month in selected_months:
        try:
            payload = get_or_build_month_dashboard_payload(
                user_id, month, result_storage=storage
            )
            if payload:
                month_payloads.append((month, payload))
        except Exception as exc:  # noqa: BLE001 - continue aggregating available months
            logger.warning("[USER_MAIN] Failed to load payload for %s/%s: %s", user_id, month, exc)

    has_uploads = _has_completed_uploads(user_id, upload_service)

    if not month_payloads:
        payload = {
            "meta": {
                "mode": "user_main_3months",
                "months": [],
                "weights": base_weights,
                "has_uploads": has_uploads,
            },
            "groups": {},
            "has_data": False,
        }
        try:
            storage.save_main_dashboard_payload(user_id, payload)
        except Exception:  # noqa: BLE001 - optional cache write
            logger.debug("[USER_MAIN] Skipped saving empty main payload for %s", user_id)
        return payload

    weights: Dict[str, float] = {}
    total_weight = 0.0
    for month, weight in zip(selected_months, base_weights):
        if any(month == entry[0] for entry in month_payloads):
            weights[month] = weight
            total_weight += weight

    if total_weight > 0:
        for month in list(weights.keys()):
            weights[month] = weights[month] / total_weight

    logger.debug("[USER_MAIN] Normalized weights: %s", weights)

    aggregated_groups = _merge_group_stats(month_payloads, weights)
    logger.debug("[USER_MAIN] Aggregated groups count for %s: %s", user_id, len(aggregated_groups))

    weighted_scores = calculate_weighted_scores_from_groups(
        aggregated_groups, aggregated_groups.get("postflop_all")
    )
    logger.debug(
        "[USER_MAIN] Weighted scores for %s -> keys=%s", user_id, list(weighted_scores.keys())
    )

    months_used = []
    for month, payload in month_payloads:
        month_weights = weights.get(month, 0.0)
        months_used.append({"month": month, "weight": month_weights})

    payload = {
        "meta": {
            "mode": "user_main_3months",
            "months": months_used,
            "weights": base_weights,
            "has_uploads": has_uploads,
        },
        "groups": aggregated_groups,
        "weighted_scores": weighted_scores,
        "has_data": True,
    }

    try:
        storage.save_main_dashboard_payload(user_id, payload)
    except Exception as exc:  # noqa: BLE001 - cache best-effort
        logger.debug(
            "[USER_MAIN] Failed to persist main payload for %s: %s", user_id, exc
        )

    logger.info(
        "[USER_MAIN] Built main dashboard payload for %s in %.2fs",
        user_id,
        time.monotonic() - start,
    )
    return payload


def build_user_main_dashboard_payload(user_id: str) -> dict:
    """
    Devolve o payload completo da Main Page do utilizador,
    agregando os últimos 3 meses concluídos com pesos 50/30/20.
    """

    return get_or_build_main_dashboard_payload(user_id)


def get_user_main_debug_snapshot(
    user_id: str,
    result_storage,
    user_months_service,
    uploads_repo=None,
):
    """
    Returns a debug snapshot of the user's dashboard-related state:
    - uploads and their statuses
    - existence of pipeline results (global and per-month)
    - existence of cached main/monthly dashboards
    - months detected by UserMonthsService
    """

    storage = result_storage or ResultStorageService()
    months_service = user_months_service or UserMonthsService()
    uploads_service = uploads_repo or UploadService()

    snapshot = {
        "user_id": user_id,
        "uploads": [],
        "pipeline_results": {
            "global_exists": False,
            "months": {},
        },
        "dashboard_cache": {
            "main_exists": False,
            "monthly": {},
        },
        "months_service": {
            "months_map": {},
        },
    }

    try:
        uploads = uploads_service.list_all_uploads(user_id)
        for upload in uploads:
            snapshot["uploads"].append(
                {
                    "id": getattr(upload, "id", None) or (upload.get("id") if isinstance(upload, dict) else None),
                    "status": getattr(upload, "status", None) or (upload.get("status") if isinstance(upload, dict) else None),
                    "created_at": getattr(upload, "created_at", None)
                    or (upload.get("created_at") if isinstance(upload, dict) else None),
                    "updated_at": getattr(upload, "updated_at", None)
                    or (upload.get("updated_at") if isinstance(upload, dict) else None),
                    "client_upload_token": getattr(upload, "client_upload_token", None)
                    or (upload.get("client_upload_token") if isinstance(upload, dict) else None),
                }
            )
    except Exception:  # noqa: BLE001 - debug snapshot is best-effort
        logger.exception("[USER_DEBUG] Failed to list uploads for user %s", user_id)

    months_map = {}
    try:
        months_map = months_service.get_user_months_map(user_id) or {}
    except Exception:  # noqa: BLE001 - continue with empty months map
        logger.exception("[USER_DEBUG] Failed to load months map for user %s", user_id)

    snapshot["months_service"]["months_map"] = months_map if isinstance(months_map, dict) else {}

    known_months = list(snapshot["months_service"]["months_map"].keys())
    user_token = f"user-{user_id}"

    try:
        global_result = storage.get_pipeline_result(user_token)
        snapshot["pipeline_results"]["global_exists"] = bool(global_result)
    except FileNotFoundError:
        snapshot["pipeline_results"]["global_exists"] = False
    except Exception:  # noqa: BLE001 - ignore failures but log
        logger.exception("[USER_DEBUG] Failed to read global pipeline result for %s", user_token)

    for month in known_months:
        try:
            month_result = storage.get_pipeline_result(user_token, month=month)
            snapshot["pipeline_results"]["months"][month] = {"exists": bool(month_result)}
        except FileNotFoundError:
            snapshot["pipeline_results"]["months"][month] = {"exists": False}
        except Exception:  # noqa: BLE001 - continue with best-effort data
            snapshot["pipeline_results"]["months"][month] = {"exists": False}
            logger.exception(
                "[USER_DEBUG] Failed to read pipeline result for %s/%s", user_token, month
            )

    try:
        main_cache = storage.load_main_dashboard_payload(user_id)
        snapshot["dashboard_cache"]["main_exists"] = bool(main_cache)
    except Exception:  # noqa: BLE001 - keep going when cache is missing
        logger.exception("[USER_DEBUG] Failed to load main dashboard cache for %s", user_id)

    for month in known_months:
        try:
            month_cache = storage.load_month_dashboard_payload(user_id, month)
            snapshot["dashboard_cache"]["monthly"][month] = {"exists": bool(month_cache)}
        except Exception:  # noqa: BLE001 - ignore cache errors but log
            snapshot["dashboard_cache"]["monthly"][month] = {"exists": False}
            logger.exception(
                "[USER_DEBUG] Failed to load month dashboard cache for %s/%s", user_id, month
            )

    return snapshot


# Mini-changelog (caching strategy):
# - Main dashboard payloads are cached at /results/by_user/<id>/main_dashboard.json
#   and are rebuilt on-demand from cached monthly payloads when missing.
# - Monthly dashboard payloads live under /results/by_user/<id>/months/dashboard_month_<YYYY-MM>.json
#   and are generated from precomputed pipeline_result_<YYYY-MM>.json without rereading HHs.
# - has_data now honours real uploads (status done/processed/completed) to avoid false
#   "Ainda não carregaste nenhum ficheiro" when pipeline results exist but caches are cold.


if __name__ == "__main__":  # pragma: no cover - dev helper
    test_user_id = "<USER_ID_WITH_UPLOADS>"
    payload = build_user_main_dashboard_payload(test_user_id)
    print("HAS_DATA:", bool(payload))
    if payload:
        print("GROUP_KEYS:", list(payload.get("groups", {}).keys()))
        print("MONTHS:", payload.get("meta", {}).get("months"))
