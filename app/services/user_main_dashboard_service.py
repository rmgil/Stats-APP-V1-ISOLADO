import logging
from typing import Any, Dict, List, Tuple

from app.api_dashboard import build_user_month_dashboard_payload
from app.services.user_months_service import UserMonthsService

logger = logging.getLogger(__name__)

MONTH_WEIGHTS_3 = [0.5, 0.3, 0.2]
MONTH_WEIGHTS_2 = [0.7, 0.3]
MONTH_WEIGHTS_1 = [1.0]


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


def _aggregate_stat(
    stat_name: str,
    month_payloads: List[Tuple[str, Dict[str, Any]]],
    weights: Dict[str, float],
) -> Dict[str, Any]:
    opportunities_total = 0
    attempts_total = 0
    frequency_by_month: List[Any] = []
    note_weights: List[Tuple[float, float]] = []
    ideal_value = None

    for month, payload in month_payloads:
        stat_data = (
            payload.get("stats", {})
            .get(stat_name, {})
            if isinstance(payload.get("stats"), dict)
            else payload.get("groups", {})
        )
        # If groups are present, search inside each group
        if isinstance(stat_data, dict) and "score" not in stat_data:
            stat_data = None
            for group_data in payload.get("groups", {}).values():
                group_stat = group_data.get("stats", {}).get(stat_name)
                if group_stat:
                    stat_data = group_stat
                    break

        if not isinstance(stat_data, dict):
            frequency_by_month.append(None)
            continue

        opportunities_total += stat_data.get("opportunities", 0) or 0
        attempts_total += stat_data.get("attempts", 0) or 0
        frequency_by_month.append(stat_data.get("percentage"))

        if stat_data.get("ideal") is not None and ideal_value is None:
            ideal_value = stat_data.get("ideal")

        score = stat_data.get("score")
        if score is not None:
            note_weights.append((score, weights.get(month, 0.0)))

    normalized_total = sum(weight for _, weight in note_weights if weight > 0)
    adjusted_weights = [
        (value, weight / normalized_total)
        for value, weight in note_weights
        if weight > 0 and normalized_total > 0
    ]

    note = _weighted_average(adjusted_weights) if adjusted_weights else None

    return {
        "score": note,
        "opportunities": opportunities_total,
        "attempts": attempts_total,
        "frequencies_by_month": frequency_by_month,
        "ideal": ideal_value,
    }


def _merge_group_stats(
    month_payloads: List[Tuple[str, Dict[str, Any]]], weights: Dict[str, float]
) -> Dict[str, Any]:
    aggregated_groups: Dict[str, Any] = {}
    stats_counter = 0

    group_keys = set()
    for _, payload in month_payloads:
        group_keys.update(payload.get("groups", {}).keys())

    for group_name in group_keys:
        group_stats: Dict[str, Any] = {}
        group_hands = 0
        subgroup_scores: Dict[str, List[Tuple[float, float]]] = {}
        group_score_values: List[Tuple[float, float]] = []

        for month, payload in month_payloads:
            group_data = payload.get("groups", {}).get(group_name, {})
            if not isinstance(group_data, dict):
                continue

            group_hands += group_data.get("hands_count", 0) or 0

            for stat_name in group_data.get("stats", {}):
                if stat_name not in group_stats:
                    group_stats[stat_name] = _aggregate_stat(stat_name, month_payloads, weights)
                    stats_counter += 1

            for subgroup, data in (group_data.get("subgroups") or {}).items():
                if data.get("score") is None:
                    continue
                subgroup_scores.setdefault(subgroup, []).append(
                    (data.get("score"), weights.get(month, 0.0))
                )

            if group_data.get("overall_score") is not None:
                group_score_values.append((group_data.get("overall_score"), weights.get(month, 0.0)))

        merged_subgroups = {}
        for subgroup, entries in subgroup_scores.items():
            merged = _weighted_average(entries)
            if merged is not None:
                merged_subgroups[subgroup] = {"score": merged}

        aggregated_groups[group_name] = {
            "label": next(
                (p.get("groups", {}).get(group_name, {}).get("label") for _, p in month_payloads if group_name in p.get("groups", {})),
                group_name,
            ),
            "hands_count": group_hands,
            "stats": group_stats,
            "subgroups": merged_subgroups,
            "overall_score": _weighted_average(group_score_values) or 0,
        }

    logger.debug("[USER_MAIN] Aggregated %s stats across groups", stats_counter)
    return aggregated_groups


def build_user_main_dashboard_payload(user_id: str) -> dict:
    """
    Devolve o payload completo da Main Page do utilizador,
    agregando os últimos 3 meses concluídos com pesos 50/30/20.
    """

    months_service = UserMonthsService()
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
            payload = build_user_month_dashboard_payload(user_id, month)
            month_payloads.append((month, payload))
        except Exception as exc:  # noqa: BLE001 - continue aggregating available months
            logger.warning("[USER_MAIN] Failed to load payload for %s/%s: %s", user_id, month, exc)

    if not month_payloads:
        return {
            "meta": {
                "mode": "user_main_3months",
                "months": [],
                "weights": [],
            },
            "groups": {},
        }

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

    weighted_scores = {}
    months_used = []
    for month, payload in month_payloads:
        month_weights = weights.get(month, 0.0)
        months_used.append({"month": month, "weight": month_weights})

        for group_key in ["nonko", "pko", "postflop", "overall"]:
            source = payload.get("weighted_scores", {}).get(group_key)
            if source is None:
                continue
            weighted_scores.setdefault(group_key, []).append((source, month_weights))

    def merge_group_scores(entries: List[Tuple[Dict[str, Any], float]]):
        scores_list = []
        for data, weight in entries:
            if data is None:
                continue
            score_value = data.get("group_score") if isinstance(data, dict) else None
            if score_value is not None:
                scores_list.append((score_value, weight))
        return _weighted_average(scores_list)

    final_weighted_scores: Dict[str, Any] = {}
    if "nonko" in weighted_scores:
        merged_score = merge_group_scores(weighted_scores["nonko"])
        if merged_score is not None:
            final_weighted_scores["nonko"] = {"group_score": merged_score}
    if "pko" in weighted_scores:
        merged_score = merge_group_scores(weighted_scores["pko"])
        if merged_score is not None:
            final_weighted_scores["pko"] = {"group_score": merged_score}
    if "postflop" in weighted_scores:
        merged_score = merge_group_scores(weighted_scores["postflop"])
        if merged_score is not None:
            final_weighted_scores["postflop"] = {"group_score": merged_score}
    if "overall" in weighted_scores:
        merged_score = merge_group_scores(weighted_scores["overall"])
        if merged_score is not None:
            final_weighted_scores["overall"] = merged_score

    logger.debug(
        "[USER_MAIN] Aggregating months for %s -> months_used=%s weights=%s",
        user_id,
        months_used,
        weights,
    )

    return {
        "meta": {
            "mode": "user_main_3months",
            "months": months_used,
            "weights": base_weights,
        },
        "groups": aggregated_groups,
        "weighted_scores": final_weighted_scores,
    }


if __name__ == "__main__":  # pragma: no cover - dev helper
    test_user_id = "<USER_ID_WITH_UPLOADS>"
    payload = build_user_main_dashboard_payload(test_user_id)
    print("HAS_DATA:", bool(payload))
    if payload:
        print("GROUP_KEYS:", list(payload.get("groups", {}).keys()))
        print("MONTHS:", payload.get("meta", {}).get("months"))
