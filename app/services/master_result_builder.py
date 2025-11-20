"""Utilities to rebuild a consolidated master payload per user.

This module scans all active uploads for a user, loads their processed
pipeline results, aggregates the data, and writes a dashboard-compatible
payload under ``results/by_user/<user_id>``.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from app.pipeline.multi_site_runner import (
    _aggregate_month_groups,
)
from app.pipeline.sanity_checks import log_monthly_global_consistency, log_reference_consistency
from app.services.result_storage import ResultStorageService
from app.services.upload_service import UploadService
from app.stats.aggregate import MultiSiteAggregator
from app.services.storage import get_storage
from app.stats.stat_categories import (
    BB_DEFENSE_STATS,
    BVB_STATS,
    RFI_STATS,
    SB_DEFENSE_STATS,
    SQUEEZE_STATS,
    THREEBET_CC_STATS,
    VS_3BET_STATS,
)
from app.score.bb_defense_scorer import BBDefenseScorer
from app.score.bvb_scorer import calculate_bvb_scores
from app.score.sb_defense_scorer import SBDefenseScorer
from app.score.squeeze_scorer import SqueezeScorer
from app.score.threbet_cc_scorer import calculate_3bet_cc_scores
from app.score.vs_3bet_scorer import calculate_vs_3bet_scores
from app.stats.stat_categories import filter_stats

logger = logging.getLogger(__name__)


def _deduplicate_valid_records(records: Iterable[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, set]]:
    """Deduplicate valid_hand_records preserving first occurrence."""

    seen: set[str] = set()
    deduped: List[Dict[str, Any]] = []
    group_map: Dict[str, set] = defaultdict(set)

    for record in records:
        hand_id = record.get("hand_id")
        group_key = record.get("group")
        if not hand_id or hand_id in seen:
            continue

        seen.add(hand_id)
        deduped.append(record)
        if group_key:
            group_map[group_key].add(hand_id)

    return deduped, group_map


def _merge_discards(results: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    aggregated: Dict[str, int] = defaultdict(int)

    for result in results:
        for reason, count in (result.get("aggregated_discards") or {}).items():
            aggregated[reason] += int(count or 0)

    if aggregated and "total" not in aggregated:
        aggregated["total"] = sum(v for k, v in aggregated.items() if k != "total")

    if not aggregated:
        aggregated["total"] = 0

    return dict(aggregated)


def _build_score_blocks(group: str, aggregated_stats: Dict[str, Any]) -> Dict[str, Any]:
    """Compute category score blocks mirroring pipeline aggregation."""

    from app.stats.scoring_calculator import ScoringCalculator

    scoring_calc = ScoringCalculator()
    bvb_calc = calculate_bvb_scores
    squeeze_calc = SqueezeScorer()
    bb_defense_scorer = BBDefenseScorer()
    sb_defense_scorer = SBDefenseScorer()

    rfi_stats = filter_stats(aggregated_stats, RFI_STATS)
    bvb_stats = filter_stats(aggregated_stats, BVB_STATS)
    threbet_stats = filter_stats(aggregated_stats, THREEBET_CC_STATS)
    vs_3bet_stats = filter_stats(aggregated_stats, VS_3BET_STATS)
    squeeze_stats = filter_stats(aggregated_stats, SQUEEZE_STATS)
    bb_defense_stats = filter_stats(aggregated_stats, BB_DEFENSE_STATS)
    sb_defense_stats = filter_stats(aggregated_stats, SB_DEFENSE_STATS)

    if "9max" in group:
        table_format = "9max"
    elif "6max" in group:
        table_format = "6max"
    else:
        table_format = "PKO"

    return {
        "rfi": scoring_calc.calculate_group_scores(group, rfi_stats) if rfi_stats else {},
        "bvb": bvb_calc(bvb_stats, table_format) if bvb_stats else {},
        "threbet_cc": calculate_3bet_cc_scores(threbet_stats, table_format) if threbet_stats else {},
        "vs_3bet": calculate_vs_3bet_scores(vs_3bet_stats, table_format) if vs_3bet_stats else {},
        "squeeze": squeeze_calc.calculate_squeeze_scores(squeeze_stats, group) if squeeze_stats else {},
        "bb_defense": bb_defense_scorer.calculate_bb_defense_scores(bb_defense_stats, bvb_stats, group) if bb_defense_stats else {},
        "sb_defense": sb_defense_scorer.calculate_group_score(sb_defense_stats, table_format) if sb_defense_stats else {},
    }


def _merge_combined_groups(
    aggregator: MultiSiteAggregator,
    all_groups: set,
    group_id_sets: Dict[str, set],
    output_dir: Path,
    group_sources: Dict[str, set],
) -> Dict[str, Any]:
    """Re-run the combined aggregation on the provided aggregator."""

    combined = _aggregate_month_groups(aggregator, str(output_dir), all_groups)

    for group_key, group_data in combined.items():
        group_data["hand_count"] = len(group_id_sets.get(group_key, set()))
        if "postflop_hands_count" not in group_data:
            group_data["postflop_hands_count"] = group_data["hand_count"]
        group_data["sites_included"] = sorted(group_sources.get(group_key, set()))
        group_data["scores"] = _build_score_blocks(group_key, group_data.get("stats", {}))

    return combined


def _merge_sites_placeholder(group_id_sets: Dict[str, set]) -> Dict[str, Any]:
    """Build a minimal sites map compatible with dashboard counters."""

    if not group_id_sets:
        return {}

    return {
        "all": {
            group: {"hand_count": len(hand_ids)}
            for group, hand_ids in group_id_sets.items()
        }
    }


def _merge_pipeline_results(
    result_entries: List[Tuple[str, Dict[str, Any]]],
    *,
    month_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Merge a list of pipeline_result payloads into a single structure."""

    aggregator = MultiSiteAggregator()
    valid_records: List[Dict[str, Any]] = []
    hands_per_month: Dict[str, int] = defaultdict(int)
    total_hands = 0
    valid_hands = 0
    group_sources: Dict[str, set] = defaultdict(set)

    for token, result in result_entries:
        total_hands += int(result.get("total_hands") or 0)
        valid_hands += int(result.get("valid_hands") or 0)

        month_counts = result.get("hands_per_month") or {}
        if not isinstance(month_counts, dict):
            month_counts = {}

        if month_key:
            if month_key in month_counts:
                hands_per_month[month_key] += int(month_counts.get(month_key) or 0)
        else:
            for month, count in month_counts.items():
                hands_per_month[month] += int(count or 0)

        if result.get("valid_hand_records"):
            valid_records.extend(result["valid_hand_records"])

        for site_name, site_groups in (result.get("sites") or {}).items():
            if not isinstance(site_groups, dict):
                continue

            for group_key, group_info in site_groups.items():
                if not isinstance(group_info, dict):
                    continue

                site_key = f"{site_name}__{token}"
                aggregator.add_site_results(
                    site_key,
                    group_key,
                    group_info.get("stats", {}),
                    group_info.get("hands_by_stat", {}),
                )
                group_sources[group_key].add(site_name)

    if month_key:
        filtered_records = []
        for record in valid_records:
            record_month = record.get("month") if isinstance(record, dict) else None
            if record_month and record_month != month_key:
                continue
            filtered_records.append(record)
        valid_records = filtered_records

    deduped_records, group_id_sets = _deduplicate_valid_records(valid_records)
    aggregated_discards = _merge_discards(result for _, result in result_entries)
    all_groups = set(group_id_sets.keys()) or set(aggregator.group_hand_ids.keys())

    output_dir = Path("work") / "by_user_temp"
    output_dir.mkdir(parents=True, exist_ok=True)
    combined_groups = _merge_combined_groups(aggregator, all_groups, group_id_sets, output_dir, group_sources)

    total_valid = len({record.get("hand_id") for record in deduped_records})
    total_discard = aggregated_discards.get("total", 0)

    payload: Dict[str, Any] = {
        "status": "completed",
        "multi_site": True,
        "combined": combined_groups,
        "valid_hand_records": deduped_records,
        "valid_hands": total_valid,
        "total_hands": total_valid + total_discard,
        "aggregated_discards": aggregated_discards,
        "classification": {
            "discarded_hands": aggregated_discards,
            "total_hands": total_valid + total_discard,
            "valid_hands": total_valid,
        },
        "hands_per_month": dict(hands_per_month),
        "sites": _merge_sites_placeholder(group_id_sets),
    }

    if month_key:
        payload["month"] = month_key
        payload["multi_month"] = False
    else:
        payload["multi_month"] = len(hands_per_month) > 1

    return payload


def _upload_user_results_to_storage(output_root: Path, user_id: str) -> None:
    """Upload consolidated user results to Supabase Storage when available."""

    storage = get_storage()
    if not storage.use_cloud:
        logger.debug("[MASTER] Storage running in local mode; skipping upload for %s", user_id)
        return

    storage_prefix = f"/results/by_user/{user_id}"
    uploaded = 0

    for file_path in output_root.rglob("*.json"):
        relative = file_path.relative_to(output_root)
        storage_path = f"{storage_prefix}/{relative}".replace("\\", "/")
        try:
            with open(file_path, "rb") as handle:
                storage.upload_fileobj(handle, storage_path, "application/json")
            uploaded += 1
        except Exception as exc:  # noqa: BLE001 - continue uploading remaining files
            logger.warning(
                "[MASTER] Failed to upload %s to %s: %s",
                file_path,
                storage_path,
                exc,
            )

    logger.info(
        "[MASTER] Uploaded %s aggregated artifact(s) for user %s to %s",
        uploaded,
        user_id,
        storage_prefix,
    )


def rebuild_user_master_results(user_id: str) -> Path:
    """Rebuild consolidated dashboard artifacts for all active uploads of a user."""

    upload_service = UploadService()
    result_service = ResultStorageService()

    uploads = upload_service.list_active_uploads(user_id)
    tokens = [u.get("token") for u in uploads if u.get("token")]

    if not tokens:
        raise ValueError(f"No active uploads found for user {user_id}")

    results: List[Tuple[str, Dict[str, Any]]] = []
    months_map: Dict[str, List[Tuple[str, Dict[str, Any]]]] = defaultdict(list)

    for token in tokens:
        result = result_service.get_pipeline_result(token)
        if not result:
            logger.warning("[MASTER] Skipping token %s (no pipeline_result found)", token)
            continue

        results.append((token, result))

        months_from_global = []
        try:
            hands_per_month = result.get("hands_per_month") or {}
            if isinstance(hands_per_month, dict):
                months_from_global = [
                    month_key
                    for month_key, count in hands_per_month.items()
                    if month_key and int(count or 0) > 0
                ]
        except Exception:
            months_from_global = []

        if not months_from_global:
            try:
                months_info = result_service.list_available_months(token)
            except Exception:
                months_info = []
            months_from_global = [
                month_entry.get("month")
                for month_entry in months_info
                if isinstance(month_entry, dict) and month_entry.get("month")
            ]

        for month_key in months_from_global:
            if not month_key:
                continue
            try:
                month_result = result_service.get_pipeline_result(token, month=month_key)
            except FileNotFoundError:
                continue
            if month_result:
                months_map[month_key].append((token, month_result))

    if not results:
        raise ValueError(f"No pipeline results available for user {user_id}")

    master_payload = _merge_pipeline_results(results)

    output_root = Path("results") / "by_user" / str(user_id)
    output_root.mkdir(parents=True, exist_ok=True)

    global_path = output_root / "pipeline_result_global.json"
    global_path.write_text(json.dumps(master_payload, indent=2), encoding="utf-8")

    global_upper_path = output_root / "pipeline_result_GLOBAL.json"
    global_upper_path.write_text(json.dumps(master_payload, indent=2), encoding="utf-8")

    try:
        log_reference_consistency(global_upper_path)
    except Exception as exc:  # noqa: BLE001 - log but continue
        logger.warning("[MASTER] Reference check failed for %s: %s", user_id, exc)

    month_entries = []
    for month_key, month_results in months_map.items():
        merged_month = _merge_pipeline_results(month_results, month_key=month_key)
        month_entries.append(
            {
                "month": month_key,
                "total_hands": merged_month.get("total_hands"),
                "valid_hands": merged_month.get("valid_hands"),
                "has_data": merged_month.get("valid_hands", 0) > 0,
            }
        )

        month_path = output_root / f"pipeline_result_{month_key}.json"
        month_path.write_text(json.dumps(merged_month, indent=2), encoding="utf-8")

        legacy_dir = output_root / "months" / month_key
        legacy_dir.mkdir(parents=True, exist_ok=True)
        (legacy_dir / "pipeline_result.json").write_text(
            json.dumps(merged_month, indent=2), encoding="utf-8"
        )

    if month_entries:
        months_manifest = {"months": sorted(month_entries, key=lambda x: x.get("month", ""))}
        (output_root / "months_manifest.json").write_text(
            json.dumps(months_manifest, indent=2), encoding="utf-8"
        )

    monthly_paths = {
        entry["month"]: output_root / f"pipeline_result_{entry['month']}.json"
        for entry in month_entries
        if entry.get("month")
    }

    try:
        log_monthly_global_consistency(global_upper_path, monthly_paths)
    except Exception as exc:  # noqa: BLE001 - keep aggregation resilient
        logger.warning(
            "[MASTER] Failed to run monthly/global sanity check for %s: %s", user_id, exc
        )

    logger.info(
        "[MASTER] Built master payload for user %s with %s uploads and %s valid hands at %s",
        user_id,
        len(results),
        master_payload.get("valid_hands", 0),
        output_root,
    )

    try:
        _upload_user_results_to_storage(output_root, user_id)
    except Exception as exc:  # noqa: BLE001 - never break caller due to upload errors
        logger.warning("[MASTER] Failed to upload aggregated artifacts for %s: %s", user_id, exc)

    return output_root

