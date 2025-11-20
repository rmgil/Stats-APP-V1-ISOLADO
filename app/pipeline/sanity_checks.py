"""Sanity checks to keep pipeline outputs consistent.

These helpers operate strictly on produced ``pipeline_result`` artifacts so they
do not interfere with the poker logic itself. They emit warnings when the
stored JSON files diverge from reference expectations or when monthly sums do
not align with their corresponding global result.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


logger = logging.getLogger(__name__)

REFERENCE_TOTALS = {
    "total": 60091,
    "valid": 47897,
    "mystery": 11493,
    "lt4": 697,
}

REFERENCE_STATS = {
    ("nonko_9max", "Early RFI"): (806, 169),
    ("nonko_6max", "Early RFI"): (105, 26),
    ("pko", "Early RFI"): (6349, 1450),
    ("postflop_all", "Flop CBet IP %"): (1394, 1248),
}


def _load_pipeline_result(path: Path) -> Dict:
    if not path.exists():
        raise FileNotFoundError(path)
    return json.loads(path.read_text())


def _stat_counts(result: Dict, group: str, stat: str) -> Tuple[int, int]:
    combined = result.get("combined") or {}
    group_data = combined.get(group) or {}
    stat_sources = [group_data.get("stats") or {}, group_data.get("postflop_stats") or {}]

    for source in stat_sources:
        if stat in source and isinstance(source.get(stat), dict):
            entry = source[stat]
            opps = int(entry.get("opportunities") or entry.get("opps") or 0)
            atts = int(entry.get("attempts") or entry.get("att") or 0)
            return opps, atts

    return 0, 0


def _collect_core_counters(result: Dict) -> Dict[str, int]:
    discards = result.get("aggregated_discards") or {}
    return {
        "total": int(result.get("total_hands") or 0),
        "valid": int(result.get("valid_hands") or 0),
        "mystery": int(discards.get("mystery") or 0),
        "lt4": int(discards.get("less_than_4_players") or 0),
    }


def _collect_reference_stats(result: Dict) -> Dict[Tuple[str, str], Tuple[int, int]]:
    return {key: _stat_counts(result, key[0], key[1]) for key in REFERENCE_STATS}


def log_reference_consistency(global_result_path: Path) -> None:
    """Warn when the stored global result deviates from reference counters."""

    try:
        result = _load_pipeline_result(global_result_path)
    except FileNotFoundError:
        logger.debug("[SANITY] Global result not found at %s", global_result_path)
        return
    except Exception as exc:  # noqa: BLE001 - never break pipeline on logging
        logger.warning("[SANITY] Failed to read %s: %s", global_result_path, exc)
        return

    counters = _collect_core_counters(result)
    if counters.get("total") != REFERENCE_TOTALS["total"]:
        logger.debug(
            "[SANITY] Skipping reference check for %s (total_hands=%s)",
            global_result_path,
            counters.get("total"),
        )
        return

    mismatches: List[str] = []
    for key, expected in REFERENCE_TOTALS.items():
        if counters.get(key) != expected:
            mismatches.append(f"{key} expected={expected} got={counters.get(key)}")

    stats = _collect_reference_stats(result)
    for key, expected_pair in REFERENCE_STATS.items():
        observed = stats.get(key, (0, 0))
        if observed != expected_pair:
            group, stat = key
            mismatches.append(
                f"{group} {stat} expected={expected_pair[1]}/{expected_pair[0]} got={observed[1]}/{observed[0]}"
            )

    if mismatches:
        logger.warning(
            "[SANITY] Reference mismatch for %s: %s",
            global_result_path,
            "; ".join(mismatches),
        )
    else:
        logger.info("[SANITY] Reference counters matched for %s", global_result_path)


def _collect_hand_ids(records: Iterable[Dict]) -> List[str]:
    ids: List[str] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        hand_id = record.get("hand_id")
        if hand_id:
            ids.append(hand_id)
    return ids


def log_monthly_global_consistency(global_path: Path, monthly_paths: Dict[str, Path]) -> None:
    """Compare summed monthly payloads with the stored global payload."""

    try:
        global_payload = _load_pipeline_result(global_path)
    except FileNotFoundError:
        logger.debug("[SANITY] Global payload missing for consistency check: %s", global_path)
        return
    except Exception as exc:  # noqa: BLE001 - warn but do not fail
        logger.warning("[SANITY] Failed to read global payload at %s: %s", global_path, exc)
        return

    monthly_results: Dict[str, Dict] = {}
    for month, path in monthly_paths.items():
        try:
            monthly_results[month] = _load_pipeline_result(path)
        except FileNotFoundError:
            logger.warning("[SANITY] Monthly payload missing for %s at %s", month, path)
        except Exception as exc:  # noqa: BLE001 - continue checking remaining months
            logger.warning("[SANITY] Failed to read monthly payload for %s at %s: %s", month, path, exc)

    if not monthly_results:
        logger.debug("[SANITY] No monthly payloads available to compare against %s", global_path)
        return

    global_counters = _collect_core_counters(global_payload)
    global_stats = _collect_reference_stats(global_payload)
    monthly_totals = {key: 0 for key in REFERENCE_TOTALS}
    monthly_stats = {key: (0, 0) for key in REFERENCE_STATS}

    monthly_ids: Dict[str, set] = {}
    for month, payload in monthly_results.items():
        counters = _collect_core_counters(payload)
        for key in monthly_totals:
            monthly_totals[key] += counters.get(key, 0)

        for key in monthly_stats:
            opps, atts = _stat_counts(payload, key[0], key[1])
            prev_opps, prev_atts = monthly_stats[key]
            monthly_stats[key] = (prev_opps + opps, prev_atts + atts)

        monthly_ids[month] = set(
            _collect_hand_ids(payload.get("valid_hand_records") or [])
        )

    mismatches: List[str] = []
    for key, expected in global_counters.items():
        if monthly_totals.get(key) != expected:
            mismatches.append(
                f"{key} global={expected} monthly_sum={monthly_totals.get(key)}"
            )

    for key, expected in global_stats.items():
        if monthly_stats.get(key) != expected:
            group, stat = key
            observed = monthly_stats.get(key, (0, 0))
            mismatches.append(
                f"{group} {stat} global={expected[1]}/{expected[0]} monthly_sum={observed[1]}/{observed[0]}"
            )

    global_ids = set(_collect_hand_ids(global_payload.get("valid_hand_records") or []))
    monthly_union: set = set()
    duplicate_ids: set = set()
    for month, ids in monthly_ids.items():
        overlap = monthly_union & ids
        if overlap:
            duplicate_ids.update(overlap)
        monthly_union.update(ids)
        if not ids.issubset(global_ids):
            extra = ids - global_ids
            logger.warning(
                "[SANITY] Monthly payload %s includes %s hand(s) not present globally", month, len(extra)
            )

    if duplicate_ids:
        logger.warning("[SANITY] %s hand(s) counted in multiple months", len(duplicate_ids))
    if monthly_union and global_ids and not monthly_union.issubset(global_ids):
        logger.warning(
            "[SANITY] Monthly union larger than global set (%s vs %s)",
            len(monthly_union),
            len(global_ids),
        )

    if mismatches:
        logger.warning(
            "[SANITY] Monthly/global mismatch for %s: %s",
            global_path,
            "; ".join(mismatches),
        )
    else:
        logger.info(
            "[SANITY] Monthly payloads match global counters for %s", global_path
        )
