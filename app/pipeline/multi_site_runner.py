"""
Multi-site pipeline runner that extends the new_runner to handle multiple poker sites
and supports monthly bucketing for multi-month uploads.

# NOTE:
# Monthly pipeline results are now built by reusing the exact same global pipeline
# on a per-month subset of the hands. There is no separate logic for counting
# valid/mystery/<4 or stat opportunities – everything goes through the global builder.
#
# DEBUG SUMMARY (global pipeline): the global counters were drifting because some
# sites ended up with hands that were neither marked as válidas/mystery/<4 nor
# reflected in any room summary. The pipeline now tracks raw vs parsed hands,
# forces every parsed hand into either a discard bucket or “discarded_no_reason”,
# and publishes per-room totals alongside a new /api/debug/global-stats/<token>
# endpoint so we can verify the counts end-to-end.
"""
import os
import json
import logging
import shutil
import hashlib
from collections import defaultdict
from pathlib import Path
from typing import Dict, Any, Tuple, List, Optional, Set
from app.pipeline.global_samples import (
    GlobalSamples,
    build_global_samples,
    POSTFLOP_GROUP_KEY,
)
from app.pipeline.sanity_checks import (
    log_monthly_global_consistency,
    log_reference_consistency,
)
from app.pipeline.pipeline_result import build_pipeline_result_payload
from app.pipeline.new_runner import run_simplified_pipeline
from app.stats.aggregate import MultiSiteAggregator
from app.parse.site_parsers.site_detector import detect_poker_site
from app.pipeline.month_bucketizer import (
    MonthBucket,
    build_month_buckets,
    generate_months_manifest,
    resolve_month_for_file,
)
from app.parse.runner import ParserRunner
from app.services.tournament_repository import TournamentRepository
from app.partition.months import month_key_from_datetime, normalize_month_key, parse_timestamp
from app.stats.stat_categories import (
    BB_DEFENSE_STATS,
    BVB_STATS,
    POSTFLOP_KEYWORDS,
    RFI_STATS,
    SB_DEFENSE_STATS,
    SQUEEZE_STATS,
    THREEBET_CC_STATS,
    VS_3BET_STATS,
    filter_stats,
    filter_stats_by_keyword,
)

logger = logging.getLogger(__name__)

SUMMARY_KEYWORDS = ("summary", "resumo", "- summary")


def _is_summary_filename(file_name: str) -> bool:
    """Return True when the file name indicates a tournament summary."""
    lower_name = file_name.lower()
    return any(keyword in lower_name for keyword in SUMMARY_KEYWORDS)


def _is_dev_environment() -> bool:
    """Return True when running in a development environment."""
    env_candidates = [
        os.getenv("ENVIRONMENT", ""),
        os.getenv("FLASK_ENV", ""),
        os.getenv("APP_ENV", ""),
    ]
    return any(value and value.lower().startswith("dev") for value in env_candidates)


def _count_hands_per_month(records: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)

    for record in records or []:
        month_key: Optional[str] = None
        ts = record.get("timestamp_utc") if isinstance(record, dict) else None
        dt = parse_timestamp(ts) if ts else None
        if dt:
            month_key = month_key_from_datetime(dt)
        elif isinstance(record, dict):
            month_key = normalize_month_key(record.get("month"))

        if month_key:
            counts[month_key] += 1

    return dict(counts)


def _empty_debug_totals() -> Dict[str, Any]:
    return {
        'raw_lines': 0,
        'parsed_hands': 0,
        'valid_hands': 0,
        'mystery_hands': 0,
        'lt4_hands': 0,
        'discarded_no_reason': 0,
        'by_room': {},
    }


def _merge_debug_totals(target: Dict[str, Any], source: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not source:
        return target

    target['raw_lines'] += int(source.get('raw_lines') or source.get('parsed_hands') or 0)
    target['parsed_hands'] += int(source.get('parsed_hands') or 0)
    target['valid_hands'] += int(source.get('valid_hands') or 0)
    target['mystery_hands'] += int(source.get('mystery_hands') or 0)
    target['lt4_hands'] += int(source.get('lt4_hands') or 0)
    target['discarded_no_reason'] += int(source.get('discarded_no_reason') or 0)

    for room, counts in (source.get('by_room') or {}).items():
        room_bucket = target['by_room'].setdefault(room, {
            'valid_hands': 0,
            'total_hands': 0,
            'mystery_hands': 0,
            'lt4_hands': 0,
            'discarded_no_reason': 0,
        })

        room_bucket['valid_hands'] += int(counts.get('valid_hands') or 0)
        room_bucket['total_hands'] += int(counts.get('total_hands') or counts.get('parsed_hands') or 0)
        room_bucket['mystery_hands'] += int(counts.get('mystery_hands') or 0)
        room_bucket['lt4_hands'] += int(counts.get('lt4_hands') or 0)
        room_bucket['discarded_no_reason'] += int(counts.get('discarded_no_reason') or 0)

    return target


def _reconcile_debug_totals(debug_totals: Dict[str, Any], discard_counts: Optional[Dict[str, int]]) -> Dict[str, Any]:
    discard_counts = discard_counts or {}
    discard_total = int(discard_counts.get('total') or sum(
        int(v or 0) for k, v in discard_counts.items() if k != 'total'
    ))

    missing = debug_totals['parsed_hands'] - (debug_totals['valid_hands'] + discard_total)
    if missing > debug_totals.get('discarded_no_reason', 0):
        debug_totals['discarded_no_reason'] = missing

    debug_totals['raw_lines'] = max(debug_totals.get('raw_lines', 0), debug_totals['parsed_hands'])
    return debug_totals


def _validate_category_counts(valid_hand_records: List[Dict[str, Any]], total_valid_hands: int) -> None:
    """Assert that subgroup counts never exceed the total number of valid hands."""
    if not _is_dev_environment() or not valid_hand_records:
        return

    nonko_ids = {r['hand_id'] for r in valid_hand_records if r.get('group') in ('nonko_9max', 'nonko_6max')}
    pko_ids = {r['hand_id'] for r in valid_hand_records if r.get('group') == 'pko'}

    # Ensure no overlaps and that subgroup counts are bounded by the total
    overlap = nonko_ids & pko_ids
    assert not overlap, f"Hands counted in both NON-KO and PKO: {len(overlap)}"
    assert len(nonko_ids) + len(pko_ids) <= total_valid_hands, (
        f"Category counts exceed total valid hands ({len(nonko_ids)} + {len(pko_ids)} > {total_valid_hands})"
    )


def _validate_month_totals(hands_per_month: Dict[str, int], total_valid_hands: int) -> None:
    """Assert that the sum of monthly hands equals the reported total."""
    if not _is_dev_environment() or not hands_per_month:
        return

    month_sum = sum(hands_per_month.values())
    assert month_sum == total_valid_hands, (
        f"Monthly hand totals ({month_sum}) do not match global valid hands ({total_valid_hands})"
    )


def _write_month_pipeline_result(bucket: MonthBucket, month_result: Dict[str, Any]) -> None:
    """Persist the monthly pipeline_result payload to disk."""

    month_work_path = Path(bucket.work_dir)
    month_work_path.mkdir(parents=True, exist_ok=True)

    month_result_path = month_work_path / "pipeline_result.json"
    month_result_path.write_text(json.dumps(month_result, indent=2), encoding='utf-8')

    root_dir = month_work_path.parents[1] if len(month_work_path.parents) >= 2 else month_work_path
    legacy_path = root_dir / f"pipeline_result_{bucket.month}.json"
    legacy_path.write_text(json.dumps(month_result, indent=2), encoding='utf-8')

def _read_text_file(path: Path) -> str:
    try:
        return path.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        return path.read_text(encoding='latin-1', errors='ignore')


def build_aggregate_pipeline_result(
    token: str,
    result_data: Dict[str, Any],
    work_dir: str,
    *,
    aggregated_discards: Dict[str, int],
    hands_per_month: Dict[str, int],
    global_samples: GlobalSamples,
    global_debug: Dict[str, Any],
    postflop_sample: Optional[Any],
    manifest_builder: Any,
    bucket_results: Optional[Dict[str, Any]] = None,
    months_manifest: Optional[Dict[str, Any]] = None,
    multi_month: bool = False,
) -> Dict[str, Any]:
    """
    Constrói e persiste o pipeline_result GLOBAL.

    Qualquer falha aqui deve ser considerada grave – não há captura de exceções
    neste nível.
    """

    extra_payload: Dict[str, Any] = {
        'token': token,
        'debug': global_debug,
        'rooms': global_debug.get('by_room', {}),
    }

    if multi_month:
        extra_payload.update(
            {
                'months': bucket_results or {},
                'months_manifest': months_manifest or {},
                'multi_month': True,
            }
        )

    result_payload = build_pipeline_result_payload(
        combined=result_data.get('combined', {}),
        valid_hand_records=result_data.get('valid_hand_records', []),
        aggregated_discards=aggregated_discards,
        sites=result_data.get('sites', {}),
        hands_per_month=hands_per_month,
        postflop_hands_count=postflop_sample.hand_count if postflop_sample else None,
        samples=global_samples,
        extra=extra_payload,
    )

    manifest = manifest_builder.get_combined_manifest()
    if aggregated_discards:
        manifest['discards'] = aggregated_discards

    manifest_path = os.path.join(work_dir, "multi_site_manifest.json")
    with open(manifest_path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2)

    global_result_path = os.path.join(work_dir, "pipeline_result_global.json")
    with open(global_result_path, 'w', encoding='utf-8') as f:
        json.dump(result_payload, f, indent=2)
    logger.info(f"[{token}] ✅ Wrote global pipeline_result to {global_result_path}")

    global_result_upper = os.path.join(work_dir, "pipeline_result_GLOBAL.json")
    with open(global_result_upper, 'w', encoding='utf-8') as f:
        json.dump(result_payload, f, indent=2)
    logger.info(f"[{token}] ✅ Wrote GLOBAL pipeline_result to {global_result_upper}")

    log_reference_consistency(Path(global_result_upper))

    return result_payload


def build_monthly_results_for_token(
    token: str,
    work_dir: str,
    result_data: Dict[str, Any],
    buckets: List[MonthBucket],
    bucket_results: Dict[str, Dict[str, Any]],
    bucket_lookup: Dict[str, MonthBucket],
    global_debug: Dict[str, Any],
    global_aggregator: MultiSiteAggregator,
    global_groups: Set[str],
    progress_callback=None,
) -> Dict[str, Any]:
    """
    Constrói os resultados mensais para um token.

    Qualquer falha aqui deve ser LOGADA mas nunca propagada.
    """

    logger.info(
        "[MONTHLY] Building monthly results for token=%s with months=%s",
        token,
        list(bucket_results.keys()),
    )

    context: Dict[str, Any] = {
        'result_data': result_data,
        'hands_per_month': _count_hands_per_month(result_data.get('valid_hand_records', [])),
        'global_samples': None,
        'postflop_sample': None,
        'months_manifest': {},
        'bucket_results': bucket_results,
        'global_discards': {},
    }

    try:
        logger.info(f"[{token}] Aggregating global results across all months")

        if progress_callback:
            progress_callback(80, 'A agregar resultados globais...')

        global_combined = _aggregate_month_groups(global_aggregator, work_dir, global_groups)

        group_id_sets: Dict[str, set] = defaultdict(set)
        for record in result_data.get('valid_hand_records', []):
            group_id_sets[record['group']].add(record['hand_id'])

        for group, group_data in global_combined.items():
            total_postflop_hands = 0

            for month_result in result_data['months'].values():
                if month_result.get('status') == 'completed':
                    for site_data in month_result.get('sites', {}).values():
                        if group in site_data:
                            total_postflop_hands += site_data[group].get('postflop_hands_count', 0)

            group_data['hand_count'] = len(group_id_sets.get(group, set()))
            group_data['postflop_hands_count'] = total_postflop_hands

        result_data['combined'] = global_combined

        logger.info(f"[{token}] Writing global cross-format postflop outputs")
        global_aggregator.write_cross_format_postflop_outputs(work_dir)

        months_manifest = generate_months_manifest(token, buckets, bucket_results)
        result_data['months_manifest'] = months_manifest

        manifest_path = os.path.join(work_dir, "months_manifest.json")
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(months_manifest, f, indent=2)

        global_discards: Dict[str, int] = {}
        hands_per_month: Dict[str, int] = {}

        for month_key, month_result in result_data['months'].items():
            if month_result.get('status') != 'completed':
                continue

            month_records = month_result.get('valid_hand_records', [])
            month_counts = _count_hands_per_month(month_records)
            if not month_counts and month_key:
                month_counts = {
                    month_key: len({record['hand_id'] for record in month_records})
                }

            for key, count in month_counts.items():
                hands_per_month[key] = count

            month_discards = month_result.get('aggregated_discards', {})
            for reason, count in month_discards.items():
                if reason == 'total':
                    continue
                global_discards[reason] = global_discards.get(reason, 0) + count

        global_samples = build_global_samples(
            result_data.get('valid_hand_records', []),
            global_discards,
        )

        global_debug = _reconcile_debug_totals(global_debug, global_samples.discard_counts)

        logger.info(
            f"[{token}] Global classification totals: valid_hands={global_samples.validas}, "
            f"discarded={global_samples.discard_counts.get('total', 0)}, "
            f"total={global_samples.total_encontradas}"
        )

        for group_key, group_sample in global_samples.groups.items():
            if group_key == POSTFLOP_GROUP_KEY:
                continue
            if group_key in result_data.get('combined', {}):
                result_data['combined'][group_key]['hand_count'] = group_sample.hand_count

        postflop_sample = global_samples.groups.get(POSTFLOP_GROUP_KEY)
        if postflop_sample:
            assert postflop_sample.hand_count == global_samples.validas, (
                "POSTFLOP group count must match total valid hands"
            )

        monthly_summaries: List[GlobalSamples] = []
        monthly_valid_union: Set[str] = set()

        for month_key, month_result in result_data['months'].items():
            if month_result.get('status') != 'completed':
                continue

            month_records = month_result.get('valid_hand_records', [])
            month_hand_ids = {
                record.get('hand_id')
                for record in month_records
                if isinstance(record, dict) and record.get('hand_id')
            }

            month_samples = global_samples.restrict_to(
                month_hand_ids,
                month_result.get('aggregated_discards'),
            )

            monthly_summaries.append(month_samples)
            monthly_valid_union.update(month_hand_ids)

            combined_groups = month_result.get('combined', {})
            for group_key, group_sample in month_samples.groups.items():
                if group_key == POSTFLOP_GROUP_KEY:
                    continue
                if group_key in combined_groups:
                    combined_groups[group_key]['hand_count'] = group_sample.hand_count

            postflop_month = month_samples.groups.get(POSTFLOP_GROUP_KEY)

            month_counts = _count_hands_per_month(month_records)
            if not month_counts and month_key:
                month_counts = {month_key: month_samples.validas}

            updated_month = build_pipeline_result_payload(
                combined=combined_groups,
                valid_hand_records=month_result.get('valid_hand_records', []),
                aggregated_discards=month_samples.discard_counts,
                sites=month_result.get('sites', {}),
                hands_per_month=month_counts,
                month=month_key,
                postflop_hands_count=postflop_month.hand_count if postflop_month else None,
                samples=month_samples,
                extra={
                    'sites_detected': month_result.get('sites_detected', []),
                    'discarded_hands': month_result.get('discarded_hands', {}),
                    'debug': month_result.get('debug'),
                    'rooms': month_result.get('rooms', {}),
                },
            )

            for key, count in month_counts.items():
                hands_per_month[key] = count
            bucket_results[month_key] = updated_month
            bucket = bucket_lookup.get(month_key)
            if bucket:
                _write_month_pipeline_result(bucket, updated_month)

        if monthly_summaries:
            total_valid_months = sum(summary.validas for summary in monthly_summaries)
            total_hands_months = sum(
                summary.total_encontradas for summary in monthly_summaries
            )
            total_mystery = sum(summary.mystery for summary in monthly_summaries)
            total_lt4 = sum(summary.lt4_players for summary in monthly_summaries)

            logger.info(
                "[%s] Global vs monthly sanity check: total=%s/%s valid=%s/%s mystery=%s/%s lt4=%s/%s",
                token,
                global_samples.total_encontradas,
                total_hands_months,
                global_samples.validas,
                total_valid_months,
                global_samples.mystery,
                total_mystery,
                global_samples.lt4_players,
                total_lt4,
            )

            global_valid_id_set = set(global_samples.valid_hand_ids)

            if global_samples.total_encontradas != total_hands_months:
                logger.warning(
                    "[%s] Monthly total hands mismatch (%s != %s)",
                    token,
                    total_hands_months,
                    global_samples.total_encontradas,
                )

            if global_samples.validas != total_valid_months:
                logger.warning(
                    "[%s] Monthly valid hand sum mismatch (%s != %s)",
                    token,
                    total_valid_months,
                    global_samples.validas,
                )

            if global_samples.mystery != total_mystery:
                logger.warning(
                    "[%s] Monthly mystery sum mismatch (%s != %s)",
                    token,
                    total_mystery,
                    global_samples.mystery,
                )

            if global_samples.lt4_players != total_lt4:
                logger.warning(
                    "[%s] Monthly <4 player sum mismatch (%s != %s)",
                    token,
                    total_lt4,
                    global_samples.lt4_players,
                )

            if monthly_valid_union != global_valid_id_set:
                logger.warning(
                    "[%s] Monthly valid hand IDs do not match global set", token
                )

        if hands_per_month:
            logger.info("[%s] [DEBUG] hands_per_month global: %s", token, hands_per_month)

        _validate_category_counts(result_data.get('valid_hand_records', []), global_samples.validas)
        _validate_month_totals(hands_per_month, global_samples.validas)

        context.update(
            {
                'result_data': result_data,
                'hands_per_month': hands_per_month,
                'global_samples': global_samples,
                'postflop_sample': postflop_sample,
                'months_manifest': months_manifest,
                'bucket_results': bucket_results,
                'global_discards': global_samples.discard_counts,
            }
        )

    except Exception:
        logger.exception("[MONTHLY] Failed to build monthly results for token=%s", token)

        fallback_samples = build_global_samples(
            result_data.get('valid_hand_records', []),
            {},
        )

        # NOTE: Monthly failures must never break the global pipeline. Ensure
        # downstream consumers always receive usable samples.
        context.update(
            {
                'global_samples': fallback_samples,
                'postflop_sample': fallback_samples.groups.get(POSTFLOP_GROUP_KEY),
                'hands_per_month': context.get('hands_per_month') or {},
                'months_manifest': context.get('months_manifest') or {},
                'bucket_results': context.get('bucket_results') or {},
                'global_discards': fallback_samples.discard_counts,
            }
        )

    return context


def _ingest_tournaments_for_user(
    user_key: str,
    source_dir: Path,
    repository: TournamentRepository,
    parser_runner: ParserRunner,
    token: str,
) -> Dict[str, Any]:
    summary = {
        'total_files': 0,
        'stored': 0,
        'replaced': 0,
        'months': {},
    }

    for file_path in source_dir.rglob('*.txt'):
        if not file_path.is_file():
            continue

        if _is_summary_filename(file_path.name):
            logger.info("[%s] Skipping summary-like file: %s", token, file_path.name)
            continue

        summary['total_files'] += 1

        content = _read_text_file(file_path)
        metadata = parser_runner.extract_tournament_metadata(content, file_id=file_path.name) or {}

        month = resolve_month_for_file(content, file_path, metadata)
        tournament_id = metadata.get('tournament_id')

        if not tournament_id:
            digest = hashlib.sha1(content.encode('utf-8', errors='ignore')).hexdigest()[:12]
            tournament_id = f"{file_path.stem}_{digest}"

        stored_path, replaced = repository.store_tournament(
            user_key,
            month,
            tournament_id,
            content,
            file_path.name,
        )

        if replaced:
            summary['replaced'] += 1
            logger.info(
                "[%s] Replaced tournament %s for %s (stored at %s)",
                token,
                tournament_id,
                month,
                stored_path,
            )
        else:
            summary['stored'] += 1
            logger.info(
                "[%s] Stored new tournament %s for %s (stored at %s)",
                token,
                tournament_id,
                month,
                stored_path,
            )

        summary['months'][month] = summary['months'].get(month, 0) + 1

    logger.info(
        "[%s] Tournament ingest summary for %s: total=%s, stored=%s, replaced=%s",
        token,
        user_key,
        summary['total_files'],
        summary['stored'],
        summary['replaced'],
    )

    return summary


def detect_sites_in_directory(directory: str) -> Dict[str, List[str]]:
    """
    Detect which poker sites are present in the extracted files
    
    Returns:
        Dictionary mapping site names to list of file paths
    """
    site_files = {}
    
    # Scan all txt files in directory
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(('.txt', '.xml')):
                if _is_summary_filename(file):
                    logger.info("Ignoring summary-like file during detection: %s", file)
                    continue
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        # Read first 5KB to detect site
                        content = f.read(5000)
                    
                    # Use proven site detector from OLD parser system
                    site = detect_poker_site(content, file)
                    if site:
                        if site not in site_files:
                            site_files[site] = []
                        site_files[site].append(file_path)
                        logger.info(f"Detected {site} format in {file}")
                except Exception as e:
                    logger.warning(f"Could not read {file_path}: {e}")
    
    return site_files


def _copy_files_to_dir(files: List[str], dest_dir: str):
    """
    Copy files to a destination directory.
    
    Args:
        files: List of source file paths
        dest_dir: Destination directory path
    """
    os.makedirs(dest_dir, exist_ok=True)
    for file_path in files:
        dest_path = os.path.join(dest_dir, os.path.basename(file_path))
        shutil.copy2(file_path, dest_path)


def _process_site_for_month(
    site: str, 
    files: List[str], 
    month_work_dir: str, 
    token: str, 
    aggregator: MultiSiteAggregator,
    progress_callback,
    base_progress: int
) -> dict:
    """
    Process a single poker site within a month.
    
    Args:
        site: Site identifier (pokerstars, ggpoker, etc.)
        files: List of file paths for this site
        month_work_dir: Work directory for this month
        token: Processing token
        aggregator: MultiSiteAggregator to accumulate results
        progress_callback: Optional progress callback
        base_progress: Base progress percentage
        
    Returns:
        Dictionary with site processing results
    """
    from app.classify.group_classifier import classify_into_final_groups
    from app.stats.preflop_stats import PreflopStats
    from app.stats.hand_collector import HandCollector
    from app.stats.preflop_stats_multisite import patch_preflop_stats
    from app.stats.scoring_calculator import ScoringCalculator
    from app.score.bvb_scorer import calculate_bvb_scores
    from app.score.threbet_cc_scorer import calculate_3bet_cc_scores
    from app.score.vs_3bet_scorer import calculate_vs_3bet_scores
    from app.score.squeeze_scorer import SqueezeScorer
    from app.score.bb_defense_scorer import BBDefenseScorer
    from app.score.sb_defense_scorer import SBDefenseScorer
    from app.utils.progress_tracker import progress_tracker
    from app.utils.hand_streaming import stream_hands_from_combined_file, count_hands_in_file
    from app.utils.memory_monitor import log_memory_usage
    import gc
    
    logger.info(f"[{token}] Processing {len(files)} files from {site}")
    
    # Create site-specific directory
    site_dir = os.path.join(month_work_dir, "by_site", site)
    site_input_dir = os.path.join(site_dir, "in")
    
    # Copy site files to site-specific directory
    _copy_files_to_dir(files, site_input_dir)
    
    # Classify files into groups
    classified_dir = os.path.join(site_dir, "classified")
    progress_tracker.update_stage(token, 'classification', 'in_progress', f'Classificando mãos de {site}...')
    classification_stats = classify_into_final_groups(site_input_dir, classified_dir, token=token)
    
    # Process each group for this site
    site_stats = {}
    site_discards = {}
    site_valid_hand_records: List[Dict[str, Any]] = []

    # Store discard stats for this site
    if 'discarded_hands' in classification_stats:
        site_discards = classification_stats['discarded_hands']

    for record in classification_stats.get('valid_hand_records', []) or []:
        record_with_site = dict(record)
        record_with_site['site'] = site
        site_valid_hand_records.append(record_with_site)

    discards = classification_stats.get('discarded_hands') or {}
    valid_total = sum(classification_stats.get('hands_per_group', {}).values())
    parsed_hands = int(classification_stats.get('parsed_hands') or classification_stats.get('total_hands') or 0)
    raw_segments = int(classification_stats.get('raw_segments') or parsed_hands)
    counted_discards = sum(
        int(v or 0) for k, v in discards.items() if k not in ['total', 'total_segments']
    )
    site_discarded_no_reason = max(parsed_hands - (valid_total + counted_discards), 0)

    site_room_summary = {
        site: {
            'valid_hands': valid_total,
            'total_hands': parsed_hands,
            'mystery_hands': int(discards.get('mystery', 0) or 0),
            'lt4_hands': int(discards.get('less_than_4_players', 0) or 0),
            'discarded_no_reason': site_discarded_no_reason,
        }
    }
    
    for group_key, group_label in classification_stats['group_labels'].items():
        group_dir = os.path.join(classified_dir, group_key)
        
        # Skip if no files in group (unless NON-KO)
        is_nonko = 'nonko' in group_key.lower()
        if classification_stats['groups'][group_key] == 0 and not is_nonko:
            continue
        
        # Get hand count
        hand_count = classification_stats.get('hands_per_group', {}).get(group_key, 0)
        
        # Read combined file if it exists
        combined_file = os.path.join(group_dir, f"{group_key}_combined.txt")
        if not os.path.exists(combined_file):
            continue
        
        # Apply multi-site patch
        patch_preflop_stats()
        
        progress_tracker.update_stage(token, 'classification', 'completed', 'Classificação concluída')
        progress_tracker.update_stage(token, 'parsing', 'in_progress', f'Analisando mãos de {site}/{group_key}...')
        
        # Create hand collector for this site and group
        hand_collector = HandCollector(os.path.join(site_dir, "hands_by_stat", group_key))
        
        # Create preflop calculator with hand collector
        preflop_calculator = PreflopStats(hand_collector=hand_collector)
        
        # ADD POSTFLOP STATS CALCULATION - Using raw text (same as preflop)
        from app.stats.postflop_calculator_v3 import PostflopCalculatorV3
        postflop_calculator = PostflopCalculatorV3(hand_collector=hand_collector)
        
        # Log memory before processing
        log_memory_usage(f"{token}/{site}/{group_key} BEFORE")
        
        # Count total hands for progress tracking
        total_hands = count_hands_in_file(combined_file, site)
        progress_tracker.update_stage(token, 'parsing', 'in_progress', f'Processando {total_hands} mãos...')
        logger.info(f"[{token}] {site}/{group_key}: Processing {total_hands} hands with streaming (memory-efficient)")
        
        # Process hands in streaming mode - one at a time
        hands_processed = 0
        batch_size = 100  # Run GC every 100 hands
        
        for hand_text in stream_hands_from_combined_file(combined_file, site):
            if hand_text.strip():
                # Analyze hand for both preflop and postflop stats
                preflop_calculator.analyze_hand(hand_text)
                postflop_calculator.analyze_hand(hand_text)
                
                hands_processed += 1
                
                # Garbage collection every batch_size hands
                if hands_processed % batch_size == 0:
                    gc.collect()
                    if hands_processed % 500 == 0:
                        log_memory_usage(f"{token}/{site}/{group_key} @ {hands_processed}")
                        logger.info(f"[{token}] {site}/{group_key}: Processed {hands_processed}/{total_hands} hands")
        
        # Final garbage collection and memory log
        gc.collect()
        log_memory_usage(f"{token}/{site}/{group_key} AFTER")
        logger.info(f"[{token}] {site}/{group_key}: Completed processing {hands_processed} hands")
        
        # Get all statistics
        all_stats = preflop_calculator.get_stats_summary()
        
        # Get postflop statistics
        postflop_stats = postflop_calculator.get_stats_summary()
        postflop_hands_count = postflop_calculator.get_hands_count()
        
        progress_tracker.update_stage(token, 'parsing', 'completed', 'Análise concluída')
        progress_tracker.update_stage(token, 'stats', 'in_progress', 'Calculando estatísticas...')
        
        # Log detailed summary for verification
        logger.info(f"[{token}] {site}/{group_key}: Calculated {len(postflop_stats)} postflop stats, {postflop_hands_count} hands eligible")
        
        # Merge postflop stats into all_stats
        all_stats.update(postflop_stats)
        
        # Calculate scores for this site/group (same logic as combined)
        scoring_calc = ScoringCalculator()
        
        # Get RFI stats
        rfi_stats = {k: v for k, v in all_stats.items() 
                    if "RFI" in k or k in ["CO Steal", "BTN Steal"]}
        rfi_scores = scoring_calc.calculate_group_scores(group_key, rfi_stats) if rfi_stats else {}
        
        # Get BvB stats
        bvb_stats = {k: v for k, v in all_stats.items() 
                    if k in ["SB UO VPIP", "BB fold vs SB steal", "BB raise vs SB limp UOP", "SB Steal"]}
        
        # Determine table format
        if "9max" in group_key:
            table_format = "9max"
        elif "6max" in group_key:
            table_format = "6max"
        else:
            table_format = "PKO"
        
        bvb_scores = calculate_bvb_scores(bvb_stats, table_format) if bvb_stats else {}
        
        # Calculate 3bet CC, vs 3bet (match combined path filters)
        threbet_cc_stats = {k: v for k, v in all_stats.items() 
                           if "3bet" in k or "Cold Call" in k or "VPIP" in k or "BTN fold to CO steal" in k}
        threbet_cc_scores = calculate_3bet_cc_scores(threbet_cc_stats, table_format) if threbet_cc_stats else {}
        
        vs_3bet_stats = {k: v for k, v in all_stats.items() 
                       if "Fold to 3bet" in k}
        vs_3bet_scores = calculate_vs_3bet_scores(vs_3bet_stats, table_format) if vs_3bet_stats else {}
        
        squeeze_stats = {k: v for k, v in all_stats.items() 
                       if "Squeeze" in k}
        squeeze_scorer = SqueezeScorer()
        squeeze_scores = squeeze_scorer.calculate_squeeze_scores(squeeze_stats, group_key) if squeeze_stats else {}
        
        bb_defense_stats = {k: v for k, v in all_stats.items() 
                          if "BB fold vs CO steal" in k or "BB fold vs BTN steal" in k or "BB resteal vs BTN steal" in k}
        bb_defense_scorer = BBDefenseScorer()
        bb_defense_scores = bb_defense_scorer.calculate_bb_defense_scores(bb_defense_stats, bvb_stats, group_key) if bb_defense_stats else {}
        
        sb_defense_stats = {k: v for k, v in all_stats.items() 
                          if "SB fold to CO Steal" in k or "SB fold to BTN Steal" in k or "SB resteal vs BTN" in k}
        sb_defense_scorer = SBDefenseScorer()
        sb_defense_scores = sb_defense_scorer.calculate_group_score(sb_defense_stats, table_format) if sb_defense_stats else {}
        
        # Calculate overall_score (weighted average of category scores)
        category_scores = []
        if rfi_scores.get('overall_score'):
            category_scores.append(rfi_scores['overall_score'])
        if bvb_scores.get('overall_score'):
            category_scores.append(bvb_scores['overall_score'])
        if threbet_cc_scores.get('overall_score'):
            category_scores.append(threbet_cc_scores['overall_score'])
        if vs_3bet_scores.get('overall_score'):
            category_scores.append(vs_3bet_scores['overall_score'])
        if squeeze_scores.get('overall_score'):
            category_scores.append(squeeze_scores['overall_score'])
        if bb_defense_scores.get('overall_score'):
            category_scores.append(bb_defense_scores['overall_score'])
        if sb_defense_scores.get('overall_score'):
            category_scores.append(sb_defense_scores['overall_score'])
        
        overall_score = sum(category_scores) / len(category_scores) if category_scores else 0
        
        # Save collected hands
        saved_stats = hand_collector.save_all()
        hands_by_stat = hand_collector.get_hands_by_stat()
        
        # Store site-specific results WITH SCORES
        site_stats[group_key] = {
            'stats': all_stats,
            'overall_score': overall_score,
            'hands_by_stat': hands_by_stat,
            'hand_count': hand_count,
            'postflop_hands_count': postflop_hands_count,
            'postflop_stats': postflop_stats,
            'scores': {
                'rfi': rfi_scores,
                'bvb': bvb_scores,
                'threbet_cc': threbet_cc_scores,
                'vs_3bet': vs_3bet_scores,
                'squeeze': squeeze_scores,
                'bb_defense': bb_defense_scores,
                'sb_defense': sb_defense_scores
            },
            'sites_included': [site]
        }
        
        # Add to aggregator
        aggregator.add_site_results(site, group_key, all_stats, hands_by_stat)

        logger.info(f"[{token}] {site}/{group_key}: Processed {hand_count} hands")

    site_debug = {
        'raw_lines': raw_segments,
        'parsed_hands': parsed_hands,
        'valid_hands': valid_total,
        'mystery_hands': int(discards.get('mystery', 0) or 0),
        'lt4_hands': int(discards.get('less_than_4_players', 0) or 0),
        'discarded_no_reason': site_discarded_no_reason,
        'by_room': site_room_summary,
    }

    return {
        'site_stats': site_stats,
        'site_discards': site_discards,
        'valid_hand_records': site_valid_hand_records,
        'debug': site_debug,
        'room_summary': site_room_summary,
    }


def _aggregate_month_groups(month_aggregator: MultiSiteAggregator, month_work_dir: str, all_groups: set) -> dict:
    """
    Aggregate statistics across all sites for a specific month.
    
    Args:
        month_aggregator: MultiSiteAggregator with all site results
        month_work_dir: Work directory for this month
        all_groups: Set of all group keys found
        
    Returns:
        Dictionary with combined statistics for all groups
    """
    from app.stats.scoring_calculator import ScoringCalculator
    from app.score.bvb_scorer import calculate_bvb_scores
    from app.score.threbet_cc_scorer import calculate_3bet_cc_scores
    from app.score.vs_3bet_scorer import calculate_vs_3bet_scores
    from app.score.squeeze_scorer import SqueezeScorer
    from app.score.bb_defense_scorer import BBDefenseScorer
    from app.score.sb_defense_scorer import SBDefenseScorer
    
    logger.info("Aggregating statistics across all sites for month")
    
    combined_stats = {}
    
    for group in all_groups:
        # Write combined outputs (now returns {'overall_score': X, 'stats': {...}})
        aggregated_result = month_aggregator.write_combined_outputs(month_work_dir, group)
        overall_score = aggregated_result['overall_score']
        aggregated_stats = aggregated_result['stats']
        
        # Calculate scores for aggregated stats (legacy scoring by category)
        scoring_calc = ScoringCalculator()
        
        # Get RFI stats
        rfi_stats = filter_stats(aggregated_stats, RFI_STATS)
        rfi_scores = scoring_calc.calculate_group_scores(group, rfi_stats) if rfi_stats else {}
        
        # Get BvB stats  
        bvb_stats = filter_stats(aggregated_stats, BVB_STATS)
        
        # Determine table format
        if "9max" in group:
            table_format = "9max"
        elif "6max" in group:
            table_format = "6max"
        else:
            table_format = "PKO"
        
        bvb_scores = calculate_bvb_scores(bvb_stats, table_format) if bvb_stats else {}
        
        # Get other stat categories
        threbet_cc_stats = filter_stats(aggregated_stats, THREEBET_CC_STATS)
        threbet_cc_scores = calculate_3bet_cc_scores(threbet_cc_stats, table_format) if threbet_cc_stats else {}
        
        vs_3bet_stats = filter_stats(aggregated_stats, VS_3BET_STATS)
        vs_3bet_scores = calculate_vs_3bet_scores(vs_3bet_stats, table_format) if vs_3bet_stats else {}
        
        squeeze_stats = filter_stats(aggregated_stats, SQUEEZE_STATS)
        squeeze_scorer = SqueezeScorer()
        squeeze_scores = squeeze_scorer.calculate_squeeze_scores(squeeze_stats, group) if squeeze_stats else {}
        
        bb_defense_stats = filter_stats(aggregated_stats, BB_DEFENSE_STATS)
        bb_defense_scorer = BBDefenseScorer()
        bb_defense_scores = bb_defense_scorer.calculate_bb_defense_scores(bb_defense_stats, bvb_stats, group) if bb_defense_stats else {}
        
        sb_defense_stats = filter_stats(aggregated_stats, SB_DEFENSE_STATS)
        sb_defense_scorer = SBDefenseScorer()
        sb_defense_scores = sb_defense_scorer.calculate_group_score(sb_defense_stats, table_format) if sb_defense_stats else {}
        
        # Get POSTFLOP stats (case-insensitive matching)
        postflop_stats = filter_stats_by_keyword(aggregated_stats, POSTFLOP_KEYWORDS)
        
        combined_stats[group] = {
            'stats': aggregated_stats,
            'overall_score': overall_score,
            'hand_count': aggregated_result.get('hand_count', 0),
            'postflop_hands_count': aggregated_result.get('postflop_hands_count', 0),
            'postflop_stats': postflop_stats,
            'scores': {
                'rfi': rfi_scores,
                'bvb': bvb_scores,
                'threbet_cc': threbet_cc_scores,
                'vs_3bet': vs_3bet_scores,
                'squeeze': squeeze_scores,
                'bb_defense': bb_defense_scores,
                'sb_defense': sb_defense_scores
            },
            'sites_included': list(month_aggregator.site_data.get(group, {}).keys()) if hasattr(month_aggregator, 'site_data') else []
        }
    
    # Note: Dashboard's aggregate_postflop_stats() will handle aggregating postflop from all groups
    # No need to pre-aggregate here - each group has its own postflop_stats for flexibility
    
    return combined_stats


def _accumulate_global(month_result: dict, global_aggregator: MultiSiteAggregator):
    """
    Accumulate a month's results into the global aggregator.
    
    Args:
        month_result: Month processing result dictionary
        global_aggregator: Global MultiSiteAggregator to accumulate into
    """
    # Accumulate site results from this month into global aggregator
    for site, site_data in month_result.get('sites', {}).items():
        for group_key, group_info in site_data.items():
            if isinstance(group_info, dict) and 'stats' in group_info:
                # Add this site/group's stats and hands to global aggregator
                global_aggregator.add_site_results(
                    site, 
                    group_key, 
                    group_info['stats'], 
                    group_info.get('hands_by_stat', {})
                )


def _process_month_bucket(
    bucket: MonthBucket, 
    token: str, 
    progress_callback, 
    base_progress: int, 
    progress_weight: int
) -> dict:
    """
    Process a single month bucket.
    
    Args:
        bucket: MonthBucket to process
        token: Processing token
        progress_callback: Optional progress callback
        base_progress: Base progress percentage
        progress_weight: Weight for this month's progress
        
    Returns:
        Month result dictionary
    """
    from app.utils.progress_tracker import progress_tracker
    
    logger.info(f"[{token}] Processing month bucket: {bucket.month}")
    
    month_work_dir = bucket.work_dir
    input_dir = bucket.input_dir
    
    # Detect sites in this month's files
    logger.info(f"[{token}] Month {bucket.month}: Detecting poker sites")
    progress_tracker.update_stage(token, 'detection', 'in_progress', f'Mês {bucket.month} - Detectando salas...')
    
    site_files = detect_sites_in_directory(input_dir)
    
    if not site_files:
        logger.warning(f"[{token}] Month {bucket.month}: No recognized poker sites found")
        return {
            'month': bucket.month,
            'status': 'no_sites',
            'sites': {},
            'combined': {}
        }
    
    logger.info(f"[{token}] Month {bucket.month}: Detected {len(site_files)} sites: {list(site_files.keys())}")
    
    # Process each site
    month_aggregator = MultiSiteAggregator()
    all_groups = set()
    month_sites = {}
    month_discards = {}
    month_valid_records: List[Dict[str, Any]] = []
    month_debug = _empty_debug_totals()
    
    total_sites = len(site_files)
    for site_idx, (site, files) in enumerate(site_files.items(), 1):
        if progress_callback:
            percent = base_progress + int((site_idx / total_sites) * progress_weight)
            progress_callback(percent, f'Mês {bucket.month} - {site} - a processar...')
        
        site_result = _process_site_for_month(
            site, files, month_work_dir, token,
            month_aggregator, progress_callback, base_progress
        )

        # Collect results
        month_sites[site] = site_result['site_stats']
        if site_result['site_discards']:
            month_discards[site] = site_result['site_discards']

        month_debug = _merge_debug_totals(month_debug, site_result.get('debug'))

        if site_result.get('valid_hand_records'):
            for record in site_result['valid_hand_records']:
                record_with_month = dict(record)
                if not record_with_month.get('month'):
                    record_with_month['month'] = bucket.month
                month_valid_records.append(record_with_month)
        
        # Track all groups
        for group_key in site_result['site_stats'].keys():
            all_groups.add(group_key)
    
    # Aggregate results for this month
    logger.info(f"[{token}] Month {bucket.month}: Aggregating {len(all_groups)} groups across {len(site_files)} sites")
    combined_stats = _aggregate_month_groups(month_aggregator, month_work_dir, all_groups)
    
    # Write cross-format postflop outputs
    logger.info(f"[{token}] Month {bucket.month}: Writing cross-format postflop outputs")
    cross_format_postflop = month_aggregator.write_cross_format_postflop_outputs(month_work_dir)
    
    # Calculate total hands for this month (from normalized records)
    group_id_sets: Dict[str, set] = defaultdict(set)
    for record in month_valid_records:
        group_id_sets[record['group']].add(record['hand_id'])

    total_postflop_hands = 0
    for group, group_data in combined_stats.items():
        if isinstance(group_data, dict):
            total_postflop_hands += group_data.get('postflop_hands_count', 0)
            group_data['hand_count'] = len(group_id_sets.get(group, set()))

    aggregated_discards: Dict[str, int] = {}
    if month_discards:
        for site, discards in month_discards.items():
            for reason, count in discards.items():
                if reason not in ['total', 'total_segments']:
                    aggregated_discards[reason] = aggregated_discards.get(reason, 0) + count

    month_samples = build_global_samples(month_valid_records, aggregated_discards)

    month_debug = _reconcile_debug_totals(month_debug, month_samples.discard_counts)

    month_result = build_pipeline_result_payload(
        combined=combined_stats,
        valid_hand_records=month_valid_records,
        aggregated_discards=aggregated_discards,
        sites=month_sites,
        hands_per_month={bucket.month: month_samples.validas},
        month=bucket.month,
        postflop_hands_count=total_postflop_hands,
        samples=month_samples,
        extra={
            'sites_detected': list(site_files.keys()),
            'discarded_hands': month_discards,
            'debug': month_debug,
            'rooms': month_debug.get('by_room', {}),
        },
    )

    _validate_category_counts(month_valid_records, month_samples.validas)
    
    # Save month result
    _write_month_pipeline_result(bucket, month_result)
    logger.info(f"[{token}] ✅ Wrote monthly pipeline_result for {bucket.month}")

    total_discarded = month_result['classification']['discarded_hands'].get('total', 0)
    logger.info(
        f"[{token}] Month {bucket.month}: completed with valid_hands={month_result['valid_hands']}, "
        f"discarded={total_discarded}, total={month_result['total_hands']}"
    )

    logger.debug(
        f"[{token}] Month {bucket.month}: classification={month_result['classification']}, "
        f"aggregated_discards={month_result.get('aggregated_discards')}"
    )
    
    return month_result


def run_multi_site_pipeline(
    archive_path: str,
    work_root: str = "work",
    token: Optional[str] = None,
    progress_callback=None,
    user_id: Optional[str] = None,
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """
    Run pipeline that handles multiple poker sites and aggregates results.
    Supports monthly bucketing for multi-month uploads.
    
    1. Extract archive
    2. Build month buckets (if multi-month)
    3. For each month: detect sites, process sites, aggregate
    4. Aggregate global results across months
    5. Generate manifests and save results
    
    Args:
        archive_path: Path to the archive file
        work_root: Root directory for work files
        token: Optional pre-generated token (if None, generates new one)
        progress_callback: Optional callback(percent, message) for progress updates
    """
    from app.pipeline.runner import safe_extract_archive, generate_token, log_step
    from app.utils.progress_tracker import progress_tracker
    
    # Use provided token or generate new one
    if token is None:
        token = generate_token()
        # Initialize progress tracking only if we generated the token
        progress_tracker.init_job(token)
    
    work_dir = os.path.join(work_root, token)
    
    # Create directories
    os.makedirs(work_dir, exist_ok=True)
    os.makedirs(os.path.join(work_dir, "in"), exist_ok=True)
    os.makedirs(os.path.join(work_dir, "_logs"), exist_ok=True)
    
    result_data = {
        'token': token,
        'status': 'processing',
        'multi_site': True,
        'sites': {},
        'combined': {},
        'valid_hand_records': [],
    }
    global_debug = _empty_debug_totals()
    
    try:
        # Step 1: Extract archive
        logger.info(f"[{token}] Extracting archive")
        log_step(token, "extract", "started", "Extracting archive recursively")
        progress_tracker.update_stage(token, 'extraction', 'in_progress', 'Extraindo arquivos...')
        
        input_dir = os.path.join(work_dir, "in")
        file_count = safe_extract_archive(archive_path, input_dir)
        
        if file_count == 0:
            log_step(token, "extract", "failed", "", "No .txt files found")
            progress_tracker.fail_job(token, 'Nenhum arquivo .txt encontrado')
            raise ValueError("No .txt files found in archive")
        
        log_step(token, "extract", "completed", f"Extracted {file_count} files")
        progress_tracker.update_stage(token, 'extraction', 'completed', f'{file_count} arquivos extraídos')
        result_data['extracted_files'] = file_count
        
        if progress_callback:
            progress_callback(35, f'Extraídos {file_count} ficheiros')
        
        use_month_bucketizer = False
        buckets: List[MonthBucket] = []
        is_multi_month = False

        if use_month_bucketizer:
            # Step 2: Ingest tournaments and build month buckets (disabled in global mode)
            parser_runner = ParserRunner()
            repository = TournamentRepository()
            user_key = user_id or token

            logger.info(f"[{token}] Ingesting tournaments for user dataset {user_key}")
            ingest_summary = _ingest_tournaments_for_user(user_key, Path(input_dir), repository, parser_runner, token)

            dataset_dir = Path(work_dir) / "dataset"
            total_tournaments = repository.export_dataset(user_key, dataset_dir)

            result_data['tournament_ingest'] = ingest_summary

            if total_tournaments == 0:
                logger.error(f"[{token}] No tournaments available after ingest")
                raise ValueError("No tournament files available for processing")

            logger.info(f"[{token}] Building month buckets")
            buckets = build_month_buckets(
                token,
                str(dataset_dir),
                work_root,
                metadata_resolver=parser_runner.extract_tournament_metadata,
            )

            if not buckets:
                logger.error(f"[{token}] No files to process after bucketing")
                raise ValueError("No processable files found")

            logger.info(f"[{token}] Created {len(buckets)} month bucket(s): {[b.month for b in buckets]}")

            # Check if single-month or multi-month
            is_multi_month = len(buckets) > 1
        else:
            logger.info(f"[{token}] Global mode: skipping month bucketizer and processing files directly")

        if not is_multi_month:
            # Single month - use existing code path for backwards compatibility
            logger.info(f"[{token}] Single-month upload detected, using standard pipeline")
            
            # Detect sites in extracted files
            logger.info(f"[{token}] Detecting poker sites in files")
            progress_tracker.update_stage(token, 'detection', 'in_progress', 'Detectando salas de poker...')
            
            if progress_callback:
                progress_callback(40, 'A detetar salas de poker...')
            
            site_files = detect_sites_in_directory(input_dir)
            
            if not site_files:
                logger.warning(f"[{token}] No recognized poker sites found, falling back to single-site processing")
                return run_simplified_pipeline(archive_path, work_root, token=token)
            
            result_data['sites_detected'] = list(site_files.keys())
            logger.info(f"[{token}] Detected {len(site_files)} sites: {list(site_files.keys())}")
            
            if progress_callback:
                sites_str = ', '.join(site_files.keys())
                progress_callback(45, f'Detetadas {len(site_files)} salas: {sites_str}')
            
            # Process each site separately (EXISTING LOGIC)
            aggregator = MultiSiteAggregator()
            all_groups = set()
            
            total_sites = len(site_files)
            for site_idx, (site, files) in enumerate(site_files.items(), 1):
                if progress_callback:
                    percent = 45 + (site_idx / total_sites) * 25
                    progress_callback(int(percent), f'A processar {site} ({len(files)} ficheiros)...')
                
                site_result = _process_site_for_month(
                    site, files, work_dir, token,
                    aggregator, progress_callback, 45
                )

                # Store site results
                result_data['sites'][site] = site_result['site_stats']

                global_debug = _merge_debug_totals(global_debug, site_result.get('debug'))

                if site_result.get('valid_hand_records'):
                    result_data['valid_hand_records'].extend(site_result['valid_hand_records'])

                # Store discards
                if site_result['site_discards']:
                    if 'discarded_hands' not in result_data:
                        result_data['discarded_hands'] = {}
                    result_data['discarded_hands'][site] = site_result['site_discards']
                
                # Track all groups
                for group_key in site_result['site_stats'].keys():
                    all_groups.add(group_key)
            
            # Aggregate results across all sites
            logger.info(f"[{token}] Aggregating statistics across all sites")
            combined_stats = _aggregate_month_groups(aggregator, work_dir, all_groups)
            
            # Calculate hand counts based on normalized valid hand records
            group_id_sets: Dict[str, set] = defaultdict(set)
            for record in result_data.get('valid_hand_records', []):
                group_id_sets[record['group']].add(record['hand_id'])

            for group, group_data in combined_stats.items():
                total_postflop_hands = 0
                for site in result_data.get('sites', {}).values():
                    if group in site:
                        total_postflop_hands += site[group].get('postflop_hands_count', 0)

                group_data['hand_count'] = len(group_id_sets.get(group, set()))
                group_data['postflop_hands_count'] = total_postflop_hands
            
            result_data['combined'] = combined_stats
            
            if progress_callback:
                progress_callback(75, 'A agregar resultados de todas as salas...')
            
            # Write cross-format postflop outputs
            logger.info(f"[{token}] Writing cross-format postflop outputs")
            cross_format_postflop = aggregator.write_cross_format_postflop_outputs(work_dir)
            logger.info(f"[{token}] Cross-format postflop complete: {len(cross_format_postflop)} stats")
            
            if progress_callback:
                progress_callback(80, 'A finalizar processamento...')
            
            aggregated_discards: Dict[str, int] = {}
            if 'discarded_hands' in result_data:
                for site, discards in result_data['discarded_hands'].items():
                    for reason, count in discards.items():
                        if reason not in ['total', 'total_segments']:
                            aggregated_discards[reason] = aggregated_discards.get(reason, 0) + count

            global_samples = build_global_samples(
                result_data.get('valid_hand_records', []),
                aggregated_discards,
            )

            hands_per_month = _count_hands_per_month(result_data.get('valid_hand_records', []))

            global_debug = _reconcile_debug_totals(global_debug, global_samples.discard_counts)

            for group_key, group_sample in global_samples.groups.items():
                if group_key == POSTFLOP_GROUP_KEY:
                    continue
                if group_key in result_data.get('combined', {}):
                    result_data['combined'][group_key]['hand_count'] = group_sample.hand_count

            postflop_sample = global_samples.groups.get(POSTFLOP_GROUP_KEY)
            if postflop_sample:
                assert postflop_sample.hand_count == global_samples.validas, (
                    "POSTFLOP group count must match total valid hands"
                )

            result_data = build_aggregate_pipeline_result(
                token,
                result_data,
                work_dir,
                aggregated_discards=aggregated_discards,
                hands_per_month=hands_per_month,
                global_samples=global_samples,
                global_debug=global_debug,
                postflop_sample=postflop_sample,
                manifest_builder=aggregator,
            )
            
        else:
            # Multi-month processing mode
            logger.info(f"[{token}] Multi-month upload detected: {len(buckets)} month(s)")
            result_data['multi_month'] = True
            result_data['months'] = {}
            
            # Create global aggregator
            global_aggregator = MultiSiteAggregator()
            global_groups = set()
            bucket_results = {}
            bucket_lookup: Dict[str, MonthBucket] = {}
            
            # Calculate progress weights
            total_months = len(buckets)
            progress_per_month = 40 / total_months  # 40-80% range for month processing
            
            # Process each month bucket
            for month_idx, bucket in enumerate(buckets, 1):
                try:
                    logger.info(f"[{token}] Processing month {month_idx}/{total_months}: {bucket.month}")
                    
                    base_progress = 40 + int((month_idx - 1) * progress_per_month)
                    progress_weight = int(progress_per_month)
                    
                    if progress_callback:
                        progress_callback(base_progress, f'Mês {month_idx}/{total_months} ({bucket.month})...')
                    
                    # Process this month
                    month_result = _process_month_bucket(
                        bucket, token, progress_callback,
                        base_progress, progress_weight
                    )
                    
                    # Store month result
                    result_data['months'][bucket.month] = month_result
                    bucket_results[bucket.month] = month_result
                    bucket_lookup[bucket.month] = bucket

                    global_debug = _merge_debug_totals(global_debug, month_result.get('debug'))

                    if month_result.get('valid_hand_records'):
                        result_data['valid_hand_records'].extend(month_result['valid_hand_records'])
                    
                    # Accumulate into global aggregator
                    _accumulate_global(month_result, global_aggregator)
                    
                    # Track global groups
                    for group in month_result.get('combined', {}).keys():
                        global_groups.add(group)
                    
                    logger.info(f"[{token}] Month {bucket.month} completed successfully")
                    
                except Exception as e:
                    logger.error(f"[{token}] Month {bucket.month} failed: {str(e)}")
                    result_data['months'][bucket.month] = {
                        'month': bucket.month,
                        'status': 'failed',
                        'error': str(e)
                    }
                    # Continue with other months
            
            monthly_context = build_monthly_results_for_token(
                token,
                work_dir,
                result_data,
                buckets,
                bucket_results,
                bucket_lookup,
                global_debug,
                global_aggregator,
                global_groups,
                progress_callback,
            )

            result_data = monthly_context.get('result_data', result_data)
            hands_per_month = monthly_context.get('hands_per_month', {})
            global_samples = monthly_context.get('global_samples')
            postflop_sample = monthly_context.get('postflop_sample')
            months_manifest = monthly_context.get('months_manifest')
            bucket_results = monthly_context.get('bucket_results', bucket_results)
            aggregated_discards = monthly_context.get('global_discards') or {}

            if global_samples is None:
                global_samples = build_global_samples(
                    result_data.get('valid_hand_records', []),
                    aggregated_discards,
                )
                postflop_sample = global_samples.groups.get(POSTFLOP_GROUP_KEY)

            # NOTE: Monthly consistency issues must never cause the pipeline to fail.
            # Global pipeline remains the source of truth for upload success/failure.
            try:
                _validate_category_counts(
                    result_data.get('valid_hand_records', []),
                    global_samples.validas,
                )
                _validate_month_totals(hands_per_month, global_samples.validas)
            except Exception:
                logger.exception(
                    "[%s] [MONTHLY] Validation failed (category/month totals). "
                    "Treating monthly validation as best-effort.",
                    token,
                )

            result_data = build_aggregate_pipeline_result(
                token,
                result_data,
                work_dir,
                aggregated_discards=global_samples.discard_counts,
                hands_per_month=hands_per_month,
                global_samples=global_samples,
                global_debug=global_debug,
                postflop_sample=postflop_sample,
                manifest_builder=global_aggregator,
                bucket_results=bucket_results,
                months_manifest=months_manifest,
                multi_month=True,
            )
        
        global_result_upper = os.path.join(work_dir, "pipeline_result_GLOBAL.json")

        if is_multi_month:
            monthly_paths = {
                bucket.month: Path(work_dir) / f"pipeline_result_{bucket.month}.json"
                for bucket in buckets
                if getattr(bucket, "month", None)
            }
            try:
                log_monthly_global_consistency(Path(global_result_upper), monthly_paths)
            except Exception:
                logger.exception(
                    "[%s] [MONTHLY] log_monthly_global_consistency failed; "
                    "treating as non-fatal monthly diagnostics issue.",
                    token,
                )

        legacy_result_path = os.path.join(work_dir, "pipeline_result.json")
        with open(legacy_result_path, 'w', encoding='utf-8') as f:
            json.dump(result_data, f, indent=2)

        result_data['status'] = 'completed'

        if is_multi_month:
            log_step(token, "pipeline", "completed", f"Multi-month multi-site pipeline completed ({len(buckets)} months)")
        else:
            log_step(token, "pipeline", "completed", f"Multi-site pipeline completed successfully")

        # Mark progress as complete
        progress_tracker.update_stage(token, 'stats', 'completed', 'Dashboard pronto!')
        progress_tracker.complete_job(token)

        # Upload results to Supabase Storage (if enabled)
        logger.info(f"[{token}] Uploading results to Supabase Storage")
        try:
            from app.services.storage import get_storage
            storage = get_storage()

            def _upload_directory(local_dir: str, remote_prefix: str) -> int:
                uploaded = 0
                if not os.path.isdir(local_dir):
                    return uploaded

                for root_dir, _, files in os.walk(local_dir):
                    for file_name in files:
                        local_path = os.path.join(root_dir, file_name)
                        relative_path = os.path.relpath(local_path, local_dir)
                        storage_key = f"{remote_prefix}/{relative_path}".replace('\\', '/')
                        content_type = 'application/json' if file_name.endswith('.json') else 'text/plain'

                        with open(local_path, 'rb') as stream:
                            storage.upload_file_stream(stream, storage_key, content_type)

                        uploaded += 1

                logger.info(f"[{token}] Uploaded {uploaded} files under {remote_prefix}")
                return uploaded

            # Upload new global pipeline_result
            global_result_path = os.path.join(work_dir, "pipeline_result_global.json")
            if os.path.exists(global_result_path):
                with open(global_result_path, 'rb') as f:
                    storage_path = f"/results/{token}/pipeline_result_global.json"
                    storage.upload_file_stream(f, storage_path, 'application/json')
                    logger.info(f"[{token}] ✅ Uploaded pipeline_result_global.json")

            global_result_upper = os.path.join(work_dir, "pipeline_result_GLOBAL.json")
            if os.path.exists(global_result_upper):
                with open(global_result_upper, 'rb') as f:
                    storage_path = f"/results/{token}/pipeline_result_GLOBAL.json"
                    storage.upload_file_stream(f, storage_path, 'application/json')
                    logger.info(f"[{token}] ✅ Uploaded pipeline_result_GLOBAL.json")

            # Upload legacy aggregate file for backwards compatibility
            aggregate_result_path = os.path.join(work_dir, "pipeline_result.json")
            if os.path.exists(aggregate_result_path):
                with open(aggregate_result_path, 'rb') as f:
                    storage_path = f"/results/{token}/pipeline_result.json"
                    storage.upload_file_stream(f, storage_path, 'application/json')
                    logger.info(f"[{token}] ✅ Uploaded legacy pipeline_result.json")

            # Upload aggregated hands_by_stat files (global scope)
            hands_dir = os.path.join(work_dir, "hands_by_stat")
            _upload_directory(hands_dir, f"/results/{token}/hands_by_stat")

            # Upload months_manifest.json (if multi-month)
            if is_multi_month:
                manifest_local = os.path.join(work_dir, "months_manifest.json")
                if os.path.exists(manifest_local):
                    with open(manifest_local, 'rb') as f:
                        storage_path = f"/results/{token}/months_manifest.json"
                        storage.upload_file_stream(f, storage_path, 'application/json')
                        logger.info(f"[{token}] ✅ Uploaded months_manifest.json")

                # Upload each month's results and hands_by_stat/
                for bucket in buckets:
                    month = bucket.month
                    month_work_dir = bucket.work_dir

                    # Upload monthly pipeline_result.json
                    month_result_path = os.path.join(month_work_dir, "pipeline_result.json")
                    if os.path.exists(month_result_path):
                        with open(month_result_path, 'rb') as f:
                            storage_path = f"/results/{token}/months/{month}/pipeline_result.json"
                            storage.upload_file_stream(f, storage_path, 'application/json')
                            logger.info(f"[{token}] ✅ Uploaded pipeline_result.json for month {month}")

                    month_root_path = os.path.join(work_dir, f"pipeline_result_{month}.json")
                    if os.path.exists(month_root_path):
                        with open(month_root_path, 'rb') as f:
                            storage_path = f"/results/{token}/pipeline_result_{month}.json"
                            storage.upload_file_stream(f, storage_path, 'application/json')
                            logger.info(f"[{token}] ✅ Uploaded pipeline_result_{month}.json")

                    month_hands_dir = os.path.join(month_work_dir, "hands_by_stat")
                    _upload_directory(month_hands_dir, f"/results/{token}/months/{month}/hands_by_stat")

            logger.info(f"[{token}] ✅ All results uploaded to Supabase Storage successfully")
        except Exception as e:
            logger.error(f"[{token}] Failed to upload results to Supabase Storage: {e}")
            # Don't fail the job if storage upload fails in non-production
            # But log prominently for debugging

            if progress_callback:
                progress_callback(100, 'Processamento concluído!')

            return True, f"Pipeline completed successfully (storage upload failed: {e})", result_data

        if progress_callback:
            progress_callback(100, 'Processamento concluído!')

        return True, "Pipeline completed successfully", result_data

    except Exception as e:
        import traceback

        traceback_str = traceback.format_exc()
        logger.exception("[%s] Multi-site pipeline failed", token)

        error_info = {
            'error': str(e),
            'traceback': traceback_str
        }

        try:
            logs_dir = Path(work_dir) / "_logs"
            logs_dir.mkdir(parents=True, exist_ok=True)
            with (logs_dir / "last_error.json").open('w', encoding='utf-8') as f:
                json.dump(error_info, f, indent=2)
        except Exception:
            logger.warning("[%s] Failed to persist last_error.json", token)

        result_data['status'] = 'failed'
        result_data['error'] = error_info

        # Mark progress as failed
        progress_tracker.fail_job(token, str(e))

        return False, str(e), None
