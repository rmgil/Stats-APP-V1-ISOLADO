"""
Multi-site pipeline runner that extends the new_runner to handle multiple poker sites
and supports monthly bucketing for multi-month uploads.
"""
import os
import json
import logging
import shutil
from pathlib import Path
from typing import Dict, Any, Tuple, List, Optional
from app.pipeline.new_runner import run_simplified_pipeline
from app.stats.aggregate import MultiSiteAggregator
from app.parse.site_parsers.site_detector import detect_poker_site
from app.pipeline.month_bucketizer import MonthBucket, build_month_buckets, generate_months_manifest

logger = logging.getLogger(__name__)

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
    
    # Store discard stats for this site
    if 'discarded_hands' in classification_stats:
        site_discards = classification_stats['discarded_hands']
    
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
    
    return {
        'site_stats': site_stats,
        'site_discards': site_discards
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
        rfi_stats = {k: v for k, v in aggregated_stats.items() 
                    if "RFI" in k or k in ["CO Steal", "BTN Steal"]}
        rfi_scores = scoring_calc.calculate_group_scores(group, rfi_stats) if rfi_stats else {}
        
        # Get BvB stats  
        bvb_stats = {k: v for k, v in aggregated_stats.items() 
                    if k in ["SB UO VPIP", "BB fold vs SB steal", "BB raise vs SB limp UOP", "SB Steal"]}
        
        # Determine table format
        if "9max" in group:
            table_format = "9max"
        elif "6max" in group:
            table_format = "6max"
        else:
            table_format = "PKO"
        
        bvb_scores = calculate_bvb_scores(bvb_stats, table_format) if bvb_stats else {}
        
        # Get other stat categories
        threbet_cc_stats = {k: v for k, v in aggregated_stats.items() 
                           if "3bet" in k or "Cold Call" in k or "VPIP" in k or "BTN fold to CO steal" in k}
        threbet_cc_scores = calculate_3bet_cc_scores(threbet_cc_stats, table_format) if threbet_cc_stats else {}
        
        vs_3bet_stats = {k: v for k, v in aggregated_stats.items() 
                        if "Fold to 3bet" in k}
        vs_3bet_scores = calculate_vs_3bet_scores(vs_3bet_stats, table_format) if vs_3bet_stats else {}
        
        squeeze_stats = {k: v for k, v in aggregated_stats.items() 
                        if "Squeeze" in k}
        squeeze_scorer = SqueezeScorer()
        squeeze_scores = squeeze_scorer.calculate_squeeze_scores(squeeze_stats, group) if squeeze_stats else {}
        
        bb_defense_stats = {k: v for k, v in aggregated_stats.items() 
                           if "BB fold vs CO steal" in k or "BB fold vs BTN steal" in k or "BB resteal vs BTN steal" in k}
        bb_defense_scorer = BBDefenseScorer()
        bb_defense_scores = bb_defense_scorer.calculate_bb_defense_scores(bb_defense_stats, bvb_stats, group) if bb_defense_stats else {}
        
        sb_defense_stats = {k: v for k, v in aggregated_stats.items() 
                           if "SB fold to CO Steal" in k or "SB fold to BTN Steal" in k or "SB resteal vs BTN" in k}
        sb_defense_scorer = SBDefenseScorer()
        sb_defense_scores = sb_defense_scorer.calculate_group_score(sb_defense_stats, table_format) if sb_defense_stats else {}
        
        # Get POSTFLOP stats (case-insensitive matching)
        postflop_stats = {k: v for k, v in aggregated_stats.items() 
                        if any(keyword.lower() in k.lower() for keyword in ["Flop CBet", "Flop fold", "Flop raise", 
                               "Check Raise", "Flop Bet vs", "Turn CBet", "Turn Donk", 
                               "Turn Fold", "Bet Turn", "WTSD", "w$sd", "w$wsf",
                               "River Agg", "River Bet"])}
        
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
        
        # Track all groups
        for group_key in site_result['site_stats'].keys():
            all_groups.add(group_key)
    
    # Aggregate results for this month
    logger.info(f"[{token}] Month {bucket.month}: Aggregating {len(all_groups)} groups across {len(site_files)} sites")
    combined_stats = _aggregate_month_groups(month_aggregator, month_work_dir, all_groups)
    
    # Write cross-format postflop outputs
    logger.info(f"[{token}] Month {bucket.month}: Writing cross-format postflop outputs")
    cross_format_postflop = month_aggregator.write_cross_format_postflop_outputs(month_work_dir)
    
    # Calculate total hands for this month (from deduplicated combined groups)
    total_hands = 0
    total_postflop_hands = 0
    for group_data in combined_stats.values():
        if isinstance(group_data, dict):
            total_hands += group_data.get('hand_count', 0)
            total_postflop_hands += group_data.get('postflop_hands_count', 0)
    
    # Build month result
    month_result = {
        'month': bucket.month,
        'status': 'completed',
        'multi_site': True,
        'sites_detected': list(site_files.keys()),
        'sites': month_sites,
        'combined': combined_stats,
        'valid_hands': total_hands,
        'total_hands': total_hands
    }
    
    # Add discard stats if any
    if month_discards:
        month_result['discarded_hands'] = month_discards
        
        # Aggregate discards for this month
        aggregated_discards = {}
        for site, discards in month_discards.items():
            for reason, count in discards.items():
                if reason not in ['total', 'total_segments']:
                    aggregated_discards[reason] = aggregated_discards.get(reason, 0) + count
        
        aggregated_discards['total'] = sum(aggregated_discards.values())
        month_result['aggregated_discards'] = aggregated_discards
        
        # Update total hands to include discards
        month_result['total_hands'] = total_hands + aggregated_discards['total']
    
    # Save month result
    month_result_path = os.path.join(month_work_dir, "pipeline_result.json")
    with open(month_result_path, 'w', encoding='utf-8') as f:
        json.dump(month_result, f, indent=2)
    
    logger.info(f"[{token}] Month {bucket.month}: Processing completed")
    
    return month_result


def run_multi_site_pipeline(archive_path: str, work_root: str = "work", token: Optional[str] = None, progress_callback=None) -> Tuple[bool, str, Dict[str, Any]]:
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
        'combined': {}
    }
    
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
        
        # Step 2: Build month buckets
        logger.info(f"[{token}] Building month buckets")
        buckets = build_month_buckets(token, input_dir, work_root)
        
        if not buckets:
            logger.error(f"[{token}] No files to process after bucketing")
            raise ValueError("No processable files found")
        
        logger.info(f"[{token}] Created {len(buckets)} month bucket(s): {[b.month for b in buckets]}")
        
        # Check if single-month or multi-month
        is_multi_month = len(buckets) > 1 or (len(buckets) == 1 and buckets[0].month != 'unknown')
        
        if not is_multi_month:
            # Single month (or unknown) - use existing code path for backwards compatibility
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
            
            # Calculate hand counts
            for group, group_data in combined_stats.items():
                total_hands = 0
                total_postflop_hands = 0
                for site in result_data.get('sites', {}).values():
                    if group in site:
                        total_hands += site[group].get('hand_count', 0)
                        total_postflop_hands += site[group].get('postflop_hands_count', 0)
                
                group_data['hand_count'] = total_hands
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
            
            # Aggregate discard statistics
            if 'discarded_hands' in result_data:
                aggregated_discards = {}
                for site, discards in result_data['discarded_hands'].items():
                    for reason, count in discards.items():
                        if reason not in ['total', 'total_segments']:
                            aggregated_discards[reason] = aggregated_discards.get(reason, 0) + count
                
                aggregated_discards['total'] = sum(aggregated_discards.values())
                result_data['aggregated_discards'] = aggregated_discards
                
                # Calculate total valid hands
                total_valid_hands = 0
                for group_data in combined_stats.values():
                    if 'stats' in group_data:
                        for stat_name, stat_data in group_data['stats'].items():
                            if isinstance(stat_data, dict) and 'valid_hands' in stat_data:
                                total_valid_hands += stat_data.get('valid_hands', 0)
                                break
                
                if total_valid_hands == 0:
                    for site, site_data in result_data['sites'].items():
                        for group_key, group_info in site_data.items():
                            if isinstance(group_info, dict):
                                total_valid_hands += group_info.get('hand_count', 0)
                
                total_hands = total_valid_hands + aggregated_discards['total']
                
                result_data['classification'] = {
                    'discarded_hands': aggregated_discards,
                    'total_hands': total_hands,
                    'valid_hands': total_valid_hands
                }
            
            # Generate manifest
            manifest = aggregator.get_combined_manifest()
            
            if 'aggregated_discards' in result_data:
                manifest['discards'] = result_data['aggregated_discards']
            
            manifest_path = os.path.join(work_dir, "multi_site_manifest.json")
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=2)
            
        else:
            # Multi-month processing mode
            logger.info(f"[{token}] Multi-month upload detected: {len(buckets)} month(s)")
            result_data['multi_month'] = True
            result_data['months'] = {}
            
            # Create global aggregator
            global_aggregator = MultiSiteAggregator()
            global_groups = set()
            bucket_results = {}
            
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
            
            # Aggregate global results
            logger.info(f"[{token}] Aggregating global results across all months")
            
            if progress_callback:
                progress_callback(80, 'A agregar resultados globais...')
            
            # Create combined results for all months
            global_combined = _aggregate_month_groups(global_aggregator, work_dir, global_groups)
            
            # Calculate global hand counts
            for group, group_data in global_combined.items():
                total_hands = 0
                total_postflop_hands = 0
                
                for month_result in result_data['months'].values():
                    if month_result.get('status') == 'completed':
                        for site_data in month_result.get('sites', {}).values():
                            if group in site_data:
                                total_hands += site_data[group].get('hand_count', 0)
                                total_postflop_hands += site_data[group].get('postflop_hands_count', 0)
                
                group_data['hand_count'] = total_hands
                group_data['postflop_hands_count'] = total_postflop_hands
            
            result_data['combined'] = global_combined
            
            # Write global cross-format postflop
            logger.info(f"[{token}] Writing global cross-format postflop outputs")
            global_aggregator.write_cross_format_postflop_outputs(work_dir)
            
            # Generate months manifest
            months_manifest = generate_months_manifest(token, buckets, bucket_results)
            result_data['months_manifest'] = months_manifest
            
            # Save months manifest
            manifest_path = os.path.join(work_dir, "months_manifest.json")
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(months_manifest, f, indent=2)
            
            # Aggregate global discard statistics
            global_discards = {}
            for month_result in result_data['months'].values():
                if 'aggregated_discards' in month_result:
                    for reason, count in month_result['aggregated_discards'].items():
                        if reason != 'total':
                            global_discards[reason] = global_discards.get(reason, 0) + count
            
            if global_discards:
                global_discards['total'] = sum(global_discards.values())
                
                # Calculate global valid hands from deduplicated combined groups
                # NOTE: Combined groups already have correct hand_count after global deduplication
                total_valid_hands = 0
                for group_data in global_combined.values():
                    total_valid_hands += group_data.get('hand_count', 0)
                
                total_hands = total_valid_hands + global_discards['total']
                
                logger.info(f"[{token}] Classification totals: valid_hands={total_valid_hands}, discarded={global_discards['total']}, total={total_hands}")
                
                result_data['classification'] = {
                    'discarded_hands': global_discards,
                    'total_hands': total_hands,
                    'valid_hands': total_valid_hands
                }
            
            # Generate global manifest
            manifest = global_aggregator.get_combined_manifest()
            if global_discards:
                manifest['discards'] = global_discards
            
            manifest_path = os.path.join(work_dir, "multi_site_manifest.json")
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=2)
        
        # Save final result
        result_file = os.path.join(work_dir, "pipeline_result.json")
        with open(result_file, 'w') as f:
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
            
            # Upload aggregate pipeline_result.json
            aggregate_result_path = os.path.join(work_dir, "pipeline_result.json")
            if os.path.exists(aggregate_result_path):
                with open(aggregate_result_path, 'rb') as f:
                    storage_path = f"/results/{token}/pipeline_result.json"
                    storage.upload_file_stream(f, storage_path, 'application/json')
                    logger.info(f"[{token}] ✅ Uploaded aggregate pipeline_result.json")
            
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
            
            logger.info(f"[{token}] ✅ All results uploaded to Supabase Storage successfully")
        except Exception as e:
            logger.error(f"[{token}] Failed to upload results to Supabase Storage: {e}")
            # Don't fail the job if storage upload fails in non-production
            # But log prominently for debugging
        
        if progress_callback:
            progress_callback(100, 'Processamento concluído!')
        
        return True, token, result_data
        
    except Exception as e:
        import traceback
        traceback_str = traceback.format_exc()
        logger.error(f"[{token}] Multi-site pipeline failed: {str(e)}")
        logger.error(f"[{token}] Traceback:\n{traceback_str}")
        error_info = {
            'error': str(e),
            'traceback': traceback_str
        }
        
        result_data['status'] = 'failed'
        result_data['error'] = error_info
        
        # Mark progress as failed
        progress_tracker.fail_job(token, str(e))
        
        return False, token, result_data
