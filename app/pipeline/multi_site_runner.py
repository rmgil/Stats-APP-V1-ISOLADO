"""
Multi-site pipeline runner that extends the new_runner to handle multiple poker sites
"""
import os
import json
import logging
import copy
from pathlib import Path
from typing import Dict, Any, Tuple, List, Optional
from app.pipeline.new_runner import run_simplified_pipeline
from app.stats.aggregate import MultiSiteAggregator
from app.parse.site_parsers.site_detector import detect_poker_site
from app.parse.utils import extract_month_from_hand
from collections import defaultdict

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

def run_multi_site_pipeline(archive_path: str, work_root: str = "work", token: Optional[str] = None, progress_callback=None) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Run pipeline that handles multiple poker sites and aggregates results
    
    1. Extract archive
    2. Detect which sites are present
    3. Process each site through existing pipeline
    4. Aggregate results across sites
    
    Args:
        archive_path: Path to the archive file
        work_root: Root directory for work files
        token: Optional pre-generated token (if None, generates new one)
        progress_callback: Optional callback(percent, message) for progress updates
    """
    from app.pipeline.runner import safe_extract_archive, generate_token, log_step
    from app.classify.group_classifier import classify_into_final_groups, create_group_manifest
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
    import re
    
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
        
        # Step 2: Detect sites in extracted files
        logger.info(f"[{token}] Detecting poker sites in files")
        progress_tracker.update_stage(token, 'detection', 'in_progress', 'Detectando salas de poker...')
        
        if progress_callback:
            progress_callback(40, 'A detetar salas de poker...')
        
        site_files = detect_sites_in_directory(input_dir)
        
        if not site_files:
            logger.warning(f"[{token}] No recognized poker sites found, falling back to single-site processing")
            # Fall back to regular pipeline if no sites detected - pass token to avoid creating new one
            return run_simplified_pipeline(archive_path, work_root, token=token)
        
        result_data['sites_detected'] = list(site_files.keys())
        logger.info(f"[{token}] Detected {len(site_files)} sites: {list(site_files.keys())}")
        
        if progress_callback:
            sites_str = ', '.join(site_files.keys())
            progress_callback(45, f'Detetadas {len(site_files)} salas: {sites_str}')
        
        # Step 3: Process each site separately
        aggregator = MultiSiteAggregator()
        all_groups = set()
        
        total_sites = len(site_files)
        for site_idx, (site, files) in enumerate(site_files.items(), 1):
            logger.info(f"[{token}] Processing {len(files)} files from {site}")
            
            if progress_callback:
                percent = 45 + (site_idx / total_sites) * 25
                progress_callback(int(percent), f'A processar {site} ({len(files)} ficheiros)...')
            
            # Create site-specific directory
            site_dir = os.path.join(work_dir, "by_site", site)
            site_input_dir = os.path.join(site_dir, "in")
            os.makedirs(site_input_dir, exist_ok=True)
            
            # Copy site files to site-specific directory
            import shutil
            for file_path in files:
                dest_path = os.path.join(site_input_dir, os.path.basename(file_path))
                shutil.copy2(file_path, dest_path)
            
            # Classify files into groups
            classified_dir = os.path.join(site_dir, "classified")
            progress_tracker.update_stage(token, 'classification', 'in_progress', f'Classificando mãos de {site}...')
            classification_stats = classify_into_final_groups(site_input_dir, classified_dir, token=token)
            
            # Process each group for this site
            site_stats = {}
            
            # Store discard stats for this site
            if 'discarded_hands' in classification_stats:
                if 'discarded_hands' not in result_data:
                    result_data['discarded_hands'] = {}
                # Store discards by site
                result_data['discarded_hands'][site] = classification_stats['discarded_hands']
            
            for group_key, group_label in classification_stats['group_labels'].items():
                group_dir = os.path.join(classified_dir, group_key)
                
                # Skip if no files in group (unless NON-KO)
                is_nonko = 'nonko' in group_key.lower()
                if classification_stats['groups'][group_key] == 0 and not is_nonko:
                    continue
                
                all_groups.add(group_key)
                
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
                from app.stats.postflop_calculator_v4 import PostflopCalculatorV4
                postflop_calculator = PostflopCalculatorV4(hand_collector=hand_collector)
                
                # MEMORY OPTIMIZATION: Stream hands instead of loading all into memory
                from app.utils.hand_streaming import stream_hands_from_combined_file, count_hands_in_file
                from app.utils.memory_monitor import log_memory_usage
                import gc
                
                # Log memory before processing
                log_memory_usage(f"{token}/{site}/{group_key} BEFORE")
                
                # Count total hands for progress tracking
                total_hands = count_hands_in_file(combined_file, site)
                progress_tracker.update_stage(token, 'parsing', 'in_progress', f'Processando {total_hands} mãos...')
                logger.info(f"[{token}] {site}/{group_key}: Processing {total_hands} hands with streaming (memory-efficient)")
                
                # MONTHLY SEPARATION: Create monthly calculators (lazy instantiation)
                monthly_hand_collectors = {}
                monthly_preflop_calcs = {}
                monthly_postflop_calcs = {}
                months_seen = set()
                
                # Process hands in streaming mode - one at a time
                hands_processed = 0
                batch_size = 100  # Run GC every 100 hands
                
                for hand_text in stream_hands_from_combined_file(combined_file, site):
                    if hand_text.strip():
                        # Analyze hand for AGGREGATE stats (preflop and postflop)
                        preflop_calculator.analyze_hand(hand_text)
                        postflop_calculator.analyze_hand(hand_text)
                        
                        # MONTHLY SEPARATION: Extract month and route to monthly calculator
                        month = extract_month_from_hand(hand_text)
                        if month:
                            # Lazy create monthly calculators on first encounter
                            if month not in months_seen:
                                months_seen.add(month)
                                month_dir = os.path.join(site_dir, "months", month, "hands_by_stat", group_key)
                                monthly_hand_collectors[month] = HandCollector(month_dir)
                                monthly_preflop_calcs[month] = PreflopStats(hand_collector=monthly_hand_collectors[month])
                                monthly_postflop_calcs[month] = PostflopCalculatorV4(hand_collector=monthly_hand_collectors[month])
                                logger.info(f"[{token}] {site}/{group_key}: Created monthly calculators for {month}")
                            
                            # Route hand to its monthly calculator
                            monthly_preflop_calcs[month].analyze_hand(hand_text)
                            monthly_postflop_calcs[month].analyze_hand(hand_text)
                        
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
                
                # Determine table format (needed for both monthly and aggregate scoring)
                if "9max" in group_key:
                    table_format = "9max"
                elif "6max" in group_key:
                    table_format = "6max"
                else:
                    table_format = "PKO"
                
                # MONTHLY STATS: Process stats for each month
                months_data = {}
                if monthly_preflop_calcs:
                    logger.info(f"[{token}] {site}/{group_key}: Processing stats for {len(monthly_preflop_calcs)} months")
                    
                    for month in sorted(monthly_preflop_calcs.keys()):
                        logger.info(f"[{token}] {site}/{group_key}: Calculating stats for {month}")
                        
                        # Get monthly calculators
                        month_preflop_calc = monthly_preflop_calcs[month]
                        month_postflop_calc = monthly_postflop_calcs[month]
                        month_hand_collector = monthly_hand_collectors[month]
                        
                        # Get stats summary
                        month_all_stats = month_preflop_calc.get_stats_summary()
                        month_postflop_stats = month_postflop_calc.get_stats_summary()
                        month_postflop_hands_count = month_postflop_calc.get_hands_count()
                        
                        # Merge postflop into all stats
                        month_all_stats.update(month_postflop_stats)
                        
                        # Calculate scores (reuse same logic as aggregate)
                        month_scoring_calc = ScoringCalculator()
                        
                        # RFI scores
                        month_rfi_stats = {k: v for k, v in month_all_stats.items() if "RFI" in k or k in ["CO Steal", "BTN Steal"]}
                        month_rfi_scores = month_scoring_calc.calculate_group_scores(group_key, month_rfi_stats) if month_rfi_stats else {}
                        
                        # BvB scores
                        month_bvb_stats = {k: v for k, v in month_all_stats.items() if k in ["SB UO VPIP", "BB fold vs SB steal", "BB raise vs SB limp UOP", "SB Steal"]}
                        month_bvb_scores = calculate_bvb_scores(month_bvb_stats, table_format) if month_bvb_stats else {}
                        
                        # 3bet/CC scores
                        month_threbet_cc_stats = {k: v for k, v in month_all_stats.items() if "3bet" in k or "Cold Call" in k or "VPIP" in k or "BTN fold to CO steal" in k}
                        month_threbet_cc_scores = calculate_3bet_cc_scores(month_threbet_cc_stats, table_format) if month_threbet_cc_stats else {}
                        
                        # vs 3bet scores
                        month_vs_3bet_stats = {k: v for k, v in month_all_stats.items() if "Fold to 3bet" in k}
                        month_vs_3bet_scores = calculate_vs_3bet_scores(month_vs_3bet_stats, table_format) if month_vs_3bet_stats else {}
                        
                        # Squeeze scores
                        month_squeeze_stats = {k: v for k, v in month_all_stats.items() if "Squeeze" in k}
                        month_squeeze_scorer = SqueezeScorer()
                        month_squeeze_scores = month_squeeze_scorer.calculate_squeeze_scores(month_squeeze_stats, group_key) if month_squeeze_stats else {}
                        
                        # BB defense scores
                        month_bb_defense_stats = {k: v for k, v in month_all_stats.items() if "BB fold vs CO steal" in k or "BB fold vs BTN steal" in k or "BB resteal vs BTN steal" in k}
                        month_bb_defense_scorer = BBDefenseScorer()
                        month_bb_defense_scores = month_bb_defense_scorer.calculate_bb_defense_scores(month_bb_defense_stats, month_bvb_stats, group_key) if month_bb_defense_stats else {}
                        
                        # SB defense scores
                        month_sb_defense_stats = {k: v for k, v in month_all_stats.items() if "SB fold to CO Steal" in k or "SB fold to BTN Steal" in k or "SB resteal vs BTN" in k}
                        month_sb_defense_scorer = SBDefenseScorer()
                        month_sb_defense_scores = month_sb_defense_scorer.calculate_group_score(month_sb_defense_stats, table_format) if month_sb_defense_stats else {}
                        
                        # Calculate overall score
                        month_category_scores = []
                        if month_rfi_scores.get('overall_score'):
                            month_category_scores.append(month_rfi_scores['overall_score'])
                        if month_bvb_scores.get('overall_score'):
                            month_category_scores.append(month_bvb_scores['overall_score'])
                        if month_threbet_cc_scores.get('overall_score'):
                            month_category_scores.append(month_threbet_cc_scores['overall_score'])
                        if month_vs_3bet_scores.get('overall_score'):
                            month_category_scores.append(month_vs_3bet_scores['overall_score'])
                        if month_squeeze_scores.get('overall_score'):
                            month_category_scores.append(month_squeeze_scores['overall_score'])
                        if month_bb_defense_scores.get('overall_score'):
                            month_category_scores.append(month_bb_defense_scores['overall_score'])
                        if month_sb_defense_scores.get('overall_score'):
                            month_category_scores.append(month_sb_defense_scores['overall_score'])
                        
                        month_overall_score = sum(month_category_scores) / len(month_category_scores) if month_category_scores else 0
                        
                        # Save monthly hands_by_stat files
                        month_saved_stats = month_hand_collector.save_all()
                        month_hands_by_stat = month_hand_collector.get_hands_by_stat()
                        
                        # Store monthly data
                        months_data[month] = {
                            'stats': month_all_stats,
                            'overall_score': month_overall_score,
                            'postflop_stats': month_postflop_stats,
                            'postflop_hands_count': month_postflop_hands_count,
                            'scores': {
                                'rfi': month_rfi_scores,
                                'bvb': month_bvb_scores,
                                'threbet_cc': month_threbet_cc_scores,
                                'vs_3bet': month_vs_3bet_scores,
                                'squeeze': month_squeeze_scores,
                                'bb_defense': month_bb_defense_scores,
                                'sb_defense': month_sb_defense_scores
                            },
                            'hands_by_stat': month_hands_by_stat
                        }
                        
                        logger.info(f"[{token}] {site}/{group_key}: Completed monthly stats for {month} (score: {month_overall_score:.1f})")
                
                # Get AGGREGATE statistics
                all_stats = preflop_calculator.get_stats_summary()
                
                # Get postflop statistics
                postflop_stats = postflop_calculator.get_stats_summary()
                postflop_hands_count = postflop_calculator.get_hands_count()
                
                progress_tracker.update_stage(token, 'parsing', 'completed', 'Análise concluída')
                progress_tracker.update_stage(token, 'stats', 'in_progress', 'Calculando estatísticas...')
                
                if progress_callback:
                    percent = 70 + (site_idx / total_sites) * 5
                    progress_callback(int(percent), f'A calcular estatísticas {site}/{group_key}...')
                
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
                
                # Store site-specific results WITH SCORES AND MONTHLY DATA
                site_stats[group_key] = {
                    'stats': all_stats,
                    'overall_score': overall_score,  # Add overall score
                    'hands_by_stat': hands_by_stat,
                    'hand_count': hand_count,
                    'postflop_hands_count': postflop_hands_count,
                    'postflop_stats': postflop_stats,
                    'scores': {  # Add individual category scores
                        'rfi': rfi_scores,
                        'bvb': bvb_scores,
                        'threbet_cc': threbet_cc_scores,
                        'vs_3bet': vs_3bet_scores,
                        'squeeze': squeeze_scores,
                        'bb_defense': bb_defense_scores,
                        'sb_defense': sb_defense_scores
                    },
                    'sites_included': [site],  # Track which site this is from
                    'months_data': months_data  # Add monthly breakdown
                }
                
                # Add to aggregator
                aggregator.add_site_results(site, group_key, all_stats, hands_by_stat)
                
                logger.info(f"[{token}] {site}/{group_key}: Processed {hand_count} hands")
            
            result_data['sites'][site] = site_stats
        
        # Step 4: Aggregate results across all sites
        logger.info(f"[{token}] Aggregating statistics across all sites")
        
        combined_stats = {}
        for group in all_groups:
            # Write combined outputs (now returns {'overall_score': X, 'stats': {...}})
            aggregated_result = aggregator.write_combined_outputs(work_dir, group)
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
            
            # Calculate total hands and postflop hands for this group across all sites
            total_hands = 0
            total_postflop_hands = 0
            for site in result_data.get('sites', {}).values():
                if group in site:
                    total_hands += site[group].get('hand_count', 0)
                    total_postflop_hands += site[group].get('postflop_hands_count', 0)
            
            # AGGREGATE MONTHLY DATA across all sites for this group
            combined_months_data = {}
            for site_name, site in result_data.get('sites', {}).items():
                if group in site and 'months_data' in site[group]:
                    site_months = site[group]['months_data']
                    for month, month_data in site_months.items():
                        if month not in combined_months_data:
                            # First time seeing this month - initialize with deep copy
                            combined_months_data[month] = copy.deepcopy(month_data)
                            logger.info(f"[{token}] Combined/{group}: Initialized {month} from {site_name}")
                        else:
                            # Aggregate stats from another site for same month
                            logger.info(f"[{token}] Combined/{group}: Aggregating {month} from {site_name}")
                            
                            # Merge regular stats (sum opportunities and attempts)
                            for stat_name, stat_data in month_data.get('stats', {}).items():
                                if stat_name in combined_months_data[month]['stats']:
                                    # Sum opportunities and attempts
                                    if 'opportunities' in stat_data:
                                        combined_months_data[month]['stats'][stat_name]['opportunities'] += stat_data['opportunities']
                                    if 'attempts' in stat_data:
                                        combined_months_data[month]['stats'][stat_name]['attempts'] += stat_data['attempts']
                                    # Recalculate percentage
                                    opps = combined_months_data[month]['stats'][stat_name].get('opportunities', 0)
                                    atts = combined_months_data[month]['stats'][stat_name].get('attempts', 0)
                                    if opps > 0:
                                        combined_months_data[month]['stats'][stat_name]['percentage'] = (atts / opps) * 100
                                else:
                                    # New stat from this site - DEEP COPY to avoid reference mutation
                                    combined_months_data[month]['stats'][stat_name] = copy.deepcopy(stat_data)
                            
                            # Merge postflop stats
                            for stat_name, stat_data in month_data.get('postflop_stats', {}).items():
                                if stat_name in combined_months_data[month].get('postflop_stats', {}):
                                    if 'opportunities' in stat_data:
                                        combined_months_data[month]['postflop_stats'][stat_name]['opportunities'] += stat_data['opportunities']
                                    if 'attempts' in stat_data:
                                        combined_months_data[month]['postflop_stats'][stat_name]['attempts'] += stat_data['attempts']
                                    # Recalculate percentage
                                    opps = combined_months_data[month]['postflop_stats'][stat_name].get('opportunities', 0)
                                    atts = combined_months_data[month]['postflop_stats'][stat_name].get('attempts', 0)
                                    if opps > 0:
                                        combined_months_data[month]['postflop_stats'][stat_name]['percentage'] = (atts / opps) * 100
                                else:
                                    if 'postflop_stats' not in combined_months_data[month]:
                                        combined_months_data[month]['postflop_stats'] = {}
                                    # DEEP COPY to avoid reference mutation
                                    combined_months_data[month]['postflop_stats'][stat_name] = copy.deepcopy(stat_data)
                            
                            # Update postflop_hands_count
                            combined_months_data[month]['postflop_hands_count'] = combined_months_data[month].get('postflop_hands_count', 0) + month_data.get('postflop_hands_count', 0)
                            
                            # NOTE: Scores will be recalculated below after aggregation
                            # NOTE: hands_by_stat will remain from first site (download files already written separately per site)
            
            # RECALCULATE SCORES for aggregated monthly data
            for month, month_data in combined_months_data.items():
                combined_month_stats = month_data.get('stats', {})
                
                # Recalculate all category scores based on aggregated stats
                month_scoring_calc = ScoringCalculator()
                month_rfi_stats = {k: v for k, v in combined_month_stats.items() if "RFI" in k or k in ["CO Steal", "BTN Steal"]}
                month_rfi_scores = month_scoring_calc.calculate_group_scores(group, month_rfi_stats) if month_rfi_stats else {}
                
                month_bvb_stats = {k: v for k, v in combined_month_stats.items() if k in ["SB UO VPIP", "BB fold vs SB steal", "BB raise vs SB limp UOP", "SB Steal"]}
                month_bvb_scores = calculate_bvb_scores(month_bvb_stats, table_format) if month_bvb_stats else {}
                
                month_threbet_cc_stats = {k: v for k, v in combined_month_stats.items() if "3bet" in k or "Cold Call" in k or "VPIP" in k or "BTN fold to CO steal" in k}
                month_threbet_cc_scores = calculate_3bet_cc_scores(month_threbet_cc_stats, table_format) if month_threbet_cc_stats else {}
                
                month_vs_3bet_stats = {k: v for k, v in combined_month_stats.items() if "Fold to 3bet" in k}
                month_vs_3bet_scores = calculate_vs_3bet_scores(month_vs_3bet_stats, table_format) if month_vs_3bet_stats else {}
                
                month_squeeze_stats = {k: v for k, v in combined_month_stats.items() if "Squeeze" in k}
                month_squeeze_scorer = SqueezeScorer()
                month_squeeze_scores = month_squeeze_scorer.calculate_squeeze_scores(month_squeeze_stats, group) if month_squeeze_stats else {}
                
                month_bb_defense_stats = {k: v for k, v in combined_month_stats.items() if "BB fold vs CO steal" in k or "BB fold vs BTN steal" in k or "BB resteal vs BTN steal" in k}
                month_bb_defense_scorer = BBDefenseScorer()
                month_bb_defense_scores = month_bb_defense_scorer.calculate_bb_defense_scores(month_bb_defense_stats, month_bvb_stats, group) if month_bb_defense_stats else {}
                
                month_sb_defense_stats = {k: v for k, v in combined_month_stats.items() if "SB fold to CO Steal" in k or "SB fold to BTN Steal" in k or "SB resteal vs BTN" in k}
                month_sb_defense_scorer = SBDefenseScorer()
                month_sb_defense_scores = month_sb_defense_scorer.calculate_group_score(month_sb_defense_stats, table_format) if month_sb_defense_stats else {}
                
                # Recalculate overall score
                month_category_scores = []
                if month_rfi_scores.get('overall_score'):
                    month_category_scores.append(month_rfi_scores['overall_score'])
                if month_bvb_scores.get('overall_score'):
                    month_category_scores.append(month_bvb_scores['overall_score'])
                if month_threbet_cc_scores.get('overall_score'):
                    month_category_scores.append(month_threbet_cc_scores['overall_score'])
                if month_vs_3bet_scores.get('overall_score'):
                    month_category_scores.append(month_vs_3bet_scores['overall_score'])
                if month_squeeze_scores.get('overall_score'):
                    month_category_scores.append(month_squeeze_scores['overall_score'])
                if month_bb_defense_scores.get('overall_score'):
                    month_category_scores.append(month_bb_defense_scores['overall_score'])
                if month_sb_defense_scores.get('overall_score'):
                    month_category_scores.append(month_sb_defense_scores['overall_score'])
                
                month_overall_score = sum(month_category_scores) / len(month_category_scores) if month_category_scores else 0
                
                # Update month_data with recalculated scores
                combined_months_data[month]['overall_score'] = month_overall_score
                combined_months_data[month]['scores'] = {
                    'rfi': month_rfi_scores,
                    'bvb': month_bvb_scores,
                    'threbet_cc': month_threbet_cc_scores,
                    'vs_3bet': month_vs_3bet_scores,
                    'squeeze': month_squeeze_scores,
                    'bb_defense': month_bb_defense_scores,
                    'sb_defense': month_sb_defense_scores
                }
            
            combined_stats[group] = {
                'stats': aggregated_stats,
                'hand_count': total_hands,  # CRITICAL: Add hand_count for dashboard
                'overall_score': overall_score,  # Include overall_score from new scoring system
                'postflop_stats': postflop_stats,  # Add postflop stats separately
                'postflop_hands_count': total_postflop_hands,  # Add total postflop hands
                'scores': {
                    'rfi': rfi_scores,
                    'bvb': bvb_scores,
                    'threbet_cc': threbet_cc_scores,
                    'vs_3bet': vs_3bet_scores,
                    'squeeze': squeeze_scores,
                    'bb_defense': bb_defense_scores,
                    'sb_defense': sb_defense_scores
                },
                'sites_included': list(aggregator.site_data.get(group, {}).keys()) if hasattr(aggregator, 'site_data') else [],
                'months_data': combined_months_data  # Add aggregated monthly data
            }
        
        result_data['combined'] = combined_stats
        
        if progress_callback:
            progress_callback(75, 'A agregar resultados de todas as salas...')
        
        # Write cross-format postflop outputs (all formats combined)
        logger.info(f"[{token}] Writing cross-format postflop outputs")
        cross_format_postflop = aggregator.write_cross_format_postflop_outputs(work_dir)
        logger.info(f"[{token}] Cross-format postflop complete: {len(cross_format_postflop)} stats")
        
        if progress_callback:
            progress_callback(80, 'A finalizar processamento...')
        
        # Aggregate discard statistics across all sites
        if 'discarded_hands' in result_data:
            aggregated_discards = {}
            for site, discards in result_data['discarded_hands'].items():
                for reason, count in discards.items():
                    if reason not in ['total', 'total_segments', 'per_month']:  # Skip totals and monthly data
                        aggregated_discards[reason] = aggregated_discards.get(reason, 0) + count
            
            # Calculate total discarded
            aggregated_discards['total'] = sum(aggregated_discards.values())
            
            # Store aggregated discards
            result_data['aggregated_discards'] = aggregated_discards
            
            # Calculate total valid hands across all sites and groups
            total_valid_hands = 0
            for group_data in combined_stats.values():
                if 'stats' in group_data:
                    # Sum opportunities from any stat (they should all have same valid hand count)
                    for stat_name, stat_data in group_data['stats'].items():
                        if isinstance(stat_data, dict) and 'valid_hands' in stat_data:
                            total_valid_hands += stat_data.get('valid_hands', 0)
                            break  # Only need one stat per group since all should have same count
            
            # If we couldn't get valid hands from stats, try counting from aggregator
            if total_valid_hands == 0:
                # Count hands based on groups
                for site, site_data in result_data['sites'].items():
                    for group_key, group_info in site_data.items():
                        if isinstance(group_info, dict):
                            total_valid_hands += group_info.get('hand_count', 0)
            
            # Calculate correct total hands: valid + ALL discarded  
            total_hands = total_valid_hands + aggregated_discards['total']
            
            # Store in classification format for dashboard display
            result_data['classification'] = {
                'discarded_hands': aggregated_discards,
                'total_hands': total_hands,
                'valid_hands': total_valid_hands
            }
        
        # Generate manifest
        manifest = aggregator.get_combined_manifest()
        
        # Add discard stats to manifest
        if 'aggregated_discards' in result_data:
            manifest['discards'] = result_data['aggregated_discards']
        
        manifest_path = os.path.join(work_dir, "multi_site_manifest.json")
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)
        
        # Save final result
        result_file = os.path.join(work_dir, "pipeline_result.json")
        with open(result_file, 'w') as f:
            json.dump(result_data, f, indent=2)
        
        result_data['status'] = 'completed'
        log_step(token, "pipeline", "completed", f"Multi-site pipeline completed successfully ({len(site_files)} sites)")
        
        # Mark progress as complete
        progress_tracker.update_stage(token, 'stats', 'completed', 'Dashboard pronto!')
        progress_tracker.complete_job(token)
        
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