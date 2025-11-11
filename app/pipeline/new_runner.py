"""
New simplified pipeline runner for the updated workflow
"""
import os
import json
import logging
import traceback
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Tuple, Optional, List
from collections import defaultdict
from app.pipeline.runner import safe_extract_archive, generate_token, log_step
from app.parse.utils import extract_month_from_hand

logger = logging.getLogger(__name__)


def group_hands_by_month(hands: List[str], token: str) -> Dict[str, List[str]]:
    """
    Group hand history texts by month (YYYY-MM format).
    
    Args:
        hands: List of raw hand history texts
        token: Processing token for logging
        
    Returns:
        Dictionary mapping month (YYYY-MM) to list of hand texts
        Example: {"2025-11": [hand1, hand2], "2025-10": [hand3]}
    """
    monthly_hands = defaultdict(list)
    no_date_hands = []
    
    for hand_text in hands:
        if not hand_text or not hand_text.strip():
            continue
            
        month = extract_month_from_hand(hand_text)
        if month:
            monthly_hands[month].append(hand_text)
        else:
            no_date_hands.append(hand_text)
    
    # Log grouping results
    if no_date_hands:
        logger.warning(f"[{token}] {len(no_date_hands)} hands without extractable date")
    
    if monthly_hands:
        sorted_months = sorted(monthly_hands.keys())
        logger.info(f"[{token}] Grouped hands into {len(sorted_months)} months:")
        for month in sorted_months:
            logger.info(f"[{token}]   {month}: {len(monthly_hands[month])} hands")
    
    return dict(monthly_hands)

def run_simplified_pipeline(archive_path: str, work_root: str = "work", token: Optional[str] = None) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Run simplified pipeline:
    1. Extract archive (recursive)
    2. Classify into 3 groups (9-max nonKO, 6-max nonKO, PKO)
    3. Parse and compute stats for each group
    
    Args:
        archive_path: Path to the archive file
        work_root: Root directory for work files
        token: Optional pre-generated token (if None, generates new one)
    """
    from app.classify.group_classifier import classify_into_final_groups, create_group_manifest
    from app.parse.derive import derive_hands_enriched
    # from app.stats.compute import compute_group_stats  # Will implement later
    
    # Use provided token or generate new one
    # Track if token was provided externally (for heartbeat logic)
    token_provided = token is not None
    if token is None:
        token = generate_token()
    
    work_dir = os.path.join(work_root, token)
    
    # Create directories
    os.makedirs(work_dir, exist_ok=True)
    os.makedirs(os.path.join(work_dir, "in"), exist_ok=True)
    os.makedirs(os.path.join(work_dir, "_logs"), exist_ok=True)
    
    result_data = {
        'token': token,
        'status': 'processing',
        'groups': {}
    }
    
    try:
        # Step 1: Extract archive
        logger.info(f"[{token}] Extracting archive")
        log_step(token, "extract", "started", "Extracting archive recursively")
        
        input_dir = os.path.join(work_dir, "in")
        file_count = safe_extract_archive(archive_path, input_dir)
        
        if file_count == 0:
            log_step(token, "extract", "failed", "", "No .txt files found")
            raise ValueError("No .txt files found in archive")
        
        log_step(token, "extract", "completed", f"Extracted {file_count} files")
        result_data['extracted_files'] = file_count
        
        # Step 2: Classify into 3 groups
        logger.info(f"[{token}] Classifying into groups")
        log_step(token, "classify", "started", "Classifying into 3 groups")
        
        classified_dir = os.path.join(work_dir, "classified")
        classification_stats = classify_into_final_groups(input_dir, classified_dir, token=token)
        
        # Log detailed discard statistics if available
        if 'discarded_hands' in classification_stats:
            discards = classification_stats['discarded_hands']
            logger.info(f"[{token}] === RESUMO DE PROCESSAMENTO ===")
            logger.info(f"[{token}] Total de mãos encontradas: {classification_stats.get('total_hands', 0)}")
            logger.info(f"[{token}] Processadas com sucesso: {classification_stats.get('total_hands', 0) - discards.get('total', 0)}")
            logger.info(f"[{token}] Descartadas: {discards.get('total', 0)}")
            logger.info(f"[{token}]   - Mystery Bounty: {discards.get('mystery', 0)} mãos")
            logger.info(f"[{token}]   - Menos de 4 jogadores: {discards.get('less_than_4_players', 0)} mãos")
            logger.info(f"[{token}]   - Resumos de torneio: {discards.get('tournament_summary', 0)} mãos")
            logger.info(f"[{token}]   - Cash games: {discards.get('cash_game', 0)} mãos")
            logger.info(f"[{token}]   - Formato inválido: {discards.get('invalid_format', 0)} mãos")
            logger.info(f"[{token}]   - Outros: {discards.get('other', 0)} mãos")
        
        # Save classification manifest
        manifest_path = os.path.join(work_dir, "group_manifest.json")
        create_group_manifest(classification_stats, manifest_path)
        
        log_step(token, "classify", "completed", f"Classified into {len(classification_stats['groups'])} groups")
        result_data['classification'] = classification_stats
        
        # Step 3: Parse and compute stats for each group
        logger.info(f"[{token}] Computing statistics for each group")
        log_step(token, "stats", "started", "Computing group statistics")
        
        group_stats = {}
        # NOTE: Stack and all-in exclusions REMOVED - validations now per-stat only
        
        for group_key, group_label in classification_stats['group_labels'].items():
            group_dir = os.path.join(classified_dir, group_key)
            
            # For NON-KO groups, always process even if empty to maintain consistency
            # This ensures NON-KO stats are always available for weighted average calculation
            is_nonko = 'nonko' in group_key.lower()
            
            # Skip if no files in group UNLESS it's a NON-KO group
            if classification_stats['groups'][group_key] == 0 and not is_nonko:
                continue
            
            # Get hand count from classification stats
            hand_count = classification_stats.get('hands_per_group', {}).get(group_key, 0)
            
            # If we have a combined file, count hands in it
            if hand_count == 0:
                combined_file = os.path.join(group_dir, f"{group_key}_combined.txt")
                if os.path.exists(combined_file):
                    with open(combined_file, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        # Count hands by splitting on double newlines
                        hands = [h for h in content.split('\n\n') if h.strip()]
                        hand_count = len(hands)
            
            # Compute RFI stats for this group (keep original implementation)
            from app.stats.rfi_calculator import RFICalculator
            from app.stats.scoring_calculator import ScoringCalculator
            from app.stats.preflop_stats import PreflopStats
            from app.score.bvb_scorer import calculate_bvb_scores
            from app.score.threbet_cc_scorer import calculate_3bet_cc_scores
            from app.score.vs_3bet_scorer import calculate_vs_3bet_scores
            from app.score.squeeze_scorer import SqueezeScorer
            from app.score.bb_defense_scorer import BBDefenseScorer
            from app.score.sb_defense_scorer import SBDefenseScorer
            
            # Create hand collector for this group with unique path per format
            from app.stats.hand_collector import HandCollector
            hand_collector = HandCollector(os.path.join(work_dir, "hands_by_stat", group_key))
            
            # Apply multi-site patch for parsing hands from different sites
            from app.stats.preflop_stats_multisite import patch_preflop_stats
            patch_preflop_stats()
            
            # Create single preflop calculator for ALL stats including RFI with hand collector
            preflop_calculator = PreflopStats(hand_collector=hand_collector)
            
            # Create AGGREGATE postflop calculator (will be fed in single pass with preflop)
            from app.stats.postflop_calculator_v4 import PostflopCalculatorV4
            postflop_calculator = PostflopCalculatorV4(hand_collector=hand_collector)
            
            # Check if this is an empty NON-KO group
            is_empty_nonko = is_nonko and hand_count == 0
            
            # Read combined hands file (skip if empty NON-KO group)
            combined_file = os.path.join(group_dir, f"{group_key}_combined.txt")
            
            # If combined file doesn't exist in the expected location, check by_site directory
            if not os.path.exists(combined_file):
                # Try to find it in by_site directory structure (for multi_site processing)
                alt_combined_file = os.path.join(work_dir, "by_site", "pokerstars", "classified", group_key, f"{group_key}_combined.txt")
                if os.path.exists(alt_combined_file):
                    combined_file = alt_combined_file
                    logger.info(f"[{token}] Using alternative combined file path: {alt_combined_file}")
            
            # Initialize monthly calculators dict
            monthly_preflop_calcs = {}
            monthly_postflop_calcs = {}
            monthly_hand_collectors = {}
            undated_hand_count = 0
            
            if os.path.exists(combined_file) and not is_empty_nonko:
                with open(combined_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    
                    # Try different split patterns to find individual hands
                    # PokerStars hands typically start with "Poker Hand #"
                    if "Poker Hand #" in content:
                        # Split by hand header
                        hands = re.split(r'(?=Poker Hand #)', content)
                        hands = [h for h in hands if h.strip()]
                    else:
                        # Fallback to double newline split
                        hands = content.split('\n\n')
                        hands = [h for h in hands if h.strip() and len(h) > 100]
                    
                    total_hands = len(hands)
                    logger.info(f"[{token}] Calculating stats for {group_key}: {total_hands} hands to process")
                    
                    # Group hands by month for monthly stats
                    monthly_hands_grouped = group_hands_by_month(hands, token)
                    
                    # Create monthly calculators for each month
                    for month in monthly_hands_grouped.keys():
                        monthly_hand_collectors[month] = HandCollector(
                            os.path.join(work_dir, "months", group_key, month, "hands_by_stat")
                        )
                        monthly_preflop_calcs[month] = PreflopStats(
                            hand_collector=monthly_hand_collectors[month]
                        )
                        from app.stats.postflop_calculator_v4 import PostflopCalculatorV4
                        monthly_postflop_calcs[month] = PostflopCalculatorV4(
                            hand_collector=monthly_hand_collectors[month]
                        )
                    
                    # Single pass: process hands for aggregate AND monthly stats
                    analyzed_count = 0
                    hero_found_count = 0
                    for idx, hand in enumerate(hands, 1):
                        if hand.strip():
                            # Check if Hero is in this hand
                            if "Hero" in hand:
                                hero_found_count += 1
                            
                            # SINGLE PASS: Process for AGGREGATE stats (preflop + postflop)
                            preflop_calculator.analyze_hand(hand)
                            postflop_calculator.analyze_hand(hand)
                            analyzed_count += 1
                            
                            # SINGLE PASS: Process for MONTHLY stats (preflop + postflop)
                            month = extract_month_from_hand(hand)
                            if month and month in monthly_preflop_calcs:
                                monthly_preflop_calcs[month].analyze_hand(hand)
                                monthly_postflop_calcs[month].analyze_hand(hand)
                            elif not month:
                                undated_hand_count += 1
            elif is_empty_nonko:
                # For empty NON-KO groups, log that we're creating zero stats
                logger.info(f"[{token}] Creating zero statistics for empty group: {group_key}")
                analyzed_count = 0
                hero_found_count = 0
            
            # Get RFI statistics from preflop calculator (which already calculates them)
            all_stats = preflop_calculator.get_stats_summary()
            # Filter only RFI stats (Early RFI, Middle RFI, CO Steal, BTN Steal)
            rfi_stats = {k: v for k, v in all_stats.items() if "RFI" in k or k in ["CO Steal", "BTN Steal"]}
            
            # Get BvB statistics from preflop calculator
            bvb_stats = preflop_calculator.get_stats_summary()
            # Filter only BvB stats
            bvb_stats = {k: v for k, v in bvb_stats.items() if k in ["SB UO VPIP", "BB fold vs SB steal", "BB raise vs SB limp UOP", "SB Steal"]}
            
            # Get 3bet/CC statistics from the preflop calculator
            threbet_cc_stats = preflop_calculator.get_stats_summary()
            # Filter only 3bet/CC stats
            threbet_cc_stats = {k: v for k, v in threbet_cc_stats.items() if "3bet" in k or "Cold Call" in k or "VPIP" in k or "BTN fold to CO steal" in k}
            
            # Get vs 3bet statistics from the preflop calculator
            vs_3bet_stats = preflop_calculator.get_stats_summary()
            # Filter only vs 3bet stats
            vs_3bet_stats = {k: v for k, v in vs_3bet_stats.items() if "Fold to 3bet" in k}
            
            # Get Squeeze statistics from the preflop calculator
            squeeze_stats = preflop_calculator.get_stats_summary()
            # Filter only Squeeze stats
            squeeze_stats = {k: v for k, v in squeeze_stats.items() if "Squeeze" in k}
            
            # Get BB Defense statistics from the preflop calculator
            bb_defense_stats = preflop_calculator.get_stats_summary()
            # Filter only BB defense stats (exclude BB fold vs SB steal which comes from BvB)
            bb_defense_stats = {k: v for k, v in bb_defense_stats.items() if "BB fold vs CO steal" in k or "BB fold vs BTN steal" in k or "BB resteal vs BTN steal" in k}
            
            # Get SB Defense statistics from the preflop calculator
            sb_defense_stats = preflop_calculator.get_stats_summary()
            # Filter only SB defense stats
            sb_defense_stats = {k: v for k, v in sb_defense_stats.items() if "SB fold to CO Steal" in k or "SB fold to BTN Steal" in k or "SB resteal vs BTN" in k}
            
            # Get AGGREGATE postflop statistics (already processed in single pass above)
            # NOTE: Duplicate file reading REMOVED - postflop now processed alongside preflop
            postflop_stats = postflop_calculator.get_stats_summary()
            postflop_hands_count = postflop_calculator.get_hands_count()
            
            # Log detailed summary for verification
            postflop_calculator.log_final_summary()
            
            # Log stats
            logger.info(f"[{token}] RFI stats for {group_key}: {rfi_stats}")
            logger.info(f"[{token}] BvB stats for {group_key}: {bvb_stats}")
            logger.info(f"[{token}] 3bet/CC stats for {group_key}: {threbet_cc_stats}")
            logger.info(f"[{token}] vs 3bet stats for {group_key}: {vs_3bet_stats}")
            logger.info(f"[{token}] Squeeze stats for {group_key}: {squeeze_stats}")
            logger.info(f"[{token}] BB Defense stats for {group_key}: {bb_defense_stats}")
            logger.info(f"[{token}] SB Defense stats for {group_key}: {sb_defense_stats}")
            logger.info(f"[{token}] POSTFLOP stats for {group_key}: {len(postflop_stats)} stats calculated, {postflop_hands_count} hands eligible")
            
            # Calculate RFI scores
            scoring_calc = ScoringCalculator()
            rfi_scores = scoring_calc.calculate_group_scores(group_key, rfi_stats)
            
            # Calculate BvB scores based on group format
            if "9max" in group_key:
                table_format = "9max"
            elif "6max" in group_key:
                table_format = "6max"
            elif "pko" in group_key:
                table_format = "pko"
            else:
                table_format = "9max"  # Default
            
            bvb_scores = calculate_bvb_scores(bvb_stats, table_format)
            
            # Calculate 3bet/CC scores
            threbet_cc_scores = calculate_3bet_cc_scores(threbet_cc_stats, table_format)
            
            # Calculate vs 3bet scores
            vs_3bet_scores = calculate_vs_3bet_scores(vs_3bet_stats, table_format)
            
            # Calculate Squeeze scores
            squeeze_scorer = SqueezeScorer()
            squeeze_scores = squeeze_scorer.calculate_squeeze_scores(squeeze_stats, group_key)
            
            # Calculate BB Defense scores
            bb_defense_scorer = BBDefenseScorer()
            bb_defense_scores = bb_defense_scorer.calculate_bb_defense_scores(bb_defense_stats, bvb_stats, group_key)
            
            # Calculate SB Defense scores
            sb_defense_scorer = SBDefenseScorer()
            # Determine table format for SB defense scoring
            if "9max" in group_key:
                sb_table_format = "9max"
            elif "6max" in group_key:
                sb_table_format = "6max"
            else:
                sb_table_format = "PKO"
            sb_defense_scores = sb_defense_scorer.calculate_group_score(sb_defense_stats, sb_table_format)
            
            # Combine scores
            scores = {
                'rfi': rfi_scores,
                'bvb': bvb_scores,
                'threbet_cc': threbet_cc_scores,
                'vs_3bet': vs_3bet_scores,
                'squeeze': squeeze_scores,
                'bb_defense': bb_defense_scores,
                'sb_defense': sb_defense_scores
            }
            
            # Save collected hands by stat (AGGREGATE)
            saved_stats = hand_collector.save_all()
            logger.info(f"[{token}] Saved hands for {len(saved_stats)} statistics")
            
            # Get list of stats with collected hands
            stats_with_hands = hand_collector.get_stats_with_hands()
            
            # CALCULATE MONTHLY STATS AND SCORES
            months_data = {}
            for month in sorted(monthly_preflop_calcs.keys()):
                logger.info(f"[{token}] Calculating stats for {group_key} - {month}")
                
                # Get stats from monthly calculators
                month_preflop_calc = monthly_preflop_calcs[month]
                month_postflop_calc = monthly_postflop_calcs[month]
                
                month_all_stats = month_preflop_calc.get_stats_summary()
                
                # Extract categorized stats (same filters as aggregate)
                month_rfi_stats = {k: v for k, v in month_all_stats.items() if "RFI" in k or k in ["CO Steal", "BTN Steal"]}
                month_bvb_stats = {k: v for k, v in month_all_stats.items() if k in ["SB UO VPIP", "BB fold vs SB steal", "BB raise vs SB limp UOP", "SB Steal"]}
                month_threbet_cc_stats = {k: v for k, v in month_all_stats.items() if "3bet" in k or "Cold Call" in k or "VPIP" in k or "BTN fold to CO steal" in k}
                month_vs_3bet_stats = {k: v for k, v in month_all_stats.items() if "Fold to 3bet" in k}
                month_squeeze_stats = {k: v for k, v in month_all_stats.items() if "Squeeze" in k}
                month_bb_defense_stats = {k: v for k, v in month_all_stats.items() if "BB fold vs CO steal" in k or "BB fold vs BTN steal" in k or "BB resteal vs BTN steal" in k}
                month_sb_defense_stats = {k: v for k, v in month_all_stats.items() if "SB fold to CO Steal" in k or "SB fold to BTN Steal" in k or "SB resteal vs BTN" in k}
                month_postflop_stats = month_postflop_calc.get_stats_summary()
                month_postflop_hands_count = month_postflop_calc.get_hands_count()
                
                # Calculate scores for this month (reuse same scoring logic)
                month_rfi_scores = scoring_calc.calculate_group_scores(group_key, month_rfi_stats)
                month_bvb_scores = calculate_bvb_scores(month_bvb_stats, table_format)
                month_threbet_cc_scores = calculate_3bet_cc_scores(month_threbet_cc_stats, table_format)
                month_vs_3bet_scores = calculate_vs_3bet_scores(month_vs_3bet_stats, table_format)
                month_squeeze_scores = squeeze_scorer.calculate_squeeze_scores(month_squeeze_stats, group_key)
                month_bb_defense_scores = bb_defense_scorer.calculate_bb_defense_scores(month_bb_defense_stats, month_bvb_stats, group_key)
                month_sb_defense_scores = sb_defense_scorer.calculate_group_score(month_sb_defense_stats, sb_table_format)
                
                # Save monthly hands by stat
                monthly_hand_collectors[month].save_all()
                month_stats_with_hands = monthly_hand_collectors[month].get_stats_with_hands()
                
                # Store monthly data
                months_data[month] = {
                    'rfi_stats': month_rfi_stats,
                    'bvb_stats': month_bvb_stats,
                    'threbet_cc_stats': month_threbet_cc_stats,
                    'vs_3bet_stats': month_vs_3bet_stats,
                    'squeeze_stats': month_squeeze_stats,
                    'bb_defense_stats': month_bb_defense_stats,
                    'sb_defense_stats': month_sb_defense_stats,
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
                    'hands_by_stat': month_stats_with_hands
                }
                
                logger.info(f"[{token}] Completed stats for {group_key} - {month}")
            
            # NOTE: Exclusion tracking REMOVED - get_exclusion_counts() now returns (0, 0)
            # Validations happen per-stat, not per-hand
            
            group_stats[group_key] = {
                'label': group_label,
                'file_count': classification_stats['groups'][group_key],
                'hand_count': hand_count,  # Add hand_count for API consistency
                'hands_parsed': hand_count,
                'total_hands': hand_count,  # Add explicit total_hands field
                'undated_hand_count': undated_hand_count,  # Hands without extractable date
                'rfi_stats': rfi_stats,  # Add RFI statistics (AGGREGATE)
                'bvb_stats': bvb_stats,  # Add BvB statistics (AGGREGATE)
                'threbet_cc_stats': threbet_cc_stats,  # Add 3bet/CC statistics (AGGREGATE)
                'vs_3bet_stats': vs_3bet_stats,  # Add vs 3bet statistics (AGGREGATE)
                'squeeze_stats': squeeze_stats,  # Add Squeeze statistics (AGGREGATE)
                'bb_defense_stats': bb_defense_stats,  # Add BB Defense statistics (AGGREGATE)
                'sb_defense_stats': sb_defense_stats,  # Add SB Defense statistics (AGGREGATE)
                'postflop_stats': postflop_stats,  # Add POSTFLOP statistics (AGGREGATE)
                'postflop_hands_count': postflop_hands_count,  # Add count of hands eligible for postflop
                'scores': scores,  # Add scoring information (AGGREGATE)
                'stats': {},  # Will be populated with other stats later
                'hands_by_stat': stats_with_hands,  # Add information about collected hands (AGGREGATE)
                'months': months_data  # Add MONTHLY separation - stats/scores per YYYY-MM
            }
        
        # NOTE: Exclusion counts REMOVED from discarded_hands
        # Stack and all-in validations now happen per-stat, not per-hand
        # Only hand-level exclusions remain: Mystery, <4 players, cash games, invalid format
        
        # Log final summary
        total_hands_found = classification_stats.get('total_hands', 0)
        total_discarded = classification_stats.get('discarded_hands', {}).get('total', 0)
        total_classified = total_hands_found - total_discarded
        logger.info(f"[{token}] ===== FINAL HAND COUNT =====")
        logger.info(f"[{token}] Total hands found: {total_hands_found}")
        logger.info(f"[{token}] Hands classified (entered dataset): {total_classified}")
        logger.info(f"[{token}] Hands discarded (hand-level only): {total_discarded}")
        logger.info(f"[{token}] Stack/all-in exclusions: 0 (now validated per-stat)")
        logger.info(f"[{token}] ==============================")
        
        log_step(token, "stats", "completed", "Statistics computed for all groups")
        result_data['groups'] = group_stats
        result_data['status'] = 'completed'
        
        # Save final result
        result_file = os.path.join(work_dir, "pipeline_result.json")
        with open(result_file, 'w') as f:
            json.dump(result_data, f, indent=2)
        
        log_step(token, "pipeline", "completed", "Pipeline completed successfully")
        logger.info(f"[{token}] Pipeline completed successfully")
        
        return True, token, result_data
        
    except Exception as e:
        error_info = {
            'error': str(e),
            'traceback': traceback.format_exc()
        }
        
        log_step(token, "pipeline", "failed", "", str(e))
        logger.error(f"[{token}] Pipeline failed: {str(e)}")
        
        # Save error info
        error_file = os.path.join(work_dir, "_logs", "error.json")
        with open(error_file, 'w') as f:
            json.dump(error_info, f, indent=2)
        
        result_data['status'] = 'failed'
        result_data['error'] = error_info
        
        return False, token, result_data