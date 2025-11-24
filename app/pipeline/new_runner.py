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
from typing import Dict, Any, Tuple, Optional
from app.pipeline.runner import safe_extract_archive, generate_token, log_step

logger = logging.getLogger(__name__)

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
                    
                    # Analyze each hand
                    analyzed_count = 0
                    hero_found_count = 0
                    for idx, hand in enumerate(hands, 1):
                        if hand.strip():
                            # Check if Hero is in this hand
                            if "Hero" in hand:
                                hero_found_count += 1
                            # Analyze for ALL preflop stats including RFI
                            preflop_calculator.analyze_hand(hand)
                            analyzed_count += 1
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
            
            # ADD POSTFLOP STATS CALCULATION - Using raw text (same as preflop)
            from app.stats.postflop_calculator_v3 import PostflopCalculatorV3
            
            postflop_calculator = PostflopCalculatorV3(hand_collector=hand_collector)
            
            # Find the correct combined file location
            postflop_combined_file = None
            possible_locations = [
                combined_file,  # Original location
                os.path.join(work_dir, "by_site", "gg", "classified", group_key, f"{group_key}_combined.txt"),
                os.path.join(work_dir, "by_site", "pokerstars", "classified", group_key, f"{group_key}_combined.txt"),
            ]
            
            for location in possible_locations:
                if os.path.exists(location):
                    postflop_combined_file = location
                    logger.info(f"[{token}] Found postflop data at: {location}")
                    break
            
            # Process hands for postflop stats using raw text (same as PreflopStats)
            if postflop_combined_file and not is_empty_nonko:
                with open(postflop_combined_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    
                    # Split hands based on common patterns (same as multi_site_runner)
                    if "Poker Hand #" in content:
                        hands = re.split(r'(?=Poker Hand #)', content)
                    elif "PokerStars Hand #" in content:
                        hands = re.split(r'(?=PokerStars Hand #)', content)
                    else:
                        hands = content.split('\n\n')
                    
                    hands = [h for h in hands if h.strip()]
                    
                    logger.info(f"[{token}] Processing {len(hands)} hands for postflop stats in {group_key}")
                    
                    # Analyze each hand using raw text (same as PreflopStats)
                    for hand_text in hands:
                        if hand_text.strip():
                            postflop_calculator.analyze_hand(hand_text)
            else:
                logger.warning(f"[{token}] No postflop data file found for {group_key}")
            
            # Get postflop statistics
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
            
            # Save collected hands by stat
            saved_stats = hand_collector.save_all()
            logger.info(f"[{token}] Saved hands for {len(saved_stats)} statistics")
            
            # Get list of stats with collected hands
            stats_with_hands = hand_collector.get_stats_with_hands()
            
            # NOTE: Exclusion tracking REMOVED - get_exclusion_counts() now returns (0, 0)
            # Validations happen per-stat, not per-hand
            
            group_stats[group_key] = {
                'label': group_label,
                'file_count': classification_stats['groups'][group_key],
                'hand_count': hand_count,  # Add hand_count for API consistency
                'hands_parsed': hand_count,
                'total_hands': hand_count,  # Add explicit total_hands field
                'rfi_stats': rfi_stats,  # Add RFI statistics
                'bvb_stats': bvb_stats,  # Add BvB statistics
                'threbet_cc_stats': threbet_cc_stats,  # Add 3bet/CC statistics
                'vs_3bet_stats': vs_3bet_stats,  # Add vs 3bet statistics
                'squeeze_stats': squeeze_stats,  # Add Squeeze statistics
                'bb_defense_stats': bb_defense_stats,  # Add BB Defense statistics
                'sb_defense_stats': sb_defense_stats,  # Add SB Defense statistics
                'postflop_stats': postflop_stats,  # Add POSTFLOP statistics
                'postflop_hands_count': postflop_hands_count,  # Add count of hands eligible for postflop
                'scores': scores,  # Add scoring information (includes RFI, BvB, 3bet/CC, vs 3bet, Squeeze, BB Defense and SB Defense)
                'stats': {},  # Will be populated with other stats later
                'hands_by_stat': stats_with_hands  # Add information about collected hands
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
        logger.exception("[%s] Pipeline failed", token)
        
        # Save error info
        error_file = os.path.join(work_dir, "_logs", "error.json")
        with open(error_file, 'w') as f:
            json.dump(error_info, f, indent=2)
        
        result_data['status'] = 'failed'
        result_data['error'] = error_info
        
        return False, token, result_data