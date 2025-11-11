"""Consolidated dashboard payload builder"""

import json
import yaml
from pathlib import Path
from typing import Optional, Dict, Any
from app.parse.site_parsers.site_detector import detect_poker_site
from app.score.scoring import score_step


def detect_sites_from_hands(token: str, pipeline_sites: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
    """Detect poker sites from pipeline_result['sites'] structure and aggregate hand counts"""
    import logging
    logger = logging.getLogger(__name__)
    
    hands_by_site = {}
    
    if not pipeline_sites:
        logger.warning("[SITE_DETECT] No pipeline_sites provided")
        return hands_by_site
    
    # Iterate through sites (ggpoker, pokerstars, etc.)
    for site_name, site_data in pipeline_sites.items():
        if not isinstance(site_data, dict):
            continue
        
        logger.info(f"[SITE_DETECT] Processing site: {site_name}")
        
        # Iterate through groups (nonko_9max, nonko_6max, pko)
        total_hands = 0
        for group_name, group_data in site_data.items():
            if isinstance(group_data, dict) and 'hand_count' in group_data:
                hand_count = group_data['hand_count']
                total_hands += hand_count
                logger.info(f"[SITE_DETECT] {site_name}.{group_name}: {hand_count} hands")
        
        if total_hands > 0:
            hands_by_site[site_name] = total_hands
            logger.info(f"[SITE_DETECT] Total for {site_name}: {total_hands} hands")
    
    logger.info(f"[SITE_DETECT] Final hands_by_site: {hands_by_site}")
    return hands_by_site


def transform_group_for_frontend(group_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform pipeline_result group format to frontend expected format"""
    
    # Get base stats and scores
    base_stats = group_data.get('stats', {})
    scores_data = group_data.get('scores', {})
    
    # Merge scores into stats
    enriched_stats = {}
    for stat_name, stat_data in base_stats.items():
        # Copy base stat data
        enriched_stat = stat_data.copy()
        
        # Find score in score categories (rfi, bvb, threbet_cc, vs_3bet, squeeze, bb_defense, sb_defense)
        score_found = False
        for category_name, category_scores in scores_data.items():
            if not isinstance(category_scores, dict):
                continue
                
            # Check if category has .stats (like RFI)
            if 'stats' in category_scores and isinstance(category_scores['stats'], dict):
                # Search in .stats
                if stat_name in category_scores['stats']:
                    score_info = category_scores['stats'][stat_name]
                    if isinstance(score_info, dict):
                        enriched_stat['score'] = score_info.get('score')
                        enriched_stat['ideal'] = score_info.get('ideal')
                        enriched_stat['weight'] = score_info.get('weight')
                        score_found = True
                        break
            # Check if category has .details (like sb_defense, bb_defense)
            elif 'details' in category_scores and isinstance(category_scores['details'], dict):
                # Search in .details
                if stat_name in category_scores['details']:
                    score_info = category_scores['details'][stat_name]
                    if isinstance(score_info, dict):
                        enriched_stat['score'] = score_info.get('score')
                        enriched_stat['ideal'] = score_info.get('ideal')
                        enriched_stat['weight'] = score_info.get('weight')
                        score_found = True
                        break
            # Otherwise search directly in category
            elif stat_name in category_scores:
                score_info = category_scores[stat_name]
                if isinstance(score_info, dict):
                    enriched_stat['score'] = score_info.get('score')
                    enriched_stat['ideal'] = score_info.get('ideal')
                    enriched_stat['weight'] = score_info.get('weight')
                    score_found = True
                    break
        
        enriched_stats[stat_name] = enriched_stat
    
    # Build transformed structure
    transformed = {
        'hands_count': group_data.get('hand_count', group_data.get('total_hands', 0)),
        'stats': enriched_stats,
        'postflop_stats': group_data.get('postflop_stats', {}),
        'postflop_hands_count': group_data.get('postflop_hands_count', 0),
        'overall_score': group_data.get('overall_score', 0),
        'label': group_data.get('label', ''),
        'file_count': group_data.get('file_count', 0),
        'subgroups': {},
        'scores': scores_data  # Preserve full scores structure
    }
    
    return transformed


def calculate_weighted_scores_from_groups(pipeline_groups: Dict[str, Any], postflop_all: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Calculate weighted_scores for GERAL tab from pipeline groups"""
    
    weighted_scores = {}
    
    # Category weights for aggregation
    category_weights = {
        'rfi': 0.25,
        'bvb': 0.15,
        'threbet_cc': 0.20,
        'vs_3bet': 0.10,
        'squeeze': 0.10,
        'bb_defense': 0.10,
        'sb_defense': 0.10
    }
    
    # Aggregate NON-KO (9max + 6max weighted by hands)
    nonko_9max = pipeline_groups.get('nonko_9max', {})
    nonko_6max = pipeline_groups.get('nonko_6max', {})
    
    hands_9max = nonko_9max.get('hand_count', 0)
    hands_6max = nonko_6max.get('hand_count', 0)
    total_nonko_hands = hands_9max + hands_6max
    
    if total_nonko_hands > 0:
        weight_9max = hands_9max / total_nonko_hands
        weight_6max = hands_6max / total_nonko_hands
        
        # Use overall_score directly from each group (calculated by MultiSiteAggregator)
        score_9max = nonko_9max.get('overall_score', 0)
        score_6max = nonko_6max.get('overall_score', 0)
        
        # Weighted average of 9max and 6max overall scores
        nonko_score = score_9max * weight_9max + score_6max * weight_6max
        
        # Build subgroups for display (optional, for breakdown)
        nonko_subgroups = {}
        for category, weight in category_weights.items():
            category_score_9max = nonko_9max.get('scores', {}).get(category, {}).get('overall_score') or \
                                 nonko_9max.get('scores', {}).get(category, {}).get('_weighted_average') or \
                                 nonko_9max.get('scores', {}).get(category, {}).get('score') or 0
            
            category_score_6max = nonko_6max.get('scores', {}).get(category, {}).get('overall_score') or \
                                 nonko_6max.get('scores', {}).get(category, {}).get('_weighted_average') or \
                                 nonko_6max.get('scores', {}).get(category, {}).get('score') or 0
            
            category_score = (category_score_9max * weight_9max + category_score_6max * weight_6max) if (category_score_9max or category_score_6max) else 0
            
            # Map to frontend subgroup names
            subgroup_name_map = {
                'rfi': 'RFI',
                'bvb': 'BvB', 
                'threbet_cc': '3b & CC',
                'vs_3bet': 'vs 3b IP/OOP',
                'squeeze': 'Squeeze',
                'bb_defense': 'Defesa da BB',
                'sb_defense': 'Defesa da SB'
            }
            
            if category in subgroup_name_map:
                nonko_subgroups[subgroup_name_map[category]] = {
                    'score': category_score,
                    'weight': weight
                }
        
        weighted_scores['nonko'] = {
            'group_score': round(nonko_score, 1),
            'subgroups': nonko_subgroups,
            'weight_9max': int(weight_9max * 100),
            'weight_6max': int(weight_6max * 100)
        }
    
    # PKO scores
    pko = pipeline_groups.get('pko', {})
    if pko.get('hand_count', 0) > 0:
        # Use overall_score directly from PKO group (calculated by MultiSiteAggregator)
        pko_score = pko.get('overall_score', 0)
        
        # Build subgroups for display (optional, for breakdown)
        pko_subgroups = {}
        for category, weight in category_weights.items():
            score = pko.get('scores', {}).get(category, {}).get('overall_score') or \
                   pko.get('scores', {}).get(category, {}).get('_weighted_average') or \
                   pko.get('scores', {}).get(category, {}).get('score') or 0
            
            subgroup_name_map = {
                'rfi': 'RFI',
                'bvb': 'BvB',
                'threbet_cc': '3b & CC',
                'vs_3bet': 'vs 3b IP/OOP',
                'squeeze': 'Squeeze',
                'bb_defense': 'Defesa da BB',
                'sb_defense': 'Defesa da SB'
            }
            
            if category in subgroup_name_map:
                pko_subgroups[subgroup_name_map[category]] = {
                    'score': score,
                    'weight': weight
                }
        
        weighted_scores['pko'] = {
            'group_score': round(pko_score, 1),
            'subgroups': pko_subgroups
        }
    
    # Add POSTFLOP score to weighted_scores if provided
    if postflop_all and 'overall_score' in postflop_all:
        weighted_scores['postflop'] = {
            'group_score': postflop_all['overall_score'],
            'subgroups': {}  # Subgroups already in postflop_all
        }
    
    # Calculate overall weighted score with new formula:
    # POSTFLOP = 20% fixed, remaining 80% distributed proportionally between NON-KO and PKO
    if weighted_scores:
        total_hands = total_nonko_hands + pko.get('hand_count', 0)
        if total_hands > 0:
            # Calculate proportional weights for the 80% remaining (after POSTFLOP's 20%)
            nonko_proportion = total_nonko_hands / total_hands
            pko_proportion = pko.get('hand_count', 0) / total_hands
            
            # Apply 80% to proportional weights
            nonko_weight = nonko_proportion * 0.80
            pko_weight = pko_proportion * 0.80
            postflop_weight = 0.20  # Fixed 20%
            
            overall_score = 0
            if 'nonko' in weighted_scores:
                overall_score += weighted_scores['nonko']['group_score'] * nonko_weight
            if 'pko' in weighted_scores:
                overall_score += weighted_scores['pko']['group_score'] * pko_weight
            if 'postflop' in weighted_scores:
                overall_score += weighted_scores['postflop']['group_score'] * postflop_weight
            
            weighted_scores['overall'] = overall_score
            
            # Debug log for overall calculation
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"[OVERALL CALC] NON-KO: {weighted_scores.get('nonko', {}).get('group_score', 0):.1f} × {nonko_weight:.3f} = {weighted_scores.get('nonko', {}).get('group_score', 0) * nonko_weight:.1f}")
            logger.info(f"[OVERALL CALC] PKO: {weighted_scores.get('pko', {}).get('group_score', 0):.1f} × {pko_weight:.3f} = {weighted_scores.get('pko', {}).get('group_score', 0) * pko_weight:.1f}")
            logger.info(f"[OVERALL CALC] POSTFLOP: {weighted_scores.get('postflop', {}).get('group_score', 0):.1f} × {postflop_weight:.3f} = {weighted_scores.get('postflop', {}).get('group_score', 0) * postflop_weight:.1f}")
            logger.info(f"[OVERALL CALC] TOTAL: {overall_score:.1f}")
    
    return weighted_scores


def _build_month_payload(
    month: str,
    pipeline_combined: Dict,
    classification: Dict,
    ideals: Dict,
    stat_weights: Dict,
    sites_data: Dict
) -> Dict:
    """Build payload for a specific month using same logic as aggregate
    
    Args:
        month: Month key (YYYY-MM)
        pipeline_combined: pipeline_result['combined'] containing monthly data
        classification: Classification stats with per_month discards
        ideals: Ideal ranges config
        stat_weights: Stat weights config
        sites_data: pipeline_result['sites'] for hands_by_site
    
    Returns:
        Monthly payload mirroring aggregate structure
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Build monthly groups from pipeline data
    monthly_groups = {}
    monthly_groups_raw = {}  # For postflop aggregation
    
    for group_key, group_data in pipeline_combined.items():
        if isinstance(group_data, dict) and 'months' in group_data:
            if month in group_data['months']:
                monthly_group_raw = group_data['months'][month]
                monthly_groups_raw[group_key] = monthly_group_raw
                
                # Transform to frontend format
                monthly_groups[group_key] = transform_group_for_frontend(monthly_group_raw)
    
    # Calculate monthly valid hands BEFORE adding postflop_all (to avoid double counting)
    monthly_valid_hands = sum(g.get('hands_count', 0) for g in monthly_groups.values())
    
    # Aggregate monthly postflop stats
    monthly_postflop = aggregate_postflop_stats(monthly_groups_raw, ideals, stat_weights)
    if monthly_postflop:
        monthly_groups['postflop_all'] = monthly_postflop
    
    # Calculate monthly weighted scores
    monthly_weighted = calculate_weighted_scores_from_groups(monthly_groups_raw, monthly_postflop)
    
    # Extract monthly discard stats
    monthly_discards = classification.get('discarded_hands', {}).get('per_month', {}).get(month, {})
    monthly_discarded_total = sum(monthly_discards.values()) if monthly_discards else 0
    
    # Calculate monthly overall totals
    monthly_total_hands = monthly_valid_hands + monthly_discarded_total
    
    monthly_overall = {
        'total_hands': monthly_total_hands,
        'valid_hands': monthly_valid_hands,
        'discarded_hands': monthly_discarded_total
    }
    
    # Add overall score if available
    if 'overall' in monthly_weighted:
        monthly_overall['score'] = monthly_weighted['overall']
    
    # Calculate hands_by_site for this month
    monthly_hands_by_site = {}
    if sites_data:
        for site_name, site_groups in sites_data.items():
            for group_name, group_info in site_groups.items():
                if isinstance(group_info, dict) and 'months' in group_info:
                    if month in group_info['months']:
                        month_data = group_info['months'][month]
                        hand_count = month_data.get('hand_count', 0)
                        if hand_count > 0:
                            if site_name not in monthly_hands_by_site:
                                monthly_hands_by_site[site_name] = 0
                            monthly_hands_by_site[site_name] += hand_count
    
    # Build monthly payload
    monthly_payload = {
        'overall': monthly_overall,
        'groups': monthly_groups,
        'weighted_scores': monthly_weighted,
        'discard_stats': monthly_discards
    }
    
    if monthly_hands_by_site:
        monthly_payload['hands_by_site'] = monthly_hands_by_site
    
    logger.info(f"[MONTHLY] Built payload for {month}: {monthly_total_hands} total hands ({monthly_valid_hands} valid, {monthly_discarded_total} discarded)")
    
    return monthly_payload


def build_dashboard_payload(
    token: Optional[str],
    include_months: bool = False,
    month_filter: Optional[str] = None
) -> dict:
    """Build consolidated dashboard payload from runs or current directory
    
    Args:
        token: Job token to load results from
        include_months: If True, include monthly breakdowns in response
        month_filter: If set (YYYY-MM), only include this specific month
    
    Returns:
        Dashboard payload with optional 'months' field containing per-month data
    """
    
    # Resolve base directory
    if token:
        base = Path('runs') / token
    else:
        base = Path('.')
    
    # Try to load pipeline_result.json first (new pipeline)
    # Use ResultStorage to read from cloud storage instead of local work directory
    pipeline_result = {}
    import logging
    logger = logging.getLogger(__name__)
    
    if token:
        try:
            from app.services.result_storage import get_result_storage
            result_storage = get_result_storage()
            pipeline_result = result_storage.get_pipeline_result(token)
            if pipeline_result:
                logger.info(f"[LOAD] ✓ Loaded pipeline_result from storage with keys: {list(pipeline_result.keys())}")
            else:
                logger.warning(f"[LOAD] No pipeline_result found in storage for token: {token}")
        except Exception as e:
            pipeline_result = {}
            logger.error(f"[LOAD] Failed to load pipeline_result from storage: {e}")
    
    # Find stats file
    stats_path = base / 'stats' / 'stat_counts.json'
    if not stats_path.exists():
        stats_path = Path('stats') / 'stat_counts.json'
    
    # Find score file  
    score_path = base / 'scores' / 'scorecard.json'
    if not score_path.exists():
        score_path = Path('scores') / 'scorecard.json'
    
    # Load config file for weights and ideals
    config_path = Path('app/score/config.yml')
    config = {}
    if config_path.exists():
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
        except Exception:
            config = {}
    
    # Load JSONs if they exist
    counts = {}
    if stats_path.exists():
        try:
            with open(stats_path, 'r', encoding='utf-8') as f:
                counts = json.load(f)
        except Exception:
            counts = {}
    
    score = {}
    if score_path.exists():
        try:
            with open(score_path, 'r', encoding='utf-8') as f:
                score = json.load(f)
        except Exception:
            score = {}
    
    # Normalize score keys
    overall = (
        score.get('overall') or 
        score.get('data', {}).get('overall') or 
        score.get('_raw', {}).get('overall')
    )
    
    group_level = (
        score.get('group_level') or 
        score.get('data', {}).get('group_level') or 
        {}
    )
    
    samples = (
        score.get('samples') or 
        score.get('sample') or 
        {}
    )
    
    # Get scoring data
    stat_level = score.get('stat_level') or score.get('data', {}).get('stat_level') or {}
    subgroup_level = score.get('subgroup_level') or score.get('data', {}).get('subgroup_level') or {}
    
    # Extract config data
    weights = config.get('weights', {})
    ideals = config.get('ideals', {})
    group_weights = weights.get('groups', {})
    subgroup_weights = weights.get('subgroups', {})
    stat_weights = weights.get('stats', {})
    
    # Build hierarchical groups structure
    groups = build_groups_structure(
        counts=counts.get('counts', {}),
        ideals=ideals,
        stat_level=stat_level,
        subgroup_level=subgroup_level,
        group_weights=group_weights,
        subgroup_weights=subgroup_weights,
        stat_weights=stat_weights
    )
    
    # Get list of months from counts
    months = list(counts.keys()) if counts else []
    
    # Load ingest manifest if exists
    ingest = {}
    manifest_path = base / "manifest.json"
    if manifest_path.exists():
        try:
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
                ingest = {
                    "files_total": manifest.get("files_total", 0),
                    "files_pko": manifest.get("files_pko", 0),
                    "files_mystery": manifest.get("files_mystery", 0),
                    "files_nonko": manifest.get("files_nonko", 0),
                    "timestamp": manifest.get("timestamp", ""),
                    "source_zip": manifest.get("source_zip", "")
                }
        except Exception:
            pass
    
    # Build weighted_scores from group_level for frontend compatibility
    weighted_scores = {}
    if group_level:
        for group_name, group_data in group_level.items():
            weighted_scores[group_name] = {
                "group_score": group_data.get("score"),
                "subgroups": {}
            }
            # Add subgroup scores if available
            if subgroup_level:
                for subgroup_key, subgroup_data in subgroup_level.items():
                    if subgroup_key.startswith(f"{group_name}_"):
                        subgroup_name = subgroup_key.replace(f"{group_name}_", "")
                        weighted_scores[group_name]["subgroups"][subgroup_name] = {
                            "score": subgroup_data.get("score")
                        }
    
    # Extract data from pipeline_result if available
    discard_stats = {}
    if pipeline_result and 'classification' in pipeline_result:
        import logging
        logger = logging.getLogger(__name__)
        classification = pipeline_result['classification']
        
        # Extract totals from classification
        total_hands_found = classification.get('total_hands', 0)
        discarded_data = classification.get('discarded_hands', {})
        
        # Calculate valid hands
        total_discarded = discarded_data.get('total', 0) if isinstance(discarded_data, dict) else 0
        valid_hands_calculated = total_hands_found - total_discarded
        
        # Build discard_stats for frontend
        # NOTE: Stack and all-in exclusions REMOVED - validations now per-stat only
        if isinstance(discarded_data, dict):
            discard_stats = {
                'mystery': discarded_data.get('mystery', 0),
                'less_than_4_players': discarded_data.get('less_than_4_players', 0),
                'tournament_summary': discarded_data.get('tournament_summary', 0),
                'cash_game': discarded_data.get('cash_game', 0),
                'invalid_format': discarded_data.get('invalid_format', 0),
                'other': discarded_data.get('other', 0)
            }
        
        # Create or update overall with correct data
        if not overall:
            overall = {}
        overall['total_hands'] = total_hands_found
        overall['valid_hands'] = valid_hands_calculated
        overall['discarded_hands'] = total_discarded
        
        logger.info(f"[CLASSIFICATION] Extracted: total={total_hands_found}, valid={valid_hands_calculated}, discarded={total_discarded}, mystery={discard_stats.get('mystery', 0)}")
    
    # Add preflop groups from pipeline_result to groups structure
    if pipeline_result and 'combined' in pipeline_result:
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"[DEBUG] Adding groups from pipeline_result")
        logger.info(f"[DEBUG] Pipeline groups: {list(pipeline_result['combined'].keys())}")
        
        # Get hand counts from sites structure
        sites_data = pipeline_result.get('sites', {})
        hand_counts = {}
        for site_name, site_groups in sites_data.items():
            for grp_name, grp_info in site_groups.items():
                if isinstance(grp_info, dict) and 'hand_count' in grp_info:
                    if grp_name not in hand_counts:
                        hand_counts[grp_name] = 0
                    hand_counts[grp_name] += grp_info['hand_count']
        
        # Add each group (nonko_9max, nonko_6max, pko) to groups
        for grp_name, grp_data in pipeline_result['combined'].items():
            if isinstance(grp_data, dict):
                # Add hand_count from sites data
                grp_data['hand_count'] = hand_counts.get(grp_name, 0)
                
                # Transform pipeline_result format to frontend format
                transformed_group = transform_group_for_frontend(grp_data)
                groups[grp_name] = transformed_group
                logger.info(f"[DEBUG] Added group '{grp_name}' with {transformed_group.get('hands_count', 0)} hands, {len(transformed_group.get('stats', {}))} preflop stats")
        
        # Aggregate postflop stats
        postflop_all = aggregate_postflop_stats(pipeline_result['combined'], ideals, stat_weights)
        
        if postflop_all:
            subgroups_count = len(postflop_all.get('subgroups', {}))
            total_stats = sum(len(sg.get('stats', {})) for sg in postflop_all.get('subgroups', {}).values())
            logger.info(f"[DEBUG] postflop_all result: hands={postflop_all.get('hands_count', 0)}, subgroups={subgroups_count}, total_stats={total_stats}")
            
            # Check if there's already a postflop_all in groups from pipeline
            if 'postflop_all' in groups:
                logger.warning(f"[DEBUG] WARNING: postflop_all already exists in groups! Will be overwritten.")
                logger.warning(f"[DEBUG] Existing postflop_all structure: has stats={bool(groups.get('postflop_all', {}).get('stats'))}, has subgroups={bool(groups.get('postflop_all', {}).get('subgroups'))}")
            
            # Check if postflop_all is in pipeline_result['combined']
            if 'postflop_all' in pipeline_result.get('combined', {}):
                logger.warning(f"[DEBUG] WARNING: postflop_all found in pipeline_result['combined']! This should not happen!")
                pipeline_postflop = pipeline_result['combined']['postflop_all']
                logger.warning(f"[DEBUG] Pipeline postflop_all has: file_count={pipeline_postflop.get('file_count')}, sites_included={pipeline_postflop.get('sites_included')}")
            
            groups['postflop_all'] = postflop_all
            logger.info(f"[DEBUG] Added correct postflop_all to groups")
        else:
            logger.warning(f"[DEBUG] postflop_all is None or empty")
        
        # Calculate weighted_scores from pipeline groups (including postflop)
        weighted_scores = calculate_weighted_scores_from_groups(pipeline_result['combined'], postflop_all)
        logger.info(f"[DEBUG] Calculated weighted_scores: {list(weighted_scores.keys())}")
    
    # Detect poker sites from hands
    hands_by_site = detect_sites_from_hands(token, pipeline_result.get('sites')) if token and pipeline_result else {}
    
    # Build response
    response_data = {
        "run": {
            "token": token,
            "base": str(base)
        },
        "overall": overall,
        "group_level": group_level,
        "weighted_scores": weighted_scores,  # Frontend compatibility
        "samples": samples,
        "months": months,
        "counts": counts,
        "groups": groups,  # New hierarchical structure
        "ingest": ingest,  # Classification summary
        "discard_stats": discard_stats,  # Discard breakdown for frontend
        "total_hands": overall.get('total_hands', 0) if overall else 0,  # Direct access for frontend
        "valid_hands": overall.get('valid_hands', 0) if overall else 0,  # Direct access for frontend
        "hands_by_site": hands_by_site  # Detected poker sites with hand counts
    }
    
    # Final check on postflop_all before returning
    if 'postflop_all' in response_data.get('groups', {}):
        pf_all = response_data['groups']['postflop_all']
        logger.info(f"[DEBUG] Final check - postflop_all in response has:")
        logger.info(f"[DEBUG]   - hands_count: {pf_all.get('hands_count', 'N/A')}")
        logger.info(f"[DEBUG]   - overall_score: {pf_all.get('overall_score', 'N/A')}")
        logger.info(f"[DEBUG]   - subgroups count: {len(pf_all.get('subgroups', {}))}")
        logger.info(f"[DEBUG]   - has 'stats' field: {bool(pf_all.get('stats'))}")
        logger.info(f"[DEBUG]   - has 'file_count' field: {bool(pf_all.get('file_count'))}")
        logger.info(f"[DEBUG]   - has 'sites_included' field: {bool(pf_all.get('sites_included'))}")
        
        if pf_all.get('subgroups'):
            logger.info(f"[DEBUG]   - subgroup names: {list(pf_all.get('subgroups', {}).keys())}")
    
    # BUILD MONTHLY DATA if requested
    if include_months and pipeline_result and 'combined' in pipeline_result:
        logger.info(f"[MONTHLY] Building monthly payload (filter: {month_filter or 'all'})")
        
        # Collect all available months from all groups
        all_months = set()
        for group_key, group_data in pipeline_result['combined'].items():
            if isinstance(group_data, dict) and 'months' in group_data:
                all_months.update(group_data['months'].keys())
        
        # Filter months if requested
        if month_filter:
            months_to_process = [month_filter] if month_filter in all_months else []
            if not months_to_process:
                logger.warning(f"[MONTHLY] Requested month '{month_filter}' not found in available months: {sorted(all_months)}")
        else:
            months_to_process = sorted(all_months)
        
        # Build monthly payloads
        months_data = {}
        classification_data = pipeline_result.get('classification', {})
        sites_data = pipeline_result.get('sites', {})
        
        for month in months_to_process:
            monthly_payload = _build_month_payload(
                month=month,
                pipeline_combined=pipeline_result['combined'],
                classification=classification_data,
                ideals=ideals,
                stat_weights=stat_weights,
                sites_data=sites_data
            )
            months_data[month] = monthly_payload
        
        # Validate monthly totals match aggregate (Task 13 requirement)
        if months_data and overall:
            aggregate_valid = overall.get('valid_hands', 0)
            monthly_valid_sum = sum(m['overall']['valid_hands'] for m in months_data.values())
            
            if aggregate_valid != monthly_valid_sum:
                logger.warning(f"[VALIDATION] Monthly sum mismatch! Aggregate: {aggregate_valid}, Monthly sum: {monthly_valid_sum}, Diff: {aggregate_valid - monthly_valid_sum}")
            else:
                logger.info(f"[VALIDATION] ✓ Monthly sum matches aggregate: {aggregate_valid} valid hands")
        
        # Add monthly data to response
        response_data['months_data'] = months_data
        logger.info(f"[MONTHLY] Added {len(months_data)} months to response: {sorted(months_data.keys())}")
    
    return response_data


def build_groups_structure(
    counts: Dict[str, Any], 
    ideals: Dict[str, Any],
    stat_level: Dict[str, Any],
    subgroup_level: Dict[str, Any],
    group_weights: Dict[str, float],
    subgroup_weights: Dict[str, float],
    stat_weights: Dict[str, float]
) -> Dict[str, Any]:
    """Build hierarchical groups -> subgroups -> stats structure"""
    
    groups = {}
    
    # Find the first month with actual data (skip metadata keys)
    data_month = None
    for key in counts:
        if key not in ['generated_at', 'input', 'dsl', 'metric', 'hands_processed', 'errors', 'stats_computed']:
            data_month = key
            break
    
    if not data_month:
        return groups
    
    month_data = counts[data_month]
    
    # Detect subgroup from stat name
    def get_subgroup(stat_name: str) -> str:
        """Map stat name to subgroup"""
        stat_upper = stat_name.upper()
        
        # RFI stats
        if any(x in stat_upper for x in ['RFI_', 'CO_STEAL', 'BTN_STEAL']):
            return 'RFI'
        
        # BvB stats  
        if any(x in stat_upper for x in ['SB_UO', 'SB_STEAL', 'BB_FOLD_TO_SB', 'BB_RAISE_VS_SB']):
            return 'BvB'
            
        # VS_3BET stats (check before generic 3BET to avoid mis-grouping)
        if 'FOLD_TO_3BET' in stat_upper:
            return 'VS_3BET'
            
        # 3BET stats (generic, after VS_3BET check)
        if '3BET' in stat_upper:
            return '3BET_RANGE'
            
        # Cold call stats
        if 'COLD_CALL' in stat_upper:
            return 'CC_RANGE'
            
        # VPIP stats
        if 'VPIP' in stat_upper:
            return 'VPIP'
            
        # FOLD_RANGE stats
        if 'FOLD_TO_CO_STEAL' in stat_upper:
            return 'FOLD_RANGE'
            
        # SQUEEZE stats
        if 'SQUEEZE' in stat_upper:
            return 'SQUEEZE'
            
        # BB_DEFENSE stats
        if any(x in stat_upper for x in ['BB_FOLD_VS_', 'BB_RESTEAL']):
            return 'BB_DEFENSE'
            
        # SB_DEFENSE stats
        if any(x in stat_upper for x in ['SB_FOLD_TO_', 'SB_RESTEAL']):
            return 'SB_DEFENSE'
            
        # Postflop - FLOP_CBET
        if 'FLOP_CBET' in stat_upper:
            return 'FLOP_CBET'
            
        # Postflop - VS_CBET (fold/raise vs cbet or check-raise)
        if any(x in stat_upper for x in ['FOLD_VS_CBET', 'RAISE_CBET', 'FOLD_VS_CHECK_RAISE', 'FLOP_FOLD_VS_CBET', 'FLOP_RAISE_CBET']):
            return 'VS_CBET'
            
        # Postflop - VS_SKIPPED_CBET
        if 'BET_VS_MISSED_CBET' in stat_upper or 'FLOP_BET_VS_MISSED' in stat_upper:
            return 'VS_SKIPPED_CBET'
            
        # Postflop - TURN_PLAY
        if any(x in stat_upper for x in ['TURN_CBET', 'TURN_DONK', 'BET_TURN_VS_MISSED', 'TURN_FOLD_VS_CBET']):
            return 'TURN_PLAY'
            
        # Postflop - RIVER_PLAY
        if any(x in stat_upper for x in ['WTSD', 'W$SD', 'W$WSF', 'RIVER_AGG', 'RIVER_BET', 'W$SD% B']):
            return 'RIVER_PLAY'
            
        # Default
        return 'OTHER'
    
    # Process each group in month data
    for group_name, group_stats in month_data.items():
        if not isinstance(group_stats, dict):
            continue
            
        # Organize stats by subgroup
        subgroups = {}
        
        for stat_name, stat_data in group_stats.items():
            if not isinstance(stat_data, dict):
                continue
                
            subgroup = get_subgroup(stat_name)
            
            if subgroup not in subgroups:
                subgroups[subgroup] = {
                    "weight": subgroup_weights.get(subgroup, 0.05),
                    "stats": {}
                }
            
            # Get ideal for this stat and group
            stat_ideals = ideals.get(stat_name, {})
            ideal_value = stat_ideals.get(group_name, None)
            
            # Convert single ideal to range [ideal-2, ideal+2]
            ideal_range = None
            if ideal_value is not None:
                ideal_range = [ideal_value - 2, ideal_value + 2]
            
            # Get score for this stat
            stat_score = None
            if stat_level:
                stat_key = f"{group_name}_{stat_name}"
                if stat_key in stat_level:
                    stat_score = stat_level[stat_key].get('score')
            
            # Build stat entry
            subgroups[subgroup]["stats"][stat_name] = {
                "opps": stat_data.get("opportunities", 0),
                "att": stat_data.get("attempts", 0),
                "pct": stat_data.get("percentage", 0),
                "ideal": ideal_range,
                "score": stat_score,
                "weight": stat_weights.get(stat_name, 0.05),
                "idx": stat_data.get("index_files", {})
            }
        
        # Get group weight 
        group_weight = group_weights.get(group_name, 0.1)
        
        # Add to groups structure
        groups[group_name] = {
            "weight": group_weight,
            "subgroups": subgroups
        }
    
    return groups


def aggregate_postflop_stats(pipeline_groups: Dict[str, Any], ideals: Dict[str, Any], stat_weights: Dict[str, float]) -> Dict[str, Any]:
    """Aggregate postflop stats from all groups into postflop_all with proper schema using scoring_config"""
    import logging
    from app.stats.scoring_config import SCORING_CONFIG
    from app.score.scoring import score_step
    
    logger = logging.getLogger(__name__)
    
    # Aggregate postflop stats from all groups
    aggregated_stats = {}
    total_hands = 0
    
    for group_name, group_data in pipeline_groups.items():
        if not isinstance(group_data, dict):
            continue
        
        # Debug: Log the structure of group_data
        logger.info(f"[AGGREGATE DEBUG] Group {group_name} keys: {list(group_data.keys())}")
        
        # First check for postflop_stats field
        postflop_stats = group_data.get('postflop_stats', {})
        
        # If not found, try to extract postflop stats from the stats field
        if not postflop_stats and 'stats' in group_data:
            # Extract postflop stats from the main stats dictionary
            all_stats = group_data.get('stats', {})
            postflop_stats = {}
            
            # Define postflop stat patterns
            postflop_patterns = [
                "Flop CBet", "Flop fold", "Flop raise", "Fold vs Check Raise",
                "Check Raise", "Flop Bet vs", "Turn CBet", "Turn Cbet", "Turn Donk", "Turn donk",
                "Turn Fold", "Bet Turn", "Bet turn", "WTSD", "W$SD", "W$WSF", "W$SD% B River",
                "River Agg", "River Bet", "River bet"
            ]
            
            # Extract stats that match postflop patterns
            for stat_name, stat_data in all_stats.items():
                if any(pattern.lower() in stat_name.lower() for pattern in postflop_patterns):
                    postflop_stats[stat_name] = stat_data
                    logger.debug(f"[AGGREGATE] Extracted postflop stat '{stat_name}' from group {group_name}")
        
        # Get postflop hands count - try multiple locations
        postflop_hands_count = group_data.get('postflop_hands_count', 0)
        if postflop_hands_count == 0:
            # If not found, use the general hand count as fallback
            postflop_hands_count = group_data.get('hand_count', 0)
        
        total_hands += postflop_hands_count
        
        logger.info(f"[AGGREGATE] Group {group_name}: {len(postflop_stats)} postflop stats extracted, postflop_hands_count={postflop_hands_count}")
        if postflop_stats:
            logger.info(f"[AGGREGATE] Group {group_name} postflop stats: {list(postflop_stats.keys())[:5]}...")  # Show first 5 for brevity
        
        # Aggregate each stat
        for stat_name, stat_data in postflop_stats.items():
            if stat_name not in aggregated_stats:
                aggregated_stats[stat_name] = {
                    'opportunities': 0,
                    'attempts': 0
                }
                # Initialize player_sum for W$WSF Rating
                if stat_name == 'W$WSF Rating' and 'player_sum' in stat_data:
                    aggregated_stats[stat_name]['player_sum'] = 0
                # Initialize total_hands for River Agg %
                if stat_name == 'River Agg %' and 'total_hands' in stat_data:
                    aggregated_stats[stat_name]['total_hands'] = 0
                logger.debug(f"[AGGREGATE] First time seeing '{stat_name}' from {group_name}")
            
            aggregated_stats[stat_name]['opportunities'] += stat_data.get('opportunities', 0)
            aggregated_stats[stat_name]['attempts'] += stat_data.get('attempts', 0)
            
            # Aggregate player_sum and calculate rating for W$WSF Rating
            if stat_name == 'W$WSF Rating' and 'player_sum' in stat_data:
                aggregated_stats[stat_name]['player_sum'] = aggregated_stats[stat_name].get('player_sum', 0) + stat_data.get('player_sum', 0)
                
                # Recalculate W$WSF Rating percentage after aggregating player_sum
                total_player_sum = aggregated_stats[stat_name]['player_sum']
                total_opps = aggregated_stats[stat_name]['opportunities']
                total_wins = aggregated_stats[stat_name]['attempts']
                
                if total_opps > 0 and total_player_sum > 0:
                    avg_players = total_player_sum / total_opps
                    win_rate = total_wins / total_opps
                    expected_rate = 1.0 / avg_players
                    rating = win_rate / expected_rate if expected_rate > 0 else 0
                    aggregated_stats[stat_name]['percentage'] = round(rating, 2)
            
            # Aggregate total_hands and recalculate ratio for River Agg %
            # Ratio: (bets+raises) / calls = aggression level
            if stat_name == 'River Agg %':
                # Aggregate total_hands count
                if 'total_hands' in stat_data:
                    aggregated_stats[stat_name]['total_hands'] = aggregated_stats[stat_name].get('total_hands', 0) + stat_data.get('total_hands', 0)
                
                total_bets_raises = aggregated_stats[stat_name]['attempts']  # numerator
                total_calls = aggregated_stats[stat_name]['opportunities']  # denominator
                if total_calls > 0:
                    aggregated_stats[stat_name]['percentage'] = round(total_bets_raises / total_calls, 2)
                elif total_bets_raises > 0:
                    aggregated_stats[stat_name]['percentage'] = 10.0  # Pure aggression (no calls)
                else:
                    aggregated_stats[stat_name]['percentage'] = 0.0
    
    # Log aggregated stats
    logger.info(f"[AGGREGATE] Total aggregated stats: {len(aggregated_stats)}, Total hands: {total_hands}")
    if aggregated_stats:
        first_key = list(aggregated_stats.keys())[0] if aggregated_stats else None
        if first_key:
            logger.info(f"[AGGREGATE] Sample stat '{first_key}': {aggregated_stats[first_key]}")
    
    # If no postflop stats found, return empty structure instead of None
    if not aggregated_stats:
        logger.warning("[AGGREGATE] No postflop stats found, returning empty structure")
        return {
            'weight': 0.1,
            'hands_count': total_hands,
            'subgroups': {}
        }
    
    # Get postflop configuration
    postflop_config = SCORING_CONFIG.get('postflop_all', {})
    
    # Build stats grouped by subgroup from config using 'group' field
    subgroups = {}
    group_stats_count = {}  # Track stats per group for weight calculation
    
    logger.info(f"[POSTFLOP] Processing {len(aggregated_stats)} aggregated stats: {list(aggregated_stats.keys())}")
    
    for stat_name, stat_data in aggregated_stats.items():
        opps = stat_data['opportunities']
        att = stat_data['attempts']
        
        # Use pre-calculated percentage if available (for special stats like W$WSF Rating, River Agg %)
        # Otherwise calculate standard percentage
        if 'percentage' in stat_data:
            pct = stat_data['percentage']
        else:
            pct = (att / opps * 100) if opps > 0 else 0
        
        # Get config for this stat from postflop_all
        stat_config = postflop_config.get(stat_name)
        
        if not stat_config:
            logger.warning(f"[POSTFLOP] No config found for '{stat_name}', skipping")
            continue
        
        # Get group from config (e.g., 'Flop Cbet', 'Vs Cbet', etc.)
        group_name = stat_config.get('group', 'Other')
        
        # Get scoring parameters
        ideal = stat_config.get('ideal', 50)
        osc_down = stat_config.get('oscillation_down', 5)
        osc_up = stat_config.get('oscillation_up', 5)
        stat_weight = stat_config.get('weight', 0.05)
        
        # Calculate ideal range (±2% default)
        ideal_range = [ideal - 2, ideal + 2] if ideal else None
        
        # Calculate score (skip if no ideal value - e.g., W$WSF Rating)
        score_value = None
        if opps > 0 and ideal is not None:
            score_value = score_step(
                actual_pct=pct,
                ideal_pct=ideal,
                step_down_pct=osc_down,
                step_up_pct=osc_up,
                points_per_step_down=10,
                points_per_step_up=10
            )
        
        # Initialize subgroup if needed
        if group_name not in subgroups:
            subgroups[group_name] = {
                'weight': 0.0,  # Will be calculated from stat weights
                'stats': {}
            }
            group_stats_count[group_name] = 0
        
        # Add stat to subgroup - using frontend-expected field names
        subgroups[group_name]['stats'][stat_name] = {
            'key': stat_name,  # Add key field for frontend
            'opportunities': opps,  # Frontend expects 'opportunities'
            'attempts': att,  # Frontend expects 'attempts'  
            'percentage': pct,  # Frontend expects 'percentage'
            'ideal': ideal_range,
            'score': score_value,
            'weight': stat_weight,
            'idx': {},
            # Also include short names for backward compatibility
            'opps': opps,
            'att': att,
            'pct': pct
        }
        
        # Include total_hands for River Agg % (total unique hands with action)
        if stat_name == 'River Agg %' and 'total_hands' in stat_data:
            subgroups[group_name]['stats'][stat_name]['total_hands'] = stat_data['total_hands']
        
        # Track group weight (sum of stat weights)
        subgroups[group_name]['weight'] += stat_weight
        group_stats_count[group_name] += 1
    
    # Normalize group weights to sum to 1.0
    total_weight = sum(sg['weight'] for sg in subgroups.values())
    if total_weight > 0:
        for subgroup in subgroups.values():
            subgroup['weight'] = subgroup['weight'] / total_weight
    
    logger.info(f"[POSTFLOP] Created {len(subgroups)} subgroups: {list(subgroups.keys())}")
    
    # Calculate aggregate score for each subgroup (weighted average of stat scores)
    subgroup_scores = []
    for group_name, group_data in subgroups.items():
        weighted_score_sum = 0.0
        total_stat_weight = 0.0
        
        for stat_name, stat_data in group_data['stats'].items():
            stat_score = stat_data.get('score')
            stat_weight = stat_data.get('weight', 0.0)
            
            # Only include stats with valid scores (skip W$WSF Rating with score=None)
            if stat_score is not None and stat_weight > 0:
                weighted_score_sum += stat_score * stat_weight
                total_stat_weight += stat_weight
        
        # Calculate weighted average score for this subgroup
        group_score = (weighted_score_sum / total_stat_weight) if total_stat_weight > 0 else 0
        group_data['score'] = round(group_score, 1)
        
        # Track for overall POSTFLOP score calculation
        if group_score > 0:
            subgroup_scores.append(group_score)
        
        logger.info(f"[POSTFLOP] Subgroup '{group_name}' score: {group_score:.1f}")
    
    # Calculate overall POSTFLOP score (weighted average of subgroup scores)
    # Weights: Flop Cbet 35%, Vs Cbet 20%, Vs Skipped Cbet 10%, Turn Play 20%, River play 15%
    subgroup_weights = {
        'Flop Cbet': 0.35,
        'Vs Cbet': 0.20,
        'vs Skipped Cbet': 0.10,
        'Turn Play': 0.20,
        'River play': 0.15
    }
    
    overall_postflop_score = 0
    if subgroups:
        weighted_sum = 0.0
        for group_name, group_data in subgroups.items():
            group_score = group_data.get('score', 0)
            group_weight = subgroup_weights.get(group_name, 0)
            weighted_sum += group_score * group_weight
        
        overall_postflop_score = round(weighted_sum, 1)
        logger.info(f"[POSTFLOP] Overall POSTFLOP score: {overall_postflop_score}")
    
    # Return with proper group schema (weight + subgroups + hands_count + overall_score)
    return {
        'weight': 0.1,
        'hands_count': total_hands,
        'overall_score': overall_postflop_score,
        'subgroups': subgroups
    }