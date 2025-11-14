"""
Multi-site statistics aggregation module
Combines statistics and hands from multiple poker sites while preserving original formatting
"""
import os
import json
import logging
from typing import Dict, List, Any, Optional, Set
from pathlib import Path
from app.stats.scoring_calculator import ScoringCalculator
from app.stats.scoring_config import get_stat_config
from app.utils.hand_fingerprint import fingerprint_hand

logger = logging.getLogger(__name__)

class MultiSiteAggregator:
    """Aggregates statistics and hands from multiple poker sites"""
    
    # Postflop stat names for tracking postflop hand counts
    POSTFLOP_STATS = (
        'Flop Cbet', 'Flop Cbet vs 1', 'Flop Cbet vs 2+', 'Flop fold vs Cbet',
        'Flop Raise vs Cbet', 'Turn Cbet', 'River Cbet', 'Turn Barrel',
        'River Barrel', 'WTSD', 'W$SD', 'W$WSF Rating', 'River Agg %'
    )
    
    def __init__(self):
        self.site_data = {}  # Store data by site
        self.combined_stats = {}  # Final aggregated stats
        self.group_hand_ids = {}  # Track deduplicated hand IDs by group
        self.group_postflop_ids = {}  # Track deduplicated postflop hand IDs by group
    
    def _record_hand_ids(self, group: str, stat_name: str, hands_list: List[str]):
        """
        Record hand IDs for deduplication tracking.
        
        Args:
            group: Group identifier
            stat_name: Stat name
            hands_list: List of hand texts
        """
        if group not in self.group_hand_ids:
            self.group_hand_ids[group] = set()
        if group not in self.group_postflop_ids:
            self.group_postflop_ids[group] = set()
        
        # Track all hand IDs for this group
        for hand_text in hands_list:
            if hand_text:  # Skip empty strings
                hand_id = fingerprint_hand(hand_text)
                self.group_hand_ids[group].add(hand_id)
                
                # If this is a postflop stat, also track in postflop IDs
                if stat_name in self.POSTFLOP_STATS:
                    self.group_postflop_ids[group].add(hand_id)
        
    def add_site_results(self, site: str, group: str, stats: Dict, hands_by_stat: Dict[str, List[str]]):
        """
        Add results from a single site for a specific group
        
        Args:
            site: Site identifier (pokerstars, ggpoker, winamax, 888poker)
            group: Group identifier (nonko_9max, nonko_6max, pko, mystery)
            stats: Statistics dictionary with opportunities and attempts
            hands_by_stat: Dictionary mapping stat names to list of hand texts
        """
        if site not in self.site_data:
            self.site_data[site] = {}
        
        if group not in self.site_data[site]:
            self.site_data[site][group] = {
                'stats': {},
                'hands': {}
            }
        
        self.site_data[site][group]['stats'] = stats
        self.site_data[site][group]['hands'] = hands_by_stat
        
        # Log detailed hands count per stat for debugging
        total_hands = sum(len(hands) for hands in hands_by_stat.values())
        logger.info(f"[AGGREGATOR] Added {site} results for {group}: {len(stats)} stats, {total_hands} hands total")
        
        # DEBUG: Check if hands contain actual text
        for stat_name, hands_list in hands_by_stat.items():
            if stat_name == "Early RFI" and hands_list:
                sample = hands_list[0][:80] if hands_list[0] else "EMPTY STRING"
                logger.info(f"[AGGREGATOR DEBUG] {site}/{group} Early RFI sample: {sample}")
                break
        
        # Log top 3 stats with most hands for verification
        sorted_stats = sorted(hands_by_stat.items(), key=lambda x: len(x[1]), reverse=True)[:3]
        for stat_name, hands_list in sorted_stats:
            logger.info(f"[AGGREGATOR] {site}/{group} - {stat_name}: {len(hands_list)} hands")
    
    def aggregate_stats(self, group: str) -> Dict[str, Any]:
        """
        Aggregate statistics across all sites for a specific group
        
        Returns:
            Combined statistics with percentages, scores, and overall_score
        """
        combined = {}
        
        # Iterate through all sites and combine stats
        for site, site_groups in self.site_data.items():
            if group in site_groups:
                site_stats = site_groups[group]['stats']
                
                for stat_name, stat_data in site_stats.items():
                    if stat_name not in combined:
                        combined[stat_name] = {
                            'opportunities': 0,
                            'attempts': 0,
                            'sites': []
                        }
                        # Add player_sum for W$WSF Rating
                        if stat_name == 'W$WSF Rating':
                            combined[stat_name]['player_sum'] = 0
                    
                    # Sum opportunities and attempts
                    combined[stat_name]['opportunities'] += stat_data.get('opportunities', 0)
                    combined[stat_name]['attempts'] += stat_data.get('attempts', 0)
                    
                    # Sum player_sum for W$WSF Rating
                    if stat_name == 'W$WSF Rating':
                        combined[stat_name]['player_sum'] += stat_data.get('player_sum', 0)
                    
                    combined[stat_name]['sites'].append(site)
        
        # Calculate percentages/ratios for combined stats
        for stat_name, stat_data in combined.items():
            if stat_data['opportunities'] > 0:
                # Special handling for W$WSF Rating (performance ratio)
                if stat_name == 'W$WSF Rating':
                    # Calculate average players per flop
                    avg_players = stat_data.get('player_sum', 0) / stat_data['opportunities']
                    # Calculate win rate
                    win_rate = stat_data['attempts'] / stat_data['opportunities']
                    # Calculate expected win rate (1/avg_players)
                    expected_win_rate = 1.0 / avg_players if avg_players > 0 else 0.0
                    # Rating = actual / expected
                    rating = win_rate / expected_win_rate if expected_win_rate > 0 else 0.0
                    stat_data['percentage'] = round(rating, 2)
                    logger.info(f"[{group}] {stat_name}: wins={stat_data['attempts']}, flops={stat_data['opportunities']}, avg_players={avg_players:.2f}, win_rate={win_rate:.2%}, expected={expected_win_rate:.2%}, rating={rating:.2f} from {len(stat_data['sites'])} sites")
                # Special handling for River Agg % (calls/bets ratio)
                elif stat_name == 'River Agg %':
                    # These are ratios, not percentages - don't multiply by 100
                    ratio = stat_data['attempts'] / stat_data['opportunities']
                    stat_data['percentage'] = round(ratio, 2)
                    logger.info(f"[{group}] {stat_name}: {stat_data['attempts']}/{stat_data['opportunities']} = {stat_data['percentage']} (ratio) from {len(stat_data['sites'])} sites")
                else:
                    # Regular percentage calculation
                    percentage = (stat_data['attempts'] / stat_data['opportunities']) * 100
                    stat_data['percentage'] = round(percentage, 2)
                    logger.info(f"[{group}] {stat_name}: {stat_data['attempts']}/{stat_data['opportunities']} = {stat_data['percentage']}% from {len(stat_data['sites'])} sites")
            else:
                # When opportunities = 0
                if stat_name == 'River Agg %' and stat_data['attempts'] > 0:
                    # River Agg %: If Hero has calls but no bets/raises, ratio is very high (passive play)
                    # Use 10.0 to indicate extreme passivity without breaking systems
                    stat_data['percentage'] = 10.0
                    logger.info(f"[{group}] {stat_name}: {stat_data['attempts']}/0 = 10.0 (only calls, max passivity) from {len(stat_data['sites'])} sites")
                else:
                    stat_data['percentage'] = 0.0
        
        # Calculate scores using ScoringCalculator
        calculator = ScoringCalculator()
        total_weighted_score = 0
        total_weight = 0
        
        for stat_name, stat_data in combined.items():
            # Get config for this stat in this group
            config = get_stat_config(group, stat_name)
            
            if config and stat_data.get('opportunities', 0) > 0:
                # Get the percentage value (already calculated above)
                percentage = stat_data.get('percentage', 0)
                
                # For W$WSF Rating and River Agg %, percentage is already the ratio
                # For other stats, percentage is already in % form (0-100)
                # The scoring config expects the same format as the percentage
                
                # Calculate score
                score = calculator.calculate_single_score(percentage, config)
                stat_data['score'] = round(score, 1)
                stat_data['ideal'] = config.get('ideal')
                
                # Add to weighted total for overall_score
                weight = config.get('weight', 1.0)
                total_weighted_score += score * weight
                total_weight += weight
                
                logger.info(f"[{group}] {stat_name}: score={score:.1f}, ideal={config.get('ideal')}, weight={weight}")
            else:
                # No config or no opportunities - set score to None
                stat_data['score'] = None
                stat_data['ideal'] = None
        
        # Calculate overall_score (weighted average of all stat scores)
        overall_score = 0
        if total_weight > 0:
            overall_score = total_weighted_score / total_weight
            logger.info(f"[{group}] Overall score: {overall_score:.1f} (from {total_weight} total weight)")
        
        # Add overall_score to the result (will be included in JSON output)
        result = {
            'overall_score': round(overall_score, 1),
            'stats': combined
        }
        
        return result
    
    def merge_hands_by_stat(self, group: str) -> Dict[str, List[str]]:
        """
        Merge hands from all sites for each stat, maintaining original formatting
        
        Returns:
            Dictionary mapping stat names to combined list of hands
        """
        merged_hands = {}
        
        # CRITICAL DEBUG: Log ALL sites in site_data
        logger.info(f"[MERGE CRITICAL] site_data keys: {list(self.site_data.keys())}")
        for site in self.site_data:
            logger.info(f"[MERGE CRITICAL] {site} has groups: {list(self.site_data[site].keys())}")
        
        # Log which sites are being merged for this group
        sites_in_group = [site for site in self.site_data if group in self.site_data[site]]
        logger.info(f"[MERGE] Starting merge for {group} from {len(sites_in_group)} sites: {sites_in_group}")
        
        # Iterate through all sites and combine hands for each stat
        for site, site_groups in self.site_data.items():
            if group in site_groups:
                site_hands = site_groups[group]['hands']
                logger.info(f"[MERGE] {site}/{group} has {len(site_hands)} stats with hands")
                
                for stat_name, hands_list in site_hands.items():
                    if stat_name not in merged_hands:
                        merged_hands[stat_name] = []
                    
                    # DEBUG: Check if hands_list contains actual text or just empty references
                    if stat_name == "Early RFI" and hands_list:
                        sample = hands_list[0][:100] if hands_list[0] else "EMPTY"
                        logger.info(f"[MERGE DEBUG] {site}/{group} Early RFI sample: {sample}")
                    
                    # Record hand IDs for deduplication tracking
                    self._record_hand_ids(group, stat_name, hands_list)
                    
                    # Add all hands from this site for this stat
                    before_count = len(merged_hands[stat_name])
                    merged_hands[stat_name].extend(hands_list)
                    after_count = len(merged_hands[stat_name])
                    
                    logger.info(f"[MERGE] {site}/{group} - {stat_name}: Added {len(hands_list)} hands (total: {before_count} → {after_count})")
        
        # Log total hands per stat
        logger.info(f"[MERGE COMPLETE] {group}: {len(merged_hands)} stats with hands")
        for stat_name, hands_list in merged_hands.items():
            logger.info(f"[MERGE COMPLETE] {group} - {stat_name}: {len(hands_list)} hands from all sites")
        
        # Log deduplicated counts
        dedup_count = len(self.group_hand_ids.get(group, set()))
        postflop_dedup_count = len(self.group_postflop_ids.get(group, set()))
        logger.info(f"[DEDUP] {group}: {dedup_count} unique hands, {postflop_dedup_count} unique postflop hands")
        
        return merged_hands
    
    def write_combined_outputs(self, output_dir: str, group: str):
        """
        Write combined statistics and hands to output directory
        
        Args:
            output_dir: Base output directory (e.g., work/{token})
            group: Group identifier
        """
        # Create directories
        combined_dir = os.path.join(output_dir, "combined")
        stats_dir = os.path.join(combined_dir, "stats")
        # Write hands to the regular location so downloads work
        hands_dir = os.path.join(output_dir, "hands_by_stat", group)
        
        os.makedirs(stats_dir, exist_ok=True)
        os.makedirs(hands_dir, exist_ok=True)
        
        # Aggregate stats for this group (returns {'overall_score': X, 'stats': {...}})
        aggregated_result = self.aggregate_stats(group)
        overall_score = aggregated_result['overall_score']
        aggregated_stats = aggregated_result['stats']
        
        # Write aggregated stats to JSON (including overall_score)
        stats_file = os.path.join(stats_dir, f"{group}_stats.json")
        output_data = {
            'overall_score': overall_score,
            'stats': aggregated_stats
        }
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Wrote combined stats with overall_score={overall_score} to {stats_file}")
        
        # Merge and write hands for each stat
        merged_hands = self.merge_hands_by_stat(group)
        
        # Import HandCollector for canonical filename mapping
        from app.stats.hand_collector import HandCollector
        
        for stat_name, hands_list in merged_hands.items():
            if hands_list:  # Only write if there are hands
                # Use HandCollector's canonical filename mapping
                filename = None
                for canonical_name, stat_filename in HandCollector.stat_filenames.items():
                    if canonical_name == stat_name:
                        filename = stat_filename
                        break
                
                # Fallback to sanitized name if not in mapping
                if not filename:
                    safe_filename = stat_name.replace("/", "_").replace(" ", "_") + ".txt"
                    filename = safe_filename
                    
                hands_file = os.path.join(hands_dir, filename)
                
                # Write hands separated by double newlines
                with open(hands_file, 'w', encoding='utf-8') as f:
                    # Join hands with exactly 2 newlines between them
                    content = "\n\n".join(hand.strip() for hand in hands_list if hand.strip())
                    f.write(content)
                
                logger.info(f"Wrote {len(hands_list)} combined hands to {filename}")
        
        # Create summary file
        summary_file = os.path.join(combined_dir, f"{group}_summary.json")
        summary = {
            'group': group,
            'overall_score': overall_score,
            'sites_processed': list(set(site for site in self.site_data if group in self.site_data[site])),
            'total_stats': len(aggregated_stats),
            'total_hands': sum(len(hands) for hands in merged_hands.values()),
            'stats': {
                name: {
                    'opportunities': data['opportunities'],
                    'attempts': data['attempts'],
                    'percentage': data['percentage'],
                    'hands_count': len(merged_hands.get(name, []))
                }
                for name, data in aggregated_stats.items()
            }
        }
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Created summary for {group} with overall_score={overall_score} and {len(summary['sites_processed'])} sites")
        
        # Add deduplicated hand counts to result
        aggregated_result['hand_count'] = len(self.group_hand_ids.get(group, set()))
        aggregated_result['postflop_hands_count'] = len(self.group_postflop_ids.get(group, set()))
        
        logger.info(f"[DEDUP COUNTS] {group}: hand_count={aggregated_result['hand_count']}, postflop_hands_count={aggregated_result['postflop_hands_count']}")
        
        return aggregated_result
    
    def write_cross_format_postflop_outputs(self, output_dir: str):
        """
        Write cross-format combined postflop hands (all groups merged)
        This creates unified postflop files without format subdivision
        
        Args:
            output_dir: Base output directory (e.g., work/{token})
        """
        # Collect ALL postflop hands across ALL groups
        cross_format_hands = {}
        cross_format_stats = {}
        
        # Get all groups
        all_groups = set()
        for site_groups in self.site_data.values():
            all_groups.update(site_groups.keys())
        
        # Identify postflop stats (those starting with specific keywords)
        postflop_keywords = ['Flop', 'Turn', 'River', 'WTSD', 'W$SD', 'W$WSF']
        
        # Iterate through all groups and collect postflop hands
        for group in all_groups:
            for site, site_groups in self.site_data.items():
                if group in site_groups:
                    site_stats = site_groups[group]['stats']
                    site_hands = site_groups[group]['hands']
                    
                    for stat_name, stat_data in site_stats.items():
                        # Check if this is a postflop stat
                        if any(keyword in stat_name for keyword in postflop_keywords):
                            # Aggregate stats
                            if stat_name not in cross_format_stats:
                                cross_format_stats[stat_name] = {
                                    'opportunities': 0,
                                    'attempts': 0
                                }
                            
                            cross_format_stats[stat_name]['opportunities'] += stat_data.get('opportunities', 0)
                            cross_format_stats[stat_name]['attempts'] += stat_data.get('attempts', 0)
                            
                            # Collect hands
                            if stat_name in site_hands:
                                if stat_name not in cross_format_hands:
                                    cross_format_hands[stat_name] = []
                                cross_format_hands[stat_name].extend(site_hands[stat_name])
        
        # Write cross-format postflop hands
        hands_dir = os.path.join(output_dir, "hands_by_stat")
        os.makedirs(hands_dir, exist_ok=True)
        
        # Import HandCollector for canonical filename mapping
        from app.stats.hand_collector import HandCollector
        
        for stat_name, hands_list in cross_format_hands.items():
            if hands_list:  # Only write if there are hands
                # Use HandCollector's canonical filename mapping
                filename = None
                for canonical_name, stat_filename in HandCollector.stat_filenames.items():
                    if canonical_name == stat_name:
                        filename = stat_filename
                        break
                
                # Fallback to sanitized name if not in mapping
                if not filename:
                    safe_filename = stat_name.replace("/", "_").replace(" ", "_") + ".txt"
                    filename = safe_filename
                
                hands_file = os.path.join(hands_dir, filename)
                
                # Write hands separated by double newlines
                with open(hands_file, 'w', encoding='utf-8') as f:
                    # Join hands with exactly 2 newlines between them
                    content = "\n\n".join(hand.strip() for hand in hands_list if hand.strip())
                    f.write(content)
                
                logger.info(f"Wrote {len(hands_list)} cross-format postflop hands to {filename}")
        
        logger.info(f"Created cross-format postflop outputs with {len(cross_format_hands)} stats")
        
        return cross_format_stats

    def get_combined_manifest(self) -> Dict[str, Any]:
        """
        Generate a manifest of all combined data
        
        Returns:
            Manifest with metadata about the aggregation
        """
        manifest = {
            'sites': {},
            'groups': {},
            'totals': {
                'sites': 0,
                'groups': 0,
                'stats': 0,
                'hands': 0
            }
        }
        
        # Count by site
        for site in self.site_data:
            manifest['sites'][site] = {
                'groups': list(self.site_data[site].keys()),
                'total_stats': sum(
                    len(group_data['stats']) 
                    for group_data in self.site_data[site].values()
                ),
                'total_hands': sum(
                    sum(len(hands) for hands in group_data['hands'].values())
                    for group_data in self.site_data[site].values()
                )
            }
        
        # Count by group
        all_groups = set()
        for site_groups in self.site_data.values():
            all_groups.update(site_groups.keys())
        
        for group in all_groups:
            manifest['groups'][group] = {
                'sites': [site for site in self.site_data if group in self.site_data[site]],
                'total_stats': 0,
                'total_hands': 0
            }
            
            # Count stats and hands for this group across all sites
            for site in manifest['groups'][group]['sites']:
                if group in self.site_data[site]:
                    manifest['groups'][group]['total_stats'] += len(self.site_data[site][group]['stats'])
                    manifest['groups'][group]['total_hands'] += sum(
                        len(hands) for hands in self.site_data[site][group]['hands'].values()
                    )
        
        # Calculate totals
        manifest['totals']['sites'] = len(manifest['sites'])
        manifest['totals']['groups'] = len(manifest['groups'])
        manifest['totals']['stats'] = sum(site_data['total_stats'] for site_data in manifest['sites'].values())
        manifest['totals']['hands'] = sum(site_data['total_hands'] for site_data in manifest['sites'].values())
        
        return manifest