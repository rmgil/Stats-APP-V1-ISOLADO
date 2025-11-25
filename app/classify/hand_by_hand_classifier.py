"""
Hand-by-hand classification module
Processes each hand individually for accurate 6-max vs 9-max detection
"""
import logging
import os
import json
import re
from pathlib import Path
from datetime import timezone
from typing import Dict, List, Tuple, Optional
from app.parse.hand_splitter import split_into_hands, split_into_hands_with_stats, classify_hand_format, is_tournament_summary, is_cash_game
from app.classify.run import classify_tournament
from app.utils.hand_fingerprint import fingerprint_hand
from app.partition.months import (
    DEFAULT_FALLBACK_MONTH,
    month_key_from_datetime,
    normalize_month_key,
    parse_hand_datetime,
    parse_timestamp,
)

# REMOVED: _has_allin_preflop() and _has_low_stack_villain() functions
# These were filtering hands at classification stage, preventing stat-level validation
# Stack and all-in validations are now handled per-stat in PreflopStats

logger = logging.getLogger(__name__)


def _extract_timestamp_and_month(hand_text: str) -> Tuple[str, str]:
    """
    Extract the hand timestamp in UTC and its month key using the shared parsing
    helpers. This mirrors the date parsing logic used across the pipeline to
    keep classifications aligned.

    This wrapper guarantees a tuple is always returned; on failure it logs a
    warning and provides stable fallback values.
    """

    lines = [line for line in hand_text.split('\n') if line.strip()]
    dt = None

    for line in lines:
        dt = parse_hand_datetime(line)
        if dt:
            break
        parsed = parse_timestamp(line)
        if parsed:
            dt = parsed
            break

    if not dt:
        fallback_timestamp = "1970-01-01T00:00:00Z"
        logger.warning(
            "Failed to extract timestamp/month from hand_text; using fallback month. First 80 chars: %r",
            hand_text[:80],
        )
        return fallback_timestamp, DEFAULT_FALLBACK_MONTH

    dt_utc = dt.astimezone(timezone.utc)
    timestamp = dt_utc.isoformat().replace("+00:00", "Z")
    return timestamp, month_key_from_datetime(dt_utc)


def classify_hands_individually(content: str, filename: str) -> Tuple[List[Dict], Dict]:
    """
    Classify each hand individually in a file
    Returns a tuple of (classified hands list, discard statistics)
    """
    # Use the new function to get hands and initial discard stats
    hands, splitter_discards = split_into_hands_with_stats(content)
    classified_hands = []

    # Detailed tracking of discarded hands (including splitter stats)
    # NOTE: Stack and all-in filtering REMOVED - now handled per-stat in PreflopStats
    discard_stats = {
        'mystery': 0,
        'less_than_4_players': 0,
        'tournament_summary': splitter_discards.get('tournament_summary', 0),
        'cash_game': 0,
        'invalid_format': splitter_discards.get('invalid_segments', 0),
        'other': 0,
        'total_segments': splitter_discards.get('total_segments', 0)
    }
    
    # Check if filename contains 'mystery' (case insensitive)
    # If yes, ALL hands in this file are Mystery and should be discarded
    is_mystery_file = 'mystery' in filename.lower()
    
    for idx, hand_text in enumerate(hands):
        # Note: Tournament summaries are already filtered by splitter
            
        # Check if it's a cash game
        if is_cash_game(hand_text):
            discard_stats['cash_game'] += 1
            continue
        
        # If filename contains 'mystery', discard all hands from this file
        if is_mystery_file:
            discard_stats['mystery'] += 1
            continue
        
        # Get tournament type for this hand (check filename + content)
        # Include filename for 888poker PKO detection (where PKO is only in filename)
        classification_text = filename + " " + hand_text
        tournament_type = classify_tournament(classification_text)
        
        # Skip mystery hands entirely and count them
        if tournament_type == 'MYSTERIES':
            discard_stats['mystery'] += 1
            continue
        
        # Get table format (6-max or 9-max)
        table_format = classify_hand_format(hand_text)
        
        # Skip hands that should be discarded (less than 4 players)
        if table_format == 'discard':
            discard_stats['less_than_4_players'] += 1
            continue
        
        # REMOVED: Stack and all-in filters
        # These validations are now handled per-stat in PreflopStats, not at hand level
        
        # Determine final group
        if tournament_type == 'PKO':
            group = 'pko'
        elif tournament_type == 'NON-KO':
            if table_format == '6-max':
                group = 'nonko_6max'
            elif table_format == '9-max':
                group = 'nonko_9max'
            else:
                discard_stats['invalid_format'] += 1
                continue  # Skip if can't classify
        else:
            discard_stats['other'] += 1
            continue  # Skip if can't classify
        
        classified_hands.append({
            'hand_index': idx,
            'source_file': filename,
            'tournament_type': tournament_type,
            'table_format': table_format,
            'group': group,
            'hand_text': hand_text
        })
    
    # Log summary of this file's processing
    total_hands_in_file = len(hands)
    classified_count = len(classified_hands)
    total_discarded = sum(v for k, v in discard_stats.items() if k not in ['total_segments'])
    
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"[FILE] {filename}: {total_hands_in_file} hands found, {classified_count} classified, {total_discarded} discarded")
    logger.info(f"[FILE] {filename}: Discards breakdown - Mystery: {discard_stats['mystery']}, <4 players: {discard_stats['less_than_4_players']}, Cash: {discard_stats['cash_game']}, Other: {discard_stats['other']}")
    
    return classified_hands, discard_stats

def process_files_hand_by_hand(input_dir: str, output_dir: str, token: Optional[str] = None) -> Dict:
    """
    Process all files hand-by-hand for accurate classification
    
    Args:
        input_dir: Directory with .txt files to classify
        output_dir: Directory to write classified files
        token: Optional job token for progress updates
    """
    # Create output directories
    groups = {
        'nonko_9max': '9-max NON-KO',
        'nonko_6max': '6-max NON-KO',
        'pko': 'PKO (All)'
    }
    
    for group_key in groups.keys():
        os.makedirs(os.path.join(output_dir, group_key), exist_ok=True)
    
    # Statistics
    # NOTE: Stack and all-in exclusions removed - now handled per-stat in PreflopStats
    stats = {
        'total_files': 0,
        'total_hands': 0,
        'raw_segments': 0,
        'mystery_hands': 0,
        'discarded_hands': {
            'mystery': 0,
            'less_than_4_players': 0,
            'tournament_summary': 0,
            'cash_game': 0,
            'invalid_format': 0,
            'other': 0,
            'total': 0
        },
        'groups': {
            'nonko_9max': {'files': set(), 'hands': []},
            'nonko_6max': {'files': set(), 'hands': []},
            'pko': {'files': set(), 'hands': []}
        },
        'file_details': []
    }
    
    # Group hands by their classification
    group_hands = {
        'nonko_9max': [],
        'nonko_6max': [],
        'pko': []
    }
    
    # Process all text files
    input_path = Path(input_dir)
    
    import logging
    logger = logging.getLogger(__name__)
    
    
    all_files = list(input_path.glob("*.txt"))
    total_file_count = len(all_files)
    logger.info(f"🔍 [CLASSIFICATION] Starting hand-by-hand processing: {total_file_count} files to process")
    
    valid_hand_records: List[Dict] = []

    for file_idx, txt_file in enumerate(all_files, 1):
        file_size_mb = txt_file.stat().st_size / 1024 / 1024
        logger.info(f"📄 [{file_idx}/{total_file_count}] Processing: {txt_file.name} ({file_size_mb:.2f} MB)")
        
        stats['total_files'] += 1
        
        try:
            content = txt_file.read_text(errors='ignore')
            
            # Classify hands directly (no multiprocessing - was causing deadlocks)
            logger.info(f"   ↳ Classifying hands in {txt_file.name}...")
            classified_hands, file_discard_stats = classify_hands_individually(content, txt_file.name)
            logger.info(f"   ✓ Classified {len(classified_hands)} hands from {txt_file.name}")
            
        except Exception as e:
            logger.error(f"   ❌ ERROR processing {txt_file.name}: {e}")
            import traceback
            logger.error(f"       Traceback: {traceback.format_exc()}")
            continue
        
        # Update discard statistics
        stats['mystery_hands'] += file_discard_stats['mystery']
        for discard_type, count in file_discard_stats.items():
            if discard_type != 'total_segments':  # Don't add total_segments to discarded_hands
                stats['discarded_hands'][discard_type] += count
        
        # Get the real total segments for this file
        file_total_segments = file_discard_stats.get('total_segments', len(split_into_hands(content)))

        # Track raw segments for debug/consistency checks
        stats['raw_segments'] += file_total_segments
        
        file_info = {
            'filename': txt_file.name,
            'total_hands': file_total_segments,  # Use total segments (includes discarded)
            'classified_hands': len(classified_hands),
            'group_distribution': {}
        }
        
        # Group the hands
        for hand_data in classified_hands:
            group = hand_data['group']
            hand_text = hand_data['hand_text']
            group_hands[group].append(hand_text)
            stats['groups'][group]['files'].add(txt_file.name)
            stats['groups'][group]['hands'].append(hand_data)

            timestamp_utc, month_key = _extract_timestamp_and_month(hand_text)

            valid_hand_records.append({
                'hand_id': fingerprint_hand(hand_text),
                'source_file': txt_file.name,
                'hand_index': hand_data['hand_index'],
                'group': group,
                'tournament_type': hand_data['tournament_type'],
                'table_format': hand_data['table_format'],
                **({'timestamp_utc': timestamp_utc} if timestamp_utc else {}),
                **({'month': normalize_month_key(month_key) or month_key} if month_key else {}),
            })
            
            # Update file distribution
            if group not in file_info['group_distribution']:
                file_info['group_distribution'][group] = 0
            file_info['group_distribution'][group] += 1
        
        # Don't add to total_hands here - will calculate correctly at the end
        stats['file_details'].append(file_info)
    
    # Calculate the total discarded (excluding total_segments)
    stats['discarded_hands']['total'] = sum(
        v for k, v in stats['discarded_hands'].items() 
        if k not in ['total', 'total_segments']
    )
    
    # Write grouped hands to output files
    for group_key, hands in group_hands.items():
        if hands:
            output_file = os.path.join(output_dir, group_key, f'{group_key}_combined.txt')
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('\n\n'.join(hands))
    
    # Convert sets to lists for JSON serialization
    for group_key in stats['groups']:
        stats['groups'][group_key]['file_count'] = len(stats['groups'][group_key]['files'])
        stats['groups'][group_key]['hand_count'] = len(stats['groups'][group_key]['hands'])
        stats['groups'][group_key]['files'] = list(stats['groups'][group_key]['files'])
        # Don't include full hand text in stats (too large)
        stats['groups'][group_key]['hands'] = [
            {k: v for k, v in h.items() if k != 'hand_text'} 
            for h in stats['groups'][group_key]['hands'][:10]  # Sample first 10
        ]
    
    # Calculate the correct total_hands: ALL hands processed (valid + ALL discarded)
    # The total should be the sum of all hands found in the files
    total_classified = len(valid_hand_records)
    
    # Sum all discarded hands (excluding 'total' which is already a sum)
    total_discarded = sum(
        v for k, v in stats['discarded_hands'].items() 
        if k not in ['total', 'total_segments']
    )
    
    # Total hands = valid classified hands + all discarded hands
    stats['total_hands'] = total_classified + total_discarded

    # Align parsed_hands with the calculated total for downstream debug reporting
    stats['parsed_hands'] = stats['total_hands']

    # Keep a copy of total_segments to compare against downstream totals
    stats['discarded_hands']['total_segments'] = stats['raw_segments']

    # Store normalized valid hand metadata for downstream consistency
    stats['valid_hand_records'] = valid_hand_records
    
    # Add group labels
    stats['group_labels'] = groups
    
    return stats