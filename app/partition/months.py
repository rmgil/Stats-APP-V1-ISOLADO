"""
Monthly partitioning for poker hands with Europe/Lisbon timezone.
Splits hands by year-month based on hand timestamps.
"""
import json
import os
import hashlib
from collections import defaultdict
from datetime import datetime
from dateutil import parser, tz
from typing import Dict, List, Any

# Timezone for Portugal
TZ_PT = tz.gettz("Europe/Lisbon")


def month_bucket(timestamp_utc: str) -> str:
    """
    Converte timestamp UTC (ISO) para bucket mensal 'YYYY-MM' em Europe/Lisbon.
    Se faltar timestamp, retorna 'unknown'.
    
    Args:
        timestamp_utc: ISO format timestamp string in UTC
        
    Returns:
        Month bucket in YYYY-MM format (Europe/Lisbon timezone)
    """
    if not timestamp_utc:
        return "unknown"
    
    try:
        # Parse ISO format timestamp
        dt = parser.isoparse(timestamp_utc)
        # Convert to Portugal timezone
        dt_pt = dt.astimezone(TZ_PT)
        return f"{dt_pt.year:04d}-{dt_pt.month:02d}"
    except Exception:
        return "unknown"


def make_hand_id(hand_obj: dict) -> str:
    """
    ID estável por mão, com alta unicidade:
    - site, tournament_id, file_id, button_seat, raw_offsets.hand_start, timestamp_utc
    - + hash dos nomes dos jogadores (para distinguir mãos com mesmos campos base)
    
    Args:
        hand_obj: Hand dictionary
        
    Returns:
        16-character hex hash ID
    """
    players = hand_obj.get("players", []) or []
    players_key = hash(tuple(sorted(p.get("name", "") for p in players)))
    
    parts = [
        str(hand_obj.get("site", "")),
        str(hand_obj.get("tournament_id", "")),
        str(hand_obj.get("file_id", "")),
        str(hand_obj.get("button_seat", "")),
        str(hand_obj.get("raw_offsets", {}).get("hand_start", "")),
        str(hand_obj.get("timestamp_utc", "")),
        str(players_key)
    ]
    
    s = "|".join(parts)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]


def parse_hand_datetime(hand: dict) -> str:
    """
    Extract datetime from hand object.
    
    Args:
        hand: Hand dictionary with datetime field
        
    Returns:
        Timestamp string or empty if not found
    """
    # Try different date fields that might exist
    return hand.get('timestamp_utc') or hand.get('datetime') or hand.get('timestamp') or hand.get('date') or ""


def partition_by_month(hands_jsonl: str, output_dir: str) -> Dict[str, str]:
    """
    Partition hands by year-month using Europe/Lisbon timezone.
    
    Args:
        hands_jsonl: Path to input JSONL file with hands
        output_dir: Directory to write partitioned files
        
    Returns:
        Dict mapping month keys to output file paths
    """
    # Group hands by month
    months_data = defaultdict(list)
    
    with open(hands_jsonl, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                hand = json.loads(line.strip())
                
                # Add hand_id if not present
                if 'hand_id' not in hand:
                    hand['hand_id'] = make_hand_id(hand)
                
                # Get month bucket
                timestamp = parse_hand_datetime(hand)
                month_key = month_bucket(timestamp)
                
                months_data[month_key].append(hand)
                
            except Exception as e:
                print(f"Warning: Could not process line {line_num}: {e}")
                # Put unparseable hands in "unknown" partition
                try:
                    hand = json.loads(line.strip())
                    hand['hand_id'] = make_hand_id(hand)
                    months_data['unknown'].append(hand)
                except:
                    pass
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Write partitioned files
    output_files = {}
    for month_key, hands in sorted(months_data.items()):
        output_file = os.path.join(output_dir, f"{month_key}.jsonl")
        with open(output_file, 'w', encoding='utf-8') as f:
            for hand in hands:
                f.write(json.dumps(hand, ensure_ascii=False) + '\n')
        output_files[month_key] = output_file
        print(f"  {month_key}: {len(hands)} hands → {output_file}")
    
    return output_files


def generate_month_summary(output_files: Dict[str, str]) -> dict:
    """
    Generate summary statistics for monthly partitions.
    
    Args:
        output_files: Dict mapping month keys to file paths
        
    Returns:
        Summary dict with statistics
    """
    summary = {
        'total_months': len(output_files),
        'months': {},
        'total_hands': 0,
        'timezone': 'Europe/Lisbon'
    }
    
    for month_key, filepath in sorted(output_files.items()):
        hand_count = sum(1 for _ in open(filepath, 'r'))
        summary['months'][month_key] = {
            'file': filepath,
            'hands': hand_count
        }
        summary['total_hands'] += hand_count
    
    return summary