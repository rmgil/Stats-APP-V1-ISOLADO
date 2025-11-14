"""
Final group classification for the new workflow
Groups: 9-max nonKO, 6-max nonKO, PKO
Now with hand-by-hand analysis for accurate table format detection
"""
import os
import json
import shutil
from pathlib import Path
from typing import Dict, List, Any, Optional
from app.classify.hand_by_hand_classifier import process_files_hand_by_hand

def classify_into_final_groups(input_dir: str, output_dir: str, token: Optional[str] = None) -> Dict[str, Any]:
    """
    Classify hands into final 3 groups using hand-by-hand analysis:
    - 9-max nonKO
    - 6-max nonKO  
    - PKO (includes both 9-max and 6-max PKO)
    
    Mystery hands are excluded from all groups.
    
    Args:
        input_dir: Directory with .txt files to classify
        output_dir: Directory to write classified files
        token: Optional job token for progress updates
    """
    # Use the new hand-by-hand processor
    stats = process_files_hand_by_hand(input_dir, output_dir, token=token)
    
    # Transform stats to match expected format
    result = {
        'total_files': stats['total_files'],
        'total_hands': stats['total_hands'],
        'mystery_files': stats.get('mystery_hands', 0),
        'discarded_hands': stats.get('discarded_hands', {}),  # Pass through discard stats
        'groups': {
            'nonko_9max': stats['groups']['nonko_9max']['file_count'],
            'nonko_6max': stats['groups']['nonko_6max']['file_count'],
            'pko': stats['groups']['pko']['file_count']
        },
        'hands_per_group': {
            'nonko_9max': stats['groups']['nonko_9max']['hand_count'],
            'nonko_6max': stats['groups']['nonko_6max']['hand_count'],
            'pko': stats['groups']['pko']['hand_count']
        },
        'files': stats.get('file_details', []),
        'group_labels': stats.get('group_labels', {}),
        'valid_hand_records': stats.get('valid_hand_records', [])
    }
    
    return result

def create_group_manifest(stats: Dict[str, Any], output_path: str):
    """
    Create a manifest file with group classification results
    """
    manifest = {
        'summary': {
            'total_files': stats['total_files'],
            'total_hands': stats.get('total_hands', 0),
            'mystery_files_excluded': stats['mystery_files'],
            'processed_files': stats['total_files'] - stats['mystery_files'],
            'discarded_hands': stats.get('discarded_hands', {})
        },
        'groups': stats['groups'],
        'group_labels': stats['group_labels'],
        'files': stats['files']
    }
    
    with open(output_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    return manifest