"""
Monthly File Bucketing
======================

Groups extracted hand history files by month before processing.
Each month is processed independently as if it were a separate upload.
"""
import os
import shutil
import logging
from pathlib import Path
from typing import Dict, List
from app.services.date_extractor import DateExtractor

logger = logging.getLogger(__name__)


class MonthBucket:
    """Represents a bucket of files for a specific month."""
    
    def __init__(self, month: str, work_dir: str):
        """
        Initialize a monthly bucket.
        
        Args:
            month: Month in YYYY-MM format (or 'unknown')
            work_dir: Base work directory for this bucket
        """
        self.month = month
        self.work_dir = work_dir
        self.input_dir = os.path.join(work_dir, "in")
        self.files: List[str] = []
        
        # Create directory structure
        os.makedirs(self.input_dir, exist_ok=True)
    
    def add_file(self, source_path: str):
        """Copy a file into this bucket's input directory."""
        filename = os.path.basename(source_path)
        dest_path = os.path.join(self.input_dir, filename)
        shutil.copy2(source_path, dest_path)
        self.files.append(dest_path)
        
    def get_metadata(self) -> dict:
        """Get metadata about this bucket."""
        return {
            'month': self.month,
            'file_count': len(self.files),
            'work_dir': self.work_dir,
            'input_dir': self.input_dir
        }


def build_month_buckets(token: str, input_dir: str, work_root: str) -> List[MonthBucket]:
    """
    Group files by month and create isolated work directories.
    
    Args:
        token: Processing token
        input_dir: Directory containing extracted .txt files
        work_root: Base work directory (e.g., /tmp/work)
        
    Returns:
        List of MonthBucket objects, sorted by month (unknown last)
    """
    logger.info(f"[{token}] Building monthly buckets from {input_dir}")
    
    # Get all .txt files
    txt_files = []
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.lower().endswith('.txt'):
                txt_files.append(os.path.join(root, file))
    
    if not txt_files:
        logger.warning(f"[{token}] No .txt files found in {input_dir}")
        return []
    
    # Extract months using DateExtractor
    extractor = DateExtractor()
    month_mapping = {}
    
    for file_path in txt_files:
        month = extractor.extract_month_from_file(file_path)
        if not month:
            month = 'unknown'
        
        if month not in month_mapping:
            month_mapping[month] = []
        month_mapping[month].append(file_path)
    
    # Create MonthBucket objects
    buckets = []
    for month, files in month_mapping.items():
        # Create work directory for this month
        if month == 'unknown':
            month_work_dir = os.path.join(work_root, token, "months", "unknown")
        else:
            month_work_dir = os.path.join(work_root, token, "months", month)
        
        bucket = MonthBucket(month, month_work_dir)
        
        # Copy files into bucket
        for file_path in files:
            bucket.add_file(file_path)
        
        buckets.append(bucket)
        logger.info(f"[{token}] Created bucket for {month}: {len(files)} file(s)")
    
    # Sort buckets: chronological order, unknown last
    def sort_key(bucket):
        if bucket.month == 'unknown':
            return ('9999-99', bucket.month)  # Sort unknown last
        return (bucket.month, '')
    
    buckets.sort(key=sort_key)
    
    logger.info(f"[{token}] Created {len(buckets)} monthly bucket(s)")
    return buckets


def generate_months_manifest(token: str, buckets: List[MonthBucket], bucket_results: Dict[str, dict]) -> dict:
    """
    Generate months_manifest.json with metadata about all processed months.
    
    Args:
        token: Processing token
        buckets: List of MonthBucket objects
        bucket_results: Dict mapping month -> pipeline_result dict
        
    Returns:
        months_manifest dict
    """
    from datetime import datetime, timezone
    
    months_data = []
    
    for bucket in buckets:
        month = bucket.month
        result = bucket_results.get(month, {})
        
        # Skip failed months
        if result.get('status') == 'failed':
            logger.warning(f"Skipping failed month {month} in manifest")
            continue
        
        # Extract counts from result
        hand_count = result.get('valid_hands', 0)
        total_hands = result.get('total_hands', 0)
        
        # Get site breakdown from 'sites' key (not 'site_results')
        sites = {}
        if 'sites' in result:
            for site, site_data in result['sites'].items():
                # Aggregate hand counts across all groups for this site
                site_total_hands = 0
                site_valid_hands = 0
                
                for group_key, group_info in site_data.items():
                    if isinstance(group_info, dict):
                        site_total_hands += group_info.get('hand_count', 0)
                        site_valid_hands += group_info.get('hand_count', 0)
                
                sites[site] = {
                    'valid_hands': site_valid_hands,
                    'total_hands': site_total_hands
                }
        
        months_data.append({
            'month': month,
            'file_count': bucket.get_metadata()['file_count'],
            'hand_count': hand_count,
            'total_hands': total_hands,
            'sites': sites,
            'storage_path': f"results/{token}/months/{month}/pipeline_result.json",
            'status': result.get('status', 'completed')
        })
    
    manifest = {
        'token': token,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'total_months': len(months_data),  # Only count successfully processed months
        'months': months_data
    }
    
    return manifest
