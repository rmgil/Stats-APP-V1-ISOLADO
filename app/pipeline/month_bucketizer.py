"""
Monthly File Bucketing
======================

Groups extracted hand history files by month before processing.
Each month is processed independently as if it were a separate upload.
"""
import os
import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

from app.parse.hand_splitter import split_into_hands
from app.services.date_extractor import DateExtractor
from app.services.hand_metadata import HandMetadata, extract_hand_metadata

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
        self._buffers: Dict[str, List[str]] = defaultdict(list)
        self._hand_counters: Dict[str, int] = defaultdict(int)

        # Create directory structure
        os.makedirs(self.input_dir, exist_ok=True)

    def add_hand(self, source_path: str, hand_text: str, metadata: Optional[HandMetadata] = None):
        """Append a hand to this bucket."""

        filename = os.path.basename(source_path)
        buffer_key = filename

        self._buffers[buffer_key].append(hand_text.strip())
        self._hand_counters[buffer_key] += 1

        if metadata and metadata.tournament_id:
            logger.debug(
                "Adding hand to month %s (%s) from tournament %s", self.month, filename, metadata.tournament_id
            )

    def finalize(self):
        """Write buffered hands to disk."""

        for filename, hands in self._buffers.items():
            if not hands:
                continue

            dest_path = os.path.join(self.input_dir, filename)
            dest_path = self._ensure_unique_filename(dest_path)

            with open(dest_path, "w", encoding="utf-8") as f:
                f.write("\n\n".join(hands))

            self.files.append(dest_path)

        self._buffers.clear()

    def _ensure_unique_filename(self, dest_path: str) -> str:
        """Ensure destination filename is unique within the bucket."""

        base_path = Path(dest_path)
        candidate = base_path
        counter = 1

        while candidate.exists():
            candidate = base_path.with_stem(f"{base_path.stem}_{counter}")
            counter += 1

        return str(candidate)

    def get_metadata(self) -> dict:
        """Get metadata about this bucket."""
        return {
            'month': self.month,
            'file_count': len(self.files),
            'work_dir': self.work_dir,
            'input_dir': self.input_dir,
            'hands_per_file': dict(self._hand_counters)
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
    txt_files: List[str] = []
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.lower().endswith('.txt'):
                txt_files.append(os.path.join(root, file))

    if not txt_files:
        logger.warning(f"[{token}] No .txt files found in {input_dir}")
        return []

    extractor = DateExtractor()
    buckets: Dict[str, MonthBucket] = {}
    seen_keys: set = set()

    for file_path in txt_files:
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        except Exception as exc:
            logger.warning(f"[{token}] Could not read {file_path}: {exc}")
            continue

        hands = split_into_hands(content)
        if not hands:
            logger.debug(f"[{token}] No valid hands found in {file_path}")
            continue

        logger.info(f"[{token}] Processing {len(hands)} hands from {Path(file_path).name}")

        for hand_text in hands:
            metadata = extract_hand_metadata(hand_text, file_path)
            month = metadata.month or extractor.extract_month_from_text(hand_text) or 'unknown'

            key = metadata.dedup_key(hand_text)
            if key in seen_keys:
                logger.debug(f"[{token}] Skipping duplicate hand {key}")
                continue

            seen_keys.add(key)

            if month not in buckets:
                if month == 'unknown':
                    month_work_dir = os.path.join(work_root, token, "months", "unknown")
                else:
                    month_work_dir = os.path.join(work_root, token, "months", month)

                buckets[month] = MonthBucket(month, month_work_dir)

            buckets[month].add_hand(file_path, hand_text, metadata)

    # Finalize bucket files
    for bucket in buckets.values():
        bucket.finalize()
        logger.info(
            f"[{token}] Created bucket for {bucket.month}: {len(bucket.files)} file(s), {sum(bucket._hand_counters.values())} hands"
        )

    # Sort buckets: chronological order, unknown last
    def sort_key(bucket: MonthBucket):
        if bucket.month == 'unknown':
            return ('9999-99', bucket.month)
        return (bucket.month, '')

    sorted_buckets = sorted(buckets.values(), key=sort_key)

    logger.info(f"[{token}] Created {len(sorted_buckets)} monthly bucket(s)")
    return sorted_buckets


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
