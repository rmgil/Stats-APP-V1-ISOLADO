"""
Monthly File Bucketing
======================

Groups extracted hand history files by month before processing.
Each month is processed independently as if it were a separate upload.
"""
import os
import re
import shutil
import logging
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple

from app.partition.months import (
    DEFAULT_FALLBACK_MONTH,
    infer_month_from_text,
    month_key_from_datetime,
    normalize_month_key,
    parse_timestamp,
)

logger = logging.getLogger(__name__)


class MonthBucket:
    """Represents a bucket of files for a specific month."""

    def __init__(self, month: str, work_dir: str):
        """Initialize a monthly bucket."""
        self.month = month
        self.work_dir = work_dir
        self.input_dir = os.path.join(work_dir, "in")
        self.files: List[str] = []
        self.tournaments: List[Dict[str, str]] = []

        os.makedirs(self.input_dir, exist_ok=True)

    def add_tournament_file(self, source_path: str, tournament_id: str, original_name: str):
        """Copy a tournament file into the bucket."""

        safe_name = self._sanitize_filename(tournament_id, original_name)
        dest_path = os.path.join(self.input_dir, safe_name)
        dest_path = self._ensure_unique_filename(dest_path)

        shutil.copy2(source_path, dest_path)
        self.files.append(dest_path)
        self.tournaments.append({
            'tournament_id': tournament_id,
            'source': original_name,
            'path': dest_path,
        })

    def finalize(self):
        """No-op kept for backwards compatibility."""
        return

    def _sanitize_filename(self, tournament_id: str, original_name: str) -> str:
        base = tournament_id or Path(original_name).stem
        safe = re.sub(r"[^a-zA-Z0-9._-]", "_", base)
        if not safe:
            safe = Path(original_name).stem or "tournament"
        return f"{safe}.txt"

    def _ensure_unique_filename(self, dest_path: str) -> str:
        base_path = Path(dest_path)
        candidate = base_path
        counter = 1

        while candidate.exists():
            candidate = base_path.with_stem(f"{base_path.stem}_{counter}")
            counter += 1

        return str(candidate)

    def get_metadata(self) -> dict:
        return {
            'month': self.month,
            'file_count': len(self.files),
            'work_dir': self.work_dir,
            'input_dir': self.input_dir,
            'tournaments': self.tournaments,
        }


def _month_from_metadata(metadata: Optional[Dict[str, Optional[str]]]) -> Optional[str]:
    if not metadata:
        return None

    month = normalize_month_key(metadata.get('tournament_month'))
    if month:
        return month

    timestamp_hint = metadata.get('timestamp')
    if timestamp_hint:
        dt = parse_timestamp(timestamp_hint)
        if dt:
            return month_key_from_datetime(dt)

    return None


def _month_from_path(file_path: Path) -> Optional[str]:
    for part in reversed(file_path.parts):
        month = infer_month_from_text(part)
        if month:
            return month
    return None


def resolve_month_for_file(
    content: str,
    file_path: Path,
    metadata: Optional[Dict[str, Optional[str]]],
) -> str:
    for candidate in (
        _month_from_metadata(metadata),
        infer_month_from_text(content),
        _month_from_path(file_path),
        infer_month_from_text(str(file_path.parent)),
        infer_month_from_text(str(file_path)),
    ):
        normalized = normalize_month_key(candidate)
        if normalized:
            return normalized

    logger.debug("Falling back to default month for %s", file_path)
    return DEFAULT_FALLBACK_MONTH


def build_month_buckets(
    token: str,
    input_dir: str,
    work_root: str,
    metadata_resolver: Optional[Callable[[str, str], Optional[Dict[str, Optional[str]]]]] = None,
) -> List[MonthBucket]:
    """
    Group files by month and create isolated work directories.
    
    Args:
        token: Processing token
        input_dir: Directory containing extracted .txt files
        work_root: Base work directory (e.g., /tmp/work)
        
    Returns:
        List of MonthBucket objects, sorted by month key
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

    buckets: Dict[str, MonthBucket] = {}
    seen: Set[Tuple[str, str]] = set()

    for file_path in txt_files:
        try:
            content = Path(file_path).read_text(encoding='utf-8', errors='ignore')
        except Exception as exc:
            logger.warning(f"[{token}] Could not read {file_path}: {exc}")
            continue

        metadata = metadata_resolver(content, Path(file_path).name) if metadata_resolver else None

        month = resolve_month_for_file(content, Path(file_path), metadata)
        tournament_id = (metadata or {}).get('tournament_id')

        if not tournament_id:
            tournament_id = Path(file_path).stem

        key = (month, tournament_id)
        if key in seen:
            logger.info(
                f"[{token}] Skipping duplicate tournament {tournament_id} for {month}"
            )
            continue

        seen.add(key)

        month_work_dir = os.path.join(work_root, token, "months", month)
        if month not in buckets:
            buckets[month] = MonthBucket(month, month_work_dir)

        buckets[month].add_tournament_file(file_path, tournament_id, Path(file_path).name)
        logger.debug(f"[{token}] Added tournament {tournament_id} to bucket {month}")

    # Finalize bucket files
    for bucket in buckets.values():
        bucket.finalize()
        logger.info(
            f"[{token}] Created bucket for {bucket.month}: {len(bucket.files)} tournament file(s)"
        )

    sorted_buckets = sorted(buckets.values(), key=lambda bucket: bucket.month)

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
        
        bucket_meta = bucket.get_metadata()

        months_data.append({
            'month': month,
            'file_count': bucket_meta['file_count'],
            'tournament_count': bucket_meta['file_count'],
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
