"""
Date Extractor Service for Poker Hand Histories
===============================================

Extracts timestamp from hand history files across different poker sites
and normalizes to YYYY-MM format for monthly bucketing.
"""
import re
import logging
from datetime import datetime
from typing import Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


class DateExtractor:
    """
    Extract and normalize timestamps from poker hand history files.
    
    Each poker site has a different timestamp format. This class handles
    all variations and returns a normalized YYYY-MM month string.
    """
    
    # Portuguese month names for 888.pt
    PORTUGUESE_MONTHS = {
        'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4,
        'maio': 5, 'junho': 6, 'julho': 7, 'agosto': 8,
        'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12,
        # Variations without accents
        'marco': 3,
    }
    
    # Regex patterns for each site
    PATTERNS = {
        'pokerstars': r'-\s+(?P<ts>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})(?:\s*\((?P<tz_alt>[^)]+)\))?\s*(?P<tz>[A-Z]{2,4})?',
        'ggpoker': r'-\s+(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})(?:\s*(?P<tz>UTC|GMT|CET|CEST|[+-]\d{2}:?\d{2}))?',
        '888poker': r'(?P<ts>\d{4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}:\d{2})\s*(?P<tz>[A-Z]{2,4})?',
        '888pt_iso': r'(?P<ts>\d{4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}:\d{2})\s*(?P<tz>[A-Z]{2,4})?',
        '888pt_localized': r'(?P<day>\d{1,2})\s+(?P<month_pt>[A-Za-zçã]+)\s+(?P<year>\d{4})\s+(?P<time>\d{2}:\d{2})\s*(?P<tz>[A-Z]{2,4})?',
        '888_numeric': r'(?P<day>\d{1,2})\s+(?P<month>\d{2})\s+(?P<year>\d{4})\s+(?P<time>\d{2}:\d{2}:\d{2})',
        'winamax': r'-\s+(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s*(?P<tz>[A-Z]{2,4})?',
        'wpn': r'-\s+(?P<ts>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})(?:\s*\((?P<tz_alt>[^)]+)\))?\s*(?P<tz>[A-Z]{2,4})?',
    }
    
    def __init__(self):
        """Initialize the date extractor."""
        pass
    
    def extract_month_from_file(self, file_path: str) -> Optional[str]:
        """
        Extract YYYY-MM month from a hand history file.
        
        Args:
            file_path: Path to the .txt hand history file
            
        Returns:
            Month in YYYY-MM format, or None if unable to extract
        """
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                # Read only the first ~2000 characters (first hand header)
                content = f.read(2000)
            
            # Detect site from content
            site = self._detect_site(content)
            if not site:
                logger.warning(f"Could not detect poker site for file: {file_path}")
                return None
            
            # Extract timestamp
            timestamp_str, timezone = self._extract_timestamp(content, site)
            if not timestamp_str:
                logger.warning(f"Could not extract timestamp from file: {file_path}")
                return None
            
            # Parse and normalize to YYYY-MM
            month = self._normalize_to_month(timestamp_str, timezone, site)
            if month:
                logger.debug(f"Extracted month {month} from {Path(file_path).name} (site: {site})")
            
            return month
            
        except Exception as e:
            logger.error(f"Error extracting month from {file_path}: {e}")
            return None
    
    def _detect_site(self, content: str) -> Optional[str]:
        """Detect which poker site based on file content."""
        content_sample = content[:1000]
        
        if re.search(r'PokerStars\s+(?:Hand|Game|Zoom\s+Hand)', content_sample):
            return 'pokerstars'
        elif re.search(r'Poker\s+Hand\s+#\w+:|PokerTime\.eu|GGPoker', content_sample, re.IGNORECASE):
            return 'ggpoker'
        elif re.search(r'888\.pt', content_sample):
            return '888pt'
        elif re.search(r'888poker', content_sample):
            return '888poker'
        elif re.search(r'Winamax\s+Poker', content_sample, re.IGNORECASE):
            return 'winamax'
        elif re.search(r'WPN|Winning Poker Network', content_sample, re.IGNORECASE):
            return 'wpn'
        
        return None
    
    def _extract_timestamp(self, content: str, site: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract timestamp string and timezone from content.
        
        Returns:
            (timestamp_string, timezone) or (None, None)
        """
        # Get the first hand header (up to first blank line or *** HOLE CARDS ***)
        header_end = content.find('\n\n')
        if header_end == -1:
            header_end = content.find('*** HOLE CARDS ***')
        if header_end == -1:
            header_end = min(len(content), 1500)
        
        header = content[:header_end]
        
        # Try site-specific pattern
        if site in ['888pt', '888poker']:
            # Try numeric format first (DD MM YYYY HH:MM:SS)
            match = re.search(self.PATTERNS['888_numeric'], header)
            if match:
                # Convert DD MM YYYY to standard format
                day = int(match.group('day'))
                month = int(match.group('month'))
                year = int(match.group('year'))
                time = match.group('time')
                
                timestamp = f"{year:04d}-{month:02d}-{day:02d} {time}"
                return timestamp, None
            
            # If 888pt, try localized format (DD month_name YYYY)
            if site == '888pt':
                match = re.search(self.PATTERNS['888pt_localized'], header)
                if match:
                    # Convert Portuguese date to standard format
                    day = int(match.group('day'))
                    month_name = match.group('month_pt').lower()
                    year = int(match.group('year'))
                    time = match.group('time')
                    
                    month_num = self.PORTUGUESE_MONTHS.get(month_name)
                    if month_num:
                        timestamp = f"{year:04d}-{month_num:02d}-{day:02d} {time}:00"
                        tz = match.group('tz') if 'tz' in match.groupdict() else None
                        return timestamp, tz
                
                # Fallback to ISO format
                match = re.search(self.PATTERNS['888pt_iso'], header)
            else:
                # For 888poker, try ISO format
                match = re.search(self.PATTERNS['888poker'], header)
        else:
            pattern = self.PATTERNS.get(site)
            if not pattern:
                return None, None
            match = re.search(pattern, header)
        
        if not match:
            return None, None
        
        timestamp = match.group('ts')
        tz = match.groupdict().get('tz')
        
        return timestamp, tz
    
    def _normalize_to_month(self, timestamp_str: str, timezone: Optional[str], site: str) -> Optional[str]:
        """
        Parse timestamp and return YYYY-MM format.
        
        Args:
            timestamp_str: Timestamp string (various formats)
            timezone: Timezone string (ET, UTC, GMT, etc.) or None
            site: Site name for format detection
            
        Returns:
            YYYY-MM string or None
        """
        try:
            # Parse timestamp based on format
            if '/' in timestamp_str:
                # PokerStars/WPN format: 2024/02/03 19:21:42
                dt = datetime.strptime(timestamp_str, '%Y/%m/%d %H:%M:%S')
            else:
                # ISO format: 2024-02-03 19:21:42
                dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            
            # For monthly bucketing, we don't need precise timezone conversion
            # Just use the date as-is from the hand history
            # (Timezone differences don't change the month in practice)
            
            return dt.strftime('%Y-%m')
            
        except ValueError as e:
            logger.error(f"Failed to parse timestamp '{timestamp_str}': {e}")
            return None
    
    def bucket_files_by_month(self, file_paths: list) -> dict:
        """
        Group files by month.
        
        Args:
            file_paths: List of file paths to bucket
            
        Returns:
            Dict mapping month (YYYY-MM) to list of file paths
            Includes special 'unknown' key for files without extractable dates
        """
        buckets = {}
        
        for file_path in file_paths:
            month = self.extract_month_from_file(file_path)
            
            if month:
                if month not in buckets:
                    buckets[month] = []
                buckets[month].append(file_path)
            else:
                # Files without dates go to 'unknown' bucket
                if 'unknown' not in buckets:
                    buckets['unknown'] = []
                buckets['unknown'].append(file_path)
        
        # Log summary
        logger.info(f"Bucketed {len(file_paths)} files into {len(buckets)} month(s):")
        for month, files in sorted(buckets.items()):
            logger.info(f"  {month}: {len(files)} file(s)")
        
        return buckets
