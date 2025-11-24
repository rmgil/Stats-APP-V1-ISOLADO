"""Utilities for deriving monthly buckets for poker hands.

The helpers in this module guarantee that every hand mapped into a monthly
payload receives a month key in the canonical ``YYYY-MM`` format.  They also
provide resilient parsing with sensible fallbacks when raw timestamps are
missing or malformed.
"""

import json
import os
import hashlib
import logging
import re
from collections import defaultdict
from datetime import datetime, timezone
from dateutil import parser, tz
from typing import Dict, List, Any, Optional

# Timezone for Portugal
TZ_PT = tz.gettz("Europe/Lisbon")

logger = logging.getLogger(__name__)

MONTH_KEY_PATTERN = re.compile(r"^\d{4}-\d{2}$")
DEFAULT_FALLBACK_MONTH = "1970-01"
_FAILED_TIMESTAMP_SAMPLES: List[Dict[str, Optional[str]]] = []
_FAILED_TIMESTAMP_LOG_LIMIT = 5


def normalize_month_key(value: Optional[str]) -> Optional[str]:
    """Return ``YYYY-MM`` when *value* already matches the expected format."""

    if not isinstance(value, str):
        return None

    candidate = value.strip()
    if MONTH_KEY_PATTERN.fullmatch(candidate):
        return candidate

    return None


def parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    """Parse a timestamp string into a timezone-aware ``datetime`` in UTC."""

    if value is None:
        return None

    if isinstance(value, (int, float)):
        # Interpret numeric values as Unix timestamps
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, ValueError):
            return None

    text = str(value).strip()
    if not text:
        return None

    # Normalize common separators to simplify regex parsing
    normalized = text.replace("/", "-").replace("\u2013", "-")

    # Explicit patterns matching the HH formats seen in production
    explicit_patterns = [
        r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}",  # 2025-06-02 21:01:17
        r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}Z",  # 2025-06-02T21:01:17Z
    ]

    for pat in explicit_patterns:
        match = re.search(pat, normalized)
        if match:
            candidate = match.group(0)
            try:
                dt = parser.isoparse(candidate)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
                return dt
            except (ValueError, TypeError):
                continue

    dt: Optional[datetime] = None
    try:
        dt = parser.isoparse(normalized)
    except (ValueError, TypeError):
        try:
            dt = parser.parse(normalized)
        except (ValueError, TypeError):
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt


def parse_hand_datetime(line: str) -> Optional[datetime]:
    """Extract a timezone-aware ``datetime`` from a hand header line.

    Supports the main production formats (GG, PokerStars, 888.pt) without
    inventing fallback dates. Returns ``None`` when no pattern matches.
    """

    if not isinstance(line, str) or not line.strip():
        return None

    # GG: "... - 2025/06/20 13:40:39"
    match = re.search(r"-\s*(\d{4})/(\d{2})/(\d{2})\s+(\d{2}):(\d{2}):(\d{2})", line)
    if match:
        year, month, day, hh, mm, ss = map(int, match.groups())
        return datetime(year, month, day, hh, mm, ss, tzinfo=timezone.utc)

    # PokerStars: "... - 2025/06/30 18:02:48 WET [2025/06/30 13:02:48 ET]"
    match = re.search(
        r"-\s*(\d{4})/(\d{2})/(\d{2})\s+(\d{2}):(\d{2}):(\d{2})\s+\w+",
        line,
    )
    if match:
        year, month, day, hh, mm, ss = map(int, match.groups())
        return datetime(year, month, day, hh, mm, ss, tzinfo=timezone.utc)

    # 888.pt: "*** 07 07 2025 14:27:28"
    match = re.search(
        r"\*\*\*\s+(\d{2})\s+(\d{2})\s+(\d{4})\s+(\d{2}):(\d{2}):(\d{2})",
        line,
    )
    if match:
        day, month, year, hh, mm, ss = map(int, match.groups())
        return datetime(year, month, day, hh, mm, ss, tzinfo=timezone.utc)

    return None


def month_key_from_datetime(dt: datetime) -> str:
    """Convert a timezone-aware datetime into a ``YYYY-MM`` string."""

    dt_pt = dt.astimezone(TZ_PT)
    return f"{dt_pt.year:04d}-{dt_pt.month:02d}"


_TIMESTAMP_REGEXES = [
    re.compile(r"\d{4}[\-/]\d{2}[\-/]\d{2}[ T]\d{2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:?\d{2})?"),
    re.compile(r"\d{2}[\-/]\d{2}[\-/]\d{4}[ T]\d{2}:\d{2}(?::\d{2})?"),
]

_MONTH_REGEXES = [
    re.compile(r"(?<!\d)(\d{4}[\-_/]\d{2})(?!\d)"),
]


def infer_month_from_text(text: Optional[str]) -> Optional[str]:
    """Infer a ``YYYY-MM`` month key from a free-form text snippet."""

    if not text:
        return None

    snippet = text[:5000]

    for pattern in _TIMESTAMP_REGEXES:
        match = pattern.search(snippet)
        if not match:
            continue
        dt = parse_timestamp(match.group(0))
        if dt:
            return month_key_from_datetime(dt)

    for pattern in _MONTH_REGEXES:
        match = pattern.search(snippet)
        if not match:
            continue
        normalized = match.group(1).replace("_", "-").replace("/", "-")
        month = normalize_month_key(normalized)
        if month:
            return month

    return None


def _record_failed_timestamp(
    timestamp_utc: Optional[str],
    resolved_month: str,
    debug_context: Optional[str],
) -> None:
    """Log at most ``_FAILED_TIMESTAMP_LOG_LIMIT`` fallback samples for debug."""

    if len(_FAILED_TIMESTAMP_SAMPLES) >= _FAILED_TIMESTAMP_LOG_LIMIT:
        return

    sample = {
        "timestamp": str(timestamp_utc) if timestamp_utc is not None else None,
        "resolved_month": resolved_month,
        "context": debug_context,
    }

    _FAILED_TIMESTAMP_SAMPLES.append(sample)
    logger.debug(
        "month_bucket fallback applied (%s/%s): %s",
        len(_FAILED_TIMESTAMP_SAMPLES),
        _FAILED_TIMESTAMP_LOG_LIMIT,
        sample,
    )


def month_bucket(
    timestamp_utc: Optional[str],
    fallback_month: Optional[str] = None,
    *,
    debug_context: Optional[str] = None,
) -> str:
    """Return the month bucket for *timestamp_utc* in ``YYYY-MM`` format."""

    dt = parse_timestamp(timestamp_utc)
    if dt:
        return month_key_from_datetime(dt)

    resolved = normalize_month_key(fallback_month) or DEFAULT_FALLBACK_MONTH
    _record_failed_timestamp(timestamp_utc, resolved, debug_context)
    return resolved


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


def extract_hand_timestamp(hand: dict) -> str:
    """
    Extract the most reliable datetime string from a hand object.

    The function prioritizes explicit timestamp fields and falls back to
    scanning the raw/original text for a timestamp string when necessary.
    Returns an ISO-formatted string when successful, otherwise an empty
    string.
    """

    candidate_fields = [
        hand.get("timestamp_utc"),
        hand.get("datetime"),
        hand.get("timestamp"),
        hand.get("date"),
    ]

    for value in candidate_fields:
        dt = parse_timestamp(value)
        if dt:
            return dt.isoformat()

    # Fallback: try to extract from raw/original text when available
    raw_text = hand.get("original_text") or hand.get("raw_text") or ""
    if isinstance(raw_text, str) and raw_text.strip():
        for pattern in _TIMESTAMP_REGEXES:
            match = pattern.search(raw_text)
            if match:
                dt = parse_timestamp(match.group(0))
                if dt:
                    return dt.isoformat()

    return ""


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
    last_month_key: Optional[str] = None

    with open(hands_jsonl, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                hand = json.loads(line.strip())

                # Add hand_id if not present
                if 'hand_id' not in hand:
                    hand['hand_id'] = make_hand_id(hand)

                # Get month bucket with fallback to the last valid month
                timestamp = extract_hand_timestamp(hand)
                month_key = month_bucket(
                    timestamp,
                    fallback_month=last_month_key,
                    debug_context=f"partition:{line_num}",
                )

                last_month_key = month_key
                months_data[month_key].append(hand)

            except Exception as e:
                print(f"Warning: Could not process line {line_num}: {e}")
                try:
                    hand = json.loads(line.strip())
                    hand['hand_id'] = make_hand_id(hand)
                    timestamp = extract_hand_timestamp(hand)
                    month_key = month_bucket(
                        timestamp,
                        fallback_month=last_month_key,
                        debug_context=f"partition-error:{line_num}",
                    )
                    last_month_key = month_key
                    months_data[month_key].append(hand)
                except Exception:
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
