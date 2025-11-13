"""Utility helpers to extract metadata from raw hand histories."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from app.parse.site_parsers.site_detector import detect_poker_site
from app.services.date_extractor import DateExtractor


@dataclass
class HandMetadata:
    """Metadata extracted from a single hand history text."""

    site: str = "unknown"
    tournament_id: Optional[str] = None
    hand_id: Optional[str] = None
    timestamp: Optional[str] = None
    month: Optional[str] = None

    def dedup_key(self, fallback_text: str) -> Tuple[str, str, str]:
        """Return a tuple used for deduplicating hands within an upload."""

        tournament_key = self.tournament_id or "no_tournament"

        if self.hand_id:
            hand_key = self.hand_id
        elif self.timestamp:
            hand_key = self.timestamp
        else:
            hand_key = hashlib.sha1(fallback_text.strip().encode("utf-8")).hexdigest()

        return (self.site or "unknown", tournament_key, hand_key)


def _normalize_tournament_identifier(raw_identifier: str) -> str:
    """Normalize tournament identifiers for deduplication."""

    identifier = raw_identifier.strip()

    # Winamax tournaments sometimes include quotes – keep readable but safe
    identifier = identifier.replace("\u00a0", " ")  # remove non-breaking spaces

    # Collapse whitespace
    identifier = re.sub(r"\s+", " ", identifier)

    return identifier


def _extract_tournament_id(hand_text: str) -> Optional[str]:
    """Extract tournament identifier from a hand text."""

    patterns = [
        r"Tournament\s+#([A-Za-z0-9\-]+)",
        r"Tournament\s+ID:\s*([A-Za-z0-9\-]+)",
        r"Tourney\s+#([A-Za-z0-9\-]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, hand_text, re.IGNORECASE)
        if match:
            return match.group(1)

    # Winamax style: Tournament "NAME"
    winamax_match = re.search(r'Tournament\s+"([^"]+)"', hand_text)
    if winamax_match:
        return _normalize_tournament_identifier(winamax_match.group(1))

    return None


def _extract_hand_id(hand_text: str) -> Optional[str]:
    """Extract the hand identifier using relaxed patterns."""

    patterns = [
        r"Poker Hand\s*#([A-Z0-9]+)",
        r"PokerStars\s+Hand\s*#([A-Z0-9]+)",
        r"Game\s*Hand\s*#([A-Z0-9]+)",
        r"Game\s*#([A-Z0-9]+)",
        r"Hand\s*#([A-Z0-9]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, hand_text, re.IGNORECASE)
        if match:
            return match.group(1)

    return None


def extract_hand_metadata(hand_text: str, source_file: str = "") -> HandMetadata:
    """Extract metadata (site, tournament id, timestamp) from a hand text."""

    metadata = HandMetadata()

    filename = Path(source_file).name if source_file else ""
    site = detect_poker_site(hand_text, filename)
    if site:
        metadata.site = site

    metadata.tournament_id = _extract_tournament_id(hand_text)
    metadata.hand_id = _extract_hand_id(hand_text)

    extractor = DateExtractor()
    timestamp = extractor.extract_timestamp_from_text(hand_text, site_hint=site)
    if timestamp:
        metadata.timestamp = timestamp
        metadata.month = extractor.normalize_timestamp_to_month(timestamp)
    else:
        metadata.month = extractor.extract_month_from_text(hand_text)

    if metadata.tournament_id:
        metadata.tournament_id = _normalize_tournament_identifier(metadata.tournament_id)

    return metadata

