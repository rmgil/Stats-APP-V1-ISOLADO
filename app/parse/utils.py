"""
Utility functions for hand history parsing.
Provides text processing, regex helpers, and common parsing operations.
"""

import re
from typing import Iterator, Tuple, Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


def iter_hands(text: str) -> Iterator[Tuple[int, int, str]]:
    """
    Iterate through individual hands in tournament text.
    
    Yields tuples of (start_offset, end_offset, hand_text) for each hand.
    Uses robust markers to delimit hands across different site formats.
    
    Common hand delimiters:
    - PokerStars: "PokerStars Hand #"
    - GG: "Poker Hand #"
    - WPN: "Game Hand #"
    - Winamax: "Winamax Poker -"
    - 888: "888poker Hand #"
    - Generic: "*** HOLE CARDS ***" as fallback
    """
    # Comprehensive pattern that matches all known hand start markers
    hand_patterns = [
        # Site-specific headers
        r'^PokerStars\s+(?:Hand|Game|Zoom\s+Hand)\s*#\d+',
        r'^Poker\s+Hand\s*#\w+',  # GG
        r'^Game\s+Hand\s*#\d+',  # WPN
        r'^Winamax\s+Poker\s*-',  # Winamax
        r'^888(?:poker|\.pt)\s+Hand\s*#\d+',  # 888poker and 888.pt
        r'^#Game\s+No\s*:\s*\d+',  # 888 alternative
        
        # Generic patterns
        r'^Hand\s*#\d+',
        r'^Tournament\s*#\d+.*Hand\s*#\d+',
        
        # Fallback to HOLE CARDS if no header found
        r'^\*\*\*\s*HOLE\s+CARDS\s*\*\*\*'
    ]
    
    # Combine all patterns
    combined_pattern = '|'.join(f'({p})' for p in hand_patterns)
    hand_pattern = re.compile(combined_pattern, re.MULTILINE | re.IGNORECASE)
    
    matches = list(hand_pattern.finditer(text))
    
    if not matches and '***' in text:
        # Last resort: look for any *** marker
        fallback_pattern = re.compile(r'^\*\*\*.*\*\*\*', re.MULTILINE)
        matches = list(fallback_pattern.finditer(text))
    
    # Yield each hand with its offsets
    for i, match in enumerate(matches):
        start_idx = match.start()
        
        # Find end of this hand (start of next hand or end of text)
        if i < len(matches) - 1:
            end_idx = matches[i + 1].start()
        else:
            end_idx = len(text)
        
        hand_text = text[start_idx:end_idx].strip()
        
        # Validate it's a real hand (has some content)
        if hand_text and len(hand_text) > 50:  # Minimum reasonable hand size
            yield (start_idx, end_idx, hand_text)


def extract_offsets(hand_text: str, base_offset: int = 0) -> Dict[str, int]:
    """
    Extract text offsets for key sections of a hand.
    
    Args:
        hand_text: The text of a single hand
        base_offset: Base offset to add to all positions (for click-through to original file)
    
    Returns dict with offsets for:
    - hand_start: Beginning of hand
    - hole_cards: "*** HOLE CARDS ***" section  
    - flop: "*** FLOP ***" section
    - turn: "*** TURN ***" section
    - river: "*** RIVER ***" section
    - showdown: "*** SHOWDOWN ***" or "*** SHOW DOWN ***" section
    - summary: "*** SUMMARY ***" section
    
    All offsets are relative to base_offset if provided.
    """
    offsets = {"hand_start": base_offset}
    
    # Extended markers to catch variations across sites
    markers = {
        "hole_cards": [
            r'\*\*\*\s*HOLE\s*CARDS\s*\*\*\*',
            r'\*\*\*\s*POCKET\s*CARDS\s*\*\*\*',  # WPN variant
            r'\*\*\*\s*PRE-?FLOP\s*\*\*\*',  # Winamax variant
            r'\*\*\s*Dealing\s+down\s+cards\s*\*\*'  # 888 variant
        ],
        "flop": [
            r'\*\*\*\s*FLOP\s*\*\*\*',
            r'\*\*\s*Dealing\s+flop\s*\*\*'  # 888 variant
        ],
        "turn": [
            r'\*\*\*\s*TURN\s*\*\*\*',
            r'\*\*\s*Dealing\s+turn\s*\*\*'  # 888 variant
        ],
        "river": [
            r'\*\*\*\s*RIVER\s*\*\*\*',
            r'\*\*\s*Dealing\s+river\s*\*\*'  # 888 variant
        ],
        "showdown": [
            r'\*\*\*\s*SHOW\s*DOWN\s*\*\*\*',
            r'\*\*\*\s*SHOWDOWN\s*\*\*\*',
            r'\*\*\s*Showdown\s*\*\*'
        ],
        "summary": [
            r'\*\*\*\s*SUMMARY\s*\*\*\*',
            r'\*\*\s*Summary\s*\*\*'
        ]
    }
    
    for key, patterns in markers.items():
        for pattern in patterns:
            match = re.search(pattern, hand_text, re.IGNORECASE)
            if match:
                offsets[key] = match.start() + base_offset
                break  # Use first matching pattern
    
    offsets["hand_end"] = len(hand_text) + base_offset
    
    return offsets


def clean_amount(s: str) -> Optional[float]:
    """
    Clean and normalize monetary amounts from various formats.
    
    Handles:
    - Comma as thousands separator: "1,234.56" -> 1234.56
    - Comma as decimal separator: "1234,56" -> 1234.56  
    - Currency symbols: "$100", "€100", "£100"
    - Parentheses for all-in: "(1234)" -> 1234
    """
    if not s:
        return None
    
    # Remove currency symbols and whitespace
    s = re.sub(r'[$€£¥₹¢]', '', s).strip()
    
    # Remove parentheses (often used for all-in amounts)
    s = s.strip('()')
    
    # Handle empty string after cleaning
    if not s:
        return None
    
    try:
        # Check if comma is decimal separator (European format)
        if ',' in s and '.' not in s:
            # Single comma might be decimal separator
            parts = s.split(',')
            if len(parts) == 2 and len(parts[1]) <= 2:
                # Likely decimal separator
                s = s.replace(',', '.')
            else:
                # Likely thousands separator
                s = s.replace(',', '')
        elif ',' in s and '.' in s:
            # Both present - comma is thousands separator
            s = s.replace(',', '')
        
        return float(s)
    except (ValueError, AttributeError):
        logger.debug(f"Could not parse amount: {s}")
        return None


def safe_match(pattern: str, line: str, flags: int = 0) -> Optional[re.Match]:
    """
    Safe regex matching with error handling.
    
    Args:
        pattern: Regex pattern string
        line: Text to match against
        flags: Optional regex flags
        
    Returns:
        Match object if found, None otherwise
    """
    try:
        return re.search(pattern, line, flags)
    except re.error as e:
        logger.error(f"Regex error with pattern '{pattern}': {e}")
        return None


def normalize_player_name(name: str) -> str:
    """
    Normalize player names for consistent matching.
    
    Handles:
    - Extra whitespace
    - Special characters that might vary
    """
    # Remove extra whitespace
    name = ' '.join(name.split())
    
    # Some sites add seat numbers or other info in brackets
    # e.g., "Player1 [ME]" or "Player1 (button)"
    name = re.sub(r'\s*\[.*?\]', '', name)
    name = re.sub(r'\s*\(.*?\)', '', name)
    
    return name.strip()


def parse_cards(card_string: str) -> List[str]:
    """
    Parse card representations into standardized format.
    
    Converts various formats to standard 2-char representation:
    - "Ah Kd" -> ["Ah", "Kd"]
    - "[Ah Kd]" -> ["Ah", "Kd"]
    - "A♥ K♦" -> ["Ah", "Kd"]
    """
    if not card_string:
        return []
    
    # Remove brackets
    card_string = card_string.strip('[]')
    
    # Convert unicode suits to letters
    suit_map = {
        '♠': 's', '♣': 'c', '♥': 'h', '♦': 'd',
        '♤': 's', '♧': 'c', '♡': 'h', '♢': 'd'
    }
    
    for unicode_suit, letter in suit_map.items():
        card_string = card_string.replace(unicode_suit, letter)
    
    # Find all card patterns (rank + suit)
    card_pattern = r'[AKQJT2-9][schd]'
    cards = re.findall(card_pattern, card_string, re.IGNORECASE)
    
    # Normalize to lowercase suits
    return [card[0].upper() + card[1].lower() for card in cards]


def extract_timestamp(line: str) -> Optional[str]:
    """
    Extract timestamp from hand header line.
    
    Handles various date/time formats:
    - "2024/01/15 12:34:56"
    - "2024-01-15 12:34:56 UTC"
    - "15/01/2024 12:34:56"
    """
    # ISO-like format (YYYY-MM-DD or YYYY/MM/DD)
    iso_pattern = r'(\d{4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}:\d{2})'
    match = re.search(iso_pattern, line)
    if match:
        return match.group(1)
    
    # European format (DD/MM/YYYY)
    euro_pattern = r'(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})'
    match = re.search(euro_pattern, line)
    if match:
        return match.group(1)
    
    return None