"""Network and tournament type detection utilities."""
import re
from pathlib import Path


def detect_network(text: str) -> str:
    """
    Detect poker network from hand history text.
    
    Args:
        text: Hand history content
        
    Returns:
        Network name: "pokerstars", "ggpoker", "888poker", "partypoker", "winamax", "wpn", "unknown"
    """
    # Check first 500 chars for speed
    header = text[:500].lower()
    
    # PokerStars
    if 'pokerstars hand #' in header or 'pokerstars tournament #' in header:
        return 'pokerstars'
    
    # GGPoker
    if 'poker hand #' in header and ('holdem' in header or 'omaha' in header):
        # GG format: "Poker Hand #TM..."
        if re.search(r'poker hand #tm\d+', header):
            return 'ggpoker'
    
    # 888.pt (check first - more specific)
    if '888.pt' in header:
        return '888.pt'
    
    # 888poker
    if '888poker' in header or '888 poker' in header:
        return '888poker'
    
    # PartyPoker
    if 'partypoker' in header or 'party poker' in header:
        return 'partypoker'
        
    # Winamax
    if 'winamax poker' in header:
        return 'winamax'
        
    # WPN (Winning Poker Network - ACR, BlackChip, etc)
    if 'game hand #' in header or 'winning poker network' in header:
        return 'wpn'
    
    return 'unknown'


def detect_tourney_type(text: str) -> str:
    """
    Detect tournament type from hand history.
    
    Args:
        text: Hand history content (at least first 1000 chars)
        
    Returns:
        Tournament type: "PKO", "MYSTERY", "NON_KO"
    """
    # Check header (first 1000 chars)
    header = text[:1000]
    header_lower = header.lower()
    
    # Mystery detection (highest priority)
    if 'mystery' in header_lower and 'bounty' in header_lower:
        return 'MYSTERY'
    
    # PKO detection patterns
    pko_patterns = [
        # Direct keywords
        'bounty hunter',
        'progressive knockout',
        'progressive ko',
        'pko',
        ' ko ',
        'knockout',
        
        # GGPoker specific
        'bounty builders',
        'bounty hunter',
        
        # Buy-in patterns (X+Y+fee format where Y is bounty)
        r'[€$£]\d+(?:\.\d+)?\s*\+\s*[€$£]\d+(?:\.\d+)?\s*\+',  # e.g., €3.37+€3.38+€0.75
        r'buy-in:\s*[$€£]\d+(?:\.\d+)?\s*\+\s*[$€£]\d+(?:\.\d+)?',  # Buy-in: $5+$5
    ]
    
    for pattern in pko_patterns:
        # Try as regex first for patterns with special regex chars
        if any(c in pattern for c in ['$', '€', '£', '\\', '+', '.', '?', '*', '(', ')', '[', ']']):
            # Regex pattern
            if re.search(pattern, header):
                return 'PKO'
        else:
            # String pattern
            if pattern in header_lower:
                return 'PKO'
    
    # Additional check for "bounty" alone (but not mystery)
    if 'bounty' in header_lower and 'mystery' not in header_lower:
        return 'PKO'
    
    # Default to NON-KO
    return 'NON_KO'


def smart_read_text(file_path: Path) -> str:
    """
    Read text file with encoding detection fallback.
    
    Args:
        file_path: Path to text file
        
    Returns:
        File content as string
        
    Tries UTF-8, then Latin-1, then CP1252
    """
    from pathlib import Path
    
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            return file_path.read_text(encoding=encoding)
        except (UnicodeDecodeError, LookupError):
            continue
            
    # Last resort: read as binary and decode with errors='replace'
    return file_path.read_text(encoding='utf-8', errors='replace')