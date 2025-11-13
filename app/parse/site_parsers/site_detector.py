"""Automatic detection of poker site from hand history text."""
import re
from typing import Optional


def is_tournament_summary(text: str, filename: str = "") -> bool:
    """Check if text is a tournament summary (not actual hands).
    
    Tournament summaries should be ignored as they don't contain playable hands.
    """
    # Check first 2000 chars for summary indicators
    text_start = text[:2000].lower()
    
    # WPN specific check: .ots files are always tournament summaries
    if filename.lower().endswith('.ots'):
        return True
    
    # Common tournament summary indicators
    summary_indicators = [
        'tournament summary',
        'finishing players:',
        'dear player',
        'you finished in',
        'total prize pool',
        'tournament results',
        'place:',
        'prize:',
        '1st:',
        '2nd:',
        '3rd:',
        'tournament placement'
    ]
    
    # If any indicator is found, it's likely a summary
    for indicator in summary_indicators:
        if indicator in text_start:
            return True
    
    # Additional check: summaries often lack essential hand elements
    has_hole_cards = '*** HOLE CARDS ***' in text or 'Dealt to' in text
    has_seats = bool(re.search(r'Seat\s+\d+:', text[:2000]))
    
    # If it claims to be from PokerStars but lacks hole cards and seats, likely a summary
    if 'pokerstars' in text_start and not has_hole_cards and not has_seats:
        return True
    
    return False


def detect_poker_site(text: str, filename: str = "") -> Optional[str]:
    """Detect which poker site the hand history is from.
    
    Returns:
        Site name string or None if not detected
    """
    # First check if this is a tournament summary (not a hand history)
    if is_tournament_summary(text, filename):
        return None
    
    # Check for unique patterns from each site
    # Increased to 5000 chars to catch PokerStars hands that may start later
    text_sample = text[:5000]
    
    # GG Poker patterns
    if 'GGPoker Hand #' in text_sample or ('Poker Hand #' in text_sample and 'PokerTime.eu' in text_sample):
        return 'ggpoker'
    
    # GG Poker with TM/HH hand IDs (Tournament/Hand History format)
    if re.search(r'Poker Hand #(?:TM|HH)', text_sample):
        return 'ggpoker'
    
    # 888.pt patterns (check BEFORE 888poker - more specific)
    if re.search(r'888\.pt\s+Hand History', text_sample) or re.search(r'\*{5}\s+888\.pt', text_sample):
        return '888.pt'
    
    # 888poker patterns
    if re.search(r'888poker\s+Hand History', text_sample) or re.search(r'\*{5}\s+888poker', text_sample):
        return '888poker'
    
    # PokerStars patterns - check in full text for better detection
    # PokerStars hands can start with various prefixes
    if any(pattern in text[:10000] for pattern in [
        'PokerStars Hand #',
        'PokerStars Home Game Hand #', 
        'PokerStars Game #',
        'PokerStars Zoom Hand #'
    ]):
        return 'pokerstars'
    
    # Additional PokerStars check - look for their specific format
    if re.search(r'PokerStars\s+(?:Hand|Game|Zoom\s+Hand)\s+#\d+:', text[:10000]):
        return 'pokerstars'
    
    # WPN (Winning Poker Network) patterns
    if 'Game Hand #' in text_sample and '*** HOLE CARDS ***' in text_sample:
        return 'wpn'
    
    # Winamax patterns
    if 'Winamax Poker' in text_sample:
        return 'winamax'
    
    # Party Poker patterns
    if '***** Hand History For Game' in text_sample and 'Tourney Texas Holdem' in text_sample:
        return 'partypoker'
    
    # iPoker patterns
    if 'GAME #' in text_sample and 'TEXAS_HOLDEM' in text_sample:
        return 'ipoker'
    
    # Additional GG Poker check (alternative format)
    if re.search(r'Hand\s+\#\w+\s+\|\s+Hold\'em', text_sample):
        return 'ggpoker'
    
    return None


def get_parser(site_name: str):
    """Get the appropriate parser for a poker site.
    
    Args:
        site_name: Name of the poker site
        
    Returns:
        Parser instance or None if site not supported
    """
    from .gg_parser import GGPokerParser
    from .pokerstars_parser import PokerStarsParser
    from .wpn_parser import WPNParser
    from .winamax_parser import WinamaxParser
    from .party_parser import PartyPokerParser
    from .eight88_parser import Eight88PokerParser
    from .eight88pt_parser import Eight88PtParser
    
    parsers = {
        'ggpoker': GGPokerParser,
        'pokerstars': PokerStarsParser,
        'wpn': WPNParser,
        'winamax': WinamaxParser,
        'partypoker': PartyPokerParser,
        '888poker': Eight88PokerParser,
        '888.pt': Eight88PtParser,
        'ipoker': None  # TODO: Add iPoker parser if needed
    }
    
    parser_class = parsers.get(site_name)
    if parser_class:
        return parser_class()
    return None


def parse_hand_universal(hand_text: str):
    """Parse a hand using automatic site detection.
    
    Args:
        hand_text: Raw hand history text
        
    Returns:
        Parsed hand dict or None if parsing fails
    """
    site = detect_poker_site(hand_text)
    if not site:
        return None
        
    parser = get_parser(site)
    if not parser:
        return None
    
    # Check if it's a tournament hand (not cash game)
    if not parser.is_tournament(hand_text):
        return None
    
    try:
        return parser.extract_hand_info(hand_text)
    except Exception:
        return None