"""
Hand splitter module - splits text files into individual poker hands
"""
import re
import logging
from typing import List, Tuple, Dict

logger = logging.getLogger(__name__)

def split_into_hands(content: str) -> List[str]:
    """
    Split a text file content into individual poker hands.
    Each hand typically starts with a specific pattern.
    """
    # Common hand delimiters across different poker sites
    # PokerStars: "PokerStars Hand #"
    # GGPoker: "Poker Hand #"
    # WPN: "Game Hand #"
    # Winamax: "Winamax Poker -"
    # 888: "888poker Hand #"
    
    # Don't check entire file for summary - may contain mixed content
    # Will check each hand individually later
    
    # Split by double newlines first, then validate each segment
    potential_hands = re.split(r'\n\s*\n+', content)
    
    hands = []
    current_hand = ""
    
    for segment in potential_hands:
        segment = segment.strip()
        if not segment:
            continue
            
        # Check if this is a tournament summary - skip it entirely
        if is_tournament_summary(segment):
            # If we have a current hand, save it before skipping the summary
            if current_hand:
                hands.append(current_hand)
                current_hand = ""
            continue  # Skip tournament summaries completely
            
        # Check if this segment starts a new hand
        if is_hand_start(segment):
            if current_hand:
                hands.append(current_hand)
            current_hand = segment
        else:
            # Continuation of current hand (only if not a summary)
            if current_hand:
                current_hand += "\n\n" + segment
            else:
                # Start new hand only if it has essential elements
                if has_essential_elements(segment):
                    current_hand = segment
    
    # Don't forget the last hand
    if current_hand:
        hands.append(current_hand)
    
    # Log detected hands
    logger.info(f"Split content into {len(hands)} potential hands")
    
    # Filter out non-tournament hands and summaries
    valid_hands = []
    for hand in hands:
        if not is_tournament_summary(hand) and has_essential_elements(hand):
            valid_hands.append(hand)
        else:
            logger.debug(f"Filtered out invalid hand/summary")
    
    if len(valid_hands) != len(hands):
        logger.info(f"Filtered {len(hands)} raw segments to {len(valid_hands)} valid tournament hands")
    
    return valid_hands

def split_into_hands_with_stats(content: str) -> Tuple[List[str], Dict[str, int]]:
    """
    Split content into individual hands and track discard statistics.
    Returns (valid_hands, discard_stats)
    """
    if not content:
        return [], {'tournament_summary': 0, 'invalid_segments': 0, 'total_segments': 0}
    
    discard_stats = {
        'tournament_summary': 0,
        'invalid_segments': 0,
        'total_segments': 0
    }
    
    # Split by double newlines first, then validate each segment
    potential_hands = re.split(r'\n\s*\n+', content)
    
    hands = []
    current_hand = ""
    
    for segment in potential_hands:
        segment = segment.strip()
        if not segment:
            continue
        
        discard_stats['total_segments'] += 1
            
        # Check if this is a tournament summary - skip it entirely
        if is_tournament_summary(segment):
            discard_stats['tournament_summary'] += 1
            # If we have a current hand, save it before skipping the summary
            if current_hand:
                hands.append(current_hand)
                current_hand = ""
            continue  # Skip tournament summaries completely
            
        # Check if this segment starts a new hand
        if is_hand_start(segment):
            if current_hand:
                hands.append(current_hand)
            current_hand = segment
        else:
            # Continuation of current hand (only if not a summary)
            if current_hand:
                current_hand += "\n\n" + segment
            else:
                # Start new hand only if it has essential elements
                if has_essential_elements(segment):
                    current_hand = segment
    
    # Don't forget the last hand
    if current_hand:
        hands.append(current_hand)
    
    # Filter out non-tournament hands and summaries
    valid_hands = []
    for hand in hands:
        if not is_tournament_summary(hand) and has_essential_elements(hand):
            valid_hands.append(hand)
        else:
            discard_stats['invalid_segments'] += 1
    
    return valid_hands, discard_stats

def is_hand_start(text: str) -> bool:
    """
    Check if text starts with a hand header pattern
    """
    patterns = [
        r'^PokerStars\s+(?:Hand|Game|Zoom\s+Hand|Home\s+Game\s+Hand)\s+#',
        r'^Poker\s+Hand\s+#',
        r'^Game\s+Hand\s+#',
        r'^Winamax\s+Poker\s+-',
        r'^888poker\s+Hand\s+#',
        r'^\*\*\*\*\*\s+888poker',
        r'^#Game\s+No\s*:',  # 888poker header format
        r'^Tournament\s+#\d+',
        r'^Hand\s+#\d+',
    ]
    
    first_line = text.split('\n')[0] if text else ""
    
    for pattern in patterns:
        if re.match(pattern, first_line, re.IGNORECASE):
            return True
    
    return False

def is_tournament_summary(text: str) -> bool:
    """Check if text is a tournament summary instead of actual hands"""
    if not text:
        return False
        
    text_lower = text[:3000].lower() if len(text) > 3000 else text.lower()
    
    # Common tournament summary indicators
    summary_indicators = [
        'tournament summary',
        'finishing players:',
        'dear player',
        'you finished in',
        'total prize pool',
        'tournament results',
        '1st:',
        '2nd:',
        '3rd:',
        'place:',
        'prize:'
    ]
    
    for indicator in summary_indicators:
        if indicator in text_lower:
            logger.debug(f"Found summary indicator: {indicator}")
            return True
    
    # Additional check for PokerStars: if it has PokerStars but no hole cards/seats
    if 'pokerstars' in text_lower:
        has_hole_cards = '*** HOLE CARDS ***' in text or 'Dealt to' in text
        has_seats = bool(re.search(r'Seat\s+\d+:', text))
        
        if not has_hole_cards and not has_seats:
            logger.debug("PokerStars content without hole cards or seats - likely a summary")
            return True
    
    return False

def is_cash_game(text: str) -> bool:
    """Check if hand is from a cash game instead of tournament"""
    if not text:
        return False
    
    # Check first few lines for cash game indicators
    text_sample = text[:1500] if len(text) > 1500 else text
    
    # Cash game indicators (no Tournament ID)
    # Different sites use different tournament formats:
    # - PokerStars/GG/etc: Tournament #12345
    # - Winamax: Tournament "name"
    has_tournament = bool(re.search(r'Tournament\s+(?:#\d+|"[^"]+")', text_sample))
    
    # Check for cash game specific patterns
    cash_patterns = [
        r'\$\d+\.\d+/\$\d+\.\d+',  # Stakes format like $0.50/$1.00
        r'Play Money',  # Play money games
        r'Hold\'em No Limit \(\$\d+\.\d+/\$\d+\.\d+\)',  # Full cash game format
        r'Cash Game',  # Explicit cash game mention
        r'Ring Game'  # Ring game mention
    ]
    
    # If no tournament ID and has hole cards/seats, check for cash patterns
    if not has_tournament:
        has_seats = bool(re.search(r'Seat\s+\d+:', text_sample))
        has_cards = '*** HOLE CARDS ***' in text or 'Dealt to' in text
        
        # If it has game elements but no tournament ID
        if has_seats and has_cards:
            # Check for explicit cash game patterns
            for pattern in cash_patterns:
                if re.search(pattern, text_sample, re.IGNORECASE):
                    return True
            
            # If it looks like a hand but has no tournament ID, likely cash
            return True
    
    return False

def has_essential_elements(hand_text: str) -> bool:
    """Check if hand has essential elements of a real playable hand"""
    # Must have seats to be a real hand
    has_seats = bool(re.search(r'Seat\s+\d+:', hand_text))
    
    # Must have cards dealt or hole cards section
    has_cards = '*** HOLE CARDS ***' in hand_text or 'Dealt to' in hand_text
    
    # Must have tournament ID for tournament hands
    # Different sites use different tournament formats
    has_tournament = bool(re.search(r'Tournament\s+(?:#\d+|"[^"]+")', hand_text[:1500] if len(hand_text) > 1500 else hand_text))
    
    # For a valid tournament hand, need seats and either cards or tournament ID
    return has_seats and (has_cards or has_tournament)

def count_active_players_888poker(hand_text: str) -> int:
    """
    Count ACTIVE players in 888poker hands.
    Active = players who posted blinds/antes or took any action preflop.
    This matches how HoldemManager counts "dealt in" players.
    """
    active_players = set()
    lines = hand_text.split('\n')
    
    # Find where preflop section starts and ends
    preflop_start = -1
    flop_idx = len(lines)
    
    for i, line in enumerate(lines):
        # Mark start of preflop
        if '** Dealing down cards **' in line or '*** HOLE CARDS ***' in line:
            preflop_start = i
        # Mark end of preflop
        elif '** Dealing flop **' in line or '*** FLOP ***' in line or '** Summary **' in line or '*** SUMMARY ***' in line:
            flop_idx = i
            break
    
    # Only process if we found preflop section
    if preflop_start == -1:
        return 0
    
    # Count all players who posted blinds/antes OR took action
    # Start searching a few lines before preflop marker to catch blinds/antes
    start_search = max(0, preflop_start - 10)
    
    for line in lines[start_search:flop_idx]:
        # Pattern for blinds/antes - these players are active
        blind_ante_match = re.match(r'^(.+?)\s+posts\s+(?:small\s+blind|big\s+blind|ante)\s+\[', line, re.IGNORECASE)
        if blind_ante_match:
            player_name = blind_ante_match.group(1).strip()
            active_players.add(player_name)
            continue
        
        # Pattern for actions - these players are also active
        action_match = re.match(r'^(.+?)\s+(?:folds|checks|calls|bets|raises|All-?in)', line, re.IGNORECASE)
        if action_match:
            player_name = action_match.group(1).strip()
            active_players.add(player_name)
    
    return len(active_players)

def count_players_in_hand(hand_text: str) -> int:
    """
    Count the number of ACTIVE players in a single hand by counting seat lines
    Only counts seats with chip counts (not summary lines)
    Excludes players marked as "out of hand" (PokerStars specific)
    For 888poker, uses action-based counting instead of seat-based.
    """
    # Check if this is an 888poker hand
    is_888poker = '888poker' in hand_text[:200] or '#Game No' in hand_text[:200]
    
    if is_888poker:
        # For 888poker, count active players based on preflop actions
        return count_active_players_888poker(hand_text)
    
    # Check if this is a WPN hand (Game Hand #)
    is_wpn = 'Game Hand #' in hand_text[:100]
    
    # Look for seat patterns WITH chip counts (to avoid counting summary lines)
    # Patterns support multiple formats:
    # 1. PokerStars/GG NON-KO: Seat X: PlayerName (chips in chips)
    # 2. PokerStars/GG PKO: Seat X: PlayerName (chips in chips, $bounty bounty)
    # 3. Winamax NON-KO: Seat X: PlayerName (chips)
    # 4. Winamax PKO: Seat X: PlayerName (chips, bounty€ bounty)
    # 5. WPN: Seat X: PlayerName (amount.00)
    
    # Patterns for different sites
    pokerstars_pattern = r'Seat\s+\d+:.*?\(\d+.*?in\s+chips.*?\)'
    winamax_pattern = r'Seat\s+\d+:.*?\(\d+(?:,.*?)?\)(?!\s*\()'  # Negative lookahead to avoid summary lines
    wpn_pattern = r'^Seat\s+\d+:\s*[^(]+\s*\([0-9.,]+\)$'  # WPN format: Seat X: Name (amount.00)
    
    # Detect if this is a PokerStars hand
    is_pokerstars = hand_text.startswith('PokerStars')
    
    # Find all seat lines with chips
    active_count = 0
    for line in hand_text.split('\n'):
        line = line.strip()  # Remove leading/trailing whitespace
        
        # Special handling for WPN
        if is_wpn:
            # WPN format: Seat 1: PlayerName (100000.00)
            # Only count seats at the beginning of the hand (not summary)
            if re.match(wpn_pattern, line):
                # Make sure it's not from the SUMMARY section
                # Check by looking at the context (summary comes after *** SUMMARY ***)
                if '*** SUMMARY ***' not in hand_text[:hand_text.find(line)]:
                    active_count += 1
        # Try PokerStars/GG pattern
        elif re.search(pokerstars_pattern, line):
            # Only check for "out of hand" if this is a PokerStars hand
            if is_pokerstars:
                # Check if player is "out of hand" (PokerStars format)
                # Examples:
                # - "out of hand (moved from another table into small blind)"
                # - "out of hand (moved from another table)"
                if 'out of hand' not in line.lower():
                    active_count += 1
            else:
                # For GG and other sites with "in chips", count all players with chips
                active_count += 1
        # Try Winamax pattern if PokerStars pattern didn't match
        elif re.search(r'^Seat\s+\d+:', line) and re.search(winamax_pattern, line):
            # This matches Winamax format - only count if line starts with Seat
            # and has the parentheses with numbers
            # Skip lines that look like summary lines
            if 'won' not in line.lower() and 'collected' not in line.lower():
                active_count += 1
    
    return active_count

def classify_hand_format(hand_text: str) -> str:
    """
    Classify a single hand as 6-max or 9-max based ONLY on player count
    NO text/word detection - purely based on number of participants
    
    For 888poker: Must have "Dealt to" (hero participated) to be valid
    
    Returns:
        '6-max' if 4-6 players
        '9-max' if 7+ players
        'discard' if less than 4 players OR hero didn't participate
    """
    # For 888poker, check if hero participated (must have "Dealt to")
    is_888poker = '888poker' in hand_text[:200] or '#Game No' in hand_text[:200]
    if is_888poker:
        # No "Dealt to" = hero didn't play = discard
        if 'Dealt to' not in hand_text:
            return 'discard'
    
    player_count = count_players_in_hand(hand_text)
    
    # Discard hands with less than 4 players
    if player_count < 4:
        return 'discard'
    
    # Simple classification based ONLY on player count
    if 4 <= player_count <= 6:
        return '6-max'
    elif player_count >= 7:
        return '9-max'
    else:
        # Should not reach here, but just in case
        return 'discard'

def analyze_file_hands(content: str) -> dict:
    """
    Analyze all hands in a file and return statistics
    """
    hands = split_into_hands(content)
    
    stats = {
        'total_hands': len(hands),
        '6-max_hands': 0,
        '9-max_hands': 0,
        'unknown_hands': 0,
        'player_counts': []
    }
    
    for hand in hands:
        format_type = classify_hand_format(hand)
        player_count = count_players_in_hand(hand)
        
        stats['player_counts'].append(player_count)
        
        if format_type == '6-max':
            stats['6-max_hands'] += 1
        elif format_type == '9-max':
            stats['9-max_hands'] += 1
        else:
            stats['unknown_hands'] += 1
    
    # Determine dominant format
    if stats['6-max_hands'] > stats['9-max_hands']:
        stats['dominant_format'] = '6-max'
    elif stats['9-max_hands'] > stats['6-max_hands']:
        stats['dominant_format'] = '9-max'
    else:
        stats['dominant_format'] = 'mixed'
    
    return stats