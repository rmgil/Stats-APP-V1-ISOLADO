"""
Generic/fallback parser for unrecognized poker site formats.
Provides robust hand delimitation and offset extraction for all parsers.
"""

import re
import logging
from typing import List, Dict, Optional, Tuple, Iterator
from datetime import datetime
from .schemas import Hand, Player, Action, StreetInfo, ActionType, Street
from .utils import (
    extract_offsets, clean_amount, 
    safe_match, normalize_player_name, parse_cards
)

logger = logging.getLogger(__name__)


def find_hand_boundaries(text: str) -> Iterator[Tuple[int, int, str]]:
    """
    Find hand boundaries using robust markers that work across all sites.
    
    This is the primary function for delimiting hands in any format.
    
    Yields:
        Tuples of (start_offset, end_offset, hand_text)
    """
    # Comprehensive list of hand start markers
    hand_start_patterns = [
        # Site-specific headers (most reliable)
        r'^PokerStars\s+(?:Hand|Game|Zoom\s+Hand)\s*#',
        r'^Poker\s+Hand\s*#',  # GGPoker
        r'^Game\s+Hand\s*#',  # WPN
        r'^Winamax\s+Poker',  # Winamax
        r'^888poker\s+(?:Hand|Game)',  # 888
        r'^#Game\s+No\s*:',  # 888 alternative
        
        # Generic hand markers
        r'^Hand\s*#\d+',
        r'^Tournament\s*#.*Hand\s*#',
        
        # Street markers as fallback (when headers are missing)
        r'^\*\*\*\s*HOLE\s+CARDS\s*\*\*\*',
        r'^\*\*\*\s*POCKET\s+CARDS\s*\*\*\*',
        r'^\*\*\*\s*PRE-?FLOP\s*\*\*\*',
        
        # Table info as last resort (only when no other header exists)
        r'^Table\s+[\'"].*[\'"].*\d+-max',
        # NOTE: Removed Seat pattern - it creates false positives by matching player lines within hands
        # r'^Seat\s+\d+:.*\(\d+.*chips?\)'
    ]
    
    # Combine patterns with OR
    combined_pattern = '|'.join(f'({p})' for p in hand_start_patterns)
    pattern = re.compile(combined_pattern, re.MULTILINE | re.IGNORECASE)
    
    matches = list(pattern.finditer(text))
    
    if not matches:
        logger.warning("No hand boundaries found in text")
        # Try to return entire text as single hand if it has poker content
        if 'fold' in text.lower() or 'call' in text.lower() or 'raise' in text.lower():
            yield (0, len(text), text.strip())
        return
    
    # Process each match
    for i, match in enumerate(matches):
        start_idx = match.start()
        
        # Find end of hand (start of next hand or EOF)
        if i < len(matches) - 1:
            end_idx = matches[i + 1].start()
        else:
            end_idx = len(text)
        
        hand_text = text[start_idx:end_idx].strip()
        
        # Validate hand has minimum content
        if hand_text and len(hand_text) > 30:
            yield (start_idx, end_idx, hand_text)


def extract_street_boundaries(hand_text: str, base_offset: int = 0) -> Dict[str, Tuple[int, int]]:
    """
    Extract boundaries (start, end) for each street section.
    
    Returns dict with tuples of (start_offset, end_offset) for each street.
    This allows precise extraction of actions per street.
    """
    offsets = extract_offsets(hand_text, base_offset)
    boundaries = {}
    
    # Define street order
    street_order = ['hole_cards', 'flop', 'turn', 'river', 'showdown', 'summary']
    
    for i, street in enumerate(street_order):
        if street in offsets:
            start = offsets[street]
            
            # Find end (start of next street or hand end)
            end = offsets.get('hand_end', len(hand_text) + base_offset)
            for next_street in street_order[i+1:]:
                if next_street in offsets:
                    end = offsets[next_street]
                    break
            
            boundaries[street] = (start, end)
    
    # Add preflop (from hand start to hole cards or first street marker)
    preflop_start = base_offset
    preflop_end = offsets.get('hole_cards', offsets.get('flop', offsets.get('hand_end', len(hand_text) + base_offset)))
    
    if 'hole_cards' in offsets:
        # Preflop actions are after hole cards until flop
        preflop_start = offsets['hole_cards']
        preflop_end = offsets.get('flop', offsets.get('turn', offsets.get('river', offsets.get('summary', offsets.get('hand_end')))))
    
    boundaries['preflop'] = (preflop_start, preflop_end)
    
    return boundaries


def create_empty_streets() -> Dict[Street, StreetInfo]:
    """
    Create empty StreetInfo objects for all streets.
    Ensures all hands have consistent structure even if some streets are missing.
    """
    return {
        'preflop': StreetInfo(actions=[], board=None),
        'flop': StreetInfo(actions=[], board=None),
        'turn': StreetInfo(actions=[], board=None),
        'river': StreetInfo(actions=[], board=None)
    }


class GenericParser:
    """Fallback parser for unknown formats with robust hand extraction."""
    
    def detect(self, text: str) -> bool:
        """
        Always returns True as this is the fallback parser.
        Should be checked last in the parser chain.
        """
        return True

    def extract_tournament_metadata(self, text: str, file_id: str = "") -> Optional[Dict[str, Optional[str]]]:
        """Extract basic tournament metadata from the first hand."""

        timestamp_pattern = r'(\d{4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}:\d{2})'

        for _, _, hand_text in find_hand_boundaries(text):
            lines = [line for line in hand_text.split('\n') if line.strip()]
            if not lines:
                continue

            tournament_id = None
            id_match = safe_match(r'Tournament\s*#([A-Za-z0-9\-]+)', hand_text)
            if id_match:
                tournament_id = id_match.group(1)

            timestamp = None
            month = None
            time_match = safe_match(timestamp_pattern, hand_text)
            if time_match:
                timestamp = time_match.group(1)
                normalized = timestamp.replace('/', '-').replace('\u2013', '-')
                try:
                    month = datetime.strptime(normalized, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m")
                except ValueError:
                    month = None

            return {
                'site': 'generic',
                'tournament_id': tournament_id,
                'timestamp': timestamp,
                'tournament_month': month,
            }

        return None
    
    def parse_tournament(
        self, 
        text: str, 
        file_id: str, 
        hero_aliases: dict
    ) -> List[Hand]:
        """
        Parse hands using robust boundary detection.
        Uses the improved hand delimitation functions.
        """
        hands = []
        hero_names = hero_aliases.get('global', [])
        
        # Use the robust boundary finder
        for start_idx, end_idx, hand_text in find_hand_boundaries(text):
            try:
                hand = self._parse_hand(hand_text, file_id, hero_names, start_idx)
                if hand:
                    hands.append(hand)
            except Exception as e:
                logger.warning(f"Failed to parse hand at offset {start_idx}: {e}")
                continue
        
        return hands
    
    def _parse_hand(
        self, 
        hand_text: str, 
        file_id: str,
        hero_names: List[str],
        text_offset: int
    ) -> Optional[Hand]:
        """Parse a single hand using robust offset extraction."""
        
        # Get offsets with base offset for click-through
        offsets = extract_offsets(hand_text, text_offset)
        
        # Get street boundaries for precise action extraction
        street_boundaries = extract_street_boundaries(hand_text, text_offset)
        
        lines = hand_text.split('\n')
        
        # Initialize hand with empty streets (ensures consistent structure)
        hand = Hand(
            site='other',
            file_id=file_id,
            streets=create_empty_streets(),  # All streets present even if empty
            raw_offsets=offsets
        )
        
        # Try to extract basic info from header
        if lines:
            header = lines[0]
            
            # Try to find hand/tournament ID
            id_match = safe_match(r'#(\d+)', header)
            if id_match:
                hand.tournament_id = id_match.group(1)
            
            # Try to find timestamp
            timestamp_patterns = [
                r'(\d{4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}:\d{2})',
                r'(\d{2}[-/]\d{2}[-/]\d{4}\s+\d{2}:\d{2}:\d{2})'
            ]
            for pattern in timestamp_patterns:
                match = safe_match(pattern, header)
                if match:
                    hand.timestamp_utc = match.group(1)
                    break
        
        # Parse players and stacks
        players = []
        seat_pattern = r'Seat\s+(\d+):\s*([^(]+)\s*\(([0-9,. ]+)'
        
        for line in lines[:20]:  # Check first 20 lines for seat info
            match = safe_match(seat_pattern, line)
            if match:
                seat = int(match.group(1))
                name = normalize_player_name(match.group(2))
                stack = clean_amount(match.group(3))
                
                is_hero = name in hero_names
                if is_hero:
                    hand.hero = name
                
                players.append(Player(
                    seat=seat,
                    name=name,
                    stack_chips=stack,
                    is_hero=is_hero
                ))
        
        hand.players = players
        
        # Parse blinds
        blind_patterns = {
            'sb': r'posts?\s+small\s+blind\s+([0-9,. ]+)',
            'bb': r'posts?\s+big\s+blind\s+([0-9,. ]+)',
            'ante': r'posts?\s+(?:the\s+)?ante\s+([0-9,. ]+)'
        }
        
        for line in lines:
            for blind_type, pattern in blind_patterns.items():
                match = safe_match(pattern, line, re.IGNORECASE)
                if match:
                    amount = clean_amount(match.group(1))
                    if amount:
                        hand.blinds[blind_type] = amount
        
        # Parse button
        button_match = safe_match(r'Seat\s+#(\d+)\s+is\s+the\s+button', hand_text)
        if button_match:
            hand.button_seat = int(button_match.group(1))
        
        # Parse dealt players
        dealt_pattern = r'Dealt\s+to\s+([^\[]+)'
        for line in lines:
            match = safe_match(dealt_pattern, line)
            if match:
                player_name = normalize_player_name(match.group(1))
                hand.players_dealt_in.append(player_name)
        
        # Parse actions by street
        current_street = 'preflop'
        
        for i, line in enumerate(lines):
            # Check for street changes
            if '*** FLOP ***' in line:
                current_street = 'flop'
                # Extract board cards
                board_match = safe_match(r'\[(.*?)\]', line)
                if board_match:
                    hand.streets['flop'].board = parse_cards(board_match.group(1))
            elif '*** TURN ***' in line:
                current_street = 'turn'
                board_match = safe_match(r'\[(.*?)\]', line)
                if board_match:
                    cards = parse_cards(board_match.group(1))
                    if cards:
                        hand.streets['turn'].board = [cards[-1]]  # Just the turn card
            elif '*** RIVER ***' in line:
                current_street = 'river'
                board_match = safe_match(r'\[(.*?)\]', line)
                if board_match:
                    cards = parse_cards(board_match.group(1))
                    if cards:
                        hand.streets['river'].board = [cards[-1]]  # Just the river card
            elif '*** SUMMARY ***' in line or '*** SHOW' in line:
                break  # Stop parsing actions
            
            # Parse actions
            action = self._parse_action_line(line, text_offset + sum(len(l) + 1 for l in lines[:i]))
            if action:
                hand.streets[current_street].actions.append(action)
                
                # Track all-in preflop
                if current_street == 'preflop' and action.allin:
                    hand.any_allin_preflop = True
        
        # Calculate derived stats
        if hand.streets['flop'].actions:
            # Count unique actors in flop
            flop_actors = set(a.actor for a in hand.streets['flop'].actions)
            hand.players_to_flop = len(flop_actors)
            hand.heads_up_flop = len(flop_actors) == 2
        
        return hand
    
    def _parse_action_line(self, line: str, offset: int) -> Optional[Action]:
        """Parse an action from a line of text."""
        
        # Skip non-action lines
        if not line or line.startswith('***') or line.startswith('Dealt to'):
            return None
        
        # Common action patterns
        patterns = [
            (r'([^:]+):\s*folds?', 'FOLD'),
            (r'([^:]+):\s*checks?', 'CHECK'),
            (r'([^:]+):\s*calls?\s+([0-9,. ]+)', 'CALL'),
            (r'([^:]+):\s*bets?\s+([0-9,. ]+)', 'BET'),
            (r'([^:]+):\s*raises?\s+([0-9,. ]+)\s+to\s+([0-9,. ]+)', 'RAISE'),
            (r'([^:]+):\s*all-?in\s+(?:for\s+)?([0-9,. ]+)?', 'ALLIN'),
        ]
        
        for pattern, action_type in patterns:
            match = safe_match(pattern, line, re.IGNORECASE)
            if match:
                actor = normalize_player_name(match.group(1))
                
                action = Action(
                    actor=actor,
                    type=action_type,
                    raw_offset=offset
                )
                
                # Extract amounts based on action type
                if action_type in ['CALL', 'BET']:
                    if match.lastindex >= 2:
                        action.amount = clean_amount(match.group(2))
                elif action_type == 'RAISE':
                    if match.lastindex >= 3:
                        action.amount = clean_amount(match.group(2))
                        action.to_amount = clean_amount(match.group(3))
                elif action_type == 'ALLIN':
                    if match.lastindex >= 2 and match.group(2):
                        action.amount = clean_amount(match.group(2))
                    action.allin = True
                
                return action
        
        return None