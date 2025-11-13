"""
888poker hand history parser.
Handles 888poker and 888.pt specific format.
Supports both $ and € currencies.
"""

import re
import logging
from typing import List, Optional, Set
from .schemas import Hand, Player, Action, StreetInfo
from .site_generic import find_hand_boundaries, extract_street_boundaries, create_empty_streets
from .utils import (
    extract_offsets, clean_amount,
    safe_match, normalize_player_name, parse_cards
)

logger = logging.getLogger(__name__)


class Poker888Parser:
    """Parser for 888poker and 888.pt hand histories."""
    
    def detect(self, text: str) -> bool:
        """Detect if this is an 888poker or 888.pt hand history."""
        return bool(re.search(r'888(?:poker|\.pt)|#Game\s+No', text[:1000], re.IGNORECASE))
    
    def parse_tournament(
        self,
        text: str,
        file_id: str,
        hero_aliases: dict
    ) -> List[Hand]:
        """Parse all hands from an 888poker or 888.pt tournament file."""
        hands = []
        hero_names = hero_aliases.get('888', []) + hero_aliases.get('global', [])
        
        # Detect tournament type from filename
        tournament_class = self._detect_tournament_type(file_id)
        
        for start_idx, end_idx, hand_text in find_hand_boundaries(text):
            try:
                hand = self._parse_hand(hand_text, file_id, hero_names, start_idx)
                if hand:
                    # Set tournament class based on filename
                    # Store in raw_offsets since Hand schema doesn't have tournament_class field
                    hand.raw_offsets['tournament_class'] = tournament_class
                    hands.append(hand)
            except Exception as e:
                logger.warning(f"Failed to parse 888 hand at offset {start_idx}: {e}")
                continue
        
        return hands
    
    def _detect_tournament_type(self, filename: str) -> str:
        """
        Detect tournament type based on filename keywords for 888poker.
        Returns: 'pko', 'mystery', or 'non-ko'
        """
        filename_lower = filename.lower()
        
        # Check for Mystery first (highest priority)
        if 'mystery' in filename_lower:
            return 'mystery'
        
        # Check for PKO (but not if Mystery is present)
        if 'pko' in filename_lower:
            return 'pko'
        
        # Default to NON-KO
        return 'non-ko'
    
    def _parse_hand(
        self,
        hand_text: str,
        file_id: str,
        hero_names: List[str],
        text_offset: int
    ) -> Optional[Hand]:
        """Parse a single 888poker or 888.pt hand."""
        
        offsets = extract_offsets(hand_text, text_offset)
        street_boundaries = extract_street_boundaries(hand_text, text_offset)
        lines = hand_text.split('\n')
        
        # Ensure key offsets are present
        if 'hand_start' not in offsets:
            offsets['hand_start'] = text_offset
        if 'hand_end' not in offsets:
            offsets['hand_end'] = text_offset + len(hand_text)
        
        hand = Hand(
            site='888',
            file_id=file_id,
            streets=create_empty_streets(),
            raw_offsets=offsets  # Now includes hand_start, flop, turn, river, summary
        )
        
        # Parse header
        # Example: #Game No : 778014934
        if lines:
            header = lines[0]
            
            # Extract game/hand ID - store in raw_offsets for reference
            game_match = safe_match(r'#Game\s+No\s*:\s*(\d+)', header)
            if game_match:
                # Store hand ID in raw_offsets since Hand schema doesn't have hand_id field
                hand.raw_offsets['game_no'] = game_match.group(1)
        
        # Parse tournament info and timestamp from following lines
        for line in lines[:10]:
            # Tournament ID - Format: Tournament #277007279 $ 10 + $ 1 or Tournament #279085703 9,90 € + 1,10 €
            tourn_match = safe_match(r'Tournament\s+#(\d+)', line)
            if tourn_match:
                hand.tournament_id = tourn_match.group(1)
            
            # Extract timestamp - Format: *** 04 08 2025 09:26:37
            time_match = safe_match(r'\*\*\*\s+(\d{2})\s+(\d{2})\s+(\d{4})\s+(\d{2}:\d{2}:\d{2})', line)
            if time_match:
                day = time_match.group(1)
                month = time_match.group(2) 
                year = time_match.group(3)
                time = time_match.group(4)
                # Construct timestamp in standard format
                hand.timestamp_utc = f"{year}-{month}-{day} {time}"
        
        # Parse table info
        for line in lines[:10]:
            if 'Table' in line:
                # Extract max players
                max_match = safe_match(r'(\d+)\s+Max', line, re.IGNORECASE)
                if max_match:
                    hand.table_max = int(max_match.group(1))
                
                # Extract button
                button_match = safe_match(r'Seat\s*(\d+)\s+is\s+the\s+button', line)
                if button_match:
                    hand.button_seat = int(button_match.group(1))
        
        # Parse players - 888 uses a different format
        players = []
        # Format: Seat 1: PlayerName ( 5.695 ) or Seat 1: PlayerName ( 10.000 )
        seat_pattern = r'^Seat\s+(\d+):\s*([^\(]+?)\s*\(\s*([0-9,. ]+)\s*\)'
        
        for line in lines:
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
        
        # Parse blinds - 888 format uses brackets
        for line in lines:
            # Small blind - Format: PlayerName posts small blind [30]
            # Use (.+?) to capture names with spaces
            sb_match = safe_match(r'(.+?)\s+posts\s+small\s+blind\s+\[([0-9,. ]+)\]', line, re.IGNORECASE)
            if sb_match:
                hand.blinds['sb'] = clean_amount(sb_match.group(2))
            
            # Big blind - Format: PlayerName posts big blind [60]
            bb_match = safe_match(r'(.+?)\s+posts\s+big\s+blind\s+\[([0-9,. ]+)\]', line, re.IGNORECASE)
            if bb_match:
                hand.blinds['bb'] = clean_amount(bb_match.group(2))
            
            # Ante - Format: PlayerName posts ante [7]
            ante_match = safe_match(r'(.+?)\s+posts\s+ante\s+\[([0-9,. ]+)\]', line, re.IGNORECASE)
            if ante_match:
                hand.blinds['ante'] = clean_amount(ante_match.group(2))
        
        # Parse dealt players and identify hero
        for line in lines:
            if 'Dealt to' in line:
                # 888 format: "Dealt to 98_wel [ 9s, 6s ]"
                dealt_match = safe_match(r'Dealt\s+to\s+([^\[]+)\s*\[', line)
                if dealt_match:
                    player_name = normalize_player_name(dealt_match.group(1))
                    if player_name not in hand.players_dealt_in:
                        hand.players_dealt_in.append(player_name)
                    
                    # This player is the hero (we can see their cards)
                    hand.hero = player_name
                    
                    # Update player list to mark hero
                    for player in hand.players:
                        if player.name == player_name:
                            player.is_hero = True
                    
                    # Parse hero cards - store in raw_offsets for reference
                    cards_match = safe_match(r'\[\s*([^\]]+)\s*\]', line)
                    if cards_match:
                        # Store hero cards in raw_offsets since Hand schema doesn't have hero_cards field
                        hand.raw_offsets['hero_cards'] = cards_match.group(1)
        
        # Parse actions
        current_street = None
        in_action = False
        
        for i, line in enumerate(lines):
            # 888 uses "** Dealing" markers
            if '** Dealing down cards **' in line or '*** HOLE CARDS ***' in line:
                in_action = True
                current_street = 'preflop'
            elif '** Dealing flop **' in line or '*** FLOP ***' in line:
                current_street = 'flop'
                # Try to extract board
                board_match = safe_match(r'\[(.*?)\]', line)
                if board_match:
                    hand.streets['flop'].board = parse_cards(board_match.group(1))
                else:
                    # 888 might show cards on next line
                    if i + 1 < len(lines):
                        next_line = lines[i + 1]
                        board_match = safe_match(r'\[(.*?)\]', next_line)
                        if board_match:
                            hand.streets['flop'].board = parse_cards(board_match.group(1))
            elif '** Dealing turn **' in line or '*** TURN ***' in line:
                current_street = 'turn'
                board_match = safe_match(r'\[(.*?)\]', line)
                if board_match:
                    cards = parse_cards(board_match.group(1))
                    if cards:
                        hand.streets['turn'].board = [cards[-1]]
            elif '** Dealing river **' in line or '*** RIVER ***' in line:
                current_street = 'river'
                board_match = safe_match(r'\[(.*?)\]', line)
                if board_match:
                    cards = parse_cards(board_match.group(1))
                    if cards:
                        hand.streets['river'].board = [cards[-1]]
            elif '** Summary **' in line or '*** SUMMARY ***' in line:
                break
            
            if in_action and current_street:
                action = self._parse_action_line(line, text_offset + sum(len(l) + 1 for l in lines[:i]))
                if action:
                    hand.streets[current_street].actions.append(action)
                    
                    if current_street == 'preflop' and action.allin:
                        hand.any_allin_preflop = True
        
        # Calculate derived stats
        if hand.streets.get('flop') and hand.streets['flop'].actions:
            flop_actors = set(a.actor for a in hand.streets['flop'].actions
                            if a.type not in ['FOLD'])
            hand.players_to_flop = len(flop_actors)
            hand.heads_up_flop = len(flop_actors) == 2
        
        return hand
    
    def _parse_action_line(self, line: str, offset: int) -> Optional[Action]:
        """Parse an 888poker or 888.pt action line."""
        
        if not line or line.startswith('**') or line.startswith('***') or 'Dealt to' in line:
            return None
        
        # 888 action patterns - uses brackets for amounts
        # Use (.+?) to capture names with spaces
        patterns = [
            (r'^(.+?)\s+folds', 'FOLD'),
            (r'^(.+?)\s+checks', 'CHECK'),
            (r'^(.+?)\s+calls\s+\[([0-9,. ]+)\]', 'CALL'),
            (r'^(.+?)\s+bets\s+\[([0-9,. ]+)\]', 'BET'),
            (r'^(.+?)\s+raises\s+\[([0-9,. ]+)\]', 'RAISE'),
            (r'^(.+?)\s+All-?in(?:\s+\[([0-9,. ]+)\])?', 'ALLIN'),
        ]
        
        for pattern, action_type in patterns:
            match = safe_match(pattern, line, re.IGNORECASE)
            if match:
                actor = normalize_player_name(match.group(1))
                
                action = Action(
                    actor=actor,
                    type=action_type,
                    allin=(action_type == 'ALLIN'),
                    raw_offset=offset
                )
                
                # Extract amounts
                if action_type in ['CALL', 'BET'] and match.lastindex >= 2:
                    action.amount = clean_amount(match.group(2))
                elif action_type == 'RAISE':
                    if match.lastindex >= 2:
                        action.amount = clean_amount(match.group(2))
                    if match.lastindex >= 3 and match.group(3):
                        action.to_amount = clean_amount(match.group(3))
                elif action_type == 'ALLIN' and match.lastindex >= 2 and match.group(2):
                    action.amount = clean_amount(match.group(2))
                
                return action
        
        return None