"""
WPN (Winning Poker Network) hand history parser.
Handles ACR, BlackChip, and other WPN skin formats.
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


class WPNParser:
    """Parser for WPN hand histories."""
    
    def detect(self, text: str) -> bool:
        """Detect if this is a WPN hand history."""
        return bool(re.search(r'Game\s+Hand\s+#\d+|Winning\s+Poker\s+Network', text[:1000], re.IGNORECASE))
    
    def parse_tournament(
        self,
        text: str,
        file_id: str,
        hero_aliases: dict
    ) -> List[Hand]:
        """Parse all hands from a WPN tournament file."""
        hands = []
        hero_names = hero_aliases.get('wpn', []) + hero_aliases.get('global', [])
        
        for start_idx, end_idx, hand_text in find_hand_boundaries(text):
            try:
                hand = self._parse_hand(hand_text, file_id, hero_names, start_idx)
                if hand:
                    hands.append(hand)
            except Exception as e:
                logger.warning(f"Failed to parse WPN hand at offset {start_idx}: {e}")
                continue
        
        return hands
    
    def _parse_hand(
        self,
        hand_text: str,  
        file_id: str,
        hero_names: List[str],
        text_offset: int
    ) -> Optional[Hand]:
        """Parse a single WPN hand."""
        
        offsets = extract_offsets(hand_text, text_offset)
        street_boundaries = extract_street_boundaries(hand_text, text_offset)
        lines = hand_text.split('\n')
        
        # Ensure key offsets are present
        if 'hand_start' not in offsets:
            offsets['hand_start'] = text_offset
        if 'hand_end' not in offsets:
            offsets['hand_end'] = text_offset + len(hand_text)
        
        hand = Hand(
            site='wpn',
            file_id=file_id,
            streets=create_empty_streets(),
            raw_offsets=offsets  # Now includes hand_start, flop, turn, river, summary
        )
        
        # Parse header
        if lines:
            header = lines[0]
            
            # Extract hand ID
            hand_match = safe_match(r'Hand\s*#(\d+)', header)
            if hand_match:
                hand.tournament_id = hand_match.group(1)
            
            # Extract timestamp
            time_match = safe_match(r'(\d{4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}:\d{2})', header)
            if time_match:
                hand.timestamp_utc = time_match.group(1)
        
        # Parse table info
        for line in lines[:5]:
            if 'Table' in line:
                max_match = safe_match(r'(\d+)\s+Max', line, re.IGNORECASE)
                if max_match:
                    hand.table_max = int(max_match.group(1))
                
                button_match = safe_match(r'Seat\s*#?(\d+)\s+is\s+(?:the\s+)?(?:dealer|button)', line, re.IGNORECASE)
                if button_match:
                    hand.button_seat = int(button_match.group(1))
        
        # Parse players
        players = []
        seat_pattern = r'^Seat\s+(\d+):\s*([^(]+?)\s*\(([0-9,. ]+)\)'
        
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
        
        # Parse blinds
        for line in lines:
            sb_match = safe_match(r'([^:]+):\s*(?:Posts?\s+)?small\s+blind\s+([0-9,. ]+)', line, re.IGNORECASE)
            if sb_match:
                hand.blinds['sb'] = clean_amount(sb_match.group(2))
            
            bb_match = safe_match(r'([^:]+):\s*(?:Posts?\s+)?big\s+blind\s+([0-9,. ]+)', line, re.IGNORECASE)
            if bb_match:
                hand.blinds['bb'] = clean_amount(bb_match.group(2))
            
            ante_match = safe_match(r'([^:]+):\s*(?:Posts?\s+)?ante\s+([0-9,. ]+)', line, re.IGNORECASE)
            if ante_match:
                hand.blinds['ante'] = clean_amount(ante_match.group(2))
        
        # Parse dealt players
        for line in lines:
            if 'Dealt to' in line:
                dealt_match = safe_match(r'Dealt\s+to\s+([^\[]+)', line)
                if dealt_match:
                    player_name = normalize_player_name(dealt_match.group(1))
                    hand.players_dealt_in.append(player_name)
        
        # Parse actions
        current_street = 'preflop'
        in_action = False
        
        for i, line in enumerate(lines):
            if '*** HOLE CARDS ***' in line or '*** POCKET CARDS ***' in line:
                in_action = True
                current_street = 'preflop'
            elif '*** FLOP ***' in line:
                current_street = 'flop'
                board_match = safe_match(r'\[(.*?)\]', line)
                if board_match:
                    hand.streets['flop'].board = parse_cards(board_match.group(1))
            elif '*** TURN ***' in line:
                current_street = 'turn'
                board_match = safe_match(r'\[(.*?)\]', line)
                if board_match:
                    cards = parse_cards(board_match.group(1))
                    if len(cards) >= 4:
                        hand.streets['turn'].board = [cards[3]]
            elif '*** RIVER ***' in line:
                current_street = 'river'
                board_match = safe_match(r'\[(.*?)\]', line)
                if board_match:
                    cards = parse_cards(board_match.group(1))
                    if len(cards) >= 5:
                        hand.streets['river'].board = [cards[4]]
            elif '*** SUMMARY ***' in line:
                break
            
            if in_action:
                action = self._parse_action_line(line, text_offset + sum(len(l) + 1 for l in lines[:i]))
                if action:
                    hand.streets[current_street].actions.append(action)
                    
                    if current_street == 'preflop' and action.allin:
                        hand.any_allin_preflop = True
        
        # Calculate derived stats
        if hand.streets['flop'].actions:
            flop_actors = set(a.actor for a in hand.streets['flop'].actions
                            if a.type not in ['FOLD'])
            hand.players_to_flop = len(flop_actors)
            hand.heads_up_flop = len(flop_actors) == 2
        
        return hand
    
    def _parse_action_line(self, line: str, offset: int) -> Optional[Action]:
        """Parse a WPN action line."""
        
        if not line or line.startswith('***') or 'Dealt to' in line:
            return None
        
        patterns = [
            (r'^([^:]+):\s*Folds?', 'FOLD'),
            (r'^([^:]+):\s*Checks?', 'CHECK'),
            (r'^([^:]+):\s*Calls?\s+([0-9,. ]+)', 'CALL'),
            (r'^([^:]+):\s*Bets?\s+([0-9,. ]+)', 'BET'),
            (r'^([^:]+):\s*Raises?\s+to\s+([0-9,. ]+)', 'RAISE'),
            (r'^([^:]+):\s*All-?in\s+([0-9,. ]+)', 'ALLIN'),
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
                
                if action_type in ['CALL', 'BET', 'ALLIN'] and match.lastindex >= 2:
                    action.amount = clean_amount(match.group(2))
                elif action_type == 'RAISE' and match.lastindex >= 2:
                    action.to_amount = clean_amount(match.group(2))
                
                return action
        
        return None