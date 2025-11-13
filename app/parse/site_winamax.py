"""
Winamax hand history parser.
Handles Winamax-specific French/European format.
"""

import re
import logging
from datetime import datetime
from typing import List, Optional, Set, Dict
from .schemas import Hand, Player, Action, StreetInfo
from .site_generic import find_hand_boundaries, extract_street_boundaries, create_empty_streets
from .utils import (
    extract_offsets, clean_amount,
    safe_match, normalize_player_name, parse_cards
)

logger = logging.getLogger(__name__)


class WinamaxParser:
    """Parser for Winamax hand histories."""
    
    def detect(self, text: str) -> bool:
        """Detect if this is a Winamax hand history."""
        return bool(re.search(r'Winamax\s+Poker', text[:1000], re.IGNORECASE))

    def extract_tournament_metadata(self, text: str, file_id: str = "") -> Optional[Dict[str, Optional[str]]]:
        """Extract tournament metadata (ID and month) from the first hand."""

        time_patterns = [
            (r'(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})', "%Y/%m/%d %H:%M:%S"),
            (r'(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})', "%d/%m/%Y %H:%M:%S"),
        ]

        for _, _, hand_text in find_hand_boundaries(text):
            lines = [line for line in hand_text.split('\n') if line.strip()]
            if not lines:
                continue

            header = lines[0]

            tournament_id = None
            tourn_match = safe_match(r'Tournament\s+"([^"]+)"', header)
            if tourn_match:
                tournament_id = tourn_match.group(1).strip()

            timestamp = None
            month = None
            for pattern, fmt in time_patterns:
                time_match = safe_match(pattern, header)
                if time_match:
                    timestamp = time_match.group(1)
                    try:
                        month = datetime.strptime(timestamp, fmt).strftime("%Y-%m")
                    except ValueError:
                        month = None
                    break

            return {
                'site': 'winamax',
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
        """Parse all hands from a Winamax tournament file."""
        hands = []
        hero_names = hero_aliases.get('winamax', []) + hero_aliases.get('global', [])
        
        for start_idx, end_idx, hand_text in find_hand_boundaries(text):
            try:
                hand = self._parse_hand(hand_text, file_id, hero_names, start_idx)
                if hand:
                    hands.append(hand)
            except Exception as e:
                logger.warning(f"Failed to parse Winamax hand at offset {start_idx}: {e}")
                continue
        
        return hands
    
    def _parse_hand(
        self,
        hand_text: str,
        file_id: str,
        hero_names: List[str],
        text_offset: int
    ) -> Optional[Hand]:
        """Parse a single Winamax hand."""
        
        offsets = extract_offsets(hand_text, text_offset)
        street_boundaries = extract_street_boundaries(hand_text, text_offset)
        lines = hand_text.split('\n')
        
        # Ensure key offsets are present
        if 'hand_start' not in offsets:
            offsets['hand_start'] = text_offset
        if 'hand_end' not in offsets:
            offsets['hand_end'] = text_offset + len(hand_text)
        
        hand = Hand(
            site='winamax',
            file_id=file_id,
            streets=create_empty_streets(),
            raw_offsets=offsets  # Now includes hand_start, flop, turn, river, summary
        )
        
        # Parse header
        if lines:
            header = lines[0]
            
            # Extract hand ID
            hand_match = safe_match(r'HandId:\s*#(\d+)', header)
            if hand_match:
                hand.tournament_id = hand_match.group(1)
            
            # Extract timestamp (European format common)
            time_patterns = [
                r'(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})',
                r'(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})'
            ]
            for pattern in time_patterns:
                time_match = safe_match(pattern, header)
                if time_match:
                    hand.timestamp_utc = time_match.group(1)
                    break
        
        # Parse table info
        for line in lines[:5]:
            if 'Table' in line:
                # Winamax often uses 6-max, 9-max
                max_match = safe_match(r'(\d+)-max', line)
                if max_match:
                    hand.table_max = int(max_match.group(1))
                
                # Button position
                button_match = safe_match(r'Seat\s*#(\d+)\s+is\s+the\s+button', line)
                if button_match:
                    hand.button_seat = int(button_match.group(1))
        
        # Parse players
        players = []
        # Winamax format: Seat 1: PlayerName (1500)
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
        
        # Parse blinds (Winamax may use French terms)
        for line in lines:
            # Small blind (petite blind)
            sb_patterns = [
                r'([^:]+):\s*posts?\s+small\s+blind\s+([0-9,. ]+)',
                r'([^:]+):\s*posts?\s+petite\s+blind\s+([0-9,. ]+)'
            ]
            for pattern in sb_patterns:
                sb_match = safe_match(pattern, line, re.IGNORECASE)
                if sb_match:
                    hand.blinds['sb'] = clean_amount(sb_match.group(2))
                    break
            
            # Big blind (grosse blind)
            bb_patterns = [
                r'([^:]+):\s*posts?\s+big\s+blind\s+([0-9,. ]+)',
                r'([^:]+):\s*posts?\s+grosse\s+blind\s+([0-9,. ]+)'
            ]
            for pattern in bb_patterns:
                bb_match = safe_match(pattern, line, re.IGNORECASE)
                if bb_match:
                    hand.blinds['bb'] = clean_amount(bb_match.group(2))
                    break
            
            # Ante
            ante_match = safe_match(r'([^:]+):\s*posts?\s+ante\s+([0-9,. ]+)', line, re.IGNORECASE)
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
            # Winamax may use *** PRE-FLOP *** instead of *** HOLE CARDS ***
            if '*** PRE-FLOP ***' in line or '*** HOLE CARDS ***' in line:
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
            elif '*** SUMMARY ***' in line or '*** SHOWDOWN ***' in line:
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
        """Parse a Winamax action line (supports French terms)."""
        
        if not line or line.startswith('***') or 'Dealt to' in line:
            return None
        
        # Support both English and French action terms
        patterns = [
            (r'^([^:]+):\s*(?:folds?|se\s+couche)', 'FOLD'),
            (r'^([^:]+):\s*(?:checks?|checke)', 'CHECK'),
            (r'^([^:]+):\s*(?:calls?\s+([0-9,. ]+)|suit\s+([0-9,. ]+))', 'CALL'),
            (r'^([^:]+):\s*(?:bets?\s+([0-9,. ]+)|mise\s+([0-9,. ]+))', 'BET'),
            (r'^([^:]+):\s*(?:raises?\s+to\s+([0-9,. ]+)|relance\s+à\s+([0-9,. ]+))', 'RAISE'),
            (r'^([^:]+):\s*(?:all-?in\s+([0-9,. ]+)|fait\s+tapis\s+([0-9,. ]+))', 'ALLIN'),
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
                
                # Extract amounts (handle both English and French groups)
                if action_type in ['CALL', 'BET', 'ALLIN']:
                    for i in range(2, match.lastindex + 1):
                        amount = clean_amount(match.group(i))
                        if amount is not None:
                            action.amount = amount
                            break
                elif action_type == 'RAISE':
                    for i in range(2, match.lastindex + 1):
                        amount = clean_amount(match.group(i))
                        if amount is not None:
                            action.to_amount = amount
                            break
                
                return action
        
        return None