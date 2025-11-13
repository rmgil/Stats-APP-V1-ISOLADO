"""
GGPoker hand history parser.
Handles GGPoker-specific format and variations.
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


class GGParser:
    """Parser for GGPoker hand histories."""
    
    def detect(self, text: str) -> bool:
        """Detect if this is a GGPoker hand history."""
        return bool(re.search(r'Poker\s+Hand\s+#\w+:|GGPoker', text[:1000], re.IGNORECASE))

    def extract_tournament_metadata(self, text: str, file_id: str = "") -> Optional[Dict[str, Optional[str]]]:
        """Extract tournament metadata (ID and month) from the first hand."""

        for _, _, hand_text in find_hand_boundaries(text):
            lines = [line for line in hand_text.split('\n') if line.strip()]
            if not lines:
                continue

            header = lines[0]

            tournament_id = None
            tourn_match = safe_match(r'Tournament\s*#(\w+)', header)
            if tourn_match:
                tournament_id = tourn_match.group(1)

            timestamp = None
            month = None
            time_match = safe_match(r'(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})', header)
            if time_match:
                timestamp = time_match.group(1)
                try:
                    month = datetime.strptime(timestamp, "%Y/%m/%d %H:%M:%S").strftime("%Y-%m")
                except ValueError:
                    month = None

            return {
                'site': 'gg',
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
        """Parse all hands from a GGPoker tournament file."""
        hands = []
        hero_names = hero_aliases.get('gg', []) + hero_aliases.get('global', [])
        
        for start_idx, end_idx, hand_text in find_hand_boundaries(text):
            try:
                hand = self._parse_hand(hand_text, file_id, hero_names, start_idx)
                if hand:
                    hands.append(hand)
            except Exception as e:
                logger.warning(f"Failed to parse GG hand at offset {start_idx}: {e}")
                continue
        
        return hands
    
    def _parse_hand(
        self,
        hand_text: str,
        file_id: str,
        hero_names: List[str],
        text_offset: int
    ) -> Optional[Hand]:
        """Parse a single GGPoker hand."""
        
        offsets = extract_offsets(hand_text, text_offset)
        street_boundaries = extract_street_boundaries(hand_text, text_offset)
        lines = hand_text.split('\n')
        
        # Ensure key offsets are present
        if 'hand_start' not in offsets:
            offsets['hand_start'] = text_offset
        if 'hand_end' not in offsets:
            offsets['hand_end'] = text_offset + len(hand_text)
        
        # Initialize hand
        hand = Hand(
            site='gg',
            file_id=file_id,
            streets=create_empty_streets(),
            raw_offsets=offsets  # Now includes hand_start, flop, turn, river, summary
        )
        
        # Parse header
        # Example: Poker Hand #HD123456: Tournament #T789012 ...
        if lines:
            header = lines[0]
            
            # Extract hand ID
            hand_match = safe_match(r'Hand\s*#(\w+)', header)
            if hand_match:
                hand.tournament_id = hand_match.group(1)
            
            # Extract tournament info
            tourn_match = safe_match(r'Tournament\s*#(\w+)', header)
            if tourn_match:
                hand.tournament_name = tourn_match.group(1)
            
            # Extract timestamp
            time_match = safe_match(r'(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})', header)
            if time_match:
                hand.timestamp_utc = time_match.group(1)
        
        # Parse table info
        for line in lines[:5]:
            if 'Table' in line:
                # Extract max players
                max_match = safe_match(r'(\d+)-(?:max|handed)', line)
                if max_match:
                    hand.table_max = int(max_match.group(1))
                
                # Extract button
                button_match = safe_match(r'(?:Seat|Button)\s*#(\d+)', line)
                if button_match:
                    hand.button_seat = int(button_match.group(1))
        
        # Parse players
        players = []
        # GG format: "Seat 1: 41b6e2f6 (21,671 in chips)"
        seat_pattern = r'^Seat\s+(\d+):\s*([^(]+?)\s*\(([0-9,. ]+)(?:\s+in\s+chips)?\)'
        
        # DEBUG: Log pattern
        logger.debug(f"[GGParser] Using seat_pattern: {seat_pattern}")
        
        for line in lines:
            match = safe_match(seat_pattern, line)
            if match:
                logger.debug(f"[GGParser] MATCHED player line: {line.strip()}")
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
        
        # Parse blinds and track who posted
        for line in lines:
            # Small blind
            sb_match = safe_match(r'([^:]+):\s*(?:posts?\s+)?Small\s+Blind\s+([0-9,. ]+)', line, re.IGNORECASE)
            if sb_match:
                hand.blinds['sb'] = clean_amount(sb_match.group(2))
                player = normalize_player_name(sb_match.group(1))
                if player not in hand.players_dealt_in:
                    hand.players_dealt_in.append(player)
            
            # Big blind
            bb_match = safe_match(r'([^:]+):\s*(?:posts?\s+)?Big\s+Blind\s+([0-9,. ]+)', line, re.IGNORECASE)
            if bb_match:
                hand.blinds['bb'] = clean_amount(bb_match.group(2))
                player = normalize_player_name(bb_match.group(1))
                if player not in hand.players_dealt_in:
                    hand.players_dealt_in.append(player)
            
            # Ante
            ante_match = safe_match(r'([^:]+):\s*Ante\s+([0-9,. ]+)', line, re.IGNORECASE)
            if ante_match:
                hand.blinds['ante'] = clean_amount(ante_match.group(2))
        
        # Parse dealt players and identify hero
        for line in lines:
            if 'Dealt to' in line:
                dealt_match = safe_match(r'Dealt\s+to\s+([^\[]+)', line)
                if dealt_match:
                    player_name = normalize_player_name(dealt_match.group(1))
                    hand.hero = player_name  # "Dealt to" indicates hero
                    if player_name not in hand.players_dealt_in:
                        hand.players_dealt_in.append(player_name)
                    # Update hero flag
                    for p in hand.players:
                        if p.name == player_name:
                            p.is_hero = True
        
        # Track who acted preflop
        preflop_actors: Set[str] = set()
        
        # Parse actions by street
        current_street = 'preflop'
        in_action = False
        
        for i, line in enumerate(lines):
            # Check for street changes
            if '*** HOLE CARDS ***' in line:
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
            
            # Parse actions
            if in_action:
                action = self._parse_action_line(line, text_offset + sum(len(l) + 1 for l in lines[:i]))
                if action:
                    hand.streets[current_street].actions.append(action)
                    
                    # Track preflop actors
                    if current_street == 'preflop':
                        preflop_actors.add(action.actor)
                        if action.actor not in hand.players_dealt_in:
                            hand.players_dealt_in.append(action.actor)
                        if action.allin:
                            hand.any_allin_preflop = True
        
        # Calculate players_to_flop
        if hand.streets['flop'].actions:
            flop_actors = set(a.actor for a in hand.streets['flop'].actions 
                            if a.type != 'FOLD')
            hand.players_to_flop = len(flop_actors)
        else:
            # No flop actions, count players who didn't fold preflop
            folded_preflop = set(a.actor for a in hand.streets['preflop'].actions 
                               if a.type == 'FOLD')
            active_players = set(hand.players_dealt_in) - folded_preflop
            hand.players_to_flop = len(active_players)
        
        hand.heads_up_flop = (hand.players_to_flop == 2)
        
        return hand
    
    def _parse_action_line(self, line: str, offset: int) -> Optional[Action]:
        """Parse a GGPoker action line."""
        
        # Skip non-action lines
        if not line or line.startswith('***') or 'Dealt to' in line:
            return None
        
        # Check for blind/ante posts
        if 'Small Blind' in line:
            match = safe_match(r'^([^:]+):\s*(?:posts?\s+)?Small\s+Blind\s+([0-9,. ]+)', line, re.IGNORECASE)
            if match:
                return Action(
                    actor=normalize_player_name(match.group(1)),
                    type='POST_SB',
                    amount=clean_amount(match.group(2)),
                    raw_offset=offset
                )
        
        if 'Big Blind' in line:
            match = safe_match(r'^([^:]+):\s*(?:posts?\s+)?Big\s+Blind\s+([0-9,. ]+)', line, re.IGNORECASE)
            if match:
                return Action(
                    actor=normalize_player_name(match.group(1)),
                    type='POST_BB',
                    amount=clean_amount(match.group(2)),
                    raw_offset=offset
                )
        
        if 'Ante' in line:
            match = safe_match(r'^([^:]+):\s*Ante\s+([0-9,. ]+)', line, re.IGNORECASE)
            if match:
                return Action(
                    actor=normalize_player_name(match.group(1)),
                    type='POST_ANTE',
                    amount=clean_amount(match.group(2)),
                    raw_offset=offset
                )
        
        # GG action patterns
        patterns = [
            (r'^([^:]+):\s*Folds?', 'FOLD'),
            (r'^([^:]+):\s*Checks?', 'CHECK'),
            (r'^([^:]+):\s*Calls?\s+([0-9,. ]+)', 'CALL'),
            (r'^([^:]+):\s*Bets?\s+([0-9,. ]+)', 'BET'),
            (r'^([^:]+):\s*Raises?\s+([0-9,. ]+)\s+to\s+([0-9,. ]+)', 'RAISE'),  # Fixed: captures raise amount and to_amount
            (r'^([^:]+):\s*All-?in\s+([0-9,. ]+)', 'ALLIN'),
        ]
        
        for pattern, action_type in patterns:
            match = safe_match(pattern, line, re.IGNORECASE)
            if match:
                actor = normalize_player_name(match.group(1))
                
                # Check for all-in
                allin = action_type == 'ALLIN' or 'all-in' in line.lower() or 'all in' in line.lower()
                
                action = Action(
                    actor=actor,
                    type=action_type,
                    allin=allin,
                    raw_offset=offset
                )
                
                # Extract amounts
                if action_type in ['CALL', 'BET', 'ALLIN'] and match.lastindex >= 2:
                    action.amount = clean_amount(match.group(2))
                elif action_type == 'RAISE' and match.lastindex >= 3:
                    # RAISE now has 3 groups: player, raise_amount, to_amount
                    action.amount = clean_amount(match.group(2))  # raise amount
                    action.to_amount = clean_amount(match.group(3))  # total amount
                
                return action
        
        return None