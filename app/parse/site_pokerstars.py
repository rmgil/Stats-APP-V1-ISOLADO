"""
PokerStars hand history parser.
Handles PokerStars-specific format and variations.
"""

import re
import logging
from datetime import datetime
from typing import List, Dict, Optional, Set
from .schemas import Hand, Player, Action, StreetInfo, ActionType, Street
from .site_generic import find_hand_boundaries, extract_street_boundaries, create_empty_streets
from .utils import (
    extract_offsets, clean_amount,
    safe_match, normalize_player_name, parse_cards
)

logger = logging.getLogger(__name__)


class PokerStarsParser:
    """Parser for PokerStars hand histories."""
    
    def detect(self, text: str) -> bool:
        """Detect if this is a PokerStars hand history."""
        return bool(re.search(r'PokerStars\s+(Hand|Zoom Hand|Game)', text[:1000], re.IGNORECASE))

    def extract_tournament_metadata(self, text: str, file_id: str = "") -> Optional[Dict[str, Optional[str]]]:
        """Extract tournament identifier and month from the first hand."""

        for _, _, hand_text in find_hand_boundaries(text):
            lines = [line for line in hand_text.split('\n') if line.strip()]
            if not lines:
                continue

            header = lines[0]

            tournament_id = None
            tourn_match = safe_match(r'Tournament\s*#(\d+)', header)
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
                'site': 'pokerstars',
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
        """Parse all hands from a PokerStars tournament file."""
        hands = []
        hero_names = hero_aliases.get('pokerstars', []) + hero_aliases.get('global', [])
        
        # Use robust boundary detection from site_generic
        for start_idx, end_idx, hand_text in find_hand_boundaries(text):
            try:
                hand = self._parse_hand(hand_text, file_id, hero_names, start_idx)
                if hand:
                    hands.append(hand)
            except Exception as e:
                logger.warning(f"Failed to parse PokerStars hand at offset {start_idx}: {e}")
                continue
        
        return hands
    
    def _parse_hand(
        self,
        hand_text: str,
        file_id: str,
        hero_names: List[str],
        text_offset: int
    ) -> Optional[Hand]:
        """Parse a single PokerStars hand."""
        
        offsets = extract_offsets(hand_text, text_offset)
        street_boundaries = extract_street_boundaries(hand_text, text_offset)
        lines = hand_text.split('\n')
        
        # Ensure key offsets are present
        if 'hand_start' not in offsets:
            offsets['hand_start'] = text_offset
        if 'hand_end' not in offsets:
            offsets['hand_end'] = text_offset + len(hand_text)
        
        # Initialize hand with empty streets
        hand = Hand(
            site='pokerstars',
            file_id=file_id,
            streets=create_empty_streets(),
            raw_offsets=offsets  # Now includes hand_start, flop, turn, river, summary
        )
        
        # Parse header line
        # Example: PokerStars Hand #123456: Tournament #789012, $10+$1 USD Hold'em No Limit - Level I (10/20) - 2024/01/15 12:34:56 UTC
        if lines:
            header = lines[0]
            
            # Extract hand ID
            hand_match = safe_match(r'Hand\s*#(\d+)', header)
            if hand_match:
                hand.tournament_id = hand_match.group(1)
            
            # Extract tournament name
            tourn_match = safe_match(r'Tournament\s*#\d+,\s*([^-]+)', header)
            if tourn_match:
                hand.tournament_name = tourn_match.group(1).strip()
            
            # Extract timestamp
            time_match = safe_match(r'(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})', header)
            if time_match:
                hand.timestamp_utc = time_match.group(1)
        
        # Parse table info line
        # Example: Table '123456 1' 9-max Seat #5 is the button
        for line in lines[:5]:
            if 'Table' in line:
                # Extract max players
                max_match = safe_match(r'(\d+)-max', line)
                if max_match:
                    hand.table_max = int(max_match.group(1))
                
                # Extract button seat
                button_match = safe_match(r'Seat\s*#(\d+)\s+is\s+the\s+button', line)
                if button_match:
                    hand.button_seat = int(button_match.group(1))
                break
        
        # Parse players and stacks
        players = []
        seat_pattern = r'^Seat\s+(\d+):\s*([^(]+?)\s*\(([0-9,. ]+)\s+in\s+chips\)'
        
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
        
        # Parse blinds and antes, track who posted
        for line in lines:
            # Small blind
            sb_match = safe_match(r'([^:]+):\s*posts?\s+small\s+blind\s+([0-9,. ]+)', line)
            if sb_match:
                hand.blinds['sb'] = clean_amount(sb_match.group(2))
                player = normalize_player_name(sb_match.group(1))
                if player not in hand.players_dealt_in:
                    hand.players_dealt_in.append(player)
            
            # Big blind
            bb_match = safe_match(r'([^:]+):\s*posts?\s+big\s+blind\s+([0-9,. ]+)', line)
            if bb_match:
                hand.blinds['bb'] = clean_amount(bb_match.group(2))
                player = normalize_player_name(bb_match.group(1))
                if player not in hand.players_dealt_in:
                    hand.players_dealt_in.append(player)
            
            # Ante
            ante_match = safe_match(r'([^:]+):\s*posts?\s+(?:the\s+)?ante\s+([0-9,. ]+)', line)
            if ante_match:
                hand.blinds['ante'] = clean_amount(ante_match.group(2))
        
        # Parse dealt cards to identify hero first
        for line in lines:
            if line.startswith('Dealt to'):
                dealt_match = safe_match(r'Dealt\s+to\s+([^\[]+)', line)
                if dealt_match:
                    player_name = normalize_player_name(dealt_match.group(1))
                    # This is always the hero in PokerStars format
                    hand.hero = player_name
                    if player_name not in hand.players_dealt_in:
                        hand.players_dealt_in.append(player_name)
                    # Update hero flag in players list
                    for p in hand.players:
                        if p.name == player_name:
                            p.is_hero = True
                break
        
        # Track who acted preflop to determine players_dealt_in
        preflop_actors: Set[str] = set()
        
        # Parse actions by street
        current_street = 'preflop'
        in_preflop = False
        
        for i, line in enumerate(lines):
            # Check for street markers
            if '*** HOLE CARDS ***' in line:
                in_preflop = True
                current_street = 'preflop'
            elif '*** FLOP ***' in line:
                current_street = 'flop'
                # Extract flop cards
                board_match = safe_match(r'\[(.*?)\]', line)
                if board_match:
                    hand.streets['flop'].board = parse_cards(board_match.group(1))
            elif '*** TURN ***' in line:
                current_street = 'turn'
                # Extract turn card
                board_match = safe_match(r'\[(.*?)\]', line)
                if board_match:
                    cards = parse_cards(board_match.group(1))
                    if len(cards) >= 4:  # Full board shown
                        hand.streets['turn'].board = [cards[3]]  # 4th card is turn
            elif '*** RIVER ***' in line:
                current_street = 'river'
                # Extract river card
                board_match = safe_match(r'\[(.*?)\]', line)
                if board_match:
                    cards = parse_cards(board_match.group(1))
                    if len(cards) >= 5:  # Full board shown
                        hand.streets['river'].board = [cards[4]]  # 5th card is river
            elif '*** SUMMARY ***' in line or '*** SHOW DOWN ***' in line:
                break  # Stop parsing actions
            
            # Parse actions only after we've started
            if in_preflop:
                action = self._parse_action_line(line, text_offset + sum(len(l) + 1 for l in lines[:i]))
                if action:
                    hand.streets[current_street].actions.append(action)
                    
                    # Track preflop actors for players_dealt_in
                    if current_street == 'preflop':
                        preflop_actors.add(action.actor)
                        if action.actor not in hand.players_dealt_in:
                            hand.players_dealt_in.append(action.actor)
                        # Track all-in preflop
                        if action.allin:
                            hand.any_allin_preflop = True
        
        # Calculate players_to_flop
        if hand.streets['flop'].actions:
            # Count unique actors on flop
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
        """Parse a PokerStars action line."""
        
        # Skip non-action lines
        if not line or line.startswith('***') or line.startswith('Dealt to') or line.startswith('Uncalled bet'):
            return None
        
        # Check for blind/ante posts (special actions)
        if 'posts small blind' in line:
            match = safe_match(r'^([^:]+):\s*posts?\s+small\s+blind\s+([0-9,. ]+)', line)
            if match:
                return Action(
                    actor=normalize_player_name(match.group(1)),
                    type='POST_SB',
                    amount=clean_amount(match.group(2)),
                    raw_offset=offset
                )
        
        if 'posts big blind' in line:
            match = safe_match(r'^([^:]+):\s*posts?\s+big\s+blind\s+([0-9,. ]+)', line)
            if match:
                return Action(
                    actor=normalize_player_name(match.group(1)),
                    type='POST_BB',
                    amount=clean_amount(match.group(2)),
                    raw_offset=offset
                )
        
        if 'posts the ante' in line or 'posts ante' in line:
            match = safe_match(r'^([^:]+):\s*posts?\s+(?:the\s+)?ante\s+([0-9,. ]+)', line)
            if match:
                return Action(
                    actor=normalize_player_name(match.group(1)),
                    type='POST_ANTE',
                    amount=clean_amount(match.group(2)),
                    raw_offset=offset
                )
        
        # PokerStars action patterns
        patterns = [
            (r'^([^:]+):\s*folds?', 'FOLD'),
            (r'^([^:]+):\s*checks?', 'CHECK'),
            (r'^([^:]+):\s*calls?\s+([0-9,. ]+)', 'CALL'),
            (r'^([^:]+):\s*bets?\s+([0-9,. ]+)', 'BET'),
            (r'^([^:]+):\s*raises?\s+([0-9,. ]+)\s+to\s+([0-9,. ]+)', 'RAISE'),
        ]
        
        for pattern, action_type in patterns:
            match = safe_match(pattern, line)
            if match:
                actor = normalize_player_name(match.group(1))
                
                # Check if this is an all-in
                allin = 'all-in' in line.lower() or 'all in' in line.lower() or 'is all-in' in line.lower()
                
                # Use ALLIN type when explicitly all-in
                if allin and action_type in ['CALL', 'BET', 'RAISE']:
                    final_type = 'ALLIN'
                else:
                    final_type = action_type
                
                action = Action(
                    actor=actor,
                    type=final_type,
                    allin=allin,
                    raw_offset=offset
                )
                
                # Extract amounts
                if action_type == 'CALL' and match.lastindex >= 2:
                    action.amount = clean_amount(match.group(2))
                elif action_type == 'BET' and match.lastindex >= 2:
                    action.amount = clean_amount(match.group(2))
                elif action_type == 'RAISE' and match.lastindex >= 3:
                    action.amount = clean_amount(match.group(2))
                    action.to_amount = clean_amount(match.group(3))
                
                return action
        
        return None