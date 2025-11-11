"""PokerStars specific parser."""
import re
from typing import List, Dict, Optional
from .base_parser import BaseParser


class PokerStarsParser(BaseParser):
    """Parser for PokerStars hand histories."""
    
    def __init__(self):
        super().__init__()
        self.site_name = "PokerStars"
    
    def can_parse(self, text: str) -> bool:
        """Check if this is a PokerStars hand."""
        # Look in more text for better detection
        return bool(re.search(r'PokerStars\s+(?:Hand|Game|Home\s+Game\s+Hand|Zoom\s+Hand)\s+#', text[:2000]))
    
    def is_tournament(self, text: str) -> bool:
        """Check if hand is from tournament and is a real playable hand."""
        # Check if it's a tournament summary (should be ignored)
        if self._is_summary(text):
            return False
        
        # Must have essential hand elements
        has_hole_cards = '*** HOLE CARDS ***' in text or 'Dealt to' in text
        has_seats = bool(re.search(r'Seat\s+\d+:', text))
        
        # If missing essential elements, not a valid hand
        if not has_hole_cards or not has_seats:
            return False
        
        # PokerStars tournaments have "Tournament #" format
        has_tournament = bool(re.search(r'Tournament\s+#\d+', text[:1000]))
        # Cash games have specific markers or lack tournament ID
        is_cash = bool(re.search(r'Cash Game|Ring Game|\$\d+/\$\d+\s+USD', text[:1000]))
        
        return has_tournament and not is_cash
    
    def _is_summary(self, text: str) -> bool:
        """Check if this is a tournament summary instead of a hand."""
        text_lower = text[:2000].lower()
        summary_keywords = [
            'tournament summary',
            'finishing players',
            'you finished in',
            'total prize pool',
            'tournament results'
        ]
        return any(keyword in text_lower for keyword in summary_keywords)
    
    def split_hands(self, text: str) -> List[str]:
        """Split text into individual hands."""
        # PokerStars hands can have various formats
        pattern = r'(?=PokerStars\s+(?:Hand|Game|Zoom\s+Hand|Home\s+Game\s+Hand)\s+#\d+:)'
        hands = re.split(pattern, text)
        
        # Filter out empty hands and validate each one
        valid_hands = []
        for hand in hands:
            hand = hand.strip()
            if hand and 'PokerStars' in hand:
                # Basic validation - must have key elements
                if self.is_tournament(hand):
                    valid_hands.append(hand)
        
        return valid_hands
    
    def extract_hand_info(self, hand_text: str) -> Dict:
        """Extract all information from a PokerStars hand."""
        info = {
            'site': 'pokerstars',
            'original_text': hand_text,
            'is_cash_game': False
        }
        
        lines = hand_text.split('\n')
        if not lines:
            return info
        
        # Parse header line
        header = lines[0]
        
        # Extract hand ID
        hand_match = re.search(r'Hand\s+#(\d+):', header)
        if hand_match:
            info['hand_id'] = hand_match.group(1)
        
        # Extract tournament ID
        tourn_match = re.search(r'Tournament\s+#(\d+)', header)
        if tourn_match:
            info['tournament_id'] = tourn_match.group(1)
        
        # Check if PKO (format: $10+$10+$2 or similar)
        info['is_pko'] = self.is_pko_tournament(hand_text) or bool(re.search(r'\$\d+\+\$\d+\+\$\d+', header))
        
        # Extract table info and size
        table_match = re.search(r"(\d+)'?\s*(?:-max|max)", hand_text, re.IGNORECASE)
        if table_match:
            info['table_size'] = int(table_match.group(1))
        else:
            # Count seats to determine size
            seat_count = len(re.findall(r'^Seat\s+\d+:', hand_text, re.MULTILINE))
            info['table_size'] = 9 if seat_count > 6 else 6
        
        # Extract button position (support both "Seat #X" and "Seat X" formats)
        button_match = re.search(r'Seat\s+#?(\d+)\s+is\s+the\s+button', hand_text)
        if button_match:
            info['button_seat'] = int(button_match.group(1))
        
        # Extract players
        info['players'] = self._extract_players(hand_text)
        
        # Extract hero
        info['hero'] = self.extract_hero_name(hand_text)
        
        # Extract hero cards
        dealt_match = re.search(r'Dealt\s+to\s+([^\[]+)\s*\[([^\]]+)\]', hand_text)
        if dealt_match:
            info['hero_cards'] = dealt_match.group(2).strip()
        
        # Extract blinds and ante
        info['blinds'] = self._extract_blinds(hand_text)
        info['ante'] = self._extract_ante(hand_text)
        
        # Extract actions
        info['actions'] = self.extract_actions(hand_text)
        
        # CRITICAL: Mark mathematical all-ins (ante doesn't count as available stack)
        info['actions'] = self._mark_mathematical_allins(
            players=info['players'],
            ante=info['ante'],
            actions=info['actions']
        )
        
        # Extract board
        info['board'] = self._extract_board(hand_text)
        
        # Extract showdown information
        info['showdown'] = self._extract_showdown(hand_text)
        
        # Extract winners and pot collection
        winners_info = self._extract_winners(hand_text)
        info['winners'] = winners_info['winners']
        info['pot_collected'] = winners_info['pot_collected']
        info['total_pot'] = winners_info['total_pot']
        
        return info
    
    def extract_hero_name(self, hand_text: str) -> Optional[str]:
        """Extract hero name from 'Dealt to' line."""
        dealt_match = re.search(r'Dealt\s+to\s+([^\[]+)', hand_text)
        if dealt_match:
            return dealt_match.group(1).strip()
        return None
    
    def extract_actions(self, hand_text: str) -> Dict[str, List[Dict]]:
        """Extract all actions by street."""
        actions = {
            'preflop': [],
            'flop': [],
            'turn': [],
            'river': []
        }
        
        lines = hand_text.split('\n')
        current_street = None
        
        for line in lines:
            line = line.strip()
            
            # Detect street markers
            if '*** HOLE CARDS ***' in line:
                current_street = 'preflop'
                continue
            elif '*** FLOP ***' in line:
                current_street = 'flop'
                continue
            elif '*** TURN ***' in line:
                current_street = 'turn'
                continue
            elif '*** RIVER ***' in line:
                current_street = 'river'
                continue
            elif '*** SUMMARY ***' in line or '*** SHOW DOWN ***' in line:
                break
            
            # Parse action if we're in a street
            if current_street:
                action = self._parse_action_line(line)
                if action:
                    actions[current_street].append(action)
        
        return actions
    
    def _extract_players(self, hand_text: str) -> List[Dict]:
        """Extract player information - exclude 'out of hand' players."""
        players = []
        # PokerStars formats:
        # NON-KO: Seat 1: PlayerName (30252 in chips)
        # PKO: Seat 1: PlayerName (30252 in chips, $5 bounty)
        # Updated pattern to handle both formats with optional bounty
        pattern = r'^Seat\s+(\d+):\s*([^(]+?)\s*\(([0-9,. ]+)\s+in\s+chips(?:,\s*[^\)]+)?\)(.*)$'
        
        for match in re.finditer(pattern, hand_text, re.MULTILINE):
            # Check if player is 'out of hand' - they don't participate in this hand
            line_suffix = match.group(4) if match.group(4) else ""
            if 'out of hand' in line_suffix:
                continue  # Skip players not in the hand
                
            seat = int(match.group(1))
            name = match.group(2).strip()
            # Use parse_amount like GG parser to handle various number formats
            stack = self.parse_amount(match.group(3))
            
            players.append({
                'seat': seat,
                'name': name,
                'stack': stack
            })
        
        return players
    
    def _extract_blinds(self, hand_text: str) -> Dict[str, float]:
        """Extract blind amounts from posts or header."""
        blinds = {'sb': 0, 'bb': 0}
        
        # Try to extract from header level (e.g., "Level III (150/300)")
        level_match = re.search(r'Level\s+\w+\s*\((\d+)/(\d+)\)', hand_text)
        if level_match:
            blinds['sb'] = float(level_match.group(1))
            blinds['bb'] = float(level_match.group(2))
        else:
            # Extract from posts - use same pattern as GG
            # Format: "PlayerName: posts small blind 150"
            sb_match = re.search(r'posts?\s+(?:the\s+)?small\s+blind\s+([0-9,. ]+)', hand_text, re.IGNORECASE)
            if sb_match:
                blinds['sb'] = self.parse_amount(sb_match.group(1))
            
            bb_match = re.search(r'posts?\s+(?:the\s+)?big\s+blind\s+([0-9,. ]+)', hand_text, re.IGNORECASE)
            if bb_match:
                blinds['bb'] = self.parse_amount(bb_match.group(1))
        
        return blinds
    
    def _extract_ante(self, hand_text: str) -> float:
        """Extract ante amount."""
        # Format: "PlayerName: posts the ante 40" - use GG pattern
        ante_match = re.search(r'posts?\s+(?:the\s+)?ante\s+([0-9,. ]+)', hand_text, re.IGNORECASE)
        if ante_match:
            return self.parse_amount(ante_match.group(1))
        return 0
    
    def _extract_board(self, hand_text: str) -> Dict[str, str]:
        """Extract board cards by street."""
        board = {}
        
        # PokerStars format: *** FLOP *** [Ks 2c 4h]
        flop_match = re.search(r'\*\*\* FLOP \*\*\*\s*\[([^\]]+)\]', hand_text)
        if flop_match:
            board['flop'] = flop_match.group(1).strip()
        
        # Turn includes previous cards: *** TURN *** [Ks 2c 4h] [5s]
        turn_match = re.search(r'\*\*\* TURN \*\*\*\s*\[[^\]]+\]\s*\[([^\]]+)\]', hand_text)
        if turn_match:
            board['turn'] = turn_match.group(1).strip()
        
        # River: *** RIVER *** [Ks 2c 4h 5s] [5h]
        river_match = re.search(r'\*\*\* RIVER \*\*\*\s*\[[^\]]+\]\s*\[([^\]]+)\]', hand_text)
        if river_match:
            board['river'] = river_match.group(1).strip()
        
        return board
    
    def _parse_action_line(self, line: str) -> Optional[Dict]:
        """Parse a single action line."""
        # Skip non-action lines
        if not line or line.startswith('***') or 'Dealt to' in line:
            return None
        
        # Skip blind/ante posts
        if 'posts' in line and any(x in line for x in ['blind', 'ante']):
            return None
        
        # Skip seat info and other non-action lines
        if line.startswith('Seat ') or 'is sitting out' in line or 'has timed out' in line:
            return None
        
        # Skip "out of hand" and similar
        if 'out of hand' in line or 'leaves the table' in line:
            return None
        
        # PokerStars format: "PlayerName: action [amount]"
        
        # Fold - use case-insensitive like GG
        if re.search(r'^([^:]+):\s*folds?', line, re.IGNORECASE):
            match = re.search(r'^([^:]+):\s*folds?', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'fold',
                    'amount': None
                }
        
        # Check - use case-insensitive like GG
        if re.search(r'^([^:]+):\s*checks?', line, re.IGNORECASE):
            match = re.search(r'^([^:]+):\s*checks?', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'check',
                    'amount': None
                }
        
        # Detect all-in first, then determine if it's raise/bet/call all-in
        is_allin = 'all-in' in line.lower() or 'allin' in line.lower()
        
        # Raise (including raise all-in)
        if ': raises' in line:
            # Format: "Player: raises 1000 to 2000 [and is all-in]"
            match = re.search(r'^([^:]+):\s*raises?\s+.*to\s+([0-9,. ]+)', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'raise',
                    'amount': self.parse_amount(match.group(2)),
                    'is_allin': is_allin  # CRITICAL: Preserve all-in status!
                }
        
        # Bet (including bet all-in)  
        if ': bets' in line:
            match = re.search(r'^([^:]+):\s*bets?\s+([0-9,. ]+)', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'bet',
                    'amount': self.parse_amount(match.group(2)),
                    'is_allin': is_allin  # CRITICAL: Preserve all-in status!
                }
        
        # Call (including call all-in)
        if ': calls' in line:
            match = re.search(r'^([^:]+):\s*calls?\s+([0-9,. ]+)', line, re.IGNORECASE)
            if match:
                return {
                    'player': match.group(1).strip(),
                    'action': 'call',
                    'amount': self.parse_amount(match.group(2)),
                    'is_allin': is_allin  # CRITICAL: Preserve all-in status!
                }
        
        # Pure all-in (standalone all-in action)
        if is_allin:
            player_match = re.search(r'^([^:]+):', line)
            if player_match:
                amount_match = re.search(r'([0-9,. ]+)', line)
                return {
                    'player': player_match.group(1).strip(),
                    'action': 'all-in',
                    'amount': self.parse_amount(amount_match.group(1)) if amount_match else None,
                    'is_allin': True
                }
        
        return None
    
    def _extract_showdown(self, hand_text: str) -> Dict:
        """Extract showdown information."""
        showdown = {
            'players_showed': [],
            'players_mucked': [],
            'hands_shown': {}
        }
        
        # PokerStars showdown format:
        # "PlayerName: shows [Ah Kd] (pair of Aces)"
        # "PlayerName: mucks hand"
        # "PlayerName: mucks [cards]"
        
        # Extract players who showed cards
        for match in re.finditer(r'^([^:]+):\s*shows?\s+\[([^\]]+)\]', hand_text, re.MULTILINE | re.IGNORECASE):
            player = match.group(1).strip()
            cards = match.group(2).strip()
            showdown['players_showed'].append(player)
            showdown['hands_shown'][player] = cards
        
        # Extract players who mucked
        for match in re.finditer(r'^([^:]+):\s*mucks?(?:\s+hand)?', hand_text, re.MULTILINE | re.IGNORECASE):
            player = match.group(1).strip()
            # Only add if not already in showed list
            if player not in showdown['players_showed']:
                showdown['players_mucked'].append(player)
        
        return showdown
    
    def _extract_winners(self, hand_text: str) -> Dict:
        """Extract winners and pot collection information."""
        winners = []
        pot_collected = {}
        total_pot = 0.0
        
        # PokerStars winner format:
        # "PlayerName collected 10000 from pot"
        # "PlayerName collected 5000 from side pot"
        # "PlayerName collected 3000 from main pot"
        # Exclude "Uncalled bet" lines
        
        for match in re.finditer(r'^([^:]+)\s+collected\s+([0-9,. ]+)\s+from', hand_text, re.MULTILINE):
            player = match.group(1).strip()
            # Skip if this is an "Uncalled bet" line
            if 'uncalled bet' in player.lower():
                continue
            amount = self.parse_amount(match.group(2))
            
            if player not in winners:
                winners.append(player)
            
            # Accumulate amounts if player won multiple pots
            if player in pot_collected:
                pot_collected[player] += amount
            else:
                pot_collected[player] = amount
            
            total_pot += amount
        
        # Also check for "Total pot" line in summary
        pot_match = re.search(r'Total pot\s+([0-9,. ]+)', hand_text)
        if pot_match:
            # Use the explicitly stated total pot if available
            stated_total = self.parse_amount(pot_match.group(1))
            if stated_total > total_pot:
                total_pot = stated_total
        
        return {
            'winners': winners,
            'pot_collected': pot_collected,
            'total_pot': total_pot
        }